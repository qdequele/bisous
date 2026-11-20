//! [`PlanetScaleSource`]: the `Source` lifecycle.

use std::collections::HashMap;

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, Capabilities, Change, CheckReport, CheckStatus, Checkpoint, CollectionInfo,
    Document, EventSender, LagReporter, Op, Opened, Source, SourceError, SourceEvent,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use serde_json::Value as Json;
use tokio_util::sync::CancellationToken;

use crate::config::PlanetScaleConfig;
use crate::convert;
use crate::error;
use crate::schema::{self, TableSchema, quote_ident};
use crate::stream::{Decoder, Needs, TableState};
use crate::vstream::{self, Position};

pub struct PlanetScaleSource {
    pub(crate) config: PlanetScaleConfig,
    /// Mapped table names, inside `config.database`.
    pub(crate) tables: Vec<String>,
    schemas: HashMap<String, TableSchema>,
    position: Option<Position>,
    lag: Option<LagReporter>,
}

impl std::fmt::Debug for PlanetScaleSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanetScaleSource")
            .field("config", &self.config)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl PlanetScaleSource {
    pub fn new(config: PlanetScaleConfig, tables: Vec<String>) -> Self {
        Self {
            config,
            tables,
            schemas: HashMap::new(),
            position: None,
            lag: None,
        }
    }

    fn collection(&self, table: &str) -> String {
        format!("{}.{table}", self.config.database)
    }

    async fn client(&self) -> Result<vstream::Client, SourceError> {
        Ok(vstream::tune(vstream::connect(&self.config).await?))
    }

    /// The mapped tables' schemas: every one must exist and have a key.
    async fn load_mapped(&mut self, conn: &mut Conn) -> Result<(), SourceError> {
        let found = schema::load_tables(conn, &self.config.database, Some(&self.tables)).await?;
        let mut schemas = HashMap::new();
        for t in &self.tables {
            let Some(s) = found.get(t) else {
                return Err(SourceError::Config(format!(
                    "table {} not found (or not visible to this user)",
                    self.collection(t)
                )));
            };
            if s.key.is_empty() {
                return Err(SourceError::Config(format!(
                    "table {} has no primary key; add one to sync it",
                    self.collection(t)
                )));
            }
            schemas.insert(t.clone(), s.clone());
        }
        self.schemas = schemas;
        Ok(())
    }

    async fn emit(events: &EventSender, cancel: &CancellationToken, ev: SourceEvent) -> bool {
        tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            r = events.send(ev) => r.is_ok(),
        }
    }

    /// One table, by keyset pages over its primary key, through the MySQL
    /// text protocol (values in the same text form VStream sends). Returns
    /// `false` when cancelled or the engine went away.
    async fn snapshot_table(
        &self,
        conn: &mut Conn,
        schema: &TableSchema,
        events: &EventSender,
        cancel: &CancellationToken,
    ) -> Result<bool, SourceError> {
        let collection = self.collection(&schema.table);
        let cols = schema
            .columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let keys: Vec<String> = schema
            .key
            .iter()
            .map(|&i| quote_ident(&schema.columns[i].name))
            .collect();
        let from = quote_ident(&schema.table);
        let order = keys.join(", ");
        let batch = self.config.snapshot_batch_size;
        let mut last: Option<Vec<Option<Vec<u8>>>> = None;
        loop {
            if cancel.is_cancelled() {
                return Ok(false);
            }
            let filter = match &last {
                None => String::new(),
                Some(vals) => format!(
                    " WHERE ({order}) > ({})",
                    schema
                        .key
                        .iter()
                        .zip(vals)
                        .map(|(&i, v)| literal(&schema.columns[i], v.as_deref()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            };
            let sql = format!("SELECT {cols} FROM {from}{filter} ORDER BY {order} LIMIT {batch}");
            let rows: Vec<mysql_async::Row> = conn.query(sql).await.map_err(error::mysql)?;
            let n = rows.len();
            for row in rows {
                let cells: Vec<Option<Vec<u8>>> = (0..schema.columns.len())
                    .map(|i| match row.as_ref(i) {
                        Some(Value::Bytes(b)) => Some(b.clone()),
                        Some(Value::NULL) | None => None,
                        // The text protocol only returns bytes or NULL.
                        Some(other) => Some(other.as_sql(true).into_bytes()),
                    })
                    .collect();
                let mut doc = Document::new();
                let mut values = Vec::with_capacity(cells.len());
                let mut bad = None;
                for (col, cell) in schema.columns.iter().zip(&cells) {
                    match convert::to_json(col, cell.as_deref(), false) {
                        Ok(v) => {
                            values.push(v.clone());
                            doc.insert(col.name.clone(), v);
                        }
                        Err(e) => {
                            bad = Some(e);
                            break;
                        }
                    }
                }
                let key_cells: Vec<Option<Vec<u8>>> =
                    schema.key.iter().map(|&i| cells[i].clone()).collect();
                last = Some(key_cells);
                let ev = match bad {
                    Some(e) => SourceEvent::Skipped {
                        collection: collection.clone(),
                        position: "snapshot".into(),
                        reason: e.to_string(),
                    },
                    None => SourceEvent::Change(Change {
                        collection: collection.clone(),
                        key: schema
                            .key
                            .iter()
                            .map(|&i| values.get(i).cloned().unwrap_or(Json::Null))
                            .collect(),
                        op: Op::Upsert { doc },
                    }),
                };
                if !Self::emit(events, cancel, ev).await {
                    return Ok(false);
                }
            }
            if n < batch as usize {
                return Ok(true);
            }
        }
    }
}

/// A key value as a SQL literal, from its text form: hex, so nothing in it
/// is ever read as SQL. Character columns compare as utf8mb4 strings, which
/// MySQL also converts for numbers and dates.
fn literal(col: &convert::ColumnMeta, v: Option<&[u8]>) -> String {
    let Some(b) = v else {
        return "NULL".into();
    };
    let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
    let binary = col.binary_charset
        || matches!(
            col.data_type.as_str(),
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit"
        );
    if b.is_empty() {
        "''".into()
    } else if binary {
        format!("X'{hex}'")
    } else {
        format!("CONVERT(X'{hex}' USING utf8mb4)")
    }
}

#[async_trait]
impl Source for PlanetScaleSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        let mut report = CheckReport::default();
        let mut conn = match schema::connect(&self.config).await {
            Ok(c) => c,
            Err(e @ (SourceError::Config(_) | SourceError::Transient(_))) => {
                report.push(
                    "connection",
                    CheckStatus::Failed,
                    format!(
                        "{e}. Use the database's host, user and password (Settings, Passwords); \
                         the MySQL port is {}",
                        self.config.mysql_port
                    ),
                );
                return Ok(report);
            }
            Err(e) => return Err(e),
        };
        report.push(
            "connection",
            CheckStatus::Ok,
            format!(
                "connected to {}:{}",
                self.config.host, self.config.mysql_port
            ),
        );

        let mut found = schema::load_tables(&mut conn, &self.config.database, None).await?;
        if self.tables.is_empty() {
            report.push_none_selected("tables", "table");
        }
        for t in &self.tables {
            let name = self.collection(t);
            match found.remove(t) {
                None => report.push(
                    &name,
                    CheckStatus::Failed,
                    "table not found (or not visible to this user)",
                ),
                Some(s) if s.key.is_empty() => report.push(
                    &name,
                    CheckStatus::Failed,
                    "table has no primary key; add one to sync it",
                ),
                Some(_) => {
                    let probe = format!("SELECT 1 FROM {} LIMIT 0", quote_ident(t));
                    match conn.query_drop(probe).await.map_err(error::mysql) {
                        Ok(()) => {
                            report.push(&name, CheckStatus::Ok, "readable, has a primary key")
                        }
                        Err(SourceError::Config(m)) => {
                            report.push(&name, CheckStatus::Failed, format!("no SELECT: {m}"));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        let _ = conn.disconnect().await;

        // VStream: reachable, and gives a position for the mapped tables
        // (or for any table before any is picked).
        let tables = if self.tables.is_empty() {
            vec!["/.*".to_owned()]
        } else {
            self.tables.clone()
        };
        let vstream = async {
            let mut client = self.client().await?;
            vstream::current_position(&self.config, &mut client, &tables).await
        };
        match vstream.await {
            Ok(pos) => report.push(
                "vstream",
                CheckStatus::Ok,
                format!(
                    "VStream reachable on port {}, {} shard{}",
                    self.config.grpc_port,
                    pos.shards.len(),
                    if pos.shards.len() == 1 { "" } else { "s" }
                ),
            ),
            Err(e) => report.push(
                "vstream",
                CheckStatus::Failed,
                format!(
                    "{e}. VStream (gRPC, port {}) uses the same host, user and password",
                    self.config.grpc_port
                ),
            ),
        }
        Ok(report)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let mut conn = schema::connect(&self.config).await?;
        let tables = schema::load_tables(&mut conn, &self.config.database, None).await?;
        let _ = conn.disconnect().await;
        Ok(tables
            .values()
            .map(|t| schema::collection_info(&self.config.database, t))
            .collect())
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        self.position = None;
        if self.tables.is_empty() {
            return Err(SourceError::Config("no table is mapped".into()));
        }
        let mut conn = schema::connect(&self.config).await?;
        self.load_mapped(&mut conn).await?;
        let _ = conn.disconnect().await;
        match resume {
            Some(cp) => {
                self.position = Some(Position::from_checkpoint(&cp, &self.config.database)?);
                Ok(Opened::Resumed)
            }
            None => {
                // Captured before any snapshot read: gapless (changes in
                // between are streamed again, idempotently).
                let mut client = self.client().await?;
                let pos =
                    vstream::current_position(&self.config, &mut client, &self.tables).await?;
                let start = pos.to_checkpoint()?;
                self.position = Some(pos);
                Ok(Opened::Fresh { start })
            }
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let mut conn = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            c = schema::connect(&self.config) => c?,
        };
        // Pages may be far apart when the engine is slow.
        conn.query_drop("SET SESSION net_write_timeout = 3600, net_read_timeout = 3600")
            .await
            .map_err(error::mysql)?;
        let mut tables: Vec<&TableSchema> = self.schemas.values().collect();
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        for t in tables {
            if !self.snapshot_table(&mut conn, t, &events, &cancel).await? {
                return Ok(());
            }
        }
        let _ = conn.disconnect().await;
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        // Nothing on the origin to release: positions live in the checkpoint.
        drop(acks);
        let Some(position) = self.position.clone() else {
            return Err(SourceError::Fatal("stream called before open".into()));
        };
        let mut client = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            c = self.client() => c?,
        };
        let req = vstream::request(&self.config, position.to_vgtid(), &self.tables);
        let mut stream = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            s = vstream::open_stream(&mut client, req) => s?,
        };
        let tables = self
            .schemas
            .iter()
            .map(|(k, v)| (k.clone(), TableState::new(v.clone())))
            .collect();
        let mut decoder = Decoder::new(&self.config.database, tables, position, self.lag.clone());
        let mut out = Vec::new();
        loop {
            let msg = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                m = stream.message() => m,
            };
            let resp = match msg {
                Ok(Some(r)) => r,
                Ok(None) => {
                    return Err(SourceError::Transient("VStream ended; reconnecting".into()));
                }
                Err(s) => return Err(error::grpc(&s)),
            };
            for ev in &resp.events {
                let mut needs = Needs::default();
                decoder.apply(ev, &mut out, &mut needs)?;
                if !needs.reload.is_empty() {
                    // A column the schema does not know: DDL since open.
                    let mut conn = schema::connect(&self.config).await?;
                    let fresh =
                        schema::load_tables(&mut conn, &self.config.database, Some(&needs.reload))
                            .await?;
                    let _ = conn.disconnect().await;
                    for (name, s) in fresh {
                        if let Some(state) = decoder.tables.get_mut(&name) {
                            state.schema = s;
                        }
                        out.push(SourceEvent::SchemaChanged {
                            collection: self.collection(&name),
                        });
                    }
                    if let Some(f) = &ev.field_event {
                        decoder.apply_fields(f);
                    }
                }
                for e in out.drain(..) {
                    if !Self::emit(&events, &cancel, e).await {
                        return Ok(());
                    }
                }
            }
            self.position = Some(decoder.position.clone());
        }
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        // Nothing was created on the origin.
        Ok(())
    }

    fn report_lag(&mut self, reporter: LagReporter) {
        self.lag = Some(reporter);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::ColumnMeta;

    #[test]
    fn key_literals_never_carry_sql() {
        let int = ColumnMeta::new("id", "bigint", "bigint", false, None);
        assert_eq!(literal(&int, Some(b"42")), "CONVERT(X'3432' USING utf8mb4)");
        let text = ColumnMeta::new("k", "varchar", "varchar(9)", false, None);
        assert_eq!(
            literal(&text, Some(b"'; DROP")),
            "CONVERT(X'273B2044524F50' USING utf8mb4)"
        );
        let bin = ColumnMeta::new("k", "varbinary", "varbinary(4)", false, None);
        assert_eq!(literal(&bin, Some(&[0, 255])), "X'00FF'");
        assert_eq!(literal(&text, Some(b"")), "''");
        assert_eq!(literal(&text, None), "NULL");
    }
}
