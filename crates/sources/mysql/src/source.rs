//! `MysqlSource`: check, discover, open, snapshot. The binlog loop lives in
//! `stream.rs`.

use std::collections::BTreeSet;
use std::future::Future;

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, Capabilities, Change, CheckReport, Checkpoint, CollectionInfo, Document,
    EventSender, Op, Opened, Source, SourceError, SourceEvent, SourceFactory, SourceKind,
    SourceSpec, StreamMapping,
};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Row, Value};
use tokio_util::sync::CancellationToken;

use crate::config::{CONNECT_TIMEOUT, MysqlConfig};
use crate::convert::{Origin, value_to_json};
use crate::error::map_err;
use crate::gtid::GtidSet;
use crate::position::Position;
use crate::schema::{
    TableSchema, collection_info, load_table, load_tables, quote_ident, split_collection,
};

/// Builds [`MysqlSource`]s for `kind: mysql`.
#[derive(Debug, Default, Clone, Copy)]
pub struct MysqlSourceFactory;

#[async_trait::async_trait]
impl SourceFactory for MysqlSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(MysqlSource::new(sync_id, spec, streams)?))
    }

    async fn endpoints(
        &self,
        spec: &SourceSpec,
    ) -> Result<Vec<meili_connect_core::Endpoint>, SourceError> {
        crate::endpoints::endpoints(spec)
    }
}

pub struct MysqlSource {
    pub(crate) config: MysqlConfig,
    pub(crate) server_id: u32,
    /// Mapped tables, `(database, table)`, sorted and deduplicated.
    pub(crate) tables: Vec<(String, String)>,
    /// Set by `open`: where `stream` starts.
    pub(crate) position: Option<Position>,
}

impl std::fmt::Debug for MysqlSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSource")
            .field("host", &self.config.host)
            .field("port", &self.config.port)
            .field("server_id", &self.server_id)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl MysqlSource {
    pub fn new(
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Self, SourceError> {
        if spec.kind != SourceKind::Mysql {
            return Err(SourceError::Config(format!(
                "the mysql connector cannot run a `{}` source",
                spec.kind.as_str()
            )));
        }
        let config = MysqlConfig::from_json(&spec.config)?;
        let tables: BTreeSet<(String, String)> = streams
            .iter()
            .map(|s| split_collection(&s.collection))
            .collect::<Result<_, _>>()?;
        if let Some(db) = &config.database
            && let Some((other, t)) = tables.iter().find(|(d, _)| d != db)
        {
            return Err(SourceError::Config(format!(
                "`{other}.{t}` is outside the configured database `{db}`"
            )));
        }
        Ok(Self {
            server_id: config.server_id_for(sync_id),
            config,
            tables: tables.into_iter().collect(),
            position: None,
        })
    }

    pub(crate) async fn connect(&self) -> Result<Conn, SourceError> {
        match tokio::time::timeout(CONNECT_TIMEOUT, Conn::new(self.config.opts())).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(map_err(e)),
            Err(_) => Err(SourceError::Transient(format!(
                "timed out connecting to mysql at {}:{}",
                self.config.host, self.config.port
            ))),
        }
    }

    /// Mapped tables with their current schema. Missing tables and tables
    /// without a primary key are configuration errors.
    pub(crate) async fn mapped_schemas(
        &self,
        conn: &mut Conn,
    ) -> Result<Vec<TableSchema>, SourceError> {
        let mut out = Vec::with_capacity(self.tables.len());
        for (db, table) in &self.tables {
            match load_table(conn, db, table).await? {
                None => {
                    return Err(SourceError::Config(format!(
                        "table `{db}.{table}` does not exist or is not readable"
                    )));
                }
                Some(s) if s.key.is_empty() => {
                    return Err(SourceError::Config(format!(
                        "table `{db}.{table}` has no primary key"
                    )));
                }
                Some(s) => out.push(s),
            }
        }
        Ok(out)
    }
}

/// Server facts that decide how the source runs.
#[derive(Debug, Clone)]
pub(crate) struct ServerInfo {
    pub version: String,
    pub mariadb: bool,
    pub gtid_on: bool,
}

pub(crate) async fn global_var(conn: &mut Conn, name: &str) -> Result<Option<String>, SourceError> {
    // SHOW VARIABLES never fails on an unknown name (MariaDB lacks some),
    // but it cannot be prepared: `name` is always one of our constants.
    if !name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_') {
        return Err(SourceError::Fatal(format!("bad variable name {name}")));
    }
    let row: Option<(String, Option<String>)> = conn
        .query_first(format!("SHOW GLOBAL VARIABLES LIKE '{name}'"))
        .await
        .map_err(map_err)?;
    Ok(row.and_then(|(_, v)| v))
}

pub(crate) async fn server_info(conn: &mut Conn) -> Result<ServerInfo, SourceError> {
    let version: Option<String> = conn
        .query_first("SELECT VERSION()")
        .await
        .map_err(map_err)?;
    let version = version.unwrap_or_default();
    let mariadb = version.to_ascii_lowercase().contains("mariadb");
    let gtid_mode = global_var(conn, "gtid_mode").await?.unwrap_or_default();
    Ok(ServerInfo {
        mariadb,
        gtid_on: !mariadb && gtid_mode.eq_ignore_ascii_case("ON"),
        version,
    })
}

async fn current_file_position(conn: &mut Conn) -> Result<Position, SourceError> {
    // 8.2+ renamed SHOW MASTER STATUS; MariaDB and older MySQL keep it.
    let mut last_err = None;
    for q in ["SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"] {
        match conn.query_first::<Row, _>(q).await {
            Ok(Some(mut row)) => {
                let file: Option<String> = row.take(0);
                let pos: Option<u64> = row.take(1);
                if let (Some(file), Some(pos)) = (file, pos) {
                    return Ok(Position::File { file, pos });
                }
                return Err(SourceError::Config(
                    "the binary log is not enabled (log_bin=OFF)".into(),
                ));
            }
            Ok(None) => {
                return Err(SourceError::Config(
                    "the binary log is not enabled (log_bin=OFF)".into(),
                ));
            }
            Err(e) => last_err = Some(map_err(e)),
        }
    }
    Err(last_err.unwrap_or_else(|| SourceError::Transient("no binlog status".into())))
}

/// `(file, size)` of every binlog the server still has.
async fn binary_logs(conn: &mut Conn) -> Result<Vec<(String, u64)>, SourceError> {
    let rows: Vec<Row> = conn.query("SHOW BINARY LOGS").await.map_err(map_err)?;
    Ok(rows
        .into_iter()
        .filter_map(|mut r| {
            let file: Option<String> = r.take(0);
            let size: Option<u64> = r.take(1);
            file.map(|f| (f, size.unwrap_or(0)))
        })
        .collect())
}

async fn gtid_var(conn: &mut Conn, name: &str) -> Result<GtidSet, SourceError> {
    let raw: Option<String> = conn
        .query_first(format!("SELECT @@GLOBAL.{name}"))
        .await
        .map_err(map_err)?;
    raw.unwrap_or_default()
        .parse()
        .map_err(|e| SourceError::Fatal(format!("@@{name}: {e}")))
}

/// Runs `fut` unless `cancel` fires first (`None`).
pub(crate) async fn or_cancel<T>(
    cancel: &CancellationToken,
    fut: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => None,
        v = fut => Some(v),
    }
}

/// Sends unless cancelled; `false` means stop (cancelled or engine gone).
pub(crate) async fn send(
    events: &EventSender,
    cancel: &CancellationToken,
    ev: SourceEvent,
) -> bool {
    matches!(or_cancel(cancel, events.send(ev)).await, Some(Ok(())))
}

#[async_trait]
impl Source for MysqlSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        crate::stream::check(self).await
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let mut conn = self.connect().await?;
        let tables = load_tables(&mut conn, self.config.database.as_deref()).await?;
        let _ = conn.disconnect().await;
        Ok(tables
            .into_values()
            .map(|(schema, est)| collection_info(&schema, est))
            .collect())
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        self.position = None;
        let mut conn = self.connect().await?;
        let info = server_info(&mut conn).await?;
        let log_bin = global_var(&mut conn, "log_bin").await?.unwrap_or_default();
        if !log_bin.eq_ignore_ascii_case("ON") && log_bin != "1" {
            return Err(SourceError::Config(
                "the binary log is disabled; start the server with log_bin".into(),
            ));
        }
        self.mapped_schemas(&mut conn).await?;

        let opened = match resume {
            None => {
                // Captured before any snapshot read: gapless.
                let start = if info.gtid_on {
                    Position::Gtid(gtid_var(&mut conn, "gtid_executed").await?)
                } else {
                    current_file_position(&mut conn).await?
                };
                let cp = start.to_checkpoint()?;
                self.position = Some(start);
                Opened::Fresh { start: cp }
            }
            Some(cp) => {
                let pos = Position::from_checkpoint(&cp)?;
                match &pos {
                    Position::Gtid(set) => {
                        if !info.gtid_on {
                            return Err(SourceError::PositionLost(
                                "the checkpoint is a GTID set but gtid_mode is no longer ON".into(),
                            ));
                        }
                        let purged = gtid_var(&mut conn, "gtid_purged").await?;
                        if !set.contains_set(&purged) {
                            return Err(SourceError::PositionLost(format!(
                                "the server purged binlogs this sync has not read \
                                 (gtid_purged {purged} is not within the checkpoint)"
                            )));
                        }
                    }
                    Position::File { file, pos: offset } => {
                        let logs = binary_logs(&mut conn).await?;
                        match logs.iter().find(|(f, _)| f == file) {
                            None => {
                                return Err(SourceError::PositionLost(format!(
                                    "binlog file {file} was purged"
                                )));
                            }
                            Some((_, size)) if *offset > *size => {
                                return Err(SourceError::PositionLost(format!(
                                    "offset {offset} is past the end of {file} ({size} bytes)"
                                )));
                            }
                            Some(_) => {}
                        }
                    }
                }
                self.position = Some(pos);
                Opened::Resumed
            }
        };
        let _ = conn.disconnect().await;
        Ok(opened)
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let Some(mut conn) = or_cancel(&cancel, self.connect()).await.transpose()? else {
            return Ok(());
        };
        let schemas = self.mapped_schemas(&mut conn).await?;
        conn.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(map_err)?;
        // Long pages to a slow engine must not trip the server's timeouts.
        conn.query_drop("SET SESSION net_write_timeout = 3600, net_read_timeout = 3600")
            .await
            .map_err(map_err)?;
        conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
            .await
            .map_err(map_err)?;
        for schema in &schemas {
            if !snapshot_table(
                &mut conn,
                schema,
                self.config.snapshot_batch_size,
                &events,
                &cancel,
            )
            .await?
            {
                return Ok(());
            }
        }
        conn.query_drop("COMMIT").await.map_err(map_err)?;
        let _ = conn.disconnect().await;
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        crate::stream::run(self, events, acks, cancel).await
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        // Nothing was created on the origin.
        Ok(())
    }
}

/// Keyset pages over the primary key. Returns `false` when cancelled or the
/// engine went away.
async fn snapshot_table(
    conn: &mut Conn,
    schema: &TableSchema,
    batch: u32,
    events: &EventSender,
    cancel: &CancellationToken,
) -> Result<bool, SourceError> {
    let collection = schema.collection();
    let cols = schema
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ");
    let key_cols: Vec<String> = schema
        .key
        .iter()
        .map(|&i| quote_ident(&schema.columns[i].name))
        .collect();
    let from = format!(
        "{}.{}",
        quote_ident(&schema.database),
        quote_ident(&schema.table)
    );
    let order = key_cols.join(", ");
    let first_sql = format!("SELECT {cols} FROM {from} ORDER BY {order} LIMIT {batch}");
    let after = if key_cols.len() == 1 {
        format!("{} > ?", key_cols[0])
    } else {
        format!("({}) > ({})", order, vec!["?"; key_cols.len()].join(", "))
    };
    let next_sql =
        format!("SELECT {cols} FROM {from} WHERE {after} ORDER BY {order} LIMIT {batch}");

    let mut last_key: Option<Vec<Value>> = None;
    loop {
        let page = match &last_key {
            None => or_cancel(cancel, conn.exec::<Row, _, _>(first_sql.as_str(), ())).await,
            Some(k) => {
                or_cancel(cancel, conn.exec::<Row, _, _>(next_sql.as_str(), k.clone())).await
            }
        };
        let Some(page) = page else {
            return Ok(false);
        };
        let rows = page.map_err(map_err)?;
        let n = rows.len();
        for row in rows {
            let values: Vec<Value> = row
                .unwrap_raw()
                .into_iter()
                .map(|v| v.unwrap_or(Value::NULL))
                .collect();
            if values.len() != schema.columns.len() {
                return Err(SourceError::Transient(format!(
                    "`{collection}` changed shape during the snapshot"
                )));
            }
            last_key = Some(schema.key.iter().map(|&i| values[i].clone()).collect());
            let mut doc = Document::new();
            for (col, v) in schema.columns.iter().zip(values) {
                let json = value_to_json(col, v, Origin::Snapshot)
                    .map_err(|e| SourceError::Fatal(format!("`{collection}`: {e}")))?;
                doc.insert(col.name.clone(), json);
            }
            let key = schema
                .key
                .iter()
                .map(|&i| {
                    doc.get(&schema.columns[i].name)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect();
            let change = Change {
                collection: collection.clone(),
                key,
                op: Op::Upsert { doc },
            };
            if !send(events, cancel, SourceEvent::Change(change)).await {
                return Ok(false);
            }
        }
        if n < batch as usize {
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(config: serde_json::Value) -> SourceSpec {
        SourceSpec {
            kind: SourceKind::Mysql,
            config,
        }
    }

    fn mapping(collection: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": collection, "index": "i"})).unwrap()
    }

    #[test]
    fn factory_builds_with_mapped_tables() {
        let src = MysqlSource::new(
            "sync-1",
            &spec(json!({"host": "h", "user": "u", "password": "secret-pw"})),
            &[
                mapping("shop.items"),
                mapping("shop.users"),
                mapping("shop.items"),
            ],
        )
        .unwrap();
        assert_eq!(
            src.tables,
            vec![
                ("shop".to_owned(), "items".to_owned()),
                ("shop".to_owned(), "users".to_owned())
            ]
        );
        assert_eq!(src.server_id, crate::derive_server_id("sync-1"));
        assert!(!format!("{src:?}").contains("secret-pw"));
        assert!(src.capabilities().snapshot);
        assert!(!src.capabilities().patches);
        assert!(
            MysqlSourceFactory
                .build(
                    "s",
                    &spec(json!({"host": "h", "user": "u"})),
                    &[mapping("a.b")]
                )
                .is_ok()
        );
    }

    #[test]
    fn factory_rejects_bad_mappings() {
        let bad_name = MysqlSource::new(
            "s",
            &spec(json!({"host": "h", "user": "u"})),
            &[mapping("items")],
        );
        assert!(matches!(bad_name, Err(SourceError::Config(_))));
        let outside = MysqlSource::new(
            "s",
            &spec(json!({"host": "h", "user": "u", "database": "shop"})),
            &[mapping("other.items")],
        );
        assert!(matches!(outside, Err(SourceError::Config(_))));
        let wrong_kind = MysqlSource::new(
            "s",
            &SourceSpec {
                kind: SourceKind::Postgres,
                config: json!({"host": "h", "user": "u"}),
            },
            &[mapping("a.b")],
        );
        assert!(matches!(wrong_kind, Err(SourceError::Config(_))));
    }
}
