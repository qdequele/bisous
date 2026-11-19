//! Pre-flight checks and the binlog loop.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures::StreamExt;
use meili_connect_core::{
    AckReceiver, CheckReport, CheckStatus, EventSender, SourceError, SourceEvent,
};
use mysql_async::binlog::EventType;
use mysql_async::binlog::events::{EventData, RowsEventData, TableMapEvent};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Sid};
use tokio_util::sync::CancellationToken;

use crate::ddl::{DdlKind, QueryKind, classify};
use crate::error::map_err;
use crate::position::Position;
use crate::rows::{BinlogTable, RowKind};
use crate::schema::{TableSchema, load_table};
use crate::source::{MysqlSource, global_var, or_cancel, send, server_info};

/// Heartbeat period asked of the server, and the idle checkpoint cadence.
const HEARTBEAT: Duration = Duration::from_secs(5);
/// A transaction without mapped changes moves the checkpoint at most this
/// often, so busy unrelated tables do not flood the engine.
const QUIET_CHECKPOINT_EVERY: Duration = Duration::from_secs(1);
/// Below this retention a sync paused over a weekend loses its position.
const SHORT_RETENTION_SECS: u64 = 3 * 24 * 3600;

const HEARTBEAT_LOG_EVENT_V2: u8 = 0x29;

pub(crate) async fn check(src: &mut MysqlSource) -> Result<CheckReport, SourceError> {
    let mut report = CheckReport::default();
    let mut conn = match src.connect().await {
        Ok(c) => c,
        Err(SourceError::Config(msg)) => {
            report.push("connection", CheckStatus::Failed, msg);
            return Ok(report);
        }
        Err(e) => return Err(e),
    };
    report.push(
        "connection",
        CheckStatus::Ok,
        format!("connected to {}:{}", src.config.host, src.config.port),
    );

    let info = server_info(&mut conn).await?;
    if info.mariadb {
        report.push(
            "version",
            CheckStatus::Warning,
            format!(
                "{}: MariaDB is supported on a best-effort basis (file/offset positions only)",
                info.version
            ),
        );
    } else if version_at_least(&info.version, 8) {
        report.push("version", CheckStatus::Ok, info.version.clone());
    } else {
        report.push(
            "version",
            CheckStatus::Warning,
            format!("{}: MySQL 8.0 or later is recommended", info.version),
        );
    }

    let var = |v: Option<String>| v.unwrap_or_default();
    let log_bin = var(global_var(&mut conn, "log_bin").await?);
    if log_bin.eq_ignore_ascii_case("ON") || log_bin == "1" {
        report.push("log_bin", CheckStatus::Ok, "the binary log is enabled");
    } else {
        report.push(
            "log_bin",
            CheckStatus::Failed,
            "the binary log is disabled; start the server with log_bin",
        );
    }

    let format = var(global_var(&mut conn, "binlog_format").await?);
    push_expect(&mut report, "binlog_format", &format, "ROW");
    let image = var(global_var(&mut conn, "binlog_row_image").await?);
    push_expect(&mut report, "binlog_row_image", &image, "FULL");

    match global_var(&mut conn, "binlog_row_metadata").await? {
        Some(m) if m.eq_ignore_ascii_case("FULL") => {
            report.push("binlog_row_metadata", CheckStatus::Ok, "FULL")
        }
        Some(m) => report.push(
            "binlog_row_metadata",
            CheckStatus::Warning,
            format!(
                "is '{m}': column names are read from information_schema; set it to FULL so \
                 events written before a schema change decode by name"
            ),
        ),
        None => {}
    }
    if let Some(opts) = global_var(&mut conn, "binlog_row_value_options").await?
        && opts.to_ascii_uppercase().contains("PARTIAL_JSON")
    {
        report.push(
            "binlog_row_value_options",
            CheckStatus::Failed,
            "PARTIAL_JSON logs JSON updates as diffs; set binlog_row_value_options=''",
        );
    }

    if info.gtid_on {
        report.push("gtid_mode", CheckStatus::Ok, "ON");
    } else if info.mariadb {
        report.push(
            "gtid_mode",
            CheckStatus::Warning,
            "MariaDB GTIDs are not used; positions are binlog file + offset",
        );
    } else {
        report.push(
            "gtid_mode",
            CheckStatus::Warning,
            "gtid_mode is not ON: positions fall back to binlog file + offset, which do not \
             survive a failover to another server",
        );
    }

    check_retention(&mut conn, &mut report).await?;
    check_privileges(&mut conn, &mut report).await?;

    let own_id: Option<String> = global_var(&mut conn, "server_id").await?;
    if own_id.as_deref() == Some(&src.server_id.to_string()) {
        report.push(
            "server_id",
            CheckStatus::Failed,
            format!(
                "replica server_id {} equals the server's own; set server_id in the config",
                src.server_id
            ),
        );
    } else {
        report.push(
            "server_id",
            CheckStatus::Ok,
            format!("replica server_id {}", src.server_id),
        );
    }

    for (db, table) in src.tables.clone() {
        let name = format!("{db}.{table}");
        match load_table(&mut conn, &db, &table).await? {
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
                let probe = format!(
                    "SELECT 1 FROM {}.{} LIMIT 0",
                    crate::schema::quote_ident(&db),
                    crate::schema::quote_ident(&table)
                );
                match conn.query_drop(probe).await.map_err(map_err) {
                    Ok(()) => report.push(&name, CheckStatus::Ok, "readable, has a primary key"),
                    Err(SourceError::Config(m)) => {
                        report.push(&name, CheckStatus::Failed, format!("no SELECT: {m}"))
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }
    let _ = conn.disconnect().await;
    Ok(report)
}

fn version_at_least(version: &str, major: u32) -> bool {
    version
        .split('.')
        .next()
        .and_then(|m| m.parse::<u32>().ok())
        .is_some_and(|m| m >= major)
}

fn push_expect(report: &mut CheckReport, name: &str, actual: &str, want: &str) {
    if actual.eq_ignore_ascii_case(want) {
        report.push(name, CheckStatus::Ok, want);
    } else {
        report.push(
            name,
            CheckStatus::Failed,
            format!("is '{actual}', set it to {want}"),
        );
    }
}

async fn check_retention(conn: &mut Conn, report: &mut CheckReport) -> Result<(), SourceError> {
    let secs = global_var(conn, "binlog_expire_logs_seconds")
        .await?
        .and_then(|s| s.parse::<u64>().ok());
    let days = global_var(conn, "expire_logs_days")
        .await?
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|d| *d > 0.0);
    let effective = match (secs, days) {
        (Some(s), _) if s > 0 => Some(s),
        (_, Some(d)) => Some((d * 86_400.0) as u64),
        _ => None,
    };
    match effective {
        None => report.push(
            "binlog_retention",
            CheckStatus::Ok,
            "binlogs are never expired automatically",
        ),
        Some(s) if s < SHORT_RETENTION_SECS => report.push(
            "binlog_retention",
            CheckStatus::Warning,
            format!(
                "binlogs expire after {:.1} h: a sync stopped longer than that must resync; \
                 consider binlog_expire_logs_seconds >= {SHORT_RETENTION_SECS}",
                s as f64 / 3600.0
            ),
        ),
        Some(s) => report.push(
            "binlog_retention",
            CheckStatus::Ok,
            format!("binlogs kept {:.1} days", s as f64 / 86_400.0),
        ),
    }
    Ok(())
}

async fn check_privileges(conn: &mut Conn, report: &mut CheckReport) -> Result<(), SourceError> {
    let grants: Vec<String> = conn.query("SHOW GRANTS").await.map_err(map_err)?;
    let global: Vec<String> = grants
        .iter()
        .map(|g| g.to_ascii_uppercase())
        .filter(|g| g.contains(" ON *.* "))
        .collect();
    let has = |p: &str| {
        global
            .iter()
            .any(|g| g.contains("ALL PRIVILEGES") || g.contains(p))
    };
    let uses_roles = grants
        .iter()
        .any(|g| !g.to_ascii_uppercase().contains(" ON "));
    for (privilege, alt) in [
        ("REPLICATION SLAVE", "REPLICATION REPLICA"),
        ("REPLICATION CLIENT", "BINLOG MONITOR"),
    ] {
        if has(privilege) || has(alt) {
            report.push(privilege, CheckStatus::Ok, "granted");
        } else if uses_roles {
            report.push(
                privilege,
                CheckStatus::Warning,
                "not granted directly; make sure a default role grants it",
            );
        } else {
            report.push(
                privilege,
                CheckStatus::Failed,
                format!("missing: GRANT {privilege} ON *.* TO the sync user"),
            );
        }
    }
    Ok(())
}

/// Mutable state of one `stream` call.
struct StreamState {
    position: Position,
    /// Last position handed to the engine.
    emitted: Position,
    last_emit: Instant,
    pending_gtid: Option<(String, Option<String>, u64)>,
    in_txn: bool,
    txn_had_changes: bool,
    file: String,
    offset: u64,
    mapped: HashSet<(String, String)>,
    schemas: HashMap<(String, String), TableSchema>,
    /// Binlog table id → aligned mapped table (`None`: not mapped).
    by_id: HashMap<u64, Option<BinlogTable>>,
}

impl StreamState {
    fn commit(&mut self) {
        if let Some((uuid, tag, gno)) = self.pending_gtid.take()
            && let Position::Gtid(set) = &mut self.position
        {
            set.add(&uuid, tag.as_deref(), gno);
        }
        if let Position::File { file, pos } = &mut self.position
            && !self.file.is_empty()
        {
            file.clone_from(&self.file);
            *pos = self.offset;
        }
        self.in_txn = false;
    }

    /// The checkpoint to emit now, if any.
    fn checkpoint_due(&mut self, force: bool) -> Option<Position> {
        if self.in_txn || self.position == self.emitted {
            return None;
        }
        if force || self.last_emit.elapsed() >= QUIET_CHECKPOINT_EVERY {
            self.emitted = self.position.clone();
            self.last_emit = Instant::now();
            Some(self.position.clone())
        } else {
            None
        }
    }
}

pub(crate) async fn run(
    src: &mut MysqlSource,
    events: EventSender,
    acks: AckReceiver,
    cancel: CancellationToken,
) -> Result<(), SourceError> {
    // MySQL keeps nothing per consumer: acknowledgements need no action.
    // The receiver is kept alive so the engine's sends never fail.
    let _acks = acks;
    let Some(start) = src.position.clone() else {
        return Err(SourceError::Fatal("stream called before open".into()));
    };

    let Some(meta) = or_cancel(&cancel, src.connect()).await else {
        return Ok(());
    };
    let mut meta = meta?;
    let mut schemas = HashMap::new();
    for s in src.mapped_schemas(&mut meta).await? {
        schemas.insert((s.database.clone(), s.table.clone()), s);
    }

    let Some(conn) = or_cancel(&cancel, src.connect()).await else {
        return Ok(());
    };
    let mut conn = conn?;
    // Heartbeats keep idle checkpoints flowing and detect dead links.
    let nanos = HEARTBEAT.as_nanos().to_string();
    conn.query_drop(format!(
        "SET @master_heartbeat_period = {nanos}, @source_heartbeat_period = {nanos}"
    ))
    .await
    .map_err(map_err)?;

    let sid_strings = match &start {
        Position::Gtid(set) => set.sid_strings(),
        Position::File { .. } => Vec::new(),
    };
    let sids = sid_strings
        .iter()
        .map(|s| s.parse::<Sid<'_>>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SourceError::Fatal(format!("gtid set: {e}")))?;
    let (file_bytes, offset) = match &start {
        Position::File { file, pos } => (file.as_bytes().to_vec(), *pos),
        Position::Gtid(_) => (Vec::new(), 4),
    };
    let mut request = BinlogStreamRequest::new(src.server_id);
    request = match &start {
        Position::Gtid(_) => request.with_gtid().with_gtid_set(sids),
        Position::File { .. } => request.with_filename(&file_bytes).with_pos(offset),
    };
    let Some(stream) = or_cancel(&cancel, conn.get_binlog_stream(request)).await else {
        return Ok(());
    };
    let mut stream = stream.map_err(map_err)?;
    tracing::info!(
        server_id = src.server_id,
        gtid = start.is_gtid(),
        "mysql binlog stream started"
    );

    let mut st = StreamState {
        emitted: start.clone(),
        position: start,
        last_emit: Instant::now(),
        pending_gtid: None,
        in_txn: false,
        txn_had_changes: false,
        file: String::new(),
        offset: 0,
        mapped: src.tables.iter().cloned().collect(),
        schemas,
        by_id: HashMap::new(),
    };
    let mut idle = tokio::time::interval(HEARTBEAT);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = idle.tick() => {
                if let Some(p) = st.checkpoint_due(true)
                    && !send(&events, &cancel, SourceEvent::Checkpoint(p.to_checkpoint()?)).await
                {
                    return Ok(());
                }
                continue;
            }
            next = stream.next() => next,
        };
        let event = match next {
            None => {
                return Err(SourceError::Transient(
                    "the server closed the binlog stream".into(),
                ));
            }
            Some(Err(e)) => return Err(map_err(e)),
            Some(Ok(ev)) => ev,
        };
        let header = event.header();
        if header.log_pos() != 0 {
            st.offset = u64::from(header.log_pos());
        }
        let raw_type = header.event_type_raw();
        if raw_type == EventType::HEARTBEAT_EVENT as u8 || raw_type == HEARTBEAT_LOG_EVENT_V2 {
            if let Some(p) = st.checkpoint_due(true)
                && !send(
                    &events,
                    &cancel,
                    SourceEvent::Checkpoint(p.to_checkpoint()?),
                )
                .await
            {
                return Ok(());
            }
            continue;
        }
        let is_rows_or_map = matches!(
            header.event_type(),
            Ok(EventType::TABLE_MAP_EVENT
                | EventType::WRITE_ROWS_EVENT
                | EventType::UPDATE_ROWS_EVENT
                | EventType::DELETE_ROWS_EVENT
                | EventType::WRITE_ROWS_EVENT_V1
                | EventType::UPDATE_ROWS_EVENT_V1
                | EventType::DELETE_ROWS_EVENT_V1
                | EventType::PARTIAL_UPDATE_ROWS_EVENT)
        );
        let data = match event.read_data() {
            Ok(Some(d)) => d,
            Ok(None) => continue,
            Err(e) if is_rows_or_map => {
                return Err(SourceError::Fatal(format!("undecodable binlog event: {e}")));
            }
            Err(e) => {
                tracing::debug!(event_type = raw_type, error = %e, "skipping undecodable binlog event");
                continue;
            }
        };

        let mut out: Vec<SourceEvent> = Vec::new();
        let mut committed = false;
        match data {
            EventData::RotateEvent(r) => {
                st.file = r.name().into_owned();
                st.offset = r.position();
                if !st.in_txn
                    && let Position::File { .. } = st.position
                {
                    st.commit();
                }
            }
            EventData::GtidEvent(g) => {
                let uuid = uuid::Uuid::from_bytes(g.sid()).hyphenated().to_string();
                let tag = g.tag().map(|t| t.as_str().to_owned());
                st.pending_gtid = Some((uuid, tag, g.gno()));
                st.txn_had_changes = false;
            }
            EventData::QueryEvent(q) => {
                let schema = q.schema().into_owned();
                match classify(&q.query(), &schema) {
                    QueryKind::Begin => {
                        st.in_txn = true;
                        st.txn_had_changes = false;
                    }
                    QueryKind::Commit => committed = true,
                    QueryKind::Ddl(ddl) => {
                        for (db, table) in ddl.tables {
                            let key = (db.clone(), table.clone());
                            if !st.mapped.contains(&key) {
                                continue;
                            }
                            let collection = format!("{db}.{table}");
                            st.by_id.retain(|_, t| {
                                t.as_ref().is_none_or(|t| t.collection != collection)
                            });
                            match ddl.kind {
                                DdlKind::Truncate | DdlKind::Drop => {
                                    out.push(SourceEvent::Truncate {
                                        collection: collection.clone(),
                                    });
                                }
                                DdlKind::Alter => {}
                            }
                            match load_table(&mut meta, &db, &table).await? {
                                Some(s) => {
                                    st.schemas.insert(key, s);
                                }
                                None => {
                                    st.schemas.remove(&key);
                                }
                            }
                            if ddl.kind == DdlKind::Alter {
                                out.push(SourceEvent::SchemaChanged { collection });
                            }
                            st.txn_had_changes = true;
                        }
                        if !st.in_txn {
                            committed = true;
                        }
                    }
                    QueryKind::Other => {
                        if !st.in_txn {
                            committed = true;
                        }
                    }
                }
            }
            EventData::XidEvent(_) => committed = true,
            EventData::TableMapEvent(tme) => {
                let key = (
                    tme.database_name().into_owned(),
                    tme.table_name().into_owned(),
                );
                if !st.mapped.contains(&key) {
                    st.by_id.insert(tme.table_id(), None);
                } else {
                    let count = tme.columns_count() as usize;
                    let stale = st
                        .schemas
                        .get(&key)
                        .is_none_or(|s| s.columns.len() != count);
                    if stale {
                        if let Some(s) = load_table(&mut meta, &key.0, &key.1).await? {
                            st.schemas.insert(key.clone(), s);
                        }
                        out.push(SourceEvent::SchemaChanged {
                            collection: format!("{}.{}", key.0, key.1),
                        });
                    }
                    let Some(schema) = st.schemas.get(&key) else {
                        return Err(SourceError::Config(format!(
                            "mapped table `{}.{}` no longer exists",
                            key.0, key.1
                        )));
                    };
                    let aligned = align(schema, &tme)?;
                    st.by_id.insert(tme.table_id(), Some(aligned));
                }
            }
            EventData::RowsEvent(rows) => {
                let table_id = rows.table_id();
                if let Some(Some(table)) = st.by_id.get(&table_id) {
                    let Some(tme) = stream.get_tme(table_id) else {
                        return Err(SourceError::Fatal(format!(
                            "rows event for unknown table id {table_id}"
                        )));
                    };
                    decode_rows(table, &rows, tme, &mut out)?;
                    st.txn_had_changes |= !out.is_empty();
                }
            }
            _ => {}
        }

        for ev in out {
            if !send(&events, &cancel, ev).await {
                return Ok(());
            }
        }
        if committed {
            st.commit();
            let force = std::mem::take(&mut st.txn_had_changes);
            if let Some(p) = st.checkpoint_due(force)
                && !send(
                    &events,
                    &cancel,
                    SourceEvent::Checkpoint(p.to_checkpoint()?),
                )
                .await
            {
                return Ok(());
            }
        }
    }
}

fn align(schema: &TableSchema, tme: &TableMapEvent<'_>) -> Result<BinlogTable, SourceError> {
    let count = tme.columns_count() as usize;
    let mut types = Vec::with_capacity(count);
    for i in 0..count {
        match tme.get_column_type(i) {
            Ok(Some(t)) => types.push(t),
            _ => {
                return Err(SourceError::Fatal(format!(
                    "unknown column type at position {i} of `{}`",
                    schema.collection()
                )));
            }
        }
    }
    let names = mysql_async::binlog::events::OptionalMetaExtractor::new(tme.iter_optional_meta())
        .ok()
        .and_then(|m| {
            m.iter_column_name()
                .map(|n| n.map(|n| n.name().into_owned()))
                .collect::<Result<Vec<_>, _>>()
                .ok()
        })
        .filter(|n| !n.is_empty());
    BinlogTable::align(schema, &types, names)
}

fn decode_rows(
    table: &BinlogTable,
    rows: &RowsEventData<'_>,
    tme: &TableMapEvent<'_>,
    out: &mut Vec<SourceEvent>,
) -> Result<(), SourceError> {
    let kind = match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => RowKind::Insert,
        RowsEventData::UpdateRowsEventV1(_) | RowsEventData::UpdateRowsEvent(_) => RowKind::Update,
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => RowKind::Delete,
        RowsEventData::PartialUpdateRowsEvent(_) => {
            return Err(SourceError::Config(format!(
                "`{}` is logged with partial JSON updates; set binlog_row_value_options=''",
                table.collection
            )));
        }
    };
    for row in rows.rows(tme) {
        let (before, after) = row.map_err(|e| {
            SourceError::Fatal(format!("undecodable row of `{}`: {e}", table.collection))
        })?;
        let before = before.map(|r| table.row_to_doc(r.unwrap())).transpose()?;
        let after = after.map(|r| table.row_to_doc(r.unwrap())).transpose()?;
        for change in table.changes(kind, before, after)? {
            out.push(SourceEvent::Change(change));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gtid::GtidSet;

    #[test]
    fn versions() {
        assert!(version_at_least("8.4.3", 8));
        assert!(version_at_least("9.1.0", 8));
        assert!(!version_at_least("5.7.44-log", 8));
        assert!(!version_at_least("", 8));
    }

    fn state(position: Position) -> StreamState {
        StreamState {
            emitted: position.clone(),
            position,
            last_emit: Instant::now(),
            pending_gtid: None,
            in_txn: false,
            txn_had_changes: false,
            file: String::new(),
            offset: 0,
            mapped: HashSet::new(),
            schemas: HashMap::new(),
            by_id: HashMap::new(),
        }
    }

    #[test]
    fn gtid_commit_accumulates_and_checkpoints() {
        let uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        let start: GtidSet = format!("{uuid}:1-5").parse().unwrap();
        let mut st = state(Position::Gtid(start));
        assert_eq!(st.checkpoint_due(true), None, "nothing moved");

        st.pending_gtid = Some((uuid.into(), None, 6));
        st.in_txn = true;
        assert_eq!(st.checkpoint_due(true), None, "inside a transaction");
        st.commit();
        let Some(Position::Gtid(set)) = st.checkpoint_due(true) else {
            panic!("expected a gtid checkpoint");
        };
        assert_eq!(set.to_string(), format!("{uuid}:1-6"));

        // Unforced checkpoints are throttled.
        st.pending_gtid = Some((uuid.into(), None, 7));
        st.commit();
        assert_eq!(st.checkpoint_due(false), None);
        assert!(st.checkpoint_due(true).is_some());
    }

    #[test]
    fn file_commit_uses_the_current_offset() {
        let mut st = state(Position::File {
            file: "binlog.000001".into(),
            pos: 4,
        });
        st.file = "binlog.000002".into();
        st.offset = 900;
        st.commit();
        assert_eq!(
            st.checkpoint_due(true),
            Some(Position::File {
                file: "binlog.000002".into(),
                pos: 900
            })
        );
    }
}
