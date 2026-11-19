//! Pre-flight checks and the binlog loop.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use futures::StreamExt;
use meili_connect_core::{
    AckReceiver, CheckReport, CheckStatus, EventSender, Lag, LagReporter, SourceError, SourceEvent,
    seconds_since_unix_millis,
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
use crate::source::{MysqlSource, current_file_position, global_var, or_cancel, send, server_info};

/// Heartbeat period asked of the server, and the idle checkpoint cadence.
const HEARTBEAT: Duration = Duration::from_secs(5);
/// A transaction without mapped changes moves the checkpoint at most this
/// often, so busy unrelated tables do not flood the engine.
const QUIET_CHECKPOINT_EVERY: Duration = Duration::from_secs(1);
/// How often the server's binlog position is read to measure `Lag::bytes`.
const LAG_BYTES_EVERY: Duration = Duration::from_secs(10);
/// Below this retention a sync paused over a weekend loses its position.
const SHORT_RETENTION_SECS: u64 = 3 * 24 * 3600;

const HEARTBEAT_LOG_EVENT_V2: u8 = 0x29;
/// MariaDB's GTID event: starts every MariaDB binlog group.
const MARIADB_GTID_EVENT: u8 = 0xa2;

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

    if src.tables.is_empty() {
        report.push_none_selected("tables", "table");
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

/// What the stream knows of its lag. `seconds` comes from each event's
/// header timestamp (no query); `bytes` from a periodic
/// `SHOW BINARY LOG STATUS` compared with the read position, only when both
/// are in the same binlog file.
struct StreamLag {
    reporter: Option<LagReporter>,
    seconds: Option<f64>,
    bytes: Option<u64>,
}

impl StreamLag {
    /// A binlog event written at `timestamp` (Unix seconds; 0 for events
    /// the server synthesizes, which say nothing about lag).
    fn event_at(&mut self, timestamp: u32) {
        if timestamp != 0 {
            self.seconds = Some(seconds_since_unix_millis(i64::from(timestamp) * 1000));
        }
    }

    /// A heartbeat: the server has nothing more to send.
    fn caught_up(&mut self) {
        self.seconds = Some(0.0);
        self.bytes = Some(0);
    }

    /// The server's current binlog position against ours.
    fn server_at(&mut self, server: &Position, file: &str, offset: u64) {
        self.bytes = match server {
            Position::File { file: f, pos } if f == file => Some(pos.saturating_sub(offset)),
            _ => None,
        };
    }

    fn publish(&self) {
        if let Some(r) = &self.reporter {
            r.report(Lag {
                seconds: self.seconds,
                bytes: self.bytes,
                records: None,
            });
        }
    }
}

/// Mutable state of one `stream` call.
///
/// A binlog *group* is everything logged under one GTID (or anonymous
/// GTID). A position enters the checkpoint only once its group has been
/// completely emitted:
///
/// - at `XID` or `Query(COMMIT)` for a `BEGIN … COMMIT` transaction;
/// - at `XA_PREPARE_LOG_EVENT` for an `XA START … XA END` group (see
///   below);
/// - for a group made of queries outside any transaction (DDL, `GRANT`,
///   `XA COMMIT 'x'`…), when something proves the group ended: the next
///   GTID or anonymous GTID, a rotate, or a server heartbeat (sent only
///   when the server has nothing left to send). A single query is not
///   proof: atomic `CREATE TABLE … SELECT` can log its rows after the DDL
///   under the same GTID.
///
/// XA transactions: the rows are logged at `XA PREPARE`, but the outcome
/// (`XA COMMIT` or `XA ROLLBACK`) comes later, in another group, possibly
/// never. Applying the rows at prepare would index data that may be rolled
/// back; holding them until the outcome would buffer without bound. So an
/// XA transaction that touches a mapped table fails the stream with
/// `SourceError::Config`; one that touches only unmapped tables is skipped
/// and its GTID checkpointed at its prepare. The later `XA COMMIT` /
/// `XA ROLLBACK` is a lone-query group like any other.
struct StreamState {
    position: Position,
    /// Last position handed to the engine.
    emitted: Position,
    last_emit: Instant,
    pending_gtid: Option<(String, Option<String>, u64)>,
    /// Inside `BEGIN …` or `XA START …`.
    in_txn: bool,
    /// Inside `XA START … XA_PREPARE`.
    in_xa: bool,
    /// The current group logged a query outside a transaction; it ends at
    /// the next proof of a group boundary.
    lone: bool,
    txn_had_changes: bool,
    file: String,
    offset: u64,
    mapped: HashSet<(String, String)>,
    schemas: HashMap<(String, String), TableSchema>,
    /// Binlog table id → aligned mapped table (`None`: not mapped).
    by_id: HashMap<u64, Option<BinlogTable>>,
}

/// "A group was committed: emit a checkpoint (forced when the group
/// carried changes)."
type Closed = Option<bool>;

impl StreamState {
    fn new(start: Position, mapped: HashSet<(String, String)>) -> Self {
        Self {
            emitted: start.clone(),
            position: start,
            last_emit: Instant::now(),
            pending_gtid: None,
            in_txn: false,
            in_xa: false,
            lone: false,
            txn_had_changes: false,
            file: String::new(),
            offset: 0,
            mapped,
            schemas: HashMap::new(),
            by_id: HashMap::new(),
        }
    }

    /// Adds the current group to the position; `offset` is the end of its
    /// last event (file positions).
    fn commit(&mut self, offset: u64) -> Closed {
        if let Some((uuid, tag, gno)) = self.pending_gtid.take()
            && let Position::Gtid(set) = &mut self.position
        {
            set.add(&uuid, tag.as_deref(), gno);
        }
        if let Position::File { file, pos } = &mut self.position
            && !self.file.is_empty()
        {
            file.clone_from(&self.file);
            *pos = offset;
        }
        self.in_txn = false;
        self.in_xa = false;
        self.lone = false;
        Some(std::mem::take(&mut self.txn_had_changes))
    }

    /// The previous group ended (`prev_offset`: the end of its last event).
    fn close_lone(&mut self, prev_offset: u64) -> Closed {
        if self.lone && !self.in_txn {
            self.commit(prev_offset)
        } else {
            None
        }
    }

    /// GTID, anonymous GTID or MariaDB GTID: a new group starts.
    fn group_start(
        &mut self,
        gtid: Option<(String, Option<String>, u64)>,
        prev_offset: u64,
    ) -> Closed {
        let closed = self.close_lone(prev_offset);
        if self.in_txn {
            // The previous group never ended. Leaving its GTID out of the
            // checkpoint is safe: a resume replays it.
            tracing::warn!("binlog group without an end; its position is not checkpointed");
            self.in_txn = false;
            self.in_xa = false;
        }
        self.pending_gtid = gtid;
        self.lone = false;
        self.txn_had_changes = false;
        closed
    }

    fn query(&mut self, kind: &QueryKind) -> Closed {
        match kind {
            QueryKind::Begin => {
                self.in_txn = true;
                None
            }
            QueryKind::XaStart => {
                self.in_txn = true;
                self.in_xa = true;
                None
            }
            QueryKind::Commit => self.commit(self.offset),
            QueryKind::Ddl(_) | QueryKind::Other => {
                if !self.in_txn {
                    self.lone = true;
                }
                None
            }
        }
    }

    fn xid(&mut self) -> Closed {
        self.commit(self.offset)
    }

    /// `XA_PREPARE_LOG_EVENT` (two-phase prepare or one-phase commit) ends
    /// the `XA START` group. It only touched unmapped tables (a mapped one
    /// fails the stream at its table map), so nothing is lost either way.
    fn xa_prepare(&mut self) -> Closed {
        self.commit(self.offset)
    }

    fn rotate(&mut self, file: String, pos: u64, prev_offset: u64) -> Closed {
        let closed = self.close_lone(prev_offset);
        self.file = file;
        self.offset = pos;
        if !self.in_txn
            && let Position::File { .. } = self.position
        {
            self.commit(pos);
        }
        closed
    }

    fn heartbeat(&mut self, prev_offset: u64) -> Closed {
        self.close_lone(prev_offset)
    }

    /// Refuses a mapped table inside an XA transaction (see the type docs).
    fn check_xa(&self, key: &(String, String)) -> Result<(), SourceError> {
        if self.in_xa && self.mapped.contains(key) {
            return Err(SourceError::Config(format!(
                "XA transactions on mapped tables are not supported: an XA transaction \
                 wrote to `{}.{}`",
                key.0, key.1
            )));
        }
        Ok(())
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

    let mut st = StreamState::new(start, src.tables.iter().cloned().collect());
    st.schemas = schemas;
    let mut idle = tokio::time::interval(HEARTBEAT);
    idle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut lag = StreamLag {
        reporter: src.lag.clone(),
        seconds: None,
        bytes: None,
    };
    let mut last_bytes_probe: Option<Instant> = None;

    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = idle.tick() => {
                if lag.reporter.is_some()
                    && last_bytes_probe.is_none_or(|t| t.elapsed() >= LAG_BYTES_EVERY)
                {
                    last_bytes_probe = Some(Instant::now());
                    match or_cancel(&cancel, current_file_position(&mut meta)).await {
                        None => return Ok(()),
                        Some(Ok(server)) => lag.server_at(&server, &st.file, st.offset),
                        Some(Err(e)) => {
                            tracing::debug!(error = %e, "binlog position for lag unavailable");
                            lag.bytes = None;
                        }
                    }
                    lag.publish();
                }
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
        // End of the previous event: where a group closed by this event
        // ended.
        let prev_offset = st.offset;
        if header.log_pos() != 0 {
            st.offset = u64::from(header.log_pos());
        }
        let raw_type = header.event_type_raw();
        let is_heartbeat =
            raw_type == EventType::HEARTBEAT_EVENT as u8 || raw_type == HEARTBEAT_LOG_EVENT_V2;
        if is_heartbeat || raw_type == MARIADB_GTID_EVENT {
            let force = if is_heartbeat {
                // The server is idle: whatever was pending ended.
                st.heartbeat(prev_offset);
                lag.caught_up();
                lag.publish();
                Some(true)
            } else {
                st.group_start(None, prev_offset)
            };
            if let Some(force) = force
                && let Some(p) = st.checkpoint_due(force)
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

        lag.event_at(header.timestamp());
        let mut out: Vec<SourceEvent> = Vec::new();
        let mut closed: Closed = None;
        match data {
            EventData::RotateEvent(r) => {
                closed = st.rotate(r.name().into_owned(), r.position(), prev_offset);
            }
            EventData::GtidEvent(g) => {
                let uuid = uuid::Uuid::from_bytes(g.sid()).hyphenated().to_string();
                let tag = g.tag().map(|t| t.as_str().to_owned());
                closed = st.group_start(Some((uuid, tag, g.gno())), prev_offset);
            }
            EventData::AnonymousGtidEvent(_) => {
                closed = st.group_start(None, prev_offset);
            }
            EventData::XaPrepareLogEvent(_) => closed = st.xa_prepare(),
            EventData::QueryEvent(q) => {
                let schema = q.schema().into_owned();
                let kind = classify(&q.query(), &schema);
                if let QueryKind::Ddl(ddl) = &kind {
                    for (db, table) in &ddl.tables {
                        let key = (db.clone(), table.clone());
                        if !st.mapped.contains(&key) {
                            continue;
                        }
                        let collection = format!("{db}.{table}");
                        st.by_id
                            .retain(|_, t| t.as_ref().is_none_or(|t| t.collection != collection));
                        match ddl.kind {
                            DdlKind::Truncate | DdlKind::Drop => {
                                out.push(SourceEvent::Truncate {
                                    collection: collection.clone(),
                                });
                            }
                            DdlKind::Alter => {}
                        }
                        match load_table(&mut meta, db, table).await? {
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
                }
                closed = st.query(&kind);
            }
            EventData::XidEvent(_) => closed = st.xid(),
            EventData::TableMapEvent(tme) => {
                let key = (
                    tme.database_name().into_owned(),
                    tme.table_name().into_owned(),
                );
                st.check_xa(&key)?;
                if !st.mapped.contains(&key) {
                    st.by_id.insert(tme.table_id(), None);
                } else {
                    let count = tme.columns_count() as usize;
                    let stale = st
                        .schemas
                        .get(&key)
                        .is_none_or(|s| s.columns.len() != count);
                    if stale {
                        let Some(loaded) = or_cancel(&cancel, load_settled(&mut meta, &key)).await
                        else {
                            return Ok(());
                        };
                        if let Some(s) = loaded? {
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
        if closed.is_some() {
            lag.publish();
        }
        if let Some(force) = closed
            && let Some(p) = st.checkpoint_due(force)
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

/// A table's schema as of a table map with `count` columns. The binlog
/// event can reach us before the DDL that created the table is visible in
/// `information_schema` (binlog flush precedes the data dictionary commit),
/// so a missing or keyless table is re-read for a short, bounded while
/// before the current answer is taken. (A column count that differs is not
/// retried: replaying events from before an `ALTER` is normal.)
async fn load_settled(
    meta: &mut Conn,
    key: &(String, String),
) -> Result<Option<TableSchema>, SourceError> {
    const TRIES: u32 = 20;
    const PAUSE: Duration = Duration::from_millis(50);
    let mut found = None;
    for attempt in 0..TRIES {
        found = load_table(meta, &key.0, &key.1).await?;
        if found.as_ref().is_some_and(|s| !s.key.is_empty()) {
            break;
        }
        if attempt + 1 < TRIES {
            tokio::time::sleep(PAUSE).await;
        }
    }
    Ok(found)
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
        StreamState::new(position, HashSet::new())
    }

    const UUID: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn gtid(gno: u64) -> Option<(String, Option<String>, u64)> {
        Some((UUID.into(), None, gno))
    }

    fn gtid_state() -> StreamState {
        state(Position::Gtid(format!("{UUID}:1-7").parse().unwrap()))
    }

    fn set_of(st: &StreamState) -> String {
        match &st.position {
            Position::Gtid(set) => set.to_string(),
            Position::File { .. } => panic!("gtid expected"),
        }
    }

    fn q(sql: &str) -> QueryKind {
        classify(sql, "x")
    }

    #[test]
    fn gtid_commit_accumulates_and_checkpoints() {
        let start: GtidSet = format!("{UUID}:1-5").parse().unwrap();
        let mut st = state(Position::Gtid(start));
        assert_eq!(st.checkpoint_due(true), None, "nothing moved");

        st.group_start(gtid(6), 0);
        st.query(&q("BEGIN"));
        assert_eq!(st.checkpoint_due(true), None, "inside a transaction");
        assert_eq!(st.xid(), Some(false));
        let Some(Position::Gtid(set)) = st.checkpoint_due(true) else {
            panic!("expected a gtid checkpoint");
        };
        assert_eq!(set.to_string(), format!("{UUID}:1-6"));

        // Unforced checkpoints are throttled.
        st.group_start(gtid(7), 0);
        st.query(&q("BEGIN"));
        st.query(&q("COMMIT"));
        assert_eq!(st.checkpoint_due(false), None);
        assert!(st.checkpoint_due(true).is_some());
    }

    /// Observed on MySQL 8.4: GTID 8, Query(XA START), Table_map,
    /// Write_rows, Query(XA END), XA_prepare; then GTID 9, Query(XA COMMIT).
    #[test]
    fn xa_group_is_checkpointed_at_prepare_not_at_xa_start() {
        let mut st = gtid_state();
        assert_eq!(st.group_start(gtid(8), 100), None);
        assert_eq!(st.query(&q("XA START X'61',X'',1")), None);
        assert_eq!(set_of(&st), format!("{UUID}:1-7"), "rows not emitted yet");
        assert_eq!(st.query(&q("XA END X'61',X'',1")), None);
        assert_eq!(set_of(&st), format!("{UUID}:1-7"));
        assert_eq!(st.checkpoint_due(true), None);
        assert!(st.xa_prepare().is_some());
        assert_eq!(set_of(&st), format!("{UUID}:1-8"));
        // XA COMMIT / XA ROLLBACK: a group of one query that ends itself.
        assert_eq!(st.group_start(gtid(9), 200), None);
        assert_eq!(st.query(&q("XA ROLLBACK X'61',X'',1")), Some(false));
        assert_eq!(set_of(&st), format!("{UUID}:1-9"));
    }

    #[test]
    fn mapped_tables_are_refused_inside_xa_only() {
        let mut st = gtid_state();
        st.mapped.insert(("shop".into(), "items".into()));
        let mapped = ("shop".to_owned(), "items".to_owned());
        let other = ("shop".to_owned(), "log".to_owned());
        st.group_start(gtid(8), 0);
        st.query(&q("BEGIN"));
        assert!(st.check_xa(&mapped).is_ok());
        st.xid();
        st.group_start(gtid(9), 0);
        st.query(&q("XA START 'a'"));
        assert!(st.check_xa(&other).is_ok());
        let err = st.check_xa(&mapped).unwrap_err();
        assert!(
            matches!(&err, SourceError::Config(m) if m.contains("XA transactions on mapped tables are not supported")),
            "{err:?}"
        );
        // A prepare ends the XA group.
        st.xa_prepare();
        assert!(st.check_xa(&mapped).is_ok());
    }

    /// A DDL followed by rows under one GTID, without BEGIN: the GTID is
    /// only checkpointed once the group has ended.
    #[test]
    fn lone_query_group_waits_for_the_next_group() {
        let mut st = gtid_state();
        st.group_start(gtid(8), 0);
        assert_eq!(st.query(&q("CREATE TABLE x.u (id INT PRIMARY KEY)")), None);
        assert_eq!(set_of(&st), format!("{UUID}:1-7"), "rows may still follow");
        assert_eq!(st.checkpoint_due(true), None);
        assert_eq!(st.group_start(gtid(9), 0), Some(false));
        assert_eq!(set_of(&st), format!("{UUID}:1-8"));
        assert!(st.checkpoint_due(true).is_some());

        // A heartbeat (the server is idle) or a rotate also proves the end.
        st.query(&q("CREATE USER u"));
        assert_eq!(set_of(&st), format!("{UUID}:1-8"));
        assert_eq!(st.heartbeat(0), Some(false));
        assert_eq!(set_of(&st), format!("{UUID}:1-9"));
        st.group_start(gtid(10), 0);
        st.query(&q("DROP TABLE x.u"));
        assert_eq!(set_of(&st), format!("{UUID}:1-9"));
        st.rotate("binlog.000009".into(), 4, 0);
        assert_eq!(set_of(&st), format!("{UUID}:1-10"));
        // An anonymous GTID too.
        st.group_start(gtid(11), 0);
        st.query(&q("GRANT SELECT ON x.* TO u"));
        st.group_start(None, 0);
        assert_eq!(set_of(&st), format!("{UUID}:1-11"));
    }

    #[test]
    fn create_select_as_logged_by_mysql_8_4() {
        // GTID, BEGIN, Query(CREATE TABLE … START TRANSACTION), Table_map,
        // Write_rows, Xid.
        let mut st = gtid_state();
        st.group_start(gtid(8), 0);
        st.query(&q("BEGIN"));
        st.query(&q(
            "CREATE TABLE `x`.`u` (`id` int NOT NULL) START TRANSACTION",
        ));
        assert_eq!(set_of(&st), format!("{UUID}:1-7"));
        st.txn_had_changes = true;
        assert_eq!(st.xid(), Some(true));
        assert_eq!(set_of(&st), format!("{UUID}:1-8"));
    }

    #[test]
    fn file_position_of_a_lone_group_is_the_end_of_its_last_event() {
        let mut st = state(Position::File {
            file: "binlog.000001".into(),
            pos: 4,
        });
        st.file = "binlog.000001".into();
        st.group_start(None, 200);
        st.offset = 400;
        st.query(&q("ALTER TABLE t ADD c INT"));
        // The next anonymous GTID event ends at 470.
        st.offset = 470;
        st.group_start(None, 400);
        assert_eq!(
            st.position,
            Position::File {
                file: "binlog.000001".into(),
                pos: 400
            }
        );
    }

    #[test]
    fn lag_from_event_time_and_binlog_position() {
        let (reporter, watch) = LagReporter::new();
        let mut lag = StreamLag {
            reporter: Some(reporter),
            seconds: None,
            bytes: None,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        lag.event_at(now - 30);
        lag.event_at(0); // synthesized events carry no time
        let server = Position::File {
            file: "binlog.000007".into(),
            pos: 5_000,
        };
        lag.server_at(&server, "binlog.000007", 1_200);
        lag.publish();
        let got = watch.borrow().unwrap();
        assert!((29.0..=32.0).contains(&got.seconds.unwrap()), "{got:?}");
        assert_eq!(got.bytes, Some(3_800));
        assert_eq!(got.records, None);

        // Another file: the distance is unknown.
        lag.server_at(&server, "binlog.000006", 1_200);
        assert_eq!(lag.bytes, None);
        // Idle and caught up.
        lag.caught_up();
        lag.publish();
        assert_eq!(
            *watch.borrow(),
            Some(Lag {
                seconds: Some(0.0),
                bytes: Some(0),
                records: None
            })
        );
    }

    #[test]
    fn file_commit_uses_the_current_offset() {
        let mut st = state(Position::File {
            file: "binlog.000001".into(),
            pos: 4,
        });
        st.file = "binlog.000002".into();
        st.offset = 900;
        st.xid();
        assert_eq!(
            st.checkpoint_due(true),
            Some(Position::File {
                file: "binlog.000002".into(),
                pos: 900
            })
        );
    }
}
