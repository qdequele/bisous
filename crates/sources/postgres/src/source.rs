//! [`PostgresSource`]: the `Source` lifecycle over logical replication.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, Capabilities, Change, CheckReport, Checkpoint, CollectionInfo, EventSender,
    FieldInfo, Op, Opened, Source, SourceError, SourceEvent, SourceKind,
};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationEvent};
use tokio_postgres::SimpleQueryMessage;
use tokio_util::sync::CancellationToken;

use crate::catalog::{self, TableInfo};
use crate::config::Settings;
use crate::conn::{self, CaFile, Sql};
use crate::convert::TypeRegistry;
use crate::error::{self, Context};
use crate::ident;
use crate::lsn;
use crate::pgoutput::{Decoded, Decoder, KeyColumns, Mapped};

/// Rows per `FETCH` during the snapshot: the most rows held in memory.
const SNAPSHOT_BATCH: usize = 500;
const CURSOR: &str = "meili_connect_snapshot";
/// How many times `teardown` tries to free and drop the slot (250 ms apart).
const TEARDOWN_ATTEMPTS: usize = 20;
/// Oldest supported server. `pgwire-replication` always asks pgoutput for
/// `messages 'true'`, an option pgoutput only knows from Postgres 14: an
/// older server accepts the slot and the snapshot, then refuses every
/// START_REPLICATION. Refuse it before creating anything.
pub(crate) const MIN_VERSION: i32 = 140_000;

pub(crate) fn too_old(version: i32) -> String {
    format!(
        "Postgres {}.{} is too old: logical replication through this connector needs \
         Postgres 14 or later",
        version / 10_000,
        version % 10_000
    )
}

/// One mapped table: canonical identity plus the name the user mapped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Target {
    pub(crate) schema: String,
    pub(crate) table: String,
    /// `StreamMapping::collection`, emitted unchanged in every event.
    pub(crate) name: String,
}

impl Target {
    pub(crate) fn canonical(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

/// What `open(None)` leaves for `snapshot`: the replication session that
/// created the slot (its exported snapshot lives as long as the session
/// stays idle) and the snapshot's name.
struct Pending {
    session: ReplicationClient,
    snapshot: String,
}

pub struct PostgresSource {
    kind: SourceKind,
    pub(crate) settings: Settings,
    ca: CaFile,
    pub(crate) targets: Vec<Target>,
    sql: Option<Sql>,
    pub(crate) version: i32,
    keys: KeyColumns,
    types: TypeRegistry,
    pending: Option<Pending>,
    /// Where `stream` starts: the slot's consistent point after a fresh
    /// open, the resume checkpoint otherwise.
    start: Option<Lsn>,
}

impl std::fmt::Debug for PostgresSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSource")
            .field("kind", &self.kind)
            .field("host", &self.settings.conn.host)
            .field("database", &self.settings.conn.database)
            .field("slot", &self.settings.slot)
            .field("publication", &self.settings.publication)
            .field("targets", &self.targets)
            .finish_non_exhaustive()
    }
}

impl PostgresSource {
    pub(crate) fn new(
        kind: SourceKind,
        settings: Settings,
        ca: CaFile,
        collections: &[String],
    ) -> Self {
        let mut targets: Vec<Target> = collections
            .iter()
            .map(|c| {
                let (schema, table) = ident::split_collection(c);
                Target {
                    schema: schema.to_owned(),
                    table: table.to_owned(),
                    name: c.clone(),
                }
            })
            .collect();
        targets.sort();
        targets.dedup_by(|a, b| a.schema == b.schema && a.table == b.table);
        Self {
            kind,
            settings,
            ca,
            targets,
            sql: None,
            version: 0,
            keys: KeyColumns::new(),
            types: TypeRegistry::new(),
            pending: None,
            start: None,
        }
    }

    pub(crate) fn supabase(&self) -> bool {
        self.settings.conn.supabase
    }

    pub(crate) fn table_list(&self) -> Vec<(String, String)> {
        self.targets
            .iter()
            .map(|t| (t.schema.clone(), t.table.clone()))
            .collect()
    }

    /// Connect (or reuse the connection) and read the server version.
    pub(crate) async fn connect(&mut self) -> Result<(), SourceError> {
        if self.sql.as_ref().is_some_and(|s| s.client.is_closed()) {
            self.sql = None;
        }
        if self.sql.is_none() {
            let sql = conn::connect_sql(&self.settings.conn, &CancellationToken::new()).await?;
            self.version = catalog::server_version(&sql).await?;
            if self.version >= 140_000 {
                // Between type lookups the stream's SQL session idles for
                // as long as the stream runs.
                sql.simple("SET idle_session_timeout = 0").await?;
            }
            self.sql = Some(sql);
        }
        Ok(())
    }

    pub(crate) fn db(&self) -> Result<&Sql, SourceError> {
        self.sql
            .as_ref()
            .ok_or_else(|| SourceError::Fatal("postgres connection unavailable".into()))
    }

    /// Catalog rows of the mapped tables; Config error when one is missing
    /// or has no usable key.
    async fn mapped_tables(&mut self) -> Result<Vec<TableInfo>, SourceError> {
        self.connect().await?;
        let list = self.table_list();
        let found = catalog::tables(self.db()?, self.version, Some(&list)).await?;
        let mut keys = KeyColumns::new();
        for t in &self.targets {
            let canonical = t.canonical();
            let info = found
                .iter()
                .find(|f| f.schema == t.schema && f.table == t.table)
                .ok_or_else(|| SourceError::Config(format!("table {canonical} does not exist")))?;
            if info.has_inheritance_children {
                return Err(SourceError::Config(format!(
                    "table {canonical}: {}",
                    catalog::INHERITANCE
                )));
            }
            let key = info.key().ok_or_else(|| {
                SourceError::Config(format!(
                    "table {canonical} has no primary key or usable REPLICA IDENTITY: \
                     add a primary key or ALTER TABLE ... REPLICA IDENTITY USING INDEX"
                ))
            })?;
            keys.insert(
                canonical,
                Mapped {
                    name: t.name.clone(),
                    key,
                    oid: info.oid,
                },
            );
        }
        self.keys = keys;
        Ok(found)
    }

    /// What exists under our slot and publication names, and whether each
    /// is ours. A publication is ours when its comment is our marker. A slot
    /// is ours when it is a pgoutput slot of this database and either has
    /// the name derived from the sync id or is paired with a publication of
    /// ours.
    pub(crate) async fn ownership(&mut self) -> Result<Ownership, SourceError> {
        self.connect().await?;
        let sql = self.db()?;
        let marker = catalog::marker(&self.settings.sync_id);
        let publication = catalog::publication_comment(sql, &self.settings.publication)
            .await?
            .map(|comment| comment.as_deref() == Some(marker.as_str()));
        let slot = catalog::slot(sql, &self.settings.slot).await?.map(|slot| {
            let ours = slot.plugin.as_deref() == Some("pgoutput")
                && slot.database.as_deref() == Some(self.settings.conn.database.as_str())
                && (self.settings.slot == ident::default_name(&self.settings.sync_id)
                    || publication == Some(true));
            (slot, ours)
        });
        Ok(Ownership { publication, slot })
    }

    async fn warm_types(&mut self, tables: &[TableInfo]) -> Result<(), SourceError> {
        let oids: Vec<u32> = tables
            .iter()
            .flat_map(|t| t.columns.iter().map(|c| c.type_oid))
            .collect();
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| SourceError::Fatal("postgres connection unavailable".into()))?;
        catalog::resolve_types(sql, &mut self.types, oids).await
    }

    async fn open_resume(&mut self, checkpoint: Checkpoint) -> Result<Opened, SourceError> {
        let resume = lsn::decode(&checkpoint)?;
        let slot_name = self.settings.slot.clone();
        let publication = self.settings.publication.clone();
        let database = self.settings.conn.database.clone();
        let wanted: BTreeSet<String> = self.targets.iter().map(Target::canonical).collect();
        self.connect().await?;
        let sql = self.db()?;

        let slot = catalog::slot(sql, &slot_name).await?.ok_or_else(|| {
            SourceError::PositionLost(format!("replication slot {slot_name} no longer exists"))
        })?;
        if let Some(reason) = slot.lost_reason() {
            return Err(SourceError::PositionLost(format!(
                "replication slot {slot_name} cannot stream any more: {reason}"
            )));
        }
        if slot.plugin.as_deref() != Some("pgoutput") || slot.database.as_deref() != Some(&database)
        {
            return Err(SourceError::PositionLost(format!(
                "replication slot {slot_name} is not a pgoutput slot of database {database}"
            )));
        }
        if let Some(confirmed) = slot.confirmed_flush_lsn.as_deref() {
            let confirmed = lsn::parse(confirmed)?;
            if confirmed > resume {
                return Err(SourceError::PositionLost(format!(
                    "replication slot {slot_name} already confirmed {confirmed}, past the saved \
                     checkpoint {resume}: the changes in between are gone"
                )));
            }
        }
        match catalog::publication_tables(sql, &publication).await? {
            None => {
                return Err(SourceError::PositionLost(format!(
                    "publication {publication} no longer exists"
                )));
            }
            Some(have) => {
                let missing: Vec<&String> = wanted.difference(&have).collect();
                if !missing.is_empty() {
                    return Err(SourceError::PositionLost(format!(
                        "publication {publication} does not cover {missing:?}: a resync adds them"
                    )));
                }
            }
        }
        self.start = Some(resume);
        Ok(Opened::Resumed)
    }

    async fn open_fresh(&mut self) -> Result<Opened, SourceError> {
        let list = self.table_list();
        let slot_name = self.settings.slot.clone();
        let publication = self.settings.publication.clone();
        self.connect().await?;
        let version = self.version;
        let supabase = self.supabase();
        let marker = catalog::marker(&self.settings.sync_id);
        // Never adopt what another sync or another tool owns: check both
        // names before creating or changing anything.
        let owned = self.ownership().await?;
        owned.refuse_foreign(&self.settings)?;
        let sql = self.db()?;

        // The publication must exist before the slot's start point, or
        // decoding fails on changes it cannot see the publication for.
        catalog::ensure_publication(sql, &publication, &marker, &list).await?;

        // A slot left by an interrupted first sync: its exported snapshot is
        // gone, so start over with a new one.
        if let Some((existing, _)) = owned.slot {
            if let Some(pid) = existing.active_pid {
                return Err(SourceError::Transient(format!(
                    "replication slot {slot_name} is in use by process {pid}"
                )));
            }
            sql.execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
                .await?;
        }

        let cfg = conn::replication_config(
            &self.settings.conn,
            &self.ca,
            &slot_name,
            &publication,
            Lsn::ZERO,
        );
        let mut session = ReplicationClient::connect_gated(cfg)
            .await
            .map_err(|e| error::from_wire(&e, Context::Query, supabase))?;
        let command = if version >= 150_000 {
            format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput (SNAPSHOT 'export')")
        } else {
            format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput EXPORT_SNAPSHOT")
        };
        let rows = session
            .simple_query(&command)
            .await
            .map_err(|e| error::from_wire(&e, Context::Query, supabase))?;
        let row = rows
            .first()
            .ok_or_else(|| SourceError::Fatal("CREATE_REPLICATION_SLOT returned no row".into()))?;
        let text = |i: usize| -> Result<String, SourceError> {
            row.get(i)
                .and_then(|v| v.as_ref())
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .ok_or_else(|| {
                    SourceError::Fatal(format!("CREATE_REPLICATION_SLOT: column {i} missing"))
                })
        };
        let start = lsn::parse(&text(1)?)?;
        let snapshot = text(2)?;
        tracing::info!(slot = %slot_name, %start, "created replication slot");
        self.pending = Some(Pending { session, snapshot });
        self.start = Some(start);
        Ok(Opened::Fresh {
            start: lsn::encode(start),
        })
    }

    async fn snapshot_tables(
        &mut self,
        snapshot: &str,
        events: &EventSender,
        cancel: &CancellationToken,
        session: ReplicationClient,
    ) -> Result<bool, SourceError> {
        let sql = self
            .sql
            .as_ref()
            .ok_or_else(|| SourceError::Fatal("postgres connection unavailable".into()))?;
        // The transaction sits idle whenever the engine applies
        // back-pressure, for as long as it takes: no origin-side timeout may
        // kill it (a killed snapshot restarts from scratch).
        sql.simple(&format!(
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT {};
             SET LOCAL idle_in_transaction_session_timeout = 0;
             SET LOCAL statement_timeout = 0;
             SET LOCAL lock_timeout = 0",
            ident::quote_literal(snapshot)
        ))
        .await?;
        // The snapshot is imported: the exporting session is no longer
        // needed, and closing it frees a WAL sender for the stream.
        drop(session);

        let tables = catalog::tables(sql, self.version, Some(&self.table_list())).await?;
        for target in &self.targets {
            let info = tables
                .iter()
                .find(|t| t.schema == target.schema && t.table == target.table)
                .ok_or_else(|| {
                    SourceError::Config(format!("table {} disappeared", target.canonical()))
                })?;
            let mapped = self
                .keys
                .get(&target.canonical())
                .ok_or_else(|| SourceError::Fatal(format!("{} has no key", target.canonical())))?;
            catalog::resolve_types(
                sql,
                &mut self.types,
                info.columns.iter().map(|c| c.type_oid),
            )
            .await?;
            let key_idx: Vec<usize> = mapped
                .key
                .iter()
                .map(|k| {
                    info.columns
                        .iter()
                        .position(|c| &c.name == k)
                        .ok_or_else(|| {
                            SourceError::Config(format!(
                                "key column {k} of {} is not readable",
                                target.canonical()
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            let cols = info
                .columns
                .iter()
                .map(|c| ident::quote_ident(&c.name))
                .collect::<Vec<_>>()
                .join(", ");
            sql.simple(&format!(
                "DECLARE {CURSOR} NO SCROLL CURSOR FOR SELECT {cols} FROM {}",
                ident::qualified(&info.schema, &info.table)
            ))
            .await?;
            let fetch = format!("FETCH FORWARD {SNAPSHOT_BATCH} FROM {CURSOR}");
            loop {
                let messages = tokio::select! {
                    r = sql.simple(&fetch) => r?,
                    () = cancel.cancelled() => return Ok(false),
                };
                let mut rows = 0usize;
                for msg in messages {
                    let SimpleQueryMessage::Row(row) = msg else {
                        continue;
                    };
                    rows += 1;
                    let mut doc = meili_connect_core::Document::new();
                    for (i, col) in info.columns.iter().enumerate() {
                        let value = match row.try_get(i) {
                            Ok(Some(text)) => self.types.to_json(col.type_oid, text),
                            Ok(None) => serde_json::Value::Null,
                            Err(e) => {
                                return Err(SourceError::Fatal(format!(
                                    "snapshot of {}: {e}",
                                    target.canonical()
                                )));
                            }
                        };
                        doc.insert(col.name.clone(), value);
                    }
                    let mut key = Vec::with_capacity(key_idx.len());
                    for &i in &key_idx {
                        match doc.get(&info.columns[i].name) {
                            Some(v) if !v.is_null() => key.push(v.clone()),
                            _ => {
                                return Err(SourceError::Fatal(format!(
                                    "snapshot of {}: NULL key column {}",
                                    target.canonical(),
                                    info.columns[i].name
                                )));
                            }
                        }
                    }
                    let event = SourceEvent::Change(Change {
                        collection: target.name.clone(),
                        key,
                        op: Op::Upsert { doc },
                    });
                    if !send_plain(events, event, cancel).await {
                        return Ok(false);
                    }
                }
                if rows == 0 {
                    break;
                }
            }
            sql.simple(&format!("CLOSE {CURSOR}")).await?;
        }
        sql.simple("COMMIT").await?;
        Ok(true)
    }
}

/// See [`PostgresSource::ownership`]. `None`: nothing by that name.
pub(crate) struct Ownership {
    pub(crate) publication: Option<bool>,
    pub(crate) slot: Option<(catalog::SlotInfo, bool)>,
}

impl Ownership {
    /// A Config error naming the first object that exists without being
    /// ours.
    pub(crate) fn refuse_foreign(&self, settings: &Settings) -> Result<(), SourceError> {
        if let Some(message) = self.foreign_publication(settings) {
            return Err(SourceError::Config(message));
        }
        if let Some(message) = self.foreign_slot(settings) {
            return Err(SourceError::Config(message));
        }
        Ok(())
    }

    pub(crate) fn foreign_publication(&self, settings: &Settings) -> Option<String> {
        (self.publication == Some(false)).then(|| {
            format!(
                "publication {} already exists and was not created by this sync: the name is \
                 taken, pick another publication_name (it is never changed or dropped)",
                settings.publication
            )
        })
    }

    pub(crate) fn foreign_slot(&self, settings: &Settings) -> Option<String> {
        matches!(self.slot, Some((_, false))).then(|| {
            format!(
                "replication slot {} already exists and was not created by this sync: the name \
                 is taken, pick another slot_name (it is never reused or dropped)",
                settings.slot
            )
        })
    }
}

/// Send with cancellation; false when cancelled or the engine went away.
async fn send_plain(events: &EventSender, event: SourceEvent, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        () = cancel.cancelled() => false,
        r = events.send(event) => r.is_ok(),
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: true,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        crate::check::run(self).await
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        self.connect().await?;
        let tables = catalog::tables(self.db()?, self.version, None).await?;
        Ok(tables
            .into_iter()
            .map(|t| CollectionInfo {
                name: t.collection(),
                key: t.key().unwrap_or_default(),
                fields: t
                    .columns
                    .iter()
                    .map(|c| FieldInfo {
                        name: c.name.clone(),
                        source_type: c.type_name.clone(),
                        nullable: c.nullable,
                    })
                    .collect(),
                estimated_rows: t.estimated_rows(),
            })
            .collect())
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        self.pending = None;
        self.start = None;
        if self.targets.is_empty() {
            return Err(SourceError::Config("no table is mapped".into()));
        }
        self.connect().await?;
        if self.version < MIN_VERSION {
            return Err(SourceError::Config(too_old(self.version)));
        }
        let tables = self.mapped_tables().await?;
        self.warm_types(&tables).await?;
        match resume {
            Some(cp) => self.open_resume(cp).await,
            None => self.open_fresh().await,
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let Pending { session, snapshot } = self
            .pending
            .take()
            .ok_or_else(|| SourceError::Fatal("snapshot called without a fresh open".into()))?;
        let finished = self
            .snapshot_tables(&snapshot, &events, &cancel, session)
            .await;
        match finished {
            Ok(true) => Ok(()),
            Ok(false) => {
                // Cancelled or closed: leave the transaction; the connection
                // is dropped with the source anyway.
                if let Some(sql) = &self.sql {
                    let _ = sql.simple("ROLLBACK").await;
                }
                Ok(())
            }
            Err(e) => {
                self.sql = None;
                Err(e)
            }
        }
    }

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        self.pending = None;
        let start = self
            .start
            .ok_or_else(|| SourceError::Fatal("stream called before open".into()))?;
        // Make sure the SQL connection (type lookups) is up before we hold
        // the slot.
        self.connect().await?;
        let supabase = self.supabase();
        let cfg = conn::replication_config(
            &self.settings.conn,
            &self.ca,
            &self.settings.slot,
            &self.settings.publication,
            start,
        );
        let wire =
            |e: pgwire_replication::PgWireError| error::from_wire(&e, Context::Stream, supabase);
        let mut client = ReplicationClient::connect(cfg).await.map_err(wire)?;
        tokio::select! {
            r = client.wait_started() => r.map_err(wire)?,
            () = cancel.cancelled() => return Ok(()),
        }
        tracing::info!(slot = %self.settings.slot, %start, "replication started");

        let mut state = StreamState {
            client,
            acks,
            acks_open: true,
            emitted: start,
            in_tx: false,
        };
        state.apply_ack()?;
        let mut decoder = Decoder::new(self.keys.clone());

        loop {
            let event = tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                changed = state.acks.changed(), if state.acks_open => {
                    if changed.is_err() {
                        state.acks_open = false;
                    } else {
                        state.apply_ack()?;
                    }
                    continue;
                }
                ev = state.client.recv() => ev.map_err(wire)?,
            };
            let Some(event) = event else {
                return Err(SourceError::Transient("replication stream ended".into()));
            };
            match event {
                ReplicationEvent::Begin { .. } => state.in_tx = true,
                ReplicationEvent::XLogData { data, .. } => {
                    let out = loop {
                        match decoder.decode(&data, &self.types)? {
                            Decoded::Events(out) => break out,
                            Decoded::NeedTypes(oids) => {
                                let sql = self.sql.as_ref().ok_or_else(|| {
                                    SourceError::Fatal("postgres connection unavailable".into())
                                })?;
                                catalog::resolve_types(sql, &mut self.types, oids).await?;
                            }
                            Decoded::Refetch { mut events, row } => {
                                let sql = self.sql.as_ref().ok_or_else(|| {
                                    SourceError::Fatal("postgres connection unavailable".into())
                                })?;
                                if let Some(doc) =
                                    catalog::fetch_row(sql, &mut self.types, &row).await?
                                {
                                    events.push(SourceEvent::Change(Change {
                                        collection: row.collection,
                                        key: row.key,
                                        op: Op::Upsert { doc },
                                    }));
                                }
                                break events;
                            }
                        }
                    };
                    for ev in out {
                        if !state.send(&events, ev, &cancel).await? {
                            return Ok(());
                        }
                    }
                }
                ReplicationEvent::Commit { end_lsn, .. } => {
                    state.in_tx = false;
                    if !state.checkpoint(&events, end_lsn, &cancel).await? {
                        return Ok(());
                    }
                }
                ReplicationEvent::KeepAlive { wal_end, .. } => {
                    // Outside a transaction, everything before the server's
                    // position has been decoded and sent: restarting there
                    // loses nothing, and lets the slot release WAL written
                    // by tables we do not follow.
                    if !state.in_tx
                        && wal_end > state.emitted
                        && !state.checkpoint(&events, wal_end, &cancel).await?
                    {
                        return Ok(());
                    }
                }
                ReplicationEvent::Message { .. } => {}
                ReplicationEvent::StoppedAt { .. } => {
                    return Err(SourceError::Transient("replication stream stopped".into()));
                }
            }
        }
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        self.pending = None;
        let slot_name = self.settings.slot.clone();
        let publication = self.settings.publication.clone();
        self.connect().await?;
        let owned = self.ownership().await?;
        let sql = self.db()?;
        let drop_slot = match &owned.slot {
            Some((_, true)) => true,
            Some((_, false)) => {
                tracing::warn!(slot = %slot_name, "replication slot not created by this sync: left alone");
                false
            }
            None => false,
        };
        for _ in 0..TEARDOWN_ATTEMPTS {
            if !drop_slot {
                break;
            }
            let Some(slot) = catalog::slot(sql, &slot_name).await? else {
                break;
            };
            if let Some(pid) = slot.active_pid {
                // A stream of ours that has not noticed yet: stop it. Never
                // kill a process we cannot tell is ours.
                if !catalog::is_own_backend(sql, pid).await? {
                    return Err(SourceError::Transient(format!(
                        "replication slot {slot_name} is in use by process {pid}, which is not \
                         a meili-connect session of this role: stop it, then delete again"
                    )));
                }
                sql.execute("SELECT pg_terminate_backend($1)", &[&pid])
                    .await?;
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            }
            match sql
                .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
                .await
            {
                Ok(_) => {}
                // Raced with a reconnect or a concurrent drop: look again.
                Err(SourceError::Transient(_) | SourceError::Config(_)) => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Err(e) => return Err(e),
            }
        }
        // Only report success for what is actually gone, and keep the
        // publication while the slot lives: the slot still decodes with it.
        if drop_slot && let Some(slot) = catalog::slot(sql, &slot_name).await? {
            return Err(SourceError::Transient(format!(
                "replication slot {slot_name} could not be dropped: it is still in use{}",
                slot.active_pid
                    .map(|p| format!(" by process {p}"))
                    .unwrap_or_default()
            )));
        }
        match owned.publication {
            Some(true) => {
                sql.simple(&format!(
                    "DROP PUBLICATION IF EXISTS {}",
                    ident::quote_ident(&publication)
                ))
                .await?;
            }
            Some(false) => {
                tracing::warn!(%publication, "publication not created by this sync: left alone");
            }
            None => {}
        }
        tracing::info!(slot = %slot_name, %publication, "dropped replication slot and publication");
        Ok(())
    }
}

struct StreamState {
    client: ReplicationClient,
    acks: AckReceiver,
    acks_open: bool,
    /// Highest checkpoint emitted (or the start). Acks never move the slot
    /// past it.
    emitted: Lsn,
    in_tx: bool,
}

impl StreamState {
    /// Report the latest durable checkpoint as flushed. The client sends it
    /// with the next standby status update, and answers keep-alive requests
    /// on its own whether or not it moved.
    fn apply_ack(&mut self) -> Result<(), SourceError> {
        let acked = self.acks.borrow_and_update().clone();
        if let Some(cp) = acked {
            let lsn = lsn::decode(&cp)?;
            self.client.update_applied_lsn(lsn.min(self.emitted));
        }
        Ok(())
    }

    async fn checkpoint(
        &mut self,
        events: &EventSender,
        at: Lsn,
        cancel: &CancellationToken,
    ) -> Result<bool, SourceError> {
        let sent = self
            .send(events, SourceEvent::Checkpoint(lsn::encode(at)), cancel)
            .await?;
        if sent && at > self.emitted {
            self.emitted = at;
        }
        Ok(sent)
    }

    /// Send under back-pressure while still applying acks. False when
    /// cancelled or the engine went away.
    async fn send(
        &mut self,
        events: &EventSender,
        event: SourceEvent,
        cancel: &CancellationToken,
    ) -> Result<bool, SourceError> {
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(false),
                permit = events.reserve() => {
                    return match permit {
                        Ok(permit) => {
                            permit.send(event);
                            Ok(true)
                        }
                        Err(_) => Ok(false),
                    };
                }
                changed = self.acks.changed(), if self.acks_open => {
                    if changed.is_err() {
                        self.acks_open = false;
                    } else {
                        self.apply_ack()?;
                    }
                }
            }
        }
    }
}
