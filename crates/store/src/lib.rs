//! Durable state for meili-connect.
//!
//! - [`PgStore`]: Postgres over `sqlx`, migrations applied at startup.
//! - [`MemStore`]: in memory, for tests and single-node trials.
//!
//! Both implement [`meili_connect_core::StateStore`] (what the engine needs),
//! [`SyncRepository`] (sync specs, dead letters, progress, all
//! tenant-scoped), [`LabOutbox`] (Lab usage events) and [`UsageLedger`]
//! (usage recorded with progress, drained into one outbox lane per
//! consumer: the Lab, Tinybird).
//!
//! Tenancy: every [`SyncRepository`] method takes `tenant: Option<&str>`.
//! `None` is the global (standalone) scope and only sees global rows;
//! `Some(t)` only sees rows of `t`. A row of another scope is reported as
//! not found, never returned. [`SyncRepository::list_all_for_boot`] is the
//! one exception, for the supervisor at startup.

mod error;
mod mem;
mod pg;
mod seal;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use meili_connect_core::{DeadLetter, SyncPhase, SyncSpec, Usage};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use error::RepoError;
pub use mem::{MemLane, MemStore};
pub use pg::{PgLane, PgStore};
pub use seal::{SECRET_KEY_ENV, SecretKey};

/// A sync spec as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredSync {
    pub spec: SyncSpec,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A stored sync whose spec cannot be read (unsealed with another key,
/// tampered, copied onto another row). Listed so one bad row does not hide
/// the others; it can only be deleted.
#[derive(Debug, Clone, PartialEq)]
pub struct UnreadableSync {
    pub id: String,
    pub tenant: Option<String>,
    pub name: String,
    /// The source kind as stored next to the sealed spec.
    pub kind: String,
    /// Why it cannot be read (for the operator's log, not for tenants).
    pub error: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One row of [`SyncRepository::list_syncs`] and
/// [`SyncRepository::get_sync`].
#[derive(Debug, Clone, PartialEq)]
pub enum SyncRow {
    Readable(StoredSync),
    Unreadable(UnreadableSync),
}

impl SyncRow {
    pub fn id(&self) -> &str {
        match self {
            SyncRow::Readable(s) => &s.spec.id,
            SyncRow::Unreadable(u) => &u.id,
        }
    }

    /// The stored sync, when its spec can be read.
    pub fn readable(self) -> Option<StoredSync> {
        match self {
            SyncRow::Readable(s) => Some(s),
            SyncRow::Unreadable(_) => None,
        }
    }
}

/// One row of [`SyncRepository::list_all_for_boot`]. A row that cannot be
/// unsealed (wrong key, tampered) is reported with its error instead of
/// failing the whole boot.
#[derive(Debug)]
pub struct BootSync {
    pub id: String,
    pub tenant: Option<String>,
    pub spec: Result<SyncSpec, RepoError>,
}

/// A dead letter with its store id, for paging and deletion.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredDeadLetter {
    pub id: i64,
    pub letter: DeadLetter,
}

/// Keyset paging over dead letters, oldest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadLetterPage {
    /// Return letters with an id strictly greater than this one.
    pub after: Option<i64>,
    /// Clamped to `1..=1000`.
    pub limit: u32,
}

impl Default for DeadLetterPage {
    fn default() -> Self {
        Self {
            after: None,
            limit: 100,
        }
    }
}

impl DeadLetterPage {
    pub(crate) fn clamped_limit(&self) -> u32 {
        self.limit.clamp(1, 1000)
    }
}

/// Counters of one sync for one UTC day.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DailyProgress {
    pub day: NaiveDate,
    pub written: u64,
    pub deleted: u64,
    pub rejected: u64,
    /// Dead letters not stored because the sync had reached
    /// [`DeadLetterLimits::max_per_sync`] (they are counted in `rejected`).
    #[serde(default)]
    pub dead_letters_dropped: u64,
    /// The metered units of the day (`documents_streamed`, …).
    #[serde(flatten)]
    pub usage: Usage,
}

/// What one sync may keep in the dead-letter table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeadLetterLimits {
    /// Stored letters per sync. Past it, letters are counted
    /// ([`DailyProgress::dead_letters_dropped`]) but not stored.
    pub max_per_sync: u64,
    /// A payload whose JSON is larger is replaced by
    /// `{"truncated": true, "bytes": <size>}`.
    pub max_payload_bytes: usize,
}

impl Default for DeadLetterLimits {
    fn default() -> Self {
        Self {
            max_per_sync: 10_000,
            max_payload_bytes: 64 * 1024,
        }
    }
}

/// Longest stored `reason` and `document_id`, in characters.
const MAX_LETTER_TEXT: usize = 2_048;

fn cap_text(s: &str) -> String {
    if s.chars().count() <= MAX_LETTER_TEXT {
        s.to_owned()
    } else {
        let mut out: String = s.chars().take(MAX_LETTER_TEXT).collect();
        out.push('…');
        out
    }
}

/// The letter as stored: payload, reason and id bounded.
pub(crate) fn bound_letter(letter: &DeadLetter, limits: &DeadLetterLimits) -> DeadLetter {
    let size = serde_json::to_vec(&letter.payload).map_or(usize::MAX, |b| b.len());
    let payload = if size > limits.max_payload_bytes {
        serde_json::json!({ "truncated": true, "bytes": size })
    } else {
        letter.payload.clone()
    };
    DeadLetter {
        sync_id: letter.sync_id.clone(),
        index: cap_text(&letter.index),
        document_id: cap_text(&letter.document_id),
        payload,
        reason: cap_text(&letter.reason),
        created_at: letter.created_at,
    }
}

/// How long an event waits after its `attempts`-th failed delivery:
/// `base * 2^attempts`, capped at `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryBackoff {
    pub base: Duration,
    pub max: Duration,
}

impl RetryBackoff {
    pub fn delay(&self, attempts: u32) -> Duration {
        self.base
            .saturating_mul(1u32 << attempts.min(20))
            .min(self.max)
    }
}

/// A Lab event waiting in the outbox.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxEvent {
    /// Stable across retries: use it as the Lab event id for deduplication.
    pub id: Uuid,
    pub tenant: Option<String>,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Events leased together by [`LabOutbox::lease_events`].
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxLease {
    /// Proves the lease: pass it to [`LabOutbox::mark_events_sent`].
    pub token: Uuid,
    /// Oldest first. Empty when nothing was free.
    pub events: Vec<OutboxEvent>,
}

/// A consumer of usage events, with its own outbox lane: one failing never
/// holds the other back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Lane {
    /// `usage.recorded` events for the Lab (tenant-owned syncs only).
    Lab,
    /// Rows for the Tinybird `connect_usage` data source (every sync; the
    /// global scope as tenant `""`).
    Tinybird,
}

impl Lane {
    pub const ALL: [Lane; 2] = [Lane::Lab, Lane::Tinybird];

    pub fn as_str(self) -> &'static str {
        match self {
            Lane::Lab => "lab",
            Lane::Tinybird => "tinybird",
        }
    }

    fn bit(self) -> u8 {
        match self {
            Lane::Lab => 1,
            Lane::Tinybird => 2,
        }
    }
}

pub(crate) fn lane_mask(lanes: &[Lane]) -> u8 {
    lanes.iter().fold(0, |m, l| m | l.bit())
}

pub(crate) fn lanes_of(mask: u8) -> Vec<Lane> {
    Lane::ALL
        .into_iter()
        .filter(|l| mask & l.bit() != 0)
        .collect()
}

/// Usage recorded for one sync and one lane, not enqueued yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingUsage {
    pub sync_id: String,
    /// `None`: the global scope.
    pub tenant: Option<String>,
    /// The sync's source kind (`postgres`, …) when it was recorded.
    pub source_kind: String,
    /// The sync's name when it was recorded.
    pub sync_name: String,
    pub usage: Usage,
}

/// Turns pending usage into the payload of an outbox event.
pub type EventBuilder<'a> = dyn Fn(&PendingUsage) -> serde_json::Value + Send + Sync + 'a;

/// Which pending rows [`UsageLedger::drain_usage`] takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageDrain<'a> {
    /// Every row of one sync (it stopped, or it is being deleted), or of
    /// all (the server stops).
    All(Option<&'a str>),
    /// The rows due an event: those with a billable unit (documents or
    /// bytes, with the `sync_seconds` accumulated so far); those of a sync
    /// not in `active` (it stopped: its tail goes); and those holding only
    /// `sync_seconds` for at least `idle_after`. An idle sync's seconds
    /// thus go out about every `idle_after`, not every flush.
    Due {
        idle_after: Duration,
        /// The syncs running now, accruing `sync_seconds`.
        active: &'a [String],
    },
}

/// Whether a pending row has a unit billed by the document or the byte.
pub(crate) fn billable(u: &Usage) -> bool {
    u.documents_streamed > 0 || u.documents_snapshot > 0 || u.documents_resync > 0 || u.bytes > 0
}

/// What [`UsageLedger::drain_usage`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainOutcome {
    /// This many events were enqueued (one per sync with usage).
    Enqueued(usize),
    /// The lane holds this many unsent events, at or over the cap: nothing
    /// was taken, the usage stays pending.
    Backlogged(u64),
}

/// The units of one sync on one UTC day, from the progress table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageRow {
    pub sync_id: String,
    pub day: NaiveDate,
    pub usage: Usage,
}

/// Usage metering, durable end to end.
///
/// `record_progress` adds a batch's units to a pending row per enabled
/// lane in the same transaction as the progress counters, and
/// [`UsageLedger::drain_usage`] moves pending rows into the lane's outbox in
/// one transaction: what is sent is exactly what progress recorded, and a
/// crash at any point loses nothing.
#[async_trait]
pub trait UsageLedger: Send + Sync {
    /// The lanes `record_progress` accumulates usage for (none by default).
    /// The Lab lane skips global-scope syncs.
    fn set_usage_lanes(&self, lanes: &[Lane]);

    /// The outbox of one lane.
    fn outbox(&self, lane: Lane) -> Arc<dyn LabOutbox>;

    /// Usage recorded for a lane and not enqueued yet.
    async fn pending_usage(&self, lane: Lane) -> Result<Vec<PendingUsage>, RepoError>;

    /// Atomically take the lane's pending rows that `which` selects,
    /// enqueue one event per sync with non-zero units (built by `event`),
    /// and clear what was taken. Takes nothing while the lane holds
    /// `backlog_cap` unsent events or more (`0`: no cap): the usage keeps
    /// accumulating in its pending row (one per sync, bounded).
    ///
    /// A row's age is counted from the first usage recorded after the
    /// sync's previous event, and is stored with it: it survives a restart.
    async fn drain_usage(
        &self,
        lane: Lane,
        which: UsageDrain<'_>,
        backlog_cap: u64,
        event: &EventBuilder<'_>,
    ) -> Result<DrainOutcome, RepoError>;

    /// The units of the scope's syncs per sync and day, `from..=to`.
    /// Deleted syncs are gone with their progress.
    async fn usage(
        &self,
        tenant: Option<&str>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<UsageRow>, RepoError>;
}

/// Sync specs and their per-sync data, scoped by tenant.
#[async_trait]
pub trait SyncRepository: Send + Sync {
    /// Store a new sync. `spec.tenant` must equal `tenant`. Fails with
    /// [`RepoError::Conflict`] when the id, or the name within the tenant,
    /// is taken.
    async fn create_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError>;

    /// `None` when missing or owned by another scope. A row whose spec
    /// cannot be read is [`SyncRow::Unreadable`], not an error.
    async fn get_sync(&self, tenant: Option<&str>, id: &str) -> Result<Option<SyncRow>, RepoError>;

    /// The scope's syncs, ordered by name. A row whose spec cannot be read
    /// is listed as [`SyncRow::Unreadable`] and never hides the others.
    async fn list_syncs(&self, tenant: Option<&str>) -> Result<Vec<SyncRow>, RepoError>;

    /// Replace the spec of an existing sync (matched by `spec.id`).
    /// `spec.tenant` must equal `tenant`: a sync never changes owner.
    async fn update_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError>;

    /// Delete a sync with its state, dead letters and progress.
    async fn delete_sync(&self, tenant: Option<&str>, id: &str) -> Result<(), RepoError>;

    /// Every sync of every tenant, for the supervisor at startup.
    async fn list_all_for_boot(&self) -> Result<Vec<BootSync>, RepoError>;

    /// Dead letters of one sync, oldest first.
    async fn list_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        page: DeadLetterPage,
    ) -> Result<Vec<StoredDeadLetter>, RepoError>;

    /// Delete the given dead letters of one sync (after a replay or a
    /// discard). Ids of other syncs are ignored. Returns how many went.
    async fn delete_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        ids: &[i64],
    ) -> Result<u64, RepoError>;

    /// Delete every dead letter of one sync. Returns how many went.
    async fn clear_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
    ) -> Result<u64, RepoError>;

    /// Daily counters for `from..=to` (UTC days), ascending. Days without
    /// activity are absent.
    async fn progress(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<DailyProgress>, RepoError>;

    /// Dead letters stored for one sync.
    async fn count_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
    ) -> Result<u64, RepoError>;

    /// How many syncs of every scope are in each phase (a sync that never
    /// ran is `Pending`). Phases without a sync are absent. For metrics.
    async fn phase_counts(&self) -> Result<Vec<(SyncPhase, u64)>, RepoError>;
}

/// Transactional outbox of Lab events (`usage.recorded`).
#[async_trait]
pub trait LabOutbox: Send + Sync {
    /// Queue an event. Returns its id.
    async fn enqueue_event(
        &self,
        tenant: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<Uuid, RepoError>;

    /// Lease up to `limit` unsent events whose lease is free or expired,
    /// oldest first, for `lease`, under a fresh token. Concurrent leasers
    /// never get the same event while its lease runs.
    async fn lease_events(&self, limit: u32, lease: Duration) -> Result<OutboxLease, RepoError>;

    /// Mark events as delivered, but only those still held under `lease`
    /// (the token of an [`OutboxLease`]): an event whose lease expired and
    /// was taken by another leaser is left to that leaser. Returns how many
    /// were newly marked.
    async fn mark_events_sent(&self, lease: Uuid, ids: &[Uuid]) -> Result<u64, RepoError>;

    /// Delete delivered events sent more than `older_than` ago, in bounded
    /// batches so no single statement holds locks for long. Unsent events
    /// are never deleted. Returns how many went.
    async fn prune_sent_events(&self, older_than: Duration) -> Result<u64, RepoError>;

    /// A delivery of these events (still held under `lease`) failed: count
    /// the attempt, release the lease, and keep them back until
    /// `backoff.delay(attempts)` from now. Returns how many were deferred.
    async fn defer_events(
        &self,
        lease: Uuid,
        ids: &[Uuid],
        backoff: RetryBackoff,
    ) -> Result<u64, RepoError>;

    /// Events not delivered yet (the backlog).
    async fn count_unsent(&self) -> Result<u64, RepoError>;
}

/// Checks shared by both stores before writing a spec.
pub(crate) fn validate_spec(tenant: Option<&str>, spec: &SyncSpec) -> Result<(), RepoError> {
    if spec.id.is_empty() {
        return Err(RepoError::Invalid("sync id is empty".into()));
    }
    spec.validate().map_err(RepoError::Invalid)?;
    if spec.name.is_empty() {
        return Err(RepoError::Invalid("sync name is empty".into()));
    }
    if tenant == Some("") {
        return Err(RepoError::Invalid("tenant id is empty".into()));
    }
    if spec.tenant.as_deref() != tenant {
        return Err(RepoError::Invalid(
            "the spec's tenant does not match the request's tenant".into(),
        ));
    }
    Ok(())
}

pub(crate) fn to_u64(v: i64) -> u64 {
    u64::try_from(v).unwrap_or(0)
}

pub(crate) fn to_i64(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) mod test_support {
    use chrono::{DateTime, Utc};
    use meili_connect_core::{
        Checkpoint, DeadLetter, MeiliDestination, Progress, Secret, SourceKind, SourceSpec,
        StreamMapping, SyncPhase, SyncSpec, SyncState, Usage,
    };
    use serde_json::json;

    /// Now, truncated to microseconds as Postgres stores it.
    pub fn now() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap_or_default()
    }

    pub fn spec(id: &str, tenant: Option<&str>, name: &str) -> SyncSpec {
        SyncSpec {
            id: id.into(),
            tenant: tenant.map(str::to_owned),
            name: name.into(),
            source: SourceSpec {
                kind: SourceKind::Postgres,
                config: json!({ "url": "postgres://u:hunter2@db/app" }),
            },
            destination: MeiliDestination {
                url: "http://meili:7700".into(),
                api_key: Secret::new("meili-key"),
            },
            streams: vec![StreamMapping {
                collection: "public.orders".into(),
                index: "orders".into(),
                primary_key: "id".into(),
                fields: Default::default(),
                rename: Default::default(),
                vector: None,
            }],
            mass_delete_guard: Some(0.5),
        }
    }

    /// What both stores must do with usage: accumulate it per lane with the
    /// progress, drain it atomically into the lane's outbox, keep lanes
    /// apart, keep it through a deletion, and hold it back on a backlog.
    pub async fn ledger_contract<S>(store: &S)
    where
        S: meili_connect_core::StateStore + crate::SyncRepository + crate::UsageLedger,
    {
        use crate::{DrainOutcome, Lane, PendingUsage, UsageDrain};
        use std::time::Duration;
        store.set_usage_lanes(&[Lane::Lab, Lane::Tinybird]);
        store
            .create_sync(Some("t"), &spec("ts", Some("t"), "products"))
            .await
            .unwrap();
        store
            .create_sync(None, &spec("gs", None, "global"))
            .await
            .unwrap();
        store
            .record_progress("ts", &units(2, 3, 0, 50, 4))
            .await
            .unwrap();
        store
            .record_progress("ts", &units(1, 0, 1, 10, 0))
            .await
            .unwrap();
        store
            .record_progress("gs", &units(5, 0, 0, 5, 0))
            .await
            .unwrap();
        // Counters without units add nothing to the pending usage.
        store.record_progress("ts", &pr(0, 0, 9)).await.unwrap();

        let lab = store.pending_usage(Lane::Lab).await.unwrap();
        assert_eq!(lab.len(), 1, "global syncs never reach the Lab lane");
        assert_eq!(lab[0].tenant.as_deref(), Some("t"));
        assert_eq!(lab[0].source_kind, "postgres");
        assert_eq!(lab[0].sync_name, "products");
        assert_eq!(
            lab[0].usage,
            Usage {
                documents_streamed: 3,
                documents_snapshot: 3,
                documents_resync: 1,
                bytes: 60,
                sync_seconds: 4,
            }
        );
        assert_eq!(store.pending_usage(Lane::Tinybird).await.unwrap().len(), 2);

        let build = |p: &PendingUsage| {
            json!({
                "sync": p.sync_id,
                "tenant": p.tenant,
                "streamed": p.usage.documents_streamed,
            })
        };
        // A full lane takes nothing and keeps the usage pending.
        let lab_outbox = store.outbox(Lane::Lab);
        lab_outbox
            .enqueue_event(Some("t"), &json!({}))
            .await
            .unwrap();
        assert_eq!(
            store
                .drain_usage(Lane::Lab, UsageDrain::All(None), 1, &build)
                .await
                .unwrap(),
            DrainOutcome::Backlogged(1)
        );
        assert_eq!(store.pending_usage(Lane::Lab).await.unwrap().len(), 1);
        let lease = lab_outbox
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        lab_outbox
            .mark_events_sent(lease.token, &[lease.events[0].id])
            .await
            .unwrap();

        assert_eq!(
            store
                .drain_usage(Lane::Lab, UsageDrain::All(None), 1, &build)
                .await
                .unwrap(),
            DrainOutcome::Enqueued(1)
        );
        assert!(store.pending_usage(Lane::Lab).await.unwrap().is_empty());
        assert_eq!(
            store
                .drain_usage(Lane::Lab, UsageDrain::All(None), 0, &build)
                .await
                .unwrap(),
            DrainOutcome::Enqueued(0),
            "nothing new"
        );
        let lease = lab_outbox
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(lease.events.len(), 1);
        assert_eq!(lease.events[0].payload["streamed"], 3);
        assert_eq!(lease.events[0].tenant.as_deref(), Some("t"));
        // The Tinybird lane is untouched by the Lab's drain and lease.
        assert_eq!(store.pending_usage(Lane::Tinybird).await.unwrap().len(), 2);
        let tb_outbox = store.outbox(Lane::Tinybird);
        assert!(
            tb_outbox
                .lease_events(10, Duration::from_secs(30))
                .await
                .unwrap()
                .events
                .is_empty()
        );

        // Drain one sync, then delete the other: its usage is still sent.
        assert_eq!(
            store
                .drain_usage(Lane::Tinybird, UsageDrain::All(Some("gs")), 0, &build)
                .await
                .unwrap(),
            DrainOutcome::Enqueued(1)
        );
        store
            .record_progress("ts", &units(7, 0, 0, 0, 0))
            .await
            .unwrap();
        store.delete_sync(Some("t"), "ts").await.unwrap();
        assert_eq!(
            store
                .drain_usage(Lane::Tinybird, UsageDrain::All(None), 0, &build)
                .await
                .unwrap(),
            DrainOutcome::Enqueued(1)
        );
        let lease = tb_outbox
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        let mut streamed: Vec<(String, u64)> = lease
            .events
            .iter()
            .map(|e| {
                (
                    e.payload["sync"].as_str().unwrap().to_owned(),
                    e.payload["streamed"].as_u64().unwrap(),
                )
            })
            .collect();
        streamed.sort();
        assert_eq!(streamed, vec![("gs".into(), 5), ("ts".into(), 10)]);
        assert_eq!(tb_outbox.count_unsent().await.unwrap(), 2);
        assert_eq!(lab_outbox.count_unsent().await.unwrap(), 1);

        // No lanes: nothing accumulates.
        store.set_usage_lanes(&[]);
        store
            .record_progress("gs", &units(1, 0, 0, 0, 0))
            .await
            .unwrap();
        assert!(
            store
                .pending_usage(Lane::Tinybird)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// What both stores take with [`crate::UsageDrain::Due`]: billable usage
    /// at once (with the seconds so far), only seconds once the row is
    /// `idle_after` old or its sync stopped; nothing else, and nothing lost.
    /// `age` makes the pending rows look older (the store's own clock).
    pub async fn idle_usage_contract<S>(store: &S, age: impl AsyncFn(&S, std::time::Duration))
    where
        S: meili_connect_core::StateStore + crate::SyncRepository + crate::UsageLedger,
    {
        use crate::{DrainOutcome, Lane, PendingUsage, UsageDrain};
        use std::time::Duration;
        store.set_usage_lanes(&[Lane::Lab]);
        store
            .create_sync(Some("t"), &spec("ts", Some("t"), "products"))
            .await
            .unwrap();
        let build = |p: &PendingUsage| json!({"s": p.usage.sync_seconds, "n": p.usage.documents_streamed, "b": p.usage.bytes});
        let active = ["ts".to_owned()];
        let hour = Duration::from_secs(3600);
        let due = UsageDrain::Due {
            idle_after: hour,
            active: &active,
        };
        let drain = async |which| match store.drain_usage(Lane::Lab, which, 0, &build).await {
            Ok(DrainOutcome::Enqueued(n)) => n,
            other => panic!("{other:?}"),
        };
        let outbox = store.outbox(Lane::Lab);
        let take = async || {
            let lease = outbox
                .lease_events(10, Duration::from_secs(30))
                .await
                .unwrap();
            let ids: Vec<uuid::Uuid> = lease.events.iter().map(|e| e.id).collect();
            outbox.mark_events_sent(lease.token, &ids).await.unwrap();
            lease
                .events
                .into_iter()
                .map(|e| e.payload)
                .collect::<Vec<_>>()
        };

        // An idle sync: its seconds accumulate, no event.
        for _ in 0..3 {
            store
                .record_progress("ts", &units(0, 0, 0, 0, 15))
                .await
                .unwrap();
            assert_eq!(drain(due).await, 0);
        }
        assert_eq!(
            store.pending_usage(Lane::Lab).await.unwrap()[0]
                .usage
                .sync_seconds,
            45
        );
        // Recording more does not make the row younger.
        age(store, Duration::from_secs(3599)).await;
        store
            .record_progress("ts", &units(0, 0, 0, 0, 15))
            .await
            .unwrap();
        age(store, Duration::from_secs(2)).await;
        assert_eq!(drain(due).await, 1, "an idle period passed");
        assert_eq!(take().await, vec![json!({"s": 60, "n": 0, "b": 0})]);
        assert_eq!(drain(due).await, 0);

        // Activity takes the seconds with it.
        store
            .record_progress("ts", &units(0, 0, 0, 0, 15))
            .await
            .unwrap();
        store
            .record_progress("ts", &units(2, 0, 0, 20, 0))
            .await
            .unwrap();
        assert_eq!(drain(due).await, 1);
        assert_eq!(take().await, vec![json!({"s": 15, "n": 2, "b": 20})]);

        // A stopped sync's tail goes at the next drain, or at once.
        store
            .record_progress("ts", &units(0, 0, 0, 0, 5))
            .await
            .unwrap();
        let stopped = UsageDrain::Due {
            idle_after: hour,
            active: &[],
        };
        assert_eq!(drain(stopped).await, 1);
        store
            .record_progress("ts", &units(0, 0, 0, 0, 7))
            .await
            .unwrap();
        assert_eq!(drain(UsageDrain::All(Some("other"))).await, 0);
        assert_eq!(drain(UsageDrain::All(Some("ts"))).await, 1);
        assert_eq!(
            take().await,
            vec![
                json!({"s": 5, "n": 0, "b": 0}),
                json!({"s": 7, "n": 0, "b": 0})
            ]
        );
        assert!(store.pending_usage(Lane::Lab).await.unwrap().is_empty());
    }

    /// Phase counts span every scope; a sync that never ran is pending.
    pub async fn phases_and_letters_contract<S>(store: &S)
    where
        S: meili_connect_core::StateStore + crate::SyncRepository,
    {
        store
            .create_sync(None, &spec("a", None, "a"))
            .await
            .unwrap();
        store
            .create_sync(Some("t"), &spec("b", Some("t"), "b"))
            .await
            .unwrap();
        store
            .create_sync(Some("u"), &spec("c", Some("u"), "c"))
            .await
            .unwrap();
        store.save(&state("b")).await.unwrap();
        let mut held = state("c");
        held.phase = SyncPhase::Held;
        store.save(&held).await.unwrap();
        let mut counts = store.phase_counts().await.unwrap();
        counts.sort_by_key(|(p, _)| format!("{p:?}"));
        assert_eq!(
            counts,
            vec![
                (SyncPhase::Held, 1),
                (SyncPhase::Pending, 1),
                (SyncPhase::Streaming, 1)
            ]
        );
        store
            .push_dead_letters(&[letter("b", "1"), letter("b", "2"), letter("c", "3")])
            .await
            .unwrap();
        assert_eq!(store.count_dead_letters(Some("t"), "b").await.unwrap(), 2);
        assert!(store.count_dead_letters(None, "b").await.is_err(), "scoped");
    }

    /// Progress of a batch: counters only, no units.
    pub fn pr(written: u64, deleted: u64, rejected: u64) -> Progress {
        Progress {
            written,
            deleted,
            rejected,
            ..Progress::default()
        }
    }

    /// Progress carrying units.
    pub fn units(streamed: u64, snapshot: u64, resync: u64, bytes: u64, seconds: u64) -> Progress {
        Progress {
            written: streamed + snapshot + resync,
            usage: Usage {
                documents_streamed: streamed,
                documents_snapshot: snapshot,
                documents_resync: resync,
                bytes,
                sync_seconds: seconds,
            },
            ..Progress::default()
        }
    }

    pub fn state(sync_id: &str) -> SyncState {
        SyncState {
            sync_id: sync_id.into(),
            phase: SyncPhase::Streaming,
            checkpoint: Some(Checkpoint(json!({ "lsn": "0/16B3748" }))),
            message: None,
            resyncing: false,
            resync_reason: None,
            filling: Vec::new(),
            updated_at: now(),
        }
    }

    /// A spec `SyncSpec::validate` refuses.
    pub fn invalid_specs(id: &str, tenant: Option<&str>) -> Vec<SyncSpec> {
        let mut no_streams = spec(id, tenant, "n");
        no_streams.streams.clear();
        let mut bad_index = spec(id, tenant, "n");
        bad_index.streams[0].index = "bad uid".into();
        let mut bad_guard = spec(id, tenant, "n");
        bad_guard.mass_delete_guard = Some(2.0);
        vec![no_streams, bad_index, bad_guard]
    }

    /// A record the source skipped: no payload, a position as the id.
    pub fn skipped_letter(sync_id: &str, position: &str) -> DeadLetter {
        DeadLetter {
            sync_id: sync_id.into(),
            index: "orders".into(),
            document_id: position.into(),
            payload: serde_json::Value::Null,
            reason: "undecodable record".into(),
            created_at: now(),
        }
    }

    pub fn letter(sync_id: &str, doc: &str) -> DeadLetter {
        DeadLetter {
            sync_id: sync_id.into(),
            index: "orders".into(),
            document_id: doc.into(),
            payload: json!({ "kind": "replace", "doc": { "id": doc } }),
            reason: "invalid_document_fields".into(),
            created_at: now(),
        }
    }
}
