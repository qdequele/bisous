//! Durable state for meili-connect.
//!
//! - [`PgStore`]: Postgres over `sqlx`, migrations applied at startup.
//! - [`MemStore`]: in memory, for tests and single-node trials.
//!
//! Both implement [`meili_connect_core::StateStore`] (what the engine needs),
//! [`SyncRepository`] (sync specs, dead letters, progress, all
//! tenant-scoped) and [`LabOutbox`] (Lab usage events).
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

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use meili_connect_core::{DeadLetter, SyncSpec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use error::RepoError;
pub use mem::MemStore;
pub use pg::PgStore;
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
pub(crate) mod test_support {
    use chrono::{DateTime, Utc};
    use meili_connect_core::{
        Checkpoint, DeadLetter, MeiliDestination, Secret, SourceKind, SourceSpec, StreamMapping,
        SyncPhase, SyncSpec, SyncState,
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

    pub fn state(sync_id: &str) -> SyncState {
        SyncState {
            sync_id: sync_id.into(),
            phase: SyncPhase::Streaming,
            checkpoint: Some(Checkpoint(json!({ "lsn": "0/16B3748" }))),
            message: None,
            resyncing: false,
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
