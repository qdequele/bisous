//! The contract of the durable state the engine needs.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Checkpoint, StoreError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPhase {
    /// Created, never run.
    Pending,
    /// Reading existing data (first run, or a resync into shadow indexes).
    Snapshotting,
    /// Following the change stream.
    Streaming,
    /// Stopped by the user.
    Paused,
    /// The origin is deliberately unavailable (`SourceError::Paused`).
    SourcePaused,
    /// Stopped by a guard or a configuration error; needs the user.
    Held,
    /// Stopped by a fatal error.
    Failed,
}

/// Who asked for a resync. It decides how its documents are metered
/// (`docs/LAB_INTEGRATION.md`, "Pricing").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResyncReason {
    /// The user (`POST /syncs/{id}/resync`): metered as `documents_snapshot`.
    User,
    /// The engine, after `SourceError::PositionLost`: metered as
    /// `documents_resync` (free).
    System,
}

/// Everything the engine persists about one sync. `checkpoint` is `None`
/// until the first snapshot has completed: a crash mid-snapshot restarts
/// the snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncState {
    pub sync_id: String,
    pub phase: SyncPhase,
    pub checkpoint: Option<Checkpoint>,
    /// Human-readable reason for `Held`, `Failed`, `SourcePaused`.
    pub message: Option<String>,
    /// Set while a resync is building shadow indexes.
    pub resyncing: bool,
    /// Why the resync in progress runs; kept across a crash so the rest of
    /// it is metered the same way. `None` when not resyncing (and for a
    /// resync persisted before this field existed: metered as `System`).
    #[serde(default)]
    pub resync_reason: Option<ResyncReason>,
    /// The live indexes a snapshot in progress writes to directly because
    /// they held no document when it started: every document in them comes
    /// from this sync. If the snapshot is interrupted, the next run empties
    /// them and fills them again in place instead of building a shadow index
    /// (whose settings would have to be copied, which Meilisearch does not
    /// allow for an embedder with an API key). Empty once the snapshot has
    /// completed, and for a state saved before this field existed.
    #[serde(default)]
    pub filling: Vec<String>,
    pub updated_at: DateTime<Utc>,
}

/// How an applied document is metered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentKind {
    /// Applied from the change stream.
    Streamed,
    /// Written by a first sync or a resync the user asked for.
    Snapshot,
    /// Rewritten by a resync the engine started (position lost).
    Resync,
}

/// The metered units of `docs/LAB_INTEGRATION.md` ("Pricing"). The field
/// names are the unit names of the `usage.recorded` event: never rename
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Usage {
    /// Document writes and deletes applied from the change stream.
    #[serde(default)]
    pub documents_streamed: u64,
    /// Documents written by a first sync or a user-requested resync.
    #[serde(default)]
    pub documents_snapshot: u64,
    /// Documents rewritten by a resync the engine started.
    #[serde(default)]
    pub documents_resync: u64,
    /// Uncompressed payload bytes sent to Meilisearch for accepted
    /// documents.
    #[serde(default)]
    pub bytes: u64,
    /// Seconds spent in `snapshotting` or `streaming`.
    #[serde(default)]
    pub sync_seconds: u64,
}

impl Usage {
    /// `n` documents of one kind.
    pub fn documents(kind: DocumentKind, n: u64) -> Self {
        let mut u = Self::default();
        match kind {
            DocumentKind::Streamed => u.documents_streamed = n,
            DocumentKind::Snapshot => u.documents_snapshot = n,
            DocumentKind::Resync => u.documents_resync = n,
        }
        u
    }

    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    /// Every document unit together.
    pub fn total_documents(&self) -> u64 {
        self.documents_streamed
            .saturating_add(self.documents_snapshot)
            .saturating_add(self.documents_resync)
    }

    /// Adds `other`, saturating.
    pub fn add(&mut self, other: &Usage) {
        self.documents_streamed = self
            .documents_streamed
            .saturating_add(other.documents_streamed);
        self.documents_snapshot = self
            .documents_snapshot
            .saturating_add(other.documents_snapshot);
        self.documents_resync = self.documents_resync.saturating_add(other.documents_resync);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.sync_seconds = self.sync_seconds.saturating_add(other.sync_seconds);
    }
}

/// What one applied batch (or one tick of sync time) adds to a sync's
/// counters. `written + deleted` are the documents Meilisearch accepted;
/// `rejected` counts refused and skipped records, never metered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Progress {
    pub written: u64,
    pub deleted: u64,
    pub rejected: u64,
    pub usage: Usage,
    /// For metrics only, never stored: how long the batch took from its
    /// first source event to Meilisearch's tasks succeeded.
    pub batch_age: Option<std::time::Duration>,
}

impl Progress {
    pub fn is_zero(&self) -> bool {
        self.written == 0 && self.deleted == 0 && self.rejected == 0 && self.usage.is_zero()
    }
}

/// A document Meilisearch refused, kept for replay or discard.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadLetter {
    pub sync_id: String,
    pub index: String,
    pub document_id: String,
    /// The operation as JSON (`{"kind":"replace","doc":{...}}`).
    pub payload: serde_json::Value,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    /// `None` when the sync has never run.
    async fn load(&self, sync_id: &str) -> Result<Option<SyncState>, StoreError>;

    /// Upsert the whole state. Must be durable before returning: the engine
    /// acknowledges the checkpoint to the source right after.
    async fn save(&self, state: &SyncState) -> Result<(), StoreError>;

    async fn push_dead_letters(&self, letters: &[DeadLetter]) -> Result<(), StoreError>;

    /// Per-sync counters for usage and the UI. Additive, called once per
    /// applied batch (and by the supervisor for `sync_seconds`). A store
    /// that meters usage records the units in the same transaction as the
    /// counters, so what is billed is exactly what progress recorded.
    async fn record_progress(&self, sync_id: &str, progress: &Progress) -> Result<(), StoreError>;
}
