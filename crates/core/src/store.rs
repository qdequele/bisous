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
    pub updated_at: DateTime<Utc>,
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
    /// applied batch; implementations may aggregate.
    async fn record_progress(
        &self,
        sync_id: &str,
        written: u64,
        deleted: u64,
        rejected: u64,
    ) -> Result<(), StoreError>;
}
