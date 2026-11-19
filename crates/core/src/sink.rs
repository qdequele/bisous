//! The contract of the Meilisearch writer.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Document, SinkError};

/// One index on the destination. `uid` is the live index name; during a
/// resync the engine passes the shadow uid instead.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexTarget {
    pub uid: String,
    /// Name of the field that holds the document id. The sink injects it.
    pub primary_key: String,
}

/// A change resolved to a document id, ready to write. The engine has
/// already coalesced a batch so that each id appears at most once per
/// consecutive run of the same kind; the sink must preserve the order of
/// the slice across kinds (an upsert after a delete of the same id must
/// land after it).
#[derive(Debug, Clone, PartialEq)]
pub enum DocOp {
    /// Full replace (`POST /indexes/{uid}/documents`).
    Replace {
        id: String,
        doc: Document,
    },
    /// Partial update (`PUT /indexes/{uid}/documents`). `unset` fields are
    /// written as `null`.
    Merge {
        id: String,
        doc: Document,
        unset: Vec<String>,
    },
    Delete {
        id: String,
    },
}

impl DocOp {
    pub fn id(&self) -> &str {
        match self {
            DocOp::Replace { id, .. } | DocOp::Merge { id, .. } | DocOp::Delete { id } => id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RejectedDoc {
    pub op: DocOp,
    /// Meilisearch's error message for this document.
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ApplyReport {
    pub written: u64,
    pub deleted: u64,
    /// Documents Meilisearch refused individually (found by bisecting a
    /// failed task). The engine sends them to the dead-letter queue and
    /// carries on; they do not fail the batch.
    pub rejected: Vec<RejectedDoc>,
}

#[async_trait]
pub trait Sink: Send + Sync {
    /// Create the index if missing with this primary key. Fails with
    /// `SinkError::Config` when the index exists with a different primary
    /// key: a sync never overwrites an index keyed differently.
    async fn ensure_index(&self, target: &IndexTarget) -> Result<(), SinkError>;

    /// Write the batch and return only once Meilisearch reports every
    /// resulting task as succeeded (or the failures are isolated into
    /// `rejected`). The engine advances the checkpoint on `Ok`, so
    /// returning early loses data on a crash.
    async fn apply(&self, target: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError>;

    /// Atomically swap two indexes and wait for the task.
    async fn swap(&self, a: &str, b: &str) -> Result<(), SinkError>;

    /// Delete an index and wait for the task. Missing index is `Ok`.
    async fn delete_index(&self, uid: &str) -> Result<(), SinkError>;

    /// Copy the settings of `from` onto `to` and wait for the task, so a
    /// shadow index is built with the live index's configuration.
    async fn copy_settings(&self, from: &str, to: &str) -> Result<(), SinkError>;

    /// Number of documents in the index, `None` when it does not exist.
    async fn document_count(&self, uid: &str) -> Result<Option<u64>, SinkError>;
}
