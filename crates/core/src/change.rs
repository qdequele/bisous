//! What a source emits.

use serde::{Deserialize, Serialize};

use crate::Checkpoint;

/// A Meilisearch document body, before the primary key is injected.
pub type Document = serde_json::Map<String, serde_json::Value>;

/// One row-level change, already converted to JSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Change {
    /// Source-side name the change belongs to: `schema.table` for SQL
    /// databases, `database.collection` for MongoDB, the topic for Kafka,
    /// the table name for DynamoDB. It is matched against
    /// [`crate::StreamMapping::collection`].
    pub collection: String,
    /// The row's key columns, in key order. The engine turns them into the
    /// Meilisearch document id with [`crate::document_id`]. Never empty.
    pub key: Vec<serde_json::Value>,
    pub op: Op,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// The full row. Replaces the document.
    Upsert {
        doc: Document,
    },
    /// Only some fields (Postgres unchanged TOAST columns, DynamoDB
    /// `KEYS_ONLY`-style views, MongoDB update descriptions). Merged into
    /// the existing document. `unset` lists fields to remove.
    Patch {
        doc: Document,
        unset: Vec<String>,
    },
    Delete,
}

/// The stream a source produces, in commit order.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceEvent {
    Change(Change),
    /// "Every change emitted before this marker is covered by this
    /// checkpoint." Sources emit it at transaction or batch boundaries, and
    /// at least every few seconds while idle if the position moves
    /// (keep-alives), so the engine can advance the durable position.
    Checkpoint(Checkpoint),
    /// All rows of a collection were removed (`TRUNCATE`, dropped
    /// collection). The engine applies its mass-delete guard.
    Truncate {
        collection: String,
    },
    /// The collection's shape changed (DDL, new relation message). Purely
    /// informational: documents are schemaless on the Meilisearch side.
    SchemaChanged {
        collection: String,
    },
}
