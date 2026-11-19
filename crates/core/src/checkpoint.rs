//! A durable source position.

use serde::{Deserialize, Serialize};

/// An opaque, source-defined position: an LSN for Postgres, a GTID set for
/// MySQL, a resume token for MongoDB, a map of partition offsets for Kafka,
/// a map of shard sequence numbers for DynamoDB.
///
/// Only the source that produced a checkpoint interprets it. The engine and
/// the store treat it as JSON. A source must be able to resume from any
/// checkpoint it has ever emitted, and re-emitting changes that precede the
/// checkpoint's successor is allowed: delivery is at-least-once and writes
/// are idempotent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Checkpoint(pub serde_json::Value);

impl Checkpoint {
    pub fn new(value: impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self(serde_json::to_value(value)?))
    }

    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.0.clone())
    }
}
