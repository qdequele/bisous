//! MongoDB source: change streams.
//!
//! - **Position**: one change stream for the whole sync (database-level when
//!   every mapped collection lives in one database, deployment-level
//!   otherwise) with a `$match` on the mapped namespaces, so ordering across
//!   collections is preserved and one resume token describes the position.
//!   The checkpoint is that token ([`position::Position`]).
//! - **Fresh start**: the stream is opened before any snapshot read and its
//!   initial (post-batch) resume token is the start position; the server's
//!   `operationTime` is the fallback when no token is returned.
//! - **Snapshot**: a `find` cursor per mapped collection, bounded batches.
//!   It reads from the deployment's default read preference (primary unless
//!   the URI says otherwise); reading from a lagging secondary could miss
//!   writes made just before the start position.
//! - **Events**: see [`event`] for the mapping and [`convert`] for BSON →
//!   JSON.
//! - **Origin footprint**: none. Change streams hold nothing on the server,
//!   so acks need no action and `teardown` has nothing to remove. A stopped
//!   sync can resume as long as its token is still in the oplog; after that,
//!   opening fails with `ChangeStreamHistoryLost` → `PositionLost`.
//! - **Atlas**: `mongodb+srv://` URIs work as is. A paused cluster is only
//!   recognisable when Atlas says "paused" in the error; otherwise it looks
//!   like an unreachable server (`Transient`).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod config;
pub mod convert;
pub mod error;
pub mod event;
pub mod position;
mod source;

use meili_connect_core::{
    Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};

pub use config::{FullDocumentMode, MongoConfig};
pub use source::MongoSource;

/// Builds [`MongoSource`]s from `SourceKind::Mongodb` specs.
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoSourceFactory;

impl SourceFactory for MongoSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        if spec.kind != SourceKind::Mongodb {
            return Err(SourceError::Config(format!(
                "the MongoDB connector cannot build a {} source",
                spec.kind.as_str()
            )));
        }
        let config = MongoConfig::from_json(&spec.config)?;
        Ok(Box::new(MongoSource::new(sync_id, config, streams)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn factory() {
        let streams: Vec<StreamMapping> =
            serde_json::from_value(json!([{"collection": "shop.orders", "index": "orders"}]))
                .unwrap();
        let spec = SourceSpec {
            kind: SourceKind::Mongodb,
            config: json!({"uri": "mongodb://localhost:27017"}),
        };
        let source = MongoSourceFactory.build("s1", &spec, &streams).unwrap();
        assert_eq!(
            source.capabilities(),
            meili_connect_core::Capabilities {
                snapshot: true,
                patches: true
            }
        );
        let wrong = SourceSpec {
            kind: SourceKind::Kafka,
            config: json!({}),
        };
        assert!(matches!(
            MongoSourceFactory.build("s1", &wrong, &streams),
            Err(SourceError::Config(_))
        ));
    }
}
