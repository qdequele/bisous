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
//! - **Events**: see [`event`] for the mapping, [`convert`] for BSON →
//!   JSON and [`key`] for the `_id` → document id encoding.
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
pub mod key;
pub mod position;
mod source;

use meili_connect_core::{
    Endpoint, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};

pub use config::{FullDocumentMode, MongoConfig};
pub use source::MongoSource;

/// Builds [`MongoSource`]s from `SourceKind::Mongodb` specs.
#[derive(Debug, Clone, Copy, Default)]
pub struct MongoSourceFactory;

#[async_trait::async_trait]
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

    /// The seed hosts of the URI; for `mongodb+srv://` the hosts the SRV
    /// record lists (looked up here, as the driver does). Members a replica
    /// set advertises later are not known in advance.
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        if spec.kind != SourceKind::Mongodb {
            return Err(SourceError::Config(format!(
                "the MongoDB connector cannot read a {} config",
                spec.kind.as_str()
            )));
        }
        let config = MongoConfig::from_json(&spec.config)?;
        let scrub = error::Scrubber::new(&config.uri);
        let opts = mongodb::options::ClientOptions::parse(config.uri.expose())
            .await
            .map_err(|e| error::map_error(&e, "resolving the hosts of the URI", &scrub))?;
        seed_endpoints(&opts.hosts)
    }
}

fn seed_endpoints(hosts: &[mongodb::options::ServerAddress]) -> Result<Vec<Endpoint>, SourceError> {
    let mut out = Vec::with_capacity(hosts.len());
    for host in hosts {
        match host {
            mongodb::options::ServerAddress::Tcp { host, port } => {
                let ep = Endpoint::new(host, port.unwrap_or(27017));
                if !out.contains(&ep) {
                    out.push(ep);
                }
            }
            _ => {
                return Err(SourceError::Config(
                    "Unix socket addresses are not supported, give host names or addresses".into(),
                ));
            }
        }
    }
    if out.is_empty() {
        return Err(SourceError::Config("the URI names no host".into()));
    }
    Ok(out)
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

    #[tokio::test]
    async fn endpoints_are_every_seed_host() {
        let ep = |uri: &str| {
            let spec = SourceSpec {
                kind: SourceKind::Mongodb,
                config: json!({ "uri": uri }),
            };
            async move { MongoSourceFactory.endpoints(&spec).await }
        };
        assert_eq!(
            ep("mongodb://u:hunter2@A.example.com:27018,b.example.com,10.0.0.7:1/db?replicaSet=rs")
                .await
                .unwrap(),
            vec![
                Endpoint::new("a.example.com", 27018),
                Endpoint::new("b.example.com", 27017),
                Endpoint::new("10.0.0.7", 1),
            ]
        );
        assert_eq!(
            ep("mongodb://[::1]:27017").await.unwrap(),
            vec![Endpoint::new("::1", 27017)]
        );
        let err = ep("mongodb://%2Ftmp%2Fmongodb-27017.sock")
            .await
            .unwrap_err();
        assert!(matches!(err, SourceError::Config(_)), "{err:?}");
        // An SRV name that does not exist fails without leaking the URI.
        let err = ep("mongodb+srv://u:hunter2@nope.invalid/db")
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("hunter2"), "{err}");
    }

    #[test]
    fn srv_results_become_endpoints() {
        use mongodb::options::ServerAddress;
        let hosts = vec![
            ServerAddress::Tcp {
                host: "shard-00.example.net".into(),
                port: Some(27017),
            },
            ServerAddress::Tcp {
                host: "shard-01.example.net".into(),
                port: None,
            },
        ];
        assert_eq!(
            seed_endpoints(&hosts).unwrap(),
            vec![
                Endpoint::new("shard-00.example.net", 27017),
                Endpoint::new("shard-01.example.net", 27017)
            ]
        );
        assert!(seed_endpoints(&[]).is_err());
    }
}
