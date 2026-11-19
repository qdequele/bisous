//! Kafka source: JSON, Debezium envelopes, tombstones.
//!
//! One sync is one consumer with a manual partition assignment at the
//! offsets of the checkpoint saved by the engine
//! (`{topic: {partition: next_offset}}`). No consumer-group rebalancing: the
//! positions are deterministic and owned by our store. Acknowledged offsets
//! are also committed to the broker under `meili-connect-<sync_id>`, purely
//! so the user's tooling can watch the lag.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod checkpoint;
mod client;
pub mod config;
pub mod decode;
mod source;

use meili_connect_core::{
    Endpoint, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};

pub use config::{Format, KafkaConfig, StartFrom};
pub use source::{KafkaSource, RECORD_KEY};

/// Builds [`KafkaSource`]s. Topics are the `collection` of each mapping.
#[derive(Debug, Clone, Copy, Default)]
pub struct KafkaSourceFactory;

#[async_trait::async_trait]
impl SourceFactory for KafkaSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(build_source(sync_id, spec, streams)?))
    }

    /// Every bootstrap server. The brokers the cluster advertises in its
    /// metadata are only known once connected and are not listed here.
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        if spec.kind != SourceKind::Kafka {
            return Err(SourceError::Config(format!(
                "the kafka connector cannot read a `{}` config",
                spec.kind.as_str()
            )));
        }
        let config = KafkaConfig::from_value(&spec.config)?;
        config
            .bootstrap_servers
            .iter()
            .map(|server| {
                // librdkafka accepts `PROTOCOL://host:port` too.
                let bare = server.split_once("://").map_or(server.as_str(), |(_, b)| b);
                Endpoint::parse(bare, 9092).ok_or_else(|| {
                    SourceError::Config(format!("bootstrap server `{server}` is not host:port"))
                })
            })
            .collect()
    }
}

/// Same as [`KafkaSourceFactory::build`], with the concrete type (gives
/// access to [`KafkaSource::skipped_records`]).
pub fn build_source(
    sync_id: &str,
    spec: &SourceSpec,
    streams: &[StreamMapping],
) -> Result<KafkaSource, SourceError> {
    if spec.kind != SourceKind::Kafka {
        return Err(SourceError::Config(format!(
            "the kafka connector cannot build a `{}` source",
            spec.kind.as_str()
        )));
    }
    if sync_id.is_empty() {
        return Err(SourceError::Config("sync id must not be empty".to_owned()));
    }
    let config = KafkaConfig::from_value(&spec.config)?;
    let mut topics: Vec<String> = Vec::new();
    for s in streams {
        if !is_valid_topic(&s.collection) {
            return Err(SourceError::Config(format!(
                "`{}` is not a valid Kafka topic name",
                s.collection
            )));
        }
        if !topics.contains(&s.collection) {
            topics.push(s.collection.clone());
        }
    }
    // No topic is fine for `check` and `discover`; `open` refuses it.
    topics.sort();
    Ok(KafkaSource::new(sync_id, config, topics))
}

fn is_valid_topic(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapping(collection: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": collection, "index": "idx"})).unwrap()
    }

    fn spec(config: serde_json::Value) -> SourceSpec {
        SourceSpec {
            kind: SourceKind::Kafka,
            config,
        }
    }

    #[test]
    fn builds_with_deduplicated_topics() {
        let s = build_source(
            "s1",
            &spec(json!({"bootstrap_servers": ["b:9092"]})),
            &[mapping("b"), mapping("a"), mapping("b")],
        )
        .unwrap();
        assert_eq!(s.topics(), ["a", "b"]);
        let caps = s.capabilities();
        assert!(!caps.snapshot && !caps.patches);
        assert_eq!(s.skipped_records(), 0);
    }

    #[test]
    fn rejects_wrong_kind_and_bad_topics() {
        let ok = json!({"bootstrap_servers": ["b:9092"]});
        let wrong = SourceSpec {
            kind: SourceKind::Postgres,
            config: ok.clone(),
        };
        assert!(matches!(
            KafkaSourceFactory.build("s", &wrong, &[mapping("a")]),
            Err(SourceError::Config(_))
        ));
        assert!(matches!(
            KafkaSourceFactory.build("s", &spec(ok), &[mapping("public.users x")]),
            Err(SourceError::Config(_))
        ));
    }

    /// No mapping: `check` and `discover` work (the setup UI tests the
    /// connection before the topics are picked); `open` refuses it.
    #[tokio::test]
    async fn no_mapping_builds_for_check_but_does_not_open() {
        let mut s = KafkaSourceFactory
            .build("s", &spec(json!({"bootstrap_servers": ["b:9092"]})), &[])
            .unwrap();
        assert!(matches!(s.open(None).await, Err(SourceError::Config(_))));
    }

    #[tokio::test]
    async fn endpoints_are_the_bootstrap_servers() {
        let got = KafkaSourceFactory
            .endpoints(&spec(json!({"bootstrap_servers": [
                "B1.example.com:9093", "10.0.0.1", "[::1]:9094", "SASL_SSL://k.example.com:9095"
            ]})))
            .await
            .unwrap();
        assert_eq!(
            got,
            vec![
                Endpoint::new("b1.example.com", 9093),
                Endpoint::new("10.0.0.1", 9092),
                Endpoint::new("::1", 9094),
                Endpoint::new("k.example.com", 9095),
            ]
        );
        assert!(matches!(
            KafkaSourceFactory
                .endpoints(&spec(json!({"bootstrap_servers": ["h:notaport"]})))
                .await,
            Err(SourceError::Config(_))
        ));
    }

    #[tokio::test]
    async fn stream_before_open_is_fatal() {
        let mut s = KafkaSourceFactory
            .build(
                "s",
                &spec(json!({"bootstrap_servers": ["b:9092"]})),
                &[mapping("a")],
            )
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let (_ack_tx, ack_rx) = meili_connect_core::ack_channel();
        let r = s
            .stream(tx, ack_rx, tokio_util::sync::CancellationToken::new())
            .await;
        assert!(matches!(r, Err(SourceError::Fatal(_))));
    }
}
