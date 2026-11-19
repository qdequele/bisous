//! Dispatches a [`SourceSpec`] to the connector crate of its kind.

use meili_connect_core::{
    Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};
use meili_connect_source_dynamodb::DynamoSourceFactory;
use meili_connect_source_kafka::KafkaSourceFactory;
use meili_connect_source_mongodb::MongoSourceFactory;
use meili_connect_source_mysql::MysqlSourceFactory;
use meili_connect_source_postgres::PostgresSourceFactory;

/// The five connectors behind one [`SourceFactory`]. Postgres serves both
/// `postgres` and `supabase`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceRegistry;

impl SourceRegistry {
    pub fn new() -> Self {
        Self
    }
}

impl SourceFactory for SourceRegistry {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        match spec.kind {
            SourceKind::Postgres | SourceKind::Supabase => {
                PostgresSourceFactory.build(sync_id, spec, streams)
            }
            SourceKind::Mysql => MysqlSourceFactory.build(sync_id, spec, streams),
            SourceKind::Mongodb => MongoSourceFactory.build(sync_id, spec, streams),
            SourceKind::Kafka => KafkaSourceFactory.build(sync_id, spec, streams),
            SourceKind::Dynamodb => DynamoSourceFactory.build(sync_id, spec, streams),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    fn mapping(c: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": c, "index": "i"})).unwrap()
    }

    #[test]
    fn every_kind_reaches_its_connector() {
        let cases = [
            (
                SourceKind::Postgres,
                json!({"url": "postgres://u:p@localhost:5432/db"}),
                "public.t",
            ),
            (
                SourceKind::Supabase,
                json!({"url": "postgres://u:p@db.abc.supabase.co:5432/postgres"}),
                "public.t",
            ),
            (SourceKind::Mysql, json!({"host": "h", "user": "u"}), "db.t"),
            (
                SourceKind::Mongodb,
                json!({"uri": "mongodb://localhost:27017"}),
                "db.c",
            ),
            (
                SourceKind::Kafka,
                json!({"bootstrap_servers": ["localhost:9092"]}),
                "topic",
            ),
            (
                SourceKind::Dynamodb,
                json!({"region": "eu-west-1"}),
                "table",
            ),
        ];
        for (kind, config, collection) in cases {
            let spec = SourceSpec { kind, config };
            let built = SourceRegistry.build("s1", &spec, &[mapping(collection)]);
            assert!(built.is_ok(), "{kind:?}: {:?}", built.err());
        }
    }

    #[test]
    fn a_config_of_the_wrong_shape_is_a_config_error() {
        let spec = SourceSpec {
            kind: SourceKind::Mongodb,
            config: json!({"nope": true}),
        };
        assert!(matches!(
            SourceRegistry.build("s1", &spec, &[mapping("db.c")]),
            Err(SourceError::Config(_))
        ));
    }
}
