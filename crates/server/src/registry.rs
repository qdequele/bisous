//! Dispatches a [`SourceSpec`] to the connector crate of its kind.

use meili_connect_core::{
    Endpoint, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};
use meili_connect_source_dynamodb::DynamoSourceFactory;
use meili_connect_source_firestore::FirestoreSourceFactory;
use meili_connect_source_kafka::KafkaSourceFactory;
use meili_connect_source_mongodb::MongoSourceFactory;
use meili_connect_source_mysql::MysqlSourceFactory;
use meili_connect_source_planetscale::PlanetScaleSourceFactory;
use meili_connect_source_postgres::PostgresSourceFactory;

/// The six connectors behind one [`SourceFactory`]. Postgres serves both
/// `postgres` and `supabase`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceRegistry;

impl SourceRegistry {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl SourceFactory for SourceRegistry {
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        match spec.kind {
            SourceKind::Postgres | SourceKind::Supabase => {
                PostgresSourceFactory.endpoints(spec).await
            }
            SourceKind::Mysql => MysqlSourceFactory.endpoints(spec).await,
            SourceKind::Mongodb => MongoSourceFactory.endpoints(spec).await,
            SourceKind::Kafka => KafkaSourceFactory.endpoints(spec).await,
            SourceKind::Dynamodb => DynamoSourceFactory.endpoints(spec).await,
            SourceKind::Firestore => FirestoreSourceFactory.endpoints(spec).await,
            SourceKind::Planetscale => PlanetScaleSourceFactory.endpoints(spec).await,
        }
    }

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
            SourceKind::Firestore => FirestoreSourceFactory.build(sync_id, spec, streams),
            SourceKind::Planetscale => PlanetScaleSourceFactory.build(sync_id, spec, streams),
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
            (
                SourceKind::Firestore,
                json!({"emulator_host": "localhost:8080", "project_id": "demo-app"}),
                "**/orders",
            ),
        ];
        for (kind, config, collection) in cases {
            let spec = SourceSpec { kind, config };
            let built = SourceRegistry.build("s1", &spec, &[mapping(collection)]);
            assert!(built.is_ok(), "{kind:?}: {:?}", built.err());
            // `/sources/check` and `/sources/discover` build without
            // streams (the setup UI tests the connection before the
            // collections are picked).
            let probe = SourceRegistry.build("s1", &spec, &[]);
            assert!(probe.is_ok(), "{kind:?} without streams: {:?}", probe.err());
        }
    }

    /// Every kind declares its endpoints through the registry (the trait's
    /// default refuses, which would make every source unusable).
    #[tokio::test]
    async fn every_kind_declares_its_endpoints() {
        let cases = [
            (
                SourceKind::Postgres,
                json!({"url": "postgres://u:p@pg.example.com:6543/db"}),
                vec![Endpoint::new("pg.example.com", 6543)],
            ),
            (
                SourceKind::Supabase,
                json!({"url": "postgres://u:p@db.abc.supabase.co:5432/postgres"}),
                vec![Endpoint::new("db.abc.supabase.co", 5432)],
            ),
            (
                SourceKind::Mysql,
                json!({"host": "my.example.com", "user": "u"}),
                vec![Endpoint::new("my.example.com", 3306)],
            ),
            (
                SourceKind::Mongodb,
                json!({"uri": "mongodb://a.example.com:27018,b.example.com"}),
                vec![
                    Endpoint::new("a.example.com", 27018),
                    Endpoint::new("b.example.com", 27017),
                ],
            ),
            (
                SourceKind::Kafka,
                json!({"bootstrap_servers": ["k.example.com:9093"]}),
                vec![Endpoint::new("k.example.com", 9093)],
            ),
            (
                SourceKind::Dynamodb,
                json!({"region": "eu-west-1", "endpoint_url": "http://ddb.example.com:8000"}),
                vec![Endpoint::new("ddb.example.com", 8000)],
            ),
            (
                SourceKind::Firestore,
                json!({"emulator_host": "fs.example.com:8080", "project_id": "demo-app"}),
                vec![Endpoint::new("fs.example.com", 8080)],
            ),
        ];
        for (kind, config, expected) in cases {
            let spec = SourceSpec { kind, config };
            assert_eq!(
                SourceRegistry.endpoints(&spec).await.unwrap(),
                expected,
                "{kind:?}"
            );
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
