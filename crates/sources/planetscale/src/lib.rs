//! PlanetScale (Vitess) MySQL source: VStream over gRPC.
//!
//! PlanetScale's MySQL databases are Vitess keyspaces: they serve the MySQL
//! protocol, but no binlog to outside readers. Changes come through
//! vtgate's `VStream` (`vtgateservice.Vitess/VStream`, gRPC over TLS on
//! port 443, the database's user and password as HTTP basic auth), the API
//! PlanetScale's own connectors use.
//!
//! # How a sync runs
//!
//! - **open(None)** loads the mapped tables' schemas over the MySQL
//!   protocol (each must exist and have a primary key), then captures every
//!   shard's current position: a VStream from `current` on all shards of
//!   the keyspace, read until its first `VGTID` event. That position is the
//!   start checkpoint.
//! - **snapshot** reads each table by keyset pages over its primary key,
//!   through the MySQL text protocol: the same text forms VStream sends, so
//!   one converter (`convert`) serves both. The snapshot starts after the
//!   position was captured, so every change made meanwhile is streamed
//!   again: upserts and deletes by key make that overlap harmless.
//! - **stream** runs a VStream from the checkpoint with one filter rule per
//!   mapped table. Changes are emitted as their `ROW` events arrive (an
//!   update that changes the key deletes the old document); the checkpoint
//!   (every shard's GTID set) at each `COMMIT`, and at heartbeats while
//!   idle. A `FIELD` event naming an unknown column reloads that table's
//!   schema; `TRUNCATE` and `DROP TABLE` are `Truncate` events.
//! - **open(Some(position))** checks the position belongs to the database;
//!   a position whose binlogs were purged fails when the stream starts,
//!   as `PositionLost`.
//! - Nothing is created on the origin: **teardown** has nothing to do.
//!
//! Collections are named `database.table`, like the `mysql` connector's,
//! and documents are the same JSON.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod config;
mod convert;
mod error;
mod proto;
mod schema;
mod source;
mod stream;
mod vstream;

use std::collections::BTreeSet;

use meili_connect_core::{
    Endpoint, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};

pub use config::{PlanetScaleConfig, TabletKind};
pub use source::PlanetScaleSource;

/// Builds [`PlanetScaleSource`]s for `kind: planetscale`.
#[derive(Debug, Default, Clone, Copy)]
pub struct PlanetScaleSourceFactory;

impl PlanetScaleSourceFactory {
    pub fn new() -> Self {
        Self
    }

    pub fn build_source(
        &self,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<PlanetScaleSource, SourceError> {
        if spec.kind != SourceKind::Planetscale {
            return Err(SourceError::Fatal(format!(
                "the planetscale connector cannot build a {} source",
                spec.kind.as_str()
            )));
        }
        let config = PlanetScaleConfig::from_json(&spec.config)?;
        let mut tables = BTreeSet::new();
        for s in streams {
            let (db, table) = s.collection.split_once('.').ok_or_else(|| {
                SourceError::Config(format!(
                    "collection `{}` must be database.table",
                    s.collection
                ))
            })?;
            if db != config.database {
                return Err(SourceError::Config(format!(
                    "collection `{}` is not in the database {}: a PlanetScale source reads one \
                     database",
                    s.collection, config.database
                )));
            }
            if table.is_empty() {
                return Err(SourceError::Config(format!(
                    "collection `{}` names no table",
                    s.collection
                )));
            }
            tables.insert(table.to_owned());
        }
        Ok(PlanetScaleSource::new(config, tables.into_iter().collect()))
    }
}

#[async_trait::async_trait]
impl SourceFactory for PlanetScaleSourceFactory {
    fn build(
        &self,
        _sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(self.build_source(spec, streams)?))
    }

    /// The host, on its MySQL and its gRPC port.
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        endpoints(spec)
    }
}

fn endpoints(spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
    if spec.kind != SourceKind::Planetscale {
        return Err(SourceError::Config(format!(
            "the planetscale connector cannot read a {} config",
            spec.kind.as_str()
        )));
    }
    let c = PlanetScaleConfig::from_json(&spec.config)?;
    let mut out = vec![Endpoint::new(&c.host, c.mysql_port)];
    if c.grpc_port != c.mysql_port {
        out.push(Endpoint::new(&c.host, c.grpc_port));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(config: serde_json::Value) -> SourceSpec {
        SourceSpec {
            kind: SourceKind::Planetscale,
            config,
        }
    }

    fn mapping(c: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": c, "index": "i"})).unwrap()
    }

    #[test]
    fn endpoints_are_both_ports_of_the_host() {
        let s = spec(json!({"host": "AWS.connect.psdb.cloud", "database": "shop", "user": "u"}));
        assert_eq!(
            endpoints(&s).unwrap(),
            vec![
                Endpoint::new("aws.connect.psdb.cloud", 3306),
                Endpoint::new("aws.connect.psdb.cloud", 443)
            ]
        );
        assert!(endpoints(&spec(json!({"host": "h"}))).is_err());
    }

    #[test]
    fn collections_are_tables_of_the_one_database() {
        let f = PlanetScaleSourceFactory::new();
        let s = spec(json!({"host": "h", "database": "shop", "user": "u", "password": "pw"}));
        let src = f
            .build_source(
                &s,
                &[
                    mapping("shop.orders"),
                    mapping("shop.items"),
                    mapping("shop.orders"),
                ],
            )
            .unwrap();
        assert_eq!(src.tables, vec!["items", "orders"]);
        assert!(!format!("{src:?}").contains("pw\""));
        for bad in ["other.orders", "orders", "shop."] {
            assert!(
                matches!(
                    f.build_source(&s, &[mapping(bad)]),
                    Err(SourceError::Config(_))
                ),
                "{bad}"
            );
        }
        let mysql = SourceSpec {
            kind: SourceKind::Mysql,
            config: json!({}),
        };
        assert!(f.build("s", &mysql, &[]).is_err());
    }
}
