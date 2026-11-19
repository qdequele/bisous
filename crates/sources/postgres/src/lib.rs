//! Postgres and Supabase source: logical replication (pgoutput).
//!
//! # How a sync runs
//!
//! - **open(None)** creates (or resets) the publication
//!   `meili_connect_<sync_id>` for exactly the mapped tables, then the
//!   logical slot of the same name over the replication protocol with
//!   `EXPORT_SNAPSHOT`. The slot's consistent point is the start
//!   checkpoint; the exported snapshot shows the database exactly as of
//!   that point.
//! - **snapshot** imports that snapshot into a `REPEATABLE READ READ ONLY`
//!   transaction (`SET TRANSACTION SNAPSHOT`) and reads each table through a
//!   server-side cursor, `FETCH`ing a bounded batch at a time. Every row
//!   committed before the consistent point is read; every later commit is
//!   streamed. No gap, no overlap.
//! - **stream** runs `START_REPLICATION` from the start (or resume) LSN,
//!   decodes pgoutput v1, emits changes and a `Checkpoint` at each commit
//!   (its end LSN) and on keep-alives outside transactions, and reports the
//!   engine's acknowledged checkpoint as the flush position.
//! - **open(Some(lsn))** verifies the slot still exists, is not
//!   invalidated, and has not confirmed past `lsn`; otherwise
//!   `PositionLost`.
//! - **teardown** drops the slot and the publication.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod catalog;
mod check;
mod config;
mod conn;
mod convert;
mod error;
mod ident;
mod lsn;
mod pgoutput;
mod source;

use meili_connect_core::{
    Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};

pub use config::{PostgresConfig, SslMode};
pub use source::PostgresSource;

/// Builds [`PostgresSource`]s for `postgres` and `supabase` specs.
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresSourceFactory;

impl PostgresSourceFactory {
    pub fn new() -> Self {
        Self
    }

    /// Typed constructor, for callers that want the concrete type.
    pub fn build_source(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<PostgresSource, SourceError> {
        if !matches!(spec.kind, SourceKind::Postgres | SourceKind::Supabase) {
            return Err(SourceError::Fatal(format!(
                "the postgres connector cannot build a {} source",
                spec.kind.as_str()
            )));
        }
        let config = PostgresConfig::from_value(&spec.config)?;
        let settings = config.resolve(spec.kind, sync_id)?;
        let ca = conn::CaFile::new(settings.conn.ca_cert.as_deref())?;
        let collections: Vec<String> = streams.iter().map(|s| s.collection.clone()).collect();
        Ok(PostgresSource::new(spec.kind, settings, ca, &collections))
    }
}

impl SourceFactory for PostgresSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(self.build_source(sync_id, spec, streams)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapping(collection: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": collection, "index": "i"})).unwrap()
    }

    #[test]
    fn factory_builds_both_kinds() {
        let f = PostgresSourceFactory::new();
        for kind in [SourceKind::Postgres, SourceKind::Supabase] {
            let spec = SourceSpec {
                kind,
                config: json!({"host": "db.abc.supabase.co", "user": "postgres", "password": "pw"}),
            };
            let src = f
                .build_source("s1", &spec, &[mapping("public.items")])
                .unwrap();
            let caps = src.capabilities();
            assert!(caps.snapshot && caps.patches);
            assert!(!format!("{src:?}").contains("pw"));
        }
    }

    #[test]
    fn factory_rejects_other_kinds_and_poolers() {
        let f = PostgresSourceFactory::new();
        let spec = SourceSpec {
            kind: SourceKind::Mysql,
            config: json!({}),
        };
        assert!(f.build("s", &spec, &[]).is_err());
        let spec = SourceSpec {
            kind: SourceKind::Supabase,
            config: json!({"url": "postgres://postgres.ref:pw@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"}),
        };
        assert!(matches!(
            f.build("s", &spec, &[]),
            Err(SourceError::Config(_))
        ));
    }
}
