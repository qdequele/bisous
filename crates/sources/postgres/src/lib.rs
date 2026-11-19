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
//!
//! Ownership: the publication we create carries the comment
//! `meili-connect:<sync_id>`; a slot is ours when it is a pgoutput slot of
//! this database named after the sync id or paired with a publication of
//! ours. An existing object that is not ours is never altered, reused or
//! dropped: `check` fails and `open` returns `Config` (the name is taken).
//!
//! Postgres 14 or later: `pgwire-replication` always asks pgoutput for
//! `messages`, which older servers reject at `START_REPLICATION`.

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
    Endpoint, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
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

#[async_trait::async_trait]
impl SourceFactory for PostgresSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(self.build_source(sync_id, spec, streams)?))
    }

    /// The one host and port the connection and the replication session
    /// go to (the discrete fields win over the URL's, as in `build`).
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        endpoints(spec)
    }
}

fn endpoints(spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
    if !matches!(spec.kind, SourceKind::Postgres | SourceKind::Supabase) {
        return Err(SourceError::Config(format!(
            "the postgres connector cannot read a {} config",
            spec.kind.as_str()
        )));
    }
    let settings = PostgresConfig::from_value(&spec.config)?.resolve(spec.kind, "endpoints")?;
    let host = settings.conn.host.trim();
    // libpq-style paths are Unix sockets and lists are several servers:
    // neither can be vetted as one network address.
    if host.starts_with('/') || host.starts_with('@') {
        return Err(SourceError::Config(
            "postgres source: Unix sockets are not supported, give a host name or address".into(),
        ));
    }
    if host.contains(',') {
        return Err(SourceError::Config(
            "postgres source: give one host, not a list".into(),
        ));
    }
    Ok(vec![Endpoint::new(host, settings.conn.port)])
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

    /// A project ref that does not resolve: a typo or a deleted project
    /// before the sync ever ran needs the user, not a silent "paused".
    #[tokio::test]
    async fn unknown_supabase_host_is_config_before_the_first_run() {
        let spec = SourceSpec {
            kind: SourceKind::Supabase,
            config: json!({"host": "db.zzzzzzzzzzzzzzzzzzzzqq.supabase.co",
                           "user": "postgres", "password": "pw"}),
        };
        let f = PostgresSourceFactory::new();
        let mut src = f
            .build_source("s1", &spec, &[mapping("public.items")])
            .unwrap();
        let report = src.check().await.unwrap();
        assert_eq!(
            report.items[0].status,
            meili_connect_core::CheckStatus::Failed,
            "{report:#?}"
        );
        let err = src.open(None).await.unwrap_err();
        assert!(matches!(err, SourceError::Config(_)), "{err:?}");
        // A sync that already ran: the same failure is a paused project.
        let resume = meili_connect_core::Checkpoint(json!({"lsn": "0/16B3748"}));
        let err = src.open(Some(resume)).await.unwrap_err();
        assert!(matches!(err, SourceError::Paused(_)), "{err:?}");
    }

    #[tokio::test]
    async fn endpoints_follow_the_url_and_the_discrete_fields() {
        let f = PostgresSourceFactory::new();
        let ep = |config: serde_json::Value| {
            let spec = SourceSpec {
                kind: SourceKind::Postgres,
                config,
            };
            async move { f.endpoints(&spec).await }
        };
        assert_eq!(
            ep(json!({"url": "postgres://u:pw@Db.Example.com:6543/app"}))
                .await
                .unwrap(),
            vec![Endpoint::new("db.example.com", 6543)]
        );
        assert_eq!(
            ep(json!({"url": "postgres://u:pw@[::1]/app"}))
                .await
                .unwrap(),
            vec![Endpoint::new("::1", 5432)]
        );
        // The discrete host overrides the URL's: the vetted host is the
        // one connected to.
        assert_eq!(
            ep(json!({"url": "postgres://u:pw@public.example.com/app", "host": "10.0.0.1"}))
                .await
                .unwrap(),
            vec![Endpoint::new("10.0.0.1", 5432)]
        );
        assert_eq!(
            ep(json!({"url": "postgres://u:pw@x/app?host=169.254.169.254&port=80"}))
                .await
                .unwrap(),
            vec![Endpoint::new("169.254.169.254", 80)]
        );
        for bad in [
            json!({"host": "/var/run/postgresql", "user": "u"}),
            json!({"host": "a,b", "user": "u"}),
            json!({"user": "u"}),
        ] {
            assert!(
                matches!(ep(bad.clone()).await, Err(SourceError::Config(_))),
                "{bad}"
            );
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
