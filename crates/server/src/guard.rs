//! The host policy and the error surface around the connectors.
//!
//! - [`GuardedSources`] wraps the connector registry: every source it
//!   builds vets the connector's endpoints ([`SourceFactory::endpoints`])
//!   against the [`HostPolicy`] before its first call that may connect
//!   (check, discover, open, teardown). The engine builds a new source for
//!   every attempt, so a sync is vetted again on every (re)start, and a
//!   name that started resolving somewhere forbidden stops the sync
//!   (`Config` → held).
//! - In coarse mode (syncs of a Lab tenant, calls of the Lab) the errors of
//!   the source and of the sink are reduced to their class and a fixed
//!   description: no driver cause chain, nothing a remote server sent. The
//!   policy's own refusals stay as they are (they name the host the tenant
//!   gave, nothing else). The full error is logged for the operator.

use std::sync::Arc;

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, ApplyReport, Capabilities, CheckReport, Checkpoint, CollectionInfo, DocOp,
    Endpoint, EventSender, HostPolicy, HostPolicyError, IndexTarget, Opened, Sink, SinkError,
    Source, SourceError, SourceFactory, SourceSpec, StreamMapping,
};
use tokio_util::sync::CancellationToken;

use crate::metrics::Metrics;

/// Why a source spec did not pass the policy.
#[derive(Debug)]
pub enum VetError {
    /// The policy refused one of the endpoints.
    Policy(HostPolicyError),
    /// The connector could not tell its endpoints (bad config, SRV lookup).
    Source(SourceError),
}

/// Vet every endpoint of `spec` as a source origin.
pub async fn vet_source(
    factory: &dyn SourceFactory,
    policy: &HostPolicy,
    spec: &SourceSpec,
) -> Result<Vec<Endpoint>, VetError> {
    let endpoints = factory.endpoints(spec).await.map_err(VetError::Source)?;
    for endpoint in &endpoints {
        policy
            .vet_origin(endpoint)
            .await
            .map_err(VetError::Policy)?;
    }
    Ok(endpoints)
}

/// Vet a Meilisearch URL. A refusal names the host.
pub async fn vet_destination(policy: &HostPolicy, url: &str) -> Result<(), String> {
    if policy.is_any() {
        return Ok(());
    }
    let (host, port) = url_host_port(url).ok_or("destination.url has no host")?;
    policy
        .resolve(&host, port)
        .await
        .map(|_| ())
        .map_err(|e| format!("destination.url: {e}"))
}

/// `http(s)://[user@]host[:port]/…` → host (IPv6 without brackets) and port.
fn url_host_port(url: &str) -> Option<(String, u16)> {
    let (scheme, rest) = url.trim().split_once("://")?;
    let default = match scheme.to_ascii_lowercase().as_str() {
        "http" => 80,
        "https" => 443,
        _ => return None,
    };
    let authority = rest.split(['/', '?', '#']).next()?;
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let ep = Endpoint::parse(host_port, default)?;
    Some((ep.host, ep.port))
}

// ------------------------------------------------------------ coarse errors

const SOURCE_UNREACHABLE: &str =
    "the source could not be reached or did not answer in time; retrying";
const SOURCE_POSITION_LOST: &str = "the source no longer holds the saved position";
const SOURCE_CONFIG: &str = "the source refused the connection or the configuration (address, \
     credentials, privileges or prerequisites); run the source check for details";
const SOURCE_PAUSED: &str = "the source is paused";
const SOURCE_FATAL: &str = "the source returned an unexpected error";

const SINK_UNREACHABLE: &str = "Meilisearch could not be reached or did not answer in time";
const SINK_CONFIG: &str =
    "Meilisearch refused the request: check the URL, the API key and the index primary key";
const SINK_FATAL: &str = "Meilisearch rejected the request";

/// The class of a source error with a fixed description.
pub fn coarse_source_error(e: SourceError) -> SourceError {
    tracing::debug!(error = %e, "source error (reported coarsely)");
    match e {
        SourceError::Transient(_) => SourceError::Transient(SOURCE_UNREACHABLE.into()),
        SourceError::PositionLost(_) => SourceError::PositionLost(SOURCE_POSITION_LOST.into()),
        SourceError::Config(_) => SourceError::Config(SOURCE_CONFIG.into()),
        SourceError::Paused(_) => SourceError::Paused(SOURCE_PAUSED.into()),
        SourceError::Fatal(_) => SourceError::Fatal(SOURCE_FATAL.into()),
    }
}

/// The class of a sink error with a fixed description.
pub fn coarse_sink_error(e: SinkError) -> SinkError {
    tracing::debug!(error = %e, "Meilisearch error (reported coarsely)");
    match e {
        SinkError::Transient(_) => SinkError::Transient(SINK_UNREACHABLE.into()),
        SinkError::Config(_) => SinkError::Config(SINK_CONFIG.into()),
        SinkError::Fatal(_) => SinkError::Fatal(SINK_FATAL.into()),
    }
}

// ------------------------------------------------------------ sources

/// The registry behind the host policy. `coarse` reduces the errors of the
/// sources it builds (see the module documentation).
#[derive(Clone)]
pub struct GuardedSources {
    inner: Arc<dyn SourceFactory>,
    policy: Arc<HostPolicy>,
    coarse: bool,
    metrics: Option<Arc<Metrics>>,
}

impl GuardedSources {
    pub fn new(inner: Arc<dyn SourceFactory>, policy: Arc<HostPolicy>, coarse: bool) -> Self {
        Self {
            inner,
            policy,
            coarse,
            metrics: None,
        }
    }

    /// Count the errors of the sources it builds (`connect_source_errors_total`).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    fn wrap(&self, inner: Box<dyn Source>, spec: &SourceSpec, vetted: bool) -> Box<dyn Source> {
        Box::new(GuardedSource {
            inner,
            factory: Arc::clone(&self.inner),
            policy: Arc::clone(&self.policy),
            spec: spec.clone(),
            coarse: self.coarse,
            vetted,
            metrics: self.metrics.clone(),
        })
    }

    /// A source whose endpoints the caller has just vetted.
    pub fn build_vetted(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        let inner = self.inner.build(sync_id, spec, streams)?;
        Ok(self.wrap(inner, spec, true))
    }
}

#[async_trait]
impl SourceFactory for GuardedSources {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        // Building connects to nothing; the first call that may connect
        // vets the endpoints (`build` cannot wait on a DNS lookup).
        let inner = self.inner.build(sync_id, spec, streams)?;
        Ok(self.wrap(inner, spec, false))
    }

    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        self.inner.endpoints(spec).await
    }
}

struct GuardedSource {
    inner: Box<dyn Source>,
    factory: Arc<dyn SourceFactory>,
    policy: Arc<HostPolicy>,
    spec: SourceSpec,
    coarse: bool,
    vetted: bool,
    metrics: Option<Arc<Metrics>>,
}

impl GuardedSource {
    fn surface(&self, e: SourceError) -> SourceError {
        if let Some(m) = &self.metrics {
            m.source_error(self.spec.kind.as_str(), &e);
        }
        if self.coarse {
            coarse_source_error(e)
        } else {
            e
        }
    }

    async fn ensure_vetted(&mut self) -> Result<(), SourceError> {
        if self.vetted {
            return Ok(());
        }
        match vet_source(&*self.factory, &self.policy, &self.spec).await {
            Ok(_) => {
                self.vetted = true;
                Ok(())
            }
            Err(VetError::Policy(e)) => {
                let e = SourceError::Config(e.to_string());
                if let Some(m) = &self.metrics {
                    m.source_error(self.spec.kind.as_str(), &e);
                }
                Err(e)
            }
            Err(VetError::Source(e)) => Err(self.surface(e)),
        }
    }
}

#[async_trait]
impl Source for GuardedSource {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.check().await;
        r.map_err(|e| self.surface(e))
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.discover().await;
        r.map_err(|e| self.surface(e))
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.open(resume).await;
        r.map_err(|e| self.surface(e))
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.snapshot(events, cancel).await;
        r.map_err(|e| self.surface(e))
    }

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.stream(events, acks, cancel).await;
        r.map_err(|e| self.surface(e))
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        self.ensure_vetted().await?;
        let r = self.inner.teardown().await;
        r.map_err(|e| self.surface(e))
    }
}

// ------------------------------------------------------------ sink

/// A sink whose errors are reduced to their class.
pub struct CoarseSink(pub Arc<dyn Sink>);

#[async_trait]
impl Sink for CoarseSink {
    async fn ensure_index(&self, target: &IndexTarget) -> Result<(), SinkError> {
        self.0.ensure_index(target).await.map_err(coarse_sink_error)
    }

    async fn apply(&self, target: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError> {
        self.0.apply(target, ops).await.map_err(coarse_sink_error)
    }

    async fn swap(&self, a: &str, b: &str) -> Result<(), SinkError> {
        self.0.swap(a, b).await.map_err(coarse_sink_error)
    }

    async fn delete_all_documents(&self, uid: &str) -> Result<(), SinkError> {
        self.0
            .delete_all_documents(uid)
            .await
            .map_err(coarse_sink_error)
    }

    async fn delete_index(&self, uid: &str) -> Result<(), SinkError> {
        self.0.delete_index(uid).await.map_err(coarse_sink_error)
    }

    async fn copy_settings(&self, from: &str, to: &str) -> Result<(), SinkError> {
        self.0
            .copy_settings(from, to)
            .await
            .map_err(coarse_sink_error)
    }

    async fn document_count(&self, uid: &str) -> Result<Option<u64>, SinkError> {
        self.0.document_count(uid).await.map_err(coarse_sink_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_split_into_host_and_port() {
        assert_eq!(
            url_host_port("https://Meili.example.com/prefix"),
            Some(("meili.example.com".into(), 443))
        );
        assert_eq!(
            url_host_port("http://u@[::1]:7700"),
            Some(("::1".into(), 7700))
        );
        assert_eq!(url_host_port("http://h"), Some(("h".into(), 80)));
        assert_eq!(url_host_port("ftp://h"), None);
    }

    #[test]
    fn coarse_errors_keep_the_class_and_drop_the_text() {
        let e = coarse_source_error(SourceError::Transient(
            "connect to 10.0.0.5:5432: connection refused".into(),
        ));
        assert!(matches!(e, SourceError::Transient(_)));
        assert!(!e.to_string().contains("10.0.0.5"));
        let e = coarse_sink_error(SinkError::Fatal("HTTP 500: <html>".into()));
        assert!(matches!(e, SinkError::Fatal(_)));
        assert!(!e.to_string().contains("html"));
    }
}
