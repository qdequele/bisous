//! Prometheus metrics: `GET /metrics`.
//!
//! The `prometheus` crate, with its own [`Registry`] per server (no global
//! recorder: tests run several servers in one process, and scrape-time
//! values such as the outbox backlog and the lag histogram are computed by
//! the handler from the live state, which a push-style recorder fits
//! badly). Text format 0.0.4.
//!
//! Labels never carry a tenant or a sync id (cardinality and privacy):
//! only phases, document kinds, error classes, source kinds and results.
//! The names are a contract with the deploy box's dashboards and alert
//! rules (`docs/LAB_INTEGRATION.md`, "Telemetry"): never rename one.
//!
//! Authentication: `Authorization: Bearer <METRICS_TOKEN>` only, compared
//! in constant time. Without `METRICS_TOKEN` the route answers 404; the
//! management tokens (Lab, admin) are refused (and may not equal it).

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use meili_connect_core::{
    ApplyReport, DocOp, IndexTarget, Sink, SinkError, SourceError, SyncPhase, Usage,
};
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use crate::auth::secret_eq;
use crate::error::ApiError;
use crate::state::AppState;

/// Buckets of `connect_batch_seconds`: one `apply` call.
const BATCH_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];
/// Buckets of `connect_lag_seconds`: how far behind the origin each sync is.
pub const LAG_BUCKETS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0, 21_600.0, 86_400.0,
];

pub const PHASES: [SyncPhase; 7] = [
    SyncPhase::Pending,
    SyncPhase::Snapshotting,
    SyncPhase::Streaming,
    SyncPhase::Paused,
    SyncPhase::SourcePaused,
    SyncPhase::Held,
    SyncPhase::Failed,
];

pub fn phase_name(p: SyncPhase) -> &'static str {
    match p {
        SyncPhase::Pending => "pending",
        SyncPhase::Snapshotting => "snapshotting",
        SyncPhase::Streaming => "streaming",
        SyncPhase::Paused => "paused",
        SyncPhase::SourcePaused => "source_paused",
        SyncPhase::Held => "held",
        SyncPhase::Failed => "failed",
    }
}

fn source_class(e: &SourceError) -> &'static str {
    match e {
        SourceError::Transient(_) => "transient",
        SourceError::PositionLost(_) => "position_lost",
        SourceError::Config(_) => "config",
        SourceError::Paused(_) => "paused",
        SourceError::Fatal(_) => "fatal",
    }
}

fn sink_class(e: &SinkError) -> &'static str {
    match e {
        SinkError::Transient(_) => "transient",
        SinkError::Config(_) => "config",
        SinkError::Fatal(_) => "fatal",
    }
}

/// The counters the server updates as things happen. Gauges and the lag
/// histogram are computed when scraped.
pub struct Metrics {
    registry: Registry,
    documents: IntCounterVec,
    bytes: IntCounter,
    sync_seconds: IntCounter,
    batch_seconds: Histogram,
    sink_errors: IntCounterVec,
    source_errors: IntCounterVec,
    dead_letters: IntCounter,
    lab_events_sent: IntCounterVec,
    tinybird_events_sent: IntCounterVec,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

fn register<T: prometheus::core::Collector + Clone + 'static>(
    registry: &Registry,
    c: T,
) -> prometheus::Result<T> {
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

fn counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> prometheus::Result<IntCounterVec> {
    register(registry, IntCounterVec::new(Opts::new(name, help), labels)?)
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// The definitions are static and their names unique, so building them
    /// cannot fail; a failure is a programming error caught by the tests.
    #[allow(clippy::expect_used)]
    pub fn new() -> Self {
        Self::try_new().expect("static metric definitions are valid")
    }

    fn try_new() -> prometheus::Result<Self> {
        let registry = Registry::new();
        let documents = counter_vec(
            &registry,
            "connect_documents_total",
            "Documents Meilisearch accepted, by kind (streamed, snapshot, resync).",
            &["kind"],
        )?;
        for kind in ["streamed", "snapshot", "resync"] {
            documents.with_label_values(&[kind]);
        }
        // `process_*` (memory, file descriptors), where the OS tells.
        #[cfg(target_os = "linux")]
        registry.register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))?;
        let bytes = register(
            &registry,
            IntCounter::new(
                "connect_bytes_total",
                "Uncompressed payload bytes Meilisearch accepted.",
            )?,
        )?;
        let sync_seconds = register(
            &registry,
            IntCounter::new(
                "connect_sync_seconds_total",
                "Seconds syncs spent snapshotting or streaming.",
            )?,
        )?;
        let batch_seconds = register(
            &registry,
            Histogram::with_opts(
                HistogramOpts::new(
                    "connect_batch_seconds",
                    "Time from a batch's first source event to Meilisearch's tasks succeeded.",
                )
                .buckets(BATCH_BUCKETS.to_vec()),
            )?,
        )?;
        let sink_errors = counter_vec(
            &registry,
            "connect_sink_errors_total",
            "Meilisearch errors, by class (transient, config, fatal).",
            &["class"],
        )?;
        let source_errors = counter_vec(
            &registry,
            "connect_source_errors_total",
            "Source errors of running syncs, by source kind and class.",
            &["kind", "class"],
        )?;
        let dead_letters = register(
            &registry,
            IntCounter::new(
                "connect_dead_letters_total",
                "Documents rejected by Meilisearch or skipped by a source.",
            )?,
        )?;
        let lab_events_sent = counter_vec(
            &registry,
            "connect_lab_events_sent_total",
            "Usage events sent to the Lab, by result (accepted, rejected: answered but not \
             accepted, retried later; failed: the batch did not go through).",
            &["result"],
        )?;
        let tinybird_events_sent = counter_vec(
            &registry,
            "connect_tinybird_events_sent_total",
            "Usage rows sent to Tinybird, by result (accepted, rejected: quarantined; failed).",
            &["result"],
        )?;
        // Every series exists from the start (at 0): a dashboard or an
        // alert never sees "no data" for a class that simply did not occur.
        for class in ["transient", "config", "fatal"] {
            sink_errors.with_label_values(&[class]);
        }
        for kind in [
            "postgres",
            "supabase",
            "mysql",
            "mongodb",
            "kafka",
            "dynamodb",
            "planetscale",
        ] {
            for class in ["transient", "position_lost", "config", "paused", "fatal"] {
                source_errors.with_label_values(&[kind, class]);
            }
        }
        for result in ["accepted", "rejected", "failed"] {
            lab_events_sent.with_label_values(&[result]);
            tinybird_events_sent.with_label_values(&[result]);
        }
        Ok(Self {
            registry,
            documents,
            bytes,
            sync_seconds,
            batch_seconds,
            sink_errors,
            source_errors,
            dead_letters,
            lab_events_sent,
            tinybird_events_sent,
        })
    }

    /// One applied batch: from its first event to Meilisearch's tasks
    /// succeeded.
    pub fn batch(&self, age: std::time::Duration) {
        self.batch_seconds.observe(age.as_secs_f64());
    }

    pub fn record_usage(&self, usage: &Usage) {
        for (kind, n) in [
            ("streamed", usage.documents_streamed),
            ("snapshot", usage.documents_snapshot),
            ("resync", usage.documents_resync),
        ] {
            if n > 0 {
                self.documents.with_label_values(&[kind]).inc_by(n);
            }
        }
        self.bytes.inc_by(usage.bytes);
        self.sync_seconds.inc_by(usage.sync_seconds);
    }

    pub fn dead_letters(&self, n: u64) {
        self.dead_letters.inc_by(n);
    }

    pub fn source_error(&self, kind: &str, e: &SourceError) {
        self.source_errors
            .with_label_values(&[kind, source_class(e)])
            .inc();
    }

    pub fn sink_error(&self, e: &SinkError) {
        self.sink_errors.with_label_values(&[sink_class(e)]).inc();
    }

    /// `result`: `accepted`, `rejected` or `failed` (the deploy box's
    /// alert rules use these values).
    pub fn lab_events(&self, result: &str, n: u64) {
        if n > 0 {
            self.lab_events_sent.with_label_values(&[result]).inc_by(n);
        }
    }

    /// `result`: `accepted`, `rejected` or `failed`.
    pub fn tinybird_events(&self, result: &str, n: u64) {
        if n > 0 {
            self.tinybird_events_sent
                .with_label_values(&[result])
                .inc_by(n);
        }
    }

    /// The registry's metrics plus the scrape-time ones, in text format.
    pub fn render(&self, scrape: &Scrape) -> String {
        let mut families = self.registry.gather();
        let extra = Registry::new();
        let syncs = IntGaugeVec::new(Opts::new("connect_syncs", "Syncs by phase."), &["phase"]);
        if let Ok(syncs) = syncs {
            for phase in PHASES {
                let n = scrape
                    .phases
                    .iter()
                    .find(|(p, _)| *p == phase)
                    .map_or(0, |(_, n)| *n);
                syncs
                    .with_label_values(&[phase_name(phase)])
                    .set(i64::try_from(n).unwrap_or(i64::MAX));
            }
            let _ = extra.register(Box::new(syncs));
        }
        for (name, help, value) in [
            (
                "connect_outbox_backlog",
                "Usage events not delivered to the Lab yet.",
                scrape.lab_backlog,
            ),
            (
                "connect_tinybird_backlog",
                "Usage rows not delivered to Tinybird yet.",
                scrape.tinybird_backlog,
            ),
        ] {
            if let (Some(value), Ok(g)) = (value, prometheus::IntGauge::new(name, help)) {
                g.set(i64::try_from(value).unwrap_or(i64::MAX));
                let _ = extra.register(Box::new(g));
            }
        }
        if let Ok(lag) = Histogram::with_opts(
            HistogramOpts::new(
                "connect_lag_seconds",
                "How far behind its origin each running sync is (one observation per sync, \
                 computed at scrape time).",
            )
            .buckets(LAG_BUCKETS.to_vec()),
        ) {
            for seconds in &scrape.lags {
                lag.observe(*seconds);
            }
            let _ = extra.register(Box::new(lag));
        }
        families.extend(extra.gather());
        families.sort_by(|a, b| a.name().cmp(b.name()));
        let mut out = Vec::new();
        if let Err(e) = TextEncoder::new().encode(&families, &mut out) {
            tracing::error!(error = %e, "cannot encode the metrics");
        }
        String::from_utf8(out).unwrap_or_default()
    }
}

/// What `/metrics` reads from the live state when scraped.
#[derive(Debug, Default)]
pub struct Scrape {
    pub phases: Vec<(SyncPhase, u64)>,
    /// `None` when the store could not tell (the gauge is left out).
    pub lab_backlog: Option<u64>,
    pub tinybird_backlog: Option<u64>,
    /// Lag of each running sync that can tell, in seconds.
    pub lags: Vec<f64>,
}

/// `GET /metrics`.
pub async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(expected) = &state.metrics_token else {
        return ApiError::not_found("metrics are not enabled on this server: set METRICS_TOKEN")
            .into_response();
    };
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, t)| t.trim());
    if !token.is_some_and(|t| secret_eq(expected.expose(), t)) {
        return ApiError::unauthorized("missing or invalid metrics token").into_response();
    }
    let scrape = state.scrape().await;
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)],
        state.metrics.render(&scrape),
    )
        .into_response()
}

/// A sink that counts its errors by class.
pub struct InstrumentedSink {
    inner: Arc<dyn Sink>,
    metrics: Arc<Metrics>,
}

impl InstrumentedSink {
    pub fn new(inner: Arc<dyn Sink>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }

    fn count<T>(&self, r: Result<T, SinkError>) -> Result<T, SinkError> {
        if let Err(e) = &r {
            self.metrics.sink_error(e);
        }
        r
    }
}

#[async_trait::async_trait]
impl Sink for InstrumentedSink {
    async fn ensure_index(&self, target: &IndexTarget) -> Result<(), SinkError> {
        let r = self.inner.ensure_index(target).await;
        self.count(r)
    }

    async fn apply(&self, target: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError> {
        let r = self.inner.apply(target, ops).await;
        self.count(r)
    }

    async fn swap(&self, a: &str, b: &str) -> Result<(), SinkError> {
        let r = self.inner.swap(a, b).await;
        self.count(r)
    }

    async fn delete_all_documents(&self, uid: &str) -> Result<(), SinkError> {
        let r = self.inner.delete_all_documents(uid).await;
        self.count(r)
    }

    async fn delete_index(&self, uid: &str) -> Result<(), SinkError> {
        let r = self.inner.delete_index(uid).await;
        self.count(r)
    }

    async fn copy_settings(&self, from: &str, to: &str) -> Result<(), SinkError> {
        let r = self.inner.copy_settings(from, to).await;
        self.count(r)
    }

    async fn document_count(&self, uid: &str) -> Result<Option<u64>, SinkError> {
        let r = self.inner.document_count(uid).await;
        self.count(r)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn every_documented_name_is_rendered_without_identifying_labels() {
        let m = Metrics::new();
        m.record_usage(&Usage {
            documents_streamed: 3,
            documents_snapshot: 5,
            documents_resync: 0,
            bytes: 800,
            sync_seconds: 2,
        });
        m.dead_letters(1);
        m.source_error("postgres", &SourceError::Transient("x".into()));
        m.sink_error(&SinkError::Config("x".into()));
        m.lab_events("accepted", 4);
        m.batch(std::time::Duration::from_millis(30));
        let text = m.render(&Scrape {
            phases: vec![(SyncPhase::Streaming, 2), (SyncPhase::Held, 1)],
            lab_backlog: Some(7),
            tinybird_backlog: Some(0),
            lags: vec![0.5, 120.0],
        });
        for line in [
            "connect_syncs{phase=\"streaming\"} 2",
            "connect_syncs{phase=\"held\"} 1",
            "connect_syncs{phase=\"failed\"} 0",
            "connect_documents_total{kind=\"streamed\"} 3",
            "connect_documents_total{kind=\"snapshot\"} 5",
            "connect_documents_total{kind=\"resync\"} 0",
            "connect_bytes_total 800",
            "connect_sink_errors_total{class=\"config\"} 1",
            "connect_source_errors_total{class=\"transient\",kind=\"postgres\"} 1",
            "connect_dead_letters_total 1",
            "connect_outbox_backlog 7",
            "connect_lab_events_sent_total{result=\"accepted\"} 4",
            "connect_batch_seconds_count 1",
            "connect_lag_seconds_count 2",
            "connect_lag_seconds_bucket{le=\"1\"} 1",
            "# TYPE connect_batch_seconds histogram",
        ] {
            assert!(text.contains(line), "missing {line:?} in\n{text}");
        }
        assert!(!text.contains("tenant") && !text.contains("sync_id"));
    }
}
