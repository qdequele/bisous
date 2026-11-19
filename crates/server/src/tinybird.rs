//! Usage analytics in Tinybird (optional: `TINYBIRD_URL` + `TINYBIRD_TOKEN`).
//!
//! The same usage the Lab is sent, as rows of the `connect_usage` data
//! source (`tinybird/`), from the outbox's own `tinybird` lane: leased,
//! backed off and pruned on its own, so the Lab failing never holds
//! Tinybird back and the other way round. At least once: a row is marked
//! sent only once Tinybird answered; `event_id` is the outbox row id (stable
//! across retries) and the data source deduplicates on it.
//!
//! `POST {TINYBIRD_URL}/v0/events?name=connect_usage&wait=true`, NDJSON,
//! `Authorization: Bearer <TINYBIRD_TOKEN>`. Quarantined rows (a schema
//! drift between this file and the data source) are not retried: they are
//! logged as an error and counted as `rejected`.
//!
//! `GET /usage` reads the `connect_tenant_usage` pipe with the same token.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::NaiveDate;
use meili_connect_core::{Secret, Usage};
use meili_connect_store::{LabOutbox, RetryBackoff};
use serde::Deserialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::lab::{LEASE, MAX_ACK_BYTES, RETENTION, RETRY, next_wait};
use crate::limits::LogLimiter;
use crate::metrics::Metrics;

/// The data source the rows go to (`tinybird/datasources`).
pub const DATASOURCE: &str = "connect_usage";
/// The endpoint `GET /usage` reads (`tinybird/pipes`).
pub const USAGE_PIPE: &str = "connect_tenant_usage";
/// Rows per request.
pub const BATCH_SIZE: u32 = 500;

/// `TINYBIRD_URL` + `TINYBIRD_TOKEN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TinybirdConfig {
    /// The workspace's regional API host, without trailing slash.
    pub url: String,
    /// Needs APPEND on `connect_usage` and READ on `connect_tenant_usage`.
    pub token: Secret,
}

impl TinybirdConfig {
    pub fn events_url(&self) -> String {
        format!("{}/v0/events?name={DATASOURCE}&wait=true", self.url)
    }

    pub fn pipe_url(&self) -> String {
        format!("{}/v0/pipes/{USAGE_PIPE}.json", self.url)
    }
}

fn client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(20))
        .build()
}

/// Outcome of one round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    Idle,
    Sent {
        accepted: usize,
        quarantined: usize,
    },
    /// Deferred and retried after its backoff.
    Failed {
        reason: &'static str,
        pending: usize,
    },
}

pub struct TinybirdSender {
    outbox: Arc<dyn LabOutbox>,
    config: TinybirdConfig,
    http: reqwest::Client,
    lease: Duration,
    retry: RetryBackoff,
    metrics: Option<Arc<Metrics>>,
}

#[derive(Debug, Default, Deserialize)]
struct Ack {
    #[serde(default)]
    successful_rows: u64,
    #[serde(default)]
    quarantined_rows: u64,
}

impl TinybirdSender {
    pub fn new(outbox: Arc<dyn LabOutbox>, config: TinybirdConfig) -> Result<Self, reqwest::Error> {
        Ok(Self {
            outbox,
            config,
            http: client()?,
            lease: LEASE,
            retry: RETRY,
            metrics: None,
        })
    }

    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_retry_backoff(mut self, retry: RetryBackoff) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    fn count(&self, result: &str, n: usize) {
        if let Some(m) = &self.metrics {
            m.tinybird_events(result, n as u64);
        }
    }

    async fn defer(&self, lease: Uuid, ids: &[Uuid]) {
        if let Err(e) = self.outbox.defer_events(lease, ids, self.retry).await {
            tracing::debug!(error = %e, "cannot defer undelivered Tinybird rows");
        }
    }

    pub async fn deliver_once(&self) -> Delivery {
        let lease = match self.outbox.lease_events(BATCH_SIZE, self.lease).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "cannot read the Tinybird outbox lane");
                return Delivery::Failed {
                    reason: "store",
                    pending: 0,
                };
            }
        };
        if lease.events.is_empty() {
            return Delivery::Idle;
        }
        let pending = lease.events.len();
        let ids: Vec<Uuid> = lease.events.iter().map(|e| e.id).collect();
        let mut body = Vec::new();
        for e in &lease.events {
            let mut row = e.payload.clone();
            if let Value::Object(map) = &mut row {
                map.insert("event_id".into(), Value::String(e.id.to_string()));
            }
            if serde_json::to_writer(&mut body, &row).is_err() {
                self.defer(lease.token, &ids).await;
                self.count("failed", pending);
                return Delivery::Failed {
                    reason: "malformed",
                    pending,
                };
            }
            body.push(b'\n');
        }
        let resp = self
            .http
            .post(self.config.events_url())
            .bearer_auth(self.config.token.expose())
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await;
        let failed = async |reason: &'static str| {
            self.defer(lease.token, &ids).await;
            self.count("failed", pending);
            Delivery::Failed { reason, pending }
        };
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() { "timeout" } else { "connect" };
                tracing::debug!(reason, "Tinybird unreachable; usage rows stay pending");
                return failed(reason).await;
            }
        };
        let status = resp.status();
        if !status.is_success() {
            tracing::debug!(%status, "Tinybird did not take the usage rows");
            let reason = match status.as_u16() {
                401 | 403 => "unauthorized",
                _ => "status",
            };
            return failed(reason).await;
        }
        let ack = match crate::lab::read_capped(resp, MAX_ACK_BYTES).await {
            // A 2xx without a readable body still stored the rows.
            Some(bytes) => serde_json::from_slice::<Ack>(&bytes).unwrap_or_default(),
            None => Ack::default(),
        };
        if let Err(e) = self.outbox.mark_events_sent(lease.token, &ids).await {
            // They are sent again; the data source deduplicates by event_id.
            tracing::warn!(error = %e, "cannot mark Tinybird rows as sent");
        }
        let quarantined = usize::try_from(ack.quarantined_rows)
            .unwrap_or(pending)
            .min(pending);
        if quarantined > 0 {
            tracing::error!(
                quarantined,
                successful = ack.successful_rows,
                "Tinybird quarantined usage rows: the connect_usage data source and the engine \
                 disagree on the schema (tinybird/README.md)"
            );
        }
        self.count("accepted", pending - quarantined);
        self.count("rejected", quarantined);
        Delivery::Sent {
            accepted: pending - quarantined,
            quarantined,
        }
    }

    /// Deliver every `interval` until `cancel`, draining while batches come
    /// back full; prune hourly; back off while deliveries fail.
    pub async fn run(self, interval: Duration, cancel: CancellationToken) {
        let mut last_prune: Option<Instant> = None;
        let mut failures: u32 = 0;
        let mut log = LogLimiter::new(Duration::from_secs(300));
        loop {
            for _ in 0..20 {
                match self.deliver_once().await {
                    Delivery::Sent {
                        accepted,
                        quarantined,
                    } if (accepted + quarantined) as u32 == BATCH_SIZE => failures = 0,
                    Delivery::Failed { reason, pending } => {
                        failures = failures.saturating_add(1);
                        if let Some(suppressed) = log.allow() {
                            tracing::warn!(
                                reason,
                                pending,
                                suppressed,
                                "usage rows could not be delivered to Tinybird; retrying with \
                                 backoff"
                            );
                        }
                        break;
                    }
                    _ => {
                        failures = 0;
                        log.reset();
                        break;
                    }
                }
            }
            if last_prune.is_none_or(|t| t.elapsed() >= Duration::from_secs(3600)) {
                match self.outbox.prune_sent_events(RETENTION).await {
                    Ok(n) if n > 0 => tracing::info!(pruned = n, "old Tinybird rows pruned"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "cannot prune sent Tinybird rows"),
                }
                last_prune = Some(Instant::now());
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(next_wait(interval, failures)) => {}
            }
        }
    }
}

/// One row of the `connect_tenant_usage` pipe.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PipeRow {
    pub day: NaiveDate,
    pub sync_id: String,
    #[serde(flatten)]
    pub usage: Usage,
}

#[derive(Debug, Deserialize)]
struct PipeAnswer {
    data: Vec<PipeRow>,
}

/// Reads `connect_tenant_usage` (rows per day and sync).
pub struct TinybirdReader {
    config: TinybirdConfig,
    http: reqwest::Client,
}

impl TinybirdReader {
    pub fn new(config: TinybirdConfig) -> Result<Self, reqwest::Error> {
        Ok(Self {
            config,
            http: client()?,
        })
    }

    /// The tenant's usage per day and sync (`""`: the global scope).
    pub async fn usage(
        &self,
        tenant: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<PipeRow>, String> {
        let resp = self
            .http
            .get(self.config.pipe_url())
            .bearer_auth(self.config.token.expose())
            .query(&[
                ("tenant_id", tenant.to_owned()),
                ("date_from", from.to_string()),
                ("date_to", to.to_string()),
            ])
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    "timeout".to_owned()
                } else {
                    "unreachable".to_owned()
                }
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("status {}", status.as_u16()));
        }
        let bytes = crate::lab::read_capped(resp, 16 * 1024 * 1024)
            .await
            .ok_or_else(|| "answer too large".to_owned())?;
        serde_json::from_slice::<PipeAnswer>(&bytes)
            .map(|a| a.data)
            .map_err(|_| "malformed answer".to_owned())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_store::{Lane, MemStore, PendingUsage, UsageLedger};
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(server: &MockServer) -> TinybirdConfig {
        TinybirdConfig {
            url: server.uri(),
            token: Secret::new("tb-token"),
        }
    }

    async fn seed(store: &MemStore, n: u64) -> Vec<Uuid> {
        let lane = store.outbox(Lane::Tinybird);
        let mut ids = Vec::new();
        for i in 0..n {
            let p = PendingUsage {
                sync_id: format!("s{i}"),
                tenant: None,
                source_kind: "postgres".into(),
                sync_name: "n".into(),
                usage: Usage {
                    documents_streamed: i + 1,
                    ..Usage::default()
                },
            };
            ids.push(
                lane.enqueue_event(None, &crate::usage::tinybird_row(&p))
                    .await
                    .unwrap(),
            );
        }
        ids
    }

    #[tokio::test]
    async fn rows_go_as_ndjson_with_their_event_id_and_are_marked_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/events"))
            .and(query_param("name", "connect_usage"))
            .and(query_param("wait", "true"))
            .and(header("authorization", "Bearer tb-token"))
            .and(header("content-type", "application/x-ndjson"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(
                    serde_json::json!({"successful_rows": 2, "quarantined_rows": 0}),
                ),
            )
            .mount(&server)
            .await;
        let store = Arc::new(MemStore::new());
        let ids = seed(&store, 2).await;
        let metrics = Arc::new(Metrics::new());
        let sender = TinybirdSender::new(store.outbox(Lane::Tinybird), config(&server))
            .unwrap()
            .with_metrics(Arc::clone(&metrics));
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Sent {
                accepted: 2,
                quarantined: 0
            }
        );
        assert_eq!(sender.deliver_once().await, Delivery::Idle);
        let req = &server.received_requests().await.unwrap()[0];
        let rows: Vec<Value> = String::from_utf8(req.body.clone())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["event_id"], ids[0].to_string());
        assert_eq!(rows[0]["tenant_id"], "");
        assert_eq!(rows[1]["documents_streamed"], 2);
        // The stable column names.
        let mut keys: Vec<&str> = rows[0]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "bytes",
                "documents_resync",
                "documents_snapshot",
                "documents_streamed",
                "event_id",
                "occurred_at",
                "operation",
                "source_kind",
                "sync_id",
                "sync_seconds",
                "tenant_id"
            ]
        );
        let text = metrics.render(&Default::default());
        assert!(text.contains("connect_tinybird_events_sent_total{result=\"accepted\"} 2"));
    }

    #[tokio::test]
    async fn a_failing_tinybird_keeps_the_rows_and_resends_the_same_ids() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"successful_rows": 0, "quarantined_rows": 1}),
                ),
            )
            .mount(&server)
            .await;
        let store = Arc::new(MemStore::new());
        seed(&store, 1).await;
        let sender = TinybirdSender::new(store.outbox(Lane::Tinybird), config(&server))
            .unwrap()
            .with_lease(Duration::from_millis(50))
            .with_retry_backoff(RetryBackoff {
                base: Duration::from_millis(50),
                max: Duration::from_millis(50),
            });
        assert!(matches!(
            sender.deliver_once().await,
            Delivery::Failed {
                reason: "status",
                pending: 1
            }
        ));
        assert_eq!(sender.deliver_once().await, Delivery::Idle, "backing off");
        tokio::time::sleep(Duration::from_millis(80)).await;
        // Quarantined: logged, counted, not retried.
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Sent {
                accepted: 0,
                quarantined: 1
            }
        );
        let received = server.received_requests().await.unwrap();
        assert_eq!(received[0].body, received[1].body, "same rows, same ids");
        assert_eq!(
            store.outbox(Lane::Tinybird).count_unsent().await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn the_usage_pipe_is_read_with_the_tenant_and_the_range() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v0/pipes/connect_tenant_usage.json"))
            .and(query_param("tenant_id", "team-a"))
            .and(query_param("date_from", "2026-11-01"))
            .and(query_param("date_to", "2026-11-19"))
            .and(header("authorization", "Bearer tb-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "meta": [],
                "data": [{"day": "2026-11-18", "sync_id": "s1", "documents_streamed": 4,
                          "documents_snapshot": 0, "documents_resync": 0, "bytes": 40,
                          "sync_seconds": 9}],
                "rows": 1
            })))
            .mount(&server)
            .await;
        let reader = TinybirdReader::new(config(&server)).unwrap();
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        let rows = reader
            .usage("team-a", d("2026-11-01"), d("2026-11-19"))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].usage.documents_streamed, 4);
        assert_eq!(rows[0].usage.sync_seconds, 9);
        server.reset().await;
        assert!(
            reader
                .usage("team-a", d("2026-11-01"), d("2026-11-19"))
                .await
                .is_err()
        );
    }
}
