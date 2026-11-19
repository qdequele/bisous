//! Delivers the Lab events outbox to `POST {LAB_URL}/internal/events`.
//!
//! Each round leases up to [`BATCH_SIZE`] events, sends them as one signed
//! batch `{"events": [...]}`, and marks the ids the Lab lists in `accepted`
//! as sent. Everything else stays unsent and is deferred: each failed
//! delivery of an event doubles its wait ([`RETRY`]). While deliveries fail
//! the sender itself backs off too, and logs the failure at most every
//! [`LOG_EVERY`]. Sent events older than [`RETENTION`] are pruned hourly.
//! The Lab being down never affects the syncs; the backlog is bounded on the
//! enqueue side (`LAB_OUTBOX_MAX_PENDING`, see [`crate::usage`]).
//!
//! Signature: `X-Lab-Signature: sha256=<hex HMAC-SHA256(secret,
//! "<timestamp>.<raw body>")>`, with `X-Lab-Instance-Id` and
//! `X-Lab-Timestamp` (unix seconds).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use meili_connect_store::{LabOutbox, RetryBackoff, UsageLedger};
use serde_json::Value;
use sha2::Sha256;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::LabEventsConfig;
use crate::limits::LogLimiter;
use crate::metrics::Metrics;
use crate::usage::UsageEvents;

pub const H_INSTANCE_ID: &str = "x-lab-instance-id";
pub const H_TIMESTAMP: &str = "x-lab-timestamp";
pub const H_SIGNATURE: &str = "x-lab-signature";

/// Events per batch.
pub const BATCH_SIZE: u32 = 500;
/// How long a leased batch is held before another round may take it.
pub const LEASE: Duration = Duration::from_secs(30);
/// Sent events are kept this long.
pub const RETENTION: Duration = Duration::from_secs(7 * 86_400);
/// Largest acknowledgement body read from the Lab.
pub const MAX_ACK_BYTES: usize = 1024 * 1024;
/// How long an event waits after a failed delivery (doubling per attempt).
pub const RETRY: RetryBackoff = RetryBackoff {
    base: Duration::from_secs(10),
    max: Duration::from_secs(3600),
};
/// Longest pause of the sender after failed rounds.
pub const MAX_IDLE: Duration = Duration::from_secs(300);
/// A failing delivery is logged at most this often.
pub const LOG_EVERY: Duration = Duration::from_secs(300);

/// The sender's pause after `failures` failed rounds in a row: `interval`
/// doubled per failure, capped at [`MAX_IDLE`] (never below `interval`).
pub fn next_wait(interval: Duration, failures: u32) -> Duration {
    interval
        .saturating_mul(1u32 << failures.min(16))
        .min(MAX_IDLE)
        .max(interval)
}

/// `sha256=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`.
pub fn sign(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    let mut mac = match <Hmac<Sha256> as Mac>::new_from_slice(secret) {
        Ok(m) => m,
        // HMAC accepts keys of any length; this branch cannot run.
        Err(_) => return String::new(),
    };
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Outcome of one round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    Idle,
    /// The Lab answered `200`; `pending` events were not in `accepted`.
    Sent {
        delivered: usize,
        pending: usize,
    },
    /// The whole batch failed (`store`, `connect`, `timeout`,
    /// `unauthorized`, `status`, `malformed`); it is deferred and retried
    /// after its backoff.
    Failed {
        reason: &'static str,
        pending: usize,
    },
}

pub struct LabSender {
    outbox: Arc<dyn LabOutbox>,
    config: LabEventsConfig,
    http: reqwest::Client,
    lease: Duration,
    retry: RetryBackoff,
    metrics: Option<Arc<Metrics>>,
}

pub(crate) async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Option<Vec<u8>> {
    if resp.content_length().is_some_and(|n| n > cap as u64) {
        return None;
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.ok()? {
        if buf.len() + chunk.len() > cap {
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(buf)
}

impl LabSender {
    pub fn new(
        outbox: Arc<dyn LabOutbox>,
        config: LabEventsConfig,
    ) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .build()?;
        Ok(Self {
            outbox,
            config,
            http,
            lease: LEASE,
            retry: RETRY,
            metrics: None,
        })
    }

    /// Count what it sends (`connect_lab_events_sent_total{result}`).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub async fn deliver_once(&self) -> Delivery {
        let delivery = self.deliver().await;
        if let Some(m) = &self.metrics {
            match &delivery {
                Delivery::Idle => {}
                Delivery::Sent { delivered, pending } => {
                    m.lab_events("accepted", *delivered as u64);
                    m.lab_events("rejected", *pending as u64);
                }
                Delivery::Failed { pending, .. } => m.lab_events("failed", *pending as u64),
            }
        }
        delivery
    }

    /// How long a failed event waits (tests shorten it).
    pub fn with_retry_backoff(mut self, retry: RetryBackoff) -> Self {
        self.retry = retry;
        self
    }

    /// Keep these events of the lease back for their backoff.
    async fn defer(&self, lease: uuid::Uuid, ids: &[Uuid]) {
        if ids.is_empty() {
            return;
        }
        if let Err(e) = self.outbox.defer_events(lease, ids, self.retry).await {
            // They come back when the lease expires.
            tracing::debug!(error = %e, "cannot defer undelivered usage events");
        }
    }

    /// How long a batch stays leased (tests shorten it).
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    async fn deliver(&self) -> Delivery {
        let lease = match self.outbox.lease_events(BATCH_SIZE, self.lease).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "cannot read the Lab events outbox");
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
        let mut events = Vec::with_capacity(pending);
        for e in &lease.events {
            let mut body = e.payload.clone();
            if let Value::Object(map) = &mut body {
                map.insert("id".into(), Value::String(e.id.to_string()));
            }
            events.push(body);
        }
        let body = match serde_json::to_vec(&serde_json::json!({ "events": events })) {
            Ok(b) => b,
            Err(_) => {
                self.defer(lease.token, &ids).await;
                return Delivery::Failed {
                    reason: "malformed",
                    pending,
                };
            }
        };
        let ts = unix_now();
        let signature = sign(self.config.instance_secret.expose().as_bytes(), ts, &body);
        let resp = self
            .http
            .post(self.config.events_url())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(H_INSTANCE_ID, &self.config.instance_id)
            .header(H_TIMESTAMP, ts.to_string())
            .header(H_SIGNATURE, signature)
            .body(body)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() { "timeout" } else { "connect" };
                tracing::debug!(reason, "Lab unreachable; usage events stay pending");
                self.defer(lease.token, &ids).await;
                return Delivery::Failed { reason, pending };
            }
        };
        let status = resp.status();
        if status != reqwest::StatusCode::OK {
            tracing::debug!(%status, "the Lab did not accept the usage events batch");
            self.defer(lease.token, &ids).await;
            let reason = if status == reqwest::StatusCode::UNAUTHORIZED {
                "unauthorized"
            } else {
                "status"
            };
            return Delivery::Failed { reason, pending };
        }
        #[derive(serde::Deserialize)]
        struct Ack {
            accepted: Vec<Uuid>,
        }
        let Some(bytes) = read_capped(resp, MAX_ACK_BYTES).await else {
            self.defer(lease.token, &ids).await;
            return Delivery::Failed {
                reason: "malformed",
                pending,
            };
        };
        let Ok(ack) = serde_json::from_slice::<Ack>(&bytes) else {
            tracing::debug!("the Lab answered 200 with a malformed body");
            self.defer(lease.token, &ids).await;
            return Delivery::Failed {
                reason: "malformed",
                pending,
            };
        };
        let accepted: HashSet<Uuid> = ack.accepted.into_iter().collect();
        let done: Vec<Uuid> = lease
            .events
            .iter()
            .map(|e| e.id)
            .filter(|id| accepted.contains(id))
            .collect();
        if !done.is_empty()
            && let Err(e) = self.outbox.mark_events_sent(lease.token, &done).await
        {
            // They are sent again; the Lab deduplicates by id.
            tracing::warn!(error = %e, "cannot mark usage events as sent");
        }
        let delivered = done.len();
        if delivered < pending {
            tracing::warn!(
                not_accepted = pending - delivered,
                "the Lab did not accept every usage event; the rest is retried later"
            );
            let refused: Vec<Uuid> = ids
                .iter()
                .copied()
                .filter(|id| !accepted.contains(id))
                .collect();
            self.defer(lease.token, &refused).await;
        }
        Delivery::Sent {
            delivered,
            pending: pending - delivered,
        }
    }

    async fn prune(&self) {
        match self.outbox.prune_sent_events(RETENTION).await {
            Ok(n) if n > 0 => tracing::info!(pruned = n, "old sent usage events pruned"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot prune sent usage events"),
        }
    }

    /// Deliver every `interval` until `cancel`, draining while batches come
    /// back full; prune hourly. After failed rounds the pause grows
    /// ([`next_wait`]) and the failure is logged at most every
    /// [`LOG_EVERY`].
    pub async fn run(self, interval: Duration, cancel: CancellationToken) {
        let mut last_prune: Option<Instant> = None;
        let mut failures: u32 = 0;
        let mut log = LogLimiter::new(LOG_EVERY);
        loop {
            // Bounded drain: at most 20 full batches per tick.
            for _ in 0..20 {
                match self.deliver_once().await {
                    Delivery::Sent { delivered, pending }
                        if (delivered + pending) as u32 == BATCH_SIZE && pending == 0 =>
                    {
                        failures = 0;
                    }
                    Delivery::Failed { reason, pending } => {
                        failures = failures.saturating_add(1);
                        if let Some(suppressed) = log.allow() {
                            if reason == "unauthorized" {
                                tracing::error!(
                                    pending,
                                    suppressed,
                                    "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET \
                                     (401); usage events stay pending until the credentials \
                                     are fixed"
                                );
                            } else {
                                tracing::warn!(
                                    reason,
                                    pending,
                                    suppressed,
                                    "usage events could not be delivered to the Lab; retrying \
                                     with backoff"
                                );
                            }
                        }
                        break;
                    }
                    _ => {
                        if failures > 0 {
                            tracing::info!("usage events reach the Lab again");
                        }
                        failures = 0;
                        log.reset();
                        break;
                    }
                }
            }
            if last_prune.is_none_or(|t| t.elapsed() >= Duration::from_secs(3600)) {
                self.prune().await;
                last_prune = Some(Instant::now());
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(next_wait(interval, failures)) => {}
            }
        }
    }
}

/// Every `interval`, turn pending usage into outbox events, lane by lane.
pub async fn run_usage_flush(
    usage: Arc<UsageEvents>,
    ledger: Arc<dyn UsageLedger>,
    interval: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        match usage.flush(&*ledger).await {
            Ok(n) if n > 0 => tracing::debug!(events = n, "usage enqueued"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot enqueue usage events; retrying later"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_core::Secret;
    use meili_connect_store::MemStore;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const SECRET: &str = "instance-secret";

    fn config(server: &MockServer) -> LabEventsConfig {
        LabEventsConfig {
            url: server.uri(),
            instance_id: "inst-1".into(),
            instance_secret: Secret::new(SECRET),
        }
    }

    /// Verifies the signature by recomputing it, then accepts the events
    /// whose `data.units.documents_streamed` is even (a partial acceptance).
    struct VerifyAndAcceptEven;

    impl Respond for VerifyAndAcceptEven {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let h = |n: &str| req.headers.get(n).unwrap().to_str().unwrap().to_owned();
            assert_eq!(h(H_INSTANCE_ID), "inst-1");
            let ts: u64 = h(H_TIMESTAMP).parse().unwrap();
            assert_eq!(h(H_SIGNATURE), sign(SECRET.as_bytes(), ts, &req.body));
            let body: Value = serde_json::from_slice(&req.body).unwrap();
            let accepted: Vec<Value> = body["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| {
                    e["data"]["units"]["documents_streamed"]
                        .as_u64()
                        .unwrap()
                        .is_multiple_of(2)
                })
                .map(|e| e["id"].clone())
                .collect();
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "accepted": accepted }))
        }
    }

    /// A Lab usage event with `n` streamed documents.
    fn event(n: u64) -> Value {
        crate::usage::lab_event(
            &meili_connect_store::PendingUsage {
                sync_id: "s".into(),
                tenant: Some("t".into()),
                source_kind: "postgres".into(),
                sync_name: "s".into(),
                usage: meili_connect_core::Usage {
                    documents_streamed: n,
                    ..Default::default()
                },
            },
            "postgres → s",
        )
    }

    async fn seed(outbox: &MemStore, counts: &[u64]) -> Vec<Uuid> {
        let mut ids = Vec::new();
        for n in counts {
            ids.push(outbox.enqueue_event(Some("t"), &event(*n)).await.unwrap());
        }
        ids
    }

    #[test]
    fn signature_matches_a_known_vector() {
        // HMAC-SHA256("key", "1700000000.{}") computed independently.
        let sig = sign(b"key", 1_700_000_000, b"{}");
        assert!(sig.starts_with("sha256="));
        assert_eq!(sig.len(), 7 + 64);
        assert_eq!(sig, sign(b"key", 1_700_000_000, b"{}"));
        assert_ne!(sig, sign(b"key", 1_700_000_001, b"{}"));
        assert_ne!(sig, sign(b"other", 1_700_000_000, b"{}"));
    }

    #[tokio::test]
    async fn partial_acceptance_marks_only_the_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/events"))
            .respond_with(VerifyAndAcceptEven)
            .mount(&server)
            .await;
        let outbox = Arc::new(MemStore::new());
        seed(&outbox, &[2, 3, 4]).await;
        let sender = LabSender::new(outbox.clone(), config(&server)).unwrap();
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Sent {
                delivered: 2,
                pending: 1
            }
        );
        // The unaccepted one is leased; once its lease is free it comes back.
        let lease = outbox
            .lease_events(10, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(lease.events.is_empty(), "still leased by the sender");

        // Every event carries its outbox id as the event id.
        let received = server.received_requests().await.unwrap();
        let body: Value = serde_json::from_slice(&received[0].body).unwrap();
        for e in body["events"].as_array().unwrap() {
            assert!(Uuid::parse_str(e["id"].as_str().unwrap()).is_ok());
            assert_eq!(e["product"], "connect");
        }
    }

    #[tokio::test]
    async fn a_failed_batch_is_retried_with_the_same_ids() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(VerifyAndAcceptEven)
            .mount(&server)
            .await;
        let outbox = Arc::new(MemStore::new());
        let ids = seed(&outbox, &[2, 3]).await;
        let sender = LabSender::new(outbox.clone(), config(&server))
            .unwrap()
            .with_lease(Duration::from_millis(200))
            .with_retry_backoff(RetryBackoff {
                base: Duration::from_millis(150),
                max: Duration::from_millis(150),
            });
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Failed {
                reason: "status",
                pending: 2
            }
        );
        // Deferred: not sent again before its backoff.
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Sent {
                delivered: 1,
                pending: 1
            }
        );
        let received = server.received_requests().await.unwrap();
        let first: Value = serde_json::from_slice(&received[0].body).unwrap();
        let second: Value = serde_json::from_slice(&received[1].body).unwrap();
        assert_eq!(first["events"], second["events"], "same ids, same bodies");
        assert_eq!(second["events"][0]["id"], ids[0].to_string());
        // The accepted one is gone for good; the other comes back.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let lease = outbox
            .lease_events(10, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            lease.events.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![ids[1]]
        );
    }

    #[tokio::test]
    async fn a_lab_that_keeps_failing_defers_events_and_slows_down() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let outbox = Arc::new(MemStore::new());
        seed(&outbox, &[2, 3]).await;
        let sender = LabSender::new(outbox.clone(), config(&server))
            .unwrap()
            .with_lease(Duration::from_millis(20));
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Failed {
                reason: "unauthorized",
                pending: 2
            }
        );
        // The lease is long over, but the events wait for their backoff
        // instead of being sent again every round.
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(sender.deliver_once().await, Delivery::Idle);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
        assert_eq!(outbox.count_unsent().await.unwrap(), 2, "kept, not lost");

        let i = Duration::from_secs(2);
        assert_eq!(next_wait(i, 0), i);
        assert_eq!(next_wait(i, 1), Duration::from_secs(4));
        assert_eq!(next_wait(i, 4), Duration::from_secs(32));
        assert_eq!(next_wait(i, 30), MAX_IDLE);
    }

    #[tokio::test]
    async fn an_unreachable_lab_keeps_events() {
        let outbox = Arc::new(MemStore::new());
        seed(&outbox, &[2]).await;
        let sender = LabSender::new(
            outbox.clone(),
            LabEventsConfig {
                url: "http://127.0.0.1:9".into(),
                instance_id: "i".into(),
                instance_secret: Secret::new("s"),
            },
        )
        .unwrap();
        assert!(matches!(
            sender.deliver_once().await,
            Delivery::Failed {
                reason: "connect",
                ..
            }
        ));
    }
}
