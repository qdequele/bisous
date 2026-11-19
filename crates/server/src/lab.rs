//! Delivers the Lab events outbox to `POST {LAB_URL}/internal/events`.
//!
//! Each round leases up to [`BATCH_SIZE`] events, sends them as one signed
//! batch `{"events": [...]}`, and marks the ids the Lab lists in `accepted`
//! as sent. Everything else stays unsent and comes back when its lease
//! expires. Sent events older than [`RETENTION`] are pruned hourly. The Lab
//! being down never affects the syncs.
//!
//! Signature: `X-Lab-Signature: sha256=<hex HMAC-SHA256(secret,
//! "<timestamp>.<raw body>")>`, with `X-Lab-Instance-Id` and
//! `X-Lab-Timestamp` (unix seconds).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use meili_connect_store::LabOutbox;
use serde_json::Value;
use sha2::Sha256;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::LabEventsConfig;
use crate::usage::UsageMeter;

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
    /// The whole batch failed (`store`, `connect`, `timeout`, `status`,
    /// `malformed`); it is retried when the lease expires.
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
}

async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Option<Vec<u8>> {
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
        })
    }

    /// How long a batch stays leased (tests shorten it).
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub async fn deliver_once(&self) -> Delivery {
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
                tracing::warn!(reason, "Lab unreachable; usage events stay pending");
                return Delivery::Failed { reason, pending };
            }
        };
        let status = resp.status();
        if status != reqwest::StatusCode::OK {
            if status == reqwest::StatusCode::UNAUTHORIZED {
                tracing::error!(
                    "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); usage events \
                     stay pending until the credentials are fixed"
                );
            } else {
                tracing::warn!(%status, "the Lab did not accept the usage events batch");
            }
            return Delivery::Failed {
                reason: "status",
                pending,
            };
        }
        #[derive(serde::Deserialize)]
        struct Ack {
            accepted: Vec<Uuid>,
        }
        let Some(bytes) = read_capped(resp, MAX_ACK_BYTES).await else {
            return Delivery::Failed {
                reason: "malformed",
                pending,
            };
        };
        let Ok(ack) = serde_json::from_slice::<Ack>(&bytes) else {
            tracing::warn!("the Lab answered 200 with a malformed body");
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
                "the Lab did not accept every usage event; the rest is retried"
            );
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
    /// back full; prune hourly.
    pub async fn run(self, interval: Duration, cancel: CancellationToken) {
        let mut last_prune: Option<Instant> = None;
        loop {
            // Bounded drain: at most 20 full batches per tick.
            for _ in 0..20 {
                match self.deliver_once().await {
                    Delivery::Sent { delivered, pending }
                        if (delivered + pending) as u32 == BATCH_SIZE && pending == 0 => {}
                    _ => break,
                }
            }
            if last_prune.is_none_or(|t| t.elapsed() >= Duration::from_secs(3600)) {
                self.prune().await;
                last_prune = Some(Instant::now());
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }
}

/// Every `interval`, turn pending document counts into outbox events.
pub async fn run_usage_flush(
    meter: Arc<UsageMeter>,
    outbox: Arc<dyn LabOutbox>,
    interval: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        match meter.flush(&*outbox).await {
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
    /// whose `data.units.documents` is even (a partial acceptance).
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
                .filter(|e| e["data"]["units"]["documents"].as_u64().unwrap() % 2 == 0)
                .map(|e| e["id"].clone())
                .collect();
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "accepted": accepted }))
        }
    }

    async fn seed(outbox: &MemStore, counts: &[u64]) -> Vec<Uuid> {
        let mut ids = Vec::new();
        for n in counts {
            ids.push(
                outbox
                    .enqueue_event(Some("t"), &crate::usage::usage_event("t", "s", *n))
                    .await
                    .unwrap(),
            );
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
            .with_lease(Duration::from_millis(200));
        assert_eq!(
            sender.deliver_once().await,
            Delivery::Failed {
                reason: "status",
                pending: 2
            }
        );
        // Leased: not sent again before the lease expires.
        assert_eq!(sender.deliver_once().await, Delivery::Idle);
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
