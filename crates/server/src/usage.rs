//! Usage metering: the units of `docs/LAB_INTEGRATION.md` ("Pricing"),
//! turned into events, at least once and without loss.
//!
//! The engine records every batch's units with its progress
//! (`StateStore::record_progress`); the store adds them to a pending row per
//! enabled lane ([`Lane::Lab`], [`Lane::Tinybird`]) and sync **in the same
//! transaction** as the progress counters. Every `USAGE_FLUSH_INTERVAL_SECS`,
//! [`UsageEvents::flush`] drains each lane: one transaction takes the
//! pending rows and enqueues one event per sync with non-zero units into the
//! lane's outbox, from which that lane's sender delivers them at least once
//! (deduplicated downstream by event id). So what is sent is exactly what
//! progress recorded, and a crash at any point loses nothing.
//!
//! Remaining double count, bounded: a crash after Meilisearch accepted a
//! batch and before the engine recorded it and saved its checkpoint replays
//! that one batch from the last checkpoint, and its units are recorded once
//! more. At most one batch per index per crash (`docs/LAB_INTEGRATION.md`
//! accepts it).
//!
//! Global-scope syncs are never sent to the Lab (the store does not even
//! accumulate them on its lane); Tinybird gets them with `tenant_id ""`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use meili_connect_core::{DeadLetter, Progress, StateStore, StoreError, SyncSpec, SyncState};
use meili_connect_store::{DrainOutcome, Lane, PendingUsage, RepoError, UsageLedger};
use serde_json::{Value, json};

use crate::limits::LogLimiter;
use crate::metrics::Metrics;

/// Lab product name of meili-connect.
pub const PRODUCT: &str = "connect";
/// The Lab operation of a sync's usage.
pub const OPERATION: &str = "sync";

/// `"postgres → products"`: the source kind and the indexes it feeds.
pub fn description(spec: &SyncSpec) -> String {
    let indexes: Vec<&str> = spec.streams.iter().map(|s| s.index.as_str()).collect();
    format!("{} → {}", spec.source.kind.as_str(), indexes.join(", "))
}

/// The units map of an event: the five unit names of the contract.
fn units(p: &PendingUsage) -> Value {
    json!({
        "documents_streamed": p.usage.documents_streamed,
        "documents_snapshot": p.usage.documents_snapshot,
        "documents_resync": p.usage.documents_resync,
        "bytes": p.usage.bytes,
        "sync_seconds": p.usage.sync_seconds,
    })
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The Lab `usage.recorded` event, without its `id`: the outbox row id (a
/// UUIDv7, stable across retries) is set as the event id when it is sent.
pub fn lab_event(p: &PendingUsage, description: &str) -> Value {
    json!({
        "type": "usage.recorded",
        "occurred_at": now_rfc3339(),
        "account_id": p.tenant,
        "api_key_id": null,
        "product": PRODUCT,
        "data": {
            "operation": OPERATION,
            "units": units(p),
            "job_id": p.sync_id,
            "description": description,
        },
    })
}

/// A row of the Tinybird `connect_usage` data source, without its
/// `event_id` (the outbox row id, set when it is sent). Column names never
/// change: see `tinybird/README.md`.
pub fn tinybird_row(p: &PendingUsage) -> Value {
    json!({
        "occurred_at": now_rfc3339(),
        "tenant_id": p.tenant.as_deref().unwrap_or(""),
        "sync_id": p.sync_id,
        "source_kind": p.source_kind,
        "operation": OPERATION,
        "documents_streamed": p.usage.documents_streamed,
        "documents_snapshot": p.usage.documents_snapshot,
        "documents_resync": p.usage.documents_resync,
        "bytes": p.usage.bytes,
        "sync_seconds": p.usage.sync_seconds,
    })
}

/// Turns pending usage into events, lane by lane. Knows the description of
/// every registered sync (bounded by the syncs); a sync deleted meanwhile
/// is described from what the store kept (`kind → name`).
#[derive(Debug)]
pub struct UsageEvents {
    lanes: Vec<Lane>,
    descriptions: Mutex<HashMap<String, String>>,
    /// Unsent events of a lane beyond which its usage stays pending
    /// (`LAB_OUTBOX_MAX_PENDING`; 0: no cap).
    backlog_cap: u64,
    backlog_log: Mutex<LogLimiter>,
}

impl UsageEvents {
    pub fn new(lanes: Vec<Lane>) -> Self {
        Self {
            lanes,
            descriptions: Mutex::new(HashMap::new()),
            backlog_cap: 0,
            backlog_log: Mutex::new(LogLimiter::new(Duration::from_secs(300))),
        }
    }

    /// Keep a lane's usage pending while it holds `cap` unsent events.
    pub fn with_backlog_cap(mut self, cap: u64) -> Self {
        self.backlog_cap = cap;
        self
    }

    /// Where usage goes (none: nothing is metered into events).
    pub fn lanes(&self) -> &[Lane] {
        &self.lanes
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, String>> {
        self.descriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Remember how to describe a sync (idempotent; a new spec replaces).
    pub fn register(&self, spec: &SyncSpec) {
        self.lock().insert(spec.id.clone(), description(spec));
    }

    pub fn forget(&self, sync_id: &str) {
        self.lock().remove(sync_id);
    }

    pub fn description(&self, p: &PendingUsage) -> String {
        self.lock()
            .get(&p.sync_id)
            .cloned()
            .unwrap_or_else(|| format!("{} → {}", p.source_kind, p.sync_name))
    }

    /// The event of `p` on `lane`.
    pub fn event(&self, lane: Lane, p: &PendingUsage) -> Value {
        match lane {
            Lane::Lab => lab_event(p, &self.description(p)),
            Lane::Tinybird => tinybird_row(p),
        }
    }

    async fn drain(
        &self,
        ledger: &dyn UsageLedger,
        lane: Lane,
        sync_id: Option<&str>,
    ) -> Result<usize, RepoError> {
        let build = |p: &PendingUsage| self.event(lane, p);
        match ledger
            .drain_usage(lane, sync_id, self.backlog_cap, &build)
            .await?
        {
            DrainOutcome::Enqueued(n) => Ok(n),
            DrainOutcome::Backlogged(backlog) => {
                let allowed = self
                    .backlog_log
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .allow();
                if let Some(suppressed) = allowed {
                    tracing::warn!(
                        lane = lane.as_str(),
                        backlog,
                        cap = self.backlog_cap,
                        suppressed,
                        "an events outbox lane is full (LAB_OUTBOX_MAX_PENDING): usage stays \
                         pending in the state database and is enqueued once the lane drains"
                    );
                }
                Ok(0)
            }
        }
    }

    /// Enqueue the pending usage of every lane. Lanes are independent: one
    /// failing does not keep the others from being drained; the first
    /// error is returned after trying all. Returns how many events were
    /// enqueued.
    pub async fn flush(&self, ledger: &dyn UsageLedger) -> Result<usize, RepoError> {
        let mut total = 0;
        let mut first_error = None;
        for &lane in &self.lanes {
            match self.drain(ledger, lane, None).await {
                Ok(n) => total += n,
                Err(e) => {
                    tracing::warn!(lane = lane.as_str(), error = %e, "cannot enqueue usage events");
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(total),
        }
    }

    /// Enqueue the pending usage of one sync now (it is being deleted).
    /// When this fails the usage stays pending and goes with the next
    /// flush: nothing is lost.
    pub async fn flush_sync(&self, ledger: &dyn UsageLedger, sync_id: &str) {
        for &lane in &self.lanes {
            if let Err(e) = self.drain(ledger, lane, Some(sync_id)).await {
                tracing::warn!(sync_id, lane = lane.as_str(), error = %e,
                    "usage of a deleted sync could not be enqueued yet; it stays pending");
            }
        }
    }
}

/// The engine's store as the server gives it to the engine: what it
/// records is also counted in the Prometheus metrics.
pub struct MeteredStore {
    inner: Arc<dyn StateStore>,
    metrics: Arc<Metrics>,
}

impl MeteredStore {
    pub fn new(inner: Arc<dyn StateStore>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait]
impl StateStore for MeteredStore {
    async fn load(&self, sync_id: &str) -> Result<Option<SyncState>, StoreError> {
        self.inner.load(sync_id).await
    }

    async fn save(&self, state: &SyncState) -> Result<(), StoreError> {
        self.inner.save(state).await
    }

    async fn push_dead_letters(&self, letters: &[DeadLetter]) -> Result<(), StoreError> {
        self.inner.push_dead_letters(letters).await?;
        self.metrics.dead_letters(letters.len() as u64);
        Ok(())
    }

    async fn record_progress(&self, sync_id: &str, progress: &Progress) -> Result<(), StoreError> {
        self.inner.record_progress(sync_id, progress).await?;
        self.metrics.record_usage(&progress.usage);
        if let Some(age) = progress.batch_age {
            self.metrics.batch(age);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_core::{
        MeiliDestination, Secret, SourceKind, SourceSpec, StreamMapping, Usage,
    };
    use meili_connect_store::{LabOutbox, MemStore, SyncRepository};

    fn spec(id: &str, tenant: Option<&str>) -> SyncSpec {
        SyncSpec {
            id: id.into(),
            tenant: tenant.map(str::to_owned),
            name: format!("sync {id}"),
            source: SourceSpec {
                kind: SourceKind::Postgres,
                config: json!({}),
            },
            destination: MeiliDestination {
                url: "http://meili:7700".into(),
                api_key: Secret::new("k"),
            },
            streams: vec![
                serde_json::from_value::<StreamMapping>(
                    json!({"collection": "public.p", "index": "products"}),
                )
                .unwrap(),
            ],
            mass_delete_guard: None,
        }
    }

    fn progress(streamed: u64, snapshot: u64, bytes: u64, seconds: u64) -> Progress {
        Progress {
            written: streamed + snapshot,
            usage: Usage {
                documents_streamed: streamed,
                documents_snapshot: snapshot,
                documents_resync: 0,
                bytes,
                sync_seconds: seconds,
            },
            ..Progress::default()
        }
    }

    #[tokio::test]
    async fn the_lab_event_has_the_contract_shape() {
        let store = MemStore::new();
        let events = UsageEvents::new(vec![Lane::Lab]);
        store.set_usage_lanes(events.lanes());
        let s = spec("s1", Some("team-a"));
        store.create_sync(Some("team-a"), &s).await.unwrap();
        events.register(&s);
        store
            .record_progress("s1", &progress(1200, 0, 1_482_113, 60))
            .await
            .unwrap();
        assert_eq!(events.flush(&store).await.unwrap(), 1);
        assert_eq!(events.flush(&store).await.unwrap(), 0, "nothing new");

        let lease = store
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(lease.events.len(), 1);
        let e = &lease.events[0].payload;
        assert_eq!(e["type"], "usage.recorded");
        assert_eq!(e["product"], "connect");
        assert_eq!(e["account_id"], "team-a");
        assert!(e["api_key_id"].is_null());
        assert!(e["occurred_at"].as_str().unwrap().ends_with('Z'));
        assert_eq!(
            e["data"],
            json!({
                "operation": "sync",
                "units": {
                    "documents_streamed": 1200,
                    "documents_snapshot": 0,
                    "documents_resync": 0,
                    "bytes": 1_482_113,
                    "sync_seconds": 60,
                },
                "job_id": "s1",
                "description": "postgres → products",
            })
        );
        // Exactly the fields of the contract (the id is added on send).
        let mut keys: Vec<&str> = e.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "account_id",
                "api_key_id",
                "data",
                "occurred_at",
                "product",
                "type"
            ]
        );
    }

    #[tokio::test]
    async fn global_syncs_are_never_sent_to_the_lab_but_reach_tinybird() {
        let store = MemStore::new();
        let events = UsageEvents::new(vec![Lane::Lab, Lane::Tinybird]);
        store.set_usage_lanes(events.lanes());
        let g = spec("g", None);
        store.create_sync(None, &g).await.unwrap();
        events.register(&g);
        store
            .record_progress("g", &progress(5, 0, 50, 0))
            .await
            .unwrap();
        assert_eq!(events.flush(&store).await.unwrap(), 1);
        assert_eq!(
            store.count_unsent().await.unwrap(),
            0,
            "nothing for the Lab"
        );
        let tb = store.outbox(Lane::Tinybird);
        let lease = tb.lease_events(10, Duration::from_secs(30)).await.unwrap();
        let row = &lease.events[0].payload;
        assert_eq!(row["tenant_id"], "");
        assert_eq!(row["sync_id"], "g");
        assert_eq!(row["source_kind"], "postgres");
        assert_eq!(row["operation"], "sync");
        assert_eq!(row["documents_streamed"], 5);
        assert_eq!(row["bytes"], 50);
    }

    #[tokio::test]
    async fn a_deleted_sync_is_flushed_then_described_from_the_store() {
        let store = MemStore::new();
        let events = UsageEvents::new(vec![Lane::Lab]);
        store.set_usage_lanes(events.lanes());
        let s = spec("s1", Some("t"));
        store.create_sync(Some("t"), &s).await.unwrap();
        events.register(&s);
        store
            .record_progress("s1", &progress(2, 0, 0, 0))
            .await
            .unwrap();
        events.flush_sync(&store, "s1").await;
        events.forget("s1");
        store.delete_sync(Some("t"), "s1").await.unwrap();
        let lease = store
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(lease.events.len(), 1);
        assert_eq!(
            lease.events[0].payload["data"]["description"],
            "postgres → products"
        );
        // Unknown to the registry: described from what the store kept.
        let p = PendingUsage {
            sync_id: "gone".into(),
            tenant: Some("t".into()),
            source_kind: "mongodb".into(),
            sync_name: "orders".into(),
            usage: Usage::default(),
        };
        assert_eq!(events.description(&p), "mongodb → orders");
    }

    #[tokio::test]
    async fn a_full_lab_lane_does_not_hold_tinybird_back() {
        let store = MemStore::new();
        let events = UsageEvents::new(vec![Lane::Lab, Lane::Tinybird]).with_backlog_cap(1);
        store.set_usage_lanes(events.lanes());
        let s = spec("s1", Some("t"));
        store.create_sync(Some("t"), &s).await.unwrap();
        // The Lab is down: its lane is full.
        store.enqueue_event(Some("t"), &json!({})).await.unwrap();
        store
            .record_progress("s1", &progress(5, 0, 0, 0))
            .await
            .unwrap();
        assert_eq!(events.flush(&store).await.unwrap(), 1, "Tinybird's only");
        assert_eq!(store.pending_usage(Lane::Lab).await.unwrap().len(), 1);
        assert_eq!(
            store.outbox(Lane::Tinybird).count_unsent().await.unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn a_full_lane_keeps_the_usage_pending() {
        let store = MemStore::new();
        let events = UsageEvents::new(vec![Lane::Lab]).with_backlog_cap(2);
        store.set_usage_lanes(events.lanes());
        let s = spec("s1", Some("t"));
        store.create_sync(Some("t"), &s).await.unwrap();
        for _ in 0..2 {
            store.enqueue_event(Some("t"), &json!({})).await.unwrap();
        }
        store
            .record_progress("s1", &progress(5, 0, 0, 0))
            .await
            .unwrap();
        assert_eq!(events.flush(&store).await.unwrap(), 0);
        assert_eq!(store.pending_usage(Lane::Lab).await.unwrap().len(), 1);
        store
            .record_progress("s1", &progress(1, 0, 0, 0))
            .await
            .unwrap();
        // Room again: the whole count goes out at once.
        let roomy = UsageEvents::new(vec![Lane::Lab]).with_backlog_cap(10);
        assert_eq!(roomy.flush(&store).await.unwrap(), 1);
        let lease = store
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        let sent = lease.events.last().unwrap();
        assert_eq!(sent.payload["data"]["units"]["documents_streamed"], 6);
    }
}
