//! Usage metering: documents written or deleted per sync, turned into Lab
//! `usage.recorded` events.
//!
//! [`MeteredStore`] wraps the engine's [`StateStore`]: every successful
//! `record_progress` also adds `written + deleted` to the sync's pending
//! count in a [`UsageMeter`]. [`UsageMeter::flush`] (every
//! `USAGE_FLUSH_INTERVAL_SECS`) enqueues one event per tenant-owned sync
//! with a non-zero count into the outbox, from which the Lab sender
//! delivers them at least once.
//!
//! Counts held in memory and not yet enqueued are lost on a crash; a
//! graceful shutdown flushes them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use meili_connect_core::{DeadLetter, Progress, StateStore, StoreError, SyncState};
use meili_connect_store::{LabOutbox, RepoError};
use serde_json::{Value, json};

use crate::limits::LogLimiter;

/// Lab product name of meili-connect.
pub const PRODUCT: &str = "connect";

#[derive(Debug, Default)]
struct Entry {
    tenant: Option<String>,
    documents: u64,
}

/// Pending document counts of the registered syncs. Bounded by the number
/// of syncs: counts of an unregistered sync are ignored.
#[derive(Debug)]
pub struct UsageMeter {
    entries: Mutex<HashMap<String, Entry>>,
    /// Unsent outbox events beyond which `flush` keeps the counts in
    /// memory instead of enqueueing (0: no cap).
    backlog_cap: u64,
    backlog_log: Mutex<LogLimiter>,
}

impl Default for UsageMeter {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            backlog_cap: 0,
            backlog_log: Mutex::new(LogLimiter::new(Duration::from_secs(300))),
        }
    }
}

impl UsageMeter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop enqueueing while the outbox holds `cap` unsent events or more;
    /// counts keep accumulating in memory (one number per sync) meanwhile.
    pub fn with_backlog_cap(mut self, cap: u64) -> Self {
        self.backlog_cap = cap;
        self
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Start metering a sync (idempotent; keeps a pending count).
    pub fn register(&self, sync_id: &str, tenant: Option<&str>) {
        let mut entries = self.lock();
        let entry = entries.entry(sync_id.to_owned()).or_default();
        entry.tenant = tenant.map(str::to_owned);
    }

    /// Stop metering a deleted sync. Its pending count is returned so the
    /// caller can still enqueue it.
    pub fn forget(&self, sync_id: &str) -> Option<(Option<String>, u64)> {
        self.lock().remove(sync_id).map(|e| (e.tenant, e.documents))
    }

    pub fn add(&self, sync_id: &str, documents: u64) {
        if documents == 0 {
            return;
        }
        if let Some(entry) = self.lock().get_mut(sync_id) {
            entry.documents = entry.documents.saturating_add(documents);
        }
    }

    /// Pending count of one sync (tests, status).
    pub fn pending(&self, sync_id: &str) -> u64 {
        self.lock().get(sync_id).map_or(0, |e| e.documents)
    }

    /// Take every non-zero count of a tenant-owned sync. Global syncs are
    /// not billed: their counts are dropped.
    fn take(&self) -> Vec<(String, String, u64)> {
        let mut out = Vec::new();
        for (id, entry) in self.lock().iter_mut() {
            let n = std::mem::take(&mut entry.documents);
            if n > 0
                && let Some(tenant) = &entry.tenant
            {
                out.push((id.clone(), tenant.clone(), n));
            }
        }
        out.sort();
        out
    }

    fn give_back(&self, sync_id: &str, tenant: &str, documents: u64) {
        let mut entries = self.lock();
        let entry = entries.entry(sync_id.to_owned()).or_insert_with(|| Entry {
            tenant: Some(tenant.to_owned()),
            documents: 0,
        });
        entry.documents = entry.documents.saturating_add(documents);
    }

    /// Enqueue one `usage.recorded` event per tenant-owned sync with a
    /// non-zero count. On a store error the counts not yet enqueued are put
    /// back for the next flush. Returns how many events were enqueued.
    pub async fn flush(&self, outbox: &dyn LabOutbox) -> Result<usize, RepoError> {
        if self.backlog_cap > 0 {
            let backlog = outbox.count_unsent().await?;
            if backlog >= self.backlog_cap {
                let allowed = self
                    .backlog_log
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .allow();
                if let Some(suppressed) = allowed {
                    tracing::warn!(
                        backlog,
                        cap = self.backlog_cap,
                        suppressed,
                        "the Lab events outbox is full (LAB_OUTBOX_MAX_PENDING): usage keeps \
                         being counted in memory and is enqueued once the Lab takes events again"
                    );
                }
                return Ok(0);
            }
        }
        let pending = self.take();
        let mut enqueued = 0;
        let mut iter = pending.into_iter();
        while let Some((sync_id, tenant, n)) = iter.next() {
            if let Err(e) = outbox
                .enqueue_event(Some(&tenant), &usage_event(&tenant, &sync_id, n))
                .await
            {
                self.give_back(&sync_id, &tenant, n);
                for (sync_id, tenant, n) in iter {
                    self.give_back(&sync_id, &tenant, n);
                }
                return Err(e);
            }
            enqueued += 1;
        }
        Ok(enqueued)
    }

    /// Enqueue the remaining count of a sync being deleted.
    pub async fn flush_one(&self, outbox: &dyn LabOutbox, sync_id: &str) -> Result<(), RepoError> {
        if let Some((Some(tenant), n)) = self.forget(sync_id)
            && n > 0
        {
            outbox
                .enqueue_event(Some(&tenant), &usage_event(&tenant, sync_id, n))
                .await?;
        }
        Ok(())
    }
}

/// The event body, without its `id`: the outbox row id (a UUIDv7, stable
/// across retries) is set as the event id when the batch is sent.
pub fn usage_event(tenant: &str, sync_id: &str, documents: u64) -> Value {
    json!({
        "type": "usage.recorded",
        "occurred_at": Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        "account_id": tenant,
        "api_key_id": null,
        "product": PRODUCT,
        "data": {
            "operation": "sync",
            "units": { "documents": documents },
            "job_id": sync_id,
            "description": format!("{documents} documents written or deleted by sync {sync_id}"),
        },
    })
}

/// The engine's store, with usage metering on `record_progress`.
pub struct MeteredStore {
    inner: Arc<dyn StateStore>,
    meter: Arc<UsageMeter>,
}

impl MeteredStore {
    pub fn new(inner: Arc<dyn StateStore>, meter: Arc<UsageMeter>) -> Self {
        Self { inner, meter }
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
        self.inner.push_dead_letters(letters).await
    }

    async fn record_progress(&self, sync_id: &str, progress: &Progress) -> Result<(), StoreError> {
        self.inner.record_progress(sync_id, progress).await?;
        self.meter
            .add(sync_id, progress.written.saturating_add(progress.deleted));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_store::{MemStore, SyncRepository};
    use std::time::Duration;

    #[tokio::test]
    async fn record_progress_accumulates_written_and_deleted() {
        let mem = Arc::new(MemStore::permissive());
        let meter = Arc::new(UsageMeter::new());
        meter.register("s1", Some("team-a"));
        let store = MeteredStore::new(mem.clone(), meter.clone());
        let pr = |written, deleted, rejected| Progress {
            written,
            deleted,
            rejected,
            ..Progress::default()
        };
        store.record_progress("s1", &pr(10, 5, 3)).await.unwrap();
        store.record_progress("s1", &pr(1, 0, 0)).await.unwrap();
        // Unregistered syncs are ignored: bounded by the registered set.
        store
            .record_progress("other", &pr(100, 0, 0))
            .await
            .unwrap();
        assert_eq!(meter.pending("s1"), 16);
        assert_eq!(meter.pending("other"), 0);
    }

    #[tokio::test]
    async fn flush_enqueues_one_event_per_tenant_sync() {
        let outbox = MemStore::new();
        let meter = UsageMeter::new();
        meter.register("s1", Some("team-a"));
        meter.register("s2", Some("team-b"));
        meter.register("s3", Some("team-b"));
        meter.register("global", None);
        meter.add("s1", 7);
        meter.add("s2", 3);
        meter.add("global", 50);
        assert_eq!(meter.flush(&outbox).await.unwrap(), 2);
        assert_eq!(meter.pending("s1"), 0);
        assert_eq!(meter.pending("global"), 0);
        // Nothing new: nothing enqueued.
        assert_eq!(meter.flush(&outbox).await.unwrap(), 0);

        let lease = outbox
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(lease.events.len(), 2);
        let mut bodies: Vec<_> = lease.events.iter().map(|e| e.payload.clone()).collect();
        bodies.sort_by_key(|b| b["data"]["job_id"].as_str().unwrap().to_owned());
        let b = &bodies[0];
        assert_eq!(b["type"], "usage.recorded");
        assert_eq!(b["product"], "connect");
        assert_eq!(b["account_id"], "team-a");
        assert!(b["api_key_id"].is_null());
        assert_eq!(b["data"]["operation"], "sync");
        assert_eq!(b["data"]["units"]["documents"], 7);
        assert_eq!(b["data"]["job_id"], "s1");
        assert!(b["occurred_at"].as_str().unwrap().ends_with('Z'));
        assert!(lease.events[0].tenant.is_some());
    }

    #[tokio::test]
    async fn forget_flushes_the_remainder() {
        let outbox = MemStore::new();
        let meter = UsageMeter::new();
        meter.register("s1", Some("team-a"));
        meter.add("s1", 4);
        meter.flush_one(&outbox, "s1").await.unwrap();
        meter.add("s1", 4);
        assert_eq!(meter.pending("s1"), 0, "forgotten syncs stop counting");
        let lease = outbox
            .lease_events(10, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(lease.events.len(), 1);
    }

    struct FailingOutbox;

    #[async_trait]
    impl LabOutbox for FailingOutbox {
        async fn enqueue_event(&self, _: Option<&str>, _: &Value) -> Result<uuid::Uuid, RepoError> {
            Err(RepoError::Transient("down".into()))
        }
        async fn lease_events(
            &self,
            _: u32,
            _: Duration,
        ) -> Result<meili_connect_store::OutboxLease, RepoError> {
            unreachable!()
        }
        async fn mark_events_sent(
            &self,
            _: uuid::Uuid,
            _: &[uuid::Uuid],
        ) -> Result<u64, RepoError> {
            unreachable!()
        }
        async fn prune_sent_events(&self, _: Duration) -> Result<u64, RepoError> {
            unreachable!()
        }
        async fn defer_events(
            &self,
            _: uuid::Uuid,
            _: &[uuid::Uuid],
            _: meili_connect_store::RetryBackoff,
        ) -> Result<u64, RepoError> {
            unreachable!()
        }
        async fn count_unsent(&self) -> Result<u64, RepoError> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn a_failed_flush_keeps_the_counts() {
        let meter = UsageMeter::new();
        meter.register("s1", Some("t"));
        meter.register("s2", Some("t"));
        meter.add("s1", 2);
        meter.add("s2", 3);
        assert!(meter.flush(&FailingOutbox).await.is_err());
        assert_eq!(meter.pending("s1"), 2);
        assert_eq!(meter.pending("s2"), 3);
    }

    #[tokio::test]
    async fn a_full_outbox_keeps_the_counts_in_memory() {
        let outbox = MemStore::new();
        for _ in 0..3 {
            outbox.enqueue_event(Some("t"), &json!({})).await.unwrap();
        }
        let meter = UsageMeter::new().with_backlog_cap(3);
        meter.register("s1", Some("t"));
        meter.add("s1", 5);
        assert_eq!(meter.flush(&outbox).await.unwrap(), 0);
        assert_eq!(meter.pending("s1"), 5, "still counted");
        meter.add("s1", 1);
        assert_eq!(outbox.count_unsent().await.unwrap(), 3, "nothing enqueued");
        // Room again: the whole count goes out at once.
        let roomy = UsageMeter::new().with_backlog_cap(10);
        roomy.register("s1", Some("t"));
        roomy.add("s1", 6);
        assert_eq!(roomy.flush(&outbox).await.unwrap(), 1);
        assert_eq!(outbox.count_unsent().await.unwrap(), 4);
    }

    #[test]
    fn unused_trait_import_guard() {
        // `SyncRepository` is imported to make sure MemStore satisfies both
        // traits the server needs from one value.
        fn both<T: SyncRepository + LabOutbox>(_: &T) {}
        both(&MemStore::new());
    }
}
