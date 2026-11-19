//! In-memory store for tests and single-node trials. Nothing survives a
//! restart.
//!
//! [`MemStore::new`] behaves like [`crate::PgStore`]: the [`StateStore`]
//! writes (`save`, `push_dead_letters`, `record_progress`) for a sync that
//! does not exist fail with [`StoreError::NotFound`], as the foreign keys do
//! in Postgres. [`MemStore::permissive`] accepts unknown sync ids, for
//! engine tests that never create a spec.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use meili_connect_core::{
    DeadLetter, Progress, StateStore, StoreError, SyncSpec, SyncState, Usage,
};
use uuid::Uuid;

use crate::{
    BootSync, DailyProgress, DeadLetterLimits, DeadLetterPage, DrainOutcome, EventBuilder,
    LabOutbox, Lane, OutboxEvent, OutboxLease, PendingUsage, RepoError, RetryBackoff,
    StoredDeadLetter, StoredSync, SyncRepository, SyncRow, UnreadableSync, UsageLedger, UsageRow,
    bound_letter, lane_mask, lanes_of, validate_spec,
};

#[derive(Default)]
struct Inner {
    syncs: HashMap<String, StoredSync>,
    states: HashMap<String, SyncState>,
    dead_letters: BTreeMap<i64, DeadLetter>,
    next_dead_letter: i64,
    /// written, deleted, rejected, dead letters dropped; the units.
    progress: BTreeMap<(String, NaiveDate), ([u64; 4], Usage)>,
    outbox: Vec<OutboxRow>,
    /// Usage recorded and not yet enqueued, per lane and sync.
    pending: BTreeMap<(Lane, String), PendingUsage>,
    /// Test hook: rows reported as unreadable, like a Postgres row sealed
    /// with another key.
    unreadable: std::collections::HashSet<String>,
}

struct OutboxRow {
    lane: Lane,
    event: OutboxEvent,
    leased_until: Option<DateTime<Utc>>,
    lease_token: Option<Uuid>,
    sent_at: Option<DateTime<Utc>>,
    attempts: u32,
    next_attempt_at: Option<DateTime<Utc>>,
}

impl Inner {
    fn owned(&self, tenant: Option<&str>, id: &str) -> Result<(), RepoError> {
        match self.syncs.get(id) {
            Some(s) if s.spec.tenant.as_deref() == tenant => Ok(()),
            _ => Err(RepoError::NotFound(format!("sync {id}"))),
        }
    }

    fn row(&self, s: &StoredSync) -> SyncRow {
        if self.unreadable.contains(&s.spec.id) {
            SyncRow::Unreadable(UnreadableSync {
                id: s.spec.id.clone(),
                tenant: s.spec.tenant.clone(),
                name: s.spec.name.clone(),
                kind: s.spec.source.kind.as_str().to_owned(),
                error: "the spec cannot be unsealed".into(),
                created_at: s.created_at,
                updated_at: s.updated_at,
            })
        } else {
            SyncRow::Readable(s.clone())
        }
    }

    fn name_taken(&self, tenant: Option<&str>, name: &str, except_id: &str) -> bool {
        self.syncs.values().any(|s| {
            s.spec.id != except_id && s.spec.tenant.as_deref() == tenant && s.spec.name == name
        })
    }
}

#[derive(Default)]
pub struct MemStore {
    inner: Arc<Mutex<Inner>>,
    permissive: bool,
    limits: DeadLetterLimits,
    /// [`UsageLedger::set_usage_lanes`], as a bit mask.
    lanes: AtomicU8,
}

/// One lane of the outbox of a [`MemStore`].
pub struct MemLane {
    inner: Arc<Mutex<Inner>>,
    lane: Lane,
}

fn lock_inner(inner: &Mutex<Inner>) -> Result<MutexGuard<'_, Inner>, RepoError> {
    inner
        .lock()
        .map_err(|_| RepoError::Fatal("memory store lock poisoned".into()))
}

fn enqueue(
    inner: &mut Inner,
    lane: Lane,
    tenant: Option<&str>,
    payload: &serde_json::Value,
) -> Uuid {
    let id = Uuid::now_v7();
    inner.outbox.push(OutboxRow {
        lane,
        event: OutboxEvent {
            id,
            tenant: tenant.map(str::to_owned),
            payload: payload.clone(),
            created_at: Utc::now(),
        },
        leased_until: None,
        lease_token: None,
        sent_at: None,
        attempts: 0,
        next_attempt_at: None,
    });
    id
}

impl MemStore {
    /// A store that, like Postgres, refuses state for unknown syncs.
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: a store whose [`StateStore`] methods accept sync ids
    /// that were never created, so an engine can run without specs.
    pub fn permissive() -> Self {
        Self {
            permissive: true,
            ..Self::default()
        }
    }

    /// Test helper: from now on report this sync as unreadable, as
    /// Postgres does for a row sealed with another key.
    pub fn make_unreadable(&self, id: &str) {
        if let Ok(mut inner) = self.lock() {
            inner.unreadable.insert(id.to_owned());
        }
    }

    /// Bound what one sync may keep in the dead-letter table.
    pub fn with_dead_letter_limits(mut self, limits: DeadLetterLimits) -> Self {
        self.limits = limits;
        self
    }

    fn check_sync(&self, inner: &Inner, sync_id: &str) -> Result<(), StoreError> {
        if self.permissive || inner.syncs.contains_key(sync_id) {
            Ok(())
        } else {
            Err(StoreError::NotFound(format!("sync {sync_id}")))
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>, StoreError> {
        self.inner
            .lock()
            .map_err(|_| StoreError::Fatal("memory store lock poisoned".into()))
    }
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore").finish_non_exhaustive()
    }
}

#[async_trait]
impl StateStore for MemStore {
    async fn load(&self, sync_id: &str) -> Result<Option<SyncState>, StoreError> {
        Ok(self.lock()?.states.get(sync_id).cloned())
    }

    async fn save(&self, state: &SyncState) -> Result<(), StoreError> {
        let mut inner = self.lock()?;
        self.check_sync(&inner, &state.sync_id)?;
        inner.states.insert(state.sync_id.clone(), state.clone());
        Ok(())
    }

    async fn push_dead_letters(&self, letters: &[DeadLetter]) -> Result<(), StoreError> {
        let mut inner = self.lock()?;
        // All or nothing, like the transaction in Postgres.
        for letter in letters {
            self.check_sync(&inner, &letter.sync_id)?;
        }
        let day = Utc::now().date_naive();
        for letter in letters {
            let stored = inner
                .dead_letters
                .values()
                .filter(|d| d.sync_id == letter.sync_id)
                .count() as u64;
            if stored >= self.limits.max_per_sync {
                let (c, _) = inner
                    .progress
                    .entry((letter.sync_id.clone(), day))
                    .or_default();
                c[3] = c[3].saturating_add(1);
                continue;
            }
            inner.next_dead_letter += 1;
            let id = inner.next_dead_letter;
            inner
                .dead_letters
                .insert(id, bound_letter(letter, &self.limits));
        }
        Ok(())
    }

    async fn record_progress(&self, sync_id: &str, p: &Progress) -> Result<(), StoreError> {
        let day = Utc::now().date_naive();
        let mut inner = self.lock()?;
        self.check_sync(&inner, sync_id)?;
        let (c, usage) = inner.progress.entry((sync_id.to_owned(), day)).or_default();
        c[0] = c[0].saturating_add(p.written);
        c[1] = c[1].saturating_add(p.deleted);
        c[2] = c[2].saturating_add(p.rejected);
        usage.add(&p.usage);
        if p.usage.is_zero() {
            return Ok(());
        }
        // Under the same lock as the counters: the pending usage is exactly
        // what progress recorded.
        let (tenant, kind, name) = match inner.syncs.get(sync_id) {
            Some(s) => (
                s.spec.tenant.clone(),
                s.spec.source.kind.as_str().to_owned(),
                s.spec.name.clone(),
            ),
            None => (None, String::new(), String::new()),
        };
        for lane in lanes_of(self.lanes.load(Ordering::Relaxed)) {
            // The Lab bills tenants only: global syncs are never sent.
            if lane == Lane::Lab && tenant.is_none() {
                continue;
            }
            let entry = inner
                .pending
                .entry((lane, sync_id.to_owned()))
                .or_insert_with(|| PendingUsage {
                    sync_id: sync_id.to_owned(),
                    tenant: tenant.clone(),
                    source_kind: kind.clone(),
                    sync_name: name.clone(),
                    usage: Default::default(),
                });
            entry.usage.add(&p.usage);
        }
        Ok(())
    }
}

#[async_trait]
impl SyncRepository for MemStore {
    async fn create_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError> {
        validate_spec(tenant, spec)?;
        let mut inner = self.lock()?;
        if inner.syncs.contains_key(&spec.id) {
            return Err(RepoError::Conflict(format!("sync id {} exists", spec.id)));
        }
        if inner.name_taken(tenant, &spec.name, &spec.id) {
            return Err(RepoError::Conflict(format!(
                "a sync named {:?} exists",
                spec.name
            )));
        }
        let now = Utc::now();
        let stored = StoredSync {
            spec: spec.clone(),
            created_at: now,
            updated_at: now,
        };
        inner.syncs.insert(spec.id.clone(), stored.clone());
        Ok(stored)
    }

    async fn get_sync(&self, tenant: Option<&str>, id: &str) -> Result<Option<SyncRow>, RepoError> {
        let inner = self.lock()?;
        Ok(inner
            .syncs
            .get(id)
            .filter(|s| s.spec.tenant.as_deref() == tenant)
            .map(|s| inner.row(s)))
    }

    async fn list_syncs(&self, tenant: Option<&str>) -> Result<Vec<SyncRow>, RepoError> {
        let inner = self.lock()?;
        let mut out: Vec<&StoredSync> = inner
            .syncs
            .values()
            .filter(|s| s.spec.tenant.as_deref() == tenant)
            .collect();
        out.sort_by(|a, b| a.spec.name.cmp(&b.spec.name));
        Ok(out.into_iter().map(|s| inner.row(s)).collect())
    }

    async fn update_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError> {
        validate_spec(tenant, spec)?;
        let mut inner = self.lock()?;
        inner.owned(tenant, &spec.id)?;
        if inner.name_taken(tenant, &spec.name, &spec.id) {
            return Err(RepoError::Conflict(format!(
                "a sync named {:?} exists",
                spec.name
            )));
        }
        let entry = inner
            .syncs
            .get_mut(&spec.id)
            .ok_or_else(|| RepoError::NotFound(format!("sync {}", spec.id)))?;
        entry.spec = spec.clone();
        entry.updated_at = Utc::now();
        Ok(entry.clone())
    }

    async fn delete_sync(&self, tenant: Option<&str>, id: &str) -> Result<(), RepoError> {
        let mut inner = self.lock()?;
        inner.owned(tenant, id)?;
        inner.syncs.remove(id);
        inner.states.remove(id);
        inner.dead_letters.retain(|_, d| d.sync_id != id);
        inner.progress.retain(|(sync_id, _), _| sync_id != id);
        Ok(())
    }

    async fn list_all_for_boot(&self) -> Result<Vec<BootSync>, RepoError> {
        let inner = self.lock()?;
        let mut out: Vec<BootSync> = inner
            .syncs
            .values()
            .map(|s| BootSync {
                id: s.spec.id.clone(),
                tenant: s.spec.tenant.clone(),
                spec: match inner.row(s) {
                    SyncRow::Readable(r) => Ok(r.spec),
                    SyncRow::Unreadable(u) => Err(RepoError::Fatal(u.error)),
                },
            })
            .collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn list_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        page: DeadLetterPage,
    ) -> Result<Vec<StoredDeadLetter>, RepoError> {
        let inner = self.lock()?;
        inner.owned(tenant, sync_id)?;
        let start = page.after.unwrap_or(0).saturating_add(1);
        Ok(inner
            .dead_letters
            .range(start..)
            .filter(|(_, d)| d.sync_id == sync_id)
            .take(page.clamped_limit() as usize)
            .map(|(id, letter)| StoredDeadLetter {
                id: *id,
                letter: letter.clone(),
            })
            .collect())
    }

    async fn delete_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        ids: &[i64],
    ) -> Result<u64, RepoError> {
        let mut inner = self.lock()?;
        inner.owned(tenant, sync_id)?;
        let mut n = 0;
        for id in ids {
            if inner
                .dead_letters
                .get(id)
                .is_some_and(|d| d.sync_id == sync_id)
            {
                inner.dead_letters.remove(id);
                n += 1;
            }
        }
        Ok(n)
    }

    async fn clear_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
    ) -> Result<u64, RepoError> {
        let mut inner = self.lock()?;
        inner.owned(tenant, sync_id)?;
        let before = inner.dead_letters.len();
        inner.dead_letters.retain(|_, d| d.sync_id != sync_id);
        Ok((before - inner.dead_letters.len()) as u64)
    }

    async fn progress(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<DailyProgress>, RepoError> {
        let inner = self.lock()?;
        inner.owned(tenant, sync_id)?;
        if from > to {
            return Ok(Vec::new());
        }
        Ok(inner
            .progress
            .range((sync_id.to_owned(), from)..=(sync_id.to_owned(), to))
            .map(|((_, day), (c, usage))| DailyProgress {
                day: *day,
                written: c[0],
                deleted: c[1],
                rejected: c[2],
                dead_letters_dropped: c[3],
                usage: *usage,
            })
            .collect())
    }
}

impl MemStore {
    /// The outbox of one lane.
    pub fn lane(&self, lane: Lane) -> MemLane {
        MemLane {
            inner: Arc::clone(&self.inner),
            lane,
        }
    }
}

#[async_trait]
impl UsageLedger for MemStore {
    fn set_usage_lanes(&self, lanes: &[Lane]) {
        self.lanes.store(lane_mask(lanes), Ordering::Relaxed);
    }

    fn outbox(&self, lane: Lane) -> Arc<dyn LabOutbox> {
        Arc::new(self.lane(lane))
    }

    async fn pending_usage(&self, lane: Lane) -> Result<Vec<PendingUsage>, RepoError> {
        Ok(lock_inner(&self.inner)?
            .pending
            .iter()
            .filter(|((l, _), _)| *l == lane)
            .map(|(_, p)| p.clone())
            .collect())
    }

    async fn drain_usage(
        &self,
        lane: Lane,
        sync_id: Option<&str>,
        backlog_cap: u64,
        event: &EventBuilder<'_>,
    ) -> Result<DrainOutcome, RepoError> {
        let mut inner = lock_inner(&self.inner)?;
        if backlog_cap > 0 {
            let backlog = inner
                .outbox
                .iter()
                .filter(|r| r.lane == lane && r.sent_at.is_none())
                .count() as u64;
            if backlog >= backlog_cap {
                return Ok(DrainOutcome::Backlogged(backlog));
            }
        }
        let keys: Vec<(Lane, String)> = inner
            .pending
            .keys()
            .filter(|(l, id)| *l == lane && sync_id.is_none_or(|s| s == id))
            .cloned()
            .collect();
        let mut enqueued = 0;
        for key in keys {
            let Some(p) = inner.pending.remove(&key) else {
                continue;
            };
            if p.usage.is_zero() {
                continue;
            }
            enqueue(&mut inner, lane, p.tenant.as_deref(), &event(&p));
            enqueued += 1;
        }
        Ok(DrainOutcome::Enqueued(enqueued))
    }

    async fn usage(
        &self,
        tenant: Option<&str>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<UsageRow>, RepoError> {
        let inner = lock_inner(&self.inner)?;
        if from > to {
            return Ok(Vec::new());
        }
        Ok(inner
            .progress
            .iter()
            .filter(|((id, day), _)| {
                *day >= from
                    && *day <= to
                    && inner
                        .syncs
                        .get(id)
                        .is_some_and(|s| s.spec.tenant.as_deref() == tenant)
            })
            .map(|((id, day), (_, usage))| UsageRow {
                sync_id: id.clone(),
                day: *day,
                usage: *usage,
            })
            .collect())
    }
}

#[async_trait]
impl LabOutbox for MemStore {
    async fn enqueue_event(
        &self,
        tenant: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<Uuid, RepoError> {
        self.lane(Lane::Lab).enqueue_event(tenant, payload).await
    }

    async fn lease_events(&self, limit: u32, lease: Duration) -> Result<OutboxLease, RepoError> {
        self.lane(Lane::Lab).lease_events(limit, lease).await
    }

    async fn mark_events_sent(&self, lease: Uuid, ids: &[Uuid]) -> Result<u64, RepoError> {
        self.lane(Lane::Lab).mark_events_sent(lease, ids).await
    }

    async fn prune_sent_events(&self, older_than: Duration) -> Result<u64, RepoError> {
        self.lane(Lane::Lab).prune_sent_events(older_than).await
    }

    async fn defer_events(
        &self,
        lease: Uuid,
        ids: &[Uuid],
        backoff: RetryBackoff,
    ) -> Result<u64, RepoError> {
        self.lane(Lane::Lab).defer_events(lease, ids, backoff).await
    }

    async fn count_unsent(&self) -> Result<u64, RepoError> {
        self.lane(Lane::Lab).count_unsent().await
    }
}

#[async_trait]
impl LabOutbox for MemLane {
    async fn enqueue_event(
        &self,
        tenant: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<Uuid, RepoError> {
        Ok(enqueue(
            &mut *lock_inner(&self.inner)?,
            self.lane,
            tenant,
            payload,
        ))
    }

    async fn lease_events(&self, limit: u32, lease: Duration) -> Result<OutboxLease, RepoError> {
        let token = Uuid::new_v4();
        let now = Utc::now();
        let until = chrono::Duration::from_std(lease)
            .ok()
            .and_then(|d| now.checked_add_signed(d))
            .unwrap_or(DateTime::<Utc>::MAX_UTC);
        let mut inner = lock_inner(&self.inner)?;
        let mut events = Vec::new();
        // Rows are kept in insertion (= creation) order.
        for row in inner.outbox.iter_mut() {
            if events.len() >= limit as usize {
                break;
            }
            if row.lane == self.lane
                && row.sent_at.is_none()
                && row.leased_until.is_none_or(|t| t <= now)
                && row.next_attempt_at.is_none_or(|t| t <= now)
            {
                row.leased_until = Some(until);
                row.lease_token = Some(token);
                events.push(row.event.clone());
            }
        }
        Ok(OutboxLease { token, events })
    }

    async fn mark_events_sent(&self, lease: Uuid, ids: &[Uuid]) -> Result<u64, RepoError> {
        let now = Utc::now();
        let mut inner = lock_inner(&self.inner)?;
        let mut n = 0;
        for row in inner.outbox.iter_mut() {
            if row.lane == self.lane
                && row.sent_at.is_none()
                && row.lease_token == Some(lease)
                && ids.contains(&row.event.id)
            {
                row.sent_at = Some(now);
                row.leased_until = None;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn prune_sent_events(&self, older_than: Duration) -> Result<u64, RepoError> {
        let cutoff = chrono::Duration::from_std(older_than)
            .ok()
            .and_then(|d| Utc::now().checked_sub_signed(d))
            .unwrap_or(DateTime::<Utc>::MIN_UTC);
        let mut inner = lock_inner(&self.inner)?;
        let before = inner.outbox.len();
        let lane = self.lane;
        inner
            .outbox
            .retain(|row| row.lane != lane || row.sent_at.is_none_or(|t| t >= cutoff));
        Ok((before - inner.outbox.len()) as u64)
    }

    async fn defer_events(
        &self,
        lease: Uuid,
        ids: &[Uuid],
        backoff: RetryBackoff,
    ) -> Result<u64, RepoError> {
        let now = Utc::now();
        let mut inner = lock_inner(&self.inner)?;
        let mut n = 0;
        for row in inner.outbox.iter_mut() {
            if row.lane == self.lane
                && row.sent_at.is_none()
                && row.lease_token == Some(lease)
                && ids.contains(&row.event.id)
            {
                let wait = chrono::Duration::from_std(backoff.delay(row.attempts))
                    .unwrap_or(chrono::Duration::MAX);
                row.attempts = row.attempts.saturating_add(1);
                row.next_attempt_at = Some(
                    now.checked_add_signed(wait)
                        .unwrap_or(DateTime::<Utc>::MAX_UTC),
                );
                row.leased_until = None;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn count_unsent(&self) -> Result<u64, RepoError> {
        Ok(lock_inner(&self.inner)?
            .outbox
            .iter()
            .filter(|r| r.lane == self.lane && r.sent_at.is_none())
            .count() as u64)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::test_support::{invalid_specs, letter, pr, skipped_letter, spec, state};

    #[tokio::test]
    async fn state_round_trip() {
        let store = MemStore::new();
        store
            .create_sync(None, &spec("s1", None, "n"))
            .await
            .unwrap();
        assert!(store.load("s1").await.unwrap().is_none());
        let st = state("s1");
        store.save(&st).await.unwrap();
        assert_eq!(store.load("s1").await.unwrap(), Some(st.clone()));
        let mut st2 = st;
        st2.resyncing = true;
        store.save(&st2).await.unwrap();
        assert_eq!(store.load("s1").await.unwrap(), Some(st2));
    }

    /// Same contract as the Postgres foreign keys.
    #[tokio::test]
    async fn unknown_syncs_are_not_found_like_postgres() {
        let store = MemStore::new();
        assert!(matches!(
            store.save(&state("ghost")).await,
            Err(StoreError::NotFound(_))
        ));
        assert!(matches!(
            store.record_progress("ghost", &pr(1, 0, 0)).await,
            Err(StoreError::NotFound(_))
        ));
        store
            .create_sync(None, &spec("s", None, "n"))
            .await
            .unwrap();
        // One unknown sync fails the whole batch.
        assert!(matches!(
            store
                .push_dead_letters(&[letter("s", "1"), letter("ghost", "2")])
                .await,
            Err(StoreError::NotFound(_))
        ));
        assert!(
            store
                .list_dead_letters(None, "s", DeadLetterPage::default())
                .await
                .unwrap()
                .is_empty()
        );
        store.push_dead_letters(&[letter("s", "1")]).await.unwrap();
        store.record_progress("s", &pr(1, 0, 0)).await.unwrap();
        store.save(&state("s")).await.unwrap();

        // The test helper accepts anything.
        let loose = MemStore::permissive();
        loose.save(&state("ghost")).await.unwrap();
        loose
            .push_dead_letters(&[letter("ghost", "1")])
            .await
            .unwrap();
        loose.record_progress("ghost", &pr(1, 0, 0)).await.unwrap();
        assert!(loose.load("ghost").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn skipped_records_round_trip_as_dead_letters() {
        let store = MemStore::new();
        store
            .create_sync(None, &spec("s", None, "n"))
            .await
            .unwrap();
        let letters = vec![
            skipped_letter("s", "orders/3@1842"),
            letter("s", "1"),
            skipped_letter("s", "public.orders@0/16B3748"),
        ];
        store.push_dead_letters(&letters).await.unwrap();
        let got = store
            .list_dead_letters(None, "s", DeadLetterPage::default())
            .await
            .unwrap();
        assert_eq!(
            got.into_iter().map(|d| d.letter).collect::<Vec<_>>(),
            letters
        );
    }

    #[tokio::test]
    async fn invalid_specs_are_refused() {
        let store = MemStore::new();
        for bad in invalid_specs("s", None) {
            assert!(matches!(
                store.create_sync(None, &bad).await,
                Err(RepoError::Invalid(_))
            ));
        }
        store
            .create_sync(None, &spec("s", None, "n"))
            .await
            .unwrap();
        for bad in invalid_specs("s", None) {
            assert!(matches!(
                store.update_sync(None, &bad).await,
                Err(RepoError::Invalid(_))
            ));
        }
    }

    #[tokio::test]
    async fn crud_and_tenant_isolation() {
        let store = MemStore::new();
        let a = spec("a1", Some("ta"), "orders");
        let b = spec("b1", Some("tb"), "orders");
        let g = spec("g1", None, "orders");
        store.create_sync(Some("ta"), &a).await.unwrap();
        store.create_sync(Some("tb"), &b).await.unwrap();
        store.create_sync(None, &g).await.unwrap();

        // Same name in the same scope conflicts; id reuse conflicts.
        let dup = spec("a2", Some("ta"), "orders");
        assert!(matches!(
            store.create_sync(Some("ta"), &dup).await,
            Err(RepoError::Conflict(_))
        ));
        let id_reuse = spec("a1", Some("tb"), "other");
        assert!(matches!(
            store.create_sync(Some("tb"), &id_reuse).await,
            Err(RepoError::Conflict(_))
        ));
        // Tenant mismatch is invalid.
        assert!(matches!(
            store.create_sync(Some("tb"), &a).await,
            Err(RepoError::Invalid(_))
        ));

        assert!(store.get_sync(Some("tb"), "a1").await.unwrap().is_none());
        assert!(store.get_sync(None, "a1").await.unwrap().is_none());
        assert_eq!(
            store
                .get_sync(Some("ta"), "a1")
                .await
                .unwrap()
                .unwrap()
                .readable()
                .unwrap()
                .spec,
            a
        );
        let ids = |v: Vec<SyncRow>| v.iter().map(|s| s.id().to_owned()).collect::<Vec<_>>();
        assert_eq!(ids(store.list_syncs(Some("ta")).await.unwrap()), ["a1"]);
        assert_eq!(ids(store.list_syncs(None).await.unwrap()), ["g1"]);

        // Updating another tenant's sync is not found.
        let mut a_by_b = a.clone();
        a_by_b.tenant = Some("tb".into());
        assert!(matches!(
            store.update_sync(Some("tb"), &a_by_b).await,
            Err(RepoError::NotFound(_))
        ));
        let mut a2 = a.clone();
        a2.name = "orders-v2".into();
        assert_eq!(
            store.update_sync(Some("ta"), &a2).await.unwrap().spec.name,
            "orders-v2"
        );
        assert!(matches!(
            store.delete_sync(Some("tb"), "a1").await,
            Err(RepoError::NotFound(_))
        ));
        assert_eq!(store.list_all_for_boot().await.unwrap().len(), 3);
        store.delete_sync(Some("ta"), "a1").await.unwrap();
        assert!(store.get_sync(Some("ta"), "a1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_cascades_and_dead_letters_page() {
        let store = MemStore::new();
        store
            .create_sync(Some("t"), &spec("s", Some("t"), "n"))
            .await
            .unwrap();
        store
            .create_sync(Some("u"), &spec("o", Some("u"), "n"))
            .await
            .unwrap();
        store.save(&state("s")).await.unwrap();
        let letters: Vec<_> = (0..5).map(|i| letter("s", &i.to_string())).collect();
        store.push_dead_letters(&letters).await.unwrap();
        store.push_dead_letters(&[letter("o", "x")]).await.unwrap();
        store.record_progress("s", &pr(3, 1, 0)).await.unwrap();

        let p1 = store
            .list_dead_letters(
                Some("t"),
                "s",
                DeadLetterPage {
                    after: None,
                    limit: 2,
                },
            )
            .await
            .unwrap();
        assert_eq!(p1.len(), 2);
        let p2 = store
            .list_dead_letters(
                Some("t"),
                "s",
                DeadLetterPage {
                    after: Some(p1[1].id),
                    limit: 10,
                },
            )
            .await
            .unwrap();
        assert_eq!(p2.len(), 3);
        assert!(matches!(
            store
                .list_dead_letters(Some("u"), "s", DeadLetterPage::default())
                .await,
            Err(RepoError::NotFound(_))
        ));
        // The other sync's letter id is ignored.
        let other = store
            .list_dead_letters(Some("u"), "o", DeadLetterPage::default())
            .await
            .unwrap();
        assert_eq!(
            store
                .delete_dead_letters(Some("t"), "s", &[p1[0].id, other[0].id])
                .await
                .unwrap(),
            1
        );

        store.delete_sync(Some("t"), "s").await.unwrap();
        assert!(store.load("s").await.unwrap().is_none());
        let inner = store.lock().unwrap();
        assert!(inner.dead_letters.values().all(|d| d.sync_id == "o"));
        assert!(inner.progress.keys().all(|(id, _)| id != "s"));
    }

    #[tokio::test]
    async fn progress_adds_up() {
        let store = MemStore::new();
        store
            .create_sync(None, &spec("s", None, "n"))
            .await
            .unwrap();
        store.record_progress("s", &pr(3, 1, 0)).await.unwrap();
        store.record_progress("s", &pr(2, 0, 4)).await.unwrap();
        let today = Utc::now().date_naive();
        let p = store.progress(None, "s", today, today).await.unwrap();
        assert_eq!(
            p,
            [DailyProgress {
                day: today,
                written: 5,
                deleted: 1,
                rejected: 4,
                dead_letters_dropped: 0,
                usage: Default::default(),
            }]
        );
        assert!(matches!(
            store.progress(Some("x"), "s", today, today).await,
            Err(RepoError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn usage_is_durable_per_lane() {
        crate::test_support::ledger_contract(&MemStore::new()).await;
    }

    #[tokio::test]
    async fn units_add_up_per_day() {
        let store = MemStore::new();
        store
            .create_sync(None, &spec("s", None, "n"))
            .await
            .unwrap();
        store
            .record_progress("s", &crate::test_support::units(2, 3, 4, 100, 7))
            .await
            .unwrap();
        store
            .record_progress("s", &crate::test_support::units(1, 0, 0, 10, 1))
            .await
            .unwrap();
        let today = Utc::now().date_naive();
        let p = store.progress(None, "s", today, today).await.unwrap();
        assert_eq!(
            p[0].usage,
            Usage {
                documents_streamed: 3,
                documents_snapshot: 3,
                documents_resync: 4,
                bytes: 110,
                sync_seconds: 8,
            }
        );
        // The API shape: units flattened next to the counters.
        let json = serde_json::to_value(&p[0]).unwrap();
        assert_eq!(json["documents_streamed"], 3);
        assert_eq!(json["sync_seconds"], 8);
    }

    #[tokio::test]
    async fn dead_letters_are_capped_per_sync_and_bounded_in_size() {
        let store = MemStore::new().with_dead_letter_limits(DeadLetterLimits {
            max_per_sync: 3,
            max_payload_bytes: 64,
        });
        for id in ["a", "b"] {
            store.create_sync(None, &spec(id, None, id)).await.unwrap();
        }
        let mut big = letter("a", "big");
        big.payload = serde_json::json!({"doc": "x".repeat(1000)});
        big.reason = "r".repeat(10_000);
        store.push_dead_letters(&[big]).await.unwrap();
        for i in 0..5 {
            store
                .push_dead_letters(&[letter("a", &i.to_string())])
                .await
                .unwrap();
        }
        store.push_dead_letters(&[letter("b", "1")]).await.unwrap();
        let a = store
            .list_dead_letters(None, "a", DeadLetterPage::default())
            .await
            .unwrap();
        assert_eq!(a.len(), 3, "capped");
        assert_eq!(
            a[0].letter.payload,
            serde_json::json!({"truncated": true, "bytes": 1010})
        );
        assert!(a[0].letter.reason.chars().count() <= 2_049);
        let today = Utc::now().date_naive();
        let p = store.progress(None, "a", today, today).await.unwrap();
        assert_eq!(p[0].dead_letters_dropped, 3);
        // Another sync has its own budget; clearing frees room.
        assert_eq!(
            store
                .list_dead_letters(None, "b", DeadLetterPage::default())
                .await
                .unwrap()
                .len(),
            1
        );
        store.clear_dead_letters(None, "a").await.unwrap();
        store
            .push_dead_letters(&[letter("a", "again")])
            .await
            .unwrap();
        assert_eq!(
            store
                .list_dead_letters(None, "a", DeadLetterPage::default())
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn deferred_events_wait_for_their_backoff() {
        let store = MemStore::new();
        let id = store
            .enqueue_event(Some("t"), &serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(store.count_unsent().await.unwrap(), 1);
        let lease = store
            .lease_events(10, Duration::from_millis(10))
            .await
            .unwrap();
        let backoff = RetryBackoff {
            base: Duration::from_secs(60),
            max: Duration::from_secs(3600),
        };
        assert_eq!(
            store
                .defer_events(lease.token, &[id], backoff)
                .await
                .unwrap(),
            1
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        // The lease is over, but the event waits for its backoff.
        let again = store
            .lease_events(10, Duration::from_secs(1))
            .await
            .unwrap();
        assert!(again.events.is_empty());
        assert_eq!(store.count_unsent().await.unwrap(), 1);
        assert_eq!(backoff.delay(0), Duration::from_secs(60));
        assert_eq!(backoff.delay(3), Duration::from_secs(480));
        assert_eq!(backoff.delay(30), Duration::from_secs(3600));
    }

    #[tokio::test]
    async fn outbox_leases() {
        let store = MemStore::new();
        let ids: Vec<Uuid> = enqueue_three(&store).await;
        let first = store
            .lease_events(2, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(
            first.events.iter().map(|e| e.id).collect::<Vec<_>>(),
            ids[..2].to_vec()
        );
        let second = store
            .lease_events(10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert_ne!(first.token, second.token);
        assert!(
            store
                .lease_events(10, Duration::from_secs(60))
                .await
                .unwrap()
                .events
                .is_empty()
        );
        // Each lease marks only its own events.
        assert_eq!(store.mark_events_sent(first.token, &ids).await.unwrap(), 2);
        assert_eq!(store.mark_events_sent(second.token, &ids).await.unwrap(), 1);
        assert_eq!(store.mark_events_sent(second.token, &ids).await.unwrap(), 0);

        // An expired lease frees the event.
        let id = store
            .enqueue_event(None, &serde_json::json!({}))
            .await
            .unwrap();
        store.lease_events(1, Duration::ZERO).await.unwrap();
        let again = store
            .lease_events(1, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(again.events[0].id, id);
    }

    #[tokio::test]
    async fn outbox_stale_lease_cannot_mark_sent() {
        let store = MemStore::new();
        let id = store
            .enqueue_event(None, &serde_json::json!({}))
            .await
            .unwrap();
        let stale = store.lease_events(1, Duration::ZERO).await.unwrap();
        let fresh = store
            .lease_events(1, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(fresh.events[0].id, id);
        assert_eq!(store.mark_events_sent(stale.token, &[id]).await.unwrap(), 0);
        assert_eq!(store.mark_events_sent(fresh.token, &[id]).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn outbox_prunes_only_old_sent_events() {
        let store = MemStore::new();
        let ids = enqueue_three(&store).await;
        let lease = store
            .lease_events(2, Duration::from_secs(60))
            .await
            .unwrap();
        store.mark_events_sent(lease.token, &ids).await.unwrap();
        // The first was sent two hours ago.
        store.lock().unwrap().outbox[0].sent_at = Some(Utc::now() - chrono::Duration::hours(2));
        assert_eq!(
            store
                .prune_sent_events(Duration::from_secs(3600))
                .await
                .unwrap(),
            1
        );
        assert_eq!(store.prune_sent_events(Duration::ZERO).await.unwrap(), 1);
        // The unsent one stays, whatever its age.
        assert_eq!(store.prune_sent_events(Duration::ZERO).await.unwrap(), 0);
        let rest = store
            .lease_events(10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(rest.events.len(), 1);
        assert_eq!(rest.events[0].id, ids[2]);
    }

    async fn enqueue_three(store: &MemStore) -> Vec<Uuid> {
        let mut ids = Vec::new();
        for i in 0..3 {
            ids.push(
                store
                    .enqueue_event(Some("t"), &serde_json::json!({ "n": i }))
                    .await
                    .unwrap(),
            );
        }
        ids
    }
}
