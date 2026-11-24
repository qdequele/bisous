//! `sync.state_changed`: tells the Lab when a tenant's sync changes phase,
//! through the same signed outbox as usage (`crate::lab`).
//!
//! Every phase the engine or the manager persists goes through
//! [`crate::usage::MeteredStore::save`], which reports it here
//! ([`SyncEvents::observe`]); a deletion is reported by the manager
//! ([`SyncEvents::deleted`]). Only Lab tenants' syncs are tracked, and only
//! when the Lab events are configured.
//!
//! **No floods.** A change is not sent at once: it waits until its phase has
//! held for the settle window ([`DEFAULT_SETTLE`]). A newer change replaces
//! a waiting one, and a change back to the phase the Lab was last told
//! about cancels it. So a sync flapping between streaming and an error
//! state sends nothing until it stays in one, then one event; at most one
//! event per sync per settle window in any case. One exception: a snapshot
//! that ends (`snapshotting` → `streaming`) is always reported, even when
//! the Lab was told `streaming` last (a quick resync), and a deletion is
//! reported at the next tick (a change still waiting is then dropped: the
//! deletion supersedes it). `from` is always the phase right before `to`.
//!
//! Events are built when their change is sent to the outbox queue, with
//! `occurred_at` the time of the change; the outbox row id (a UUIDv7 made
//! at enqueue) is the event id, kept across delivery retries. A queue that
//! cannot be enqueued (store down, `LAB_OUTBOX_MAX_PENDING` reached) is
//! retried at the next tick. A change still waiting when the process stops
//! gracefully is sent then; one waiting at a crash is lost (the Lab then
//! learns the phase at the next change).
//!
//! `sync.health_changed` (`crate::sync_health`) rides the same machinery:
//! per (sync, cause) status changes wait for the same settle window, go
//! through the same queue and outbox, and a deleted or forgotten sync drops
//! its health state with the rest.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use meili_connect_core::{SyncPhase, SyncSpec, SyncState};
use meili_connect_store::LabOutbox;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

use crate::limits::LogLimiter;
use crate::reason;
use crate::sync_health::{self, Cause, HealthTrack, Reading};
use crate::usage::{PRODUCT, description};

/// The Lab event type.
pub const EVENT_TYPE: &str = "sync.state_changed";
/// How long a new phase must hold before it is reported.
pub const DEFAULT_SETTLE: Duration = Duration::from_secs(30);
/// How often waiting changes are looked at.
pub const TICK: Duration = Duration::from_secs(5);
/// Events waiting for the outbox beyond this are dropped (oldest first).
const MAX_QUEUED: usize = 10_000;
/// Longest `message` sent.
const MAX_MESSAGE: usize = 1_000;

/// A phase of the contract's `syncState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reported {
    Phase(SyncPhase),
    Deleted,
}

impl Reported {
    pub fn as_str(self) -> &'static str {
        match self {
            Reported::Deleted => "deleted",
            Reported::Phase(p) => match p {
                SyncPhase::Pending => "pending",
                SyncPhase::Snapshotting => "snapshotting",
                SyncPhase::Streaming => "streaming",
                SyncPhase::Paused => "paused",
                SyncPhase::SourcePaused => "source_paused",
                SyncPhase::Held => "held",
                SyncPhase::Failed => "failed",
            },
        }
    }
}

#[derive(Debug, Clone)]
struct Change {
    from: SyncPhase,
    to: Reported,
    resync: Option<bool>,
    message: Option<String>,
    at: DateTime<Utc>,
    since: Instant,
}

#[derive(Debug, Clone, Copy)]
struct Observed {
    phase: SyncPhase,
    resyncing: bool,
}

#[derive(Debug)]
struct Tracked {
    tenant: String,
    label: String,
    /// The phase persisted last (`None` until the first save after boot).
    current: Option<Observed>,
    /// What the Lab was told last (or the phase found at the first save).
    reported: Option<SyncPhase>,
    pending: Option<Change>,
    /// `sync.health_changed`, per cause (`crate::sync_health`).
    health: BTreeMap<Cause, HealthTrack>,
}

#[derive(Debug, Default)]
struct Inner {
    syncs: HashMap<String, Tracked>,
    /// Built events waiting for the outbox: `(tenant, payload)`.
    queue: VecDeque<(String, Value)>,
}

pub struct SyncEvents {
    outbox: Option<Arc<dyn LabOutbox>>,
    settle: Duration,
    backlog_cap: u64,
    inner: Mutex<Inner>,
    log: Mutex<LogLimiter>,
}

impl std::fmt::Debug for SyncEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncEvents")
            .field("enabled", &self.outbox.is_some())
            .field("settle", &self.settle)
            .finish_non_exhaustive()
    }
}

/// The `sync.state_changed` event of a change, without its `id` (the outbox
/// row id, set when it is sent).
fn event(sync_id: &str, tenant: &str, label: &str, c: &Change) -> Value {
    let mut data = Map::new();
    data.insert("sync_id".into(), json!(sync_id));
    data.insert("sync_name".into(), json!(label));
    data.insert("from".into(), json!(Reported::Phase(c.from).as_str()));
    data.insert("to".into(), json!(c.to.as_str()));
    if let Some(resync) = c.resync {
        data.insert("resync".into(), json!(resync));
    }
    let reason = match c.to {
        Reported::Phase(phase) => reason::for_phase(phase, c.message.as_deref()),
        Reported::Deleted => None,
    };
    if let Some(reason) = reason {
        data.insert("reason".into(), json!(reason.code()));
    }
    if let Some(message) = c.message.as_deref().filter(|m| !m.is_empty()) {
        data.insert("message".into(), json!(truncate(message, MAX_MESSAGE)));
    }
    json!({
        "type": EVENT_TYPE,
        "occurred_at": c.at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "account_id": tenant,
        "api_key_id": null,
        "product": PRODUCT,
        "data": data,
    })
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl SyncEvents {
    /// Reports nothing (no Lab).
    pub fn disabled() -> Self {
        Self::build(None)
    }

    /// Reports to the Lab lane's outbox.
    pub fn new(outbox: Arc<dyn LabOutbox>) -> Self {
        Self::build(Some(outbox))
    }

    fn build(outbox: Option<Arc<dyn LabOutbox>>) -> Self {
        Self {
            outbox,
            settle: DEFAULT_SETTLE,
            backlog_cap: 0,
            inner: Mutex::new(Inner::default()),
            log: Mutex::new(LogLimiter::new(Duration::from_secs(300))),
        }
    }

    /// How long a new phase must hold before it is reported.
    pub fn with_settle(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    /// Keep events back while the outbox holds `cap` unsent ones (`0`: no
    /// cap), like usage (`LAB_OUTBOX_MAX_PENDING`).
    pub fn with_backlog_cap(mut self, cap: u64) -> Self {
        self.backlog_cap = cap;
        self
    }

    pub fn is_enabled(&self) -> bool {
        self.outbox.is_some()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Track a tenant's sync (idempotent; a new spec renames it). Global
    /// syncs are never reported.
    pub fn register(&self, spec: &SyncSpec) {
        let (Some(_), Some(tenant)) = (&self.outbox, &spec.tenant) else {
            return;
        };
        let label = description(spec);
        let mut inner = self.lock();
        match inner.syncs.get_mut(&spec.id) {
            Some(t) => {
                t.tenant = tenant.clone();
                t.label = label;
            }
            None => {
                inner.syncs.insert(
                    spec.id.clone(),
                    Tracked {
                        tenant: tenant.clone(),
                        label,
                        current: None,
                        reported: None,
                        pending: None,
                        health: BTreeMap::new(),
                    },
                );
            }
        }
    }

    /// Whether the sync is reported (a Lab tenant's, with the Lab events
    /// on).
    pub fn tracks(&self, sync_id: &str) -> bool {
        self.lock().syncs.contains_key(sync_id)
    }

    /// A health measure of a tracked sync (`crate::sync_health`): a status
    /// change waits for the settle window like a phase change. `state` is
    /// the sync's phase when it was measured.
    pub fn observe_health(
        &self,
        sync_id: &str,
        cause: Cause,
        reading: Reading,
        state: Option<SyncPhase>,
    ) {
        let mut inner = self.lock();
        if let Some(t) = inner.syncs.get_mut(sync_id) {
            t.health.entry(cause).or_default().observe(reading, state);
        }
    }

    /// Stop tracking a sync without reporting anything (a creation rolled
    /// back).
    pub fn forget(&self, sync_id: &str) {
        self.lock().syncs.remove(sync_id);
    }

    /// Whether the next save of this sync needs its previous phase first
    /// ([`SyncEvents::baseline`]).
    pub fn needs_baseline(&self, sync_id: &str) -> bool {
        self.lock()
            .syncs
            .get(sync_id)
            .is_some_and(|t| t.current.is_none())
    }

    /// The phase stored before this process saved anything for the sync
    /// (`None`: no state yet, so `pending`).
    pub fn baseline(&self, sync_id: &str, saved: Option<&SyncState>) {
        let mut inner = self.lock();
        if let Some(t) = inner.syncs.get_mut(sync_id)
            && t.current.is_none()
        {
            let observed = saved.map_or(
                Observed {
                    phase: SyncPhase::Pending,
                    resyncing: false,
                },
                |s| Observed {
                    phase: s.phase,
                    resyncing: s.resyncing,
                },
            );
            t.current = Some(observed);
            t.reported.get_or_insert(observed.phase);
        }
    }

    /// A state was saved.
    pub fn observe(&self, state: &SyncState) {
        let mut inner = self.lock();
        let Some(t) = inner.syncs.get_mut(&state.sync_id) else {
            return;
        };
        let Some(current) = t.current else {
            // No baseline (it could not be read): this save is the baseline.
            t.current = Some(Observed {
                phase: state.phase,
                resyncing: state.resyncing,
            });
            t.reported.get_or_insert(state.phase);
            return;
        };
        let to = state.phase;
        if to == current.phase {
            // No change; a waiting change takes the latest message.
            if let Some(p) = &mut t.pending
                && p.to == Reported::Phase(to)
            {
                p.message.clone_from(&state.message);
            }
            if to == SyncPhase::Snapshotting {
                t.current = Some(Observed {
                    phase: to,
                    resyncing: current.resyncing || state.resyncing,
                });
            }
            return;
        }
        let from = current.phase;
        let resync = if to == SyncPhase::Snapshotting {
            Some(state.resyncing)
        } else if from == SyncPhase::Snapshotting {
            Some(current.resyncing)
        } else {
            None
        };
        t.current = Some(Observed {
            phase: to,
            resyncing: state.resyncing,
        });
        let snapshot_end = from == SyncPhase::Snapshotting && to == SyncPhase::Streaming;
        if t.reported == Some(to) && !snapshot_end {
            // Back where the Lab thinks it is: nothing to tell.
            t.pending = None;
            return;
        }
        t.pending = Some(Change {
            from,
            to: Reported::Phase(to),
            resync,
            message: state.message.clone(),
            at: Utc::now(),
            since: Instant::now(),
        });
    }

    /// The sync was deleted: reported at the next tick, then forgotten.
    pub fn deleted(&self, sync_id: &str) {
        let mut inner = self.lock();
        let Some(t) = inner.syncs.remove(sync_id) else {
            return;
        };
        let from = t
            .current
            .map(|c| c.phase)
            .or(t.reported)
            .unwrap_or(SyncPhase::Pending);
        let change = Change {
            from,
            to: Reported::Deleted,
            resync: None,
            message: None,
            at: Utc::now(),
            since: Instant::now(),
        };
        let payload = event(sync_id, &t.tenant, &t.label, &change);
        push(&mut inner.queue, t.tenant, payload);
    }

    /// Move the changes that settled (all of them with `force`) to the
    /// queue, then enqueue the queue into the outbox. Returns how many
    /// events were enqueued.
    pub async fn flush(&self, force: bool) -> usize {
        let Some(outbox) = &self.outbox else {
            return 0;
        };
        {
            let mut inner = self.lock();
            let Inner { syncs, queue } = &mut *inner;
            for (id, t) in syncs.iter_mut() {
                let due = t
                    .pending
                    .as_ref()
                    .is_some_and(|p| force || p.since.elapsed() >= self.settle);
                if due && let Some(change) = t.pending.take() {
                    if let Reported::Phase(p) = change.to {
                        t.reported = Some(p);
                    }
                    push(
                        queue,
                        t.tenant.clone(),
                        event(id, &t.tenant, &t.label, &change),
                    );
                }
                for (cause, track) in &mut t.health {
                    if let Some(change) = track.take_due(self.settle, force) {
                        push(
                            queue,
                            t.tenant.clone(),
                            sync_health::event(id, &t.tenant, &t.label, *cause, &change),
                        );
                    }
                }
            }
            if queue.is_empty() {
                return 0;
            }
        }
        if self.backlog_cap > 0 {
            match outbox.count_unsent().await {
                Ok(n) if n >= self.backlog_cap => {
                    self.warn(format_args!(
                        "the Lab events outbox is full (LAB_OUTBOX_MAX_PENDING): sync state \
                         changes wait in memory"
                    ));
                    return 0;
                }
                _ => {}
            }
        }
        let mut sent = 0;
        loop {
            let Some((tenant, payload)) = self.lock().queue.pop_front() else {
                break;
            };
            match outbox.enqueue_event(Some(&tenant), &payload).await {
                Ok(_) => sent += 1,
                Err(e) => {
                    self.lock().queue.push_front((tenant, payload));
                    self.warn(format_args!(
                        "cannot enqueue sync state or health changes ({e}); retrying"
                    ));
                    break;
                }
            }
        }
        sent
    }

    fn warn(&self, what: std::fmt::Arguments<'_>) {
        let allowed = self.log.lock().unwrap_or_else(|p| p.into_inner()).allow();
        if let Some(suppressed) = allowed {
            tracing::warn!(suppressed, "{what}");
        }
    }

    /// Flush every [`TICK`] until `cancel`, then once more with everything
    /// still waiting.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(TICK) => {}
            }
            self.flush(false).await;
        }
        self.flush(true).await;
    }
}

fn push(queue: &mut VecDeque<(String, Value)>, tenant: String, payload: Value) {
    if queue.len() >= MAX_QUEUED {
        queue.pop_front();
        tracing::warn!("too many sync state changes wait for the outbox; the oldest is dropped");
    }
    queue.push_back((tenant, payload));
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_core::{MeiliDestination, Secret, SourceKind, SourceSpec, StreamMapping};
    use meili_connect_store::MemStore;

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

    fn state(id: &str, phase: SyncPhase, resyncing: bool, message: Option<&str>) -> SyncState {
        SyncState {
            sync_id: id.into(),
            phase,
            checkpoint: None,
            message: message.map(str::to_owned),
            resyncing,
            resync_reason: None,
            updated_at: Utc::now(),
        }
    }

    fn setup(settle: Duration) -> (Arc<MemStore>, SyncEvents) {
        let store = Arc::new(MemStore::new());
        let events = SyncEvents::new(store.clone() as Arc<dyn LabOutbox>).with_settle(settle);
        events.register(&spec("s1", Some(TENANT)));
        (store, events)
    }

    /// Saves `phase` the way `MeteredStore::save` reports it.
    fn save(events: &SyncEvents, s: &SyncState) {
        if events.needs_baseline(&s.sync_id) {
            events.baseline(&s.sync_id, None);
        }
        events.observe(s);
    }

    /// A Lab account id (the contract wants a UUID).
    const TENANT: &str = "0190c5a0-0000-7000-8000-000000000000";

    /// The vendored Lab contract (`contracts/vendor/lab/`).
    const SCHEMA: &str = include_str!("../../../contracts/vendor/lab/lab-events.schema.json");

    /// Fails unless `event` (with its id, as the sender sends it) is a
    /// valid Lab event.
    pub(crate) fn assert_valid(event: &Value, id: uuid::Uuid) {
        let schema: Value = serde_json::from_str(SCHEMA).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let mut sent = event.clone();
        sent["id"] = json!(id.to_string());
        let errors: Vec<String> = validator
            .iter_errors(&sent)
            .map(|e| format!("{e} at {}", e.instance_path()))
            .collect();
        assert!(errors.is_empty(), "{sent}: {errors:?}");
    }

    /// Every event in the outbox, each checked against the contract.
    async fn sent(store: &MemStore) -> Vec<Value> {
        let lease = store
            .lease_events(100, Duration::from_secs(60))
            .await
            .unwrap();
        for e in &lease.events {
            assert_valid(&e.payload, e.id);
        }
        lease.events.into_iter().map(|e| e.payload).collect()
    }

    #[test]
    fn every_kind_of_change_matches_the_contract() {
        let id = uuid::Uuid::now_v7();
        let phases = [
            SyncPhase::Pending,
            SyncPhase::Snapshotting,
            SyncPhase::Streaming,
            SyncPhase::Paused,
            SyncPhase::SourcePaused,
            SyncPhase::Held,
            SyncPhase::Failed,
        ];
        let messages = [
            None,
            Some("out_of_credits".to_owned()),
            Some(format!("source configuration error: {}", "x".repeat(5000))),
        ];
        let mut targets: Vec<Reported> = phases.iter().map(|p| Reported::Phase(*p)).collect();
        targets.push(Reported::Deleted);
        for from in phases {
            for to in &targets {
                for message in &messages {
                    for resync in [None, Some(true)] {
                        let c = Change {
                            from,
                            to: *to,
                            resync,
                            message: message.clone(),
                            at: Utc::now(),
                            since: Instant::now(),
                        };
                        assert_valid(&event("s1", TENANT, "postgres → products", &c), id);
                    }
                }
            }
        }
        // Every reason code is one the contract accepts.
        for r in reason::Reason::ALL {
            let c = Change {
                from: SyncPhase::Streaming,
                to: Reported::Phase(SyncPhase::Held),
                resync: None,
                message: Some("m".into()),
                at: Utc::now(),
                since: Instant::now(),
            };
            let mut e = event("s1", TENANT, "l", &c);
            e["data"]["reason"] = json!(r.code());
            assert_valid(&e, id);
        }
    }

    #[tokio::test]
    async fn the_usage_event_matches_the_contract_too() {
        let e = crate::usage::lab_event(
            &meili_connect_store::PendingUsage {
                sync_id: "s".into(),
                tenant: Some(TENANT.into()),
                source_kind: "postgres".into(),
                sync_name: "s".into(),
                usage: meili_connect_core::Usage {
                    documents_streamed: 3,
                    ..Default::default()
                },
            },
            "postgres → s",
        );
        assert_valid(&e, uuid::Uuid::now_v7());
    }

    #[tokio::test]
    async fn a_first_sync_reports_the_snapshot_and_its_end() {
        let (store, events) = setup(Duration::ZERO);
        save(&events, &state("s1", SyncPhase::Snapshotting, false, None));
        assert_eq!(events.flush(false).await, 1);
        save(&events, &state("s1", SyncPhase::Streaming, false, None));
        assert_eq!(events.flush(false).await, 1);
        let all = sent(&store).await;
        assert_eq!(all[0]["type"], EVENT_TYPE);
        assert_eq!(all[0]["product"], "connect");
        assert_eq!(all[0]["account_id"], TENANT);
        assert_eq!(
            all[0]["data"],
            json!({"sync_id": "s1", "sync_name": "postgres → products",
                   "from": "pending", "to": "snapshotting", "resync": false})
        );
        assert_eq!(
            all[1]["data"],
            json!({"sync_id": "s1", "sync_name": "postgres → products",
                   "from": "snapshotting", "to": "streaming", "resync": false})
        );
    }

    #[tokio::test]
    async fn the_same_phase_twice_is_not_a_change() {
        let (store, events) = setup(Duration::ZERO);
        events.baseline("s1", Some(&state("s1", SyncPhase::Streaming, false, None)));
        // Checkpoints, a retried error's message: same phase.
        save(&events, &state("s1", SyncPhase::Streaming, false, None));
        save(
            &events,
            &state(
                "s1",
                SyncPhase::Streaming,
                false,
                Some("transient source error: x"),
            ),
        );
        assert_eq!(events.flush(true).await, 0);
        // A hold saved twice (engine, then supervisor) is one event with
        // the last message.
        save(
            &events,
            &state(
                "s1",
                SyncPhase::Held,
                false,
                Some("a batch would delete 9 of 10"),
            ),
        );
        save(
            &events,
            &state(
                "s1",
                SyncPhase::Held,
                false,
                Some("sync held: a batch would delete 9 of 10"),
            ),
        );
        assert_eq!(events.flush(false).await, 1);
        let all = sent(&store).await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0]["data"]["from"], "streaming");
        assert_eq!(all[0]["data"]["to"], "held");
        assert_eq!(all[0]["data"]["reason"], "delete_guard");
        assert_eq!(
            all[0]["data"]["message"],
            "sync held: a batch would delete 9 of 10"
        );
    }

    #[tokio::test]
    async fn flapping_sends_nothing_until_it_settles() {
        let (store, events) = setup(Duration::from_millis(300));
        events.baseline("s1", Some(&state("s1", SyncPhase::Streaming, false, None)));
        let paused = Some("source paused: the source is paused");
        for _ in 0..20 {
            save(
                &events,
                &state("s1", SyncPhase::SourcePaused, false, paused),
            );
            assert_eq!(events.flush(false).await, 0);
            save(&events, &state("s1", SyncPhase::Streaming, false, None));
            assert_eq!(events.flush(false).await, 0);
        }
        // Back to what the Lab was told: nothing waits.
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(events.flush(false).await, 0);
        // It stays paused: one event once it held for the window.
        save(
            &events,
            &state("s1", SyncPhase::SourcePaused, false, paused),
        );
        assert_eq!(events.flush(false).await, 0, "not settled yet");
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(events.flush(false).await, 1);
        let all = sent(&store).await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0]["data"]["to"], "source_paused");
        assert_eq!(all[0]["data"]["reason"], "source_unreachable");
    }

    #[tokio::test]
    async fn a_quick_resync_still_reports_its_end_with_the_flag() {
        let (store, events) = setup(Duration::from_secs(3600));
        events.baseline("s1", Some(&state("s1", SyncPhase::Streaming, false, None)));
        save(&events, &state("s1", SyncPhase::Snapshotting, true, None));
        save(&events, &state("s1", SyncPhase::Streaming, false, None));
        assert_eq!(events.flush(false).await, 0, "waits for the window");
        assert_eq!(events.flush(true).await, 1, "a graceful stop sends it");
        let all = sent(&store).await;
        assert_eq!(
            all[0]["data"],
            json!({"sync_id": "s1", "sync_name": "postgres → products",
                   "from": "snapshotting", "to": "streaming", "resync": true})
        );
    }

    #[tokio::test]
    async fn a_deletion_is_reported_and_forgets_the_sync() {
        let (store, events) = setup(Duration::from_secs(3600));
        events.baseline("s1", Some(&state("s1", SyncPhase::Paused, false, None)));
        events.deleted("s1");
        assert_eq!(events.flush(false).await, 1, "no settle window");
        save(&events, &state("s1", SyncPhase::Streaming, false, None));
        assert_eq!(events.flush(true).await, 0, "no longer tracked");
        let all = sent(&store).await;
        assert_eq!(all[0]["data"]["from"], "paused");
        assert_eq!(all[0]["data"]["to"], "deleted");
        assert!(all[0]["data"].get("reason").is_none());
    }

    #[tokio::test]
    async fn global_syncs_and_a_server_without_the_lab_report_nothing() {
        let (store, events) = setup(Duration::ZERO);
        events.register(&spec("g", None));
        save(&events, &state("g", SyncPhase::Held, false, Some("x")));
        events.deleted("g");
        assert_eq!(events.flush(true).await, 0);
        assert!(sent(&store).await.is_empty());

        let off = SyncEvents::disabled();
        off.register(&spec("s1", Some("t")));
        save(&off, &state("s1", SyncPhase::Held, false, Some("x")));
        off.deleted("s1");
        assert_eq!(off.flush(true).await, 0);
    }

    #[tokio::test]
    async fn out_of_credits_is_insufficient_credits() {
        let (store, events) = setup(Duration::ZERO);
        events.baseline("s1", Some(&state("s1", SyncPhase::Streaming, false, None)));
        save(
            &events,
            &state(
                "s1",
                SyncPhase::Held,
                false,
                Some(crate::credits::OUT_OF_CREDITS),
            ),
        );
        events.flush(false).await;
        let all = sent(&store).await;
        assert_eq!(all[0]["data"]["reason"], "insufficient_credits");
        assert_eq!(all[0]["data"]["message"], "out_of_credits");
    }

    #[tokio::test]
    async fn a_full_outbox_keeps_the_events_for_later() {
        let store = Arc::new(MemStore::new());
        let events = SyncEvents::new(store.clone() as Arc<dyn LabOutbox>)
            .with_settle(Duration::ZERO)
            .with_backlog_cap(1);
        events.register(&spec("s1", Some("t")));
        store.enqueue_event(Some("t"), &json!({})).await.unwrap();
        save(&events, &state("s1", SyncPhase::Snapshotting, false, None));
        assert_eq!(events.flush(false).await, 0);
        let lease = store
            .lease_events(10, Duration::from_secs(60))
            .await
            .unwrap();
        store
            .mark_events_sent(lease.token, &[lease.events[0].id])
            .await
            .unwrap();
        assert_eq!(events.flush(false).await, 1, "sent once there is room");
    }

    // ------------------------------------------------ sync.health_changed

    use crate::sync_health::{self as health, Cause, HealthConfig, Reading};

    fn wal(bytes: u64, limit: Option<u64>) -> Reading {
        Reading::wal(
            Some(&meili_connect_core::Retention {
                bytes,
                limit_bytes: limit,
                status: Some("reserved".into()),
            }),
            &HealthConfig::default(),
        )
    }

    const GIB: u64 = 1 << 30;

    #[test]
    fn every_health_event_matches_the_contract() {
        let id = uuid::Uuid::now_v7();
        let readings = [wal(0, None), wal(3 * GIB, Some(GIB)), wal(5, Some(0))];
        let states = [
            None,
            Some(SyncPhase::Streaming),
            Some(SyncPhase::Paused),
            Some(SyncPhase::Held),
            Some(SyncPhase::Failed),
            Some(SyncPhase::SourcePaused),
            Some(SyncPhase::Snapshotting),
        ];
        for cause in [Cause::Lag, Cause::WalRetained] {
            for status in [health::Status::Ok, health::Status::Warning] {
                for reading in readings {
                    for state in states {
                        let c = health::HealthChange {
                            status,
                            reading,
                            state,
                            at: Utc::now(),
                            since: Instant::now(),
                        };
                        assert_valid(&health::event("s1", TENANT, "l", cause, &c), id);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn health_changes_are_reported_per_cause_once_settled() {
        let (store, events) = setup(Duration::from_millis(300));
        events.baseline("s1", Some(&state("s1", SyncPhase::Paused, false, None)));
        let paused = Some(SyncPhase::Paused);
        // A paused sync's slot over its cap's half: warning (settled).
        events.observe_health("s1", Cause::WalRetained, wal(600 << 20, Some(GIB)), paused);
        assert_eq!(events.flush(false).await, 0, "not settled yet");
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(events.flush(false).await, 1);
        // Lag is another cause: its own change.
        let lag = Reading::lag(1000.0, &HealthConfig::default());
        events.observe_health("s1", Cause::Lag, lag, Some(SyncPhase::Streaming));
        assert_eq!(events.flush(true).await, 1);
        // Hovering in the band, or still over: nothing.
        for b in [GIB / 2 - 1, 2 * GIB, GIB * 9 / 20] {
            events.observe_health("s1", Cause::WalRetained, wal(b, Some(GIB)), paused);
        }
        assert_eq!(events.flush(true).await, 0);
        // Flapping across inside the window: nothing.
        for _ in 0..10 {
            events.observe_health("s1", Cause::WalRetained, wal(0, Some(GIB)), paused);
            events.observe_health("s1", Cause::WalRetained, wal(GIB, Some(GIB)), paused);
        }
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(events.flush(false).await, 0, "back where the Lab is");
        // Well under: ok.
        events.observe_health("s1", Cause::WalRetained, wal(GIB / 10, Some(GIB)), paused);
        assert_eq!(events.flush(true).await, 1);
        let all = sent(&store).await;
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|e| e["type"] == health::EVENT_TYPE));
        assert_eq!(
            all[0]["data"],
            json!({"sync_id": "s1", "sync_name": "postgres → products", "status": "warning",
                   "cause": "wal_retained", "state": "paused",
                   "retained_wal_bytes": 600u64 << 20, "threshold": GIB / 2,
                   "source_limit_bytes": GIB})
        );
        assert_eq!(all[1]["data"]["cause"], "lag");
        assert_eq!(all[1]["data"]["lag_seconds"], 1000);
        assert_eq!(all[1]["data"]["threshold"], 900);
        assert_eq!(all[2]["data"]["status"], "ok");
        assert_eq!(all[2]["data"]["retained_wal_bytes"], GIB / 10);
    }

    #[tokio::test]
    async fn a_deleted_sync_drops_its_pending_health() {
        let (store, events) = setup(Duration::from_secs(3600));
        events.baseline("s1", Some(&state("s1", SyncPhase::Failed, false, None)));
        events.observe_health("s1", Cause::WalRetained, wal(2 * GIB, None), None);
        events.deleted("s1");
        assert_eq!(events.flush(true).await, 1, "only the deletion");
        let all = sent(&store).await;
        assert_eq!(all[0]["type"], EVENT_TYPE);
        // Measures of an untracked sync are ignored.
        events.observe_health("s1", Cause::WalRetained, wal(2 * GIB, None), None);
        assert!(!events.tracks("s1"));
        assert_eq!(events.flush(true).await, 0);
    }

    #[tokio::test]
    async fn health_of_global_syncs_and_without_the_lab_is_not_reported() {
        let (store, events) = setup(Duration::ZERO);
        events.register(&spec("g", None));
        assert!(!events.tracks("g"));
        events.observe_health("g", Cause::WalRetained, wal(2 * GIB, None), None);
        assert_eq!(events.flush(true).await, 0);
        assert!(sent(&store).await.is_empty());
        let off = SyncEvents::disabled();
        off.register(&spec("s1", Some(TENANT)));
        off.observe_health("s1", Cause::WalRetained, wal(2 * GIB, None), None);
        assert_eq!(off.flush(true).await, 0);
    }

    #[test]
    fn long_messages_are_cut_on_a_char_boundary() {
        assert_eq!(truncate("héllo", 2), "h");
        assert_eq!(truncate("abc", 10), "abc");
    }
}
