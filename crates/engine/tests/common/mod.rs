//! Fakes for the engine tests: a scripted origin + source, a HashMap-backed
//! sink and an in-memory store.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, ApplyReport, Capabilities, Change, CheckReport, Checkpoint, CollectionInfo,
    DeadLetter, DocOp, Document, EventSender, FieldSelection, IndexTarget, MeiliDestination, Op,
    Opened, RejectedDoc, Secret, Sink, SinkError, Source, SourceError, SourceEvent, SourceFactory,
    SourceKind, SourceSpec, StateStore, StoreError, StreamMapping, SyncPhase, SyncSpec, SyncState,
    document_id,
};
use meili_connect_engine::{SyncDeps, SyncOptions};
use serde_json::{Value, json};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

pub const COLLECTION: &str = "public.products";
pub const INDEX: &str = "products";
pub const SYNC_ID: &str = "0123456789abcdef";

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

pub fn doc(v: Value) -> Document {
    match v {
        Value::Object(m) => m,
        _ => Document::new(),
    }
}

pub fn pos(cp: &Checkpoint) -> u64 {
    cp.0.as_u64().unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------- origin

/// The fake database. Its change log is a list of events; a `Checkpoint`
/// event in the log is a commit marker, emitted as `Checkpoint(json!(n))`
/// where `n` is the log position right after it.
#[derive(Default)]
pub struct Origin {
    pub st: Mutex<OriginState>,
    notify: Notify,
}

pub struct OriginState {
    pub rows: BTreeMap<(String, String), (Vec<Value>, Document)>,
    pub log: Vec<SourceEvent>,
    pub capabilities: Capabilities,
    pub open_faults: VecDeque<SourceError>,
    /// For each `stream` call: fail after sending this many events.
    pub stream_faults: VecDeque<(usize, SourceError)>,
    /// Resuming before this position is `PositionLost`.
    pub min_resume: u64,
    /// Appended to the log right after the first snapshot row is sent.
    pub during_snapshot: Vec<(Change, bool)>,
    pub snapshot_delay: Duration,
    pub opens: Vec<Option<Checkpoint>>,
    pub snapshots: usize,
    pub sent: usize,
    pub acks: Vec<Checkpoint>,
    pub ack_violations: Vec<String>,
    pub stream_active: bool,
    pub store: Option<Arc<MemStore>>,
}

impl Default for OriginState {
    fn default() -> Self {
        Self {
            rows: BTreeMap::new(),
            log: Vec::new(),
            capabilities: Capabilities {
                snapshot: true,
                patches: true,
            },
            open_faults: VecDeque::new(),
            stream_faults: VecDeque::new(),
            min_resume: 0,
            during_snapshot: Vec::new(),
            snapshot_delay: Duration::ZERO,
            opens: Vec::new(),
            snapshots: 0,
            sent: 0,
            acks: Vec::new(),
            ack_violations: Vec::new(),
            stream_active: false,
            store: None,
        }
    }
}

impl OriginState {
    fn apply(&mut self, change: &Change) {
        let key = (change.collection.clone(), document_id(&change.key));
        match &change.op {
            Op::Upsert { doc } => {
                self.rows.insert(key, (change.key.clone(), doc.clone()));
            }
            Op::Patch { doc, unset } => {
                let entry = self
                    .rows
                    .entry(key)
                    .or_insert_with(|| (change.key.clone(), Document::new()));
                entry.1.extend(doc.clone());
                for f in unset {
                    entry.1.remove(f);
                }
            }
            Op::Delete => {
                self.rows.remove(&key);
            }
        }
    }

    fn record(&mut self, change: Change, commit: bool) {
        self.apply(&change);
        self.log.push(SourceEvent::Change(change));
        if commit {
            self.log.push(SourceEvent::Checkpoint(Checkpoint(json!(0))));
        }
    }
}

pub fn upsert(id: i64, v: Value) -> Change {
    Change {
        collection: COLLECTION.into(),
        key: vec![json!(id)],
        op: Op::Upsert { doc: doc(v) },
    }
}

pub fn patch(id: i64, v: Value, unset: &[&str]) -> Change {
    Change {
        collection: COLLECTION.into(),
        key: vec![json!(id)],
        op: Op::Patch {
            doc: doc(v),
            unset: unset.iter().map(|s| s.to_string()).collect(),
        },
    }
}

pub fn delete(id: i64) -> Change {
    Change {
        collection: COLLECTION.into(),
        key: vec![json!(id)],
        op: Op::Delete,
    }
}

impl Origin {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn state(&self) -> MutexGuard<'_, OriginState> {
        lock(&self.st)
    }

    /// Existing rows, not in the change log (data before the sync).
    pub fn seed(&self, changes: Vec<Change>) {
        let mut st = self.state();
        for c in &changes {
            st.apply(c);
        }
    }

    /// One transaction: the changes then a commit marker.
    pub fn tx(&self, changes: Vec<Change>) {
        {
            let mut st = self.state();
            let n = changes.len();
            for (i, c) in changes.into_iter().enumerate() {
                st.record(c, i + 1 == n);
            }
        }
        self.notify.notify_waiters();
    }

    /// Changes without a commit marker.
    pub fn uncommitted(&self, changes: Vec<Change>) {
        {
            let mut st = self.state();
            for c in changes {
                st.record(c, false);
            }
        }
        self.notify.notify_waiters();
    }

    pub fn truncate(&self, collection: &str) {
        {
            let mut st = self.state();
            st.rows.retain(|(c, _), _| c != collection);
            st.log.push(SourceEvent::Truncate {
                collection: collection.into(),
            });
            st.log.push(SourceEvent::Checkpoint(Checkpoint(json!(0))));
        }
        self.notify.notify_waiters();
    }

    pub fn log_len(&self) -> u64 {
        self.state().log.len() as u64
    }

    /// Rows as the destination should hold them for an identity mapping.
    pub fn expected(&self) -> BTreeMap<String, Document> {
        self.state()
            .rows
            .iter()
            .filter(|((c, _), _)| c == COLLECTION)
            .map(|((_, id), (_, d))| {
                let mut d = d.clone();
                d.insert("id".into(), Value::String(id.clone()));
                (id.clone(), d)
            })
            .collect()
    }

    fn check_ack(&self, ack: &Checkpoint) {
        let mut st = self.state();
        st.acks.push(ack.clone());
        let saved = st
            .store
            .as_ref()
            .and_then(|s| s.state(SYNC_ID))
            .and_then(|s| s.checkpoint);
        let ok = saved.as_ref().is_some_and(|s| pos(s) >= pos(ack));
        if !ok {
            st.ack_violations
                .push(format!("ack {ack:?} ahead of saved {saved:?}"));
        }
    }
}

pub struct FakeFactory(pub Arc<Origin>);

impl SourceFactory for FakeFactory {
    fn build(
        &self,
        _sync_id: &str,
        _spec: &SourceSpec,
        _streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        Ok(Box::new(FakeSource {
            origin: Arc::clone(&self.0),
            pos: 0,
        }))
    }
}

pub struct FakeSource {
    origin: Arc<Origin>,
    pos: usize,
}

struct Active(Arc<Origin>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.state().stream_active = false;
    }
}

#[async_trait]
impl Source for FakeSource {
    fn capabilities(&self) -> Capabilities {
        self.origin.state().capabilities
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        Ok(CheckReport::default())
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        Ok(Vec::new())
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        let mut st = self.origin.state();
        st.opens.push(resume.clone());
        if let Some(e) = st.open_faults.pop_front() {
            return Err(e);
        }
        match resume {
            Some(cp) => {
                let n = pos(&cp);
                if n < st.min_resume || n > st.log.len() as u64 {
                    return Err(SourceError::PositionLost(format!("position {n} is gone")));
                }
                self.pos = n as usize;
                Ok(Opened::Resumed)
            }
            None => {
                self.pos = st.log.len();
                Ok(Opened::Fresh {
                    start: Checkpoint(json!(self.pos)),
                })
            }
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let (rows, during, delay) = {
            let mut st = self.origin.state();
            st.snapshots += 1;
            let rows: Vec<Change> = st
                .rows
                .iter()
                .map(|((c, _), (key, d))| Change {
                    collection: c.clone(),
                    key: key.clone(),
                    op: Op::Upsert { doc: d.clone() },
                })
                .collect();
            (
                rows,
                std::mem::take(&mut st.during_snapshot),
                st.snapshot_delay,
            )
        };
        let mut during = Some(during);
        for row in rows {
            if !delay.is_zero() {
                tokio::select! {
                    _ = cancel.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                r = events.send(SourceEvent::Change(row)) => if r.is_err() { return Ok(()) },
            }
            self.origin.state().sent += 1;
            if let Some(changes) = during.take() {
                let mut st = self.origin.state();
                for (c, commit) in changes {
                    st.record(c, commit);
                }
            }
        }
        if let Some(changes) = during.take() {
            let mut st = self.origin.state();
            for (c, commit) in changes {
                st.record(c, commit);
            }
        }
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        mut acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let origin = Arc::clone(&self.origin);
        let fault = {
            let mut st = origin.state();
            st.stream_active = true;
            st.stream_faults.pop_front()
        };
        let _active = Active(Arc::clone(&origin));
        let mut fault = fault;
        let mut sent_here = 0usize;
        loop {
            if acks.has_changed().unwrap_or(false)
                && let Some(ack) = acks.borrow_and_update().clone()
            {
                origin.check_ack(&ack);
            }
            if let Some((after, _)) = &fault
                && sent_here >= *after
                && let Some((_, e)) = fault.take()
            {
                return Err(e);
            }
            let next = {
                let st = origin.state();
                st.log.get(self.pos).cloned()
            };
            match next {
                Some(event) => {
                    let event = match event {
                        SourceEvent::Checkpoint(_) => {
                            SourceEvent::Checkpoint(Checkpoint(json!(self.pos + 1)))
                        }
                        other => other,
                    };
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        r = events.send(event) => if r.is_err() { return Ok(()) },
                    }
                    self.pos += 1;
                    sent_here += 1;
                    origin.state().sent += 1;
                }
                None => {
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = origin.notify.notified() => {}
                        r = acks.changed() => {
                            if r.is_ok() && let Some(ack) = acks.borrow_and_update().clone() {
                                origin.check_ack(&ack);
                            }
                        }
                        _ = tokio::time::sleep(Duration::from_millis(5)) => {}
                    }
                }
            }
        }
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

// ---------------------------------------------------------------- sink

#[derive(Debug, Clone, Default)]
pub struct FakeIndex {
    pub primary_key: String,
    pub docs: BTreeMap<String, Document>,
    pub settings: Value,
}

#[derive(Default)]
pub struct SinkState {
    pub indexes: BTreeMap<String, FakeIndex>,
    pub apply_faults: VecDeque<SinkError>,
    pub reject: HashSet<String>,
    pub apply_delay: Duration,
    pub calls: Vec<String>,
    pub batch_sizes: Vec<usize>,
    /// Origin events sent minus documents applied, sampled on every apply.
    pub in_flight: Vec<i64>,
    pub applied_ops: usize,
    pub origin: Option<Arc<Origin>>,
}

#[derive(Default)]
pub struct FakeSink {
    pub st: Mutex<SinkState>,
}

impl FakeSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn state(&self) -> MutexGuard<'_, SinkState> {
        lock(&self.st)
    }

    pub fn docs(&self, uid: &str) -> BTreeMap<String, Document> {
        self.state()
            .indexes
            .get(uid)
            .map(|i| i.docs.clone())
            .unwrap_or_default()
    }

    pub fn calls(&self) -> Vec<String> {
        self.state().calls.clone()
    }

    pub fn create(&self, uid: &str, settings: Value, docs: Vec<(String, Document)>) {
        self.state().indexes.insert(
            uid.into(),
            FakeIndex {
                primary_key: "id".into(),
                docs: docs.into_iter().collect(),
                settings,
            },
        );
    }
}

#[async_trait]
impl Sink for FakeSink {
    async fn ensure_index(&self, target: &IndexTarget) -> Result<(), SinkError> {
        let mut st = self.state();
        st.calls.push(format!("ensure {}", target.uid));
        match st.indexes.get(&target.uid) {
            Some(i) if i.primary_key != target.primary_key => Err(SinkError::Config(format!(
                "index {} has primary key {}",
                target.uid, i.primary_key
            ))),
            Some(_) => Ok(()),
            None => {
                st.indexes.insert(
                    target.uid.clone(),
                    FakeIndex {
                        primary_key: target.primary_key.clone(),
                        docs: BTreeMap::new(),
                        settings: Value::Null,
                    },
                );
                Ok(())
            }
        }
    }

    async fn apply(&self, target: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError> {
        let delay = {
            let mut st = self.state();
            if let Some(origin) = st.origin.clone() {
                let sent = origin.state().sent as i64;
                let applied = st.applied_ops as i64;
                st.in_flight.push(sent - applied);
            }
            st.apply_delay
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let mut st = self.state();
        if let Some(e) = st.apply_faults.pop_front() {
            return Err(e);
        }
        st.calls.push(format!("apply {} {}", target.uid, ops.len()));
        st.batch_sizes.push(ops.len());
        st.applied_ops += ops.len();
        let reject = st.reject.clone();
        let index = st
            .indexes
            .get_mut(&target.uid)
            .ok_or_else(|| SinkError::Config(format!("index {} missing", target.uid)))?;
        let mut report = ApplyReport::default();
        for op in ops {
            if reject.contains(op.id()) {
                report.rejected.push(RejectedDoc {
                    op,
                    reason: "invalid document".into(),
                });
                continue;
            }
            match op {
                DocOp::Replace { id, mut doc } => {
                    doc.insert(target.primary_key.clone(), Value::String(id.clone()));
                    index.docs.insert(id, doc);
                    report.written += 1;
                }
                DocOp::Merge { id, doc, unset } => {
                    let entry = index.docs.entry(id.clone()).or_default();
                    entry.extend(doc);
                    for f in unset {
                        entry.insert(f, Value::Null);
                    }
                    entry.insert(target.primary_key.clone(), Value::String(id));
                    report.written += 1;
                }
                DocOp::Delete { id } => {
                    index.docs.remove(&id);
                    report.deleted += 1;
                }
            }
        }
        Ok(report)
    }

    async fn swap(&self, a: &str, b: &str) -> Result<(), SinkError> {
        let mut st = self.state();
        st.calls.push(format!("swap {a} {b}"));
        let (Some(ia), Some(ib)) = (st.indexes.remove(a), st.indexes.remove(b)) else {
            return Err(SinkError::Config("swap of a missing index".into()));
        };
        st.indexes.insert(a.into(), ib);
        st.indexes.insert(b.into(), ia);
        Ok(())
    }

    async fn delete_index(&self, uid: &str) -> Result<(), SinkError> {
        let mut st = self.state();
        st.calls.push(format!("delete {uid}"));
        st.indexes.remove(uid);
        Ok(())
    }

    async fn copy_settings(&self, from: &str, to: &str) -> Result<(), SinkError> {
        let mut st = self.state();
        st.calls.push(format!("copy_settings {from} {to}"));
        let settings = st
            .indexes
            .get(from)
            .map(|i| i.settings.clone())
            .ok_or_else(|| SinkError::Config(format!("{from} missing")))?;
        st.indexes.entry(to.into()).or_default().settings = settings;
        Ok(())
    }

    async fn document_count(&self, uid: &str) -> Result<Option<u64>, SinkError> {
        Ok(self.state().indexes.get(uid).map(|i| i.docs.len() as u64))
    }
}

// ---------------------------------------------------------------- store

type SavePredicate = Box<dyn Fn(&SyncState) -> bool + Send>;

#[derive(Default)]
pub struct StoreState {
    pub states: HashMap<String, SyncState>,
    pub history: Vec<SyncState>,
    pub dead: Vec<DeadLetter>,
    pub progress: HashMap<String, (u64, u64, u64)>,
    /// The next save matching this fails once with a transient error.
    pub fail_once: Option<SavePredicate>,
}

#[derive(Default)]
pub struct MemStore {
    pub st: Mutex<StoreState>,
}

impl MemStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inner(&self) -> MutexGuard<'_, StoreState> {
        lock(&self.st)
    }

    pub fn state(&self, id: &str) -> Option<SyncState> {
        self.inner().states.get(id).cloned()
    }

    pub fn phase(&self, id: &str) -> Option<SyncPhase> {
        self.state(id).map(|s| s.phase)
    }

    pub fn checkpoint(&self, id: &str) -> Option<u64> {
        self.state(id).and_then(|s| s.checkpoint).map(|c| pos(&c))
    }
}

#[async_trait]
impl StateStore for MemStore {
    async fn load(&self, sync_id: &str) -> Result<Option<SyncState>, StoreError> {
        Ok(self.state(sync_id))
    }

    async fn save(&self, state: &SyncState) -> Result<(), StoreError> {
        let mut st = self.inner();
        if st.fail_once.as_ref().is_some_and(|p| p(state)) {
            st.fail_once = None;
            return Err(StoreError::Transient("store unavailable".into()));
        }
        st.history.push(state.clone());
        st.states.insert(state.sync_id.clone(), state.clone());
        Ok(())
    }

    async fn push_dead_letters(&self, letters: &[DeadLetter]) -> Result<(), StoreError> {
        self.inner().dead.extend_from_slice(letters);
        Ok(())
    }

    async fn record_progress(
        &self,
        sync_id: &str,
        written: u64,
        deleted: u64,
        rejected: u64,
    ) -> Result<(), StoreError> {
        let mut st = self.inner();
        let entry = st.progress.entry(sync_id.into()).or_default();
        entry.0 += written;
        entry.1 += deleted;
        entry.2 += rejected;
        Ok(())
    }
}

// ---------------------------------------------------------------- harness

pub struct Harness {
    pub origin: Arc<Origin>,
    pub sink: Arc<FakeSink>,
    pub store: Arc<MemStore>,
}

impl Harness {
    pub fn new() -> Self {
        let origin = Origin::new();
        let sink = FakeSink::new();
        let store = MemStore::new();
        origin.state().store = Some(Arc::clone(&store));
        sink.state().origin = Some(Arc::clone(&origin));
        Self {
            origin,
            sink,
            store,
        }
    }

    pub fn deps(&self) -> SyncDeps {
        SyncDeps {
            sources: Arc::new(FakeFactory(Arc::clone(&self.origin))),
            sink: Arc::clone(&self.sink) as Arc<dyn Sink>,
            store: Arc::clone(&self.store) as Arc<dyn StateStore>,
        }
    }

    pub fn assert_acks_sound(&self) {
        let st = self.origin.state();
        assert!(st.ack_violations.is_empty(), "{:?}", st.ack_violations);
    }
}

pub fn spec() -> SyncSpec {
    SyncSpec {
        id: SYNC_ID.into(),
        tenant: None,
        name: "products".into(),
        source: SourceSpec {
            kind: SourceKind::Postgres,
            config: json!({}),
        },
        destination: MeiliDestination {
            url: "http://localhost:7700".into(),
            api_key: Secret::new("key"),
        },
        streams: vec![StreamMapping {
            collection: COLLECTION.into(),
            index: INDEX.into(),
            primary_key: "id".into(),
            fields: FieldSelection::All,
            rename: Default::default(),
            vector: None,
        }],
        mass_delete_guard: Some(0.5),
    }
}

pub fn options() -> SyncOptions {
    SyncOptions {
        channel_capacity: 16,
        max_docs: 100,
        max_bytes: 1 << 20,
        max_delay: Duration::from_millis(10),
        shutdown_grace: Duration::from_secs(2),
        release_hold: false,
        resync: false,
    }
}

/// Polls `cond` every few ms; panics after `secs`.
pub async fn wait_for(what: &str, secs: u64, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    while !cond() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
