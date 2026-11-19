//! One attempt of one sync: `run_sync`.
//!
//! See `docs/ARCHITECTURE.md`, "The sync loop". In short: load the state,
//! open the source (resuming from the saved checkpoint), snapshot when the
//! open is fresh, then stream. Events are batched, mapped, coalesced and
//! applied per index; the last `Checkpoint` event covered by an applied
//! batch is saved, and only once the store returned `Ok` is it published on
//! the ack channel.

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Utc;
use meili_connect_core::{
    AckReceiver, AckSender, Checkpoint, DeadLetter, DocOp, DocumentKind, IndexTarget, Opened,
    Progress, ResyncReason, Sink, SinkError, Source, SourceError, SourceEvent, SourceFactory,
    StateStore, StoreError, SyncPhase, SyncSpec, SyncState, Usage, ack_channel,
};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::coalesce::coalesce;
use crate::error::EngineError;
use crate::mapping::Mapper;

/// The three collaborators of a sync. Cheap to clone.
#[derive(Clone)]
pub struct SyncDeps {
    pub sources: Arc<dyn SourceFactory>,
    pub sink: Arc<dyn Sink>,
    pub store: Arc<dyn StateStore>,
}

/// Tuning and one-shot requests for one `run_sync` call.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncOptions {
    /// Capacity of the bounded channel between the source and the engine.
    pub channel_capacity: usize,
    /// Flush a batch once it holds this many document operations.
    pub max_docs: usize,
    /// Flush a batch once its documents weigh about this many bytes.
    pub max_bytes: usize,
    /// Flush a non-empty batch at the latest this long after it started.
    pub max_delay: Duration,
    /// How long a stopping source task gets before it is aborted.
    pub shutdown_grace: Duration,
    /// Apply the action a guard held on the previous run (one-shot: the
    /// first guard that trips in this run is let through).
    pub release_hold: bool,
    /// Ignore the saved checkpoint and rebuild every index from a fresh
    /// snapshot, through shadow indexes when the live ones hold data.
    pub resync: bool,
    /// Who asked for `resync`: the user (metered as `documents_snapshot`)
    /// or the engine after a lost position (`documents_resync`).
    pub resync_reason: ResyncReason,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            max_docs: 1000,
            max_bytes: 8 * 1024 * 1024,
            max_delay: Duration::from_secs(1),
            shutdown_grace: Duration::from_secs(10),
            release_hold: false,
            resync: false,
            resync_reason: ResyncReason::User,
        }
    }
}

/// `{index}__connect_{first 8 chars of the sync id}`.
pub fn shadow_index_uid(index: &str, sync_id: &str) -> String {
    let short: String = sync_id.chars().take(8).collect();
    format!("{index}__connect_{short}")
}

/// How many `PositionLost` in a row a plain [`run_sync`] call turns into a
/// resync before holding the sync.
const MAX_RESYNCS_PER_RUN: u32 = 3;

/// What the runs of one supervised sync share across attempts: the budget
/// of consecutive resyncs and which one-shot requests a run consumed.
/// Cheap to clone; clones share their state.
#[derive(Debug, Clone)]
pub struct RunMonitor {
    inner: Arc<Mutex<MonitorState>>,
    max_consecutive_resyncs: u32,
    healthy_after: Duration,
}

#[derive(Debug, Default)]
struct MonitorState {
    consecutive_resyncs: u32,
    /// Start of the current streaming period, while one is running.
    streaming_since: Option<Instant>,
    /// A streaming period lasted `healthy_after` since the last
    /// [`RunMonitor::take_streamed_healthily`].
    streamed_healthily: bool,
    resync_consumed: bool,
    release_consumed: bool,
    /// Start of the current period in `snapshotting` or `streaming`.
    active_since: Option<Instant>,
    /// Time spent active and not yet taken by
    /// [`RunMonitor::take_active_seconds`].
    active: Duration,
    /// When a checkpoint was last saved.
    last_checkpoint: Option<Instant>,
}

impl MonitorState {
    /// Folds a streaming period that is already healthy into the counters.
    fn settle(&mut self, healthy_after: Duration) {
        if self
            .streaming_since
            .is_some_and(|since| since.elapsed() >= healthy_after)
        {
            self.consecutive_resyncs = 0;
            self.streamed_healthily = true;
        }
    }
}

impl RunMonitor {
    /// A resync caused by `PositionLost` counts as consecutive until the
    /// sync has streamed for `healthy_after` without interruption; past
    /// `max_consecutive_resyncs` the sync is held instead.
    pub fn new(max_consecutive_resyncs: u32, healthy_after: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MonitorState::default())),
            max_consecutive_resyncs,
            healthy_after,
        }
    }

    fn state(&self) -> MutexGuard<'_, MonitorState> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Resyncs started after a lost position since the last healthy
    /// streaming period.
    pub fn consecutive_resyncs(&self) -> u32 {
        let mut st = self.state();
        st.settle(self.healthy_after);
        st.consecutive_resyncs
    }

    /// Whether a run reached the point where the requested resync is
    /// persisted (`SyncState::resyncing`). Resets the flag.
    pub fn take_resync_consumed(&self) -> bool {
        std::mem::take(&mut self.state().resync_consumed)
    }

    /// Whether a run applied the action a guard had held. Resets the flag.
    pub fn take_release_consumed(&self) -> bool {
        std::mem::take(&mut self.state().release_consumed)
    }

    /// Whether a streaming period lasted `healthy_after`. Resets the flag.
    pub fn take_streamed_healthily(&self) -> bool {
        let mut st = self.state();
        st.settle(self.healthy_after);
        std::mem::take(&mut st.streamed_healthily)
    }

    fn streaming_started(&self) {
        self.state().streaming_since = Some(Instant::now());
    }

    fn streaming_stopped(&self) {
        let mut st = self.state();
        st.settle(self.healthy_after);
        st.streaming_since = None;
    }

    /// Takes one resync from the budget; `false` once it is spent.
    fn begin_resync(&self) -> bool {
        let mut st = self.state();
        st.settle(self.healthy_after);
        st.streaming_since = None;
        if st.consecutive_resyncs >= self.max_consecutive_resyncs {
            return false;
        }
        st.consecutive_resyncs += 1;
        true
    }

    fn resync_persisted(&self) {
        self.state().resync_consumed = true;
    }

    /// A period in `snapshotting` or `streaming` starts (idempotent).
    fn activate(&self) {
        let mut st = self.state();
        if st.active_since.is_none() {
            st.active_since = Some(Instant::now());
        }
    }

    /// The active period ends; its time waits for
    /// [`RunMonitor::take_active_seconds`].
    fn deactivate(&self) {
        let mut st = self.state();
        if let Some(since) = st.active_since.take() {
            st.active += since.elapsed();
        }
    }

    /// Whole seconds spent in `snapshotting` or `streaming` since the last
    /// call (the fraction is carried over): the `sync_seconds` unit.
    pub fn take_active_seconds(&self) -> u64 {
        let mut st = self.state();
        if let Some(since) = st.active_since {
            let now = Instant::now();
            st.active += now.saturating_duration_since(since);
            st.active_since = Some(now);
        }
        let whole = st.active.as_secs();
        st.active -= Duration::from_secs(whole);
        whole
    }

    /// A checkpoint was saved.
    fn checkpointed(&self) {
        self.state().last_checkpoint = Some(Instant::now());
    }

    /// How long ago a run of this monitor last saved a checkpoint.
    pub fn since_last_checkpoint(&self) -> Option<Duration> {
        self.state().last_checkpoint.map(|t| t.elapsed())
    }

    fn release_applied(&self) {
        self.state().release_consumed = true;
    }
}

/// Runs one sync until it is cancelled (`Ok`) or an error stops it.
///
/// `PositionLost` (from `open` or `stream`) and `options.resync` are handled
/// here by a resync into shadow indexes, at most three in a row; past that
/// the sync is held. Guards save the state as `Held` and return
/// [`EngineError::Held`]. Every other error is returned as is; mapping it
/// to a phase and a retry policy is the supervisor's job.
pub async fn run_sync(
    spec: SyncSpec,
    deps: SyncDeps,
    options: SyncOptions,
    cancel: CancellationToken,
) -> Result<(), EngineError> {
    let monitor = RunMonitor::new(MAX_RESYNCS_PER_RUN, Duration::MAX);
    run_sync_monitored(spec, deps, options, cancel, &monitor).await
}

/// [`run_sync`] with a [`RunMonitor`] shared across calls, so the resync
/// budget spans attempts and the caller learns which one-shot requests
/// (`options.resync`, `options.release_hold`) were consumed.
pub async fn run_sync_monitored(
    spec: SyncSpec,
    deps: SyncDeps,
    options: SyncOptions,
    cancel: CancellationToken,
    monitor: &RunMonitor,
) -> Result<(), EngineError> {
    spec.validate().map_err(EngineError::Spec)?;
    let loaded = match guarded(&cancel, deps.store.load(&spec.id)).await {
        Ok(state) => state,
        Err(Halt::Cancelled) => return Ok(()),
        Err(Halt::Error(e)) => return Err(e),
    };
    let state = loaded.unwrap_or_else(|| SyncState {
        sync_id: spec.id.clone(),
        phase: SyncPhase::Pending,
        checkpoint: None,
        message: None,
        resyncing: false,
        resync_reason: None,
        updated_at: Utc::now(),
    });
    let live = spec
        .streams
        .iter()
        .map(|m| IndexTarget {
            uid: m.index.clone(),
            primary_key: m.primary_key.clone(),
        })
        .collect();
    let mut resync = options.resync || state.resyncing;
    // A resync persisted without its reason predates the field: free.
    let mut reason = if options.resync {
        options.resync_reason
    } else {
        state.resync_reason.unwrap_or(ResyncReason::System)
    };
    let mut runner = Runner {
        mapper: Mapper::new(&spec.streams),
        spec: &spec,
        deps: &deps,
        opts: &options,
        cancel: &cancel,
        monitor,
        live,
        state,
        release: options.release_hold,
    };

    loop {
        let attempted = runner.attempt(resync, reason).await;
        monitor.deactivate();
        match attempted {
            Ok(()) | Err(Halt::Cancelled) => return Ok(()),
            Err(Halt::Error(EngineError::Source(SourceError::PositionLost(lost)))) => {
                if monitor.begin_resync() {
                    warn!(sync_id = %spec.id, reason = %lost, "source position lost, resyncing");
                    resync = true;
                    reason = ResyncReason::System;
                    continue;
                }
                let message = format!(
                    "the source position was lost again after {} resyncs in a row ({reason}): \
                     the origin keeps its changes for less time than a snapshot takes. Raise \
                     the origin's retention (replication slot WAL limit, binlog expiry, oplog \
                     size, stream retention), then resync",
                    monitor.max_consecutive_resyncs,
                    reason = lost,
                );
                warn!(sync_id = %spec.id, %message, "sync held");
                let saved = message.clone();
                let persisted = runner
                    .update(move |s| {
                        s.phase = SyncPhase::Held;
                        s.message = Some(saved);
                    })
                    .await;
                if let Err(Halt::Error(e)) = persisted {
                    warn!(sync_id = %spec.id, error = %e, "could not persist the hold");
                }
                return Err(EngineError::Held(message));
            }
            Err(Halt::Error(e)) => return Err(e),
        }
    }
}

/// Why a step stopped: cancellation is not an error.
enum Halt {
    Cancelled,
    Error(EngineError),
}

impl From<EngineError> for Halt {
    fn from(e: EngineError) -> Self {
        Halt::Error(e)
    }
}
impl From<SourceError> for Halt {
    fn from(e: SourceError) -> Self {
        Halt::Error(e.into())
    }
}
impl From<SinkError> for Halt {
    fn from(e: SinkError) -> Self {
        Halt::Error(e.into())
    }
}
impl From<StoreError> for Halt {
    fn from(e: StoreError) -> Self {
        Halt::Error(e.into())
    }
}

/// Awaits `fut` unless `cancel` fires first.
async fn guarded<T, E>(
    cancel: &CancellationToken,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, Halt>
where
    Halt: From<E>,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(Halt::Cancelled),
        r = fut => r.map_err(Halt::from),
    }
}

/// The spawned source call; hands the source back with its result.
type SourceTask = JoinHandle<(Box<dyn Source>, Result<(), SourceError>)>;

#[derive(Clone, Copy)]
enum Mode<'a> {
    /// Writes only; no checkpoints, no guards. Metered as this kind.
    Snapshot(DocumentKind),
    /// Writes, guards, checkpoints and acks.
    Stream(&'a AckSender),
}

/// The pending batch. Bounded by `max_docs` / `max_bytes`; only the last
/// checkpoint is kept.
struct Batch {
    ops: Vec<Vec<DocOp>>,
    /// Records the source skipped (`SourceEvent::Skipped`), dead-lettered
    /// with the batch.
    skipped: Vec<DeadLetter>,
    docs: usize,
    bytes: usize,
    checkpoint: Option<Checkpoint>,
    started: Option<Instant>,
}

impl Batch {
    fn new(indexes: usize) -> Self {
        Self {
            ops: vec![Vec::new(); indexes],
            skipped: Vec::new(),
            docs: 0,
            bytes: 0,
            checkpoint: None,
            started: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.started.is_none()
    }

    fn touch(&mut self) {
        if self.started.is_none() {
            self.started = Some(Instant::now());
        }
    }

    fn push(&mut self, index: usize, op: DocOp) {
        self.bytes += op_size(&op);
        self.docs += 1;
        if let Some(slot) = self.ops.get_mut(index) {
            slot.push(op);
        }
        self.touch();
    }

    fn skip(&mut self, letter: DeadLetter) {
        self.bytes += letter.document_id.len() + letter.reason.len();
        self.docs += 1;
        self.skipped.push(letter);
        self.touch();
    }

    fn age(&self) -> Duration {
        self.started.map(|s| s.elapsed()).unwrap_or_default()
    }

    fn take(&mut self) -> Batch {
        std::mem::replace(self, Batch::new(self.ops.len()))
    }
}

struct Runner<'a> {
    spec: &'a SyncSpec,
    deps: &'a SyncDeps,
    opts: &'a SyncOptions,
    cancel: &'a CancellationToken,
    monitor: &'a RunMonitor,
    mapper: Mapper,
    /// Live targets, one per mapping, same order as `spec.streams`.
    live: Vec<IndexTarget>,
    state: SyncState,
    release: bool,
}

impl Runner<'_> {
    /// Saves a modified copy of the state; the in-memory state only changes
    /// once the store returned `Ok`.
    async fn update(&mut self, change: impl FnOnce(&mut SyncState)) -> Result<(), Halt> {
        let mut next = self.state.clone();
        change(&mut next);
        next.updated_at = Utc::now();
        guarded(self.cancel, self.deps.store.save(&next)).await?;
        self.state = next;
        Ok(())
    }

    async fn attempt(&mut self, resync: bool, reason: ResyncReason) -> Result<(), Halt> {
        let mut source =
            self.deps
                .sources
                .build(&self.spec.id, &self.spec.source, &self.spec.streams)?;
        let resume = if resync {
            None
        } else {
            self.state.checkpoint.clone()
        };
        if resync && (!self.state.resyncing || self.state.resync_reason != Some(reason)) {
            self.update(|s| {
                s.resyncing = true;
                s.resync_reason = Some(reason);
                s.phase = SyncPhase::Snapshotting;
                s.message = None;
            })
            .await?;
        }
        // A first sync, or a resync the user asked for, is a snapshot; a
        // resync the engine started is metered apart (free).
        let kind = if resync && reason == ResyncReason::System {
            DocumentKind::Resync
        } else {
            DocumentKind::Snapshot
        };
        if resync {
            // From here on the saved state carries the resync.
            self.monitor.resync_persisted();
        }

        let opened = guarded(self.cancel, source.open(resume.clone())).await?;
        let (acks, acks_rx) = ack_channel();
        match opened {
            Opened::Fresh { start } => {
                info!(sync_id = %self.spec.id, resync, "fresh start");
                source = self.initial_load(source, kind).await?;
                let saved = start.clone();
                self.update(move |s| {
                    s.checkpoint = Some(saved);
                    s.phase = SyncPhase::Streaming;
                    s.message = None;
                    s.resyncing = false;
                    s.resync_reason = None;
                })
                .await?;
                self.monitor.checkpointed();
                acks.send_replace(Some(start));
            }
            Opened::Resumed => {
                info!(sync_id = %self.spec.id, "resumed");
                for target in self.live.clone() {
                    guarded(self.cancel, self.deps.sink.ensure_index(&target)).await?;
                }
                acks.send_replace(resume);
                if self.state.phase != SyncPhase::Streaming
                    || self.state.message.is_some()
                    || self.state.resyncing
                {
                    self.update(|s| {
                        s.phase = SyncPhase::Streaming;
                        s.message = None;
                        s.resyncing = false;
                        s.resync_reason = None;
                    })
                    .await?;
                }
            }
        }

        let live = self.live.clone();
        self.monitor.activate();
        self.monitor.streaming_started();
        let streamed = self
            .pump(source, &live, Mode::Stream(&acks), Some(acks_rx))
            .await;
        self.monitor.streaming_stopped();
        streamed.map(|_| ())
    }

    /// Prepares the targets (shadow indexes for live indexes that hold
    /// data), runs the snapshot when the source can, then swaps the shadows
    /// in.
    async fn initial_load(
        &mut self,
        source: Box<dyn Source>,
        kind: DocumentKind,
    ) -> Result<Box<dyn Source>, Halt> {
        let snapshot = source.capabilities().snapshot;
        let sink = Arc::clone(&self.deps.sink);
        let mut targets = Vec::with_capacity(self.live.len());
        let mut swaps: Vec<(IndexTarget, IndexTarget)> = Vec::new();
        // Every live index first: one keyed differently fails with the
        // sink's `Config` error before any shadow is built, since a swap
        // would otherwise replace it.
        for live in &self.live {
            guarded(self.cancel, sink.ensure_index(live)).await?;
        }
        for live in self.live.clone() {
            let populated = snapshot
                && guarded(self.cancel, sink.document_count(&live.uid))
                    .await?
                    .unwrap_or(0)
                    > 0;
            if populated {
                let shadow = IndexTarget {
                    uid: shadow_index_uid(&live.uid, &self.spec.id),
                    primary_key: live.primary_key.clone(),
                };
                // A crashed resync may have left a partial shadow behind:
                // snapshots restart from scratch.
                guarded(self.cancel, sink.delete_index(&shadow.uid)).await?;
                guarded(self.cancel, sink.ensure_index(&shadow)).await?;
                guarded(self.cancel, sink.copy_settings(&live.uid, &shadow.uid)).await?;
                targets.push(shadow.clone());
                swaps.push((live, shadow));
            } else {
                targets.push(live);
            }
        }

        let resyncing = !swaps.is_empty() || self.state.resyncing;
        self.update(|s| {
            s.phase = SyncPhase::Snapshotting;
            s.message = None;
            s.resyncing = resyncing;
            if !resyncing {
                s.resync_reason = None;
            } else if s.resync_reason.is_none() {
                // Shadows for a first sync into populated indexes (a PUT
                // that changed the streams): the user's doing.
                s.resync_reason = Some(match kind {
                    DocumentKind::Resync => ResyncReason::System,
                    _ => ResyncReason::User,
                });
            }
        })
        .await?;
        self.monitor.activate();

        let source = if snapshot {
            info!(sync_id = %self.spec.id, shadows = swaps.len(), "snapshot started");
            let source = self
                .pump(source, &targets, Mode::Snapshot(kind), None)
                .await?;
            // A source returns `Ok` from a cancelled snapshot: never take
            // that for a complete one.
            if self.cancel.is_cancelled() {
                return Err(Halt::Cancelled);
            }
            info!(sync_id = %self.spec.id, "snapshot done");
            source
        } else {
            source
        };

        for (live, shadow) in swaps {
            guarded(self.cancel, sink.swap(&live.uid, &shadow.uid)).await?;
            guarded(self.cancel, sink.delete_index(&shadow.uid)).await?;
            info!(sync_id = %self.spec.id, index = %live.uid, "shadow index swapped in");
        }
        Ok(source)
    }

    /// Runs the source task (`snapshot` without `acks_rx`, `stream` with
    /// it) next to the consumer loop. Either side failing stops both. On
    /// success (a snapshot that completed) the source is handed back.
    async fn pump(
        &mut self,
        mut source: Box<dyn Source>,
        targets: &[IndexTarget],
        mode: Mode<'_>,
        acks_rx: Option<AckReceiver>,
    ) -> Result<Box<dyn Source>, Halt> {
        let (tx, mut rx) = mpsc::channel(self.opts.channel_capacity.max(1));
        let child = self.cancel.child_token();
        let token = child.clone();
        let mut task: SourceTask = tokio::spawn(async move {
            let result = match acks_rx {
                None => source.snapshot(tx, token).await,
                Some(acks) => source.stream(tx, acks, token).await,
            };
            (source, result)
        });

        let mut batch = Batch::new(targets.len());
        let consumed = self.consume(&mut rx, &mut batch, targets, mode).await;
        drop(rx);
        if let Err(halt) = consumed {
            child.cancel();
            self.stop_task(&mut task).await;
            return Err(halt);
        }

        // The channel closed: the source task returned.
        let (source, result) = match (&mut task).await {
            Ok(out) => out,
            Err(e) => {
                return Err(
                    SourceError::Fatal(format!("source task ended abnormally: {e}")).into(),
                );
            }
        };
        result?;
        if self.cancel.is_cancelled() {
            return Err(Halt::Cancelled);
        }
        match mode {
            Mode::Snapshot(_) => {
                self.flush(&mut batch, targets, mode).await?;
                Ok(source)
            }
            Mode::Stream(_) => Err(SourceError::Transient(
                "the source stream ended without being cancelled".into(),
            )
            .into()),
        }
    }

    async fn stop_task(&self, task: &mut SourceTask) {
        tokio::select! {
            _ = &mut *task => {}
            _ = tokio::time::sleep(self.opts.shutdown_grace) => {
                warn!(sync_id = %self.spec.id, "source did not stop in time, aborting it");
                task.abort();
            }
        }
    }

    /// Reads events until the channel closes (`Ok`), cancellation or an
    /// error.
    async fn consume(
        &mut self,
        rx: &mut mpsc::Receiver<SourceEvent>,
        batch: &mut Batch,
        targets: &[IndexTarget],
        mode: Mode<'_>,
    ) -> Result<(), Halt> {
        enum Next {
            Cancelled,
            Deadline,
            Event(Option<SourceEvent>),
        }
        let cancel = self.cancel;
        loop {
            let has_deadline = !batch.is_empty();
            let deadline = batch
                .started
                .map(|s| s + self.opts.max_delay)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => Next::Cancelled,
                _ = tokio::time::sleep_until(deadline), if has_deadline => Next::Deadline,
                event = rx.recv() => Next::Event(event),
            };
            match next {
                Next::Cancelled => return Err(Halt::Cancelled),
                Next::Deadline => self.flush(batch, targets, mode).await?,
                Next::Event(None) => return Ok(()),
                Next::Event(Some(event)) => self.on_event(event, batch, targets, mode).await?,
            }
        }
    }

    async fn on_event(
        &mut self,
        event: SourceEvent,
        batch: &mut Batch,
        targets: &[IndexTarget],
        mode: Mode<'_>,
    ) -> Result<(), Halt> {
        match event {
            SourceEvent::Change(change) => {
                for (index, op) in self.mapper.map(change) {
                    batch.push(index, op);
                }
                if batch.docs >= self.opts.max_docs || batch.bytes >= self.opts.max_bytes {
                    self.flush(batch, targets, mode).await?;
                }
            }
            SourceEvent::Checkpoint(checkpoint) => {
                if let Mode::Stream(_) = mode {
                    batch.checkpoint = Some(checkpoint);
                    batch.touch();
                    if batch.docs > 0 && batch.age() >= self.opts.max_delay {
                        self.flush(batch, targets, mode).await?;
                    }
                }
            }
            SourceEvent::Truncate { collection } => {
                if let Mode::Stream(_) = mode {
                    self.flush(batch, targets, mode).await?;
                    self.truncate(&collection).await?;
                }
            }
            SourceEvent::SchemaChanged { collection } => {
                debug!(sync_id = %self.spec.id, %collection, "schema changed");
            }
            SourceEvent::Skipped {
                collection,
                position,
                reason,
            } => {
                let index = self
                    .mapper
                    .indexes_for(&collection)
                    .first()
                    .and_then(|&i| self.live.get(i))
                    .map_or(collection, |t| t.uid.clone());
                batch.skip(DeadLetter {
                    sync_id: self.spec.id.clone(),
                    index,
                    document_id: position,
                    payload: Value::Null,
                    reason,
                    created_at: Utc::now(),
                });
                if batch.docs >= self.opts.max_docs || batch.bytes >= self.opts.max_bytes {
                    self.flush(batch, targets, mode).await?;
                }
            }
        }
        Ok(())
    }

    /// Coalesce, guard, apply per index in order, dead-letter, record
    /// progress, then save and ack the batch's checkpoint (stream only).
    async fn flush(
        &mut self,
        batch: &mut Batch,
        targets: &[IndexTarget],
        mode: Mode<'_>,
    ) -> Result<(), Halt> {
        if batch.is_empty() {
            return Ok(());
        }
        let Batch {
            ops,
            skipped,
            checkpoint,
            ..
        } = batch.take();
        let ops: Vec<Vec<DocOp>> = ops.into_iter().map(coalesce).collect();

        let released = match mode {
            Mode::Stream(_) => self.mass_delete_guard(&ops, targets).await?,
            Mode::Snapshot(_) => false,
        };
        let kind = match mode {
            Mode::Stream(_) => DocumentKind::Streamed,
            Mode::Snapshot(kind) => kind,
        };

        let (mut written, mut deleted, mut bytes) = (0u64, 0u64, 0u64);
        let mut rejected = skipped.len() as u64;
        let mut letters = skipped;
        for (index, ops) in ops.into_iter().enumerate() {
            if ops.is_empty() {
                continue;
            }
            let Some(target) = targets.get(index) else {
                continue;
            };
            let report = guarded(self.cancel, self.deps.sink.apply(target, ops)).await?;
            written += report.written;
            deleted += report.deleted;
            bytes = bytes.saturating_add(report.bytes);
            rejected += report.rejected.len() as u64;
            let live_uid = self.live.get(index).map_or(&target.uid, |t| &t.uid);
            for doc in report.rejected {
                letters.push(DeadLetter {
                    sync_id: self.spec.id.clone(),
                    index: live_uid.clone(),
                    document_id: doc.op.id().to_owned(),
                    payload: op_payload(&doc.op),
                    reason: doc.reason,
                    created_at: Utc::now(),
                });
            }
        }
        if released {
            self.monitor.release_applied();
        }
        if !letters.is_empty() {
            warn!(sync_id = %self.spec.id, count = letters.len(), "documents rejected or skipped, dead-lettered");
            guarded(self.cancel, self.deps.store.push_dead_letters(&letters)).await?;
        }
        if written + deleted + rejected > 0 {
            // Only what Meilisearch accepted is metered.
            let mut usage = Usage::documents(kind, written.saturating_add(deleted));
            usage.bytes = bytes;
            let progress = Progress {
                written,
                deleted,
                rejected,
                usage,
            };
            guarded(
                self.cancel,
                self.deps.store.record_progress(&self.spec.id, &progress),
            )
            .await?;
            debug!(sync_id = %self.spec.id, written, deleted, rejected, bytes, ?kind, "batch applied");
        }

        if let (Mode::Stream(acks), Some(checkpoint)) = (mode, checkpoint)
            && self.state.checkpoint.as_ref() != Some(&checkpoint)
        {
            let saved = checkpoint.clone();
            self.update(move |s| s.checkpoint = Some(saved)).await?;
            self.monitor.checkpointed();
            // Only now is the position durable.
            acks.send_replace(Some(checkpoint));
        }
        Ok(())
    }

    /// `Ok(true)` when the guard tripped and a pending release let the
    /// batch through.
    async fn mass_delete_guard(
        &mut self,
        ops: &[Vec<DocOp>],
        targets: &[IndexTarget],
    ) -> Result<bool, Halt> {
        let Some(limit) = self.spec.mass_delete_guard else {
            return Ok(false);
        };
        let mut tripped = None;
        for (index, ops) in ops.iter().enumerate() {
            let deletes = ops
                .iter()
                .filter(|op| matches!(op, DocOp::Delete { .. }))
                .count() as u64;
            if deletes == 0 {
                continue;
            }
            let Some(target) = targets.get(index) else {
                continue;
            };
            let count = guarded(self.cancel, self.deps.sink.document_count(&target.uid)).await?;
            if let Some(count) = count
                && exceeds(deletes, count, limit)
            {
                tripped = Some(format!(
                    "a batch would delete {deletes} of the {count} documents of index `{}` \
                     (guard: {:.0}%)",
                    target.uid,
                    limit * 100.0
                ));
                break;
            }
        }
        match tripped {
            Some(message) => self.hold_or_release(message).await,
            None => Ok(false),
        }
    }

    /// Empties the index of a truncated collection, keeping its settings.
    /// `SyncSpec::validate` guarantees one index per collection and one
    /// collection per index, so nothing else lives in it.
    async fn truncate(&mut self, collection: &str) -> Result<(), Halt> {
        let mut to_clear: Vec<(IndexTarget, u64)> = Vec::new();
        for &index in self.mapper.indexes_for(collection) {
            let Some(target) = self.live.get(index).cloned() else {
                continue;
            };
            let count = guarded(self.cancel, self.deps.sink.document_count(&target.uid)).await?;
            if let Some(count) = count
                && count > 0
            {
                to_clear.push((target, count));
            }
        }
        if to_clear.is_empty() {
            return Ok(());
        }
        let mut released = false;
        if let Some(limit) = self.spec.mass_delete_guard
            && let Some((target, count)) = to_clear.iter().find(|(_, c)| exceeds(*c, *c, limit))
        {
            let message = format!(
                "`{collection}` was truncated: applying it would delete all {count} documents \
                 of index `{}`",
                target.uid
            );
            released = self.hold_or_release(message).await?;
        }
        for (target, _) in to_clear {
            guarded(
                self.cancel,
                self.deps.sink.delete_all_documents(&target.uid),
            )
            .await?;
            info!(sync_id = %self.spec.id, index = %target.uid, "index cleared after a truncate");
        }
        if released {
            self.monitor.release_applied();
        }
        Ok(())
    }

    /// Holds the sync, or returns `Ok(true)` when a pending release lets
    /// the action through (once per run).
    async fn hold_or_release(&mut self, message: String) -> Result<bool, Halt> {
        if self.release {
            self.release = false;
            info!(sync_id = %self.spec.id, %message, "hold released, applying");
            return Ok(true);
        }
        warn!(sync_id = %self.spec.id, %message, "sync held");
        let saved = message.clone();
        self.update(move |s| {
            s.phase = SyncPhase::Held;
            s.message = Some(saved);
        })
        .await?;
        Err(EngineError::Held(message).into())
    }
}

fn exceeds(deletes: u64, count: u64, limit: f64) -> bool {
    count > 0 && deletes as f64 > limit * count as f64
}

fn op_payload(op: &DocOp) -> Value {
    match op {
        DocOp::Replace { id, doc } => json!({"kind": "replace", "id": id, "doc": doc}),
        DocOp::Merge { id, doc, unset } => {
            json!({"kind": "merge", "id": id, "doc": doc, "unset": unset})
        }
        DocOp::Delete { id } => json!({"kind": "delete", "id": id}),
    }
}

/// Approximate serialized size, without serializing.
fn op_size(op: &DocOp) -> usize {
    match op {
        DocOp::Replace { id, doc } => id.len() + doc_size(doc),
        DocOp::Merge { id, doc, unset } => {
            id.len() + doc_size(doc) + unset.iter().map(|f| f.len() + 3).sum::<usize>()
        }
        DocOp::Delete { id } => id.len(),
    }
}

fn doc_size(doc: &serde_json::Map<String, Value>) -> usize {
    2 + doc
        .iter()
        .map(|(k, v)| k.len() + 4 + value_size(v))
        .sum::<usize>()
}

fn value_size(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) => 5,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        Value::Array(items) => 2 + items.iter().map(|v| value_size(v) + 1).sum::<usize>(),
        Value::Object(map) => doc_size(map),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_name_uses_first_eight_chars() {
        assert_eq!(
            shadow_index_uid("products", "0123456789abcdef"),
            "products__connect_01234567"
        );
        assert_eq!(shadow_index_uid("p", "abc"), "p__connect_abc");
    }

    #[test]
    fn guard_threshold() {
        assert!(!exceeds(5, 10, 0.5));
        assert!(exceeds(6, 10, 0.5));
        assert!(!exceeds(3, 0, 0.5));
        assert!(!exceeds(10, 10, 1.0));
    }

    #[test]
    fn size_estimate_grows_with_content() {
        let small = DocOp::Replace {
            id: "1".into(),
            doc: serde_json::Map::new(),
        };
        let mut doc = serde_json::Map::new();
        doc.insert("text".into(), Value::String("x".repeat(1000)));
        let big = DocOp::Replace {
            id: "1".into(),
            doc,
        };
        assert!(op_size(&big) > 1000);
        assert!(op_size(&small) < 10);
    }

    #[tokio::test]
    async fn only_active_time_is_metered_in_whole_seconds() {
        let monitor = RunMonitor::new(1, Duration::MAX);
        assert_eq!(monitor.take_active_seconds(), 0);
        monitor.activate();
        monitor.activate(); // idempotent
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert_eq!(monitor.take_active_seconds(), 1, "taken while running");
        monitor.deactivate();
        assert_eq!(monitor.take_active_seconds(), 0, "the fraction waits");
        // Inactive (held, paused): nothing accrues.
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(monitor.take_active_seconds(), 0);
    }

    #[tokio::test]
    async fn the_resync_budget_resets_after_healthy_streaming() {
        let monitor = RunMonitor::new(2, Duration::from_millis(20));
        assert!(monitor.begin_resync());
        assert!(monitor.begin_resync());
        assert!(!monitor.begin_resync(), "budget spent");

        // A short streaming period does not count as healthy.
        monitor.streaming_started();
        monitor.streaming_stopped();
        assert!(!monitor.begin_resync());
        assert!(!monitor.take_streamed_healthily());

        monitor.streaming_started();
        tokio::time::sleep(Duration::from_millis(30)).await;
        monitor.streaming_stopped();
        assert_eq!(monitor.consecutive_resyncs(), 0);
        assert!(monitor.take_streamed_healthily());
        assert!(!monitor.take_streamed_healthily(), "reset once taken");
        assert!(monitor.begin_resync());
        assert_eq!(monitor.consecutive_resyncs(), 1);
    }
}
