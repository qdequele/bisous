//! Runs many syncs: `run_sync` in a loop, errors mapped to phases and a
//! retry policy, phase and message persisted through the store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Utc;
use meili_connect_core::{
    Checkpoint, Progress, ResyncReason, SinkError, StoreError, SyncPhase, SyncSpec, SyncState,
    Usage,
};
use thiserror::Error;
use tokio::sync::OwnedMutexGuard;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{EngineError, Reaction};
use crate::sync::{RunMonitor, SyncDeps, SyncOptions, run_sync_monitored, shadow_index_uid};

#[derive(Debug, Clone, PartialEq)]
pub struct SupervisorConfig {
    /// Options for every `run_sync` call (the one-shot `resync` and
    /// `release_hold` flags are ignored here: use the supervisor methods).
    pub sync: SyncOptions,
    /// First retry delay after a transient error.
    pub backoff_initial: Duration,
    /// Cap of the exponential backoff.
    pub backoff_max: Duration,
    /// A run that lasted this long resets the backoff.
    pub healthy_after: Duration,
    /// Retry interval while the source is paused.
    pub paused_retry: Duration,
    /// Resyncs after a lost position allowed in a row before the sync is
    /// held: the origin's retention is then shorter than a snapshot. A
    /// resync stops counting once the sync streamed for `healthy_after`.
    pub max_consecutive_resyncs: u32,
    /// `SinkError::Transient` failures allowed in a row while the saved
    /// checkpoint does not move (the same batch keeps failing) before the
    /// sync is held with the sink's message. `0` disables the bound.
    pub max_sink_transient_failures: u32,
    /// How often the time a sync spends in `snapshotting` or `streaming`
    /// is recorded (`sync_seconds`); the rest is recorded when a run ends.
    pub seconds_interval: Duration,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            sync: SyncOptions::default(),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            healthy_after: Duration::from_secs(120),
            paused_retry: Duration::from_secs(60),
            max_consecutive_resyncs: 3,
            max_sink_transient_failures: 10,
            seconds_interval: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("sync {0} is already running")]
    AlreadyRunning(String),
    #[error("sync {0} is not known to the supervisor")]
    NotFound(String),
    #[error("the supervisor is shutting down")]
    ShuttingDown,
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The spec was refused before anything ran (`EngineError::Spec`).
    #[error(transparent)]
    Engine(#[from] EngineError),
}

/// What the supervisor knows about one sync, merged with its saved state.
#[derive(Debug, Clone, PartialEq)]
pub struct SyncStatus {
    pub sync_id: String,
    /// A supervision loop is alive for it (running or waiting to retry).
    pub running: bool,
    pub phase: SyncPhase,
    pub message: Option<String>,
    pub resyncing: bool,
    pub checkpoint: Option<Checkpoint>,
    /// Failures since the last healthy run.
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct RunInfo {
    running: bool,
    consecutive_failures: u32,
    last_error: Option<String>,
}

struct Entry {
    spec: SyncSpec,
    cancel: CancellationToken,
    handle: Option<JoinHandle<()>>,
    info: Arc<Mutex<RunInfo>>,
}

/// One-shot requests for the next run.
#[derive(Debug, Clone, Copy, Default)]
struct Launch {
    resync: bool,
    release_hold: bool,
}

/// Records the whole seconds a sync spent in `snapshotting` or
/// `streaming` since the last call.
async fn record_seconds(
    store: &dyn meili_connect_core::StateStore,
    sync_id: &str,
    monitor: &RunMonitor,
) {
    let seconds = monitor.take_active_seconds();
    if seconds == 0 {
        return;
    }
    let progress = Progress {
        usage: Usage {
            sync_seconds: seconds,
            ..Usage::default()
        },
        ..Progress::default()
    };
    if let Err(e) = store.record_progress(sync_id, &progress).await {
        warn!(sync_id, error = %e, seconds, "could not record the sync time");
    }
}

pub struct Supervisor {
    deps: SyncDeps,
    config: SupervisorConfig,
    cancel: CancellationToken,
    syncs: Mutex<HashMap<String, Entry>>,
    /// One lock per sync, held for the whole of `start`, `stop`, `remove`
    /// and the relaunches, so concurrent calls on one sync run one after
    /// the other, in the order they were issued (tokio's mutex is fair).
    ops: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl Supervisor {
    /// `cancel` is the process shutdown token: cancelling it stops every
    /// sync (see also [`Supervisor::shutdown`]).
    pub fn new(deps: SyncDeps, config: SupervisorConfig, cancel: CancellationToken) -> Self {
        Self {
            deps,
            config,
            cancel,
            syncs: Mutex::new(HashMap::new()),
            ops: Mutex::new(HashMap::new()),
        }
    }

    async fn serialize(&self, sync_id: &str) -> OwnedMutexGuard<()> {
        let op = Arc::clone(lock(&self.ops).entry(sync_id.to_owned()).or_default());
        op.lock_owned().await
    }

    /// Starts supervising a sync. Fails if it is already running; a sync
    /// that stopped (held, failed, user-stopped) is started again with the
    /// new spec.
    pub async fn start(&self, spec: SyncSpec) -> Result<(), SupervisorError> {
        spec.validate().map_err(EngineError::Spec)?;
        let _op = self.serialize(&spec.id).await;
        self.launch(spec, Launch::default())
    }

    /// Stops a sync and marks it `Paused` (stopped by the user). It stays
    /// known: `restart`, `resync` and `release` start it again.
    pub async fn stop(&self, sync_id: &str) -> Result<(), SupervisorError> {
        let _op = self.serialize(sync_id).await;
        self.halt(sync_id).await?;
        persist_phase(&*self.deps.store, sync_id, SyncPhase::Paused, None).await?;
        Ok(())
    }

    /// Stops a sync (if running), deletes the shadow indexes a crashed
    /// resync may have left (best effort), tears the source down on the
    /// origin (replication slot, publication, consumer group) and forgets
    /// the sync. Its saved state is left alone. When the teardown fails the
    /// sync stays known (stopped), so the call can be retried.
    pub async fn remove(&self, sync_id: &str) -> Result<(), SupervisorError> {
        let _op = self.serialize(sync_id).await;
        let spec = self.halt(sync_id).await?;
        for stream in &spec.streams {
            let shadow = shadow_index_uid(&stream.index, &spec.id);
            let deleted = tokio::select! {
                _ = self.cancel.cancelled() => return Err(SupervisorError::ShuttingDown),
                r = self.deps.sink.delete_index(&shadow) => r,
            };
            if let Err(e) = deleted {
                warn!(sync_id, index = %shadow, error = %e, "could not delete a shadow index");
            }
        }
        let mut source = self
            .deps
            .sources
            .build(&spec.id, &spec.source, &spec.streams)
            .map_err(EngineError::from)?;
        let torn_down = tokio::select! {
            _ = self.cancel.cancelled() => return Err(SupervisorError::ShuttingDown),
            r = source.teardown() => r,
        };
        torn_down.map_err(EngineError::from)?;
        lock(&self.syncs).remove(sync_id);
        info!(sync_id, "sync removed");
        Ok(())
    }

    /// Stops and starts again with the same spec.
    pub async fn restart(&self, sync_id: &str) -> Result<(), SupervisorError> {
        self.relaunch(sync_id, Launch::default()).await
    }

    /// Rebuilds every index of the sync from a fresh snapshot, through
    /// shadow indexes swapped in when complete.
    pub async fn resync(&self, sync_id: &str) -> Result<(), SupervisorError> {
        self.relaunch(
            sync_id,
            Launch {
                resync: true,
                release_hold: false,
            },
        )
        .await
    }

    /// Lets the action a guard held (truncate, mass delete) through, once.
    pub async fn release(&self, sync_id: &str) -> Result<(), SupervisorError> {
        self.relaunch(
            sync_id,
            Launch {
                resync: false,
                release_hold: true,
            },
        )
        .await
    }

    pub fn sync_ids(&self) -> Vec<String> {
        lock(&self.syncs).keys().cloned().collect()
    }

    pub async fn status(&self, sync_id: &str) -> Result<Option<SyncStatus>, SupervisorError> {
        let info = {
            let syncs = lock(&self.syncs);
            match syncs.get(sync_id) {
                Some(entry) => lock(&entry.info).clone(),
                None => return Ok(None),
            }
        };
        let state = self.deps.store.load(sync_id).await?;
        Ok(Some(SyncStatus {
            sync_id: sync_id.to_owned(),
            running: info.running,
            phase: state.as_ref().map_or(SyncPhase::Pending, |s| s.phase),
            message: state.as_ref().and_then(|s| s.message.clone()),
            resyncing: state.as_ref().is_some_and(|s| s.resyncing),
            checkpoint: state.and_then(|s| s.checkpoint),
            consecutive_failures: info.consecutive_failures,
            last_error: info.last_error,
        }))
    }

    /// Cancels every sync and waits for them to stop. Phases are left as
    /// they are so the syncs resume where they were on the next start.
    pub async fn shutdown(&self) {
        self.cancel.cancel();
        let handles: Vec<JoinHandle<()>> = lock(&self.syncs)
            .values_mut()
            .filter_map(|e| e.handle.take())
            .collect();
        for handle in handles {
            if let Err(e) = handle.await {
                warn!(error = %e, "sync task ended abnormally");
            }
        }
    }

    fn launch(&self, spec: SyncSpec, launch: Launch) -> Result<(), SupervisorError> {
        if self.cancel.is_cancelled() {
            return Err(SupervisorError::ShuttingDown);
        }
        let mut syncs = lock(&self.syncs);
        if let Some(entry) = syncs.get(&spec.id)
            && entry.handle.as_ref().is_some_and(|h| !h.is_finished())
        {
            return Err(SupervisorError::AlreadyRunning(spec.id.clone()));
        }
        let cancel = self.cancel.child_token();
        let info = Arc::new(Mutex::new(RunInfo {
            running: true,
            ..RunInfo::default()
        }));
        let handle = tokio::spawn(supervise(
            spec.clone(),
            self.deps.clone(),
            self.config.clone(),
            launch,
            cancel.clone(),
            Arc::clone(&info),
        ));
        syncs.insert(
            spec.id.clone(),
            Entry {
                spec,
                cancel,
                handle: Some(handle),
                info,
            },
        );
        Ok(())
    }

    async fn relaunch(&self, sync_id: &str, launch: Launch) -> Result<(), SupervisorError> {
        let _op = self.serialize(sync_id).await;
        let spec = self.halt(sync_id).await?;
        self.launch(spec, launch)
    }

    /// Cancels the sync's loop and waits for it; returns its spec.
    async fn halt(&self, sync_id: &str) -> Result<SyncSpec, SupervisorError> {
        let (spec, handle) = {
            let mut syncs = lock(&self.syncs);
            let entry = syncs
                .get_mut(sync_id)
                .ok_or_else(|| SupervisorError::NotFound(sync_id.to_owned()))?;
            entry.cancel.cancel();
            (entry.spec.clone(), entry.handle.take())
        };
        if let Some(handle) = handle
            && let Err(e) = handle.await
        {
            warn!(sync_id, error = %e, "sync task ended abnormally");
        }
        Ok(spec)
    }
}

/// Exponential backoff with jitter: `initial * 2^(failures-1)`, capped at
/// `max`, then scaled by a random factor in `[0.5, 1.0]`.
pub fn backoff_delay(failures: u32, initial: Duration, max: Duration) -> Duration {
    let exp = failures.saturating_sub(1).min(20);
    let raw = initial.saturating_mul(1u32 << exp).min(max);
    raw.mul_f64(0.5 + 0.5 * jitter())
}

/// A cheap uniform-ish number in `[0, 1)`; good enough to spread retries.
fn jitter() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x9e37_79b9_7f4a_7c15);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let mut x = SEED.fetch_add(0x9e37_79b9_7f4a_7c15, Ordering::Relaxed) ^ nanos;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

async fn persist_phase(
    store: &dyn meili_connect_core::StateStore,
    sync_id: &str,
    phase: SyncPhase,
    message: Option<String>,
) -> Result<(), StoreError> {
    let mut state = store.load(sync_id).await?.unwrap_or_else(|| SyncState {
        sync_id: sync_id.to_owned(),
        phase: SyncPhase::Pending,
        checkpoint: None,
        message: None,
        resyncing: false,
        resync_reason: None,
        updated_at: Utc::now(),
    });
    state.phase = phase;
    state.message = message;
    state.updated_at = Utc::now();
    store.save(&state).await
}

async fn persist_message(
    store: &dyn meili_connect_core::StateStore,
    sync_id: &str,
    message: String,
) -> Result<(), StoreError> {
    if let Some(mut state) = store.load(sync_id).await? {
        state.message = Some(message);
        state.updated_at = Utc::now();
        store.save(&state).await?;
    }
    Ok(())
}

async fn supervise(
    spec: SyncSpec,
    deps: SyncDeps,
    config: SupervisorConfig,
    launch: Launch,
    cancel: CancellationToken,
    info: Arc<Mutex<RunInfo>>,
) {
    let sync_id = spec.id.clone();
    let mut options = config.sync.clone();
    options.resync = launch.resync;
    // A resync launched from outside is the user's.
    options.resync_reason = ResyncReason::User;
    options.release_hold = launch.release_hold;
    let mut failures: u32 = 0;
    // Shared by every attempt, so the resync budget spans them.
    let monitor = RunMonitor::new(config.max_consecutive_resyncs, config.healthy_after);
    let mut stuck = StuckSink::default();
    info!(%sync_id, "sync supervised");

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let started = Instant::now();
        let result = {
            let run = run_sync_monitored(
                spec.clone(),
                deps.clone(),
                options.clone(),
                cancel.child_token(),
                &monitor,
            );
            tokio::pin!(run);
            let mut tick = tokio::time::interval_at(
                Instant::now() + config.seconds_interval,
                config.seconds_interval.max(Duration::from_millis(10)),
            );
            loop {
                tokio::select! {
                    r = &mut run => break r,
                    _ = tick.tick() => record_seconds(&*deps.store, &sync_id, &monitor).await,
                }
            }
        };
        record_seconds(&*deps.store, &sync_id, &monitor).await;
        // One-shot requests stay set until a run actually consumed them:
        // a run that failed early retries them.
        if monitor.take_resync_consumed() {
            options.resync = false;
        }
        let released = monitor.take_release_consumed();
        // A run that streamed healthily is past whatever was held: a
        // release left pending must not let a later guard through.
        let healthy = monitor.take_streamed_healthily();
        if released || healthy {
            options.release_hold = false;
        }
        if cancel.is_cancelled() {
            break;
        }
        if started.elapsed() >= config.healthy_after {
            failures = 0;
        }

        let error = match result {
            Ok(()) => EngineError::Source(meili_connect_core::SourceError::Transient(
                "the sync stopped without being cancelled".into(),
            )),
            Err(e) => e,
        };
        let mut reaction = error.reaction();
        let mut message = error.to_string();
        let sink_transient = matches!(error, EngineError::Sink(SinkError::Transient(_)));
        let position = deps
            .store
            .load(&sync_id)
            .await
            .map(|state| state.and_then(|s| s.checkpoint));
        let repeats = stuck.observe(position.ok(), sink_transient);
        if sink_transient
            && config.max_sink_transient_failures > 0
            && repeats >= config.max_sink_transient_failures
        {
            reaction = Reaction::Hold;
            message = format!(
                "{message} (failed {repeats} times in a row on the same batch; fix the cause, \
                 then restart the sync)"
            );
        }
        failures = failures.saturating_add(1);
        {
            let mut info = lock(&info);
            info.consecutive_failures = failures;
            info.last_error = Some(message.clone());
        }

        let store = &*deps.store;
        let wait = match reaction {
            Reaction::Retry => {
                let delay = backoff_delay(failures, config.backoff_initial, config.backoff_max);
                warn!(%sync_id, error = %message, failures, ?delay, "sync failed, retrying");
                report(persist_message(store, &sync_id, message).await, &sync_id);
                delay
            }
            Reaction::Resync => {
                warn!(%sync_id, error = %message, "position lost again, resyncing");
                options.resync = true;
                options.resync_reason = ResyncReason::System;
                backoff_delay(failures, config.backoff_initial, config.backoff_max)
            }
            Reaction::SourcePaused => {
                info!(%sync_id, error = %message, "source paused, retrying later");
                report(
                    persist_phase(store, &sync_id, SyncPhase::SourcePaused, Some(message)).await,
                    &sync_id,
                );
                config.paused_retry
            }
            Reaction::Hold => {
                warn!(%sync_id, error = %message, "sync held, waiting for the user");
                report(
                    persist_phase(store, &sync_id, SyncPhase::Held, Some(message)).await,
                    &sync_id,
                );
                break;
            }
            Reaction::Fail => {
                warn!(%sync_id, error = %message, "sync failed permanently");
                report(
                    persist_phase(store, &sync_id, SyncPhase::Failed, Some(message)).await,
                    &sync_id,
                );
                break;
            }
        };

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }

    lock(&info).running = false;
    info!(%sync_id, "sync no longer supervised");
}

/// Consecutive `SinkError::Transient` failures at one saved checkpoint.
#[derive(Debug, Default)]
struct StuckSink {
    /// The saved checkpoint the count refers to (`Some(None)`: no
    /// checkpoint yet, during the first snapshot).
    at: Option<Option<Checkpoint>>,
    count: u32,
}

impl StuckSink {
    /// Records one failed run; `position` is the saved checkpoint after it
    /// (`None` when the store could not tell). Returns the count.
    fn observe(&mut self, position: Option<Option<Checkpoint>>, sink_transient: bool) -> u32 {
        if let Some(position) = position
            && self.at.as_ref() != Some(&position)
        {
            // Progress (or a first observation): start over.
            self.at = Some(position);
            self.count = 0;
        }
        if sink_transient {
            self.count = self.count.saturating_add(1);
        } else {
            self.count = 0;
        }
        self.count
    }
}

fn report(result: Result<(), StoreError>, sync_id: &str) {
    if let Err(e) = result {
        warn!(sync_id, error = %e, "could not persist the sync phase");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(60);
        for failures in 1..40 {
            let d = backoff_delay(failures, initial, max);
            let exp = initial.saturating_mul(1 << (failures - 1).min(20)).min(max);
            assert!(d <= exp, "{failures}: {d:?} > {exp:?}");
            assert!(d >= exp / 2, "{failures}: {d:?} < half of {exp:?}");
            assert!(d <= max);
        }
    }

    #[test]
    fn stuck_sink_counts_only_at_one_checkpoint() {
        let at = |n: u64| Some(Some(Checkpoint(serde_json::json!(n))));
        let mut stuck = StuckSink::default();
        assert_eq!(stuck.observe(at(1), true), 1);
        assert_eq!(stuck.observe(at(1), true), 2);
        assert_eq!(
            stuck.observe(None, true),
            3,
            "unknown position keeps counting"
        );
        assert_eq!(stuck.observe(at(2), true), 1, "progress resets");
        assert_eq!(stuck.observe(at(2), false), 0, "another error resets");
        assert_eq!(stuck.observe(Some(None), true), 1);
    }

    #[test]
    fn jitter_is_in_range_and_varies() {
        let samples: Vec<f64> = (0..100).map(|_| jitter()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        let first = samples[0];
        assert!(samples.iter().any(|x| (x - first).abs() > 1e-9));
    }
}
