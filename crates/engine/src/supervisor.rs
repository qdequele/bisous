//! Runs many syncs: `run_sync` in a loop, errors mapped to phases and a
//! retry policy, phase and message persisted through the store.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Utc;
use meili_connect_core::{Checkpoint, StoreError, SyncPhase, SyncSpec, SyncState};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::{EngineError, Reaction};
use crate::sync::{RunMonitor, SyncDeps, SyncOptions, run_sync_monitored};

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

pub struct Supervisor {
    deps: SyncDeps,
    config: SupervisorConfig,
    cancel: CancellationToken,
    syncs: Mutex<HashMap<String, Entry>>,
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
        }
    }

    /// Starts supervising a sync. Fails if it is already running; a sync
    /// that stopped (held, failed, user-stopped) is started again with the
    /// new spec.
    pub fn start(&self, spec: SyncSpec) -> Result<(), SupervisorError> {
        spec.validate().map_err(EngineError::Spec)?;
        self.launch(spec, Launch::default())
    }

    /// Stops a sync and marks it `Paused` (stopped by the user). It stays
    /// known: `restart`, `resync` and `release` start it again.
    pub async fn stop(&self, sync_id: &str) -> Result<(), SupervisorError> {
        self.halt(sync_id).await?;
        persist_phase(&*self.deps.store, sync_id, SyncPhase::Paused, None).await?;
        Ok(())
    }

    /// Stops a sync (if running) and forgets it, without touching its state.
    pub async fn remove(&self, sync_id: &str) -> Result<(), SupervisorError> {
        self.halt(sync_id).await?;
        lock(&self.syncs).remove(sync_id);
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
    options.release_hold = launch.release_hold;
    let mut failures: u32 = 0;
    // Shared by every attempt, so the resync budget spans them.
    let monitor = RunMonitor::new(config.max_consecutive_resyncs, config.healthy_after);
    info!(%sync_id, "sync supervised");

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let started = Instant::now();
        let result = run_sync_monitored(
            spec.clone(),
            deps.clone(),
            options.clone(),
            cancel.child_token(),
            &monitor,
        )
        .await;
        options.resync = false;
        options.release_hold = false;
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
        let reaction = error.reaction();
        let message = error.to_string();
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
    fn jitter_is_in_range_and_varies() {
        let samples: Vec<f64> = (0..100).map(|_| jitter()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        let first = samples[0];
        assert!(samples.iter().any(|x| (x - first).abs() > 1e-9));
    }
}
