//! Runs the syncs of the process.
//!
//! The engine's [`Supervisor`] takes one [`Sink`] in its `SyncDeps`, but
//! every sync writes to its own Meilisearch (its spec's destination). The
//! manager therefore gives each sync its own supervisor, built with a sink
//! for that sync's destination. Supervisors are cheap (a map and a token);
//! each one only ever holds one sync. Their cancellation tokens are children
//! of the process token, so a process shutdown stops them all.
//!
//! Operations on one sync are serialized by a per-sync lock, so a `PUT`
//! racing a `resync` never leaves two supervisors running one sync.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Utc;
use meili_connect_core::{
    HostPolicy, MeiliDestination, Sink, SinkError, SourceError, SourceFactory, StateStore,
    StoreError, SyncPhase, SyncSpec, SyncState,
};
use meili_connect_engine::{
    EngineError, Supervisor, SupervisorConfig, SupervisorError, SyncDeps, SyncStatus,
    shadow_index_uid,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::guard::{CoarseSink, GuardedSources};
use crate::usage::UsageMeter;

/// Builds the sink of one sync from its destination.
pub type SinkFactory =
    Arc<dyn Fn(&MeiliDestination) -> Result<Arc<dyn Sink>, SinkError> + Send + Sync>;

/// The production sink factory: every sink connects through `policy`.
pub fn meili_sinks(policy: Arc<HostPolicy>) -> SinkFactory {
    Arc::new(move |destination: &MeiliDestination| {
        let sink = meili_connect_sink::MeiliSink::new(
            destination,
            meili_connect_sink::SinkOptions {
                host_policy: Arc::clone(&policy),
                ..meili_connect_sink::SinkOptions::default()
            },
        )?;
        Ok(Arc::new(sink) as Arc<dyn Sink>)
    })
}

/// Why a removal did not go through.
#[derive(Debug, thiserror::Error)]
pub enum RemoveError {
    /// The origin-side teardown failed: the sync is kept (stopped).
    #[error("teardown on the origin failed: {0}")]
    Teardown(String),
    #[error(transparent)]
    Supervisor(#[from] SupervisorError),
}

pub struct SyncManager {
    store: Arc<dyn StateStore>,
    sources: Arc<dyn SourceFactory>,
    sinks: SinkFactory,
    policy: Arc<HostPolicy>,
    config: SupervisorConfig,
    cancel: CancellationToken,
    meter: Arc<UsageMeter>,
    /// Time limit of a teardown run outside a supervisor.
    teardown_timeout: Duration,
    supervisors: Mutex<HashMap<String, Arc<Supervisor>>>,
    ops: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SyncManager {
    pub fn new(
        store: Arc<dyn StateStore>,
        sources: Arc<dyn SourceFactory>,
        sinks: SinkFactory,
        policy: Arc<HostPolicy>,
        config: SupervisorConfig,
        meter: Arc<UsageMeter>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            store,
            sources,
            sinks,
            policy,
            config,
            cancel,
            meter,
            teardown_timeout: Duration::from_secs(60),
            supervisors: Mutex::new(HashMap::new()),
            ops: Mutex::new(HashMap::new()),
        }
    }

    pub fn meter(&self) -> &Arc<UsageMeter> {
        &self.meter
    }

    async fn serialize(&self, sync_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let op = Arc::clone(lock(&self.ops).entry(sync_id.to_owned()).or_default());
        op.lock_owned().await
    }

    fn supervisor(&self, sync_id: &str) -> Option<Arc<Supervisor>> {
        lock(&self.supervisors).get(sync_id).cloned()
    }

    /// The connectors of one sync, behind the host policy; coarse errors
    /// for a tenant's sync.
    fn guarded_sources(&self, spec: &SyncSpec) -> GuardedSources {
        GuardedSources::new(
            Arc::clone(&self.sources),
            Arc::clone(&self.policy),
            spec.tenant.is_some(),
        )
    }

    /// The sink of one sync; coarse errors for a tenant's sync.
    fn sink(&self, spec: &SyncSpec) -> Result<Arc<dyn Sink>, SinkError> {
        let sink = (self.sinks)(&spec.destination)?;
        Ok(if spec.tenant.is_some() {
            Arc::new(CoarseSink(sink))
        } else {
            sink
        })
    }

    fn new_supervisor(&self, spec: &SyncSpec) -> Result<Arc<Supervisor>, SupervisorError> {
        let sink = self.sink(spec).map_err(EngineError::from)?;
        let deps = SyncDeps {
            sources: Arc::new(self.guarded_sources(spec)),
            sink,
            store: Arc::clone(&self.store),
        };
        Ok(Arc::new(Supervisor::new(
            deps,
            self.config.clone(),
            self.cancel.child_token(),
        )))
    }

    /// Stop the sync's current supervisor, if any, leaving its phase.
    async fn retire(&self, sync_id: &str) {
        let old = lock(&self.supervisors).remove(sync_id);
        if let Some(old) = old {
            old.shutdown().await;
        }
    }

    /// (Re)start a sync with this spec, replacing whatever ran it before.
    pub async fn run(&self, spec: SyncSpec) -> Result<(), SupervisorError> {
        let _op = self.serialize(&spec.id).await;
        self.run_locked(spec).await.map(|_| ())
    }

    async fn run_locked(&self, spec: SyncSpec) -> Result<Arc<Supervisor>, SupervisorError> {
        spec.validate().map_err(EngineError::Spec)?;
        self.retire(&spec.id).await;
        self.meter.register(&spec.id, spec.tenant.as_deref());
        let sup = self.new_supervisor(&spec)?;
        sup.start(spec.clone()).await?;
        lock(&self.supervisors).insert(spec.id.clone(), Arc::clone(&sup));
        Ok(sup)
    }

    /// Register a sync without running it (a paused sync at boot).
    pub fn register_idle(&self, spec: &SyncSpec) {
        self.meter.register(&spec.id, spec.tenant.as_deref());
    }

    /// Stop the sync and mark it `Paused`.
    pub async fn pause(&self, sync_id: &str) -> Result<(), SupervisorError> {
        let _op = self.serialize(sync_id).await;
        if let Some(sup) = self.supervisor(sync_id) {
            match sup.stop(sync_id).await {
                Ok(()) | Err(SupervisorError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        persist_phase(&*self.store, sync_id, SyncPhase::Paused).await?;
        Ok(())
    }

    /// Rebuild every index from a fresh snapshot (shadow indexes swapped in
    /// when complete). Starts the sync if it was not running.
    pub async fn resync(&self, spec: SyncSpec) -> Result<(), SupervisorError> {
        let _op = self.serialize(&spec.id).await;
        let sup = match self.supervisor(&spec.id) {
            Some(sup) => sup,
            None => self.run_locked(spec.clone()).await?,
        };
        match sup.resync(&spec.id).await {
            Err(SupervisorError::NotFound(_)) => {
                let sup = self.run_locked(spec.clone()).await?;
                sup.resync(&spec.id).await
            }
            other => other,
        }
    }

    /// Let the action a guard held through, once.
    pub async fn release(&self, spec: SyncSpec) -> Result<(), SupervisorError> {
        let _op = self.serialize(&spec.id).await;
        let sup = match self.supervisor(&spec.id) {
            Some(sup) => sup,
            None => self.run_locked(spec.clone()).await?,
        };
        match sup.release(&spec.id).await {
            Err(SupervisorError::NotFound(_)) => {
                let sup = self.run_locked(spec.clone()).await?;
                sup.release(&spec.id).await
            }
            other => other,
        }
    }

    pub async fn status(&self, sync_id: &str) -> Result<Option<SyncStatus>, SupervisorError> {
        match self.supervisor(sync_id) {
            Some(sup) => sup.status(sync_id).await,
            None => Ok(None),
        }
    }

    /// Stop the sync, delete leftover shadow indexes, tear the source down
    /// on the origin (slot, publication…) and forget the sync. When the
    /// teardown fails the sync is kept, stopped, unless `force`.
    pub async fn remove(&self, spec: &SyncSpec, force: bool) -> Result<(), RemoveError> {
        let _op = self.serialize(&spec.id).await;
        let torn_down = match self.supervisor(&spec.id) {
            // Bounded: an origin that hangs fails the call instead of
            // holding it forever (the sync is already stopped by then).
            Some(sup) => match tokio::time::timeout(
                self.teardown_timeout.saturating_mul(2),
                sup.remove(&spec.id),
            )
            .await
            {
                Err(_) => Err("teardown timed out".to_owned()),
                Ok(Ok(())) => Ok(()),
                Ok(Err(SupervisorError::NotFound(_))) => self.teardown_idle(spec).await,
                Ok(Err(SupervisorError::Engine(e))) => Err(e.to_string()),
                Ok(Err(e)) => return Err(e.into()),
            },
            None => self.teardown_idle(spec).await,
        };
        match torn_down {
            Ok(()) => {}
            Err(message) if force => {
                warn!(sync_id = %spec.id, error = %message, "teardown failed; deleting anyway (force)");
            }
            Err(message) => {
                // Keep it stopped so the call can be retried.
                self.retire(&spec.id).await;
                return Err(RemoveError::Teardown(message));
            }
        }
        self.retire(&spec.id).await;
        lock(&self.ops).remove(&spec.id);
        info!(sync_id = %spec.id, "sync removed");
        Ok(())
    }

    /// What `Supervisor::remove` does, for a sync no supervisor runs.
    async fn teardown_idle(&self, spec: &SyncSpec) -> Result<(), String> {
        if let Ok(sink) = self.sink(spec) {
            for stream in &spec.streams {
                let shadow = shadow_index_uid(&stream.index, &spec.id);
                match tokio::time::timeout(self.teardown_timeout, sink.delete_index(&shadow)).await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        warn!(sync_id = %spec.id, index = %shadow, error = %e, "could not delete a shadow index")
                    }
                    Err(_) => {
                        warn!(sync_id = %spec.id, index = %shadow, "timed out deleting a shadow index")
                    }
                }
            }
        }
        let mut source = self
            .guarded_sources(spec)
            .build(&spec.id, &spec.source, &spec.streams)
            .map_err(|e| e.to_string())?;
        match tokio::time::timeout(self.teardown_timeout, source.teardown()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => Err(SourceError::Transient("teardown timed out".into()).to_string()),
        }
    }

    /// Stop every sync, waiting at most `grace`. Phases are left as they
    /// are, so the syncs resume on the next start.
    pub async fn shutdown(&self, grace: Duration) {
        self.cancel.cancel();
        let all: Vec<Arc<Supervisor>> = lock(&self.supervisors).values().cloned().collect();
        let wait = async {
            for sup in all {
                sup.shutdown().await;
            }
        };
        if tokio::time::timeout(grace, wait).await.is_err() {
            warn!(
                ?grace,
                "some syncs did not stop within the shutdown grace period"
            );
        }
    }

    pub fn running_ids(&self) -> Vec<String> {
        lock(&self.supervisors).keys().cloned().collect()
    }
}

async fn persist_phase(
    store: &dyn StateStore,
    sync_id: &str,
    phase: SyncPhase,
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
    state.message = None;
    state.updated_at = Utc::now();
    store.save(&state).await
}
