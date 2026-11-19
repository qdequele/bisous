//! Runs the syncs of the process.
//!
//! The engine's [`Supervisor`] takes one [`Sink`] in its `SyncDeps`, but
//! every sync writes to its own Meilisearch (its spec's destination). The
//! manager therefore gives each sync its own supervisor, built with a sink
//! for that sync's destination. Supervisors are cheap (a map and a token);
//! each one only ever holds one sync. Their cancellation tokens are children
//! of the process token, so a process shutdown stops them all.
//!
//! Every mutation of a sync (create, update, delete, pause, resume, resync,
//! release) is one manager entry point that takes the sync's lock, reads
//! the stored row again inside it, writes, then starts or stops the
//! supervisor. Nothing acts on a spec read outside the lock: a `PUT` racing
//! a `DELETE` finds the sync gone, a `resume` racing a `PUT` runs the new
//! spec.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use axum::http::StatusCode;
use chrono::Utc;
use meili_connect_core::{
    HostPolicy, MeiliDestination, Sink, SinkError, SourceError, SourceFactory, StateStore,
    StoreError, SyncPhase, SyncSpec, SyncState,
};
use meili_connect_engine::{
    EngineError, Supervisor, SupervisorConfig, SupervisorError, SyncDeps, SyncStatus,
    shadow_index_uid,
};
use meili_connect_store::StoredSync;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::ApiError;
use crate::guard::{CoarseSink, GuardedSources};
use crate::redact::same_origin;
use crate::state::Backend;
use crate::usage::UsageMeter;

type ApiResult<T> = Result<T, ApiError>;

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

/// What [`SyncManager::new`] needs.
pub struct ManagerDeps {
    /// The engine's store (metered).
    pub store: Arc<dyn StateStore>,
    /// The specs, the outbox.
    pub backend: Arc<dyn Backend>,
    /// The connector registry, unguarded.
    pub sources: Arc<dyn SourceFactory>,
    pub sinks: SinkFactory,
    pub policy: Arc<HostPolicy>,
    pub config: SupervisorConfig,
    pub meter: Arc<UsageMeter>,
    /// Usage events go to the Lab: a deleted sync's count is enqueued.
    pub lab_events: bool,
    /// Syncs one tenant may own.
    pub max_syncs_per_tenant: u64,
    pub cancel: CancellationToken,
}

pub struct SyncManager {
    store: Arc<dyn StateStore>,
    backend: Arc<dyn Backend>,
    sources: Arc<dyn SourceFactory>,
    sinks: SinkFactory,
    policy: Arc<HostPolicy>,
    config: SupervisorConfig,
    cancel: CancellationToken,
    meter: Arc<UsageMeter>,
    lab_events: bool,
    max_syncs_per_tenant: u64,
    /// Time limit of a teardown run outside a supervisor.
    teardown_timeout: Duration,
    supervisors: Mutex<HashMap<String, Arc<Supervisor>>>,
    ops: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SyncManager {
    pub fn new(deps: ManagerDeps) -> Self {
        Self {
            store: deps.store,
            backend: deps.backend,
            sources: deps.sources,
            sinks: deps.sinks,
            policy: deps.policy,
            config: deps.config,
            cancel: deps.cancel,
            meter: deps.meter,
            lab_events: deps.lab_events,
            max_syncs_per_tenant: deps.max_syncs_per_tenant,
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

    /// Drop the lock of a deleted sync, unless someone waits on it (they
    /// will find the sync gone).
    fn forget_lock(&self, sync_id: &str) {
        let mut ops = lock(&self.ops);
        if ops.get(sync_id).is_some_and(|m| Arc::strong_count(m) == 1) {
            ops.remove(sync_id);
        }
    }

    /// The stored row, read inside the lock.
    async fn row(&self, tenant: Option<&str>, id: &str) -> ApiResult<StoredSync> {
        self.backend
            .get_sync(tenant, id)
            .await?
            .ok_or_else(|| ApiError::sync_not_found(id))
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

    /// Start a stored sync at boot. The API goes through the entry points.
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

    // ------------------------------------------------------------ entry points

    /// Store a new sync and start it. A tenant owns at most
    /// `max_syncs_per_tenant` syncs.
    pub async fn create(&self, spec: SyncSpec) -> ApiResult<StoredSync> {
        // Creations of one tenant run one at a time, so the count holds.
        let _quota = match &spec.tenant {
            Some(tenant) => Some(self.serialize(&format!("tenant/{tenant}")).await),
            None => None,
        };
        if let Some(tenant) = &spec.tenant {
            let owned = self.backend.list_syncs(Some(tenant)).await?.len() as u64;
            if owned >= self.max_syncs_per_tenant {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    "sync_quota_exceeded",
                    format!(
                        "this tenant already has {owned} syncs, the most it may have; delete \
                         one first"
                    ),
                ));
            }
        }
        let _op = self.serialize(&spec.id).await;
        let stored = self
            .backend
            .create_sync(spec.tenant.as_deref(), &spec)
            .await?;
        self.run_locked(spec).await?;
        Ok(stored)
    }

    /// Replace a sync's spec. `prepare` turns the row read inside the lock
    /// into the new spec (placeholders restored, validated). What happens
    /// next depends on what changed ([`SpecChange`]):
    ///
    /// - the origin: the old origin is torn down first (slot, publication…)
    ///   with the **old** spec; when that fails nothing changes (`502`)
    ///   unless `force`. Then the checkpoint is cleared: a fresh sync.
    /// - the streams or the Meilisearch URL: the checkpoint is cleared, so
    ///   the next start snapshots everything again (into shadow indexes
    ///   where the live ones hold data).
    /// - anything else (name, guard, secrets, tuning): a plain restart.
    ///
    /// A paused sync stays paused.
    pub async fn update<F>(
        &self,
        tenant: Option<&str>,
        id: &str,
        force: bool,
        prepare: F,
    ) -> ApiResult<StoredSync>
    where
        F: AsyncFnOnce(&StoredSync) -> ApiResult<SyncSpec>,
    {
        let _op = self.serialize(id).await;
        let current = self.row(tenant, id).await?;
        let spec = prepare(&current).await?;
        let old = &current.spec;
        let change = SpecChange::between(old, &spec);
        let saved = self.store.load(id).await?;
        let paused = saved.as_ref().is_some_and(|s| s.phase == SyncPhase::Paused);

        match change {
            SpecChange::Origin => match self.remove_locked(old, force).await {
                Ok(()) => {}
                Err(RemoveError::Teardown(message)) => {
                    // Nothing changed: the old spec runs again.
                    if !paused {
                        self.run_locked(old.clone()).await?;
                    }
                    return Err(ApiError::new(
                        StatusCode::BAD_GATEWAY,
                        "teardown_failed",
                        format!(
                            "the source changed and the old one could not be cleaned up \
                             ({message}); nothing was changed. Retry, or update with \
                             ?force=true and clean up the old origin by hand"
                        ),
                    ));
                }
                Err(RemoveError::Supervisor(e)) => return Err(e.into()),
            },
            // Stop it before its state is reset under it.
            SpecChange::Streams => self.retire(id).await,
            SpecChange::Other => {}
        }

        let stored = self.backend.update_sync(tenant, &spec).await?;
        if change != SpecChange::Other {
            let phase = if paused {
                SyncPhase::Paused
            } else {
                SyncPhase::Pending
            };
            self.store
                .save(&SyncState {
                    sync_id: id.to_owned(),
                    phase,
                    checkpoint: None,
                    message: None,
                    resyncing: false,
                    updated_at: Utc::now(),
                })
                .await?;
        }
        if paused {
            self.retire(id).await;
            self.meter.register(id, spec.tenant.as_deref());
            info!(sync_id = %id, ?change, "paused sync updated; it stays paused");
        } else {
            self.run_locked(spec).await?;
            info!(sync_id = %id, ?change, "sync updated and restarted");
        }
        Ok(stored)
    }

    /// Tear the sync down on the origin and delete it with its state.
    pub async fn delete(&self, tenant: Option<&str>, id: &str, force: bool) -> ApiResult<()> {
        let op = self.serialize(id).await;
        let current = self.row(tenant, id).await?;
        match self.remove_locked(&current.spec, force).await {
            Ok(()) => {}
            Err(RemoveError::Teardown(message)) => {
                return Err(ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "teardown_failed",
                    format!(
                        "{message}; the sync is kept (stopped). Retry, or delete with \
                         ?force=true and clean up the origin by hand"
                    ),
                ));
            }
            Err(RemoveError::Supervisor(e)) => return Err(e.into()),
        }
        if self.lab_events {
            if let Err(e) = self.meter.flush_one(&*self.backend, id).await {
                warn!(sync_id = %id, error = %e, "usage of a deleted sync could not be enqueued");
            }
        } else {
            self.meter.forget(id);
        }
        self.backend.delete_sync(tenant, id).await?;
        drop(op);
        self.forget_lock(id);
        info!(sync_id = %id, "sync deleted");
        Ok(())
    }

    /// Stop the sync and mark it `Paused`.
    pub async fn pause(&self, tenant: Option<&str>, id: &str) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        self.row(tenant, id).await?;
        if let Some(sup) = self.supervisor(id) {
            match sup.stop(id).await {
                Ok(()) | Err(SupervisorError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        persist_phase(&*self.store, id, SyncPhase::Paused, None).await?;
        Ok(())
    }

    /// Start the sync with its stored spec unless it runs.
    pub async fn resume(&self, tenant: Option<&str>, id: &str) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        let current = self.row(tenant, id).await?;
        let running = match self.supervisor(id) {
            Some(sup) => sup.status(id).await?.is_some_and(|s| s.running),
            None => false,
        };
        if !running {
            self.run_locked(current.spec).await?;
        }
        Ok(())
    }

    /// Rebuild every index from a fresh snapshot (shadow indexes swapped in
    /// when complete). Starts the sync if it was not running.
    pub async fn resync(&self, tenant: Option<&str>, id: &str) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        let spec = self.row(tenant, id).await?.spec;
        let sup = match self.supervisor(id) {
            Some(sup) => sup,
            None => self.run_locked(spec.clone()).await?,
        };
        match sup.resync(id).await {
            Err(SupervisorError::NotFound(_)) => {
                let sup = self.run_locked(spec).await?;
                sup.resync(id).await?;
            }
            other => other?,
        }
        Ok(())
    }

    /// Let the action a guard held through, once. Only a held sync.
    pub async fn release(&self, tenant: Option<&str>, id: &str) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        let spec = self.row(tenant, id).await?.spec;
        let phase = self.store.load(id).await?.map(|s| s.phase);
        if phase != Some(SyncPhase::Held) {
            return Err(ApiError::conflict(format!(
                "sync {id} is not held; only a held sync can be released"
            )));
        }
        let sup = match self.supervisor(id) {
            Some(sup) => sup,
            None => self.run_locked(spec.clone()).await?,
        };
        match sup.release(id).await {
            Err(SupervisorError::NotFound(_)) => {
                let sup = self.run_locked(spec).await?;
                sup.release(id).await?;
            }
            other => other?,
        }
        Ok(())
    }

    pub async fn status(&self, sync_id: &str) -> Result<Option<SyncStatus>, SupervisorError> {
        match self.supervisor(sync_id) {
            Some(sup) => sup.status(sync_id).await,
            None => Ok(None),
        }
    }

    /// Stop the sync, delete leftover shadow indexes, tear the source down
    /// on the origin (slot, publication…). When the teardown fails the sync
    /// is kept, stopped, unless `force`.
    async fn remove_locked(&self, spec: &SyncSpec, force: bool) -> Result<(), RemoveError> {
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

/// What a `PUT` changed, by the reaction it needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecChange {
    /// Another origin (kind, host, port, URL, user, database, TLS, slot or
    /// publication name…): tear the old one down, start fresh.
    Origin,
    /// Other streams, or another Meilisearch: start fresh (full snapshot).
    Streams,
    /// Name, guard, secrets, tuning: restart.
    Other,
}

impl SpecChange {
    pub fn between(old: &SyncSpec, new: &SyncSpec) -> Self {
        if !same_origin(&old.source, &new.source) {
            SpecChange::Origin
        } else if old.streams != new.streams
            || old.destination.url.trim() != new.destination.url.trim()
        {
            SpecChange::Streams
        } else {
            SpecChange::Other
        }
    }
}

async fn persist_phase(
    store: &dyn StateStore,
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
