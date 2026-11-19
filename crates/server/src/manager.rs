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

use std::collections::{HashMap, HashSet};
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
use meili_connect_store::{StoredSync, SyncRow};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::credits::{CreditCache, OUT_OF_CREDITS};
use crate::error::ApiError;
use crate::guard::{CoarseSink, GuardedSources};
use crate::metrics::{InstrumentedSink, Metrics};
use crate::redact::same_origin;
use crate::state::Backend;
use crate::usage::UsageEvents;

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
    /// Describes and drains the syncs' usage (a deleted sync's usage is
    /// enqueued before it goes).
    pub usage: Arc<UsageEvents>,
    /// Syncs one tenant may own.
    pub max_syncs_per_tenant: u64,
    /// Longest wait for one sync to stop.
    pub stop_grace: Duration,
    /// Lab mode: whether a tenant may still spend.
    pub credits: Option<Arc<CreditCache>>,
    pub metrics: Arc<Metrics>,
    /// Lab mode or a public host policy: tenants may not use the server's
    /// own identity or files.
    pub hosted: bool,
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
    usage: Arc<UsageEvents>,
    max_syncs_per_tenant: u64,
    stop_grace: Duration,
    /// Time limit of a teardown run outside a supervisor.
    teardown_timeout: Duration,
    supervisors: Mutex<HashMap<String, Arc<Supervisor>>>,
    ops: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    credits: Option<Arc<CreditCache>>,
    metrics: Arc<Metrics>,
    hosted: bool,
    /// The tenant of every known tenant-owned sync (for the credit checks).
    tenants: Mutex<HashMap<String, String>>,
    /// Syncs held because their team is out of credits; they resume by
    /// themselves.
    credit_held: Mutex<HashSet<String>>,
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
            usage: deps.usage,
            max_syncs_per_tenant: deps.max_syncs_per_tenant,
            stop_grace: deps.stop_grace,
            teardown_timeout: Duration::from_secs(60),
            supervisors: Mutex::new(HashMap::new()),
            ops: Mutex::new(HashMap::new()),
            credits: deps.credits,
            metrics: deps.metrics,
            hosted: deps.hosted,
            tenants: Mutex::new(HashMap::new()),
            credit_held: Mutex::new(HashSet::new()),
        }
    }

    pub fn usage(&self) -> &Arc<UsageEvents> {
        &self.usage
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

    /// The stored row, read inside the lock. A row whose spec cannot be
    /// read can only be deleted (with `force`).
    async fn row(&self, tenant: Option<&str>, id: &str) -> ApiResult<StoredSync> {
        match self.backend.get_sync(tenant, id).await? {
            Some(SyncRow::Readable(stored)) => Ok(stored),
            Some(SyncRow::Unreadable(_)) => Err(unreadable()),
            None => Err(ApiError::sync_not_found(id)),
        }
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
        .refusing_ambient_identity(self.hosted && spec.tenant.is_some())
    }

    /// The sink of one sync; coarse errors for a tenant's sync.
    fn sink(&self, spec: &SyncSpec) -> Result<Arc<dyn Sink>, SinkError> {
        let sink: Arc<dyn Sink> = Arc::new(InstrumentedSink::new(
            (self.sinks)(&spec.destination)?,
            Arc::clone(&self.metrics),
        ));
        Ok(if spec.tenant.is_some() {
            Arc::new(CoarseSink(sink))
        } else {
            sink
        })
    }

    fn new_supervisor(&self, spec: &SyncSpec) -> Result<Arc<Supervisor>, SupervisorError> {
        let sink = self.sink(spec).map_err(EngineError::from)?;
        let deps = SyncDeps {
            sources: Arc::new(
                self.guarded_sources(spec)
                    .with_metrics(Arc::clone(&self.metrics)),
            ),
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
    ///
    /// Bounded by `stop_grace`: the per-sync lock is held meanwhile, so a
    /// sync that does not stop must not block every later call on it. Past
    /// the grace the wait is aborted and the old loop abandoned: it is
    /// cancelled already and ends at its next await point (the engine aborts
    /// a source that does not stop within its own grace).
    async fn retire(&self, sync_id: &str) {
        let old = lock(&self.supervisors).remove(sync_id);
        if let Some(old) = old {
            let wait = tokio::spawn(async move { old.shutdown().await });
            let abort = wait.abort_handle();
            if tokio::time::timeout(self.stop_grace, wait).await.is_err() {
                abort.abort();
                warn!(
                    sync_id,
                    grace = ?self.stop_grace,
                    "the sync did not stop within the grace period; it is abandoned"
                );
            }
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
        self.register(&spec);
        lock(&self.credit_held).remove(&spec.id);
        let sup = self.new_supervisor(&spec)?;
        sup.start(spec.clone()).await?;
        lock(&self.supervisors).insert(spec.id.clone(), Arc::clone(&sup));
        Ok(sup)
    }

    fn register(&self, spec: &SyncSpec) {
        self.usage.register(spec);
        if let Some(tenant) = &spec.tenant {
            lock(&self.tenants).insert(spec.id.clone(), tenant.clone());
        }
    }

    fn unregister(&self, sync_id: &str) {
        self.usage.forget(sync_id);
        lock(&self.tenants).remove(sync_id);
        lock(&self.credit_held).remove(sync_id);
    }

    /// Register a sync without running it (a paused sync at boot). A sync
    /// held for credits resumes by itself once its team can spend again.
    pub fn register_idle(&self, spec: &SyncSpec, state: Option<&SyncState>) {
        self.register(spec);
        if state.is_some_and(is_credit_hold) {
            lock(&self.credit_held).insert(spec.id.clone());
        }
    }

    // ------------------------------------------------------------ credits

    /// Whether the sync's team may spend (always outside Lab mode and for
    /// the global scope).
    async fn can_spend(&self, tenant: Option<&str>) -> bool {
        match (&self.credits, tenant) {
            (Some(credits), Some(tenant)) => credits.can_spend(tenant).await,
            _ => true,
        }
    }

    pub fn is_credit_held(&self, sync_id: &str) -> bool {
        lock(&self.credit_held).contains(sync_id)
    }

    /// Stop the sync, nothing torn down, and mark it held for credits.
    async fn hold_for_credits(&self, sync_id: &str) -> Result<(), StoreError> {
        self.retire(sync_id).await;
        persist_phase(
            &*self.store,
            sync_id,
            SyncPhase::Held,
            Some(OUT_OF_CREDITS.to_owned()),
        )
        .await?;
        lock(&self.credit_held).insert(sync_id.to_owned());
        Ok(())
    }

    /// A start refused because the team is out of credits.
    async fn refuse_without_credits(&self, spec: &SyncSpec) -> ApiResult<()> {
        if self.can_spend(spec.tenant.as_deref()).await {
            return Ok(());
        }
        // Keep it (or put it) on hold: it resumes by itself.
        if !self.is_credit_held(&spec.id) && self.supervisor(&spec.id).is_none() {
            let phase = self.store.load(&spec.id).await?.map(|s| s.phase);
            if phase != Some(SyncPhase::Paused) {
                self.hold_for_credits(&spec.id).await?;
            }
        }
        Err(ApiError::out_of_credits())
    }

    /// Hold the running syncs of every team that cannot spend, and resume
    /// the syncs held for credits of every team that can again. Called
    /// every `CREDITS_CHECK_INTERVAL_SECS` in Lab mode.
    pub async fn enforce_credits(&self) {
        if self.credits.is_none() {
            return;
        }
        let mut by_tenant: HashMap<String, Vec<String>> = HashMap::new();
        for (id, tenant) in lock(&self.tenants).iter() {
            by_tenant
                .entry(tenant.clone())
                .or_default()
                .push(id.clone());
        }
        for (tenant, ids) in by_tenant {
            let can_spend = self.can_spend(Some(&tenant)).await;
            for id in ids {
                if let Err(e) = self.enforce_one(&tenant, &id, can_spend).await {
                    warn!(sync_id = %id, error = %e.message, "credit check could not be applied");
                }
            }
        }
    }

    async fn enforce_one(&self, tenant: &str, id: &str, can_spend: bool) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        if !can_spend {
            if self.is_credit_held(id) {
                return Ok(());
            }
            let running = match self.supervisor(id) {
                Some(sup) => sup.status(id).await?.is_some_and(|s| s.running),
                None => false,
            };
            if running {
                self.hold_for_credits(id).await?;
                info!(sync_id = %id, tenant, "team out of credits: sync held");
            }
            return Ok(());
        }
        if !self.is_credit_held(id) {
            return Ok(());
        }
        lock(&self.credit_held).remove(id);
        // Resume only what is still held for credits (the user may have
        // paused or deleted it since).
        let state = self.store.load(id).await?;
        if !state.as_ref().is_some_and(is_credit_hold) {
            return Ok(());
        }
        if let Some(SyncRow::Readable(stored)) = self.backend.get_sync(Some(tenant), id).await? {
            self.run_locked(stored.spec).await?;
            info!(sync_id = %id, tenant, "credits are back: sync resumed");
        }
        Ok(())
    }

    /// Run [`SyncManager::enforce_credits`] every `interval` until `cancel`.
    pub async fn run_credit_checks(self: Arc<Self>, interval: Duration, cancel: CancellationToken) {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(interval) => {}
            }
            self.enforce_credits().await;
        }
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
        let op = self.serialize(&spec.id).await;
        let stored = self
            .backend
            .create_sync(spec.tenant.as_deref(), &spec)
            .await?;
        // A team out of credits may create syncs; they start held.
        if !self.can_spend(spec.tenant.as_deref()).await {
            self.register(&spec);
            self.hold_for_credits(&spec.id).await?;
            info!(sync_id = %spec.id, "team out of credits: the new sync starts held");
            return Ok(stored);
        }
        // All or nothing: a sync that cannot start is not kept (it would
        // start unannounced at the next boot).
        let (id, tenant) = (spec.id.clone(), spec.tenant.clone());
        if let Err(e) = self.run_locked(spec).await {
            warn!(sync_id = %id, error = %e, "the new sync could not start; it is not kept");
            self.retire(&id).await;
            self.unregister(&id);
            if let Err(e) = self.backend.delete_sync(tenant.as_deref(), &id).await {
                warn!(sync_id = %id, error = %e, "could not roll the new sync back");
            }
            drop(op);
            self.forget_lock(&id);
            return Err(e.into());
        }
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
        let starved = !paused && !self.can_spend(tenant).await;

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
                    resync_reason: None,
                    updated_at: Utc::now(),
                })
                .await?;
        }
        if paused {
            self.retire(id).await;
            self.register(&spec);
            info!(sync_id = %id, ?change, "paused sync updated; it stays paused");
        } else if starved {
            self.register(&spec);
            self.hold_for_credits(id).await?;
            info!(sync_id = %id, ?change, "sync updated; held, its team is out of credits");
        } else {
            self.run_locked(spec).await?;
            info!(sync_id = %id, ?change, "sync updated and restarted");
        }
        Ok(stored)
    }

    /// Tear the sync down on the origin and delete it with its state. A
    /// sync whose spec cannot be read cannot be torn down: it is deleted
    /// with `force` only.
    pub async fn delete(&self, tenant: Option<&str>, id: &str, force: bool) -> ApiResult<()> {
        let op = self.serialize(id).await;
        let current = match self.backend.get_sync(tenant, id).await? {
            None => return Err(ApiError::sync_not_found(id)),
            Some(SyncRow::Unreadable(_)) if !force => return Err(unreadable()),
            Some(SyncRow::Unreadable(_)) => {
                warn!(sync_id = %id, "deleting an unreadable sync; its origin is not cleaned up");
                self.retire(id).await;
                self.usage.flush_sync(&*self.backend, id).await;
                self.unregister(id);
                self.backend.delete_sync(tenant, id).await?;
                drop(op);
                self.forget_lock(id);
                return Ok(());
            }
            Some(SyncRow::Readable(current)) => current,
        };
        match self.remove_locked(&current.spec, force).await {
            Ok(()) => {}
            Err(RemoveError::Teardown(message)) => {
                // Kept, stopped: say so durably, so a restart of the server
                // does not start it again and the status explains why.
                let note = format!(
                    "deletion failed, the sync is stopped: {message}. Retry the deletion, or \
                     delete with ?force=true and clean up the origin by hand"
                );
                persist_phase(&*self.store, id, SyncPhase::Paused, Some(note)).await?;
                return Err(ApiError::new(
                    StatusCode::BAD_GATEWAY,
                    "teardown_failed",
                    format!(
                        "{message}; the sync is kept (paused). Retry, or delete with \
                         ?force=true and clean up the origin by hand"
                    ),
                ));
            }
            Err(RemoveError::Supervisor(e)) => return Err(e.into()),
        }
        // Its pending usage goes now (or with the next flush: it is not
        // deleted with the sync).
        self.usage.flush_sync(&*self.backend, id).await;
        self.unregister(id);
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
            match tokio::time::timeout(self.stop_grace, sup.stop(id)).await {
                Ok(Ok(())) | Ok(Err(SupervisorError::NotFound(_))) => {}
                Ok(Err(e)) => return Err(e.into()),
                // Did not stop in time: abandon it (bounded as well).
                Err(_) => self.retire(id).await,
            }
        }
        persist_phase(&*self.store, id, SyncPhase::Paused, None).await?;
        // The user's pause wins over a credit hold: it stays paused.
        lock(&self.credit_held).remove(id);
        Ok(())
    }

    /// Start the sync with its stored spec unless it runs.
    pub async fn resume(&self, tenant: Option<&str>, id: &str) -> ApiResult<()> {
        let _op = self.serialize(id).await;
        let current = self.row(tenant, id).await?;
        self.refuse_without_credits(&current.spec).await?;
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
        self.refuse_without_credits(&spec).await?;
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
        let state = self.store.load(id).await?;
        if state.as_ref().map(|s| s.phase) != Some(SyncPhase::Held) {
            return Err(ApiError::conflict(format!(
                "sync {id} is not held; only a held sync can be released"
            )));
        }
        self.refuse_without_credits(&spec).await?;
        if state.as_ref().is_some_and(is_credit_hold) {
            // Credits are back: what held it is gone, nothing to let
            // through.
            self.run_locked(spec).await?;
            return Ok(());
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

    /// The lag in seconds of every running sync whose source can tell.
    pub async fn lag_seconds(&self) -> Vec<f64> {
        let all: Vec<(String, Arc<Supervisor>)> = lock(&self.supervisors)
            .iter()
            .map(|(id, s)| (id.clone(), Arc::clone(s)))
            .collect();
        let mut out = Vec::with_capacity(all.len());
        for (id, sup) in all {
            if let Ok(Some(st)) = sup.status(&id).await
                && let Some(seconds) = st.lag.and_then(|l| l.seconds)
            {
                out.push(seconds);
            }
        }
        out
    }

    pub fn running_ids(&self) -> Vec<String> {
        lock(&self.supervisors).keys().cloned().collect()
    }
}

/// The state of a sync held because its team is out of credits.
fn is_credit_hold(state: &SyncState) -> bool {
    state.phase == SyncPhase::Held && state.message.as_deref() == Some(OUT_OF_CREDITS)
}

fn unreadable() -> ApiError {
    ApiError::new(
        StatusCode::CONFLICT,
        "sync_unreadable",
        crate::api::UNREADABLE,
    )
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
        resync_reason: None,
        updated_at: Utc::now(),
    });
    state.phase = phase;
    state.message = message;
    state.updated_at = Utc::now();
    store.save(&state).await
}
