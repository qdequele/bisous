//! Shared state of the HTTP handlers.

use std::sync::Arc;
use std::time::Duration;

use meili_connect_core::{HostPolicy, Secret, SourceFactory, StateStore};
use meili_connect_store::{LabOutbox, Lane, SyncRepository, UsageLedger};

use crate::auth::AuthConfig;
use crate::guard::GuardedSources;
use crate::limits::{Limits, ProbeSlots};
use crate::manager::{SinkFactory, SyncManager};
use crate::metrics::{Metrics, Scrape};

/// Everything a store must be for the server: the engine's state, the
/// tenant-scoped repository, the Lab outbox and the usage ledger.
/// `PgStore` and `MemStore` both are.
pub trait Backend: StateStore + SyncRepository + LabOutbox + UsageLedger {}

impl<T: StateStore + SyncRepository + LabOutbox + UsageLedger> Backend for T {}

#[derive(Clone)]
pub struct AppState {
    pub backend: Arc<dyn Backend>,
    pub manager: Arc<SyncManager>,
    /// The connector registry, unguarded: go through [`AppState::sources`].
    pub registry: Arc<dyn SourceFactory>,
    pub sinks: SinkFactory,
    pub policy: Arc<HostPolicy>,
    pub auth: AuthConfig,
    /// Time limit of `check` and `discover`.
    pub probe_timeout: Duration,
    /// Usage events go to the Lab (all three `LAB_*` event variables set).
    pub lab_events: bool,
    pub metrics: Arc<Metrics>,
    /// When a stuck sync's lag deserves a warning in its status.
    pub lag_guard: crate::config::LagGuard,
    /// The only credential `GET /metrics` accepts.
    pub metrics_token: Option<Secret>,
    pub limits: Limits,
    /// Slots for `check` and `discover`.
    pub probes: Arc<ProbeSlots>,
}

impl AppState {
    /// The connectors behind the host policy; `coarse` for Lab callers.
    pub fn sources(&self, coarse: bool) -> GuardedSources {
        GuardedSources::new(Arc::clone(&self.registry), Arc::clone(&self.policy), coarse)
    }

    pub fn repo(&self) -> &dyn SyncRepository {
        &*self.backend
    }

    pub fn state_store(&self) -> &dyn StateStore {
        &*self.backend
    }

    pub fn outbox(&self) -> &dyn LabOutbox {
        &*self.backend
    }

    /// The live values `/metrics` computes when scraped.
    pub async fn scrape(&self) -> Scrape {
        let lanes = self.manager.usage().lanes();
        let backlog = async |lane: Lane| {
            if lanes.contains(&lane) {
                self.backend.outbox(lane).count_unsent().await.ok()
            } else {
                None
            }
        };
        Scrape {
            phases: self.repo().phase_counts().await.unwrap_or_default(),
            lab_backlog: backlog(Lane::Lab).await,
            tinybird_backlog: backlog(Lane::Tinybird).await,
            lags: self.manager.lag_seconds().await,
        }
    }
}
