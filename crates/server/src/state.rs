//! Shared state of the HTTP handlers.

use std::sync::Arc;
use std::time::Duration;

use meili_connect_core::{HostPolicy, Secret, SourceFactory, StateStore};
use meili_connect_store::{LabOutbox, Lane, SyncRepository, UsageLedger};

use crate::auth::AuthConfig;
use crate::external_id::{Binding, external_id_binding};
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
    /// Lab mode or a public host policy: a tenant's source may not use the
    /// server's own identity or files (`crate::ambient`).
    pub hosted: bool,
    /// Derives the external ID of a tenant's DynamoDB roles
    /// (`crate::external_id`); `None` without `CONNECT_SECRET_KEY`.
    pub external_ids: Option<Arc<crate::external_id::ExternalIds>>,
    /// The server's own AWS principal (`GET /sources/dynamodb/trust`).
    pub aws_principal: Arc<crate::external_id::AwsPrincipal>,
    /// The published egress addresses (`GET /network`).
    pub egress_ips: Arc<[String]>,
    /// `GET /usage` reads Tinybird when configured.
    pub usage_reader: Option<Arc<crate::tinybird::TinybirdReader>>,
    /// The only credential `GET /metrics` accepts.
    pub metrics_token: Option<Secret>,
    pub limits: Limits,
    /// Slots for `check` and `discover`.
    pub probes: Arc<ProbeSlots>,
}

impl AppState {
    /// The connectors behind the host policy; `coarse` for Lab callers.
    /// A tenant's DynamoDB role is assumed with its external ID.
    pub fn sources(&self, coarse: bool, tenant: Option<&str>) -> GuardedSources {
        GuardedSources::new(Arc::clone(&self.registry), Arc::clone(&self.policy), coarse)
            .binding_external_id(self.external_id_binding(tenant))
    }

    /// The external-ID rule of a source owned by `tenant`, if any.
    pub fn external_id_binding(&self, tenant: Option<&str>) -> Option<Binding> {
        external_id_binding(self.hosted, &self.external_ids, tenant)
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
