//! Shared state of the HTTP handlers.

use std::sync::Arc;
use std::time::Duration;

use meili_connect_core::{SourceFactory, StateStore};
use meili_connect_store::{LabOutbox, SyncRepository};

use crate::auth::AuthConfig;
use crate::manager::{SinkFactory, SyncManager};

/// Everything a store must be for the server: the engine's state, the
/// tenant-scoped repository and the Lab outbox. `PgStore` and `MemStore`
/// both are.
pub trait Backend: StateStore + SyncRepository + LabOutbox {}

impl<T: StateStore + SyncRepository + LabOutbox> Backend for T {}

#[derive(Clone)]
pub struct AppState {
    pub backend: Arc<dyn Backend>,
    pub manager: Arc<SyncManager>,
    pub sources: Arc<dyn SourceFactory>,
    pub sinks: SinkFactory,
    pub auth: AuthConfig,
    /// Time limit of `check` and `discover`.
    pub probe_timeout: Duration,
    /// Usage events go to the Lab (all three `LAB_*` event variables set).
    pub lab_events: bool,
}

impl AppState {
    pub fn repo(&self) -> &dyn SyncRepository {
        &*self.backend
    }

    pub fn state_store(&self) -> &dyn StateStore {
        &*self.backend
    }

    pub fn outbox(&self) -> &dyn LabOutbox {
        &*self.backend
    }
}
