//! The sync loop: snapshot, stream, coalesce, write, checkpoint, resync, supervise.
//!
//! - [`run_sync`] drives one attempt of one sync through its lifecycle
//!   (`docs/ARCHITECTURE.md`, "The sync loop").
//! - [`Supervisor`] owns many syncs, restarts them with the retry policy of
//!   their last error, and persists their phase.
//! - [`mapping`] and [`coalesce`] are the pure parts: `Change` → `DocOp`,
//!   and folding a batch so each document is written once.

pub mod coalesce;
pub mod error;
pub mod mapping;
pub mod supervisor;
pub mod sync;

pub use coalesce::coalesce;
pub use error::{EngineError, Reaction};
pub use mapping::{Mapper, map_change, map_document};
pub use supervisor::{Supervisor, SupervisorConfig, SupervisorError, SyncStatus, backoff_delay};
pub use sync::{SyncDeps, SyncOptions, run_sync, shadow_index_uid};
