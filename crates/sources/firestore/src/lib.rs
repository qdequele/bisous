//! Firestore source: paginated `RunQuery` snapshot and realtime `Listen`.
//!
//! - **Config** ([`FirestoreConfig`]): a service account JSON key (a
//!   [`meili_connect_core::Secret`]), the project (the key's by default) and
//!   database, or `emulator_host` for the Firestore emulator.
//! - **IAM**: `roles/datastore.viewer` (Cloud Datastore Viewer) on the
//!   project: `ListCollectionIds`, `RunQuery`, `RunAggregationQuery`
//!   (discover's counts) and `Listen`.
//! - **Collections** ([`target`]): a top-level collection (`products`), one
//!   subcollection (`users/alice/orders`), or a collection group
//!   (`**/orders`). The document key is the document id for a collection,
//!   the document path for a group; every document carries `_path`.
//! - **Snapshot**: every document, `page_size` at a time, in name order.
//! - **Changes** ([`listen`]): one `Listen` stream with a target per
//!   collection; changes → `Upsert`, deletes and removals → `Delete`.
//! - **Checkpoint** ([`ListenCheckpoint`]): the resume token of the last
//!   consistent snapshot of every target (a fresh sync starts from the
//!   server's read time captured before its snapshot).
//! - **Errors**: a refused key, a missing role or database → `Config`;
//!   quota, 5xx, network → `Transient` (the stream reconnects in place
//!   first); a reset the connector cannot apply or a changed set of
//!   collections → `PositionLost`.

pub mod client;
pub mod config;
pub mod convert;
pub mod errors;
pub mod listen;
pub mod source;
pub mod target;

pub use config::{FirestoreConfig, SnapshotOptions, StreamOptions};
pub use listen::ListenCheckpoint;
pub use source::{FirestoreSource, FirestoreSourceFactory};
