//! Shared contracts for meili-connect.
//!
//! Every source connector, the Meilisearch sink, the state store and the
//! engine depend on this crate and on nothing else of each other. Read
//! `docs/ARCHITECTURE.md` for the reasoning behind each contract.

pub mod change;
pub mod checkpoint;
pub mod doc_id;
pub mod error;
pub mod host_policy;
pub mod secret;
pub mod sink;
pub mod source;
pub mod spec;
pub mod store;

pub use change::{Change, Document, Op, SourceEvent};
pub use checkpoint::Checkpoint;
pub use doc_id::document_id;
pub use error::{SinkError, SourceError, StoreError};
pub use host_policy::{Endpoint, HostPolicy, HostPolicyError};
pub use secret::Secret;
pub use sink::{ApplyReport, DocOp, IndexTarget, RejectedDoc, Sink};
pub use source::{
    AckReceiver, AckSender, Capabilities, CheckItem, CheckReport, CheckStatus, CollectionInfo,
    EventSender, FieldInfo, Opened, Source, SourceFactory, ack_channel,
};
pub use spec::{
    FieldSelection, MeiliDestination, SourceKind, SourceSpec, StreamMapping, SyncSpec,
    VectorMapping,
};
pub use store::{DeadLetter, StateStore, SyncPhase, SyncState};
