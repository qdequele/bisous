//! The contract every source connector implements.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::{Checkpoint, Endpoint, SourceError, SourceEvent, SourceSpec, StreamMapping};

/// Bounded channel from the source to the engine. Back-pressure is the
/// channel being full: a source must `send().await` and never buffer
/// unboundedly on its own. A closed channel means the engine went away:
/// return `Ok(())`.
pub type EventSender = mpsc::Sender<SourceEvent>;

/// The latest checkpoint the engine has made durable (Meilisearch confirmed
/// every change before it, and the store saved it). Sources that hold
/// resources on the origin (a replication slot, consumer-group offsets)
/// release them up to this position, and only up to this position.
pub type AckReceiver = watch::Receiver<Option<Checkpoint>>;
pub type AckSender = watch::Sender<Option<Checkpoint>>;

pub fn ack_channel() -> (AckSender, AckReceiver) {
    watch::channel(None)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// The source can read the existing data (`snapshot`). False for Kafka.
    pub snapshot: bool,
    /// The source may emit [`crate::Op::Patch`].
    pub patches: bool,
}

/// Result of [`Source::open`].
#[derive(Debug, Clone, PartialEq)]
pub enum Opened {
    /// The resume checkpoint is still valid. Go straight to `stream`.
    Resumed,
    /// No resume checkpoint was given (or the source has none to honour).
    /// `start` is the stream position captured *before* any snapshot read,
    /// which is what makes snapshot-then-stream gapless. The engine runs
    /// `snapshot` (when the capability exists), saves `start`, then calls
    /// `stream`.
    Fresh { start: Checkpoint },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Ok,
    Warning,
    Failed,
}

/// One line of the pre-flight report shown to the user, e.g.
/// `wal_level` / Failed / "wal_level is 'replica', set it to 'logical'".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckItem {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CheckReport {
    pub items: Vec<CheckItem>,
}

impl CheckReport {
    pub fn push(&mut self, name: &str, status: CheckStatus, message: impl Into<String>) {
        self.items.push(CheckItem {
            name: name.to_owned(),
            status,
            message: message.into(),
        });
    }

    pub fn passed(&self) -> bool {
        self.items.iter().all(|i| i.status != CheckStatus::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    /// The source's own type name (`int8`, `varchar(255)`, `vector(1536)`).
    pub source_type: String,
    pub nullable: bool,
}

/// A table, collection or topic the user can choose to sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Same naming as [`crate::Change::collection`].
    pub name: String,
    /// Key columns in key order. Empty when the collection has no usable
    /// key: it is listed but cannot be synced.
    pub key: Vec<String>,
    /// Best effort; empty for schemaless sources.
    pub fields: Vec<FieldInfo>,
    /// Approximate row count when cheap to get.
    pub estimated_rows: Option<u64>,
}

/// One running connection to one origin, for one sync.
///
/// Lifecycle driven by the engine:
///
/// 1. `open(resume)`: connect, validate, and either confirm the resume
///    position or capture a fresh start position (creating the replication
///    slot, publication, consumer group… as needed).
/// 2. `snapshot(...)`: only after `Opened::Fresh` and only when
///    `capabilities().snapshot`. Emits every existing row of every mapped
///    collection as `Op::Upsert`, no `Checkpoint` events, then returns.
///    A snapshot interrupted for any reason is restarted from scratch.
/// 3. `stream(...)`: emits changes and checkpoints from the open position
///    until cancelled. Returns `Ok(())` only on cancellation or a closed
///    channel; anything else is an error.
///
/// Cancellation: `snapshot` and `stream` return `Ok(())` promptly when the
/// token fires; the engine never treats a snapshot as complete without
/// checking the token itself. `check`, `discover`, `open` and `teardown`
/// take no token: they must be safe to drop at any await point (the engine
/// races them against its token), leaving at worst an origin-side object
/// that a later `open` reuses or `teardown` removes.
///
/// Acks: a source that needs no origin-side release may ignore the
/// `AckReceiver`, including dropping it.
///
/// After any error the engine drops the value and builds a new one.
/// Implementations must not hold state that a fresh `open` cannot rebuild.
#[async_trait]
pub trait Source: Send {
    fn capabilities(&self) -> Capabilities;

    /// Pre-flight validation for the setup UI. Must not create or change
    /// anything on the origin.
    async fn check(&mut self) -> Result<CheckReport, SourceError>;

    /// List what can be synced. Must not create or change anything.
    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError>;

    /// See the lifecycle above. Returns `SourceError::PositionLost` when
    /// `resume` is given but can no longer be honoured.
    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError>;

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError>;

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError>;

    /// Remove what `open` created on the origin (slot, publication,
    /// consumer group). Called when the sync is deleted. Idempotent.
    async fn teardown(&mut self) -> Result<(), SourceError>;
}

/// Builds a [`Source`] from the stored spec. One implementation per
/// connector crate; the server registers them by [`crate::SourceKind`].
#[async_trait]
pub trait SourceFactory: Send + Sync {
    /// `sync_id` is stable for the life of the sync and safe to embed in
    /// origin-side names (slot, publication, consumer group).
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError>;

    /// Every host and port a source built from `spec` connects to: the
    /// server vets them against its [`crate::HostPolicy`] before each
    /// connection (check, discover, every engine attempt, teardown).
    /// Includes the hosts a seed list expands to when the connector expands
    /// it itself (MongoDB `mongodb+srv` SRV records). Must not connect to
    /// the origin. A config without a host is a `Config` error; an empty
    /// list means the connector only talks to a provider's own endpoints
    /// (AWS without `endpoint_url`).
    ///
    /// The default refuses, so a connector that does not declare its
    /// endpoints cannot be used by the server.
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        Err(SourceError::Config(format!(
            "the {} connector does not declare the hosts it connects to",
            spec.kind.as_str()
        )))
    }
}
