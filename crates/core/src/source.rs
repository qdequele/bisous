//! The contract every source connector implements.

use std::sync::Arc;

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

/// How far a streaming source is behind its origin, as the source sees it.
/// Each field is `None` when the source cannot tell.
#[derive(Debug, Clone, Copy, PartialEq, Default, Serialize, Deserialize)]
pub struct Lag {
    /// Age of the newest change seen, or `0` once caught up (Postgres
    /// commit timestamps, MongoDB cluster time, Kafka record timestamps,
    /// DynamoDB `ApproximateCreationDateTime`).
    pub seconds: Option<f64>,
    /// Origin bytes not yet confirmed (Postgres: WAL between the slot's
    /// confirmed position and the server's current position).
    pub bytes: Option<u64>,
    /// Records not yet read (Kafka: the sum of high watermark − position
    /// over the assigned partitions).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<u64>,
}

/// What a sync keeps on its origin while it is not reading it, as
/// [`Source::retention`] measures it (Postgres: the WAL its replication slot
/// holds back). Kept in every phase: a stopped sync's slot keeps WAL until
/// the origin's disk fills or the origin drops the slot.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Retention {
    /// Origin log bytes kept for the sync (Postgres: the WAL between the
    /// slot's `restart_lsn` and the server's current position).
    pub bytes: u64,
    /// The origin's own cap past which it drops what it keeps and the sync
    /// must resync (Postgres `max_slot_wal_keep_size`); `None`: no cap.
    pub limit_bytes: Option<u64>,
    /// The origin's own word on it (Postgres `wal_status`: `reserved`,
    /// `extended`, `unreserved`, `lost`), when it has one.
    pub status: Option<String>,
}

/// Where a source publishes its latest [`Lag`]: a `watch` cell, so
/// publishing never blocks and reading it needs nothing from the source
/// (no `&mut` on a source busy streaming). Cheap to clone; clones publish to
/// the same cell.
#[derive(Debug, Clone)]
pub struct LagReporter(Arc<watch::Sender<Option<Lag>>>);

/// Reads what a [`LagReporter`] published.
pub type LagWatch = watch::Receiver<Option<Lag>>;

impl LagReporter {
    pub fn new() -> (Self, LagWatch) {
        let (tx, rx) = watch::channel(None);
        (Self(Arc::new(tx)), rx)
    }

    pub fn report(&self, lag: Lag) {
        self.0.send_replace(Some(lag));
    }

    /// Nothing known (not streaming).
    pub fn clear(&self) {
        self.0.send_replace(None);
    }
}

/// Seconds since a unix timestamp in milliseconds, never negative (clocks
/// drift).
pub fn seconds_since_unix_millis(millis: i64) -> f64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    (now.saturating_sub(millis)).max(0) as f64 / 1000.0
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

    /// The one item a check run without mappings reports in place of its
    /// per-collection checks: `Ok`, since nothing is wrong yet (the setup
    /// UI tests the connection before the user picks anything). `name` is
    /// the item (`tables`), `noun` what is picked (`table`).
    pub fn push_none_selected(&mut self, name: &str, noun: &str) {
        self.push(
            name,
            CheckStatus::Ok,
            format!("no {noun} selected yet: {noun} checks run once you pick them"),
        );
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
    ///
    /// Built without mappings, it is a connection-level pre-flight (the
    /// setup UI tests the connection before the user picks the
    /// collections): it checks what does not depend on them and reports the
    /// per-collection checks as one [`CheckReport::push_none_selected`]
    /// item, never `Failed` or `Warning` for the absence of mappings.
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

    /// Where to publish how far behind the origin `stream` is. The engine
    /// calls it once, before `open`; a source that can tell reports as it
    /// streams (cheaply: no extra round trip per event). The default
    /// ignores it: the lag is then unknown.
    fn report_lag(&mut self, reporter: LagReporter) {
        let _ = reporter;
    }

    /// What the sync keeps on its origin right now ([`Retention`]), read on
    /// a short-lived connection of its own: callable on a fresh value in
    /// any phase (the sync's own run may be streaming, paused or failed),
    /// never opened. Must not create or change anything. `Ok(None)`:
    /// nothing is kept (no slot yet, or it was dropped). The default keeps
    /// nothing the source can measure.
    async fn retention(&mut self) -> Result<Option<Retention>, SourceError> {
        Ok(None)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_selected_is_one_ok_item() {
        let mut report = CheckReport::default();
        report.push("connection", CheckStatus::Ok, "connected");
        report.push_none_selected("tables", "table");
        assert!(report.passed());
        assert_eq!(
            report.items[1],
            CheckItem {
                name: "tables".into(),
                status: CheckStatus::Ok,
                message: "no table selected yet: table checks run once you pick them".into(),
            }
        );
    }
}
