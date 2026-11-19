//! Error taxonomy. The engine decides what to do from the variant alone, so
//! pick the variant by the required reaction, not by where the error arose.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SourceError {
    /// Network blips, restarts, failovers, throttling. The engine reopens
    /// the source with backoff and resumes from the last durable checkpoint.
    #[error("transient source error: {0}")]
    Transient(String),
    /// The saved position no longer exists (replication slot invalidated,
    /// binlog purged, oplog rolled over, stream records expired). The engine
    /// starts a full resync into a shadow index.
    #[error("source position lost: {0}")]
    PositionLost(String),
    /// Wrong credentials, missing privilege, missing prerequisite
    /// (`wal_level`, `binlog_format`, replica set). Needs the user: the sync
    /// is paused with this message.
    #[error("source configuration error: {0}")]
    Config(String),
    /// The source is deliberately unavailable (paused Supabase project).
    /// Shown as "source paused", retried slowly, never alerted as a failure.
    #[error("source paused: {0}")]
    Paused(String),
    /// A bug or an unsupported shape. The sync fails and stays failed.
    #[error("fatal source error: {0}")]
    Fatal(String),
}

#[derive(Debug, Error)]
pub enum SinkError {
    /// Timeouts, 5xx, 429, connection resets. Retried with backoff; the
    /// batch is replayed unchanged.
    #[error("transient Meilisearch error: {0}")]
    Transient(String),
    /// 401/403, unknown host, index primary key conflict. Needs the user.
    #[error("Meilisearch configuration error: {0}")]
    Config(String),
    /// Meilisearch rejected the whole payload for a reason that is not one
    /// document's fault and will not go away on retry.
    #[error("fatal Meilisearch error: {0}")]
    Fatal(String),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("transient store error: {0}")]
    Transient(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("store error: {0}")]
    Fatal(String),
}
