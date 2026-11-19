//! Engine errors and the reaction each one calls for.

use meili_connect_core::{SinkError, SourceError, StoreError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Sink(#[from] SinkError),
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A guard stopped the sync (`Truncate`, mass delete). The state is
    /// already saved as `Held` with this message; nothing of the offending
    /// batch was applied or checkpointed.
    #[error("sync held: {0}")]
    Held(String),
    /// The spec cannot run as written (no streams, …). Needs the user.
    #[error("invalid sync spec: {0}")]
    Spec(String),
}

/// What the supervisor does about an error. Decided by the variant alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reaction {
    /// Retry with jittered exponential backoff.
    Retry,
    /// The origin is deliberately down: `SourcePaused`, retry slowly.
    SourcePaused,
    /// Needs the user: `Held`, no retry.
    Hold,
    /// `Failed`, no retry.
    Fail,
    /// Start over into shadow indexes.
    Resync,
}

impl EngineError {
    pub fn reaction(&self) -> Reaction {
        match self {
            EngineError::Source(e) => match e {
                SourceError::Transient(_) => Reaction::Retry,
                SourceError::PositionLost(_) => Reaction::Resync,
                SourceError::Config(_) => Reaction::Hold,
                SourceError::Paused(_) => Reaction::SourcePaused,
                SourceError::Fatal(_) => Reaction::Fail,
            },
            EngineError::Sink(e) => match e {
                SinkError::Transient(_) => Reaction::Retry,
                SinkError::Config(_) => Reaction::Hold,
                SinkError::Fatal(_) => Reaction::Fail,
            },
            EngineError::Store(e) => match e {
                StoreError::Transient(_) => Reaction::Retry,
                StoreError::NotFound(_) | StoreError::Fatal(_) => Reaction::Fail,
            },
            EngineError::Held(_) | EngineError::Spec(_) => Reaction::Hold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reactions_follow_the_variant() {
        let cases: Vec<(EngineError, Reaction)> = vec![
            (SourceError::Transient("x".into()).into(), Reaction::Retry),
            (
                SourceError::PositionLost("x".into()).into(),
                Reaction::Resync,
            ),
            (SourceError::Config("x".into()).into(), Reaction::Hold),
            (
                SourceError::Paused("x".into()).into(),
                Reaction::SourcePaused,
            ),
            (SourceError::Fatal("x".into()).into(), Reaction::Fail),
            (SinkError::Transient("x".into()).into(), Reaction::Retry),
            (SinkError::Config("x".into()).into(), Reaction::Hold),
            (SinkError::Fatal("x".into()).into(), Reaction::Fail),
            (StoreError::Transient("x".into()).into(), Reaction::Retry),
            (StoreError::Fatal("x".into()).into(), Reaction::Fail),
            (EngineError::Held("x".into()), Reaction::Hold),
            (EngineError::Spec("x".into()), Reaction::Hold),
        ];
        for (err, reaction) in cases {
            assert_eq!(err.reaction(), reaction, "{err}");
        }
    }
}
