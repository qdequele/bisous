use meili_connect_core::StoreError;
use thiserror::Error;

/// Errors of [`crate::SyncRepository`] and [`crate::LabOutbox`]. Finer than
/// [`StoreError`] so the API can answer 404, 409 and 400 by variant.
#[derive(Debug, Error)]
pub enum RepoError {
    #[error("not found: {0}")]
    NotFound(String),
    /// The id, or the name within the tenant, is already taken.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The request is malformed (empty id, tenant mismatch).
    #[error("invalid: {0}")]
    Invalid(String),
    #[error("transient store error: {0}")]
    Transient(String),
    #[error("store error: {0}")]
    Fatal(String),
}

impl From<StoreError> for RepoError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::Transient(m) => RepoError::Transient(m),
            StoreError::NotFound(m) => RepoError::NotFound(m),
            StoreError::Fatal(m) => RepoError::Fatal(m),
        }
    }
}

impl From<RepoError> for StoreError {
    fn from(e: RepoError) -> Self {
        match e {
            RepoError::NotFound(m) => StoreError::NotFound(m),
            RepoError::Transient(m) => StoreError::Transient(m),
            RepoError::Conflict(m) => StoreError::Fatal(format!("conflict: {m}")),
            RepoError::Invalid(m) => StoreError::Fatal(format!("invalid: {m}")),
            RepoError::Fatal(m) => StoreError::Fatal(m),
        }
    }
}

/// Map a sqlx error by the reaction it needs. Messages never include bound
/// values.
pub(crate) fn from_sqlx(e: sqlx::Error) -> RepoError {
    match &e {
        sqlx::Error::RowNotFound => RepoError::NotFound("row not found".into()),
        sqlx::Error::Database(db) => {
            if db.is_unique_violation() {
                return RepoError::Conflict(db.message().to_owned());
            }
            if db.is_foreign_key_violation() {
                return RepoError::NotFound("sync not found".into());
            }
            let code = db.code().map(|c| c.into_owned()).unwrap_or_default();
            // 08: connection exception, 40001: serialization failure,
            // 40P01: deadlock, 53: insufficient resources, 57P0x: shutdown.
            if code.starts_with("08")
                || code == "40001"
                || code == "40P01"
                || code.starts_with("53")
                || code.starts_with("57P0")
            {
                RepoError::Transient(format!("{} ({code})", db.message()))
            } else {
                RepoError::Fatal(format!("{} ({code})", db.message()))
            }
        }
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Protocol(_) => RepoError::Transient(e.to_string()),
        _ => RepoError::Fatal(e.to_string()),
    }
}
