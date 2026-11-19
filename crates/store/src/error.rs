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
            if is_transient_sqlstate(&code) {
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

/// SQLSTATEs that go away on retry: failovers, restarts, timeouts and
/// contention. A Postgres failover produces most of them at once (a demoted
/// primary answers 25006, the old connections die with 57P01 or class 08),
/// so treating any of them as fatal would fail every sync permanently.
pub(crate) fn is_transient_sqlstate(code: &str) -> bool {
    const EXACT: &[&str] = &[
        "25006", // read_only_sql_transaction: a demoted primary
        "40001", // serialization_failure
        "40P01", // deadlock_detected
        "55P03", // lock_not_available
        "57014", // query_canceled (statement timeout)
        "57P01", // admin_shutdown
        "57P02", // crash_shutdown
        "57P03", // cannot_connect_now (starting up, in recovery)
        "57P05", // idle_session_timeout
    ];
    const CLASSES: &[&str] = &[
        "08", // connection exception
        "53", // insufficient resources (disk full, out of memory, too many connections)
        "58", // system error (I/O error)
    ];
    EXACT.contains(&code)
        || CLASSES
            .iter()
            .any(|c| code.len() == 5 && code.starts_with(c))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failover_and_contention_states_are_transient() {
        for code in [
            "25006", "57014", "55P03", "58000", "58030", "58P01", "08000", "08003", "08006",
            "08001", "57P01", "57P02", "57P03", "57P05", "40001", "40P01", "53000", "53100",
            "53200", "53300",
        ] {
            assert!(is_transient_sqlstate(code), "{code}");
        }
        for code in [
            "23505", "42P01", "42703", "22P02", "57P04", "XX000", "", "0",
        ] {
            assert!(!is_transient_sqlstate(code), "{code}");
        }
    }

    #[test]
    fn both_error_types_agree() {
        let t = RepoError::Transient("x".into());
        assert!(matches!(StoreError::from(t), StoreError::Transient(_)));
        assert!(matches!(
            RepoError::from(StoreError::Transient("x".into())),
            RepoError::Transient(_)
        ));
    }
}
