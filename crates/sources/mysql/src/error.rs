//! `mysql_async::Error` → `SourceError`, by the reaction the engine needs.

use meili_connect_core::SourceError;
use mysql_async::{DriverError, Error, IoError};

/// Server error codes that need the user (credentials, privileges, missing
/// objects or prerequisites).
const CONFIG_CODES: &[u16] = &[
    1044, // ER_DBACCESS_DENIED_ERROR
    1045, // ER_ACCESS_DENIED_ERROR
    1049, // ER_BAD_DB_ERROR
    1142, // ER_TABLEACCESS_DENIED_ERROR
    1143, // ER_COLUMNACCESS_DENIED_ERROR
    1146, // ER_NO_SUCH_TABLE
    1227, // ER_SPECIFIC_ACCESS_DENIED_ERROR
    1251, // ER_NOT_SUPPORTED_AUTH_MODE
    1370, // ER_PROCACCESS_DENIED_ERROR
    1820, // ER_MUST_CHANGE_PASSWORD
    1862, // ER_MUST_CHANGE_PASSWORD_LOGIN
    3118, // ER_ACCOUNT_HAS_BEEN_LOCKED
    3159, // ER_SECURE_TRANSPORT_REQUIRED
];

/// ER_MASTER_FATAL_ERROR_READING_BINLOG / ER_SOURCE_FATAL_ERROR_READING_BINLOG.
pub const ER_BINLOG_FATAL: u16 = 1236;

/// Messages of error 1236 that mean the requested position is gone.
fn is_position_lost_message(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    [
        "purged",
        "could not find first log file",
        "could not find gtid",
        "has more gtids",
        "binary log is not open",
        "log event entry exceeded",
        "client requested master to start replication from position",
        "client requested source to start replication from position",
        "first log file name in binary log index file",
    ]
    .iter()
    .any(|needle| m.contains(needle))
}

pub fn map_err(err: Error) -> SourceError {
    match err {
        Error::Server(e) => {
            // Server messages never contain the password; they can name the
            // user and host, which is fine to show.
            let msg = format!("mysql error {} ({}): {}", e.code, e.state, e.message);
            if e.code == ER_BINLOG_FATAL {
                if is_position_lost_message(&e.message) {
                    SourceError::PositionLost(msg)
                } else {
                    // e.g. another replica connected with our server id.
                    SourceError::Transient(msg)
                }
            } else if CONFIG_CODES.contains(&e.code) {
                SourceError::Config(msg)
            } else {
                // Shutdown (1053), too many connections (1040), lock waits,
                // deadlocks, read-only failover… all retryable.
                SourceError::Transient(msg)
            }
        }
        Error::Io(IoError::Io(e)) => SourceError::Transient(format!("mysql I/O error: {e}")),
        Error::Io(IoError::Tls(e)) => SourceError::Config(format!("mysql TLS error: {e}")),
        Error::Url(e) => SourceError::Config(format!("mysql connection options: {e}")),
        Error::Driver(e) => map_driver(e),
        Error::Other(e) => SourceError::Transient(format!("mysql error: {e}")),
    }
}

fn map_driver(e: DriverError) -> SourceError {
    match e {
        DriverError::CleartextPluginDisabled
        | DriverError::UnknownAuthPlugin { .. }
        | DriverError::NoClientSslFlagFromServer
        | DriverError::MysqlOldPasswordDisabled => SourceError::Config(format!("mysql: {e}")),
        DriverError::FromValue { .. } | DriverError::FromRow { .. } => {
            SourceError::Fatal(format!("mysql: unexpected value shape: {e}"))
        }
        other => SourceError::Transient(format!("mysql: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::ServerError;

    fn server(code: u16, message: &str) -> Error {
        Error::Server(ServerError {
            code,
            message: message.into(),
            state: "HY000".into(),
        })
    }

    #[test]
    fn server_errors_map_by_reaction() {
        assert!(matches!(
            map_err(server(1045, "Access denied for user 'u'@'h'")),
            SourceError::Config(_)
        ));
        assert!(matches!(
            map_err(server(1227, "need REPLICATION SLAVE")),
            SourceError::Config(_)
        ));
        assert!(matches!(
            map_err(server(1053, "Server shutdown in progress")),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            map_err(server(
                1236,
                "Cannot replicate because the source purged required binary logs."
            )),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            map_err(server(
                1236,
                "Could not find first log file name in binary log index file"
            )),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            map_err(server(
                1236,
                "A replica with the same server_uuid/server_id as this replica has connected"
            )),
            SourceError::Transient(_)
        ));
    }

    #[test]
    fn io_errors_are_transient() {
        let e = Error::Io(IoError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )));
        assert!(matches!(map_err(e), SourceError::Transient(_)));
    }
}
