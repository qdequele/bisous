//! Driver and gRPC errors → [`SourceError`], by the reaction the engine
//! must have. Messages never carry the password: MySQL and Vitess name the
//! user and host at most.

use meili_connect_core::SourceError;
use mysql_async::{DriverError, Error, IoError};
use tonic::{Code, Status};

/// Access denied, unknown database or table, missing privilege, bad syntax.
const CONFIG_CODES: &[u16] = &[1044, 1045, 1049, 1064, 1142, 1143, 1146, 1227, 1370];

pub fn mysql(err: Error) -> SourceError {
    match err {
        Error::Server(e) => {
            let msg = format!("mysql error {} ({}): {}", e.code, e.state, e.message);
            if CONFIG_CODES.contains(&e.code) {
                SourceError::Config(msg)
            } else {
                SourceError::Transient(msg)
            }
        }
        Error::Io(IoError::Io(e)) => SourceError::Transient(format!("mysql I/O error: {e}")),
        Error::Io(IoError::Tls(e)) => SourceError::Config(format!("mysql TLS error: {e}")),
        Error::Url(e) => SourceError::Config(format!("mysql connection options: {e}")),
        Error::Driver(e) => match e {
            DriverError::CleartextPluginDisabled
            | DriverError::UnknownAuthPlugin { .. }
            | DriverError::NoClientSslFlagFromServer => SourceError::Config(format!("mysql: {e}")),
            DriverError::FromValue { .. } | DriverError::FromRow { .. } => {
                SourceError::Fatal(format!("mysql: unexpected value shape: {e}"))
            }
            other => SourceError::Transient(format!("mysql: {other}")),
        },
        Error::Other(e) => SourceError::Transient(format!("mysql error: {e}")),
    }
}

/// What Vitess says when a shard's position is older than its binlogs.
fn is_position_lost(message: &str) -> bool {
    let m = message.to_ascii_lowercase();
    m.contains("purged")
        || m.contains("could not find gtid")
        || m.contains("cannot replicate because the source purged")
        || m.contains("position is not reachable")
}

pub fn grpc(status: &Status) -> SourceError {
    let msg = format!("VStream {:?}: {}", status.code(), status.message());
    if is_position_lost(status.message()) {
        return SourceError::PositionLost(msg);
    }
    match status.code() {
        Code::Unauthenticated | Code::PermissionDenied => SourceError::Config(format!(
            "{msg}: check the user and password (a PlanetScale password, pscale_pw_…)"
        )),
        Code::InvalidArgument | Code::NotFound | Code::FailedPrecondition => {
            SourceError::Config(msg)
        }
        Code::Unimplemented => SourceError::Config(format!(
            "{msg}: this host does not serve VStream; use the database's host name"
        )),
        _ => SourceError::Transient(msg),
    }
}

pub fn transport(err: &tonic::transport::Error) -> SourceError {
    SourceError::Transient(format!("VStream connection failed: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grpc_errors_map_by_reaction() {
        assert!(matches!(
            grpc(&Status::unauthenticated("bad credentials")),
            SourceError::Config(_)
        ));
        assert!(matches!(
            grpc(&Status::unavailable("tablet restarting")),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            grpc(&Status::unknown(
                "Cannot replicate because the source purged required binary logs"
            )),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            grpc(&Status::invalid_argument("unknown table t")),
            SourceError::Config(_)
        ));
    }
}
