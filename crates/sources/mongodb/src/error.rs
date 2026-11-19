//! Driver error → `SourceError`, chosen by the reaction the engine must
//! have, with the connection string scrubbed from every message.

use meili_connect_core::{Secret, SourceError};
use mongodb::error::{Error, ErrorKind, WriteFailure};

/// Server error codes (src/mongo/base/error_codes.yml).
pub mod codes {
    pub const UNAUTHORIZED: i32 = 13;
    pub const AUTHENTICATION_FAILED: i32 = 18;
    pub const NAMESPACE_NOT_FOUND: i32 = 26;
    pub const CURSOR_NOT_FOUND: i32 = 43;
    pub const CAPPED_POSITION_LOST: i32 = 136;
    pub const INVALID_RESUME_TOKEN: i32 = 260;
    pub const CHANGE_STREAM_FATAL: i32 = 280;
    pub const CHANGE_STREAM_HISTORY_LOST: i32 = 286;
    pub const ATLAS_ERROR: i32 = 8000;
    /// "The $changeStream stage is only supported on replica sets".
    pub const CHANGE_STREAM_NEEDS_REPLICA_SET: i32 = 40573;
    /// Codes that mean "try again": network, elections, shutdowns, timeouts.
    pub const TRANSIENT: &[i32] = &[
        6,     // HostUnreachable
        7,     // HostNotFound
        43,    // CursorNotFound
        50,    // MaxTimeMSExpired
        63,    // StaleShardVersion
        89,    // NetworkTimeout
        91,    // ShutdownInProgress
        133,   // FailedToSatisfyReadPreference
        150,   // StaleEpoch
        189,   // PrimarySteppedDown
        234,   // RetryChangeStream
        262,   // ExceededTimeLimit
        9001,  // SocketException
        10107, // NotWritablePrimary
        11600, // InterruptedAtShutdown
        11602, // InterruptedDueToReplStateChange
        13388, // StaleConfig
        13435, // NotPrimaryNoSecondaryOk
        13436, // NotPrimaryOrSecondary
    ];
}

/// The server-side code of an error, looking through wrapped sources.
pub fn error_code(err: &Error) -> Option<i32> {
    let own = match err.kind.as_ref() {
        ErrorKind::Command(c) => Some(c.code),
        ErrorKind::Write(WriteFailure::WriteConcernError(e)) => Some(e.code),
        ErrorKind::Write(WriteFailure::WriteError(e)) => Some(e.code),
        _ => None,
    };
    own.or_else(|| {
        std::error::Error::source(err)
            .and_then(|s| s.downcast_ref::<Error>())
            .and_then(error_code)
    })
}

/// Removes the connection string and its password from text that may end up
/// in logs, the UI or the store.
#[derive(Clone)]
pub struct Scrubber {
    needles: Vec<String>,
}

impl std::fmt::Debug for Scrubber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Scrubber(<redacted>)")
    }
}

impl Scrubber {
    pub fn new(uri: &Secret) -> Self {
        let uri = uri.expose();
        let mut needles = vec![uri.to_owned()];
        if let Some(password) = uri_password(uri) {
            if let Some(decoded) = percent_decode(password)
                && decoded != password
                && decoded.len() >= 3
            {
                needles.push(decoded);
            }
            if password.len() >= 3 {
                needles.push(password.to_owned());
            }
        }
        needles.retain(|n| !n.is_empty());
        // Longest first so the whole URI is replaced before its parts.
        needles.sort_by_key(|n| std::cmp::Reverse(n.len()));
        Self { needles }
    }

    pub fn scrub(&self, text: &str) -> String {
        let mut out = text.to_owned();
        for needle in &self.needles {
            if out.contains(needle.as_str()) {
                out = out.replace(needle.as_str(), "<redacted>");
            }
        }
        out
    }
}

fn uri_password(uri: &str) -> Option<&str> {
    let rest = uri.split_once("://")?.1;
    let authority = rest.split(['/', '?']).next()?;
    let userinfo = authority.rsplit_once('@')?.0;
    let password = userinfo.split_once(':')?.1;
    (!password.is_empty()).then_some(password)
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// A short, human-readable description of a driver error (the driver's own
/// `Display` dumps the whole server response).
pub fn describe(err: &Error) -> String {
    match err.kind.as_ref() {
        ErrorKind::Command(c) => format!("{} ({}): {}", c.code_name, c.code, c.message),
        other => other.to_string(),
    }
}

/// Map a driver error to the engine's taxonomy. `context` says what was
/// being done ("opening the change stream").
pub fn map_error(err: &Error, context: &str, scrub: &Scrubber) -> SourceError {
    let message = scrub.scrub(&format!("{context}: {}", describe(err)));
    let lower = message.to_ascii_lowercase();
    let code = error_code(err);

    // Atlas reports paused clusters through its proxy; only trust the word
    // when it comes from Atlas or a failed connection.
    if lower.contains("paused")
        && (code == Some(codes::ATLAS_ERROR)
            || matches!(
                err.kind.as_ref(),
                ErrorKind::ServerSelection { .. } | ErrorKind::Io(_)
            ))
    {
        return SourceError::Paused(message);
    }

    if let Some(code) = code {
        match code {
            codes::CHANGE_STREAM_HISTORY_LOST
            | codes::INVALID_RESUME_TOKEN
            | codes::CAPPED_POSITION_LOST => return SourceError::PositionLost(message),
            // 280 is also "resume token not found" on older servers.
            codes::CHANGE_STREAM_FATAL => return SourceError::PositionLost(message),
            codes::UNAUTHORIZED | codes::AUTHENTICATION_FAILED | codes::ATLAS_ERROR => {
                return SourceError::Config(message);
            }
            codes::CHANGE_STREAM_NEEDS_REPLICA_SET => {
                return SourceError::Config(format!(
                    "{message}. Change streams need a replica set or a sharded cluster"
                ));
            }
            codes::NAMESPACE_NOT_FOUND => return SourceError::Config(message),
            c if codes::TRANSIENT.contains(&c) => return SourceError::Transient(message),
            _ => {}
        }
    }

    match err.kind.as_ref() {
        ErrorKind::Authentication { .. } => SourceError::Config(message),
        ErrorKind::InvalidArgument { .. }
        | ErrorKind::InvalidTlsConfig { .. }
        | ErrorKind::IncompatibleServer { .. }
        | ErrorKind::SessionsNotSupported => SourceError::Config(message),
        ErrorKind::Io(_)
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::ConnectionPoolCleared { .. }
        | ErrorKind::DnsResolve { .. }
        | ErrorKind::Shutdown => SourceError::Transient(message),
        _ if err.contains_label("ResumableChangeStreamError")
            || err.contains_label("RetryableWriteError")
            || err.contains_label("TransientTransactionError")
            || err.contains_label("NetworkError") =>
        {
            SourceError::Transient(message)
        }
        ErrorKind::Command(_) => SourceError::Fatal(message),
        _ => SourceError::Fatal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_error(code: i32, name: &str, msg: &str) -> Error {
        // CommandError is non_exhaustive; build it through serde like the
        // driver does from a server reply.
        let c: mongodb::error::CommandError = mongodb::bson::from_document(
            mongodb::bson::doc! {"code": code, "codeName": name, "errmsg": msg},
        )
        .unwrap();
        Error::from(ErrorKind::Command(c))
    }

    fn scrubber() -> Scrubber {
        Scrubber::new(&Secret::new(
            "mongodb+srv://app:s3cr%40t@cluster0.x.mongodb.net/db",
        ))
    }

    #[test]
    fn scrubs_uri_and_password() {
        let s = scrubber();
        let text =
            "failed mongodb+srv://app:s3cr%40t@cluster0.x.mongodb.net/db and s3cr@t and s3cr%40t";
        let out = s.scrub(text);
        assert!(!out.contains("s3cr"), "{out}");
        assert!(out.contains("<redacted>"));
        assert_eq!(uri_password("mongodb://h:27017"), None);
        assert_eq!(uri_password("mongodb://u@h"), None);
        assert_eq!(uri_password("mongodb://u:p@w@h/x?y"), Some("p@w"));
        assert!(!format!("{s:?}").contains("s3cr"));
    }

    #[test]
    fn classification() {
        let s = scrubber();
        let lost = map_error(
            &command_error(286, "ChangeStreamHistoryLost", "gone"),
            "x",
            &s,
        );
        assert!(matches!(lost, SourceError::PositionLost(_)));
        let lost = map_error(&command_error(260, "InvalidResumeToken", "bad"), "x", &s);
        assert!(matches!(lost, SourceError::PositionLost(_)));
        let auth = map_error(&command_error(13, "Unauthorized", "no"), "x", &s);
        assert!(matches!(auth, SourceError::Config(_)));
        let auth = map_error(&command_error(18, "AuthenticationFailed", "no"), "x", &s);
        assert!(matches!(auth, SourceError::Config(_)));
        let rs = map_error(&command_error(40573, "Location40573", "rs only"), "x", &s);
        assert!(matches!(rs, SourceError::Config(_)));
        let np = map_error(&command_error(10107, "NotWritablePrimary", "np"), "x", &s);
        assert!(matches!(np, SourceError::Transient(_)));
        let el = map_error(
            &command_error(11602, "InterruptedDueToReplStateChange", ""),
            "x",
            &s,
        );
        assert!(matches!(el, SourceError::Transient(_)));
        let paused = map_error(
            &command_error(8000, "AtlasError", "cluster is paused"),
            "x",
            &s,
        );
        assert!(matches!(paused, SourceError::Paused(_)));
        let other = map_error(&command_error(2, "BadValue", "nope"), "x", &s);
        assert!(matches!(other, SourceError::Fatal(_)));

        let io = Error::from(ErrorKind::Io(std::sync::Arc::new(std::io::Error::other(
            "reset",
        ))));
        assert!(matches!(map_error(&io, "x", &s), SourceError::Transient(_)));
    }

    #[tokio::test]
    async fn invalid_uri_is_config_and_scrubbed() {
        let uri = Secret::new("mongodb://app:s3cr3t@host:notaport/db");
        let s = Scrubber::new(&uri);
        let err = mongodb::options::ClientOptions::parse(uri.expose())
            .await
            .unwrap_err();
        let mapped = map_error(&err, "parsing", &s);
        assert!(matches!(mapped, SourceError::Config(_)), "{mapped}");
        assert!(!mapped.to_string().contains("s3cr3t"), "{mapped}");
    }
}
