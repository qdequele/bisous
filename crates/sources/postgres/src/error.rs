//! Postgres errors → [`SourceError`], by the reaction the engine must have.
//!
//! Neither driver puts the password or the connection string in its error
//! text, and we never format our own config into an error.

use meili_connect_core::SourceError;
use pgwire_replication::PgWireError;

/// Where the error happened changes the meaning of some SQLSTATEs: a
/// missing object while streaming is our slot or publication gone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Context {
    /// Connecting, checking, discovering, snapshotting, setup.
    Query,
    /// START_REPLICATION and the replication stream.
    Stream,
}

/// Map a SQLSTATE + message.
pub fn from_sqlstate(code: &str, message: &str, ctx: Context, supabase: bool) -> SourceError {
    let lower = message.to_ascii_lowercase();
    let text = format!("{message} (SQLSTATE {code})");

    if lower.contains("invalidated") || lower.contains("has already been removed") {
        return SourceError::PositionLost(text);
    }
    if supabase && is_paused_message(&lower) {
        return SourceError::Paused(text);
    }
    match code {
        // Slot or publication does not exist (while streaming).
        "42704" if ctx == Context::Stream => SourceError::PositionLost(text),
        // Requested WAL segment removed.
        "58P01" => SourceError::PositionLost(text),
        // Auth, privileges, missing database/table, wal_level & co.
        "28000" | "28P01" | "42501" | "3D000" | "42P01" | "42704" | "55000" => {
            SourceError::Config(text)
        }
        // "all replication slots are in use", max_wal_senders exceeded.
        "53400" => SourceError::Config(text),
        // Slot active elsewhere, serialization, lock, too many connections.
        "55006" | "55P03" | "40001" | "40P01" | "53300" => SourceError::Transient(text),
        // Slot or publication already exists: another instance raced us to
        // create it. The next open sorts out whose it is.
        "42710" => SourceError::Transient(text),
        // Admin shutdown, crash shutdown, cannot connect now (starting up,
        // recovering), idle session timeout.
        c if c.starts_with("57") => SourceError::Transient(text),
        // Connection exceptions, insufficient resources, system errors.
        c if c.starts_with("08") || c.starts_with("53") || c.starts_with("58") => {
            SourceError::Transient(text)
        }
        // Syntax and data errors are our bugs or unsupported shapes.
        c if c.starts_with("42") || c.starts_with("22") => SourceError::Fatal(text),
        _ => SourceError::Transient(text),
    }
}

fn is_paused_message(lower: &str) -> bool {
    lower.contains("tenant or user not found")
        || lower.contains("project is paused")
        || lower.contains("project paused")
}

/// `tokio-postgres` errors.
pub fn from_pg(err: &tokio_postgres::Error, ctx: Context, supabase: bool) -> SourceError {
    if let Some(db) = err.as_db_error() {
        return from_sqlstate(db.code().code(), db.message(), ctx, supabase);
    }
    // `tokio-postgres` displays "error connecting to server" and keeps the
    // cause (DNS, refused, TLS) in the source chain.
    let mut msg = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(cause) = source {
        msg.push_str(": ");
        msg.push_str(&cause.to_string());
        source = cause.source();
    }
    io_like(&msg, supabase)
}

/// What a Supabase host that does not resolve looks like (see `io_like`).
const UNRESOLVED: &str = "the Supabase project does not answer (paused or deleted?)";

/// Before a sync has ever run (`check`, an `open` without a resume
/// checkpoint), a Supabase host that does not resolve is a mistyped or
/// deleted project: the user must fix it, so it is a Config error and not
/// a paused project retried quietly forever.
pub fn first_contact(err: SourceError) -> SourceError {
    match err {
        SourceError::Paused(msg) if msg.starts_with(UNRESOLVED) => SourceError::Config(format!(
            "the Supabase host does not resolve: check the project reference (or the project \
             was deleted): {msg}"
        )),
        other => other,
    }
}

/// `pgwire-replication` errors. Server errors are formatted
/// `message (SQLSTATE xxxxx)`.
pub fn from_wire(err: &PgWireError, ctx: Context, supabase: bool) -> SourceError {
    match err {
        PgWireError::Server(text) => match split_sqlstate(text) {
            Some((message, code)) => from_sqlstate(code, message, ctx, supabase),
            None => SourceError::Transient(format!("replication: {text}")),
        },
        PgWireError::Auth(text) => SourceError::Config(format!("authentication failed: {text}")),
        PgWireError::Tls(text) => SourceError::Config(format!("TLS: {text}")),
        PgWireError::Io(e) => io_like(&e.to_string(), supabase),
        PgWireError::Task(text) => SourceError::Transient(format!("replication: {text}")),
        PgWireError::MessageTooLarge { .. } => SourceError::Fatal(format!("replication: {err}")),
        PgWireError::Protocol(text) => SourceError::Fatal(format!("replication protocol: {text}")),
        _ => SourceError::Transient(format!("replication: {err}")),
    }
}

fn split_sqlstate(text: &str) -> Option<(&str, &str)> {
    let start = text.rfind("(SQLSTATE ")?;
    let code = text[start + 10..].strip_suffix(')')?;
    Some((text[..start].trim_end(), code))
}

/// Network-level failure. On Supabase, a name that no longer resolves is
/// what a paused project looks like from outside.
fn io_like(msg: &str, supabase: bool) -> SourceError {
    let lower = msg.to_ascii_lowercase();
    if supabase
        && (lower.contains("failed to lookup address")
            || lower.contains("nodename nor servname")
            || lower.contains("name or service not known")
            || lower.contains("no such host")
            || is_paused_message(&lower))
    {
        return SourceError::Paused(format!("{UNRESOLVED}: {msg}"));
    }
    if lower.contains("password authentication failed") || lower.contains("certificate") {
        return SourceError::Config(msg.to_owned());
    }
    SourceError::Transient(msg.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn sqlstates() {
        let q = Context::Query;
        assert!(matches!(
            from_sqlstate("28P01", "password authentication failed", q, false),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_sqlstate("42501", "permission denied", q, false),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_sqlstate(
                "55000",
                "logical decoding requires wal_level >= logical",
                q,
                false
            ),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_sqlstate(
                "55000",
                "can no longer get changes from replication slot \"s\": This slot has been invalidated because it exceeded the maximum reserved size.",
                Context::Stream,
                false
            ),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            from_sqlstate(
                "42704",
                "replication slot \"s\" does not exist",
                Context::Stream,
                false
            ),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            from_sqlstate("42704", "type does not exist", q, false),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_sqlstate(
                "55006",
                "replication slot \"s\" is active for PID 1",
                Context::Stream,
                false
            ),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            from_sqlstate(
                "57P01",
                "terminating connection due to administrator command",
                q,
                false
            ),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            from_sqlstate("53400", "all replication slots are in use", q, false),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_sqlstate("XX000", "Tenant or user not found", q, true),
            SourceError::Paused(_)
        ));
        assert!(matches!(
            from_sqlstate("XX000", "Tenant or user not found", q, false),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            from_sqlstate("42601", "syntax error", q, false),
            SourceError::Fatal(_)
        ));
        // Two instances racing to create the same slot or publication.
        assert!(matches!(
            from_sqlstate("42710", "replication slot \"s\" already exists", q, false),
            SourceError::Transient(_)
        ));
    }

    #[test]
    fn wire_errors() {
        let s = Context::Stream;
        let e =
            PgWireError::Server("replication slot \"x\" does not exist (SQLSTATE 42704)".into());
        assert!(matches!(
            from_wire(&e, s, false),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            from_wire(&PgWireError::Auth("bad".into()), s, false),
            SourceError::Config(_)
        ));
        assert!(matches!(
            from_wire(&PgWireError::Tls("bad".into()), s, false),
            SourceError::Config(_)
        ));
        let io = PgWireError::Io(Arc::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )));
        assert!(matches!(
            from_wire(&io, s, false),
            SourceError::Transient(_)
        ));
        let dns = PgWireError::Io(Arc::new(std::io::Error::other(
            "failed to lookup address information",
        )));
        assert!(matches!(from_wire(&dns, s, true), SourceError::Paused(_)));
        assert!(matches!(
            first_contact(from_wire(&dns, s, true)),
            SourceError::Config(_)
        ));
        // An explicit "paused" answer stays paused, even on first contact.
        assert!(matches!(
            first_contact(from_sqlstate("XX000", "project is paused", s, true)),
            SourceError::Paused(_)
        ));
        assert!(matches!(
            from_wire(&dns, s, false),
            SourceError::Transient(_)
        ));
    }

    #[test]
    fn sqlstate_split() {
        assert_eq!(
            split_sqlstate("boom (SQLSTATE 42P01)"),
            Some(("boom", "42P01"))
        );
        assert_eq!(split_sqlstate("boom"), None);
    }
}
