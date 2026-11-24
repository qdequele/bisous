//! Stable reason codes (the Lab contract's `reason`) and the coarse,
//! Lab-safe description that goes with each.
//!
//! One mapping, used both ways:
//!
//! - [`classify_source`] and [`classify_sink`] read a detailed error (the
//!   connector's text, never shown to a Lab tenant) and pick its
//!   [`Reason`]; the coarse layer (`crate::guard`) then replaces the text
//!   with that reason's fixed description ([`Reason::coarse_source`],
//!   [`Reason::coarse_sink`]).
//! - [`for_state`] reads the message persisted with a sync's phase and
//!   finds the reason again: the fixed descriptions are ours, so a tenant's
//!   message names its reason exactly; the engine's own messages (credits,
//!   resync budget, delete guard, invalid spec) have fixed markers. A
//!   message we do not recognise falls back to its error class, then to
//!   [`Reason::Internal`].
//!
//! The codes are an open set on the Lab side (`$defs/reason` of
//! `lab-events.schema.json`): never rename one, add new ones only after the
//! Lab knows them.

use meili_connect_core::{SinkError, SourceError, SyncPhase};

use crate::credits::OUT_OF_CREDITS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    /// The team has no credits (`out_of_credits`).
    InsufficientCredits,
    /// The source refused the credentials.
    SourceAuth,
    /// The source host does not resolve, refuses or does not answer.
    SourceUnreachable,
    /// Publication, slot, privilege, schema or prerequisite problem.
    SourceConfig,
    /// The position was lost again after the allowed resyncs in a row.
    ResyncBudgetSpent,
    /// Meilisearch refused the API key.
    DestinationAuth,
    /// Meilisearch refused the documents or the index.
    DestinationRejected,
    /// Meilisearch does not answer.
    DestinationUnreachable,
    /// A truncate or a mass delete was held back.
    DeleteGuard,
    /// The sync spec cannot run as written.
    InvalidConfig,
    Internal,
}

impl Reason {
    pub const ALL: [Reason; 11] = [
        Reason::InsufficientCredits,
        Reason::SourceAuth,
        Reason::SourceUnreachable,
        Reason::SourceConfig,
        Reason::ResyncBudgetSpent,
        Reason::DestinationAuth,
        Reason::DestinationRejected,
        Reason::DestinationUnreachable,
        Reason::DeleteGuard,
        Reason::InvalidConfig,
        Reason::Internal,
    ];

    /// The code of the Lab contract.
    pub fn code(self) -> &'static str {
        match self {
            Reason::InsufficientCredits => "insufficient_credits",
            Reason::SourceAuth => "source_auth",
            Reason::SourceUnreachable => "source_unreachable",
            Reason::SourceConfig => "source_config",
            Reason::ResyncBudgetSpent => "resync_budget_spent",
            Reason::DestinationAuth => "destination_auth",
            Reason::DestinationRejected => "destination_rejected",
            Reason::DestinationUnreachable => "destination_unreachable",
            Reason::DeleteGuard => "delete_guard",
            Reason::InvalidConfig => "invalid_config",
            Reason::Internal => "internal",
        }
    }

    /// The fixed description a Lab tenant sees for a source error of this
    /// reason (`class` decides the wording of the classes that share one).
    fn coarse_source(self, class: &SourceError) -> &'static str {
        match (class, self) {
            (SourceError::PositionLost(_), _) => SOURCE_POSITION_LOST,
            (SourceError::Paused(_), _) => SOURCE_PAUSED,
            (SourceError::Fatal(_), _) => SOURCE_FATAL,
            (SourceError::Transient(_), _) => SOURCE_UNREACHABLE,
            (SourceError::Config(_), Reason::SourceAuth) => SOURCE_AUTH,
            (SourceError::Config(_), Reason::SourceUnreachable) => SOURCE_UNRESOLVED,
            (SourceError::Config(_), Reason::InvalidConfig) => SOURCE_INVALID,
            (SourceError::Config(_), _) => SOURCE_CONFIG,
        }
    }

    /// The fixed description a Lab tenant sees for a Meilisearch error of
    /// this reason.
    fn coarse_sink(self, class: &SinkError) -> &'static str {
        match (class, self) {
            (SinkError::Transient(_), _) => SINK_UNREACHABLE,
            (SinkError::Fatal(_), _) => SINK_FATAL,
            (SinkError::Config(_), Reason::DestinationAuth) => SINK_AUTH,
            (SinkError::Config(_), Reason::DestinationUnreachable) => SINK_UNRESOLVED,
            (SinkError::Config(_), _) => SINK_CONFIG,
        }
    }
}

// ------------------------------------------------------------ coarse texts
//
// Lab-safe: no host, no driver text, nothing a remote server sent. Each
// names exactly one reason (see `COARSE`): keep them distinct.

const SOURCE_UNREACHABLE: &str =
    "the source could not be reached or did not answer in time; retrying";
const SOURCE_UNRESOLVED: &str =
    "the source host could not be found: check the address (or the project reference)";
const SOURCE_POSITION_LOST: &str = "the source no longer holds the saved position";
const SOURCE_AUTH: &str =
    "the source refused the credentials: check the user and the password or key";
const SOURCE_CONFIG: &str = "the source refused the configuration (privileges, publication, \
     replication slot, tables or prerequisites); run the source check for details";
const SOURCE_INVALID: &str =
    "the source configuration is invalid; run the source check for details";
const SOURCE_PAUSED: &str = "the source is paused";
const SOURCE_FATAL: &str = "the source returned an unexpected error";

const SINK_UNREACHABLE: &str = "Meilisearch could not be reached or did not answer in time";
const SINK_UNRESOLVED: &str = "the Meilisearch host could not be found: check the URL";
const SINK_AUTH: &str = "Meilisearch refused the API key: check the key and its actions";
const SINK_CONFIG: &str = "Meilisearch refused the request: check the index and its primary key";
const SINK_FATAL: &str = "Meilisearch rejected the request";

/// Every fixed description with its reason.
const COARSE: [(&str, Reason); 13] = [
    (SOURCE_UNREACHABLE, Reason::SourceUnreachable),
    (SOURCE_UNRESOLVED, Reason::SourceUnreachable),
    (SOURCE_POSITION_LOST, Reason::SourceConfig),
    (SOURCE_AUTH, Reason::SourceAuth),
    (SOURCE_CONFIG, Reason::SourceConfig),
    (SOURCE_INVALID, Reason::InvalidConfig),
    (SOURCE_PAUSED, Reason::SourceUnreachable),
    (SOURCE_FATAL, Reason::Internal),
    (SINK_UNREACHABLE, Reason::DestinationUnreachable),
    (SINK_UNRESOLVED, Reason::DestinationUnreachable),
    (SINK_AUTH, Reason::DestinationAuth),
    (SINK_CONFIG, Reason::DestinationRejected),
    (SINK_FATAL, Reason::DestinationRejected),
];

// ------------------------------------------------------------ engine markers

/// The start of the hold message when the resync budget is spent
/// (`meili_connect_engine::sync`).
const RESYNC_BUDGET_MARKER: &str = "the source position was lost again after";
/// The hold messages of the delete guards (truncate, mass delete).
const DELETE_GUARD_MARKERS: [&str; 2] = [
    "was truncated: applying it would delete",
    "a batch would delete",
];
/// `EngineError::Spec`.
const INVALID_SPEC_PREFIX: &str = "invalid sync spec:";

// ------------------------------------------------------------ classification

fn has_any(lower: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| lower.contains(n))
}

/// A name that does not resolve.
const DNS: [&str; 6] = [
    "failed to lookup address",
    "nodename nor servname",
    "name or service not known",
    "no such host",
    "does not resolve",
    "dns error",
];

/// Credentials refused, across the connectors' wordings.
const AUTH: [&str; 11] = [
    "authentication failed",
    "password authentication",
    "sqlstate 28",
    "access denied for user",
    "invalid credentials",
    "invalid username or password",
    "unrecognizedclientexception",
    "invalidclienttokenid",
    "signaturedoesnotmatch",
    "security token included in the request is invalid",
    "sasl authentication",
];

/// Meilisearch's error codes for a key it refuses (missing, unknown,
/// expired, or without the action or the index). Codes only: other
/// messages talk about API keys too (an embedder's, when copying settings),
/// and the Lab answers this reason by replacing the sync's key.
const KEY_REFUSED: [&str; 3] = [
    "invalid_api_key",
    "missing_authorization_header",
    "invalid_tenant_token",
];

/// The reason of a detailed source error.
pub fn classify_source(e: &SourceError) -> Reason {
    match e {
        SourceError::Transient(_) | SourceError::Paused(_) => Reason::SourceUnreachable,
        SourceError::PositionLost(_) => Reason::SourceConfig,
        SourceError::Fatal(_) => Reason::Internal,
        SourceError::Config(text) => {
            let lower = text.to_ascii_lowercase();
            if let Some(reason) = coarse_reason(text) {
                reason
            } else if has_any(&lower, &AUTH) {
                Reason::SourceAuth
            } else if has_any(&lower, &DNS) {
                Reason::SourceUnreachable
            } else if lower.contains("invalid") && lower.contains("config") {
                Reason::InvalidConfig
            } else {
                Reason::SourceConfig
            }
        }
    }
}

/// The reason of a detailed Meilisearch error.
pub fn classify_sink(e: &SinkError) -> Reason {
    match e {
        SinkError::Transient(_) => Reason::DestinationUnreachable,
        SinkError::Fatal(_) => Reason::DestinationRejected,
        SinkError::Config(text) => {
            let lower = text.to_ascii_lowercase();
            if let Some(reason) = coarse_reason(text) {
                reason
            } else if has_any(&lower, &KEY_REFUSED) {
                Reason::DestinationAuth
            } else if has_any(&lower, &DNS) {
                Reason::DestinationUnreachable
            } else {
                Reason::DestinationRejected
            }
        }
    }
}

/// A source error reduced to its class and its reason's fixed text.
pub fn coarse_source(e: &SourceError) -> SourceError {
    let text = classify_source(e).coarse_source(e).to_owned();
    match e {
        SourceError::Transient(_) => SourceError::Transient(text),
        SourceError::PositionLost(_) => SourceError::PositionLost(text),
        SourceError::Config(_) => SourceError::Config(text),
        SourceError::Paused(_) => SourceError::Paused(text),
        SourceError::Fatal(_) => SourceError::Fatal(text),
    }
}

/// A Meilisearch error reduced to its class and its reason's fixed text.
pub fn coarse_sink(e: &SinkError) -> SinkError {
    let text = classify_sink(e).coarse_sink(e).to_owned();
    match e {
        SinkError::Transient(_) => SinkError::Transient(text),
        SinkError::Config(_) => SinkError::Config(text),
        SinkError::Fatal(_) => SinkError::Fatal(text),
    }
}

/// The reason of one of our fixed descriptions inside `message`.
fn coarse_reason(message: &str) -> Option<Reason> {
    COARSE
        .iter()
        .find(|(text, _)| message.contains(text))
        .map(|(_, reason)| *reason)
}

/// The reason a sync is in `phase`, from the message persisted with it:
/// only a phase that stopped the sync has one (`paused`, `source_paused`,
/// `held`, `failed`), and only with a message. What `sync.state_changed`
/// and `GET /syncs/{id}/status` report as `reason`.
pub fn for_phase(phase: SyncPhase, message: Option<&str>) -> Option<Reason> {
    let stopped = !matches!(
        phase,
        SyncPhase::Pending | SyncPhase::Snapshotting | SyncPhase::Streaming
    );
    let message = message.filter(|m| !m.is_empty())?;
    stopped.then(|| for_state(message))
}

/// The reason of the message persisted with a sync's phase (what
/// `GET /syncs/{id}/status` shows as `message`).
pub fn for_state(message: &str) -> Reason {
    if message == OUT_OF_CREDITS {
        return Reason::InsufficientCredits;
    }
    if message.contains(RESYNC_BUDGET_MARKER) {
        return Reason::ResyncBudgetSpent;
    }
    if DELETE_GUARD_MARKERS.iter().any(|m| message.contains(m)) {
        return Reason::DeleteGuard;
    }
    if let Some(reason) = coarse_reason(message) {
        return reason;
    }
    let message = message.strip_prefix("sync held: ").unwrap_or(message);
    if message.starts_with(INVALID_SPEC_PREFIX) {
        return Reason::InvalidConfig;
    }
    // A message we did not coarsen (a host-policy refusal, an operator's
    // sync): its class, from the engine's error display.
    let rest = |prefix: &str| message.strip_prefix(prefix).map(|r| r.trim().to_owned());
    if let Some(r) = rest("source configuration error:") {
        return classify_source(&SourceError::Config(r));
    }
    if let Some(r) = rest("transient source error:").or_else(|| rest("source paused:")) {
        return classify_source(&SourceError::Transient(r));
    }
    if let Some(r) = rest("source position lost:") {
        return classify_source(&SourceError::PositionLost(r));
    }
    if let Some(r) = rest("Meilisearch configuration error:") {
        return classify_sink(&SinkError::Config(r));
    }
    if rest("transient Meilisearch error:").is_some() {
        return Reason::DestinationUnreachable;
    }
    if rest("fatal Meilisearch error:").is_some() {
        return Reason::DestinationRejected;
    }
    Reason::Internal
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_connect_engine::EngineError;

    /// What the supervisor persists for a coarse error (its display).
    fn persisted(e: impl Into<EngineError>) -> String {
        e.into().to_string()
    }

    #[test]
    fn every_code_is_a_valid_contract_code_and_distinct() {
        let mut seen = std::collections::HashSet::new();
        for r in Reason::ALL {
            let code = r.code();
            assert!(seen.insert(code), "{code} twice");
            assert!(code.len() <= 64);
            assert!(code.starts_with(|c: char| c.is_ascii_lowercase()));
            assert!(
                code.chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            );
        }
    }

    #[test]
    fn coarse_texts_are_distinct() {
        for (i, (a, _)) in COARSE.iter().enumerate() {
            for (b, _) in &COARSE[i + 1..] {
                assert!(!a.contains(b) && !b.contains(a), "{a:?} / {b:?}");
            }
        }
    }

    #[test]
    fn source_errors_map_to_their_reason_and_back() {
        let cases: Vec<(SourceError, Reason)> = vec![
            (
                SourceError::Config(
                    "password authentication failed for user \"app\" (SQLSTATE 28P01)".into(),
                ),
                Reason::SourceAuth,
            ),
            (
                SourceError::Config("authentication failed: bad".into()),
                Reason::SourceAuth,
            ),
            (
                SourceError::Config("Access denied for user 'app'@'10.0.0.5'".into()),
                Reason::SourceAuth,
            ),
            (
                SourceError::Config(
                    "the Supabase host does not resolve: check the project reference".into(),
                ),
                Reason::SourceUnreachable,
            ),
            (
                SourceError::Transient("connect 10.0.0.5:5432: connection refused".into()),
                Reason::SourceUnreachable,
            ),
            (
                SourceError::Paused("project is paused".into()),
                Reason::SourceUnreachable,
            ),
            (
                SourceError::Config("publication \"p\" does not exist (SQLSTATE 42704)".into()),
                Reason::SourceConfig,
            ),
            (
                SourceError::Config("permission denied for table t (SQLSTATE 42501)".into()),
                Reason::SourceConfig,
            ),
            (
                SourceError::Config("all replication slots are in use (SQLSTATE 53400)".into()),
                Reason::SourceConfig,
            ),
            (
                SourceError::Config("invalid dynamodb source config: missing field".into()),
                Reason::InvalidConfig,
            ),
            (
                SourceError::PositionLost("slot invalidated".into()),
                Reason::SourceConfig,
            ),
            (SourceError::Fatal("syntax error".into()), Reason::Internal),
        ];
        for (e, reason) in cases {
            assert_eq!(classify_source(&e), reason, "{e}");
            let coarse = coarse_source(&e);
            assert_eq!(
                std::mem::discriminant(&coarse),
                std::mem::discriminant(&e),
                "the class is kept"
            );
            assert!(!coarse.to_string().contains("10.0.0.5"));
            assert!(!coarse.to_string().contains("SQLSTATE"));
            // What the supervisor persists names the reason again.
            assert_eq!(for_state(&persisted(coarse)), reason, "{e}");
        }
    }

    #[test]
    fn sink_errors_map_to_their_reason_and_back() {
        let cases: Vec<(SinkError, Reason)> = vec![
            (
                SinkError::Config(
                    "add documents: The provided API key is invalid. (invalid_api_key)".into(),
                ),
                Reason::DestinationAuth,
            ),
            (
                SinkError::Config("HTTP 403: missing_authorization_header".into()),
                Reason::DestinationAuth,
            ),
            (
                SinkError::Config("index_primary_key_already_exists".into()),
                Reason::DestinationRejected,
            ),
            (
                // Not the sync's key: a new one would change nothing.
                SinkError::Config(
                    "cannot copy the settings of index `a` to `b`: embedder `e` has an API \
                     key, which Meilisearch only returns redacted; copying it would install a \
                     broken key"
                        .into(),
                ),
                Reason::DestinationRejected,
            ),
            (
                SinkError::Config(
                    "HTTP 403 Forbidden: the reply is not a Meilisearch error".into(),
                ),
                Reason::DestinationRejected,
            ),
            (
                SinkError::Config("dns error: failed to lookup address information".into()),
                Reason::DestinationUnreachable,
            ),
            (
                SinkError::Transient("HTTP 503".into()),
                Reason::DestinationUnreachable,
            ),
            (
                SinkError::Fatal("payload_too_large".into()),
                Reason::DestinationRejected,
            ),
        ];
        for (e, reason) in cases {
            assert_eq!(classify_sink(&e), reason, "{e}");
            let coarse = coarse_sink(&e);
            assert_eq!(for_state(&persisted(coarse)), reason, "{e}");
        }
    }

    #[test]
    fn only_a_stopped_phase_with_a_message_has_a_reason() {
        let refused = persisted(coarse_sink(&SinkError::Config(
            "add documents: The provided API key is invalid. (invalid_api_key)".into(),
        )));
        for phase in [
            SyncPhase::Paused,
            SyncPhase::SourcePaused,
            SyncPhase::Held,
            SyncPhase::Failed,
        ] {
            assert_eq!(
                for_phase(phase, Some(&refused)),
                Some(Reason::DestinationAuth),
                "{phase:?}"
            );
            assert_eq!(for_phase(phase, None), None, "{phase:?}");
            assert_eq!(for_phase(phase, Some("")), None, "{phase:?}");
        }
        // A sync that runs again may still carry its last message.
        for phase in [
            SyncPhase::Pending,
            SyncPhase::Snapshotting,
            SyncPhase::Streaming,
        ] {
            assert_eq!(for_phase(phase, Some(&refused)), None, "{phase:?}");
        }
    }

    #[test]
    fn engine_messages_have_their_reason() {
        assert_eq!(for_state(OUT_OF_CREDITS), Reason::InsufficientCredits);
        assert_eq!(
            for_state(
                "sync held: the source position was lost again after 3 resyncs in a row (x): …"
            ),
            Reason::ResyncBudgetSpent
        );
        assert_eq!(
            for_state(
                "sync held: `public.items` was truncated: applying it would delete all 5 \
                 documents of index `items`"
            ),
            Reason::DeleteGuard
        );
        assert_eq!(
            for_state("a batch would delete 9 of the 10 documents of index `items` (guard: 50%)"),
            Reason::DeleteGuard
        );
        assert_eq!(
            for_state(&EngineError::Spec("no streams".into()).to_string()),
            Reason::InvalidConfig
        );
        assert_eq!(
            for_state(&format!(
                "{} (failed 10 times in a row on the same batch; fix the cause, then restart \
                 the sync)",
                persisted(coarse_sink(&SinkError::Transient("x".into())))
            )),
            Reason::DestinationUnreachable
        );
        // A host-policy refusal is not coarsened: its class still says.
        assert_eq!(
            for_state("source configuration error: db.internal resolves to 10.0.0.5"),
            Reason::SourceConfig
        );
        assert_eq!(for_state("store error: boom"), Reason::Internal);
        assert_eq!(for_state("something else"), Reason::Internal);
    }
}
