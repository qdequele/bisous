//! Typed connector settings, read from `SourceSpec.config`.

use meili_connect_core::{Secret, SourceError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Largest `snapshot.page_size` accepted. Each page is held in memory once
/// (documents are at most 1 MiB each).
pub const MAX_PAGE_SIZE: u32 = 1000;

/// The only token endpoint a service account key is sent to, whatever the
/// key file says: a tenant-supplied `token_uri` would make the server post
/// a signed assertion to an address of the tenant's choosing.
pub const GOOGLE_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";

/// The Firestore connector configuration.
///
/// ```json
/// {
///   "service_account": "{\"type\": \"service_account\", \"project_id\": \"my-app\", ...}",
///   "project_id": "my-app",
///   "database_id": "(default)",
///   "snapshot": { "page_size": 500 }
/// }
/// ```
///
/// `service_account` is the JSON key file of a service account, as text.
/// `project_id` defaults to the key's. `emulator_host` (`host:port`)
/// replaces Google's endpoint and the key with the emulator and its stub
/// token, for development and tests.
///
/// Collections are not configured here: they come from the sync's stream
/// mappings (see [`crate::target::CollectionRef`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirestoreConfig {
    #[serde(default)]
    pub service_account: Option<Secret>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default = "default_database")]
    pub database_id: String,
    #[serde(default)]
    pub emulator_host: Option<String>,
    #[serde(default)]
    pub snapshot: SnapshotOptions,
    #[serde(default)]
    pub stream: StreamOptions,
}

fn default_database() -> String {
    "(default)".to_owned()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SnapshotOptions {
    /// Documents per `RunQuery` page, at most [`MAX_PAGE_SIZE`].
    pub page_size: u32,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self { page_size: 300 }
    }
}

/// Default of [`StreamOptions::max_tracked_documents`].
pub const DEFAULT_MAX_TRACKED: usize = 500_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamOptions {
    /// Document paths kept in memory to apply a server reset (see
    /// [`crate::listen`]); above it a reset resyncs the sync instead. About
    /// 100 bytes each.
    pub max_tracked_documents: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            max_tracked_documents: DEFAULT_MAX_TRACKED,
        }
    }
}

/// The fields of a key file the connector keeps; anything else (the
/// `token_uri`, `universe_domain`, certificate URLs) is dropped.
const KEY_FIELDS: &[&str] = &[
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
];

impl FirestoreConfig {
    /// Parse and validate. Errors are `Config` and never echo the input
    /// (it may hold a private key).
    pub fn from_value(value: &Value) -> Result<Self, SourceError> {
        let config: FirestoreConfig = serde_json::from_value(value.clone()).map_err(|e| {
            // serde messages name fields and expected types, never values.
            SourceError::Config(format!("invalid firestore source config: {e}"))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), SourceError> {
        let bad = |m: &str| Err(SourceError::Config(m.to_owned()));
        match (&self.emulator_host, &self.service_account) {
            (None, None) => {
                return bad("`service_account` (the JSON key of a service account) is required");
            }
            (None, Some(_)) => {
                self.service_account_key()?;
            }
            (Some(host), _) => {
                if host_port(host).is_none() {
                    return bad("`emulator_host` must be host:port, without a scheme");
                }
                if self.project_id.is_none() {
                    return bad("`project_id` is required with `emulator_host`");
                }
            }
        }
        if let Some(project) = &self.project_id
            && !valid_project_id(project)
        {
            return bad(
                "`project_id` must be a Google Cloud project id (lowercase letters, digits and hyphens)",
            );
        }
        if !valid_database_id(&self.database_id) {
            return bad(
                "`database_id` must be `(default)` or a Firestore database id (lowercase letters, digits and hyphens)",
            );
        }
        if self.stream.max_tracked_documents > 10_000_000 {
            return bad("`stream.max_tracked_documents` must be at most 10000000");
        }
        if self.snapshot.page_size == 0 || self.snapshot.page_size > MAX_PAGE_SIZE {
            return Err(SourceError::Config(format!(
                "`snapshot.page_size` must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        Ok(())
    }

    /// The key to mint tokens with: the fields of [`KEY_FIELDS`] and the
    /// fixed [`GOOGLE_TOKEN_URI`].
    pub fn service_account_key(&self) -> Result<Value, SourceError> {
        let bad = |m: &str| Err(SourceError::Config(m.to_owned()));
        let Some(secret) = &self.service_account else {
            return bad("`service_account` is required");
        };
        let Ok(Value::Object(parsed)) = serde_json::from_str::<Value>(secret.expose()) else {
            return bad(
                "`service_account` must be the JSON key file of a service account (paste the whole file)",
            );
        };
        if parsed.get("type").and_then(Value::as_str) != Some("service_account") {
            return bad(
                "`service_account` is not a service account key (its `type` must be `service_account`)",
            );
        }
        for field in ["client_email", "private_key", "private_key_id"] {
            if parsed
                .get(field)
                .and_then(Value::as_str)
                .is_none_or(|s| s.trim().is_empty())
            {
                return Err(SourceError::Config(format!(
                    "the service account key has no `{field}`"
                )));
            }
        }
        let mut key = serde_json::Map::new();
        for field in KEY_FIELDS {
            if let Some(v) = parsed.get(*field) {
                key.insert((*field).to_owned(), v.clone());
            }
        }
        key.insert("token_uri".into(), Value::String(GOOGLE_TOKEN_URI.into()));
        Ok(Value::Object(key))
    }

    /// The service account's email, for messages (not a secret).
    pub fn client_email(&self) -> Option<String> {
        let secret = self.service_account.as_ref()?;
        let parsed: Value = serde_json::from_str(secret.expose()).ok()?;
        parsed.get("client_email")?.as_str().map(str::to_owned)
    }

    /// The project: `project_id`, else the key's.
    pub fn project(&self) -> Result<String, SourceError> {
        if let Some(p) = &self.project_id {
            return Ok(p.clone());
        }
        let key = self.service_account_key()?;
        match key.get("project_id").and_then(Value::as_str) {
            Some(p) if valid_project_id(p) => Ok(p.to_owned()),
            _ => Err(SourceError::Config(
                "the service account key names no project: set `project_id`".into(),
            )),
        }
    }

    /// `projects/{project}/databases/{database}`.
    pub fn database_path(&self) -> Result<String, SourceError> {
        Ok(format!(
            "projects/{}/databases/{}",
            self.project()?,
            self.database_id
        ))
    }
}

/// `host:port` (a bracketed IPv6 literal allowed), without a scheme.
pub fn host_port(s: &str) -> Option<(String, u16)> {
    if s.contains("://") || s.contains('/') {
        return None;
    }
    let (host, port) = s.rsplit_once(':')?;
    let port: u16 = port.parse().ok()?;
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if host.is_empty() || port == 0 {
        return None;
    }
    Some((host.to_owned(), port))
}

/// Google Cloud project ids: 6–30 characters of `[a-z0-9-]`, starting
/// with a letter. Older projects may carry a `domain.com:` prefix.
fn valid_project_id(p: &str) -> bool {
    let id = p.rsplit_once(':').map_or(p, |(_, id)| id);
    (4..=30).contains(&id.len())
        && id.starts_with(|c: char| c.is_ascii_lowercase())
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && p.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.:".contains(&b))
}

/// `(default)`, or 4–63 characters of `[a-z0-9-]` starting with a letter.
fn valid_database_id(d: &str) -> bool {
    d == "(default)"
        || ((4..=63).contains(&d.len())
            && d.starts_with(|c: char| c.is_ascii_lowercase())
            && d.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn key_json() -> String {
        json!({
            "type": "service_account",
            "project_id": "my-app-123",
            "private_key_id": "abc",
            "private_key": "-----BEGIN PRIVATE KEY-----\nhunter2\n-----END PRIVATE KEY-----\n",
            "client_email": "sync@my-app-123.iam.gserviceaccount.com",
            "client_id": "1",
            "token_uri": "http://169.254.169.254/evil",
            "universe_domain": "evil.example"
        })
        .to_string()
    }

    #[test]
    fn minimal_config_takes_the_project_from_the_key() {
        let c = FirestoreConfig::from_value(&json!({"service_account": key_json()})).unwrap();
        assert_eq!(c.database_id, "(default)");
        assert_eq!(c.snapshot, SnapshotOptions::default());
        assert_eq!(
            c.database_path().unwrap(),
            "projects/my-app-123/databases/(default)"
        );
        assert_eq!(
            c.client_email().as_deref(),
            Some("sync@my-app-123.iam.gserviceaccount.com")
        );
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"), "{dbg}");
    }

    #[test]
    fn the_key_keeps_only_its_own_fields_and_googles_token_endpoint() {
        let c = FirestoreConfig::from_value(&json!({"service_account": key_json(), "project_id": "other-project", "database_id": "search-db"})).unwrap();
        let key = c.service_account_key().unwrap();
        assert_eq!(key["token_uri"], GOOGLE_TOKEN_URI);
        assert!(key.get("universe_domain").is_none());
        assert_eq!(
            key["client_email"],
            "sync@my-app-123.iam.gserviceaccount.com"
        );
        assert_eq!(
            c.database_path().unwrap(),
            "projects/other-project/databases/search-db"
        );
    }

    #[test]
    fn the_emulator_needs_no_key_but_a_project() {
        let c = FirestoreConfig::from_value(
            &json!({"emulator_host": "localhost:8080", "project_id": "demo-test"}),
        )
        .unwrap();
        assert_eq!(
            c.database_path().unwrap(),
            "projects/demo-test/databases/(default)"
        );
        assert!(FirestoreConfig::from_value(&json!({"emulator_host": "localhost:8080"})).is_err());
    }

    #[test]
    fn rejects_bad_configs_without_echoing_secrets() {
        let cases = [
            json!({}),
            json!({"service_account": "hunter2"}),
            json!({"service_account": "{\"type\": \"authorized_user\", \"refresh_token\": \"hunter2\"}"}),
            json!({"service_account": "{\"type\": \"service_account\", \"private_key\": \"hunter2\"}"}),
            json!({"service_account": key_json(), "project_id": "Bad_Project"}),
            json!({"service_account": key_json(), "database_id": "x"}),
            json!({"service_account": key_json(), "snapshot": {"page_size": 0}}),
            json!({"service_account": key_json(), "snapshot": {"page_size": 5000}}),
            json!({"service_account": key_json(), "unknown": true}),
            json!({"emulator_host": "http://localhost:8080", "project_id": "demo-test"}),
            json!({"emulator_host": "localhost", "project_id": "demo-test"}),
        ];
        for case in cases {
            match FirestoreConfig::from_value(&case) {
                Err(SourceError::Config(m)) => assert!(!m.contains("hunter2"), "{m}"),
                other => panic!("{case} gave {other:?}"),
            }
        }
    }

    #[test]
    fn host_ports() {
        assert_eq!(
            host_port("localhost:8080"),
            Some(("localhost".into(), 8080))
        );
        assert_eq!(host_port("[::1]:8080"), Some(("::1".into(), 8080)));
        assert_eq!(host_port("localhost"), None);
        assert_eq!(host_port("localhost:0"), None);
        assert_eq!(host_port("http://x:1"), None);
    }

    #[test]
    fn project_and_database_ids() {
        assert!(valid_project_id("my-app-123"));
        assert!(valid_project_id("example.com:my-app"));
        assert!(!valid_project_id("My-App"));
        assert!(!valid_project_id("1abc-def"));
        assert!(valid_database_id("(default)"));
        assert!(valid_database_id("search-db"));
        assert!(!valid_database_id("Search"));
    }
}
