//! Secrets are write-only.
//!
//! Every spec the API returns goes through [`redact_spec`]: the
//! Meilisearch API key, the known secret fields of each connector config,
//! and, defensively, any string field whose name looks like a credential or
//! any URL carrying a password are replaced by [`REDACTED`].
//!
//! A write may send [`REDACTED`] back for any field: [`restore_redacted`]
//! puts the stored value back at the same path, so a client can `GET`,
//! edit and `PUT` a sync without ever seeing its secrets.

use meili_connect_core::SourceKind;
use serde_json::Value;

pub const REDACTED: &str = "<redacted>";

/// Paths of the secret fields of each connector's config.
fn known_secret_paths(kind: SourceKind) -> &'static [&'static [&'static str]] {
    match kind {
        SourceKind::Postgres | SourceKind::Supabase => &[&["url"], &["password"]],
        SourceKind::Mysql => &[&["password"]],
        SourceKind::Mongodb => &[&["uri"]],
        SourceKind::Kafka => &[
            &["security", "tls", "key_pem"],
            &["security", "tls", "key_password"],
            &["security", "sasl", "password"],
        ],
        SourceKind::Dynamodb => &[
            &["credentials", "secret_access_key"],
            &["credentials", "session_token"],
            &["credentials", "external_id"],
        ],
    }
}

/// Whether a field name looks like it holds a credential.
fn sensitive_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    const PARTS: &[&str] = &[
        "password",
        "passwd",
        "passphrase",
        "secret",
        "token",
        "credential",
        "private_key",
        "privatekey",
        "api_key",
        "apikey",
        "key_pem",
    ];
    PARTS.iter().any(|p| n.contains(p))
        || n == "key"
        || (n.ends_with("_key") && n != "primary_key" && n != "partition_key" && n != "sort_key")
}

fn url_like_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n.contains("uri") || n.contains("url") || n.contains("dsn") || n.contains("connection")
}

/// A URL with a password in its user info (`scheme://user:pw@host`) or in
/// its query string.
fn url_has_credentials(s: &str) -> bool {
    let Some((_, rest)) = s.split_once("://") else {
        // `key=value` connection strings.
        let lower = s.to_ascii_lowercase();
        return lower.contains("password=") || lower.contains("pwd=");
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let lower = rest.to_ascii_lowercase();
    authority.contains('@')
        || lower.contains("password=")
        || lower.contains("pwd=")
        || lower.contains("secret")
        || lower.contains("token=")
}

fn redact_at(value: &mut Value, path: &[&str]) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut cur = value;
    for p in parents {
        match cur.get_mut(*p) {
            Some(next) => cur = next,
            None => return,
        }
    }
    if let Some(v) = cur.get_mut(*last)
        && !v.is_null()
    {
        *v = Value::String(REDACTED.to_owned());
    }
}

/// Defensive pass over a whole JSON tree.
fn redact_by_name(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                let redact = match v {
                    Value::String(s) => {
                        s != REDACTED
                            && (sensitive_name(k) || (url_like_name(k) && url_has_credentials(s)))
                    }
                    _ => false,
                };
                if redact {
                    *v = Value::String(REDACTED.to_owned());
                } else {
                    redact_by_name(v);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_by_name),
        _ => {}
    }
}

/// Redact a source config of `kind`.
pub fn redact_source_config(kind: SourceKind, config: &mut Value) {
    for path in known_secret_paths(kind) {
        redact_at(config, path);
    }
    redact_by_name(config);
}

/// Redact a serialized `SyncSpec` (or any JSON with its `source` and
/// `destination` fields).
pub fn redact_spec(spec: &mut Value) {
    let kind = spec
        .get("source")
        .and_then(|s| s.get("kind"))
        .and_then(|k| serde_json::from_value::<SourceKind>(k.clone()).ok());
    if let (Some(kind), Some(config)) = (
        kind,
        spec.get_mut("source").and_then(|s| s.get_mut("config")),
    ) {
        redact_source_config(kind, config);
    }
    redact_at(spec, &["destination", "api_key"]);
    if let Some(source) = spec.get_mut("source") {
        redact_by_name(source);
    }
}

/// Replace every [`REDACTED`] string of `incoming` by the value at the same
/// path in `stored`. Fails, naming the field, when there is nothing to
/// restore it from.
pub fn restore_redacted(incoming: &mut Value, stored: &Value) -> Result<(), String> {
    let mut path = Vec::new();
    restore(incoming, Some(stored), &mut path)
}

fn restore(
    incoming: &mut Value,
    stored: Option<&Value>,
    path: &mut Vec<String>,
) -> Result<(), String> {
    match incoming {
        Value::String(s) if s == REDACTED => match stored {
            Some(v) if !v.is_null() && v.as_str() != Some(REDACTED) => {
                *incoming = v.clone();
                Ok(())
            }
            _ => Err(format!(
                "`{}` is \"{REDACTED}\" but the sync has no stored value for it: send the real value",
                path.join(".")
            )),
        },
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                path.push(k.clone());
                restore(v, stored.and_then(|s| s.get(k.as_str())), path)?;
                path.pop();
            }
            Ok(())
        }
        Value::Array(items) => {
            for (i, v) in items.iter_mut().enumerate() {
                path.push(i.to_string());
                restore(v, stored.and_then(|s| s.get(i)), path)?;
                path.pop();
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Whether a JSON tree still holds a [`REDACTED`] placeholder (a create
/// must send real values).
pub fn contains_redacted(value: &Value) -> Option<String> {
    fn walk(v: &Value, path: &mut Vec<String>) -> Option<String> {
        match v {
            Value::String(s) if s == REDACTED => Some(path.join(".")),
            Value::Object(map) => map.iter().find_map(|(k, v)| {
                path.push(k.clone());
                let found = walk(v, path);
                path.pop();
                found
            }),
            Value::Array(items) => items.iter().enumerate().find_map(|(i, v)| {
                path.push(i.to_string());
                let found = walk(v, path);
                path.pop();
                found
            }),
            _ => None,
        }
    }
    walk(value, &mut Vec::new())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use serde_json::json;

    fn redacted(kind: &str, config: Value) -> Value {
        let mut spec = json!({
            "source": {"kind": kind, "config": config},
            "destination": {"url": "http://m", "api_key": "meili-hunter2"},
        });
        redact_spec(&mut spec);
        assert!(!spec.to_string().contains("hunter2"), "{spec}");
        assert_eq!(spec["destination"]["api_key"], REDACTED);
        assert_eq!(spec["destination"]["url"], "http://m");
        spec["source"]["config"].clone()
    }

    #[test]
    fn known_fields_of_every_connector() {
        let c = redacted(
            "postgres",
            json!({"url": "postgres://u:hunter2@h/db", "password": "hunter2", "host": "h"}),
        );
        assert_eq!(c["host"], "h");
        redacted("supabase", json!({"url": "postgres://u:hunter2@h/db"}));
        let c = redacted(
            "mysql",
            json!({"host": "h", "user": "u", "password": "hunter2"}),
        );
        assert_eq!(c["user"], "u");
        redacted("mongodb", json!({"uri": "mongodb://h/?authSource=hunter2"}));
        let c = redacted(
            "kafka",
            json!({"bootstrap_servers": ["b:9092"], "key_fields": ["id"], "security": {
                "protocol": "sasl_ssl",
                "tls": {"ca_pem": "CA", "key_pem": "hunter2", "key_password": "hunter2"},
                "sasl": {"mechanism": "PLAIN", "username": "u", "password": "hunter2"}}}),
        );
        assert_eq!(c["key_fields"], json!(["id"]));
        assert_eq!(c["security"]["tls"]["ca_pem"], "CA");
        assert_eq!(c["security"]["sasl"]["username"], "u");
        let c = redacted(
            "dynamodb",
            json!({"region": "r", "endpoint_url": "http://localhost:8000", "credentials": {
                "type": "static", "access_key_id": "AKIA", "secret_access_key": "hunter2",
                "session_token": "hunter2"}}),
        );
        assert_eq!(c["endpoint_url"], "http://localhost:8000");
        assert_eq!(c["credentials"]["access_key_id"], "AKIA");
        redacted(
            "dynamodb",
            json!({"region": "r", "credentials": {"type": "assume_role", "role_arn": "arn",
                "external_id": "hunter2"}}),
        );
    }

    #[test]
    fn unknown_fields_are_redacted_by_name() {
        let c = redacted(
            "postgres",
            json!({"host": "h", "extra": {"client_secret": "hunter2", "my_token": "hunter2",
                "dsn": "postgres://u:hunter2@h/db", "list": [{"api_key": "hunter2"}],
                "plain_url": "https://example.com/path"}}),
        );
        assert_eq!(c["extra"]["plain_url"], "https://example.com/path");
    }

    #[test]
    fn redacted_values_round_trip() {
        let stored = json!({
            "source": {"kind": "postgres", "config": {"url": "postgres://u:pw@h/db"}},
            "destination": {"url": "http://m", "api_key": "k"},
        });
        let mut view = stored.clone();
        redact_spec(&mut view);
        view["destination"]["url"] = json!("http://m2");
        restore_redacted(&mut view, &stored).unwrap();
        assert_eq!(view["source"]["config"]["url"], "postgres://u:pw@h/db");
        assert_eq!(view["destination"]["api_key"], "k");
        assert_eq!(view["destination"]["url"], "http://m2");
    }

    #[test]
    fn a_placeholder_without_a_stored_value_is_refused() {
        let stored = json!({"source": {"config": {}}});
        let mut incoming = json!({"source": {"config": {"password": REDACTED}}});
        let err = restore_redacted(&mut incoming, &stored).unwrap_err();
        assert!(err.contains("source.config.password"), "{err}");
        assert_eq!(
            contains_redacted(&json!({"a": [1, {"b": REDACTED}]})).as_deref(),
            Some("a.1.b")
        );
        assert_eq!(contains_redacted(&json!({"a": "x"})), None);
    }
}
