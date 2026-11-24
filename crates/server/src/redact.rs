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

use meili_connect_core::{SourceKind, SourceSpec};
use serde_json::Value;

pub const REDACTED: &str = "<redacted>";

/// Paths of the secret fields of each connector's config.
fn known_secret_paths(kind: SourceKind) -> &'static [&'static [&'static str]] {
    match kind {
        SourceKind::Postgres | SourceKind::Supabase => &[&["url"], &["password"]],
        SourceKind::Mysql | SourceKind::Planetscale => &[&["password"]],
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
        SourceKind::Firestore => &[&["service_account"]],
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
    // Keyed by the embedder's URL, so no field name gives these away.
    if let Some(keys) = spec
        .get_mut("destination")
        .and_then(|d| d.get_mut("embedder_keys"))
        .and_then(Value::as_object_mut)
    {
        for key in keys.values_mut().filter(|k| !k.is_null()) {
            *key = Value::String(REDACTED.to_owned());
        }
    }
    if let Some(source) = spec.get_mut("source") {
        redact_by_name(source);
    }
}

/// The non-secret fields that say where, and as whom, a connector connects.
/// A stored secret belongs to them: it is only sent again to the same place.
fn identity_paths(kind: SourceKind) -> &'static [&'static [&'static str]] {
    match kind {
        SourceKind::Postgres | SourceKind::Supabase => &[
            &["url"],
            &["host"],
            &["port"],
            &["database"],
            &["user"],
            &["sslmode"],
            &["ca_cert"],
        ],
        SourceKind::Mysql => &[
            &["host"],
            &["port"],
            &["user"],
            &["database"],
            &["tls", "mode"],
            &["tls", "ca_pem"],
        ],
        SourceKind::Planetscale => &[
            &["host"],
            &["grpc_port"],
            &["mysql_port"],
            &["user"],
            &["database"],
            &["tls"],
        ],
        SourceKind::Mongodb => &[&["uri"], &["database"]],
        SourceKind::Kafka => &[
            &["bootstrap_servers"],
            &["security", "protocol"],
            &["security", "tls", "ca_pem"],
            &["security", "tls", "cert_pem"],
            &["security", "tls", "verify_hostname"],
            &["security", "sasl", "mechanism"],
            &["security", "sasl", "username"],
        ],
        SourceKind::Dynamodb => &[
            &["region"],
            &["endpoint_url"],
            &["credentials", "type"],
            &["credentials", "access_key_id"],
            &["credentials", "role_arn"],
            &["credentials", "session_name"],
        ],
        SourceKind::Firestore => &[&["project_id"], &["database_id"], &["emulator_host"]],
    }
}

/// Fields that name objects the sync owns on the origin, besides the
/// identity: changing them orphans the old objects like a new host would.
fn origin_object_paths(kind: SourceKind) -> &'static [&'static [&'static str]] {
    match kind {
        SourceKind::Postgres | SourceKind::Supabase => &[&["slot_name"], &["publication_name"]],
        SourceKind::Mysql => &[&["server_id"]],
        _ => &[],
    }
}

/// Boolean fields that decide which origin objects the sync owns, with
/// their default: absent and the default are the same value.
fn origin_flag_paths(kind: SourceKind) -> &'static [(&'static [&'static str], bool)] {
    match kind {
        // Who owns the publication: the sync, or someone else (the Lab).
        SourceKind::Postgres | SourceKind::Supabase => &[(&["manage_publication"], true)],
        _ => &[],
    }
}

/// Both source specs (real values, no placeholders) reach the same origin
/// as the same user and own the same objects there.
pub fn same_origin(old: &SourceSpec, new: &SourceSpec) -> bool {
    let flag = |config: &Value, path: &[&str], default: bool| {
        at(config, path).and_then(Value::as_bool).unwrap_or(default)
    };
    old.kind == new.kind
        && identity_paths(old.kind)
            .iter()
            .chain(origin_object_paths(old.kind))
            .all(|path| at(&old.config, path) == at(&new.config, path))
        && origin_flag_paths(old.kind).iter().all(|(path, default)| {
            flag(&old.config, path, *default) == flag(&new.config, path, *default)
        })
}

fn at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(value, |v, p| v.get(*p))
        .filter(|v| !v.is_null())
}

/// `incoming` (with its placeholders) names the same thing as `stored`: a
/// placeholder restores the stored value, so it counts as unchanged.
fn same(incoming: Option<&Value>, stored: Option<&Value>) -> bool {
    incoming.and_then(Value::as_str) == Some(REDACTED) || incoming == stored
}

/// A [`REDACTED`] placeholder may only stand for a secret whose connection
/// identity did not change: the same source kind, hosts, ports, URLs,
/// users, databases, TLS settings… for a source secret; the same URL for
/// the Meilisearch key. Otherwise a `PUT` could send a stored secret to a
/// new host. Names the first changed field.
pub fn check_placeholders(incoming: &Value, stored: &Value) -> Result<(), String> {
    let refuse = |secret: String, changed: String| {
        Err(format!(
            "`{secret}` is \"{REDACTED}\" but `{changed}` changed: send the secret again \
             (a stored secret is only reused for the same connection)"
        ))
    };
    if let Some(dest) = incoming.get("destination")
        && let Some(secret) = contains_redacted(dest)
    {
        let url = |v: &Value| {
            v.get("destination")
                .and_then(|d| d.get("url"))
                .and_then(Value::as_str)
                .map(|u| u.trim().to_owned())
        };
        if url(incoming) != url(stored) {
            return refuse(
                format!("destination.{secret}"),
                "destination.url".to_owned(),
            );
        }
    }
    if let Some(source) = incoming.get("source")
        && let Some(secret) = contains_redacted(source)
    {
        let secret = format!("source.{secret}");
        let kind_of = |v: &Value| v.get("source").and_then(|s| s.get("kind")).cloned();
        let kind = kind_of(incoming)
            .filter(|k| Some(k) == kind_of(stored).as_ref())
            .and_then(|k| serde_json::from_value::<SourceKind>(k).ok());
        let Some(kind) = kind else {
            return refuse(secret, "source.kind".to_owned());
        };
        let config = |v: &'_ Value| v.get("source").and_then(|s| s.get("config")).cloned();
        let (new, old) = (
            config(incoming).unwrap_or(Value::Null),
            config(stored).unwrap_or(Value::Null),
        );
        for path in identity_paths(kind) {
            if !same(at(&new, path), at(&old, path)) {
                return refuse(secret, format!("source.config.{}", path.join(".")));
            }
        }
    }
    Ok(())
}

/// Replace every [`REDACTED`] string of `incoming` by the value at the same
/// path in `stored`. Fails, naming the field, when there is nothing to
/// restore it from. Run [`check_placeholders`] first.
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
        let c = redacted(
            "firestore",
            json!({"service_account": "{\"type\": \"service_account\", \"private_key\": \"hunter2\"}",
                "project_id": "my-app", "database_id": "(default)"}),
        );
        assert_eq!(c["project_id"], "my-app");
        let c = redacted(
            "planetscale",
            json!({"host": "aws.connect.psdb.cloud", "database": "shop", "user": "u",
                "password": "hunter2"}),
        );
        assert_eq!(c["host"], "aws.connect.psdb.cloud");
        assert_eq!(c["user"], "u");
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
    fn redacted_values_round_trip_for_the_same_connection_only() {
        let stored = json!({
            "source": {"kind": "postgres", "config": {
                "url": "postgres://u:pw@h/db", "password": "pw", "slot_name": "s"}},
            "destination": {"url": "http://m", "api_key": "k"},
        });
        let mut view = stored.clone();
        redact_spec(&mut view);
        // Unchanged identity, other fields edited: the secrets come back.
        view["source"]["config"]["slot_name"] = json!("s2");
        check_placeholders(&view, &stored).unwrap();
        let mut restored = view.clone();
        restore_redacted(&mut restored, &stored).unwrap();
        assert_eq!(restored["source"]["config"]["url"], "postgres://u:pw@h/db");
        assert_eq!(restored["destination"]["api_key"], "k");

        // A new Meilisearch URL with the stored key: refused.
        let mut moved = view.clone();
        moved["destination"]["url"] = json!("http://attacker.example");
        let err = check_placeholders(&moved, &stored).unwrap_err();
        assert!(
            err.contains("destination.api_key") && err.contains("destination.url"),
            "{err}"
        );
        // With the key sent again it is fine.
        moved["destination"]["api_key"] = json!("new-key");
        check_placeholders(&moved, &stored).unwrap();

        // A new source host (overriding the URL's) with the stored password.
        let mut moved = view.clone();
        moved["source"]["config"]["host"] = json!("attacker.example");
        let err = check_placeholders(&moved, &stored).unwrap_err();
        assert!(err.contains("source.config.host"), "{err}");
        // Same with another kind, user or port.
        for (path, value) in [
            ("kind", json!("mysql")),
            ("user", json!("admin")),
            ("port", json!(5433)),
            ("sslmode", json!("disable")),
        ] {
            let mut moved = view.clone();
            if path == "kind" {
                moved["source"]["kind"] = value;
            } else {
                moved["source"]["config"][path] = value;
            }
            assert!(check_placeholders(&moved, &stored).is_err(), "{path}");
        }
    }

    #[test]
    fn identity_of_every_connector() {
        let kafka = json!({"source": {"kind": "kafka", "config": {
            "bootstrap_servers": ["b:9092"],
            "security": {"protocol": "sasl_ssl", "sasl": {"mechanism": "PLAIN", "username": "u",
                "password": "pw"}}}}});
        let mut view = kafka.clone();
        redact_spec(&mut view);
        check_placeholders(&view, &kafka).unwrap();
        view["source"]["config"]["bootstrap_servers"] = json!(["evil:9092"]);
        assert!(check_placeholders(&view, &kafka).is_err());

        let dynamo = json!({"source": {"kind": "dynamodb", "config": {"region": "eu-west-1",
            "credentials": {"type": "static", "access_key_id": "AKIA", "secret_access_key": "s"}}}});
        let mut view = dynamo.clone();
        redact_spec(&mut view);
        check_placeholders(&view, &dynamo).unwrap();
        view["source"]["config"]["endpoint_url"] = json!("http://evil:8000");
        assert!(check_placeholders(&view, &dynamo).is_err());

        let mysql = json!({"source": {"kind": "mysql", "config": {"host": "h", "user": "u",
            "password": "pw"}}});
        let mut view = mysql.clone();
        redact_spec(&mut view);
        check_placeholders(&view, &mysql).unwrap();
        view["source"]["config"]["tls"] = json!({"mode": "disabled"});
        assert!(check_placeholders(&view, &mysql).is_err());

        // The MongoDB URI is itself the secret: a new URI is a real value.
        let mongo = json!({"source": {"kind": "mongodb", "config": {"uri": "mongodb://u:pw@h"}}});
        let mut view = mongo.clone();
        redact_spec(&mut view);
        check_placeholders(&view, &mongo).unwrap();
        view["source"]["config"]["uri"] = json!("mongodb://evil");
        check_placeholders(&view, &mongo).unwrap();
    }

    #[test]
    fn manage_publication_is_part_of_the_origin() {
        let spec = |config: Value| SourceSpec {
            kind: SourceKind::Supabase,
            config,
        };
        let base = json!({"host": "db.x.supabase.co", "user": "u", "publication_name": "p"});
        let mut external = base.clone();
        external["manage_publication"] = json!(false);
        let mut explicit = base.clone();
        explicit["manage_publication"] = json!(true);
        assert!(!same_origin(&spec(base.clone()), &spec(external.clone())));
        assert!(!same_origin(
            &spec(external.clone()),
            &spec(explicit.clone())
        ));
        // Absent is the default, `true`.
        assert!(same_origin(&spec(base), &spec(explicit)));
        assert!(same_origin(&spec(external.clone()), &spec(external)));
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
