//! What a user configures: one sync = one source → one Meilisearch
//! instance, with one or more collection → index mappings.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::Secret;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Postgres,
    /// Postgres with Supabase's setup rules (direct connection, IPv6,
    /// paused projects, slot limits). Same wire protocol.
    Supabase,
    Mysql,
    Mongodb,
    Kafka,
    Dynamodb,
    Firestore,
    /// PlanetScale's MySQL (Vitess): VStream over gRPC, not a binlog.
    Planetscale,
}

impl SourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceKind::Postgres => "postgres",
            SourceKind::Supabase => "supabase",
            SourceKind::Mysql => "mysql",
            SourceKind::Mongodb => "mongodb",
            SourceKind::Kafka => "kafka",
            SourceKind::Dynamodb => "dynamodb",
            SourceKind::Firestore => "firestore",
            SourceKind::Planetscale => "planetscale",
        }
    }
}

/// Connector settings. `config` is the connector's own typed struct as
/// JSON; each connector crate owns and validates its shape. Secrets inside
/// it use [`Secret`], so the whole spec is safe to `Debug`-print once
/// deserialized into the typed struct. The raw `serde_json::Value` is not:
/// never log `config` itself.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceSpec {
    pub kind: SourceKind,
    pub config: serde_json::Value,
}

impl std::fmt::Debug for SourceSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceSpec")
            .field("kind", &self.kind)
            .field("config", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeiliDestination {
    pub url: String,
    pub api_key: Secret,
    /// The API keys of embedders on the destination's indexes, by the exact
    /// `url` the embedder calls. Meilisearch never gives an embedder's key
    /// back, so settings copied to a shadow index would lose it: the sink
    /// puts the key named here back into every embedder that calls that
    /// URL, and into no other.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub embedder_keys: BTreeMap<String, Secret>,
}

/// More embedder keys than this on one destination is a mistake.
pub const MAX_EMBEDDER_KEYS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldSelection {
    #[default]
    All,
    Include(Vec<String>),
    Exclude(Vec<String>),
}

impl FieldSelection {
    pub fn keeps(&self, field: &str) -> bool {
        match self {
            FieldSelection::All => true,
            FieldSelection::Include(list) => list.iter().any(|f| f == field),
            FieldSelection::Exclude(list) => !list.iter().any(|f| f == field),
        }
    }
}

/// One collection → one index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMapping {
    /// Source-side name, same naming as [`crate::Change::collection`].
    pub collection: String,
    /// Destination index uid.
    pub index: String,
    /// Field that receives the document id. Defaults to `id`.
    #[serde(default = "default_primary_key")]
    pub primary_key: String,
    #[serde(default)]
    pub fields: FieldSelection,
    /// `source field → document field` renames, applied after selection.
    #[serde(default)]
    pub rename: std::collections::BTreeMap<String, String>,
    /// Source field holding an embedding (pgvector, a float array). When
    /// set, it is moved to `_vectors.<embedder>` instead of being indexed
    /// as a plain field.
    #[serde(default)]
    pub vector: Option<VectorMapping>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorMapping {
    pub field: String,
    pub embedder: String,
}

fn default_primary_key() -> String {
    "id".to_owned()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncSpec {
    pub id: String,
    /// Owning tenant; `None` in standalone mode.
    pub tenant: Option<String>,
    pub name: String,
    pub source: SourceSpec,
    pub destination: MeiliDestination,
    pub streams: Vec<StreamMapping>,
    /// Hold the sync instead of applying a `Truncate` or a batch that would
    /// delete more than this share of an index (0.0–1.0). `None` disables
    /// the guard.
    #[serde(default = "default_mass_delete_guard")]
    pub mass_delete_guard: Option<f64>,
}

fn default_mass_delete_guard() -> Option<f64> {
    Some(0.5)
}

impl SyncSpec {
    /// Structural checks every entry point runs before storing or running
    /// a spec. Returns the first problem as a user-facing sentence.
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("the sync id is empty".to_owned());
        }
        if self.streams.is_empty() {
            return Err("a sync needs at least one stream".to_owned());
        }
        let mut indexes = std::collections::BTreeSet::new();
        let mut collections = std::collections::BTreeSet::new();
        for stream in &self.streams {
            if stream.collection.is_empty() || stream.index.is_empty() {
                return Err("a stream needs a collection and an index".to_owned());
            }
            if !valid_index_uid(&stream.index) {
                return Err(format!(
                    "index '{}' is not a valid Meilisearch uid (letters, digits, '-' and '_', at most 400 bytes)",
                    stream.index
                ));
            }
            if stream.primary_key.is_empty() {
                return Err(format!(
                    "stream '{}' has an empty primary key",
                    stream.collection
                ));
            }
            // Document ids are derived from the row key alone, so two
            // collections in one index would overwrite each other, and a
            // truncate of one would have to spare the other.
            if !indexes.insert(stream.index.as_str()) {
                return Err(format!(
                    "index '{}' is the target of more than one stream; use one index per collection",
                    stream.index
                ));
            }
            if !collections.insert(stream.collection.as_str()) {
                return Err(format!(
                    "collection '{}' is mapped more than once",
                    stream.collection
                ));
            }
        }
        if let Some(guard) = self.mass_delete_guard
            && !(0.0..=1.0).contains(&guard)
        {
            return Err("mass_delete_guard must be between 0 and 1".to_owned());
        }
        let keys = &self.destination.embedder_keys;
        if keys.len() > MAX_EMBEDDER_KEYS {
            return Err(format!(
                "destination.embedder_keys holds more than {MAX_EMBEDDER_KEYS} keys"
            ));
        }
        for (url, key) in keys {
            // The sink compares it to an embedder's `url` byte for byte. It
            // is not a secret (every answer shows it), so it may carry none.
            let authority = url
                .split_once("://")
                .filter(|(scheme, _)| matches!(*scheme, "http" | "https"))
                .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default());
            let plain = authority.is_some_and(|a| !a.is_empty() && !a.contains('@'));
            if !plain || url.chars().any(char::is_whitespace) {
                return Err(
                    "destination.embedder_keys is keyed by the http(s) URL an embedder calls, \
                     written exactly as in the embedder's settings, without a user or a password"
                        .to_owned(),
                );
            }
            if key.is_empty() {
                return Err("destination.embedder_keys holds an empty key".to_owned());
            }
        }
        Ok(())
    }
}

fn valid_index_uid(uid: &str) -> bool {
    uid.len() <= 400
        && uid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(streams: Vec<(&str, &str)>) -> SyncSpec {
        SyncSpec {
            id: "s1".into(),
            tenant: None,
            name: "n".into(),
            source: SourceSpec {
                kind: SourceKind::Postgres,
                config: serde_json::json!({}),
            },
            destination: MeiliDestination {
                url: "http://m".into(),
                api_key: Secret::new("k"),
                embedder_keys: Default::default(),
            },
            streams: streams
                .into_iter()
                .map(|(c, i)| StreamMapping {
                    collection: c.into(),
                    index: i.into(),
                    primary_key: "id".into(),
                    fields: FieldSelection::All,
                    rename: Default::default(),
                    vector: None,
                })
                .collect(),
            mass_delete_guard: Some(0.5),
        }
    }

    #[test]
    fn validate_rejects_shared_indexes_and_bad_uids() {
        assert!(
            spec(vec![("public.a", "a"), ("public.b", "b")])
                .validate()
                .is_ok()
        );
        assert!(spec(vec![]).validate().is_err());
        assert!(
            spec(vec![("public.a", "x"), ("public.b", "x")])
                .validate()
                .is_err()
        );
        assert!(
            spec(vec![("public.a", "a"), ("public.a", "b")])
                .validate()
                .is_err()
        );
        assert!(spec(vec![("public.a", "bad uid")]).validate().is_err());
        let mut s = spec(vec![("public.a", "a")]);
        s.mass_delete_guard = Some(1.5);
        assert!(s.validate().is_err());
    }

    #[test]
    fn validate_checks_embedder_keys() {
        let with = |keys: Vec<(String, &str)>| {
            let mut s = spec(vec![("public.a", "a")]);
            s.destination.embedder_keys = keys
                .into_iter()
                .map(|(url, key)| (url, Secret::new(key)))
                .collect();
            s.validate()
        };
        let url = "https://gateway.example/v1/embeddings";
        assert!(with(vec![(url.to_owned(), "k")]).is_ok());
        assert!(with(vec![("http://10.0.0.1:8080/embed?model=m".to_owned(), "k")]).is_ok());
        for bad in [
            "gateway.example/v1/embeddings".to_owned(),
            "ftp://gateway.example/".to_owned(),
            "https://".to_owned(),
            format!("{url} "),
            format!(" {url}"),
            "https://user:password@gateway.example/v1/embeddings".to_owned(),
            "https://token@gateway.example/".to_owned(),
        ] {
            assert!(with(vec![(bad.clone(), "k")]).is_err(), "{bad:?}");
        }
        assert!(with(vec![(url.to_owned(), "")]).is_err());
        let many = |n: usize| (0..n).map(|i| (format!("{url}/{i}"), "k")).collect();
        assert!(with(many(MAX_EMBEDDER_KEYS)).is_ok());
        assert!(with(many(MAX_EMBEDDER_KEYS + 1)).is_err());
    }
}
