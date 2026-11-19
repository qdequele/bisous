//! What a user configures: one sync = one source → one Meilisearch
//! instance, with one or more collection → index mappings.

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
}

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
