//! The connector's typed configuration (`SourceSpec.config`).

use meili_connect_core::{Secret, SourceError, StreamMapping};
use serde::{Deserialize, Serialize};

/// MongoDB connector settings.
///
/// ```json
/// {
///   "uri": "mongodb+srv://user:pass@cluster0.example.mongodb.net/?retryWrites=true",
///   "database": "shop",
///   "full_document": "updateLookup"
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MongoConfig {
    /// Connection string, `mongodb://` or `mongodb+srv://` (Atlas). Holds
    /// the credentials, so it is a [`Secret`] and is scrubbed from every
    /// error message.
    pub uri: Secret,
    /// Default database. Lets mappings name a collection without its
    /// database (`orders` instead of `shop.orders`) and limits `discover`
    /// to this database.
    #[serde(default)]
    pub database: Option<String>,
    /// What change events carry for updates. See [`FullDocumentMode`].
    #[serde(default)]
    pub full_document: FullDocumentMode,
    /// Documents per `find` batch during the snapshot (bounded memory).
    #[serde(default = "default_snapshot_batch_size")]
    pub snapshot_batch_size: u32,
}

fn default_snapshot_batch_size() -> u32 {
    1_000
}

/// The change stream `fullDocument` option, spelled as MongoDB spells it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FullDocumentMode {
    /// Update events carry the current document (one extra lookup per
    /// update on the server). Updates become `Op::Upsert`.
    #[default]
    UpdateLookup,
    /// Post-images when the collection has `changeStreamPreAndPostImages`
    /// enabled, otherwise the update description.
    WhenAvailable,
    /// Post-images are required: the stream fails without them.
    Required,
    /// No full document: updates become `Op::Patch` from the update
    /// description.
    Default,
}

impl MongoConfig {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, SourceError> {
        // Deliberately not echoing serde's message verbatim with the input:
        // serde errors name fields and types, never values, so they are safe.
        let config: MongoConfig = serde_json::from_value(value.clone())
            .map_err(|e| SourceError::Config(format!("invalid MongoDB source config: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), SourceError> {
        let uri = self.uri.expose();
        if !(uri.starts_with("mongodb://") || uri.starts_with("mongodb+srv://")) {
            return Err(SourceError::Config(
                "uri must start with mongodb:// or mongodb+srv://".into(),
            ));
        }
        if let Some(db) = &self.database
            && !valid_db_name(db)
        {
            return Err(SourceError::Config(format!("invalid database name {db:?}")));
        }
        if self.snapshot_batch_size == 0 || self.snapshot_batch_size > 100_000 {
            return Err(SourceError::Config(
                "snapshot_batch_size must be between 1 and 100000".into(),
            ));
        }
        Ok(())
    }
}

fn valid_db_name(db: &str) -> bool {
    !db.is_empty()
        && db.len() < 64
        && !db
            .chars()
            .any(|c| matches!(c, '/' | '\\' | '.' | ' ' | '"' | '$' | '\0'))
}

/// One mapped collection: where it lives and the name the engine knows it by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub db: String,
    pub coll: String,
    /// The mapping's `collection` string, emitted verbatim in
    /// `Change::collection` so the engine matches it.
    pub name: String,
}

/// Resolve each mapping's `collection` to a namespace. `db.coll` is split on
/// the first dot (database names cannot contain one, collection names can);
/// a name without a dot uses the configured default database.
pub fn resolve_targets(
    config: &MongoConfig,
    streams: &[StreamMapping],
) -> Result<Vec<Target>, SourceError> {
    let mut targets: Vec<Target> = Vec::with_capacity(streams.len());
    for mapping in streams {
        let name = mapping.collection.as_str();
        let (db, coll) = match name.split_once('.') {
            // A dotted name is always database + collection, even with a
            // default database: `a.b` cannot be told apart otherwise.
            Some((db, coll)) if valid_db_name(db) && !coll.is_empty() => {
                (db.to_owned(), coll.to_owned())
            }
            _ => match &config.database {
                Some(db) if !name.is_empty() => (db.clone(), name.to_owned()),
                _ => {
                    return Err(SourceError::Config(format!(
                        "collection {name:?} must be written database.collection \
                         (or set `database` in the source config)"
                    )));
                }
            },
        };
        if coll.starts_with("system.") {
            return Err(SourceError::Config(format!(
                "{name:?} is a system collection and cannot be synced"
            )));
        }
        if targets.iter().any(|t| t.db == db && t.coll == coll) {
            continue;
        }
        targets.push(Target {
            db,
            coll,
            name: name.to_owned(),
        });
    }
    Ok(targets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapping(collection: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": collection, "index": "i"})).unwrap()
    }

    #[test]
    fn defaults() {
        let c = MongoConfig::from_json(&json!({"uri": "mongodb://localhost"})).unwrap();
        assert_eq!(c.full_document, FullDocumentMode::UpdateLookup);
        assert_eq!(c.snapshot_batch_size, 1_000);
        assert_eq!(c.database, None);
    }

    #[test]
    fn full_document_spelling() {
        for (s, m) in [
            ("updateLookup", FullDocumentMode::UpdateLookup),
            ("whenAvailable", FullDocumentMode::WhenAvailable),
            ("required", FullDocumentMode::Required),
            ("default", FullDocumentMode::Default),
        ] {
            let c = MongoConfig::from_json(
                &json!({"uri": "mongodb+srv://u:p@x.mongodb.net", "full_document": s}),
            )
            .unwrap();
            assert_eq!(c.full_document, m);
        }
        assert!(
            MongoConfig::from_json(&json!({"uri": "mongodb://h", "full_document": "nope"}))
                .is_err()
        );
    }

    #[test]
    fn rejects_bad_config_without_leaking() {
        let err = MongoConfig::from_json(&json!({"uri": "postgres://u:hunter2@h"})).unwrap_err();
        assert!(matches!(err, SourceError::Config(_)));
        assert!(!err.to_string().contains("hunter2"));
        let err = MongoConfig::from_json(&json!({"uri": "mongodb://u:hunter2@h", "extra": 1}))
            .unwrap_err();
        assert!(!err.to_string().contains("hunter2"));
        assert!(MongoConfig::from_json(&json!({"uri": "mongodb://h", "database": "a.b"})).is_err());
        assert!(
            MongoConfig::from_json(&json!({"uri": "mongodb://h", "snapshot_batch_size": 0}))
                .is_err()
        );
    }

    #[test]
    fn debug_redacts_uri() {
        let c = MongoConfig::from_json(&json!({"uri": "mongodb://u:hunter2@h"})).unwrap();
        assert!(!format!("{c:?}").contains("hunter2"));
    }

    #[test]
    fn targets() {
        let c = MongoConfig::from_json(&json!({"uri": "mongodb://h", "database": "shop"})).unwrap();
        let t = resolve_targets(
            &c,
            &[
                mapping("shop.orders"),
                mapping("users"),
                mapping("crm.a.b"),
                mapping("users"),
            ],
        )
        .unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!((t[0].db.as_str(), t[0].coll.as_str()), ("shop", "orders"));
        assert_eq!(
            (t[1].db.as_str(), t[1].coll.as_str(), t[1].name.as_str()),
            ("shop", "users", "users")
        );
        assert_eq!((t[2].db.as_str(), t[2].coll.as_str()), ("crm", "a.b"));

        let no_db = MongoConfig::from_json(&json!({"uri": "mongodb://h"})).unwrap();
        assert!(resolve_targets(&no_db, &[mapping("users")]).is_err());
        assert!(resolve_targets(&no_db, &[mapping("shop.system.views")]).is_err());
    }
}
