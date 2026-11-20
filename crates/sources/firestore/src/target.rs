//! What a stream mapping's `collection` names, and how a document maps to
//! a key.
//!
//! | `collection` | Reads | Document key |
//! |---|---|---|
//! | `products` | the top-level collection `products` | the document id (`p1`) |
//! | `users/alice/orders` | that one subcollection | the document id |
//! | `**/orders` | every collection with id `orders`, at any depth (a collection group) | the document path (`users/alice/orders/o1`) |
//!
//! A document id is unique within its collection, so a collection's keys
//! are its document ids, which stay readable as Meilisearch ids. In a
//! collection group two parents may hold the same document id, so the key
//! is the whole path (hashed by `core::document_id`, since it contains
//! `/`). Every document also carries its path as `_path`.

use gcloud_sdk::google::firestore::v1::StructuredQuery;
use gcloud_sdk::google::firestore::v1::structured_query::{
    CollectionSelector, Direction, FieldReference, Order,
};
use meili_connect_core::SourceError;

/// Prefix of a collection group mapping.
pub const GROUP_PREFIX: &str = "**/";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionRef {
    /// One collection: `parent` is the path of the document holding it
    /// (empty for a top-level collection).
    Collection { parent: String, id: String },
    /// Every collection named `id`.
    Group { id: String },
}

impl CollectionRef {
    pub fn parse(s: &str) -> Result<Self, SourceError> {
        let bad = || {
            SourceError::Config(format!(
                "`{s}` is not a Firestore collection: use `products`, a subcollection path \
                 such as `users/alice/orders`, or `**/orders` for every collection named `orders`"
            ))
        };
        if let Some(id) = s.strip_prefix(GROUP_PREFIX) {
            if !valid_segment(id) {
                return Err(bad());
            }
            return Ok(Self::Group { id: id.to_owned() });
        }
        let segments: Vec<&str> = s.split('/').collect();
        if segments.len().is_multiple_of(2) || !segments.iter().all(|seg| valid_segment(seg)) {
            return Err(bad());
        }
        let (id, parents) = segments.split_last().ok_or_else(bad)?;
        Ok(Self::Collection {
            parent: parents.join("/"),
            id: (*id).to_owned(),
        })
    }

    /// The collection id (the last segment).
    pub fn id(&self) -> &str {
        match self {
            Self::Collection { id, .. } | Self::Group { id } => id,
        }
    }

    pub fn is_group(&self) -> bool {
        matches!(self, Self::Group { .. })
    }

    /// The `parent` resource of a query over this collection.
    pub fn query_parent(&self, documents_root: &str) -> String {
        match self {
            Self::Collection { parent, .. } if !parent.is_empty() => {
                format!("{documents_root}/{parent}")
            }
            _ => documents_root.to_owned(),
        }
    }

    /// A query over every document of the collection, ordered by name (the
    /// order cursors follow).
    pub fn query(&self) -> StructuredQuery {
        StructuredQuery {
            from: vec![CollectionSelector {
                collection_id: self.id().to_owned(),
                all_descendants: self.is_group(),
            }],
            order_by: vec![Order {
                field: Some(FieldReference {
                    field_path: "__name__".into(),
                }),
                direction: Direction::Ascending as i32,
            }],
            ..Default::default()
        }
    }

    /// Whether the document at `path` (relative to the database) belongs to
    /// this collection.
    pub fn contains(&self, path: &str) -> bool {
        let Some((collection_path, _doc)) = path.rsplit_once('/') else {
            return false;
        };
        match self {
            Self::Collection { parent, id } => match collection_path.rsplit_once('/') {
                Some((p, c)) => c == id && p == parent,
                None => parent.is_empty() && collection_path == id,
            },
            Self::Group { id } => collection_path.rsplit('/').next() == Some(id.as_str()),
        }
    }

    /// The key of the document at `path` (see the module docs).
    pub fn key(&self, path: &str) -> serde_json::Value {
        match self {
            Self::Collection { .. } => {
                serde_json::Value::String(path.rsplit('/').next().unwrap_or(path).to_owned())
            }
            Self::Group { .. } => serde_json::Value::String(path.to_owned()),
        }
    }
}

/// A non-empty path segment without the reserved `__…__` form, `*` or `.`
/// alone.
fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 1500
        && !s.contains(['*', '/'])
        && s != "."
        && s != ".."
        && !(s.starts_with("__") && s.ends_with("__"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_the_three_forms() {
        assert_eq!(
            CollectionRef::parse("products").unwrap(),
            CollectionRef::Collection {
                parent: String::new(),
                id: "products".into()
            }
        );
        assert_eq!(
            CollectionRef::parse("users/alice/orders").unwrap(),
            CollectionRef::Collection {
                parent: "users/alice".into(),
                id: "orders".into()
            }
        );
        assert_eq!(
            CollectionRef::parse("**/orders").unwrap(),
            CollectionRef::Group {
                id: "orders".into()
            }
        );
        for bad in [
            "",
            "users/alice",
            "a//b",
            "**/",
            "**/a/b",
            "*/x",
            "__x__",
            "a/b/**",
        ] {
            assert!(CollectionRef::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn membership_and_keys() {
        let root = CollectionRef::parse("orders").unwrap();
        let sub = CollectionRef::parse("users/alice/orders").unwrap();
        let group = CollectionRef::parse("**/orders").unwrap();

        assert!(root.contains("orders/o1"));
        assert!(!root.contains("users/alice/orders/o1"));
        assert!(sub.contains("users/alice/orders/o1"));
        assert!(!sub.contains("users/bob/orders/o1"));
        assert!(!sub.contains("orders/o1"));
        for p in ["orders/o1", "users/alice/orders/o1", "a/b/c/d/orders/x"] {
            assert!(group.contains(p), "{p}");
        }
        assert!(!group.contains("orders_archive/o1"));
        assert!(!group.contains("orders/o1/items/i1"));

        assert_eq!(root.key("orders/o1"), json!("o1"));
        assert_eq!(sub.key("users/alice/orders/o1"), json!("o1"));
        assert_eq!(
            group.key("users/alice/orders/o1"),
            json!("users/alice/orders/o1")
        );
    }

    #[test]
    fn queries() {
        let root = "projects/p/databases/(default)/documents";
        let sub = CollectionRef::parse("users/alice/orders").unwrap();
        assert_eq!(sub.query_parent(root), format!("{root}/users/alice"));
        let q = sub.query();
        assert_eq!(q.from[0].collection_id, "orders");
        assert!(!q.from[0].all_descendants);
        let group = CollectionRef::parse("**/orders").unwrap();
        assert_eq!(group.query_parent(root), root);
        assert!(group.query().from[0].all_descendants);
    }
}
