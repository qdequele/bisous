//! Change stream event (raw BSON) → `SourceEvent`.
//!
//! - `insert`, `replace` → `Op::Upsert(fullDocument)`.
//! - `update` → `Op::Upsert(fullDocument)` when the event carries one
//!   (`updateLookup`, post-images). Otherwise an `Op::Patch` built from
//!   `updateDescription`: top-level `updatedFields` become the patch body and
//!   top-level `removedFields` become `unset`. A patch cannot express a
//!   change *inside* a field (Meilisearch merges top-level fields only), so
//!   a dotted path (`address.city`, `tags.3`) or a `truncatedArrays` entry
//!   makes the source look the document up instead ([`Mapped::Lookup`]),
//!   which also yields a full `Op::Upsert`.
//! - `delete` → `Op::Delete`.
//! - `drop`, `rename` (both the old and the new name, when mapped),
//!   `dropDatabase` (every mapped collection of the database) →
//!   `SourceEvent::Truncate`.
//! - `invalidate` → the stream is reopened with `startAfter`.
//! - Anything else (DDL events, unmapped namespaces) is skipped.
//!
//! The `_id` is the key, encoded by [`crate::key::key_of`] so that `_id`s of
//! different types never share a document id (an ObjectId as its hex
//! string, an integer as a number, other types as typed wrappers), and it
//! also stays in the document body,
//! converted to JSON like any other field. The engine injects the mapping's
//! primary key separately, so the two never collide unless the mapping's
//! primary key is literally `_id`, in which case the injected id wins.

use std::collections::HashMap;

use meili_connect_core::{Change, Op, SourceError, SourceEvent};
use mongodb::bson::{Bson, Document};

use crate::config::Target;
use crate::convert::document_to_json;
use crate::key::key_of;

/// Mapped namespaces, `(db, coll)` → the name the engine knows.
#[derive(Debug, Clone, Default)]
pub struct Namespaces {
    by_ns: HashMap<(String, String), String>,
}

impl Namespaces {
    pub fn new(targets: &[Target]) -> Self {
        Self {
            by_ns: targets
                .iter()
                .map(|t| ((t.db.clone(), t.coll.clone()), t.name.clone()))
                .collect(),
        }
    }

    pub fn name(&self, db: &str, coll: &str) -> Option<&str> {
        self.by_ns
            .get(&(db.to_owned(), coll.to_owned()))
            .map(String::as_str)
    }

    fn in_db<'a>(&'a self, db: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.by_ns
            .iter()
            .filter(move |((d, _), _)| d == db)
            .map(|(_, name)| name.as_str())
    }
}

/// What to do with one change event.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapped {
    /// Emit these (possibly none: skipped event).
    Events(Vec<SourceEvent>),
    /// Fetch the current document by `_id` and emit an upsert (or nothing
    /// if it no longer exists: its `delete` event follows).
    Lookup {
        collection: String,
        db: String,
        coll: String,
        id: Bson,
    },
    /// The stream was invalidated; reopen it after this event's token.
    Invalidate,
}

fn fatal(msg: impl Into<String>) -> SourceError {
    SourceError::Fatal(msg.into())
}

fn namespace(ev: &Document, field: &str) -> Option<(String, Option<String>)> {
    let ns = ev.get_document(field).ok()?;
    let db = ns.get_str("db").ok()?.to_owned();
    let coll = ns.get_str("coll").ok().map(str::to_owned);
    Some((db, coll))
}

pub fn map_event(ev: &Document, names: &Namespaces) -> Result<Mapped, SourceError> {
    let op = ev
        .get_str("operationType")
        .map_err(|_| fatal("change event without operationType"))?;

    match op {
        "invalidate" => return Ok(Mapped::Invalidate),
        "dropDatabase" => {
            let Some((db, _)) = namespace(ev, "ns") else {
                return Ok(Mapped::Events(vec![]));
            };
            let mut colls: Vec<&str> = names.in_db(&db).collect();
            colls.sort_unstable();
            return Ok(Mapped::Events(
                colls
                    .into_iter()
                    .map(|c| SourceEvent::Truncate {
                        collection: c.to_owned(),
                    })
                    .collect(),
            ));
        }
        "rename" => {
            let mut out = Vec::new();
            for field in ["ns", "to"] {
                if let Some((db, Some(coll))) = namespace(ev, field)
                    && let Some(name) = names.name(&db, &coll)
                    && !out.iter().any(
                        |e| matches!(e, SourceEvent::Truncate { collection } if collection == name),
                    )
                {
                    out.push(SourceEvent::Truncate {
                        collection: name.to_owned(),
                    });
                }
            }
            return Ok(Mapped::Events(out));
        }
        _ => {}
    }

    let Some((db, Some(coll))) = namespace(ev, "ns") else {
        return Ok(Mapped::Events(vec![]));
    };
    let Some(collection) = names.name(&db, &coll).map(str::to_owned) else {
        return Ok(Mapped::Events(vec![]));
    };

    if op == "drop" {
        return Ok(Mapped::Events(vec![SourceEvent::Truncate { collection }]));
    }
    if !matches!(op, "insert" | "replace" | "update" | "delete") {
        return Ok(Mapped::Events(vec![]));
    }

    let id = ev
        .get_document("documentKey")
        .ok()
        .and_then(|k| k.get("_id"))
        .cloned()
        .ok_or_else(|| {
            fatal(format!(
                "{op} event on {collection} without documentKey._id"
            ))
        })?;
    let key = vec![key_of(&id)];
    let change = |op: Op| {
        Mapped::Events(vec![SourceEvent::Change(Change {
            collection: collection.clone(),
            key: key.clone(),
            op,
        })])
    };

    if op == "delete" {
        return Ok(change(Op::Delete));
    }
    if let Ok(full) = ev.get_document("fullDocument") {
        return Ok(change(Op::Upsert {
            doc: document_to_json(full),
        }));
    }
    let lookup = Mapped::Lookup {
        collection: collection.clone(),
        db,
        coll,
        id,
    };
    if op != "update" {
        // insert/replace always carry the document; be defensive anyway.
        return Ok(lookup);
    }

    let Ok(desc) = ev.get_document("updateDescription") else {
        return Ok(lookup);
    };
    let updated = desc.get_document("updatedFields").ok();
    let removed = desc.get_array("removedFields").ok();
    let truncated = desc
        .get_array("truncatedArrays")
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    let dotted_update = updated.is_some_and(|u| u.keys().any(|k| k.contains('.')));
    let dotted_remove =
        removed.is_some_and(|r| r.iter().any(|f| f.as_str().is_none_or(|s| s.contains('.'))));
    if truncated || dotted_update || dotted_remove {
        return Ok(lookup);
    }

    let doc = updated.map(document_to_json).unwrap_or_default();
    let unset: Vec<String> = removed
        .map(|r| {
            r.iter()
                .filter_map(|f| f.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    if doc.is_empty() && unset.is_empty() {
        // A no-op update (`$set` to the same value).
        return Ok(Mapped::Events(vec![]));
    }
    Ok(change(Op::Patch { doc, unset }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{doc, oid::ObjectId};
    use serde_json::json;

    fn names() -> Namespaces {
        Namespaces::new(&[
            Target {
                db: "shop".into(),
                coll: "orders".into(),
                name: "shop.orders".into(),
            },
            Target {
                db: "shop".into(),
                coll: "users".into(),
                name: "users".into(),
            },
            Target {
                db: "crm".into(),
                coll: "leads".into(),
                name: "crm.leads".into(),
            },
        ])
    }

    fn oid() -> ObjectId {
        ObjectId::parse_str("65a1f0c2e4b0a1b2c3d4e5f6").unwrap()
    }

    fn ev(op: &str, extra: Document) -> Document {
        let mut d = doc! {
            "_id": {"_data": "82"},
            "operationType": op,
            "ns": {"db": "shop", "coll": "orders"},
            "documentKey": {"_id": oid()},
        };
        d.extend(extra);
        d
    }

    fn one_change(m: Mapped) -> Change {
        match m {
            Mapped::Events(mut v) if v.len() == 1 => match v.remove(0) {
                SourceEvent::Change(c) => c,
                other => panic!("not a change: {other:?}"),
            },
            other => panic!("expected one event, got {other:?}"),
        }
    }

    #[test]
    fn insert_and_replace_upsert() {
        for op in ["insert", "replace"] {
            let c = one_change(
                map_event(
                    &ev(op, doc! {"fullDocument": {"_id": oid(), "total": 3_i32}}),
                    &names(),
                )
                .unwrap(),
            );
            assert_eq!(c.collection, "shop.orders");
            assert_eq!(c.key, vec![json!("65a1f0c2e4b0a1b2c3d4e5f6")]);
            assert_eq!(
                c.op,
                Op::Upsert {
                    doc: json!({"_id": "65a1f0c2e4b0a1b2c3d4e5f6", "total": 3})
                        .as_object()
                        .unwrap()
                        .clone()
                }
            );
        }
    }

    #[test]
    fn update_with_full_document_is_upsert() {
        let c = one_change(
            map_event(
                &ev(
                    "update",
                    doc! {
                        "fullDocument": {"_id": oid(), "a": 1_i32},
                        "updateDescription": {"updatedFields": {"a": 1_i32}, "removedFields": []},
                    },
                ),
                &names(),
            )
            .unwrap(),
        );
        assert!(matches!(c.op, Op::Upsert { .. }));
    }

    #[test]
    fn update_without_full_document_is_patch() {
        let c = one_change(
            map_event(
                &ev(
                    "update",
                    doc! {
                        "fullDocument": Bson::Null,
                        "updateDescription": {
                            "updatedFields": {"status": "paid", "when": mongodb::bson::DateTime::from_millis(0)},
                            "removedFields": ["coupon"],
                            "truncatedArrays": [],
                        },
                    },
                ),
                &names(),
            )
            .unwrap(),
        );
        assert_eq!(
            c.op,
            Op::Patch {
                doc: json!({"status": "paid", "when": "1970-01-01T00:00:00.000Z"})
                    .as_object()
                    .unwrap()
                    .clone(),
                unset: vec!["coupon".into()],
            }
        );
    }

    #[test]
    fn dotted_paths_and_truncated_arrays_need_a_lookup() {
        let cases = [
            doc! {"updatedFields": {"address.city": "Paris"}, "removedFields": []},
            doc! {"updatedFields": {"tags.3": "x"}, "removedFields": []},
            doc! {"updatedFields": {}, "removedFields": ["address.zip"]},
            doc! {"updatedFields": {}, "removedFields": [], "truncatedArrays": [{"field": "tags", "newSize": 2_i32}]},
        ];
        for desc in cases {
            let m = map_event(&ev("update", doc! {"updateDescription": desc}), &names()).unwrap();
            assert_eq!(
                m,
                Mapped::Lookup {
                    collection: "shop.orders".into(),
                    db: "shop".into(),
                    coll: "orders".into(),
                    id: Bson::ObjectId(oid()),
                }
            );
        }
    }

    #[test]
    fn noop_update_is_skipped() {
        let m = map_event(
            &ev(
                "update",
                doc! {"updateDescription": {"updatedFields": {}, "removedFields": []}},
            ),
            &names(),
        )
        .unwrap();
        assert_eq!(m, Mapped::Events(vec![]));
    }

    #[test]
    fn delete() {
        let c = one_change(map_event(&ev("delete", doc! {}), &names()).unwrap());
        assert_eq!(c.op, Op::Delete);
    }

    #[test]
    fn compound_id_is_one_typed_key() {
        let mut e = ev("delete", doc! {});
        e.insert("documentKey", doc! {"_id": {"tenant": 1_i32, "n": "a"}});
        let c = one_change(map_event(&e, &names()).unwrap());
        assert_eq!(
            c.key,
            vec![json!({"$document": [["tenant", 1], ["n", "a"]]})]
        );
    }

    /// Mixed `_id` types in one collection: each document keeps its own
    /// Meilisearch id, and its delete event targets that same id.
    #[test]
    fn mixed_id_types_never_collide_and_deletes_match_inserts() {
        use meili_connect_core::document_id;
        use mongodb::bson::{Binary, DateTime, spec::BinarySubtype};
        let date = DateTime::from_millis(1_700_000_000_123);
        let uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: hex::decode("0123456789abcdef0123456789abcdef").unwrap(),
        };
        let pairs: Vec<(Bson, Bson)> = vec![
            (Bson::Int32(1), Bson::String("1".into())),
            (Bson::ObjectId(oid()), Bson::String(oid().to_hex())),
            (Bson::Int32(1), Bson::Double(1.0)),
            (
                Bson::DateTime(date),
                Bson::String("2023-11-14T22:13:20.123Z".into()),
            ),
            (
                Bson::Binary(uuid),
                Bson::String("01234567-89ab-cdef-0123-456789abcdef".into()),
            ),
        ];
        let id_of = |op: &str, id: &Bson| {
            let mut e = ev(op, doc! {});
            e.insert("documentKey", doc! {"_id": id.clone()});
            if op == "insert" {
                e.insert("fullDocument", doc! {"_id": id.clone()});
            }
            document_id(&one_change(map_event(&e, &names()).unwrap()).key)
        };
        for (a, b) in pairs {
            assert_ne!(id_of("insert", &a), id_of("insert", &b), "{a:?} vs {b:?}");
            for x in [&a, &b] {
                assert_eq!(id_of("insert", x), id_of("delete", x), "{x:?}");
            }
        }
    }

    #[test]
    fn unmapped_namespace_is_skipped() {
        let mut e = ev("insert", doc! {"fullDocument": {"_id": 1_i32}});
        e.insert("ns", doc! {"db": "shop", "coll": "carts"});
        assert_eq!(map_event(&e, &names()).unwrap(), Mapped::Events(vec![]));
    }

    #[test]
    fn mapping_without_db_prefix_keeps_its_name() {
        let mut e = ev("delete", doc! {});
        e.insert("ns", doc! {"db": "shop", "coll": "users"});
        assert_eq!(
            one_change(map_event(&e, &names()).unwrap()).collection,
            "users"
        );
    }

    #[test]
    fn drop_rename_drop_database_truncate() {
        let truncate = |c: &str| SourceEvent::Truncate {
            collection: c.into(),
        };
        assert_eq!(
            map_event(&ev("drop", doc! {}), &names()).unwrap(),
            Mapped::Events(vec![truncate("shop.orders")])
        );
        let mut rename = ev("rename", doc! {"to": {"db": "shop", "coll": "users"}});
        rename.remove("documentKey");
        assert_eq!(
            map_event(&rename, &names()).unwrap(),
            Mapped::Events(vec![truncate("shop.orders"), truncate("users")])
        );
        let rename_out = doc! {
            "operationType": "rename",
            "ns": {"db": "shop", "coll": "tmp"},
            "to": {"db": "shop", "coll": "orders"},
        };
        assert_eq!(
            map_event(&rename_out, &names()).unwrap(),
            Mapped::Events(vec![truncate("shop.orders")])
        );
        let drop_db = doc! {"operationType": "dropDatabase", "ns": {"db": "shop"}};
        assert_eq!(
            map_event(&drop_db, &names()).unwrap(),
            Mapped::Events(vec![truncate("shop.orders"), truncate("users")])
        );
    }

    #[test]
    fn invalidate_and_unknown() {
        assert_eq!(
            map_event(&doc! {"operationType": "invalidate"}, &names()).unwrap(),
            Mapped::Invalidate
        );
        assert_eq!(
            map_event(&ev("createIndexes", doc! {}), &names()).unwrap(),
            Mapped::Events(vec![])
        );
        assert!(map_event(&doc! {}, &names()).is_err());
    }
}
