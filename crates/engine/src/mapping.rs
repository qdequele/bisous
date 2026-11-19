//! `Change` + `StreamMapping` → `DocOp`.
//!
//! Field selection first, then renames. The vector field (when configured)
//! bypasses selection and renames and lands in `_vectors.<embedder>`. The
//! document id comes from [`document_id`]; the sink injects it under the
//! mapping's primary key.

use std::collections::HashMap;

use meili_connect_core::{Change, DocOp, Document, Op, StreamMapping, document_id};
use serde_json::Value;

/// Name of the Meilisearch reserved field holding user-provided embeddings.
pub const VECTORS_FIELD: &str = "_vectors";

/// Resolves changes against the mappings of one sync.
#[derive(Debug, Clone)]
pub struct Mapper {
    mappings: Vec<StreamMapping>,
    by_collection: HashMap<String, Vec<usize>>,
}

impl Mapper {
    pub fn new(streams: &[StreamMapping]) -> Self {
        let mut by_collection: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, m) in streams.iter().enumerate() {
            by_collection
                .entry(m.collection.clone())
                .or_default()
                .push(i);
        }
        Self {
            mappings: streams.to_vec(),
            by_collection,
        }
    }

    pub fn mappings(&self) -> &[StreamMapping] {
        &self.mappings
    }

    /// Positions (in [`Mapper::mappings`]) of the mappings fed by
    /// `collection`. Empty for an unmapped collection.
    pub fn indexes_for(&self, collection: &str) -> &[usize] {
        self.by_collection
            .get(collection)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// Maps a change to one `DocOp` per mapping it feeds, tagged with the
    /// mapping's position. An unmapped collection yields nothing.
    pub fn map(&self, change: Change) -> Vec<(usize, DocOp)> {
        let targets = self.indexes_for(&change.collection);
        match targets {
            [] => Vec::new(),
            [only] => vec![(*only, map_change(&self.mappings[*only], change))],
            many => many
                .iter()
                .map(|&i| (i, map_change(&self.mappings[i], change.clone())))
                .collect(),
        }
    }
}

/// Maps one change through one mapping. The caller has already checked that
/// `change.collection` matches the mapping.
pub fn map_change(mapping: &StreamMapping, change: Change) -> DocOp {
    let id = document_id(&change.key);
    match change.op {
        Op::Upsert { doc } => DocOp::Replace {
            id,
            doc: map_document(mapping, doc),
        },
        Op::Patch { doc, unset } => {
            let mut doc = map_document(mapping, doc);
            let mut mapped_unset = Vec::with_capacity(unset.len());
            for field in unset {
                if let Some(vector) = &mapping.vector
                    && field == vector.field
                {
                    insert_vector(&mut doc, &vector.embedder, Value::Null);
                    continue;
                }
                if !mapping.fields.keeps(&field) {
                    continue;
                }
                let name = mapping.rename.get(&field).cloned().unwrap_or(field);
                if !mapped_unset.contains(&name) {
                    mapped_unset.push(name);
                }
            }
            DocOp::Merge {
                id,
                doc,
                unset: mapped_unset,
            }
        }
        Op::Delete => DocOp::Delete { id },
    }
}

/// Applies selection, renames and the vector move to a document body.
pub fn map_document(mapping: &StreamMapping, mut doc: Document) -> Document {
    let vector = mapping.vector.as_ref().and_then(|v| {
        doc.remove(&v.field)
            .map(|value| (v.embedder.as_str(), value))
    });
    let mut out = Document::new();
    for (field, value) in doc {
        if !mapping.fields.keeps(&field) {
            continue;
        }
        let name = mapping.rename.get(&field).cloned().unwrap_or(field);
        out.insert(name, value);
    }
    if let Some((embedder, value)) = vector {
        insert_vector(&mut out, embedder, value);
    }
    out
}

fn insert_vector(doc: &mut Document, embedder: &str, value: Value) {
    let slot = doc
        .entry(VECTORS_FIELD.to_owned())
        .or_insert_with(|| Value::Object(Document::new()));
    if !slot.is_object() {
        *slot = Value::Object(Document::new());
    }
    if let Value::Object(vectors) = slot {
        vectors.insert(embedder.to_owned(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_connect_core::FieldSelection;
    use meili_connect_core::spec::VectorMapping;
    use serde_json::json;

    fn mapping() -> StreamMapping {
        StreamMapping {
            collection: "public.products".into(),
            index: "products".into(),
            primary_key: "id".into(),
            fields: FieldSelection::All,
            rename: Default::default(),
            vector: None,
        }
    }

    fn doc(v: Value) -> Document {
        match v {
            Value::Object(m) => m,
            _ => Document::new(),
        }
    }

    fn change(op: Op) -> Change {
        Change {
            collection: "public.products".into(),
            key: vec![json!(7)],
            op,
        }
    }

    #[test]
    fn upsert_keeps_all_fields_by_default() {
        let op = map_change(
            &mapping(),
            change(Op::Upsert {
                doc: doc(json!({"name": "a", "price": 3})),
            }),
        );
        assert_eq!(
            op,
            DocOp::Replace {
                id: "7".into(),
                doc: doc(json!({"name": "a", "price": 3}))
            }
        );
    }

    #[test]
    fn include_selection_then_rename() {
        let mut m = mapping();
        m.fields = FieldSelection::Include(vec!["name".into(), "price".into()]);
        m.rename.insert("name".into(), "title".into());
        let op = map_change(
            &m,
            change(Op::Upsert {
                doc: doc(json!({"name": "a", "price": 3, "secret": "x"})),
            }),
        );
        assert_eq!(
            op,
            DocOp::Replace {
                id: "7".into(),
                doc: doc(json!({"title": "a", "price": 3}))
            }
        );
    }

    #[test]
    fn exclude_selection() {
        let mut m = mapping();
        m.fields = FieldSelection::Exclude(vec!["secret".into()]);
        let mapped = map_document(&m, doc(json!({"name": "a", "secret": "x"})));
        assert_eq!(mapped, doc(json!({"name": "a"})));
    }

    #[test]
    fn vector_moves_to_vectors_and_bypasses_selection() {
        let mut m = mapping();
        m.fields = FieldSelection::Include(vec!["name".into()]);
        m.vector = Some(VectorMapping {
            field: "embedding".into(),
            embedder: "default".into(),
        });
        let mapped = map_document(&m, doc(json!({"name": "a", "embedding": [0.1, 0.2]})));
        assert_eq!(
            mapped,
            doc(json!({"name": "a", "_vectors": {"default": [0.1, 0.2]}}))
        );
    }

    #[test]
    fn vector_merges_with_source_vectors_object() {
        let mut m = mapping();
        m.vector = Some(VectorMapping {
            field: "embedding".into(),
            embedder: "mine".into(),
        });
        let mapped = map_document(
            &m,
            doc(json!({"_vectors": {"other": [1.0]}, "embedding": [0.5]})),
        );
        assert_eq!(
            mapped,
            doc(json!({"_vectors": {"other": [1.0], "mine": [0.5]}}))
        );
    }

    #[test]
    fn patch_maps_unset_fields() {
        let mut m = mapping();
        m.fields = FieldSelection::Exclude(vec!["secret".into()]);
        m.rename.insert("name".into(), "title".into());
        m.vector = Some(VectorMapping {
            field: "embedding".into(),
            embedder: "default".into(),
        });
        let op = map_change(
            &m,
            change(Op::Patch {
                doc: doc(json!({"price": 4})),
                unset: vec!["name".into(), "secret".into(), "embedding".into()],
            }),
        );
        assert_eq!(
            op,
            DocOp::Merge {
                id: "7".into(),
                doc: doc(json!({"price": 4, "_vectors": {"default": null}})),
                unset: vec!["title".into()],
            }
        );
    }

    #[test]
    fn delete_and_composite_ids() {
        let op = map_change(
            &mapping(),
            Change {
                collection: "public.products".into(),
                key: vec![json!(1), json!("x")],
                op: Op::Delete,
            },
        );
        assert_eq!(
            op,
            DocOp::Delete {
                id: document_id(&[json!(1), json!("x")])
            }
        );
    }

    #[test]
    fn mapper_drops_unmapped_and_fans_out() {
        let mut second = mapping();
        second.index = "products_copy".into();
        let mut other = mapping();
        other.collection = "public.users".into();
        other.index = "users".into();
        let mapper = Mapper::new(&[mapping(), other, second]);

        let unmapped = Change {
            collection: "public.orders".into(),
            key: vec![json!(1)],
            op: Op::Delete,
        };
        assert!(mapper.map(unmapped).is_empty());

        let mapped = mapper.map(change(Op::Delete));
        let targets: Vec<usize> = mapped.iter().map(|(i, _)| *i).collect();
        assert_eq!(targets, vec![0, 2]);
        assert_eq!(mapper.indexes_for("public.users"), &[1]);
    }
}
