//! Folding a batch of `DocOp`s for one index so that each id is written at
//! most once.
//!
//! Semantics (the model the tests check against): `Replace` sets the whole
//! document, `Merge` sets the fields of `doc` on the existing document (or an
//! empty one) and then removes the `unset` fields, `Delete` removes the
//! document. Per id:
//!
//! - a `Replace` or a `Delete` overrides whatever came before (last write
//!   wins);
//! - a `Merge` after a `Replace` is folded into the `Replace`;
//! - a `Merge` after a `Merge` merges them (the later value wins, a later
//!   set cancels an earlier unset and the other way round);
//! - a `Merge` after a `Delete` starts from an empty document, so it becomes
//!   a `Replace` of the merged fields.
//!
//! Every id ends up with exactly one op. Ops on different ids commute, so
//! the output is grouped by kind (deletes, then replaces, then merges, each
//! in first-seen order) to give the sink the longest runs of one kind.

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use meili_connect_core::DocOp;

pub fn coalesce(ops: Vec<DocOp>) -> Vec<DocOp> {
    let mut order: Vec<String> = Vec::new();
    let mut latest: HashMap<String, DocOp> = HashMap::with_capacity(ops.len());
    for op in ops {
        let op = normalize(op);
        match latest.entry(op.id().to_owned()) {
            Entry::Vacant(slot) => {
                order.push(slot.key().clone());
                slot.insert(op);
            }
            Entry::Occupied(mut slot) => {
                let prev = std::mem::replace(slot.get_mut(), DocOp::Delete { id: String::new() });
                *slot.get_mut() = fold(prev, op);
            }
        }
    }

    let mut deletes = Vec::new();
    let mut replaces = Vec::new();
    let mut merges = Vec::new();
    for id in order {
        match latest.remove(&id) {
            Some(op @ DocOp::Delete { .. }) => deletes.push(op),
            Some(op @ DocOp::Replace { .. }) => replaces.push(op),
            Some(op @ DocOp::Merge { .. }) => merges.push(op),
            None => {}
        }
    }
    deletes.append(&mut replaces);
    deletes.append(&mut merges);
    deletes
}

/// A `Merge` that both sets and unsets a field means "unset" (the model
/// applies `doc` first): drop such fields from `doc`.
fn normalize(op: DocOp) -> DocOp {
    match op {
        DocOp::Merge { id, mut doc, unset } => {
            for field in &unset {
                doc.remove(field);
            }
            DocOp::Merge { id, doc, unset }
        }
        other => other,
    }
}

fn fold(prev: DocOp, next: DocOp) -> DocOp {
    match (prev, next) {
        (_, next @ (DocOp::Replace { .. } | DocOp::Delete { .. })) => next,
        (DocOp::Delete { .. }, DocOp::Merge { id, mut doc, unset }) => {
            for field in &unset {
                doc.remove(field);
            }
            DocOp::Replace { id, doc }
        }
        (
            DocOp::Replace { id, mut doc },
            DocOp::Merge {
                doc: set, unset, ..
            },
        ) => {
            doc.extend(set);
            for field in &unset {
                doc.remove(field);
            }
            DocOp::Replace { id, doc }
        }
        (
            DocOp::Merge {
                id,
                mut doc,
                unset: earlier_unset,
            },
            DocOp::Merge {
                doc: set,
                unset: later_unset,
                ..
            },
        ) => {
            let mut unset: Vec<String> = earlier_unset
                .into_iter()
                .filter(|f| !set.contains_key(f))
                .collect();
            doc.extend(set);
            for field in later_unset {
                doc.remove(&field);
                if !unset.contains(&field) {
                    unset.push(field);
                }
            }
            DocOp::Merge { id, doc, unset }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_connect_core::Document;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn doc(v: Value) -> Document {
        match v {
            Value::Object(m) => m,
            _ => Document::new(),
        }
    }

    fn replace(id: &str, v: Value) -> DocOp {
        DocOp::Replace {
            id: id.into(),
            doc: doc(v),
        }
    }

    fn merge(id: &str, v: Value, unset: &[&str]) -> DocOp {
        DocOp::Merge {
            id: id.into(),
            doc: doc(v),
            unset: unset.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn delete(id: &str) -> DocOp {
        DocOp::Delete { id: id.into() }
    }

    /// The reference semantics.
    fn apply(model: &mut BTreeMap<String, Document>, ops: &[DocOp]) {
        for op in ops {
            match op {
                DocOp::Replace { id, doc } => {
                    model.insert(id.clone(), doc.clone());
                }
                DocOp::Merge { id, doc, unset } => {
                    let entry = model.entry(id.clone()).or_default();
                    for (k, v) in doc {
                        entry.insert(k.clone(), v.clone());
                    }
                    for f in unset {
                        entry.remove(f);
                    }
                }
                DocOp::Delete { id } => {
                    model.remove(id);
                }
            }
        }
    }

    #[test]
    fn last_write_wins() {
        let out = coalesce(vec![
            replace("1", json!({"a": 1})),
            replace("1", json!({"a": 2})),
        ]);
        assert_eq!(out, vec![replace("1", json!({"a": 2}))]);
    }

    #[test]
    fn patch_folds_into_upsert() {
        let out = coalesce(vec![
            replace("1", json!({"a": 1, "b": 1})),
            merge("1", json!({"a": 2}), &["b"]),
        ]);
        assert_eq!(out, vec![replace("1", json!({"a": 2}))]);
    }

    #[test]
    fn patch_after_patch_merges_and_handles_unset() {
        let out = coalesce(vec![
            merge("1", json!({"a": 1, "b": 1}), &["c"]),
            merge("1", json!({"c": 3}), &["a"]),
        ]);
        assert_eq!(out, vec![merge("1", json!({"b": 1, "c": 3}), &["a"])]);
    }

    #[test]
    fn anything_after_delete_starts_fresh() {
        assert_eq!(
            coalesce(vec![delete("1"), merge("1", json!({"a": 1}), &["b"])]),
            vec![replace("1", json!({"a": 1}))]
        );
        assert_eq!(
            coalesce(vec![replace("1", json!({"a": 1})), delete("1")]),
            vec![delete("1")]
        );
        assert_eq!(
            coalesce(vec![delete("1"), replace("1", json!({"a": 1}))]),
            vec![replace("1", json!({"a": 1}))]
        );
    }

    #[test]
    fn output_grouped_by_kind_in_first_seen_order() {
        let out = coalesce(vec![
            merge("m", json!({"a": 1}), &[]),
            replace("r2", json!({})),
            delete("d"),
            replace("r1", json!({})),
        ]);
        let ids: Vec<&str> = out.iter().map(DocOp::id).collect();
        assert_eq!(ids, vec!["d", "r2", "r1", "m"]);
    }

    /// xorshift64*, enough for a reproducible property test without a dep.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn random_doc(rng: &mut Rng) -> Document {
        let mut d = Document::new();
        for f in ["a", "b", "c", "d"] {
            if rng.below(2) == 0 {
                d.insert(f.into(), json!(rng.below(5)));
            }
        }
        d
    }

    fn random_op(rng: &mut Rng) -> DocOp {
        let id = format!("{}", rng.below(4));
        match rng.below(3) {
            0 => DocOp::Replace {
                id,
                doc: random_doc(rng),
            },
            1 => {
                let unset = ["a", "b", "c", "d"]
                    .into_iter()
                    .filter(|_| rng.below(3) == 0)
                    .map(String::from)
                    .collect();
                DocOp::Merge {
                    id,
                    doc: random_doc(rng),
                    unset,
                }
            }
            _ => DocOp::Delete { id },
        }
    }

    #[test]
    fn coalesced_batch_is_equivalent_to_the_original_sequence() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for _ in 0..5_000 {
            // Some pre-existing state, so merges onto existing docs are hit.
            let mut initial = BTreeMap::new();
            for id in 0..4 {
                if rng.below(2) == 0 {
                    initial.insert(id.to_string(), random_doc(&mut rng));
                }
            }
            let len = rng.below(12) as usize;
            let ops: Vec<DocOp> = (0..len).map(|_| random_op(&mut rng)).collect();

            let mut expected = initial.clone();
            apply(&mut expected, &ops);
            let coalesced = coalesce(ops.clone());
            let mut actual = initial;
            apply(&mut actual, &coalesced);
            assert_eq!(actual, expected, "ops: {ops:?}\ncoalesced: {coalesced:?}");

            // Each id at most once.
            let mut seen = std::collections::HashSet::new();
            for op in &coalesced {
                assert!(
                    seen.insert(op.id().to_owned()),
                    "duplicate id in {coalesced:?}"
                );
            }
            // Merges never both set and unset a field.
            for op in &coalesced {
                if let DocOp::Merge { doc, unset, .. } = op {
                    assert!(unset.iter().all(|f| !doc.contains_key(f)));
                }
            }
        }
    }
}
