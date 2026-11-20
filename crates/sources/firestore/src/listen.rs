//! The `Listen` protocol, as a state machine fed one response at a time.
//!
//! One stream carries one target per mapped collection (target id = mapping
//! position + 1). The server sends, per target, documents that changed
//! (`DocumentChange`), were deleted (`DocumentDelete`) or left the target
//! (`DocumentRemove`), and target changes:
//!
//! - `RESET`: the server cannot send a delta and re-sends the target's
//!   whole result set, then `CURRENT`. A document that is not re-sent was
//!   deleted. The Firestore emulator answers every resume, and every change,
//!   this way; Google's service does when a resume token is too old.
//! - `NO_CHANGE` with no target ids and a `read_time`: the whole stream is
//!   at a consistent snapshot. Its `resume_token` covers every change sent
//!   before it: it becomes a [`ListenCheckpoint`].
//! - An `ExistenceFilter` says how many documents match a target, "if
//!   different from the count of documents in the client that match, the
//!   client must manually determine which documents no longer match".
//!
//! Both a `RESET` and a filter need the set of documents the index holds.
//! The state keeps it per target, as document paths, when it knows it: from
//! the snapshot of a fresh sync, then from every change. After a restart it
//! does not (it is not part of the checkpoint), and neither when it would
//! exceed `max_tracked_documents`: a `RESET` or a filter then cannot be
//! applied, and is reported as `PositionLost` (resync into shadow indexes).

use std::collections::HashSet;

use base64::Engine as _;
use gcloud_sdk::google::firestore::v1::listen_response::ResponseType;
use gcloud_sdk::google::firestore::v1::target_change::TargetChangeType;
use gcloud_sdk::google::firestore::v1::{Document, ListenResponse, TargetChange};
use gcloud_sdk::prost_types::Timestamp;
use meili_connect_core::{Change, Checkpoint, Op, SourceError, SourceEvent};
use serde::{Deserialize, Serialize};

use crate::convert::{document_fields, relative_path};
use crate::target::CollectionRef;

/// Field every document carries: its path relative to the database.
pub const PATH_FIELD: &str = "_path";

/// The durable position: a resume token of a consistent snapshot of every
/// target, or, before the first one, the time the sync started from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenCheckpoint {
    /// Base64 of the token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_token: Option<String>,
    /// RFC 3339, nanoseconds.
    pub read_time: String,
    /// The mapped collections, in order: a token is only valid for the same
    /// targets.
    pub collections: Vec<String>,
}

impl ListenCheckpoint {
    pub fn token_bytes(&self) -> Result<Option<Vec<u8>>, SourceError> {
        self.resume_token
            .as_deref()
            .map(|t| {
                base64::engine::general_purpose::STANDARD
                    .decode(t)
                    .map_err(|_| {
                        SourceError::PositionLost(
                            "the saved resume token is not valid base64".into(),
                        )
                    })
            })
            .transpose()
    }

    pub fn read_timestamp(&self) -> Result<Timestamp, SourceError> {
        let dt = chrono::DateTime::parse_from_rfc3339(&self.read_time)
            .map_err(|_| SourceError::PositionLost("the saved read time is not valid".into()))?;
        Ok(Timestamp {
            seconds: dt.timestamp(),
            nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
        })
    }
}

pub fn format_timestamp(ts: &Timestamp) -> String {
    let nanos = u32::try_from(ts.nanos.clamp(0, 999_999_999)).unwrap_or(0);
    chrono::DateTime::from_timestamp(ts.seconds, nanos)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// One mapped collection on the stream.
#[derive(Debug, Clone)]
pub struct Mapping {
    /// The mapping's `collection`, as `Change::collection`.
    pub name: String,
    pub cref: CollectionRef,
}

#[derive(Debug, Default)]
struct TargetState {
    /// Paths of the documents the index holds, when known.
    known: Option<HashSet<Box<str>>>,
    /// Paths re-sent since a `RESET`, until `CURRENT`.
    resetting: Option<HashSet<Box<str>>>,
}

/// See the module docs.
#[derive(Debug)]
pub struct ListenState {
    mappings: Vec<Mapping>,
    targets: Vec<TargetState>,
    /// Paths held across every target's sets.
    tracked: usize,
    max_tracked: usize,
    /// Tracking was given up (too many documents): sets stay unknown.
    overflowed: bool,
    /// Last token of a consistent snapshot, and its read time.
    pub last_token: Option<Vec<u8>>,
    pub last_read_time: Option<Timestamp>,
    /// Whether the last consistent snapshot was turned into a checkpoint.
    emitted_token: Option<Vec<u8>>,
}

impl ListenState {
    /// `known`: whether the sets start known and empty (a fresh sync, whose
    /// snapshot fills them) or unknown (a resumed one).
    pub fn new(mappings: Vec<Mapping>, known: bool, max_tracked: usize) -> Self {
        let targets = mappings
            .iter()
            .map(|_| TargetState {
                known: known.then(HashSet::new),
                resetting: None,
            })
            .collect();
        Self {
            mappings,
            targets,
            tracked: 0,
            max_tracked,
            overflowed: false,
            last_token: None,
            last_read_time: None,
            emitted_token: None,
        }
    }

    pub fn mappings(&self) -> &[Mapping] {
        &self.mappings
    }

    pub fn collections(&self) -> Vec<String> {
        self.mappings.iter().map(|m| m.name.clone()).collect()
    }

    /// Whether every target's document set is known.
    pub fn all_known(&self) -> bool {
        self.targets.iter().all(|t| t.known.is_some())
    }

    /// The snapshot read `path` for mapping `index`.
    pub fn record_snapshot(&mut self, index: usize, path: &str) {
        self.insert(index, path);
    }

    /// Give up tracking: free every set.
    fn overflow(&mut self) {
        if !self.overflowed {
            tracing::warn!(
                max_tracked_documents = self.max_tracked,
                "too many Firestore documents to track: a server reset will resync the sync"
            );
        }
        self.overflowed = true;
        self.tracked = 0;
        for t in &mut self.targets {
            t.known = None;
            t.resetting = None;
        }
    }

    fn insert(&mut self, index: usize, path: &str) {
        if self.overflowed {
            return;
        }
        let Some(t) = self.targets.get_mut(index) else {
            return;
        };
        let set = match (&mut t.resetting, &mut t.known) {
            (Some(set), _) | (None, Some(set)) => set,
            (None, None) => return,
        };
        if set.insert(path.into()) {
            self.tracked += 1;
            if self.tracked > self.max_tracked {
                self.overflow();
            }
        }
    }

    fn remove(&mut self, index: usize, path: &str) {
        let Some(t) = self.targets.get_mut(index) else {
            return;
        };
        for set in [&mut t.resetting, &mut t.known].into_iter().flatten() {
            if set.remove(path) {
                self.tracked = self.tracked.saturating_sub(1);
            }
        }
    }

    /// Target id → mapping index.
    fn index(&self, target_id: i32) -> Option<usize> {
        usize::try_from(target_id)
            .ok()
            .and_then(|t| t.checked_sub(1))
            .filter(|i| *i < self.mappings.len())
    }

    /// Mapping indexes of target ids (unknown ids skipped).
    fn indexes_of(&self, target_ids: &[i32]) -> Vec<usize> {
        target_ids.iter().filter_map(|t| self.index(*t)).collect()
    }

    /// The target ids a change applies to (all when empty).
    fn indexes(&self, target_ids: &[i32]) -> Vec<usize> {
        if target_ids.is_empty() {
            (0..self.mappings.len()).collect()
        } else {
            target_ids.iter().filter_map(|t| self.index(*t)).collect()
        }
    }

    fn change(&self, index: usize, path: &str, op: Op) -> SourceEvent {
        let m = &self.mappings[index];
        SourceEvent::Change(Change {
            collection: m.name.clone(),
            key: vec![m.cref.key(path)],
            op,
        })
    }

    /// An upsert of `doc` for mapping `index`.
    pub fn upsert(&self, index: usize, doc: &Document) -> SourceEvent {
        let path = relative_path(&doc.name);
        let mut fields = document_fields(doc);
        fields.insert(
            PATH_FIELD.into(),
            serde_json::Value::String(path.to_owned()),
        );
        self.change(index, path, Op::Upsert { doc: fields })
    }

    /// Apply one response; returns the events to emit, in order.
    pub fn on_response(&mut self, resp: ListenResponse) -> Result<Vec<SourceEvent>, SourceError> {
        let Some(resp) = resp.response_type else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        match resp {
            ResponseType::DocumentChange(dc) => {
                let Some(doc) = dc.document else {
                    return Ok(out);
                };
                let path = relative_path(&doc.name).to_owned();
                for i in self.indexes_of(&dc.target_ids) {
                    out.push(self.upsert(i, &doc));
                    self.insert(i, &path);
                }
                for i in self.indexes_of(&dc.removed_target_ids) {
                    out.push(self.change(i, &path, Op::Delete));
                    self.remove(i, &path);
                }
            }
            ResponseType::DocumentDelete(d) => {
                self.removed(&d.document, &d.removed_target_ids, &mut out);
            }
            ResponseType::DocumentRemove(d) => {
                self.removed(&d.document, &d.removed_target_ids, &mut out);
            }
            ResponseType::Filter(f) => {
                if let Some(i) = self.index(f.target_id) {
                    let t = &self.targets[i];
                    match (&t.known, &t.resetting) {
                        // A filter during a reset is checked by its CURRENT.
                        (_, Some(_)) => {}
                        (Some(set), None)
                            if i64::try_from(set.len()).ok() == Some(i64::from(f.count)) => {}
                        (Some(set), None) => {
                            return Err(SourceError::PositionLost(format!(
                                "Firestore reports {} documents in `{}` where {} were synced: some deletes were missed",
                                f.count,
                                self.mappings[i].name,
                                set.len()
                            )));
                        }
                        (None, None) => {
                            return Err(SourceError::PositionLost(format!(
                                "Firestore cannot tell which documents of `{}` were deleted since the last checkpoint",
                                self.mappings[i].name
                            )));
                        }
                    }
                }
            }
            ResponseType::TargetChange(tc) => self.on_target_change(tc, &mut out)?,
        }
        Ok(out)
    }

    fn removed(&mut self, name: &str, target_ids: &[i32], out: &mut Vec<SourceEvent>) {
        let path = relative_path(name).to_owned();
        for i in self.indexes_of(target_ids) {
            out.push(self.change(i, &path, Op::Delete));
            self.remove(i, &path);
        }
    }

    fn on_target_change(
        &mut self,
        tc: TargetChange,
        out: &mut Vec<SourceEvent>,
    ) -> Result<(), SourceError> {
        let kind =
            TargetChangeType::try_from(tc.target_change_type).unwrap_or(TargetChangeType::NoChange);
        match kind {
            TargetChangeType::Add => {}
            TargetChangeType::Remove => {
                let cause = tc
                    .cause
                    .map(|c| crate::errors::short(&c.message))
                    .unwrap_or_else(|| "no cause given".into());
                return Err(SourceError::Transient(format!(
                    "Firestore removed a listen target: {cause}"
                )));
            }
            TargetChangeType::Reset => {
                for i in self.indexes(&tc.target_ids) {
                    let t = &mut self.targets[i];
                    if t.known.is_none() {
                        return Err(SourceError::PositionLost(format!(
                            "Firestore re-sent `{}` from scratch and the connector does not know which \
                             documents to delete (restarted since the snapshot, or more than \
                             `stream.max_tracked_documents`)",
                            self.mappings[i].name
                        )));
                    }
                    if let Some(old) = t.resetting.take() {
                        self.tracked = self.tracked.saturating_sub(old.len());
                    }
                    t.resetting = Some(HashSet::new());
                }
            }
            TargetChangeType::Current => {
                for i in self.indexes(&tc.target_ids) {
                    let Some(fresh) = self.targets[i].resetting.take() else {
                        continue;
                    };
                    let old = self.targets[i].known.take().unwrap_or_default();
                    for gone in old.iter().filter(|p| !fresh.contains(*p)) {
                        out.push(self.change(i, gone, Op::Delete));
                    }
                    self.tracked = self.tracked.saturating_sub(old.len());
                    self.targets[i].known = Some(fresh);
                }
            }
            TargetChangeType::NoChange => {}
        }
        // A consistent snapshot of the whole stream: a checkpoint, once no
        // target is in the middle of a reset.
        if tc.target_ids.is_empty()
            && !tc.resume_token.is_empty()
            && let Some(read_time) = tc.read_time
            && self.targets.iter().all(|t| t.resetting.is_none())
        {
            self.last_token = Some(tc.resume_token.clone());
            self.last_read_time = Some(read_time);
            if self.emitted_token.as_ref() != Some(&tc.resume_token) {
                self.emitted_token = Some(tc.resume_token.clone());
                out.push(SourceEvent::Checkpoint(
                    self.checkpoint(Some(&tc.resume_token), &read_time)?,
                ));
            }
        }
        Ok(())
    }

    pub fn checkpoint(
        &self,
        token: Option<&[u8]>,
        read_time: &Timestamp,
    ) -> Result<Checkpoint, SourceError> {
        Checkpoint::new(ListenCheckpoint {
            resume_token: token.map(|t| base64::engine::general_purpose::STANDARD.encode(t)),
            read_time: format_timestamp(read_time),
            collections: self.collections(),
        })
        .map_err(|e| SourceError::Fatal(format!("encoding a checkpoint: {e}")))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::{
        DocumentChange, DocumentDelete, ExistenceFilter, Value, value::ValueType,
    };
    use serde_json::json;

    const ROOT: &str = "projects/p/databases/(default)/documents";

    fn state(known: bool) -> ListenState {
        ListenState::new(
            vec![
                Mapping {
                    name: "products".into(),
                    cref: CollectionRef::parse("products").unwrap(),
                },
                Mapping {
                    name: "**/orders".into(),
                    cref: CollectionRef::parse("**/orders").unwrap(),
                },
            ],
            known,
            100,
        )
    }

    fn doc(path: &str, v: i64) -> Document {
        let mut fields = std::collections::HashMap::new();
        fields.insert(
            "v".into(),
            Value {
                value_type: Some(ValueType::IntegerValue(v)),
            },
        );
        Document {
            name: format!("{ROOT}/{path}"),
            fields,
            create_time: None,
            update_time: None,
        }
    }

    fn change(path: &str, v: i64, targets: &[i32]) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::DocumentChange(DocumentChange {
                document: Some(doc(path, v)),
                target_ids: targets.to_vec(),
                removed_target_ids: vec![],
            })),
        }
    }

    fn target(
        kind: TargetChangeType,
        ids: &[i32],
        token: &[u8],
        read_time: Option<i64>,
    ) -> ListenResponse {
        ListenResponse {
            response_type: Some(ResponseType::TargetChange(TargetChange {
                target_change_type: kind as i32,
                target_ids: ids.to_vec(),
                cause: None,
                resume_token: token.to_vec(),
                read_time: read_time.map(|s| Timestamp {
                    seconds: s,
                    nanos: 0,
                }),
            })),
        }
    }

    fn keys_and_ops(events: &[SourceEvent]) -> Vec<String> {
        events
            .iter()
            .map(|e| match e {
                SourceEvent::Change(c) => format!(
                    "{} {} {}",
                    c.collection,
                    c.key[0].as_str().unwrap(),
                    match &c.op {
                        Op::Upsert { .. } => "upsert",
                        Op::Delete => "delete",
                        Op::Patch { .. } => "patch",
                    }
                ),
                SourceEvent::Checkpoint(_) => "checkpoint".into(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn changes_carry_the_fields_the_path_and_the_mapping_key() {
        let mut s = state(true);
        let ev = s.on_response(change("users/a/orders/o1", 7, &[2])).unwrap();
        match &ev[..] {
            [SourceEvent::Change(c)] => {
                assert_eq!(c.collection, "**/orders");
                assert_eq!(c.key, vec![json!("users/a/orders/o1")]);
                assert_eq!(
                    c.op,
                    Op::Upsert {
                        doc: serde_json::from_value(json!({"v": 7, "_path": "users/a/orders/o1"}))
                            .unwrap()
                    }
                );
            }
            other => panic!("{other:?}"),
        }
        let ev = s.on_response(change("products/p1", 1, &[1])).unwrap();
        assert_eq!(keys_and_ops(&ev), ["products p1 upsert"]);
    }

    #[test]
    fn a_reset_deletes_what_is_not_sent_again() {
        let mut s = state(true);
        for p in ["products/a", "products/b", "products/c"] {
            s.record_snapshot(0, p);
        }
        assert!(
            s.on_response(target(TargetChangeType::Reset, &[1], b"t", None))
                .unwrap()
                .is_empty()
        );
        s.on_response(change("products/a", 2, &[1])).unwrap();
        s.on_response(change("products/d", 1, &[1])).unwrap();
        // No checkpoint while a target is being reset.
        assert!(
            s.on_response(target(TargetChangeType::NoChange, &[], b"t0", Some(5)))
                .unwrap()
                .is_empty()
        );
        let ev = s
            .on_response(target(TargetChangeType::Current, &[1], b"t1", Some(9)))
            .unwrap();
        let mut deleted = keys_and_ops(&ev);
        deleted.sort();
        assert_eq!(deleted, ["products b delete", "products c delete"]);
        let ev = s
            .on_response(target(TargetChangeType::NoChange, &[], b"t2", Some(10)))
            .unwrap();
        assert_eq!(keys_and_ops(&ev), ["checkpoint"]);
        // The same token again: nothing new to save.
        assert!(
            s.on_response(target(TargetChangeType::NoChange, &[], b"t2", Some(10)))
                .unwrap()
                .is_empty()
        );
        // The set is now {a, d}: a second reset with only `d` deletes `a`.
        s.on_response(target(TargetChangeType::Reset, &[], b"", None))
            .unwrap();
        s.on_response(change("products/d", 1, &[1])).unwrap();
        let ev = s
            .on_response(target(TargetChangeType::Current, &[], b"", None))
            .unwrap();
        assert_eq!(keys_and_ops(&ev), ["products a delete"]);
    }

    #[test]
    fn a_reset_without_a_known_set_is_a_lost_position() {
        let mut s = state(false);
        assert!(matches!(
            s.on_response(target(TargetChangeType::Reset, &[1], b"", None)),
            Err(SourceError::PositionLost(_))
        ));
    }

    #[test]
    fn deletes_and_existence_filters() {
        let mut s = state(true);
        s.on_response(change("products/a", 1, &[1])).unwrap();
        s.on_response(change("products/b", 1, &[1])).unwrap();
        let del = ListenResponse {
            response_type: Some(ResponseType::DocumentDelete(DocumentDelete {
                document: format!("{ROOT}/products/b"),
                removed_target_ids: vec![1],
                read_time: None,
            })),
        };
        assert_eq!(
            keys_and_ops(&s.on_response(del).unwrap()),
            ["products b delete"]
        );
        let filter = |count| ListenResponse {
            response_type: Some(ResponseType::Filter(ExistenceFilter {
                target_id: 1,
                count,
                unchanged_names: None,
            })),
        };
        assert!(s.on_response(filter(1)).unwrap().is_empty());
        assert!(matches!(
            s.on_response(filter(3)),
            Err(SourceError::PositionLost(_))
        ));
        assert!(matches!(
            state(false).on_response(filter(0)),
            Err(SourceError::PositionLost(_))
        ));
    }

    #[test]
    fn tracking_gives_up_past_its_cap() {
        let mut s = ListenState::new(
            vec![Mapping {
                name: "products".into(),
                cref: CollectionRef::parse("products").unwrap(),
            }],
            true,
            2,
        );
        for p in ["products/a", "products/b"] {
            s.record_snapshot(0, p);
        }
        assert!(s.all_known());
        s.record_snapshot(0, "products/c");
        assert!(!s.all_known());
        assert!(matches!(
            s.on_response(target(TargetChangeType::Reset, &[1], b"", None)),
            Err(SourceError::PositionLost(_))
        ));
    }

    #[test]
    fn checkpoints_round_trip() {
        let s = state(true);
        let cp = s
            .checkpoint(
                Some(b"tok"),
                &Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 5,
                },
            )
            .unwrap();
        let back: ListenCheckpoint = cp.decode().unwrap();
        assert_eq!(back.collections, ["products", "**/orders"]);
        assert_eq!(back.token_bytes().unwrap().unwrap(), b"tok");
        assert_eq!(
            back.read_timestamp().unwrap(),
            Timestamp {
                seconds: 1_700_000_000,
                nanos: 5
            }
        );
        assert_eq!(back.read_time, "2023-11-14T22:13:20.000000005Z");
    }
}
