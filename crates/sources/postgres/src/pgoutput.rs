//! `pgoutput` (protocol version 1) messages → [`SourceEvent`]s.
//!
//! `pgwire-replication` already turns Begin and Commit into their own
//! events; every other message arrives as an `XLogData` payload and is
//! decoded here. Decoding is pure: the caller resolves unknown type OIDs
//! (`Decoded::NeedTypes`) and retries the same message.

use std::collections::HashMap;

use meili_connect_core::{Change, Document, Op, SourceError, SourceEvent};
use serde_json::Value;

use crate::convert::{Oid, TypeRegistry};

/// A mapped table: the collection name to emit (as the user wrote it in
/// the stream mapping) and its key columns in key order.
#[derive(Debug, Clone, PartialEq)]
pub struct Mapped {
    pub name: String,
    pub key: Vec<String>,
}

/// Mapped tables by canonical `schema.table`. Tables absent from the map
/// are not synced.
pub type KeyColumns = HashMap<String, Mapped>;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub type_oid: Oid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub schema: String,
    pub table: String,
    pub collection: String,
    pub columns: Vec<Column>,
    /// Positions of the key columns in `columns`, in key order. `None` when
    /// the collection is not mapped.
    pub key_idx: Option<Vec<usize>>,
}

/// One column of a TupleData.
#[derive(Debug, Clone, PartialEq)]
enum Cell<'a> {
    Null,
    /// TOASTed and unchanged by this UPDATE: the value is not in the WAL.
    Unchanged,
    Text(&'a str),
}

#[derive(Debug, PartialEq)]
pub enum Decoded {
    /// Events to emit, in order (at most two per message).
    Events(Vec<SourceEvent>),
    /// The relation uses types the registry does not know: look them up and
    /// decode the same message again.
    NeedTypes(Vec<Oid>),
    /// A key-changing UPDATE whose new row lacks unchanged TOAST values
    /// (no full old row to take them from): emit `events` (the delete of
    /// the old key), then re-read the row by its new key and upsert it if
    /// it still exists. A later change covers a row that is gone.
    Refetch {
        events: Vec<SourceEvent>,
        row: RowRef,
    },
}

/// A row to re-read by key, with what is needed to build its document.
#[derive(Debug, Clone, PartialEq)]
pub struct RowRef {
    pub schema: String,
    pub table: String,
    pub collection: String,
    pub columns: Vec<Column>,
    /// Key column names and their values in text form, in key order.
    pub key_columns: Vec<(Column, String)>,
    pub key: Vec<Value>,
}

#[derive(Debug, Default)]
pub struct Decoder {
    relations: HashMap<u32, Relation>,
    keys: KeyColumns,
}

impl Decoder {
    pub fn new(keys: KeyColumns) -> Self {
        Self {
            relations: HashMap::new(),
            keys,
        }
    }

    pub fn decode(&mut self, data: &[u8], types: &TypeRegistry) -> Result<Decoded, SourceError> {
        let mut r = Reader::new(data);
        let tag = r.u8()?;
        match tag {
            b'R' => self.relation(&mut r, types),
            b'I' => {
                let rel = self.rel(r.u32()?)?;
                let Some(rel) = rel else {
                    return Ok(Decoded::Events(Vec::new()));
                };
                expect(r.u8()?, b'N', "insert")?;
                let new = tuple(&mut r)?;
                let key = key_of(rel, &new, None, types)?;
                let (doc, _) = row(rel, &new, None, types)?;
                Ok(Decoded::Events(vec![change(rel, key, Op::Upsert { doc })]))
            }
            b'U' => {
                let rel = self.rel(r.u32()?)?;
                let Some(rel) = rel else {
                    return Ok(Decoded::Events(Vec::new()));
                };
                let mut kind = r.u8()?;
                let prev_kind = kind;
                let mut old = None;
                if kind == b'K' || kind == b'O' {
                    old = Some(tuple(&mut r)?);
                    kind = r.u8()?;
                }
                let old_kind = old.as_ref().map(|_| prev_kind);
                expect(kind, b'N', "update")?;
                let new = tuple(&mut r)?;
                update(rel, old.as_deref(), old_kind, &new, types)
            }
            b'D' => {
                let rel = self.rel(r.u32()?)?;
                let Some(rel) = rel else {
                    return Ok(Decoded::Events(Vec::new()));
                };
                let kind = r.u8()?;
                if kind != b'K' && kind != b'O' {
                    return Err(protocol(format!("delete: unexpected tuple kind {kind}")));
                }
                let old = tuple(&mut r)?;
                let key = key_of(rel, &old, None, types)?;
                Ok(Decoded::Events(vec![change(rel, key, Op::Delete)]))
            }
            b'T' => {
                let n = r.i32()?;
                let _options = r.u8()?;
                let mut events = Vec::new();
                for _ in 0..n.max(0) {
                    let id = r.u32()?;
                    if let Some(rel) = self.relations.get(&id)
                        && rel.key_idx.is_some()
                    {
                        events.push(SourceEvent::Truncate {
                            collection: rel.collection.clone(),
                        });
                    }
                }
                Ok(Decoded::Events(events))
            }
            // Type (the relation that uses it triggers our own lookup),
            // Origin, and the Begin/Commit/Message the client already split.
            b'Y' | b'O' | b'B' | b'C' | b'M' => Ok(Decoded::Events(Vec::new())),
            other => Err(protocol(format!(
                "unsupported pgoutput message '{}'",
                char::from(other)
            ))),
        }
    }

    fn rel(&self, id: u32) -> Result<Option<&Relation>, SourceError> {
        let rel = self
            .relations
            .get(&id)
            .ok_or_else(|| protocol(format!("change for unknown relation {id}")))?;
        Ok(rel.key_idx.is_some().then_some(rel))
    }

    fn relation(
        &mut self,
        r: &mut Reader<'_>,
        types: &TypeRegistry,
    ) -> Result<Decoded, SourceError> {
        let id = r.u32()?;
        let namespace = r.cstr()?;
        let name = r.cstr()?;
        let _replica_identity = r.u8()?;
        let n = r.i16()?;
        let mut columns = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n.max(0) {
            let _flags = r.u8()?;
            let col = r.cstr()?.to_owned();
            let type_oid = r.u32()?;
            let _typmod = r.i32()?;
            columns.push(Column {
                name: col,
                type_oid,
            });
        }
        // An empty namespace means pg_catalog.
        let schema = if namespace.is_empty() {
            "pg_catalog"
        } else {
            namespace
        };
        let canonical = format!("{schema}.{name}");
        let mapped = self.keys.get(&canonical);
        let collection = mapped.map_or_else(|| canonical.clone(), |m| m.name.clone());

        let key_idx = match mapped {
            None => None,
            Some(m) => {
                let mut idx = Vec::with_capacity(m.key.len());
                for k in &m.key {
                    let pos = columns.iter().position(|c| &c.name == k).ok_or_else(|| {
                        SourceError::Config(format!(
                            "key column '{k}' of {collection} is not in its replication stream"
                        ))
                    })?;
                    idx.push(pos);
                }
                Some(idx)
            }
        };

        if key_idx.is_some() {
            let mut missing: Vec<Oid> = columns
                .iter()
                .map(|c| c.type_oid)
                .filter(|oid| !types.knows(*oid))
                .collect();
            if !missing.is_empty() {
                missing.sort_unstable();
                missing.dedup();
                return Ok(Decoded::NeedTypes(missing));
            }
        }

        let mapped = key_idx.is_some();
        let previous = self.relations.insert(
            id,
            Relation {
                schema: schema.to_owned(),
                table: name.to_owned(),
                collection: collection.clone(),
                columns,
                key_idx,
            },
        );
        // Postgres sends a Relation message before the first change of each
        // relation in a session and again after every DDL that touches it.
        // Only the second kind is a schema change.
        let changed = previous.is_some_and(|p| {
            self.relations
                .get(&id)
                .is_some_and(|now| now.columns != p.columns || now.collection != p.collection)
        });
        if mapped && changed {
            Ok(Decoded::Events(vec![SourceEvent::SchemaChanged {
                collection,
            }]))
        } else {
            Ok(Decoded::Events(Vec::new()))
        }
    }
}

fn change(rel: &Relation, key: Vec<Value>, op: Op) -> SourceEvent {
    SourceEvent::Change(Change {
        collection: rel.collection.clone(),
        key,
        op,
    })
}

/// UPDATE → Upsert, or Patch when unchanged TOAST columns are missing and
/// the key did not change. A full old row ('O', REPLICA IDENTITY FULL)
/// supplies the missing values, so the result is always an Upsert. A key
/// change emits Delete(old) first and never a Patch for the new key: the
/// new document must be whole, so without a full old row it is re-read.
fn update(
    rel: &Relation,
    old: Option<&[Cell<'_>]>,
    old_kind: Option<u8>,
    new: &[Cell<'_>],
    types: &TypeRegistry,
) -> Result<Decoded, SourceError> {
    let full_old = old.filter(|_| old_kind == Some(b'O'));
    let new_key = key_of(rel, new, old, types)?;
    let (doc, partial) = row(rel, new, full_old, types)?;
    let mut events = Vec::with_capacity(2);
    let mut key_changed = false;
    if let Some(old) = old {
        // 'K' carries the old replica identity (sent only when it changed),
        // 'O' the whole old row (REPLICA IDENTITY FULL, sent on every
        // update). Either way, compare keys.
        let old_key = key_of(rel, old, None, types)?;
        if old_key != new_key {
            key_changed = true;
            events.push(change(rel, old_key, Op::Delete));
        }
    }
    if !partial {
        events.push(change(rel, new_key, Op::Upsert { doc }));
    } else if !key_changed {
        events.push(change(
            rel,
            new_key,
            Op::Patch {
                doc,
                unset: Vec::new(),
            },
        ));
    } else {
        let idx = rel.key_idx.as_deref().unwrap_or(&[]);
        let mut key_columns = Vec::with_capacity(idx.len());
        for &i in idx {
            let text = match (new.get(i), old.and_then(|o| o.get(i))) {
                (Some(Cell::Text(t)), _) | (Some(Cell::Unchanged), Some(Cell::Text(t))) => {
                    (*t).to_owned()
                }
                _ => {
                    return Err(SourceError::Fatal(format!(
                        "{}: key column '{}' is missing from the replication stream",
                        rel.collection, rel.columns[i].name
                    )));
                }
            };
            key_columns.push((rel.columns[i].clone(), text));
        }
        return Ok(Decoded::Refetch {
            events,
            row: RowRef {
                schema: rel.schema.clone(),
                table: rel.table.clone(),
                collection: rel.collection.clone(),
                columns: rel.columns.clone(),
                key_columns,
                key: new_key,
            },
        });
    }
    Ok(Decoded::Events(events))
}

/// The key of a tuple. A key column that is an unchanged TOAST value (only
/// possible for very wide keys) is taken from `fallback`, the old tuple.
fn key_of(
    rel: &Relation,
    cells: &[Cell<'_>],
    fallback: Option<&[Cell<'_>]>,
    types: &TypeRegistry,
) -> Result<Vec<Value>, SourceError> {
    let idx = rel.key_idx.as_deref().unwrap_or(&[]);
    let mut key = Vec::with_capacity(idx.len());
    for &i in idx {
        let col = &rel.columns[i];
        let cell = match cells.get(i) {
            Some(Cell::Unchanged) | None => fallback.and_then(|f| f.get(i)),
            other => other,
        };
        match cell {
            Some(Cell::Text(t)) => key.push(types.to_json(col.type_oid, t)),
            Some(Cell::Null) => {
                return Err(SourceError::Fatal(format!(
                    "{}: key column '{}' is NULL in the replication stream",
                    rel.collection, col.name
                )));
            }
            _ => {
                return Err(SourceError::Fatal(format!(
                    "{}: key column '{}' is missing from the replication stream \
                     (unchanged TOAST value without an old tuple)",
                    rel.collection, col.name
                )));
            }
        }
    }
    if key.is_empty() {
        return Err(SourceError::Fatal(format!(
            "{}: no key columns",
            rel.collection
        )));
    }
    Ok(key)
}

/// (document, partial). `partial` is true when some columns were
/// unchanged TOAST values, absent from `fallback` too (the full old row,
/// when there is one), and so absent from the document.
fn row(
    rel: &Relation,
    cells: &[Cell<'_>],
    fallback: Option<&[Cell<'_>]>,
    types: &TypeRegistry,
) -> Result<(Document, bool), SourceError> {
    if cells.len() != rel.columns.len() {
        return Err(protocol(format!(
            "{}: tuple has {} columns, relation has {}",
            rel.collection,
            cells.len(),
            rel.columns.len()
        )));
    }
    let mut doc = Document::new();
    let mut partial = false;
    for (i, (col, cell)) in rel.columns.iter().zip(cells).enumerate() {
        let cell = match cell {
            Cell::Unchanged => fallback.and_then(|f| f.get(i)).unwrap_or(cell),
            other => other,
        };
        match cell {
            Cell::Null => {
                doc.insert(col.name.clone(), Value::Null);
            }
            Cell::Unchanged => partial = true,
            Cell::Text(t) => {
                doc.insert(col.name.clone(), types.to_json(col.type_oid, t));
            }
        }
    }
    Ok((doc, partial))
}

fn tuple<'a>(r: &mut Reader<'a>) -> Result<Vec<Cell<'a>>, SourceError> {
    let n = r.i16()?;
    let mut cells = Vec::with_capacity(n.max(0) as usize);
    for _ in 0..n.max(0) {
        match r.u8()? {
            b'n' => cells.push(Cell::Null),
            b'u' => cells.push(Cell::Unchanged),
            b't' => {
                let len = r.i32()?;
                let bytes =
                    r.take(usize::try_from(len).map_err(|_| protocol("negative length"))?)?;
                let text = std::str::from_utf8(bytes)
                    .map_err(|_| protocol("column value is not valid UTF-8"))?;
                cells.push(Cell::Text(text));
            }
            b'b' => return Err(protocol("binary tuple data is not enabled")),
            other => return Err(protocol(format!("unknown tuple cell kind {other}"))),
        }
    }
    Ok(cells)
}

fn expect(got: u8, want: u8, what: &str) -> Result<(), SourceError> {
    if got == want {
        Ok(())
    } else {
        Err(protocol(format!(
            "{what}: expected '{}', got '{}'",
            char::from(want),
            char::from(got)
        )))
    }
}

fn protocol(msg: impl Into<String>) -> SourceError {
    SourceError::Fatal(format!("pgoutput: {}", msg.into()))
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], SourceError> {
        if self.buf.len() < n {
            return Err(protocol("truncated message"));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, SourceError> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SourceError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn i16(&mut self) -> Result<i16, SourceError> {
        Ok(i16::from_be_bytes(self.array()?))
    }

    fn i32(&mut self) -> Result<i32, SourceError> {
        Ok(i32::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, SourceError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn cstr(&mut self) -> Result<&'a str, SourceError> {
        let end = self
            .buf
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| protocol("unterminated string"))?;
        let s = std::str::from_utf8(&self.buf[..end]).map_err(|_| protocol("invalid UTF-8"))?;
        self.buf = &self.buf[end + 1..];
        Ok(s)
    }
}

/// Builders for pgoutput messages, shared by the unit tests.
#[cfg(test)]
pub mod wire {
    pub enum C<'a> {
        N,
        U,
        T(&'a str),
    }

    pub fn relation(id: u32, ns: &str, name: &str, cols: &[(&str, u32, bool)]) -> Vec<u8> {
        let mut b = vec![b'R'];
        b.extend(id.to_be_bytes());
        b.extend(ns.as_bytes());
        b.push(0);
        b.extend(name.as_bytes());
        b.push(0);
        b.push(b'd');
        b.extend((cols.len() as i16).to_be_bytes());
        for (n, oid, key) in cols {
            b.push(u8::from(*key));
            b.extend(n.as_bytes());
            b.push(0);
            b.extend(oid.to_be_bytes());
            b.extend((-1i32).to_be_bytes());
        }
        b
    }

    pub fn tuple(b: &mut Vec<u8>, cells: &[C<'_>]) {
        b.extend((cells.len() as i16).to_be_bytes());
        for c in cells {
            match c {
                C::N => b.push(b'n'),
                C::U => b.push(b'u'),
                C::T(s) => {
                    b.push(b't');
                    b.extend((s.len() as i32).to_be_bytes());
                    b.extend(s.as_bytes());
                }
            }
        }
    }

    pub fn insert(id: u32, cells: &[C<'_>]) -> Vec<u8> {
        let mut b = vec![b'I'];
        b.extend(id.to_be_bytes());
        b.push(b'N');
        tuple(&mut b, cells);
        b
    }

    pub fn update(id: u32, old: Option<(u8, &[C<'_>])>, new: &[C<'_>]) -> Vec<u8> {
        let mut b = vec![b'U'];
        b.extend(id.to_be_bytes());
        if let Some((kind, cells)) = old {
            b.push(kind);
            tuple(&mut b, cells);
        }
        b.push(b'N');
        tuple(&mut b, new);
        b
    }

    pub fn delete(id: u32, kind: u8, old: &[C<'_>]) -> Vec<u8> {
        let mut b = vec![b'D'];
        b.extend(id.to_be_bytes());
        b.push(kind);
        tuple(&mut b, old);
        b
    }

    pub fn truncate(ids: &[u32]) -> Vec<u8> {
        let mut b = vec![b'T'];
        b.extend((ids.len() as i32).to_be_bytes());
        b.push(0);
        for id in ids {
            b.extend(id.to_be_bytes());
        }
        b
    }
}

#[cfg(test)]
mod tests {
    use super::wire::{self, C};
    use super::*;
    use serde_json::json;

    const INT4: u32 = 23;
    const TEXT: u32 = 25;

    fn decoder() -> (Decoder, TypeRegistry) {
        let mut keys = KeyColumns::new();
        keys.insert(
            "public.items".into(),
            Mapped {
                name: "public.items".into(),
                key: vec!["id".into()],
            },
        );
        keys.insert(
            "public.pairs".into(),
            Mapped {
                name: "pairs".into(),
                key: vec!["b".into(), "a".into()],
            },
        );
        let mut d = Decoder::new(keys);
        let t = TypeRegistry::new();
        let rel = wire::relation(
            1,
            "public",
            "items",
            &[
                ("id", INT4, true),
                ("title", TEXT, false),
                ("body", TEXT, false),
            ],
        );
        assert_eq!(d.decode(&rel, &t).unwrap(), Decoded::Events(vec![]));
        (d, t)
    }

    fn events(d: &mut Decoder, t: &TypeRegistry, msg: &[u8]) -> Vec<SourceEvent> {
        match d.decode(msg, t).unwrap() {
            Decoded::Events(e) => e,
            other => panic!("unexpected {other:?}"),
        }
    }

    fn ch(collection: &str, key: Vec<Value>, op: Op) -> SourceEvent {
        SourceEvent::Change(Change {
            collection: collection.into(),
            key,
            op,
        })
    }

    fn doc(v: Value) -> Document {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn insert_is_upsert() {
        let (mut d, t) = decoder();
        let ev = events(&mut d, &t, &wire::insert(1, &[C::T("7"), C::T("hi"), C::N]));
        assert_eq!(
            ev,
            vec![ch(
                "public.items",
                vec![json!(7)],
                Op::Upsert {
                    doc: doc(json!({"id": 7, "title": "hi", "body": null}))
                }
            )]
        );
    }

    #[test]
    fn update_with_unchanged_toast_is_patch() {
        let (mut d, t) = decoder();
        let ev = events(
            &mut d,
            &t,
            &wire::update(1, None, &[C::T("7"), C::T("new"), C::U]),
        );
        assert_eq!(
            ev,
            vec![ch(
                "public.items",
                vec![json!(7)],
                Op::Patch {
                    doc: doc(json!({"id": 7, "title": "new"})),
                    unset: vec![]
                }
            )]
        );
        let ev = events(
            &mut d,
            &t,
            &wire::update(1, None, &[C::T("7"), C::T("x"), C::T("y")]),
        );
        assert_eq!(
            ev,
            vec![ch(
                "public.items",
                vec![json!(7)],
                Op::Upsert {
                    doc: doc(json!({"id": 7, "title": "x", "body": "y"}))
                }
            )]
        );
    }

    #[test]
    fn key_change_is_delete_then_upsert() {
        let (mut d, t) = decoder();
        let ev = events(
            &mut d,
            &t,
            &wire::update(
                1,
                Some((b'K', &[C::T("7"), C::N, C::N])),
                &[C::T("8"), C::T("a"), C::T("b")],
            ),
        );
        assert_eq!(
            ev,
            vec![
                ch("public.items", vec![json!(7)], Op::Delete),
                ch(
                    "public.items",
                    vec![json!(8)],
                    Op::Upsert {
                        doc: doc(json!({"id": 8, "title": "a", "body": "b"}))
                    }
                ),
            ]
        );
    }

    #[test]
    fn full_identity_old_tuple_same_key_is_single_upsert() {
        let (mut d, t) = decoder();
        let ev = events(
            &mut d,
            &t,
            &wire::update(
                1,
                Some((b'O', &[C::T("7"), C::T("old"), C::T("b")])),
                &[C::T("7"), C::T("new"), C::T("b")],
            ),
        );
        assert_eq!(ev.len(), 1);
        assert!(matches!(
            &ev[0],
            SourceEvent::Change(Change {
                op: Op::Upsert { .. },
                ..
            })
        ));
    }

    #[test]
    fn key_change_with_toast_refetches_never_patches() {
        let (mut d, t) = decoder();
        let msg = wire::update(
            1,
            Some((b'K', &[C::T("7"), C::N, C::N])),
            &[C::T("8"), C::T("a"), C::U],
        );
        let Decoded::Refetch { events, row } = d.decode(&msg, &t).unwrap() else {
            panic!("expected a refetch")
        };
        assert_eq!(events, vec![ch("public.items", vec![json!(7)], Op::Delete)]);
        assert_eq!(
            (row.schema.as_str(), row.table.as_str()),
            ("public", "items")
        );
        assert_eq!(row.key, vec![json!(8)]);
        assert_eq!(row.key_columns.len(), 1);
        assert_eq!(row.key_columns[0].0.name, "id");
        assert_eq!(row.key_columns[0].1, "8");
        assert_eq!(row.columns.len(), 3);
    }

    #[test]
    fn full_old_row_fills_unchanged_toast() {
        let (mut d, t) = decoder();
        // Key change under REPLICA IDENTITY FULL: the old row has the value.
        let ev = events(
            &mut d,
            &t,
            &wire::update(
                1,
                Some((b'O', &[C::T("7"), C::T("old"), C::T("long")])),
                &[C::T("8"), C::T("a"), C::U],
            ),
        );
        assert_eq!(
            ev,
            vec![
                ch("public.items", vec![json!(7)], Op::Delete),
                ch(
                    "public.items",
                    vec![json!(8)],
                    Op::Upsert {
                        doc: doc(json!({"id": 8, "title": "a", "body": "long"}))
                    }
                ),
            ]
        );
        // Same key: a whole row too, instead of a Patch.
        let ev = events(
            &mut d,
            &t,
            &wire::update(
                1,
                Some((b'O', &[C::T("8"), C::T("a"), C::T("long")])),
                &[C::T("8"), C::T("b"), C::U],
            ),
        );
        assert_eq!(
            ev,
            vec![ch(
                "public.items",
                vec![json!(8)],
                Op::Upsert {
                    doc: doc(json!({"id": 8, "title": "b", "body": "long"}))
                }
            )]
        );
    }

    #[test]
    fn delete_uses_old_key() {
        let (mut d, t) = decoder();
        let ev = events(&mut d, &t, &wire::delete(1, b'K', &[C::T("9"), C::N, C::N]));
        assert_eq!(ev, vec![ch("public.items", vec![json!(9)], Op::Delete)]);
    }

    #[test]
    fn composite_key_in_key_order() {
        let (mut d, t) = decoder();
        let rel = wire::relation(
            2,
            "public",
            "pairs",
            &[("a", INT4, true), ("b", TEXT, true), ("v", TEXT, false)],
        );
        events(&mut d, &t, &rel);
        let ev = events(
            &mut d,
            &t,
            &wire::insert(2, &[C::T("1"), C::T("x"), C::T("v")]),
        );
        assert!(
            matches!(&ev[0], SourceEvent::Change(c) if c.key == vec![json!("x"), json!(1)] && c.collection == "pairs")
        );
    }

    #[test]
    fn truncate_and_unmapped() {
        let (mut d, t) = decoder();
        let other = wire::relation(3, "public", "other", &[("id", INT4, true)]);
        events(&mut d, &t, &other);
        assert_eq!(events(&mut d, &t, &wire::insert(3, &[C::T("1")])), vec![]);
        assert_eq!(
            events(&mut d, &t, &wire::truncate(&[1, 3])),
            vec![SourceEvent::Truncate {
                collection: "public.items".into()
            }]
        );
    }

    #[test]
    fn relation_change_is_schema_changed() {
        let (mut d, t) = decoder();
        // Same relation again: not a change.
        let same = wire::relation(
            1,
            "public",
            "items",
            &[
                ("id", INT4, true),
                ("title", TEXT, false),
                ("body", TEXT, false),
            ],
        );
        assert_eq!(events(&mut d, &t, &same), vec![]);
        let altered = wire::relation(
            1,
            "public",
            "items",
            &[("id", INT4, true), ("title", TEXT, false)],
        );
        assert_eq!(
            events(&mut d, &t, &altered),
            vec![SourceEvent::SchemaChanged {
                collection: "public.items".into()
            }]
        );
        let ev = events(&mut d, &t, &wire::insert(1, &[C::T("1"), C::T("t")]));
        assert!(
            matches!(&ev[0], SourceEvent::Change(c) if matches!(&c.op, Op::Upsert { doc } if doc.len() == 2))
        );
    }

    #[test]
    fn unknown_types_are_requested() {
        let (mut d, t) = decoder();
        let rel = wire::relation(
            1,
            "public",
            "items",
            &[("id", INT4, true), ("e", 99_999, false)],
        );
        assert_eq!(
            d.decode(&rel, &t).unwrap(),
            Decoded::NeedTypes(vec![99_999])
        );
        let mut t2 = t.clone();
        t2.insert(99_999, crate::convert::TypeKind::Text);
        assert!(matches!(d.decode(&rel, &t2).unwrap(), Decoded::Events(_)));
    }

    #[test]
    fn errors() {
        let (mut d, t) = decoder();
        assert!(matches!(
            d.decode(&wire::insert(42, &[C::T("1")]), &t),
            Err(SourceError::Fatal(_))
        ));
        assert!(d.decode(&[b'I', 0, 0], &t).is_err());
        assert!(d.decode(&wire::insert(1, &[C::N, C::N, C::N]), &t).is_err());
        let missing_key = wire::relation(1, "public", "items", &[("title", TEXT, false)]);
        assert!(matches!(
            d.decode(&missing_key, &t),
            Err(SourceError::Config(_))
        ));
    }
}
