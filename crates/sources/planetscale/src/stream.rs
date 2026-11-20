//! VStream events → source events.
//!
//! Per transaction vtgate sends `BEGIN`, a `FIELD` event before the first
//! rows of each table, `ROW` events, the new `VGTID` (every shard's
//! position) and `COMMIT`. Changes are emitted as they come, the checkpoint
//! at `COMMIT`, or at a `VGTID` outside a transaction (heartbeats, other
//! tables' transactions).

use std::collections::HashMap;

use meili_connect_core::{
    Change, Document, Lag, LagReporter, Op, SourceError, SourceEvent, document_id,
};
use serde_json::Value as Json;

use crate::convert::{self, ColumnMeta};
use crate::proto::{FieldEvent, Row, RowEvent, VEvent, VEventType};
use crate::schema::TableSchema;
use crate::vstream::Position;

/// One mapped table while streaming.
#[derive(Debug)]
pub struct TableState {
    pub schema: TableSchema,
    /// For each field of the last FIELD event, its column.
    fields: Vec<ColumnMeta>,
    /// Indexes into `fields` of the key columns, in key order.
    key: Vec<usize>,
    enum_as_index: bool,
}

impl TableState {
    pub fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            fields: Vec::new(),
            key: Vec::new(),
            enum_as_index: false,
        }
    }
}

/// What a batch of events asks of the caller besides the events it emits.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Needs {
    /// Tables whose FIELD event named a column the schema does not have
    /// (DDL since it was loaded): reload them, then call `apply_fields` again.
    pub reload: Vec<String>,
}

/// The decoding state of one stream.
pub struct Decoder {
    database: String,
    pub tables: HashMap<String, TableState>,
    pub position: Position,
    in_txn: bool,
    dirty: bool,
    lag: Option<LagReporter>,
}

impl Decoder {
    pub fn new(
        database: &str,
        tables: HashMap<String, TableState>,
        position: Position,
        lag: Option<LagReporter>,
    ) -> Self {
        Self {
            database: database.to_owned(),
            tables,
            position,
            in_txn: false,
            dirty: false,
            lag,
        }
    }

    fn collection(&self, table: &str) -> String {
        format!("{}.{table}", self.database)
    }

    /// vtgate may qualify table names with the keyspace.
    fn table_name<'a>(&self, name: &'a str) -> &'a str {
        name.strip_prefix(&self.database)
            .and_then(|r| r.strip_prefix('.'))
            .unwrap_or(name)
    }

    /// Turns one event into source events, pushed onto `out`.
    pub fn apply(
        &mut self,
        ev: &VEvent,
        out: &mut Vec<SourceEvent>,
        needs: &mut Needs,
    ) -> Result<(), SourceError> {
        self.report_lag(ev);
        match VEventType::try_from(ev.r#type).unwrap_or(VEventType::Unknown) {
            VEventType::Begin => self.in_txn = true,
            VEventType::Field => {
                if let Some(f) = &ev.field_event
                    && let Some(table) = self.apply_fields(f)
                {
                    needs.reload.push(table);
                }
            }
            VEventType::Row => {
                if let Some(r) = &ev.row_event {
                    self.apply_rows(r, out)?;
                }
            }
            VEventType::Vgtid => {
                if let Some(v) = &ev.vgtid {
                    self.position = Position::from_vgtid(v);
                    self.dirty = true;
                    if !self.in_txn {
                        self.checkpoint(out)?;
                    }
                }
            }
            VEventType::Commit | VEventType::Rollback => {
                self.in_txn = false;
                self.checkpoint(out)?;
            }
            VEventType::Ddl => self.apply_ddl(&ev.statement, out),
            VEventType::Heartbeat | VEventType::Other if !self.in_txn => self.checkpoint(out)?,
            _ => {}
        }
        Ok(())
    }

    fn checkpoint(&mut self, out: &mut Vec<SourceEvent>) -> Result<(), SourceError> {
        if self.dirty {
            out.push(SourceEvent::Checkpoint(self.position.to_checkpoint()?));
            self.dirty = false;
        }
        Ok(())
    }

    fn report_lag(&self, ev: &VEvent) {
        let Some(lag) = &self.lag else { return };
        if ev.timestamp > 0 && ev.current_time > 0 {
            let seconds = (ev.current_time as f64 / 1e9 - ev.timestamp as f64).max(0.0);
            lag.report(Lag {
                seconds: Some(seconds),
                bytes: None,
                records: None,
            });
        }
    }

    /// Maps the fields of a FIELD event to the table's columns. Returns the
    /// table when a field is unknown to the schema (reload it).
    pub fn apply_fields(&mut self, f: &FieldEvent) -> Option<String> {
        let name = self.table_name(&f.table_name).to_owned();
        let state = self.tables.get_mut(&name)?;
        let mut fields = Vec::with_capacity(f.fields.len());
        let mut unknown = false;
        for field in &f.fields {
            match state.schema.column(&field.name) {
                Some(c) => fields.push(c.clone()),
                None => {
                    unknown = true;
                    // Text until the schema is reloaded.
                    fields.push(ColumnMeta::new(
                        &field.name,
                        "varchar",
                        "varchar",
                        true,
                        None,
                    ));
                }
            }
        }
        state.key = state
            .schema
            .key_names()
            .iter()
            .filter_map(|k| fields.iter().position(|c| c.name.eq_ignore_ascii_case(k)))
            .collect();
        state.fields = fields;
        state.enum_as_index = !f.enum_set_string_values;
        unknown.then_some(name)
    }

    fn apply_rows(&mut self, r: &RowEvent, out: &mut Vec<SourceEvent>) -> Result<(), SourceError> {
        let name = self.table_name(&r.table_name).to_owned();
        let collection = self.collection(&name);
        let Some(state) = self.tables.get(&name) else {
            return Ok(());
        };
        if state.fields.is_empty() || state.key.len() != state.schema.key.len() {
            for (i, _) in r.row_changes.iter().enumerate() {
                out.push(SourceEvent::Skipped {
                    collection: collection.clone(),
                    position: format!("{}/{} row {i}", r.keyspace, r.shard),
                    reason: "a row arrived without its columns or primary key".into(),
                });
            }
            return Ok(());
        }
        for change in &r.row_changes {
            let before = change
                .before
                .as_ref()
                .map(|row| decode(state, row))
                .transpose();
            let after = change
                .after
                .as_ref()
                .map(|row| decode(state, row))
                .transpose();
            let (before, after) = match (before, after) {
                (Ok(b), Ok(a)) => (b, a),
                (Err(e), _) | (_, Err(e)) => {
                    out.push(SourceEvent::Skipped {
                        collection: collection.clone(),
                        position: format!("{}/{}", r.keyspace, r.shard),
                        reason: e.to_string(),
                    });
                    continue;
                }
            };
            match (before, after) {
                (Some((old_key, _)), Some((key, doc))) => {
                    if document_id(&old_key) != document_id(&key) {
                        out.push(change_event(&collection, old_key, Op::Delete));
                    }
                    out.push(change_event(&collection, key, Op::Upsert { doc }));
                }
                (None, Some((key, doc))) => {
                    out.push(change_event(&collection, key, Op::Upsert { doc }));
                }
                (Some((key, _)), None) => out.push(change_event(&collection, key, Op::Delete)),
                (None, None) => {}
            }
        }
        Ok(())
    }

    fn apply_ddl(&mut self, statement: &str, out: &mut Vec<SourceEvent>) {
        let words: Vec<String> = statement
            .split_whitespace()
            .take(3)
            .map(|w| w.trim_matches(|c| c == '`' || c == ';').to_owned())
            .collect();
        let target = match words.as_slice() {
            [t, table, ..]
                if t.eq_ignore_ascii_case("truncate") && !table.eq_ignore_ascii_case("table") =>
            {
                Some((true, table.clone()))
            }
            [t, kw, table]
                if t.eq_ignore_ascii_case("truncate") && kw.eq_ignore_ascii_case("table") =>
            {
                Some((true, table.clone()))
            }
            [verb, kw, table]
                if (verb.eq_ignore_ascii_case("alter") || verb.eq_ignore_ascii_case("drop"))
                    && kw.eq_ignore_ascii_case("table") =>
            {
                Some((verb.eq_ignore_ascii_case("drop"), table.clone()))
            }
            _ => None,
        };
        let Some((removes_rows, table)) = target else {
            return;
        };
        let table = table
            .rsplit('.')
            .next()
            .unwrap_or(&table)
            .trim_matches('`')
            .to_owned();
        if !self.tables.contains_key(&table) {
            return;
        }
        let collection = self.collection(&table);
        out.push(if removes_rows {
            SourceEvent::Truncate { collection }
        } else {
            SourceEvent::SchemaChanged { collection }
        });
    }
}

fn change_event(collection: &str, key: Vec<Json>, op: Op) -> SourceEvent {
    SourceEvent::Change(Change {
        collection: collection.to_owned(),
        key,
        op,
    })
}

/// A row image → its key and its document.
fn decode(state: &TableState, row: &Row) -> Result<(Vec<Json>, Document), convert::ConvertError> {
    let cells = row.cells().ok_or_else(|| convert::ConvertError {
        column: String::new(),
        reason: "malformed row".into(),
    })?;
    let mut doc = Document::new();
    let mut values = Vec::with_capacity(cells.len());
    for (col, cell) in state.fields.iter().zip(cells) {
        let v = convert::to_json(col, cell, state.enum_as_index)?;
        values.push(v.clone());
        doc.insert(col.name.clone(), v);
    }
    let key = state
        .key
        .iter()
        .map(|&i| values.get(i).cloned().unwrap_or(Json::Null))
        .collect();
    Ok((key, doc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Field, RowChange, ShardGtid, VGtid};
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema {
            table: "orders".into(),
            columns: vec![
                ColumnMeta::new("id", "bigint", "bigint", false, None),
                ColumnMeta::new("status", "enum", "enum('new','paid')", true, None),
                ColumnMeta::new("total", "decimal", "decimal(10,2)", true, None),
            ],
            key: vec![0],
            estimated_rows: None,
        }
    }

    fn decoder() -> Decoder {
        let mut tables = HashMap::new();
        tables.insert("orders".to_owned(), TableState::new(schema()));
        Decoder::new("shop", tables, Position { shards: vec![] }, None)
    }

    fn row(cells: &[Option<&str>]) -> Row {
        let mut lengths = Vec::new();
        let mut values = Vec::new();
        for c in cells {
            match c {
                Some(s) => {
                    lengths.push(s.len() as i64);
                    values.extend_from_slice(s.as_bytes());
                }
                None => lengths.push(-1),
            }
        }
        Row { lengths, values }
    }

    fn ev(t: VEventType) -> VEvent {
        VEvent {
            r#type: t as i32,
            ..Default::default()
        }
    }

    fn fields(names: &[&str], strings: bool) -> VEvent {
        VEvent {
            r#type: VEventType::Field as i32,
            field_event: Some(FieldEvent {
                table_name: "shop.orders".into(),
                fields: names
                    .iter()
                    .map(|n| Field {
                        name: (*n).into(),
                        r#type: 0,
                    })
                    .collect(),
                enum_set_string_values: strings,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn rows(changes: Vec<RowChange>) -> VEvent {
        VEvent {
            r#type: VEventType::Row as i32,
            row_event: Some(RowEvent {
                table_name: "orders".into(),
                row_changes: changes,
                keyspace: "shop".into(),
                shard: "-80".into(),
            }),
            ..Default::default()
        }
    }

    fn vgtid(gtid: &str) -> VEvent {
        VEvent {
            r#type: VEventType::Vgtid as i32,
            vgtid: Some(VGtid {
                shard_gtids: vec![ShardGtid {
                    keyspace: "shop".into(),
                    shard: "-80".into(),
                    gtid: gtid.into(),
                }],
            }),
            ..Default::default()
        }
    }

    fn run(d: &mut Decoder, events: Vec<VEvent>) -> Vec<SourceEvent> {
        let mut out = Vec::new();
        let mut needs = Needs::default();
        for e in &events {
            d.apply(e, &mut out, &mut needs).unwrap();
        }
        assert_eq!(needs, Needs::default());
        out
    }

    fn upsert(key: i64, doc: Json) -> SourceEvent {
        change_event(
            "shop.orders",
            vec![json!(key)],
            Op::Upsert {
                doc: doc.as_object().unwrap().clone(),
            },
        )
    }

    #[test]
    fn a_transaction_gives_its_changes_then_one_checkpoint() {
        let mut d = decoder();
        let out = run(
            &mut d,
            vec![
                ev(VEventType::Begin),
                fields(&["id", "status", "total"], true),
                rows(vec![
                    RowChange {
                        before: None,
                        after: Some(row(&[Some("1"), Some("new"), Some("9.50")])),
                    },
                    RowChange {
                        before: Some(row(&[Some("2"), Some("new"), None])),
                        after: Some(row(&[Some("3"), Some("paid"), None])),
                    },
                    RowChange {
                        before: Some(row(&[Some("4"), Some("paid"), None])),
                        after: None,
                    },
                ]),
                vgtid("MySQL56/a:1-7"),
                ev(VEventType::Commit),
            ],
        );
        assert_eq!(
            out,
            vec![
                upsert(1, json!({"id": 1, "status": "new", "total": 9.5})),
                // The key changed: the old document goes.
                change_event("shop.orders", vec![json!(2)], Op::Delete),
                upsert(3, json!({"id": 3, "status": "paid", "total": null})),
                change_event("shop.orders", vec![json!(4)], Op::Delete),
                SourceEvent::Checkpoint(d.position.to_checkpoint().unwrap()),
            ]
        );
        assert_eq!(d.position.shards[0].gtid, "MySQL56/a:1-7");
    }

    #[test]
    fn positions_outside_transactions_are_checkpoints_once() {
        let mut d = decoder();
        let out = run(
            &mut d,
            vec![
                vgtid("MySQL56/a:1-8"),
                ev(VEventType::Heartbeat),
                ev(VEventType::Heartbeat),
            ],
        );
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], SourceEvent::Checkpoint(_)));
    }

    #[test]
    fn older_vtgates_send_enum_indexes() {
        let mut d = decoder();
        let out = run(
            &mut d,
            vec![
                fields(&["id", "status", "total"], false),
                rows(vec![RowChange {
                    before: None,
                    after: Some(row(&[Some("5"), Some("2"), Some("1")])),
                }]),
            ],
        );
        assert_eq!(
            out,
            vec![upsert(5, json!({"id": 5, "status": "paid", "total": 1}))]
        );
    }

    #[test]
    fn unknown_columns_ask_for_a_reload_and_other_tables_are_ignored() {
        let mut d = decoder();
        let mut out = Vec::new();
        let mut needs = Needs::default();
        d.apply(
            &fields(&["id", "status", "total", "added"], true),
            &mut out,
            &mut needs,
        )
        .unwrap();
        assert_eq!(needs.reload, vec!["orders".to_owned()]);
        let mut other = rows(vec![RowChange {
            before: None,
            after: Some(row(&[Some("1")])),
        }]);
        if let Some(r) = other.row_event.as_mut() {
            r.table_name = "audit".into();
        }
        d.apply(&other, &mut out, &mut needs).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn rows_before_their_fields_are_skipped_not_guessed() {
        let mut d = decoder();
        let out = run(
            &mut d,
            vec![rows(vec![RowChange {
                before: None,
                after: Some(row(&[Some("1"), None, None])),
            }])],
        );
        assert!(
            matches!(&out[0], SourceEvent::Skipped { collection, .. } if collection == "shop.orders")
        );
    }

    #[test]
    fn ddl_on_a_mapped_table() {
        let mut d = decoder();
        let ddl = |s: &str| VEvent {
            r#type: VEventType::Ddl as i32,
            statement: s.into(),
            ..Default::default()
        };
        let out = run(
            &mut d,
            vec![
                ddl("truncate table `orders`"),
                ddl("TRUNCATE orders"),
                ddl("alter table orders add column x int"),
                ddl("alter table audit add column x int"),
                ddl("create table t (id int)"),
            ],
        );
        let c = "shop.orders".to_owned();
        assert_eq!(
            out,
            vec![
                SourceEvent::Truncate {
                    collection: c.clone()
                },
                SourceEvent::Truncate {
                    collection: c.clone()
                },
                SourceEvent::SchemaChanged { collection: c },
            ]
        );
    }
}
