//! Binlog row images → `Change`s.

use meili_connect_core::{Change, Document, Op, SourceError};
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use serde_json::Value as Json;

use crate::convert::{ConvertError, binlog_to_json, data_type_from_binlog};
use crate::schema::{ColumnMeta, TableSchema};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    Insert,
    Update,
    Delete,
}

/// A mapped table as one binlog `TABLE_MAP` event lays it out: one
/// `ColumnMeta` per binlog column position.
#[derive(Debug, Clone, PartialEq)]
pub struct BinlogTable {
    pub collection: String,
    pub columns: Vec<ColumnMeta>,
    /// Positions (in `columns`) of the primary key, in key order.
    pub key: Vec<usize>,
}

impl BinlogTable {
    /// Aligns the cached schema with a table map.
    ///
    /// `names` are the column names carried by the table map itself
    /// (`binlog_row_metadata=FULL`); they are authoritative because they
    /// describe the row as it was written, even when replaying events from
    /// before a later `ALTER`. Without them the current schema is used by
    /// ordinal position, which is right for the common "column added at
    /// the end" case and refused when the binlog has more columns than the
    /// table now has.
    pub fn align(
        schema: &TableSchema,
        types: &[ColumnType],
        names: Option<Vec<String>>,
    ) -> Result<Self, SourceError> {
        let collection = schema.collection();
        let columns: Vec<ColumnMeta> = match names {
            Some(names) if names.len() == types.len() => names
                .into_iter()
                .zip(types)
                .map(|(name, ct)| {
                    schema.column(&name).cloned().unwrap_or_else(|| {
                        let dt = data_type_from_binlog(*ct);
                        ColumnMeta::new(name, dt, dt, None, true)
                    })
                })
                .collect(),
            _ if types.len() <= schema.columns.len() => schema.columns[..types.len()].to_vec(),
            _ => {
                return Err(SourceError::Config(format!(
                    "binlog rows of `{collection}` have {} columns but the table now has {}; \
                     set binlog_row_metadata=FULL so columns can be matched by name",
                    types.len(),
                    schema.columns.len()
                )));
            }
        };
        let mut key = Vec::with_capacity(schema.key.len());
        for &k in &schema.key {
            let name = &schema.columns[k].name;
            match columns.iter().position(|c| &c.name == name) {
                Some(pos) => key.push(pos),
                None => {
                    return Err(SourceError::Config(format!(
                        "binlog rows of `{collection}` lack primary key column `{name}`"
                    )));
                }
            }
        }
        if key.is_empty() {
            return Err(SourceError::Config(format!(
                "`{collection}` has no primary key"
            )));
        }
        Ok(Self {
            collection,
            columns,
            key,
        })
    }

    /// One full row image → a document. `values` holds only the columns
    /// present in the image; anything but a full image is refused.
    pub fn row_to_doc(&self, values: Vec<BinlogValue<'_>>) -> Result<Document, SourceError> {
        if values.len() != self.columns.len() {
            return Err(SourceError::Config(format!(
                "binlog row image of `{}` has {} of {} columns; set binlog_row_image=FULL",
                self.collection,
                values.len(),
                self.columns.len()
            )));
        }
        let mut doc = Document::new();
        for (col, v) in self.columns.iter().zip(values) {
            let json = binlog_to_json(col, v).map_err(|e| match e {
                ConvertError::PartialJson(_) => SourceError::Config(e.to_string()),
                ConvertError::Charset { .. } => {
                    SourceError::Config(format!("`{}`: {e}", self.collection))
                }
                ConvertError::Invalid { .. } => {
                    SourceError::Fatal(format!("`{}`: {e}", self.collection))
                }
            })?;
            doc.insert(col.name.clone(), json);
        }
        Ok(doc)
    }

    pub fn key_of(&self, doc: &Document) -> Vec<Json> {
        self.key
            .iter()
            .map(|&i| {
                doc.get(&self.columns[i].name)
                    .cloned()
                    .unwrap_or(Json::Null)
            })
            .collect()
    }

    /// The changes for one row of a rows event. An update that changes the
    /// primary key becomes a delete of the old document and an upsert of
    /// the new one.
    pub fn changes(
        &self,
        kind: RowKind,
        before: Option<Document>,
        after: Option<Document>,
    ) -> Result<Vec<Change>, SourceError> {
        let missing = |what: &str| {
            SourceError::Fatal(format!(
                "{kind:?} row event on `{}` without {what} image",
                self.collection
            ))
        };
        let change = |key: Vec<Json>, op: Op| Change {
            collection: self.collection.clone(),
            key,
            op,
        };
        Ok(match kind {
            RowKind::Insert => {
                let doc = after.ok_or_else(|| missing("an after"))?;
                vec![change(self.key_of(&doc), Op::Upsert { doc })]
            }
            RowKind::Delete => {
                let doc = before.ok_or_else(|| missing("a before"))?;
                vec![change(self.key_of(&doc), Op::Delete)]
            }
            RowKind::Update => {
                let new = after.ok_or_else(|| missing("an after"))?;
                let new_key = self.key_of(&new);
                match before {
                    Some(old) => {
                        let old_key = self.key_of(&old);
                        if old_key != new_key {
                            vec![
                                change(old_key, Op::Delete),
                                change(new_key, Op::Upsert { doc: new }),
                            ]
                        } else {
                            vec![change(new_key, Op::Upsert { doc: new })]
                        }
                    }
                    None => vec![change(new_key, Op::Upsert { doc: new })],
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value;
    use serde_json::json;

    fn schema() -> TableSchema {
        TableSchema {
            database: "shop".into(),
            table: "items".into(),
            columns: vec![
                ColumnMeta::new("id", "int", "int unsigned", None, false),
                ColumnMeta::new("size", "enum", "enum('s','m','l')", Some("utf8mb4"), true),
                ColumnMeta::new(
                    "tags",
                    "set",
                    "set('new','sale','hot')",
                    Some("utf8mb4"),
                    true,
                ),
                ColumnMeta::new("name", "varchar", "varchar(20)", Some("utf8mb4"), true),
            ],
            key: vec![0],
        }
    }

    fn types() -> Vec<ColumnType> {
        vec![
            ColumnType::MYSQL_TYPE_LONG,
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_STRING,
            ColumnType::MYSQL_TYPE_VARCHAR,
        ]
    }

    fn row(id: i64, size: i64, tags: u8, name: &str) -> Vec<BinlogValue<'static>> {
        vec![
            BinlogValue::Value(Value::Int(id)),
            BinlogValue::Value(Value::Int(size)),
            BinlogValue::Value(Value::Bytes(vec![tags])),
            BinlogValue::Value(Value::Bytes(name.as_bytes().to_vec())),
        ]
    }

    #[test]
    fn enum_and_set_are_mapped_through_metadata() {
        let t = BinlogTable::align(&schema(), &types(), None).unwrap();
        let doc = t.row_to_doc(row(7, 2, 0b011, "shirt")).unwrap();
        assert_eq!(
            serde_json::Value::Object(doc),
            json!({"id": 7, "size": "m", "tags": "new,sale", "name": "shirt"})
        );
    }

    #[test]
    fn insert_update_delete() {
        let t = BinlogTable::align(&schema(), &types(), None).unwrap();
        let a = t.row_to_doc(row(1, 1, 0, "a")).unwrap();
        let b = t.row_to_doc(row(1, 3, 4, "b")).unwrap();

        let ins = t.changes(RowKind::Insert, None, Some(a.clone())).unwrap();
        assert_eq!(ins.len(), 1);
        assert_eq!(ins[0].collection, "shop.items");
        assert_eq!(ins[0].key, vec![json!(1)]);
        assert!(matches!(ins[0].op, Op::Upsert { .. }));

        let upd = t
            .changes(RowKind::Update, Some(a.clone()), Some(b.clone()))
            .unwrap();
        assert_eq!(upd.len(), 1);
        assert_eq!(upd[0].op, Op::Upsert { doc: b });

        let del = t.changes(RowKind::Delete, Some(a), None).unwrap();
        assert_eq!(del[0].op, Op::Delete);
        assert_eq!(del[0].key, vec![json!(1)]);
    }

    #[test]
    fn key_change_is_delete_then_upsert() {
        let t = BinlogTable::align(&schema(), &types(), None).unwrap();
        let old = t.row_to_doc(row(1, 1, 0, "a")).unwrap();
        let new = t.row_to_doc(row(2, 1, 0, "a")).unwrap();
        let ch = t
            .changes(RowKind::Update, Some(old), Some(new.clone()))
            .unwrap();
        assert_eq!(ch.len(), 2);
        assert_eq!(ch[0].key, vec![json!(1)]);
        assert_eq!(ch[0].op, Op::Delete);
        assert_eq!(ch[1].key, vec![json!(2)]);
        assert_eq!(ch[1].op, Op::Upsert { doc: new });
    }

    #[test]
    fn composite_keys_in_key_order() {
        let mut s = schema();
        s.key = vec![3, 0];
        let t = BinlogTable::align(&s, &types(), None).unwrap();
        let d = t.row_to_doc(row(5, 1, 0, "k")).unwrap();
        assert_eq!(t.key_of(&d), vec![json!("k"), json!(5)]);
    }

    #[test]
    fn alignment_rules() {
        // Fewer binlog columns (column added since): prefix.
        let t = BinlogTable::align(&schema(), &types()[..2], None).unwrap();
        assert_eq!(t.columns.len(), 2);
        // More binlog columns than the table has: refused without names.
        let mut more = types();
        more.push(ColumnType::MYSQL_TYPE_LONG);
        assert!(matches!(
            BinlogTable::align(&schema(), &more, None),
            Err(SourceError::Config(_))
        ));
        // With names (FULL metadata): matched by name, unknown ones typed
        // from the binlog.
        let names = vec!["id", "size", "tags", "name", "gone"]
            .into_iter()
            .map(String::from)
            .collect();
        let t = BinlogTable::align(&schema(), &more, Some(names)).unwrap();
        assert_eq!(t.columns[4].name, "gone");
        assert_eq!(t.columns[4].data_type, "int");
        assert_eq!(t.columns[1].members, vec!["s", "m", "l"]);
        // No primary key.
        let mut nokey = schema();
        nokey.key.clear();
        assert!(BinlogTable::align(&nokey, &types(), None).is_err());
    }

    #[test]
    fn undecodable_charset_is_a_config_error_naming_column_and_charset() {
        let mut s = schema();
        s.columns[3] = ColumnMeta::new("name", "varchar", "varchar(20)", Some("swe7"), true);
        let t = BinlogTable::align(&s, &types(), None).unwrap();
        let err = t.row_to_doc(row(1, 1, 0, "a")).unwrap_err();
        assert!(
            matches!(&err, SourceError::Config(m) if m.contains("`name`") && m.contains("`swe7`") && m.contains("shop.items")),
            "{err:?}"
        );
    }

    #[test]
    fn partial_images_are_refused() {
        let t = BinlogTable::align(&schema(), &types(), None).unwrap();
        let mut r = row(1, 1, 0, "a");
        r.pop();
        assert!(matches!(t.row_to_doc(r), Err(SourceError::Config(_))));
    }
}
