//! Table shapes read from `information_schema`.

use std::collections::BTreeMap;

use meili_connect_core::{CollectionInfo, FieldInfo, SourceError};
use mysql_async::Conn;
use mysql_async::prelude::Queryable;

use crate::error::map_err;

/// One column as `information_schema.COLUMNS` describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    /// Lowercase `DATA_TYPE` (`int`, `varchar`, `enum`, `json`…).
    pub data_type: String,
    /// Full `COLUMN_TYPE` (`int unsigned`, `enum('a','b')`…).
    pub column_type: String,
    pub unsigned: bool,
    /// Lowercase `CHARACTER_SET_NAME`; `None` for non-character columns.
    pub charset: Option<String>,
    /// Members of an `ENUM` or `SET`, in declaration order.
    pub members: Vec<String>,
    pub nullable: bool,
}

impl ColumnMeta {
    pub fn new(
        name: impl Into<String>,
        data_type: &str,
        column_type: &str,
        charset: Option<&str>,
        nullable: bool,
    ) -> Self {
        let data_type = data_type.to_ascii_lowercase();
        let column_type_lc = column_type.to_ascii_lowercase();
        let members = if data_type == "enum" || data_type == "set" {
            parse_members(column_type)
        } else {
            Vec::new()
        };
        Self {
            name: name.into(),
            unsigned: column_type_lc.contains("unsigned"),
            data_type,
            column_type: column_type.to_owned(),
            charset: charset.map(str::to_ascii_lowercase),
            members,
            nullable,
        }
    }
}

/// A mapped table: columns in ordinal order and primary key positions in
/// key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnMeta>,
    /// Indexes into `columns`, in primary key order.
    pub key: Vec<usize>,
}

impl TableSchema {
    pub fn collection(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    pub fn column(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.name == name)
    }
}

/// `enum('a','it''s','b\\c')` → `["a", "it's", "b\c"]`.
pub fn parse_members(column_type: &str) -> Vec<String> {
    let Some(open) = column_type.find('(') else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut chars = column_type[open + 1..].chars().peekable();
    loop {
        // Skip to the next opening quote, or stop at the closing paren.
        let mut found = false;
        for c in chars.by_ref() {
            if c == '\'' {
                found = true;
                break;
            }
            if c == ')' {
                return out;
            }
        }
        if !found {
            return out;
        }
        let mut cur = String::new();
        while let Some(c) = chars.next() {
            match c {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    cur.push('\'');
                }
                '\'' => break,
                '\\' => {
                    if let Some(n) = chars.next() {
                        cur.push(n);
                    }
                }
                c => cur.push(c),
            }
        }
        out.push(cur);
    }
}

/// `"db.table"` → `("db", "table")`.
pub fn split_collection(collection: &str) -> Result<(String, String), SourceError> {
    match collection.split_once('.') {
        Some((db, table)) if !db.is_empty() && !table.is_empty() => {
            Ok((db.to_owned(), table.to_owned()))
        }
        _ => Err(SourceError::Config(format!(
            "mysql collection `{collection}` must be named `database.table`"
        ))),
    }
}

/// Backtick-quotes an identifier.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub const SYSTEM_SCHEMAS: [&str; 4] = ["mysql", "information_schema", "performance_schema", "sys"];

type ColumnRow = (
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    String,
);

/// Loads every base table of the given databases (all user databases when
/// `None`), keyed by `(database, table)`.
pub async fn load_tables(
    conn: &mut Conn,
    database: Option<&str>,
) -> Result<BTreeMap<(String, String), (TableSchema, Option<u64>)>, SourceError> {
    let schema_filter = match database {
        Some(_) => "t.TABLE_SCHEMA = ?".to_owned(),
        None => format!(
            "t.TABLE_SCHEMA NOT IN ({})",
            SYSTEM_SCHEMAS
                .iter()
                .map(|s| format!("'{s}'"))
                .collect::<Vec<_>>()
                .join(",")
        ),
    };
    let params: Vec<mysql_async::Value> = database.map(|d| d.into()).into_iter().collect();

    let col_sql = format!(
        "SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, \
                c.CHARACTER_SET_NAME, c.IS_NULLABLE \
         FROM information_schema.COLUMNS c \
         JOIN information_schema.TABLES t \
           ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
         WHERE t.TABLE_TYPE = 'BASE TABLE' AND {schema_filter} \
         ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION"
    );
    let rows: Vec<ColumnRow> = conn.exec(col_sql, params.clone()).await.map_err(map_err)?;

    let key_sql = format!(
        "SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME \
         FROM information_schema.KEY_COLUMN_USAGE k \
         JOIN information_schema.TABLES t \
           ON t.TABLE_SCHEMA = k.TABLE_SCHEMA AND t.TABLE_NAME = k.TABLE_NAME \
         WHERE k.CONSTRAINT_NAME = 'PRIMARY' AND {schema_filter} \
         ORDER BY k.TABLE_SCHEMA, k.TABLE_NAME, k.ORDINAL_POSITION"
    );
    let keys: Vec<(String, String, String)> =
        conn.exec(key_sql, params.clone()).await.map_err(map_err)?;

    let est_sql = format!(
        "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_ROWS FROM information_schema.TABLES t \
         WHERE t.TABLE_TYPE = 'BASE TABLE' AND {schema_filter}"
    );
    let estimates: Vec<(String, String, Option<u64>)> =
        conn.exec(est_sql, params).await.map_err(map_err)?;

    let mut out: BTreeMap<(String, String), (TableSchema, Option<u64>)> = BTreeMap::new();
    for (db, table, name, data_type, column_type, charset, nullable) in rows {
        let entry = out.entry((db.clone(), table.clone())).or_insert_with(|| {
            (
                TableSchema {
                    database: db,
                    table,
                    columns: Vec::new(),
                    key: Vec::new(),
                },
                None,
            )
        });
        entry.0.columns.push(ColumnMeta::new(
            name,
            &data_type,
            &column_type,
            charset.as_deref(),
            nullable.eq_ignore_ascii_case("YES"),
        ));
    }
    for (db, table, column) in keys {
        if let Some((schema, _)) = out.get_mut(&(db, table))
            && let Some(idx) = schema.columns.iter().position(|c| c.name == column)
        {
            schema.key.push(idx);
        }
    }
    for (db, table, rows) in estimates {
        if let Some((_, est)) = out.get_mut(&(db, table)) {
            *est = rows;
        }
    }
    Ok(out)
}

/// Loads one table; `None` when it does not exist.
pub async fn load_table(
    conn: &mut Conn,
    database: &str,
    table: &str,
) -> Result<Option<TableSchema>, SourceError> {
    let rows: Vec<(String, String, String, Option<String>, String)> = conn
        .exec(
            "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME, IS_NULLABLE \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .map_err(map_err)?;
    if rows.is_empty() {
        return Ok(None);
    }
    let columns: Vec<ColumnMeta> = rows
        .into_iter()
        .map(|(name, dt, ct, cs, nullable)| {
            ColumnMeta::new(
                name,
                &dt,
                &ct,
                cs.as_deref(),
                nullable.eq_ignore_ascii_case("YES"),
            )
        })
        .collect();
    let key_names: Vec<String> = conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION",
            (database, table),
        )
        .await
        .map_err(map_err)?;
    let key = key_names
        .iter()
        .filter_map(|k| columns.iter().position(|c| &c.name == k))
        .collect();
    Ok(Some(TableSchema {
        database: database.to_owned(),
        table: table.to_owned(),
        columns,
        key,
    }))
}

pub fn collection_info(schema: &TableSchema, estimated_rows: Option<u64>) -> CollectionInfo {
    CollectionInfo {
        name: schema.collection(),
        key: schema
            .key
            .iter()
            .map(|&i| schema.columns[i].name.clone())
            .collect(),
        fields: schema
            .columns
            .iter()
            .map(|c| FieldInfo {
                name: c.name.clone(),
                source_type: c.column_type.clone(),
                nullable: c.nullable,
            })
            .collect(),
        estimated_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enum_members_parse_with_escapes() {
        assert_eq!(parse_members("enum('a','b')"), vec!["a", "b"]);
        assert_eq!(
            parse_members("enum('it''s','x,y','b\\\\c','')"),
            vec!["it's", "x,y", "b\\c", ""]
        );
        assert_eq!(parse_members("set('read','write')"), vec!["read", "write"]);
        assert!(parse_members("int").is_empty());
    }

    #[test]
    fn column_meta_flags() {
        let c = ColumnMeta::new("n", "INT", "int unsigned", None, false);
        assert!(c.unsigned);
        assert_eq!(c.data_type, "int");
        let e = ColumnMeta::new("e", "enum", "enum('x','y')", Some("UTF8MB4"), true);
        assert_eq!(e.members, vec!["x", "y"]);
        assert_eq!(e.charset.as_deref(), Some("utf8mb4"));
        assert!(e.nullable);
    }

    #[test]
    fn collection_names() {
        assert_eq!(
            split_collection("shop.orders").unwrap(),
            ("shop".into(), "orders".into())
        );
        assert!(matches!(
            split_collection("orders"),
            Err(SourceError::Config(_))
        ));
        assert!(split_collection(".x").is_err());
        assert_eq!(quote_ident("we`ird"), "`we``ird`");
    }
}
