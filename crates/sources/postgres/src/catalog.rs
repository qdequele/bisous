//! Catalog queries: tables, keys, columns, types, slots, publications.

use std::collections::{BTreeSet, HashMap};

use meili_connect_core::SourceError;

use crate::conn::Sql;
use crate::convert::{self, Oid, TypeRegistry};
use crate::ident;

#[derive(Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub schema: String,
    pub table: String,
    /// `r` table, `p` partitioned table.
    pub relkind: String,
    /// `d` default (primary key), `i` index, `f` full, `n` nothing.
    pub replica_identity: String,
    pub reltuples: f64,
    pub primary_key: Vec<String>,
    pub replica_identity_index: Vec<String>,
    pub columns: Vec<ColumnInfo>,
    pub can_select: bool,
    pub is_owner: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: Oid,
    pub type_name: String,
    pub nullable: bool,
}

impl TableInfo {
    pub fn collection(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    /// The columns that identify a row in the replication stream: the
    /// replica identity. `None` when there is none we can use.
    pub fn key(&self) -> Option<Vec<String>> {
        let key = match self.replica_identity.as_str() {
            // DEFAULT and FULL: the primary key (FULL sends the whole old
            // row, which contains it).
            "d" | "f" => self.primary_key.clone(),
            "i" => self.replica_identity_index.clone(),
            _ => Vec::new(),
        };
        (!key.is_empty()).then_some(key)
    }

    pub fn estimated_rows(&self) -> Option<u64> {
        // -1 (PG 14+) means never analyzed.
        (self.reltuples >= 0.0).then_some(self.reltuples as u64)
    }
}

pub async fn server_version(sql: &Sql) -> Result<i32, SourceError> {
    let rows = sql
        .query("SELECT current_setting('server_version_num')::int4", &[])
        .await?;
    let row = rows
        .first()
        .ok_or_else(|| SourceError::Fatal("server_version_num returned no row".into()))?;
    col::<i32>(row, 0)
}

/// Tables matching `filter` (`None` = every user table).
pub async fn tables(
    sql: &Sql,
    version: i32,
    filter: Option<&[(String, String)]>,
) -> Result<Vec<TableInfo>, SourceError> {
    let generated = if version >= 120_000 {
        "AND a.attgenerated = ''"
    } else {
        ""
    };
    let key_agg = |flag: &str| {
        format!(
            "COALESCE((SELECT array_agg(a.attname::text ORDER BY k.ord)
               FROM pg_index i
               CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
               WHERE i.indrelid = c.oid AND i.{flag}), '{{}}')"
        )
    };
    let col_agg = |expr: &str, ty: &str| {
        format!(
            "COALESCE((SELECT array_agg({expr} ORDER BY a.attnum) FROM pg_attribute a
               WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped {generated}),
               '{{}}'::{ty}[])"
        )
    };
    let where_clause = match filter {
        Some(_) => "(n.nspname, c.relname) IN (SELECT * FROM unnest($1::text[], $2::text[]))",
        None => {
            "n.nspname NOT IN ('pg_catalog', 'information_schema')
             AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'
             AND NOT c.relispartition"
        }
    };
    let query = format!(
        "SELECT n.nspname::text, c.relname::text, c.relkind::text, c.relreplident::text,
                c.reltuples::float8,
                {pk}, {ri},
                {names}, {oids}, {types}, {notnull},
                has_table_privilege(c.oid, 'SELECT'),
                pg_has_role(c.relowner, 'USAGE')
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relkind IN ('r', 'p') AND {where_clause}
         ORDER BY 1, 2",
        pk = key_agg("indisprimary"),
        ri = key_agg("indisreplident"),
        names = col_agg("a.attname::text", "text"),
        oids = col_agg("a.atttypid", "oid"),
        types = col_agg("format_type(a.atttypid, a.atttypmod)", "text"),
        notnull = col_agg("a.attnotnull", "bool"),
    );
    let rows = match filter {
        Some(list) => {
            let schemas: Vec<&str> = list.iter().map(|(s, _)| s.as_str()).collect();
            let names: Vec<&str> = list.iter().map(|(_, t)| t.as_str()).collect();
            sql.query(&query, &[&schemas, &names]).await?
        }
        None => sql.query(&query, &[]).await?,
    };
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let names: Vec<String> = col(&row, 7)?;
        let oids: Vec<Oid> = col(&row, 8)?;
        let types: Vec<String> = col(&row, 9)?;
        let notnull: Vec<bool> = col(&row, 10)?;
        let columns = names
            .into_iter()
            .zip(oids)
            .zip(types)
            .zip(notnull)
            .map(|(((name, type_oid), type_name), notnull)| ColumnInfo {
                name,
                type_oid,
                type_name,
                nullable: !notnull,
            })
            .collect();
        out.push(TableInfo {
            schema: col(&row, 0)?,
            table: col(&row, 1)?,
            relkind: col(&row, 2)?,
            replica_identity: col(&row, 3)?,
            reltuples: col(&row, 4)?,
            primary_key: col(&row, 5)?,
            replica_identity_index: col(&row, 6)?,
            columns,
            can_select: col(&row, 11)?,
            is_owner: col(&row, 12)?,
        });
    }
    Ok(out)
}

/// Teach `registry` every type in `oids` (and, transitively, their element
/// and base types).
pub async fn resolve_types(
    sql: &Sql,
    registry: &mut TypeRegistry,
    oids: impl IntoIterator<Item = Oid>,
) -> Result<(), SourceError> {
    let mut wanted: BTreeSet<Oid> = oids.into_iter().filter(|o| !registry.knows(*o)).collect();
    // Element/base types first, so classification of an array of an enum
    // sees the enum. Bounded: types nest only a few levels.
    for _ in 0..8 {
        if wanted.is_empty() {
            return Ok(());
        }
        let list: Vec<Oid> = wanted.iter().copied().collect();
        let rows = sql
            .query(
                "SELECT oid, typname::text, typtype::text, typcategory::text, typelem, typbasetype
                 FROM pg_type WHERE oid = ANY($1)",
                &[&list],
            )
            .await?;
        let mut deps = BTreeSet::new();
        let mut found = HashMap::new();
        for row in rows {
            let oid: Oid = col(&row, 0)?;
            let name: String = col(&row, 1)?;
            let typtype: String = col(&row, 2)?;
            let category: String = col(&row, 3)?;
            let elem: Oid = col(&row, 4)?;
            let base: Oid = col(&row, 5)?;
            for dep in [elem, base] {
                if dep != 0 && !registry.knows(dep) && !wanted.contains(&dep) {
                    deps.insert(dep);
                }
            }
            found.insert(
                oid,
                (
                    name,
                    first_char(&typtype),
                    first_char(&category),
                    elem,
                    base,
                ),
            );
        }
        if deps.is_empty() {
            for oid in &wanted {
                let kind = match found.get(oid) {
                    Some((name, tt, cat, elem, base)) => {
                        convert::classify(name, *tt, *cat, *elem, *base, registry)
                    }
                    None => convert::TypeKind::Text,
                };
                registry.insert(*oid, kind);
            }
            return Ok(());
        }
        wanted.extend(deps);
    }
    // Pathological nesting: whatever is left reads as text.
    for oid in wanted {
        registry.insert(oid, convert::TypeKind::Text);
    }
    Ok(())
}

/// A column, without panicking on a type mismatch.
fn col<'a, T: tokio_postgres::types::FromSql<'a>>(
    row: &'a tokio_postgres::Row,
    idx: usize,
) -> Result<T, SourceError> {
    row.try_get(idx)
        .map_err(|e| SourceError::Fatal(format!("catalog column {idx}: {e}")))
}

fn first_char(s: &str) -> char {
    s.chars().next().unwrap_or(' ')
}

#[derive(Debug, Clone, PartialEq)]
pub struct SlotInfo {
    pub plugin: Option<String>,
    pub database: Option<String>,
    pub active_pid: Option<i32>,
    pub confirmed_flush_lsn: Option<String>,
    pub wal_status: Option<String>,
    pub invalidation_reason: Option<String>,
    pub conflicting: Option<bool>,
}

/// The slot's row, read through `to_jsonb` so columns added in later
/// versions (`wal_status` 13+, `invalidation_reason` 17+) are optional.
pub async fn slot(sql: &Sql, name: &str) -> Result<Option<SlotInfo>, SourceError> {
    let rows = sql
        .query(
            "SELECT to_jsonb(s)::text FROM pg_replication_slots s WHERE slot_name = $1",
            &[&name],
        )
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let text: String = col(row, 0)?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| SourceError::Fatal(format!("pg_replication_slots row: {e}")))?;
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(str::to_owned);
    Ok(Some(SlotInfo {
        plugin: s("plugin"),
        database: s("database"),
        active_pid: v
            .get("active_pid")
            .and_then(|x| x.as_i64())
            .and_then(|x| i32::try_from(x).ok()),
        confirmed_flush_lsn: s("confirmed_flush_lsn"),
        wal_status: s("wal_status"),
        invalidation_reason: s("invalidation_reason"),
        conflicting: v.get("conflicting").and_then(|x| x.as_bool()),
    }))
}

impl SlotInfo {
    /// Why the slot cannot stream any more, if it cannot.
    pub fn lost_reason(&self) -> Option<String> {
        if self.wal_status.as_deref() == Some("lost") {
            return Some("its WAL was removed (wal_status = lost)".into());
        }
        if let Some(reason) = &self.invalidation_reason {
            return Some(format!("it was invalidated ({reason})"));
        }
        if self.conflicting == Some(true) {
            return Some("it conflicts with recovery".into());
        }
        None
    }
}

/// Tables (`schema.table`) in a publication, `None` when it does not exist.
pub async fn publication_tables(
    sql: &Sql,
    name: &str,
) -> Result<Option<BTreeSet<String>>, SourceError> {
    let exists = sql
        .query("SELECT 1 FROM pg_publication WHERE pubname = $1", &[&name])
        .await?;
    if exists.is_empty() {
        return Ok(None);
    }
    let rows = sql
        .query(
            "SELECT schemaname::text || '.' || tablename::text FROM pg_publication_tables WHERE pubname = $1",
            &[&name],
        )
        .await?;
    rows.iter()
        .map(|r| col::<String>(r, 0))
        .collect::<Result<BTreeSet<_>, _>>()
        .map(Some)
}

/// Create the publication for exactly `tables`, or reset an existing one
/// to exactly them.
pub async fn ensure_publication(
    sql: &Sql,
    version: i32,
    name: &str,
    tables: &[(String, String)],
) -> Result<(), SourceError> {
    let list = tables
        .iter()
        .map(|(s, t)| ident::qualified(s, t))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted = ident::quote_ident(name);
    let exists = publication_tables(sql, name).await?.is_some();
    if exists {
        sql.simple(&format!("ALTER PUBLICATION {quoted} SET TABLE {list}"))
            .await?;
        if version >= 130_000 {
            sql.simple(&format!(
                "ALTER PUBLICATION {quoted} SET (publish_via_partition_root = true)"
            ))
            .await?;
        }
    } else {
        // Partition changes are published under the partitioned table's
        // name, the one the user mapped (13+).
        let with = if version >= 130_000 {
            " WITH (publish_via_partition_root = true)"
        } else {
            ""
        };
        sql.simple(&format!(
            "CREATE PUBLICATION {quoted} FOR TABLE {list}{with}"
        ))
        .await?;
    }
    Ok(())
}
