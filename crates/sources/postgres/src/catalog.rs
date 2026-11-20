//! Catalog queries: tables, keys, columns, types, slots, publications.

use std::collections::{BTreeSet, HashMap};

use meili_connect_core::{Document, SourceError};

use crate::conn::Sql;
use crate::convert::{self, Oid, TypeRegistry};
use crate::ident;
use crate::pgoutput::RowRef;

/// Why a table with inheritance children cannot be synced.
pub const INHERITANCE: &str = "other tables inherit from it: their rows would be read by the \
     snapshot but their changes are published under their own names, so they would never be \
     updated or deleted. Map each table separately (declarative partitioning is supported)";

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
    pub oid: u32,
    /// A plain table with inheritance children (not partitions): a SELECT
    /// reads their rows, but their changes are published under their own
    /// names.
    pub has_inheritance_children: bool,
    /// `relrowsecurity`: row-level security is enabled.
    pub row_security: bool,
    /// `relforcerowsecurity`: the policies apply to the owner too.
    pub force_row_security: bool,
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

    /// Whether row-level security hides rows from a role with these
    /// attributes: the policies apply unless the role is a superuser, has
    /// `BYPASSRLS`, or owns the table and RLS is not forced on owners.
    pub fn rls_hides_rows(&self, superuser: bool, bypass_rls: bool) -> bool {
        self.row_security
            && !superuser
            && !bypass_rls
            && (!self.is_owner || self.force_row_security)
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
               WHERE i.indrelid = c.oid AND i.{flag}
                 -- Key columns only: INCLUDE columns do not identify a row.
                 AND k.ord <= i.indnkeyatts), '{{}}')"
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
                pg_has_role(c.relowner, 'USAGE'),
                c.oid,
                c.relkind = 'r' AND EXISTS (SELECT 1 FROM pg_inherits h WHERE h.inhparent = c.oid),
                c.relrowsecurity, c.relforcerowsecurity
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
            oid: col(&row, 13)?,
            has_inheritance_children: col(&row, 14)?,
            row_security: col(&row, 15)?,
            force_row_security: col(&row, 16)?,
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

/// The current row with key `row.key_columns`, as a whole document of the
/// relation's columns. `None` when the row (or its table, or a key column)
/// is gone: a later change in the stream covers it.
pub async fn fetch_row(
    sql: &Sql,
    registry: &mut TypeRegistry,
    row: &RowRef,
) -> Result<Option<Document>, SourceError> {
    let qualified = ident::qualified(&row.schema, &row.table);
    let rows = sql
        .query(
            "SELECT a.attname::text, a.atttypid, format_type(a.atttypid, NULL)
             FROM pg_attribute a
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped",
            &[&qualified],
        )
        .await?;
    let mut current: HashMap<String, (Oid, String)> = HashMap::with_capacity(rows.len());
    for r in &rows {
        current.insert(col(r, 0)?, (col(r, 1)?, col(r, 2)?));
    }
    // The relation's columns that still exist, typed as they are now.
    let columns: Vec<(String, Oid)> = row
        .columns
        .iter()
        .filter_map(|c| current.get(&c.name).map(|(oid, _)| (c.name.clone(), *oid)))
        .collect();
    let mut filters = Vec::with_capacity(row.key_columns.len());
    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(row.key_columns.len());
    for (i, (column, text)) in row.key_columns.iter().enumerate() {
        let Some((_, type_name)) = current.get(&column.name) else {
            return Ok(None);
        };
        filters.push(format!(
            "{} = ${}::text::{type_name}",
            ident::quote_ident(&column.name),
            i + 1
        ));
        params.push(text);
    }
    if columns.is_empty() || filters.is_empty() {
        return Ok(None);
    }
    resolve_types(sql, registry, columns.iter().map(|(_, oid)| *oid)).await?;
    let select = columns
        .iter()
        .map(|(name, _)| format!("{}::text", ident::quote_ident(name)))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {select} FROM {qualified} WHERE {}",
        filters.join(" AND ")
    );
    let found = match sql.query(&query, &params).await {
        Ok(found) => found,
        // Dropped or altered between the catalog read and the query.
        Err(SourceError::Config(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let Some(found) = found.first() else {
        return Ok(None);
    };
    let mut doc = Document::new();
    for (i, (name, oid)) in columns.iter().enumerate() {
        let value: Option<String> = col(found, i)?;
        let value = match value {
            Some(text) => registry.to_json(*oid, &text),
            None => serde_json::Value::Null,
        };
        doc.insert(name.clone(), value);
    }
    Ok(Some(doc))
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

/// The end of the WAL this server has: flushed on a primary, received or
/// replayed (whichever is further) on a standby.
pub async fn wal_end(sql: &Sql) -> Result<String, SourceError> {
    let rows = sql
        .query(
            "SELECT (CASE WHEN pg_is_in_recovery()
                      THEN GREATEST(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
                      ELSE pg_current_wal_flush_lsn() END)::text",
            &[],
        )
        .await?;
    rows.first()
        .and_then(|r| r.try_get::<_, Option<String>>(0).ok().flatten())
        .ok_or_else(|| SourceError::Transient("the server reports no WAL position".into()))
}

/// Whether backend `pid` is a session of ours: this connector's
/// application name, logged in as the current role.
pub async fn is_own_backend(sql: &Sql, pid: i32) -> Result<bool, SourceError> {
    let rows = sql
        .query(
            "SELECT 1 FROM pg_stat_activity
             WHERE pid = $1 AND application_name = $2 AND usename = current_user",
            &[&pid, &crate::conn::APPLICATION_NAME],
        )
        .await?;
    Ok(!rows.is_empty())
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

/// The comment that marks a publication as created by `sync_id`.
pub fn marker(sync_id: &str) -> String {
    format!("meili-connect:{sync_id}")
}

/// The publication's comment: `None` when it does not exist, `Some(None)`
/// when it has no comment.
pub async fn publication_comment(
    sql: &Sql,
    name: &str,
) -> Result<Option<Option<String>>, SourceError> {
    let rows = sql
        .query(
            "SELECT obj_description(oid, 'pg_publication') FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await?;
    rows.first()
        .map(|r| col::<Option<String>>(r, 0))
        .transpose()
}

/// Create the publication for exactly `tables` and mark it as ours, or
/// reset an existing one (the caller checked it carries our marker) to
/// exactly them. Partition changes are published under the partitioned
/// table's name, the one the user mapped.
pub async fn ensure_publication(
    sql: &Sql,
    name: &str,
    marker: &str,
    tables: &[(String, String)],
) -> Result<(), SourceError> {
    let list = tables
        .iter()
        .map(|(s, t)| ident::qualified(s, t))
        .collect::<Vec<_>>()
        .join(", ");
    let quoted = ident::quote_ident(name);
    let exists = publication_comment(sql, name).await?.is_some();
    // One simple query is one implicit transaction: never a publication
    // without its marker.
    let statements = if exists {
        format!(
            "ALTER PUBLICATION {quoted} SET TABLE {list};
             ALTER PUBLICATION {quoted} SET (publish_via_partition_root = true)"
        )
    } else {
        format!(
            "CREATE PUBLICATION {quoted} FOR TABLE {list} WITH (publish_via_partition_root = true);
             COMMENT ON PUBLICATION {quoted} IS {}",
            ident::quote_literal(marker)
        )
    };
    sql.simple(&statements).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(row_security: bool, force: bool, is_owner: bool) -> TableInfo {
        TableInfo {
            schema: "public".into(),
            table: "t".into(),
            relkind: "r".into(),
            replica_identity: "d".into(),
            reltuples: -1.0,
            primary_key: vec!["id".into()],
            replica_identity_index: Vec::new(),
            columns: Vec::new(),
            can_select: true,
            is_owner,
            oid: 1,
            has_inheritance_children: false,
            row_security,
            force_row_security: force,
        }
    }

    #[test]
    fn rls_hides_rows_unless_the_role_bypasses_it() {
        // RLS off: nothing is hidden.
        assert!(!table(false, false, false).rls_hides_rows(false, false));
        // A non-owner without BYPASSRLS sees only what policies allow.
        assert!(table(true, false, false).rls_hides_rows(false, false));
        assert!(!table(true, false, false).rls_hides_rows(false, true));
        assert!(!table(true, false, false).rls_hides_rows(true, false));
        // The owner bypasses it, unless FORCE ROW LEVEL SECURITY.
        assert!(!table(true, false, true).rls_hides_rows(false, false));
        assert!(table(true, true, true).rls_hides_rows(false, false));
        assert!(!table(true, true, true).rls_hides_rows(false, true));
        assert!(!table(true, true, true).rls_hides_rows(true, false));
    }
}
