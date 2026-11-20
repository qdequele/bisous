//! The MySQL-protocol side: connection, table schemas, discovery.

use std::collections::BTreeMap;
use std::time::Duration;

use meili_connect_core::{CollectionInfo, FieldInfo, SourceError};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts, OptsBuilder, SslOpts};

use crate::config::PlanetScaleConfig;
use crate::convert::ColumnMeta;
use crate::error;

/// One mapped table: its columns and key, from `information_schema`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<ColumnMeta>,
    /// Indexes into `columns`, in key order.
    pub key: Vec<usize>,
    pub estimated_rows: Option<u64>,
}

impl TableSchema {
    pub fn column(&self, name: &str) -> Option<&ColumnMeta> {
        self.columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }

    pub fn key_names(&self) -> Vec<String> {
        self.key
            .iter()
            .map(|&i| self.columns[i].name.clone())
            .collect()
    }
}

/// Connection options: TLS checked against the public roots (unless
/// `tls: false`), the session in UTC and utf8mb4 so values read the same
/// way as VStream's.
pub fn opts(cfg: &PlanetScaleConfig) -> Opts {
    let mut b = OptsBuilder::default()
        .ip_or_hostname(cfg.host.clone())
        .tcp_port(cfg.mysql_port)
        .user(Some(cfg.user.clone()))
        .pass(Some(cfg.password.expose().to_owned()))
        .db_name(Some(cfg.database.clone()))
        .prefer_socket(false)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .init(vec![
            "SET NAMES utf8mb4".to_owned(),
            "SET time_zone = '+00:00'".to_owned(),
        ]);
    if cfg.tls {
        b = b.ssl_opts(SslOpts::default());
    }
    Opts::from(b)
}

pub async fn connect(cfg: &PlanetScaleConfig) -> Result<Conn, SourceError> {
    match tokio::time::timeout(Duration::from_secs(30), Conn::new(opts(cfg))).await {
        Ok(r) => r.map_err(error::mysql),
        Err(_) => Err(SourceError::Transient(format!(
            "no MySQL answer from {}:{} within 30 s",
            cfg.host, cfg.mysql_port
        ))),
    }
}

type ColumnRow = (String, String, String, String, String, Option<String>);

/// Every base table of the database (or only `only`), with its columns and
/// primary key. A table without one has an empty `key`.
pub async fn load_tables(
    conn: &mut Conn,
    database: &str,
    only: Option<&[String]>,
) -> Result<BTreeMap<String, TableSchema>, SourceError> {
    let rows: Vec<ColumnRow> = conn
        .exec(
            "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, c.IS_NULLABLE, \
                    c.CHARACTER_SET_NAME \
             FROM information_schema.COLUMNS c \
             JOIN information_schema.TABLES t \
               ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
             WHERE c.TABLE_SCHEMA = ? AND t.TABLE_TYPE = 'BASE TABLE' \
             ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION",
            (database,),
        )
        .await
        .map_err(error::mysql)?;
    let mut tables: BTreeMap<String, TableSchema> = BTreeMap::new();
    for (table, name, data_type, column_type, nullable, charset) in rows {
        if only.is_some_and(|o| !o.iter().any(|t| t == &table)) {
            continue;
        }
        let entry = tables.entry(table.clone()).or_insert_with(|| TableSchema {
            table,
            columns: Vec::new(),
            key: Vec::new(),
            estimated_rows: None,
        });
        entry.columns.push(ColumnMeta::new(
            &name,
            &data_type,
            &column_type,
            nullable == "YES",
            charset.as_deref(),
        ));
    }
    // Primary key columns in key order.
    let keys: Vec<(String, String)> = conn
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = ? AND INDEX_NAME = 'PRIMARY' \
             ORDER BY TABLE_NAME, SEQ_IN_INDEX",
            (database,),
        )
        .await
        .map_err(error::mysql)?;
    for (table, column) in keys {
        if let Some(t) = tables.get_mut(&table)
            && let Some(i) = t.columns.iter().position(|c| c.name == column)
        {
            t.key.push(i);
        }
    }
    let estimates: Vec<(String, Option<u64>)> = conn
        .exec(
            "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'",
            (database,),
        )
        .await
        .map_err(error::mysql)?;
    for (table, rows) in estimates {
        if let Some(t) = tables.get_mut(&table) {
            t.estimated_rows = rows;
        }
    }
    Ok(tables)
}

pub fn collection_info(database: &str, t: &TableSchema) -> CollectionInfo {
    CollectionInfo {
        name: format!("{database}.{}", t.table),
        key: t.key_names(),
        fields: t
            .columns
            .iter()
            .map(|c| FieldInfo {
                name: c.name.clone(),
                source_type: c.column_type.clone(),
                nullable: c.nullable,
            })
            .collect(),
        estimated_rows: t.estimated_rows,
    }
}

/// `` `a``b` `` for an identifier.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_ident("t"), "`t`");
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }
}
