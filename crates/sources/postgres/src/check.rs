//! Pre-flight report. Reads only: creates and changes nothing.

use meili_connect_core::{CheckReport, CheckStatus, SourceError};

use crate::catalog;
use crate::conn::Sql;
use crate::source::{MIN_VERSION, PostgresSource, too_old};

pub async fn run(src: &mut PostgresSource) -> Result<CheckReport, SourceError> {
    let mut report = CheckReport::default();
    if let Err(e) = src.connect().await {
        let mut message = e.to_string();
        if src.supabase() && matches!(e, SourceError::Transient(_)) {
            message.push_str(
                ". Supabase direct connections use IPv6: if this network has no IPv6, \
                 enable the project's IPv4 add-on.",
            );
        }
        let status = match e {
            SourceError::Paused(_) => CheckStatus::Warning,
            _ => CheckStatus::Failed,
        };
        report.push("connection", status, message);
        return Ok(report);
    }
    report.push("connection", CheckStatus::Ok, "connected");

    let version = src.version;
    let slot_name = src.settings.slot.clone();
    let publication = src.settings.publication.clone();
    let list = src.table_list();
    let targets = src.targets.clone();
    let sql = src.db()?;

    if version < MIN_VERSION {
        report.push("server_version", CheckStatus::Failed, too_old(version));
    } else {
        report.push("server_version", CheckStatus::Ok, version_text(version));
    }

    let wal_level = setting(sql, "wal_level").await?.unwrap_or_default();
    if wal_level == "logical" {
        report.push("wal_level", CheckStatus::Ok, "wal_level is logical");
    } else {
        report.push(
            "wal_level",
            CheckStatus::Failed,
            format!(
                "wal_level is '{wal_level}': set it to 'logical' and restart Postgres \
                 (on RDS/Aurora: rds.logical_replication = 1; on Cloud SQL: \
                 cloudsql.logical_decoding = on)"
            ),
        );
    }

    let rows = sql
        .query(
            "SELECT r.rolsuper, r.rolreplication,
                    COALESCE((SELECT pg_has_role(current_user, x.oid, 'MEMBER')
                              FROM pg_roles x WHERE x.rolname = 'rds_replication'), false)
             FROM pg_roles r WHERE r.rolname = current_user",
            &[],
        )
        .await?;
    let (superuser, replication, rds) = match rows.first() {
        Some(r) => (
            r.try_get::<_, bool>(0).unwrap_or(false),
            r.try_get::<_, bool>(1).unwrap_or(false),
            r.try_get::<_, bool>(2).unwrap_or(false),
        ),
        None => (false, false, false),
    };
    if superuser || replication || rds {
        report.push(
            "replication_privilege",
            CheckStatus::Ok,
            "the role can replicate",
        );
    } else {
        report.push(
            "replication_privilege",
            CheckStatus::Failed,
            "the role lacks REPLICATION: ALTER ROLE <user> WITH REPLICATION \
             (on RDS: GRANT rds_replication TO <user>)",
        );
    }

    let slot_exists = catalog::slot(sql, &slot_name).await?.is_some();
    let rows = sql
        .query(
            "SELECT current_setting('max_replication_slots')::int8,
                    (SELECT count(*) FROM pg_replication_slots)::int8,
                    current_setting('max_wal_senders')::int8,
                    (SELECT count(*) FROM pg_stat_replication)::int8",
            &[],
        )
        .await?;
    if let Some(r) = rows.first() {
        let max_slots: i64 = r.try_get(0).unwrap_or(0);
        let used_slots: i64 = r.try_get(1).unwrap_or(0);
        let max_senders: i64 = r.try_get(2).unwrap_or(0);
        let used_senders: i64 = r.try_get(3).unwrap_or(0);
        if slot_exists {
            report.push(
                "replication_slots",
                CheckStatus::Ok,
                format!("slot {slot_name} already exists"),
            );
        } else if used_slots < max_slots {
            report.push(
                "replication_slots",
                CheckStatus::Ok,
                format!(
                    "{} of {max_slots} replication slots free",
                    max_slots - used_slots
                ),
            );
        } else {
            report.push(
                "replication_slots",
                CheckStatus::Failed,
                format!(
                    "all {max_slots} replication slots are in use: raise max_replication_slots \
                     or drop an unused slot"
                ),
            );
        }
        // The first sync briefly holds two WAL senders (slot creation and
        // the stream), one afterwards.
        if used_senders < max_senders {
            report.push(
                "wal_senders",
                CheckStatus::Ok,
                format!(
                    "{} of {max_senders} WAL senders free",
                    max_senders - used_senders
                ),
            );
        } else {
            report.push(
                "wal_senders",
                CheckStatus::Failed,
                format!("all {max_senders} WAL senders are busy: raise max_wal_senders"),
            );
        }
    }

    match setting(sql, "max_slot_wal_keep_size").await? {
        None => report.push(
            "max_slot_wal_keep_size",
            CheckStatus::Ok,
            "not available before Postgres 13: a stalled slot keeps WAL without limit",
        ),
        Some(v) if v == "-1" => report.push(
            "max_slot_wal_keep_size",
            CheckStatus::Ok,
            "unlimited: the slot is never invalidated, a stalled sync grows the WAL until it \
             catches up",
        ),
        Some(v) => report.push(
            "max_slot_wal_keep_size",
            CheckStatus::Warning,
            format!(
                "max_slot_wal_keep_size is {v} MB: if the sync falls further behind, Postgres \
                 invalidates the slot and the indexes are rebuilt from a new snapshot"
            ),
        ),
    }

    let existing = catalog::publication_tables(sql, &publication).await?;
    if existing.is_none() {
        let can_create = sql
            .query(
                "SELECT has_database_privilege(current_user, current_database(), 'CREATE')",
                &[],
            )
            .await?
            .first()
            .and_then(|r| r.try_get::<_, bool>(0).ok())
            .unwrap_or(false);
        if can_create || superuser {
            report.push(
                "publication",
                CheckStatus::Ok,
                format!("publication {publication} will be created"),
            );
        } else {
            report.push(
                "publication",
                CheckStatus::Failed,
                "the role cannot create publications: GRANT CREATE ON DATABASE <db> TO <user>",
            );
        }
    } else {
        report.push(
            "publication",
            CheckStatus::Ok,
            format!("publication {publication} exists"),
        );
    }

    if targets.is_empty() {
        report.push("tables", CheckStatus::Failed, "no table is mapped");
        return Ok(report);
    }
    let found = catalog::tables(sql, version, Some(&list)).await?;
    for t in &targets {
        let name = format!("table {}", t.canonical());
        let Some(info) = found
            .iter()
            .find(|f| f.schema == t.schema && f.table == t.table)
        else {
            report.push(
                &name,
                CheckStatus::Failed,
                "does not exist (or is not visible)",
            );
            continue;
        };
        let mut problems = Vec::new();
        match (info.key(), info.replica_identity.as_str()) {
            (Some(_), _) => {}
            (None, "n") => problems.push(
                "REPLICA IDENTITY is NOTHING: updates and deletes cannot be replicated; \
                 add a primary key or set REPLICA IDENTITY USING INDEX"
                    .to_owned(),
            ),
            (None, "f") => problems.push(
                "REPLICA IDENTITY FULL without a primary key: rows have no stable id; \
                 add a primary key"
                    .to_owned(),
            ),
            (None, _) => problems.push(
                "no primary key: add one or set REPLICA IDENTITY USING INDEX <unique index>"
                    .to_owned(),
            ),
        }
        if !info.can_select {
            problems.push("the role cannot SELECT from it".to_owned());
        }
        let in_publication = existing
            .as_ref()
            .is_some_and(|p| p.contains(&t.canonical()));
        if !info.is_owner && !superuser && !in_publication {
            problems
                .push("the role does not own it, so it cannot add it to a publication".to_owned());
        }
        if problems.is_empty() {
            let key = info.key().unwrap_or_default().join(", ");
            let rows = info
                .estimated_rows()
                .map(|n| format!(", ~{n} rows"))
                .unwrap_or_default();
            report.push(&name, CheckStatus::Ok, format!("key ({key}){rows}"));
        } else {
            report.push(&name, CheckStatus::Failed, problems.join("; "));
        }
    }
    Ok(report)
}

async fn setting(sql: &Sql, name: &str) -> Result<Option<String>, SourceError> {
    let rows = sql
        .query("SELECT setting FROM pg_settings WHERE name = $1", &[&name])
        .await?;
    Ok(rows.first().and_then(|r| r.try_get::<_, String>(0).ok()))
}

fn version_text(num: i32) -> String {
    let major = num / 10_000;
    if major >= 10 {
        format!("Postgres {major}.{}", num % 10_000)
    } else {
        format!(
            "Postgres {}.{}.{}",
            num / 10_000,
            (num / 100) % 100,
            num % 100
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_text() {
        assert_eq!(super::version_text(170_002), "Postgres 17.2");
        assert_eq!(super::version_text(90_624), "Postgres 9.6.24");
    }
}
