//! Pre-flight report. Reads only: creates and changes nothing.

use meili_connect_core::{CheckReport, CheckStatus, SourceError};

use crate::catalog;
use crate::conn::Sql;
use crate::provider::Provider;
use crate::source::{MIN_VERSION, PostgresSource, too_old};

pub async fn run(src: &mut PostgresSource) -> Result<CheckReport, SourceError> {
    let mut report = CheckReport::default();
    if let Err(e) = src.connect().await.map_err(crate::error::first_contact) {
        let mut message = e.to_string();
        if src.supabase() && matches!(e, SourceError::Transient(_)) {
            message.push_str(
                ". Supabase direct connections use IPv6: if this network has no IPv6, \
                 enable the project's IPv4 add-on.",
            );
        }
        if let (Some(p), SourceError::Transient(_)) = (src.provider(), &e) {
            message.push_str(&connection_hint(p));
        }
        let status = match e {
            SourceError::Paused(_) => CheckStatus::Warning,
            _ => CheckStatus::Failed,
        };
        report.push("connection", status, message);
        return Ok(report);
    }
    report.push("connection", CheckStatus::Ok, "connected");

    let provider = src.provider();
    let version = src.version;
    let slot_name = src.settings.slot.clone();
    let publication = src.settings.publication.clone();
    let list = src.table_list();
    let targets = src.targets.clone();
    let owned = src.ownership().await?;
    let foreign_slot = owned.foreign_slot(&src.settings);
    let foreign_publication = owned.foreign_publication(&src.settings);
    let manage_publication = src.settings.manage_publication;
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
            wal_level_message(&wal_level, provider),
        );
    }

    let rows = sql
        .query(
            "SELECT r.rolsuper, r.rolreplication,
                    COALESCE((SELECT pg_has_role(current_user, x.oid, 'MEMBER')
                              FROM pg_roles x WHERE x.rolname = 'rds_replication'), false),
                    r.rolbypassrls
             FROM pg_roles r WHERE r.rolname = current_user",
            &[],
        )
        .await?;
    let (superuser, replication, rds, bypass_rls) = match rows.first() {
        Some(r) => (
            r.try_get::<_, bool>(0).unwrap_or(false),
            r.try_get::<_, bool>(1).unwrap_or(false),
            r.try_get::<_, bool>(2).unwrap_or(false),
            r.try_get::<_, bool>(3).unwrap_or(false),
        ),
        None => (false, false, false, false),
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
            replication_message(provider),
        );
    }

    let slot_exists = owned.slot.is_some();
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
        if let Some(message) = foreign_slot {
            report.push("replication_slots", CheckStatus::Failed, message);
        } else if slot_exists {
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
                slots_full_message(max_slots, provider),
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
                senders_full_message(max_senders, provider),
            );
        }
    }

    if crate::source::slot_failover(version, provider) {
        let failover = if slot_exists {
            sql.query(
                "SELECT failover FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot_name],
            )
            .await?
            .first()
            .and_then(|r| r.try_get::<_, bool>(0).ok())
        } else {
            None
        };
        report.push(
            "slot_failover",
            if failover == Some(false) {
                CheckStatus::Warning
            } else {
                CheckStatus::Ok
            },
            failover_message(&slot_name, slot_exists, failover),
        );
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
    if !manage_publication {
        // Managed elsewhere: it must exist and cover every mapped table.
        match &existing {
            None => report.push(
                "publication",
                CheckStatus::Failed,
                format!(
                    "publication {publication} does not exist: it is managed outside \
                     meili-connect (manage_publication: false)"
                ),
            ),
            Some(have) => {
                let missing: Vec<String> = targets
                    .iter()
                    .map(|t| t.canonical())
                    .filter(|t| !have.contains(t))
                    .collect();
                if missing.is_empty() {
                    report.push(
                        "publication",
                        CheckStatus::Ok,
                        format!("publication {publication} exists (managed outside meili-connect)"),
                    );
                } else {
                    report.push(
                        "publication",
                        CheckStatus::Failed,
                        format!(
                            "publication {publication} does not cover {}: it is managed outside \
                             meili-connect (manage_publication: false), add them to it",
                            missing.join(", ")
                        ),
                    );
                }
            }
        }
    } else if existing.is_none() {
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
    } else if let Some(message) = foreign_publication {
        report.push("publication", CheckStatus::Failed, message);
    } else {
        report.push(
            "publication",
            CheckStatus::Ok,
            format!("publication {publication} exists"),
        );
    }

    if targets.is_empty() {
        report.push_none_selected("tables", "table");
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
        if info.has_inheritance_children {
            problems.push(catalog::INHERITANCE.to_owned());
        }
        if !info.can_select {
            problems.push("the role cannot SELECT from it".to_owned());
        }
        let in_publication = existing
            .as_ref()
            .is_some_and(|p| p.contains(&t.canonical()));
        // Only a publication we manage needs the table's owner to add it.
        if manage_publication && !info.is_owner && !superuser && !in_publication {
            problems
                .push("the role does not own it, so it cannot add it to a publication".to_owned());
        }
        if info.rls_hides_rows(superuser, bypass_rls) {
            problems.push(RLS.to_owned());
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

/// PlanetScale requires failover-enabled slots: a new one is created so, an
/// older one (created before) is fixed with `ALTER_REPLICATION_SLOT`.
fn failover_message(slot: &str, exists: bool, failover: Option<bool>) -> String {
    match (exists, failover) {
        (true, Some(false)) => format!(
            "slot {slot} is not failover-enabled: PlanetScale gives 3 days to fix it, then a \
             switchover may lose it. Pause the sync, then on a replication=database \
             connection: ALTER_REPLICATION_SLOT {slot} (FAILOVER true); or resync it"
        ),
        (true, _) => format!("slot {slot} is failover-enabled"),
        (false, _) => format!(
            "slot {slot} will be created failover-enabled (FAILOVER true), as PlanetScale requires"
        ),
    }
}

/// Appended to a connection failure: what lets Connect in on that provider.
fn connection_hint(provider: Provider) -> String {
    format!(". On {}: {}", provider.name(), provider.network_fix())
}

fn wal_level_message(wal_level: &str, provider: Option<Provider>) -> String {
    match provider {
        Some(p) => format!(
            "wal_level is '{wal_level}', logical replication needs 'logical'. On {}: {}",
            p.name(),
            p.wal_level_fix()
        ),
        None => format!(
            "wal_level is '{wal_level}': set it to 'logical' and restart Postgres \
             (on RDS/Aurora: rds.logical_replication = 1; on Cloud SQL: \
             cloudsql.logical_decoding = on)"
        ),
    }
}

fn replication_message(provider: Option<Provider>) -> String {
    match provider {
        Some(p) => format!(
            "the role lacks REPLICATION. On {}: {}",
            p.name(),
            p.replication_fix()
        ),
        None => "the role lacks REPLICATION: ALTER ROLE <user> WITH REPLICATION \
                 (on RDS: GRANT rds_replication TO <user>)"
            .to_owned(),
    }
}

fn slots_full_message(max_slots: i64, provider: Option<Provider>) -> String {
    match provider {
        Some(Provider::Neon) => format!(
            "all {max_slots} replication slots are in use: drop an unused slot (Neon sets \
             max_replication_slots itself)"
        ),
        Some(p) => format!(
            "all {max_slots} replication slots are in use: raise max_replication_slots {} or \
             drop an unused slot",
            p.parameter_place()
        ),
        None => format!(
            "all {max_slots} replication slots are in use: raise max_replication_slots \
             or drop an unused slot"
        ),
    }
}

fn senders_full_message(max_senders: i64, provider: Option<Provider>) -> String {
    match provider {
        Some(Provider::Neon) => format!(
            "all {max_senders} WAL senders are busy: stop an unused replication connection \
             (Neon sets max_wal_senders itself)"
        ),
        Some(p) => format!(
            "all {max_senders} WAL senders are busy: raise max_wal_senders {}",
            p.parameter_place()
        ),
        None => format!("all {max_senders} WAL senders are busy: raise max_wal_senders"),
    }
}

/// A mapped table whose row-level security policies apply to the role.
const RLS: &str = "row-level security is on and the role does not bypass it: the snapshot would \
     only copy the rows its policies allow; grant BYPASSRLS to the role or use the table owner";

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

    /// With a provider the failed check names that provider's fix; without
    /// one the message is the generic one it always was.
    #[test]
    fn messages_name_the_provider_fix() {
        use super::*;
        let generic = wal_level_message("replica", None);
        assert!(generic.contains("restart Postgres"), "{generic}");
        for (p, needle) in [
            (Provider::Rds, "DB parameter group"),
            (Provider::Aurora, "DB cluster parameter group"),
            (Provider::CloudSql, "cloudsql.logical_decoding = on"),
            (Provider::Azure, "wal_level = logical"),
            (Provider::Neon, "Logical replication"),
        ] {
            let m = wal_level_message("replica", Some(p));
            assert!(m.contains(needle) && m.contains(p.name()), "{m}");
            assert!(m.contains("'replica'"), "{m}");
        }
        assert!(replication_message(Some(Provider::Aurora)).contains("rds_replication"));
        assert!(replication_message(Some(Provider::CloudSql)).contains("cloudsqlsuperuser"));
        assert!(replication_message(None).contains("ALTER ROLE"));
        assert!(slots_full_message(10, Some(Provider::Rds)).contains("DB parameter group"));
        assert!(slots_full_message(10, Some(Provider::Neon)).contains("drop an unused slot"));
        assert!(senders_full_message(10, Some(Provider::Azure)).contains("server parameters"));
        assert!(connection_hint(Provider::CloudSql).contains("authorized networks"));
        assert!(connection_hint(Provider::Rds).contains("security group"));
        assert!(replication_message(Some(Provider::PlanetScale)).contains("<branch id>"));
        assert!(failover_message("s", true, Some(false)).contains("ALTER_REPLICATION_SLOT s"));
        assert!(failover_message("s", false, None).contains("FAILOVER true"));
        assert_eq!(
            failover_message("s", true, Some(true)),
            "slot s is failover-enabled"
        );
    }

    #[test]
    fn slots_fail_over_only_where_the_provider_requires_it() {
        use crate::provider::Provider;
        use crate::source::create_slot_command;
        let ps = Some(Provider::PlanetScale);
        assert_eq!(
            create_slot_command("s", 170_002, ps),
            "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER true)"
        );
        assert_eq!(
            create_slot_command("s", 180_000, Some(Provider::Rds)),
            "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (SNAPSHOT 'export')"
        );
        assert_eq!(
            create_slot_command("s", 160_000, ps),
            "CREATE_REPLICATION_SLOT s LOGICAL pgoutput (SNAPSHOT 'export')"
        );
        assert_eq!(
            create_slot_command("s", 140_000, None),
            "CREATE_REPLICATION_SLOT s LOGICAL pgoutput EXPORT_SNAPSHOT"
        );
    }
}
