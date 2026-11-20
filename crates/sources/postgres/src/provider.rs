//! Hosted Postgres providers: where the fix for a failed check lives.
//!
//! The optional `provider` of the config (or, without it, the host name)
//! names the service the database runs on. It changes nothing about how
//! the sync runs: only the check messages, which then name the setting,
//! console page or grant that provider uses instead of the generic
//! `postgresql.conf` advice.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Neon (`*.neon.tech`).
    Neon,
    /// Amazon RDS for PostgreSQL (`*.rds.amazonaws.com`).
    Rds,
    /// Amazon Aurora PostgreSQL (`*.cluster-*.rds.amazonaws.com`).
    Aurora,
    /// Google Cloud SQL for PostgreSQL (addressed by IP: never detected).
    CloudSql,
    /// Azure Database for PostgreSQL flexible server
    /// (`*.postgres.database.azure.com`).
    Azure,
}

impl Provider {
    /// The provider a host name belongs to, when it says so.
    pub fn detect(host: &str) -> Option<Self> {
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        if h.ends_with(".neon.tech") {
            Some(Provider::Neon)
        } else if h.ends_with(".rds.amazonaws.com") || h.ends_with(".rds.amazonaws.com.cn") {
            // Aurora cluster endpoints: <name>.cluster-<id>.<region>... and
            // the reader endpoint <name>.cluster-ro-<id>...
            if h.split('.').any(|label| label.starts_with("cluster-")) {
                Some(Provider::Aurora)
            } else {
                Some(Provider::Rds)
            }
        } else if h.ends_with(".postgres.database.azure.com") {
            Some(Provider::Azure)
        } else {
            None
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Provider::Neon => "Neon",
            Provider::Rds => "RDS",
            Provider::Aurora => "Aurora",
            Provider::CloudSql => "Cloud SQL",
            Provider::Azure => "Azure",
        }
    }

    /// How to turn `wal_level` to `logical` there.
    pub fn wal_level_fix(self) -> &'static str {
        match self {
            Provider::Neon => {
                "enable logical replication in the project settings (Settings, Logical \
                 replication); it cannot be turned off again and restarts every compute"
            }
            Provider::Rds => {
                "set rds.logical_replication = 1 in the instance's custom DB parameter group, \
                 then reboot the instance"
            }
            Provider::Aurora => {
                "set rds.logical_replication = 1 in the DB cluster parameter group, then reboot \
                 the writer instance"
            }
            Provider::CloudSql => {
                "set the database flag cloudsql.logical_decoding = on on the instance (it \
                 restarts)"
            }
            Provider::Azure => {
                "set the server parameter wal_level = logical, then restart the server"
            }
        }
    }

    /// How to give the role the right to replicate there.
    pub fn replication_fix(self) -> &'static str {
        match self {
            Provider::Rds | Provider::Aurora => {
                "GRANT rds_replication TO <user> (as the master user)"
            }
            Provider::CloudSql => {
                "ALTER USER <user> WITH REPLICATION (as a cloudsqlsuperuser, such as postgres)"
            }
            Provider::Azure => "ALTER ROLE <user> WITH REPLICATION (as the server admin)",
            Provider::Neon => {
                "use a role created in the Neon console, CLI or API (a neon_superuser member, \
                 which has REPLICATION), or ALTER ROLE <user> WITH REPLICATION for a role \
                 created in SQL"
            }
        }
    }

    /// Where `max_replication_slots` and `max_wal_senders` are changed.
    pub fn parameter_place(self) -> &'static str {
        match self {
            Provider::Rds => "in the DB parameter group",
            Provider::Aurora => "in the DB cluster parameter group",
            Provider::CloudSql => "with the instance's database flags",
            Provider::Azure => "in the server parameters",
            Provider::Neon => "(Neon sets it from the compute size: drop an unused slot)",
        }
    }

    /// What lets Connect reach the database over the network.
    pub fn network_fix(self) -> &'static str {
        match self {
            Provider::Rds | Provider::Aurora => {
                "the instance must be publicly accessible and its security group must allow \
                 inbound TCP 5432 from Connect's egress addresses"
            }
            Provider::CloudSql => {
                "the instance needs a public IP, with Connect's egress addresses in its \
                 authorized networks"
            }
            Provider::Azure => {
                "the server needs public access, with a firewall rule for Connect's egress \
                 addresses"
            }
            Provider::Neon => "if IP Allow is on, add Connect's egress addresses to it",
        }
    }
}

/// A Neon pooled host (`ep-<name>-pooler.<region>...neon.tech`): PgBouncer
/// in transaction mode, which cannot carry a replication session.
pub fn is_neon_pooler(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    h.ends_with(".neon.tech") && h.split('.').next().is_some_and(|e| e.ends_with("-pooler"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_name_their_provider() {
        for (host, want) in [
            (
                "ep-cool-darkness-123456.us-east-2.aws.neon.tech",
                Some(Provider::Neon),
            ),
            (
                "mydb.abc123xyz.eu-west-1.rds.amazonaws.com",
                Some(Provider::Rds),
            ),
            (
                "MyDB.ABC123.eu-west-1.rds.amazonaws.com.",
                Some(Provider::Rds),
            ),
            (
                "app.cluster-abc123.eu-west-1.rds.amazonaws.com",
                Some(Provider::Aurora),
            ),
            (
                "app.cluster-ro-abc123.eu-west-1.rds.amazonaws.com",
                Some(Provider::Aurora),
            ),
            ("db.abc.rds.amazonaws.com.cn", Some(Provider::Rds)),
            ("srv.postgres.database.azure.com", Some(Provider::Azure)),
            ("34.76.1.2", None),
            ("db.example.com", None),
            ("neon.tech.example.com", None),
        ] {
            assert_eq!(Provider::detect(host), want, "{host}");
        }
    }

    #[test]
    fn neon_pooler_hosts() {
        assert!(is_neon_pooler(
            "ep-cool-darkness-123456-pooler.us-east-2.aws.neon.tech"
        ));
        assert!(!is_neon_pooler(
            "ep-cool-darkness-123456.us-east-2.aws.neon.tech"
        ));
        assert!(!is_neon_pooler("x-pooler.example.com"));
    }

    #[test]
    fn serde_names() {
        let p: Provider = serde_json::from_str("\"cloud_sql\"").unwrap();
        assert_eq!(p, Provider::CloudSql);
        assert_eq!(
            serde_json::to_string(&Provider::Aurora).unwrap(),
            "\"aurora\""
        );
        assert!(serde_json::from_str::<Provider>("\"heroku\"").is_err());
    }
}
