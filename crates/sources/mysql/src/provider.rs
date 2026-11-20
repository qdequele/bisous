//! Hosted MySQL providers: where the fix for a failed check lives.
//!
//! The optional `provider` of the config (or, without it, the host name)
//! names the service the server runs on. It changes nothing about how the
//! sync runs: only the check messages, which then name the parameter
//! group, flag or procedure that provider uses instead of `my.cnf`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    /// Amazon RDS for MySQL (`*.rds.amazonaws.com`).
    Rds,
    /// Amazon Aurora MySQL (`*.cluster-*.rds.amazonaws.com`).
    Aurora,
    /// Google Cloud SQL for MySQL (addressed by IP: never detected).
    CloudSql,
    /// Azure Database for MySQL flexible server
    /// (`*.mysql.database.azure.com`).
    Azure,
}

impl Provider {
    /// The provider a host name belongs to, when it says so.
    pub fn detect(host: &str) -> Option<Self> {
        let h = host.trim_end_matches('.').to_ascii_lowercase();
        if h.ends_with(".rds.amazonaws.com") || h.ends_with(".rds.amazonaws.com.cn") {
            if h.split('.').any(|label| label.starts_with("cluster-")) {
                Some(Provider::Aurora)
            } else {
                Some(Provider::Rds)
            }
        } else if h.ends_with(".mysql.database.azure.com") {
            Some(Provider::Azure)
        } else {
            None
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Provider::Rds => "RDS",
            Provider::Aurora => "Aurora",
            Provider::CloudSql => "Cloud SQL",
            Provider::Azure => "Azure",
        }
    }

    /// How to turn the binary log on there.
    pub fn log_bin_fix(self) -> &'static str {
        match self {
            Provider::Rds => {
                "RDS writes binary logs only while automated backups are on: set the backup \
                 retention period to 1 day or more"
            }
            Provider::Aurora => {
                "set binlog_format = ROW in the DB cluster parameter group, then reboot the \
                 writer instance"
            }
            Provider::CloudSql => {
                "enable point-in-time recovery on the instance (Backups), which turns on \
                 binary logging"
            }
            Provider::Azure => {
                "binary logging is always on on a flexible server: check that this is the \
                 primary server, not a read replica"
            }
        }
    }

    /// Where a binlog setting (`binlog_format`, `binlog_row_image`…) is
    /// changed there.
    pub fn parameter_place(self) -> &'static str {
        match self {
            Provider::Rds => "in the instance's custom DB parameter group",
            Provider::Aurora => {
                "in the DB cluster parameter group (then reboot the writer instance)"
            }
            Provider::CloudSql => "with the instance's database flags",
            Provider::Azure => "in the server parameters",
        }
    }

    /// How to keep binlogs longer there.
    pub fn retention_fix(self) -> &'static str {
        match self {
            Provider::Rds | Provider::Aurora => {
                "CALL mysql.rds_set_configuration('binlog retention hours', 72) as the master \
                 user"
            }
            Provider::CloudSql => {
                "raise the binlog_expire_logs_seconds flag (1 day by default) and the \
                 instance's transaction log retention (Backups), which caps it"
            }
            Provider::Azure => "raise the server parameter binlog_expire_logs_seconds",
        }
    }

    /// What lets Connect reach the server over the network.
    pub fn network_fix(self) -> &'static str {
        match self {
            Provider::Rds | Provider::Aurora => {
                "the instance must be publicly accessible and its security group must allow \
                 inbound TCP 3306 from Connect's egress addresses"
            }
            Provider::CloudSql => {
                "the instance needs a public IP, with Connect's egress addresses in its \
                 authorized networks"
            }
            Provider::Azure => {
                "the server needs public access, with a firewall rule for Connect's egress \
                 addresses"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_name_their_provider() {
        for (host, want) in [
            (
                "shop.abc123.eu-west-1.rds.amazonaws.com",
                Some(Provider::Rds),
            ),
            (
                "shop.cluster-abc123.eu-west-1.rds.amazonaws.com",
                Some(Provider::Aurora),
            ),
            (
                "shop.cluster-ro-abc123.eu-west-1.rds.amazonaws.com",
                Some(Provider::Aurora),
            ),
            ("Shop.MySQL.Database.Azure.com", Some(Provider::Azure)),
            ("srv.postgres.database.azure.com", None),
            ("34.76.1.2", None),
            ("mysql.example.com", None),
        ] {
            assert_eq!(Provider::detect(host), want, "{host}");
        }
    }

    #[test]
    fn serde_names() {
        let p: Provider = serde_json::from_str("\"cloud_sql\"").unwrap();
        assert_eq!(p, Provider::CloudSql);
        assert!(serde_json::from_str::<Provider>("\"neon\"").is_err());
    }
}
