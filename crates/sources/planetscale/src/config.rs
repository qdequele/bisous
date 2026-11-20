//! The connector's typed configuration (`SourceSpec.config`).

use meili_connect_core::{Secret, SourceError};
use serde::{Deserialize, Serialize};

/// Which tablets VStream reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TabletKind {
    /// The primary: always present, sees changes first.
    #[default]
    Primary,
    /// A replica: no load on the primary, follows it closely.
    Replica,
    /// A read-only (batch) tablet, where the plan has one.
    Rdonly,
}

impl TabletKind {
    pub fn proto(self) -> crate::proto::TabletType {
        match self {
            TabletKind::Primary => crate::proto::TabletType::Primary,
            TabletKind::Replica => crate::proto::TabletType::Replica,
            TabletKind::Rdonly => crate::proto::TabletType::Rdonly,
        }
    }
}

/// `SourceSpec.config` for `kind: planetscale`.
///
/// One PlanetScale database (a Vitess keyspace), reached on two ports of the
/// same host: VStream over gRPC (443) for changes, the MySQL protocol (3306)
/// for discovery, schema and the first copy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanetScaleConfig {
    /// `aws.connect.psdb.cloud`, as in the database's connection settings.
    pub host: String,
    /// The database (keyspace). Mapped tables live in it.
    pub database: String,
    pub user: String,
    #[serde(default)]
    pub password: Secret,
    /// VStream (gRPC) port.
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
    /// MySQL protocol port.
    #[serde(default = "default_mysql_port")]
    pub mysql_port: u16,
    /// TLS on both connections, certificates checked against the public
    /// roots. `false` only for a local Vitess (tests).
    #[serde(default = "default_true")]
    pub tls: bool,
    #[serde(default)]
    pub tablet_type: TabletKind,
    /// VStream's `cells`. Default: PlanetScale's own cell on `*.psdb.cloud`
    /// hosts, the vtgate's cell elsewhere.
    #[serde(default)]
    pub cells: Option<String>,
    /// Rows per keyset page during the first copy.
    #[serde(default = "default_batch")]
    pub snapshot_batch_size: u32,
}

/// The cell PlanetScale's own connector names (planetscale/airbyte-source).
pub const PLANETSCALE_CELL: &str = "planetscale_operator_default";

fn default_grpc_port() -> u16 {
    443
}
fn default_mysql_port() -> u16 {
    3306
}
fn default_true() -> bool {
    true
}
fn default_batch() -> u32 {
    1000
}

impl PlanetScaleConfig {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, SourceError> {
        // serde messages may quote the offending value: take the password
        // out first so it can never reach one.
        let mut value = value.clone();
        let password = match value.as_object_mut().and_then(|o| o.remove("password")) {
            None | Some(serde_json::Value::Null) => Secret::default(),
            Some(serde_json::Value::String(s)) => Secret::new(s),
            Some(_) => {
                return Err(SourceError::Config(
                    "invalid planetscale config: password must be a string".into(),
                ));
            }
        };
        let mut cfg: PlanetScaleConfig = serde_json::from_value(value)
            .map_err(|e| SourceError::Config(format!("invalid planetscale config: {e}")))?;
        cfg.password = password;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), SourceError> {
        let bad = |m: &str| Err(SourceError::Config(format!("planetscale config: {m}")));
        if self.host.trim().is_empty() || self.host.contains('/') || self.host.contains(',') {
            return bad("host must be one host name, such as aws.connect.psdb.cloud");
        }
        if self.database.trim().is_empty() {
            return bad("database is empty");
        }
        if self.database.contains('`') || self.database.contains('.') {
            return bad("database must be a plain database name");
        }
        if self.user.trim().is_empty() {
            return bad("user is empty");
        }
        if self.grpc_port == 0 || self.mysql_port == 0 {
            return bad("ports must not be 0");
        }
        if self.snapshot_batch_size == 0 {
            return bad("snapshot_batch_size must be positive");
        }
        Ok(())
    }

    /// A PlanetScale-hosted database (as opposed to a self-run Vitess).
    pub fn is_planetscale_host(&self) -> bool {
        let h = self.host.trim_end_matches('.').to_ascii_lowercase();
        h == "psdb.cloud" || h.ends_with(".psdb.cloud")
    }

    pub fn cells(&self) -> String {
        match &self.cells {
            Some(c) => c.clone(),
            None if self.is_planetscale_host() => PLANETSCALE_CELL.to_owned(),
            None => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_config_gets_defaults() {
        let c = PlanetScaleConfig::from_json(&json!({
            "host": "aws.connect.psdb.cloud", "database": "shop",
            "user": "u", "password": "pscale_pw_x"
        }))
        .unwrap();
        assert_eq!((c.grpc_port, c.mysql_port), (443, 3306));
        assert!(c.tls);
        assert_eq!(c.tablet_type, TabletKind::Primary);
        assert_eq!(c.cells(), PLANETSCALE_CELL);
        assert_eq!(c.password.expose(), "pscale_pw_x");
        assert!(!format!("{c:?}").contains("pscale_pw_x"));
    }

    #[test]
    fn a_local_vitess_has_no_default_cell() {
        let c = PlanetScaleConfig::from_json(&json!({
            "host": "127.0.0.1", "database": "shop", "user": "u",
            "grpc_port": 33575, "mysql_port": 33577, "tls": false,
            "tablet_type": "replica"
        }))
        .unwrap();
        assert_eq!(c.cells(), "");
        assert_eq!(c.tablet_type.proto(), crate::proto::TabletType::Replica);
    }

    #[test]
    fn invalid_configs_never_echo_the_password() {
        for bad in [
            json!({"host": "", "database": "d", "user": "u", "password": "hunter2"}),
            json!({"host": "h", "database": "", "user": "u", "password": "hunter2"}),
            json!({"host": "h", "database": "a.b", "user": "u", "password": "hunter2"}),
            json!({"host": "h", "database": "d", "user": "u", "password": "hunter2", "port": 1}),
            json!({"host": "h", "database": "d", "user": "u", "password": 5}),
        ] {
            let err = PlanetScaleConfig::from_json(&bad).unwrap_err();
            assert!(matches!(err, SourceError::Config(_)), "{bad}");
            assert!(!err.to_string().contains("hunter2"), "{err}");
        }
    }
}
