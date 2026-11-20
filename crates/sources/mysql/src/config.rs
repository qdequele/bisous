//! Typed connector settings (`SourceSpec.config`).

use std::time::Duration;

use meili_connect_core::{Secret, SourceError};
use mysql_async::{Opts, OptsBuilder, SslOpts};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::provider::Provider;

/// How the connection is encrypted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TlsMode {
    /// Plain TCP. Only for local networks.
    Disabled,
    /// Encrypted, certificate not verified (MySQL's own self-signed
    /// certificates work).
    #[default]
    Required,
    /// Encrypted, certificate chain verified against `ca_pem` (or the
    /// built-in roots), host name not checked.
    VerifyCa,
    /// Encrypted, chain and host name verified.
    VerifyIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub mode: TlsMode,
    /// PEM-encoded CA certificate(s) for `verify_ca` / `verify_identity`.
    #[serde(default)]
    pub ca_pem: Option<String>,
}

/// `SourceSpec.config` for `kind: mysql`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Secret,
    /// Restricts `discover` to one database. Mapped tables must live in it.
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub tls: TlsConfig,
    /// Replica server id used on the binlog connection. Must be unique among
    /// the replicas of the server. Derived from the sync id when absent.
    #[serde(default)]
    pub server_id: Option<u32>,
    /// Rows per keyset page during the snapshot.
    #[serde(default = "default_batch")]
    pub snapshot_batch_size: u32,
    /// The hosted service the server runs on (`rds`, `aurora`, `cloud_sql`,
    /// `azure`). Optional, detected from the host when absent; it only
    /// makes failed checks name that provider's fix.
    #[serde(default)]
    pub provider: Option<Provider>,
}

fn default_port() -> u16 {
    3306
}

fn default_batch() -> u32 {
    1000
}

/// Lower bound of derived server ids. MySQL servers are usually numbered
/// from 1 and real replicas stay small; derived ids live in the upper half
/// of the `u32` range so they never collide with them.
pub const DERIVED_SERVER_ID_BASE: u32 = 0x8000_0000;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

impl MysqlConfig {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, SourceError> {
        // serde error messages may quote the offending value: take the
        // password out first so it can never reach one.
        let mut value = value.clone();
        let password = match value.as_object_mut().and_then(|o| o.remove("password")) {
            None | Some(serde_json::Value::Null) => Secret::default(),
            Some(serde_json::Value::String(s)) => Secret::new(s),
            Some(_) => {
                return Err(SourceError::Config(
                    "invalid mysql config: password must be a string".into(),
                ));
            }
        };
        let mut cfg: MysqlConfig = serde_json::from_value(value)
            .map_err(|e| SourceError::Config(format!("invalid mysql config: {e}")))?;
        cfg.password = password;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), SourceError> {
        if self.host.trim().is_empty() {
            return Err(SourceError::Config("mysql config: host is empty".into()));
        }
        if self.user.trim().is_empty() {
            return Err(SourceError::Config("mysql config: user is empty".into()));
        }
        if self.port == 0 {
            return Err(SourceError::Config("mysql config: port is 0".into()));
        }
        if self.server_id == Some(0) {
            return Err(SourceError::Config(
                "mysql config: server_id must not be 0".into(),
            ));
        }
        if self.snapshot_batch_size == 0 {
            return Err(SourceError::Config(
                "mysql config: snapshot_batch_size must be positive".into(),
            ));
        }
        if self.tls.ca_pem.is_some() && self.tls.mode == TlsMode::Disabled {
            return Err(SourceError::Config(
                "mysql config: tls.ca_pem is set but tls.mode is disabled".into(),
            ));
        }
        Ok(())
    }

    /// The hosted provider, named in the config or detected from the host.
    pub fn provider(&self) -> Option<Provider> {
        self.provider.or_else(|| Provider::detect(&self.host))
    }

    pub fn server_id_for(&self, sync_id: &str) -> u32 {
        self.server_id.unwrap_or_else(|| derive_server_id(sync_id))
    }

    /// Connection options. The session is pinned to UTC and utf8mb4 so
    /// `TIMESTAMP` values and strings decode the same way everywhere.
    pub fn opts(&self) -> Opts {
        let mut b = OptsBuilder::default()
            .ip_or_hostname(self.host.clone())
            .tcp_port(self.port)
            .user(Some(self.user.clone()))
            .pass(Some(self.password.expose().to_owned()))
            .prefer_socket(false)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .stmt_cache_size(Some(32))
            .init(vec![
                "SET NAMES utf8mb4".to_owned(),
                "SET time_zone = '+00:00'".to_owned(),
            ]);
        if let Some(ssl) = self.ssl_opts() {
            b = b.ssl_opts(ssl);
        }
        Opts::from(b)
    }

    fn ssl_opts(&self) -> Option<SslOpts> {
        let roots = || {
            self.tls
                .ca_pem
                .as_ref()
                .map(|pem| vec![pem.as_bytes().to_vec().into()])
                .unwrap_or_default()
        };
        match self.tls.mode {
            TlsMode::Disabled => None,
            TlsMode::Required => Some(
                SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true),
            ),
            TlsMode::VerifyCa => Some(
                SslOpts::default()
                    .with_root_certs(roots())
                    .with_danger_skip_domain_validation(true),
            ),
            TlsMode::VerifyIdentity => Some(SslOpts::default().with_root_certs(roots())),
        }
    }
}

/// Deterministic replica server id for a sync: two syncs get different ids
/// (up to a 1 in 2^31 hash collision) and a restarted sync reuses its own,
/// so the server replaces its stale dump thread instead of keeping two.
pub fn derive_server_id(sync_id: &str) -> u32 {
    let digest = Sha256::digest(sync_id.as_bytes());
    let raw = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    // [BASE, u32::MAX - 1]: never 0, never u32::MAX.
    DERIVED_SERVER_ID_BASE + raw % (u32::MAX - DERIVED_SERVER_ID_BASE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_config_gets_defaults() {
        let cfg = MysqlConfig::from_json(&json!({
            "host": "db", "user": "repl", "password": "pw"
        }))
        .unwrap();
        assert_eq!(cfg.port, 3306);
        assert_eq!(cfg.tls.mode, TlsMode::Required);
        assert_eq!(cfg.snapshot_batch_size, 1000);
        assert_eq!(cfg.password.expose(), "pw");
        assert!(cfg.database.is_none());
        assert!(!format!("{cfg:?}").contains("pw\""));
        assert!(format!("{cfg:?}").contains("<redacted>"));
    }

    #[test]
    fn provider_named_or_detected() {
        let cfg = MysqlConfig::from_json(&json!({
            "host": "10.0.0.5", "user": "repl", "provider": "cloud_sql"
        }))
        .unwrap();
        assert_eq!(cfg.provider(), Some(Provider::CloudSql));
        let cfg = MysqlConfig::from_json(&json!({
            "host": "shop.abc.eu-west-1.rds.amazonaws.com", "user": "repl"
        }))
        .unwrap();
        assert_eq!(cfg.provider, None);
        assert_eq!(cfg.provider(), Some(Provider::Rds));
        let cfg = MysqlConfig::from_json(&json!({"host": "db", "user": "repl"})).unwrap();
        assert_eq!(cfg.provider(), None);
        assert!(
            MysqlConfig::from_json(&json!({"host": "db", "user": "repl", "provider": "neon"}))
                .is_err()
        );
    }

    #[test]
    fn full_config_parses() {
        let cfg = MysqlConfig::from_json(&json!({
            "host": "db", "port": 3307, "user": "repl", "password": "pw",
            "database": "shop", "server_id": 4242,
            "tls": {"mode": "verify_identity", "ca_pem": "-----BEGIN CERTIFICATE-----"},
            "snapshot_batch_size": 50
        }))
        .unwrap();
        assert_eq!(cfg.port, 3307);
        assert_eq!(cfg.database.as_deref(), Some("shop"));
        assert_eq!(cfg.tls.mode, TlsMode::VerifyIdentity);
        assert_eq!(cfg.server_id_for("anything"), 4242);
        let _ = cfg.opts();
    }

    #[test]
    fn invalid_configs_are_config_errors() {
        for bad in [
            json!({"host": "", "user": "u"}),
            json!({"host": "h", "user": ""}),
            json!({"host": "h", "user": "u", "server_id": 0}),
            json!({"host": "h", "user": "u", "unknown": 1}),
            json!({"host": "h", "user": "u", "tls": {"mode": "disabled", "ca_pem": "x"}}),
            json!({"host": "h", "user": "u", "tls": {"mode": "sometimes"}}),
            json!({"user": "u"}),
        ] {
            assert!(
                matches!(MysqlConfig::from_json(&bad), Err(SourceError::Config(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn config_error_never_echoes_the_password() {
        let err = MysqlConfig::from_json(&json!({
            "host": "h", "user": "u", "password": 12345678
        }))
        .unwrap_err();
        assert!(!err.to_string().contains("12345678"), "{err}");
    }

    #[test]
    fn derived_server_id_is_stable_high_and_distinct() {
        let a = derive_server_id("sync-a");
        assert_eq!(a, derive_server_id("sync-a"));
        assert_ne!(a, derive_server_id("sync-b"));
        let mut seen = std::collections::HashSet::new();
        for i in 0..10_000 {
            let id = derive_server_id(&format!("sync-{i}"));
            assert!((DERIVED_SERVER_ID_BASE..u32::MAX).contains(&id));
            seen.insert(id);
        }
        assert_eq!(seen.len(), 10_000);
    }
}
