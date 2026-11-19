//! Process configuration, read once from the environment.
//!
//! Every variable is documented in `.env.example` and the README. Secrets
//! are wrapped in [`Secret`] as soon as they are read, so a `Debug` print of
//! [`Config`] never shows them.

use std::net::SocketAddr;
use std::time::Duration;

use meili_connect_core::host_policy::{self, PolicyMode};
use meili_connect_core::{Endpoint, HostPolicy, Secret};
use thiserror::Error;

use crate::limits::Limits;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// Variables whose blank value is an error rather than "unset".
const AUTH_VARS: &[&str] = &["ADMIN_API_KEY", "LAB_SERVICE_TOKEN"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Pretty,
    Json,
}

/// `LAB_URL` + `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabEventsConfig {
    /// Base URL, without trailing slash.
    pub url: String,
    pub instance_id: String,
    pub instance_secret: Secret,
}

impl LabEventsConfig {
    pub fn events_url(&self) -> String {
        format!("{}/internal/events", self.url)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub listen_addr: SocketAddr,
    /// `None`: in-memory store.
    pub database_url: Option<Secret>,
    /// Base64 32-byte key sealing specs at rest. Required with `database_url`.
    pub secret_key: Option<Secret>,
    pub admin_api_key: Option<Secret>,
    pub lab_service_token: Option<Secret>,
    /// No management auth: allowed only on a loopback address or with
    /// `ALLOW_OPEN_MODE=true`.
    pub open_mode: bool,
    pub lab_events: Option<LabEventsConfig>,
    pub log_format: LogFormat,
    /// `RUST_LOG` syntax; `LOG_LEVEL` is the simple form.
    pub log_filter: String,
    /// How often accumulated document counts become usage events.
    pub usage_flush_interval: Duration,
    /// How often the Lab sender looks for due events.
    pub lab_send_interval: Duration,
    /// Time limit of `/sources/check` and `/sources/discover`.
    pub source_probe_timeout: Duration,
    /// Time given to the syncs to stop on SIGTERM.
    pub shutdown_grace: Duration,
    /// Where sources and destinations may point (`CONNECT_HOST_POLICY`,
    /// `CONNECT_HOST_ALLOWLIST`). `public` by default when
    /// `LAB_SERVICE_TOKEN` is set, `any` otherwise. The state database is
    /// always refused as a source origin.
    pub host_policy: HostPolicy,
    /// Quotas and bounds.
    pub limits: Limits,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Reads through `get` so tests do not touch the process environment.
    /// Blank values count as unset, except for the auth variables: a blank
    /// `ADMIN_API_KEY` or `LAB_SERVICE_TOKEN` is an error, never open mode.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        for name in AUTH_VARS {
            if get(name).is_some_and(|v| v.trim().is_empty()) {
                return Err(ConfigError(format!(
                    "{name} is set but blank: give it a value or remove it"
                )));
            }
        }
        let get = |k: &str| get(k).filter(|v| !v.trim().is_empty());

        let listen_addr = get("LISTEN_ADDR")
            .unwrap_or_else(|| "0.0.0.0:8080".to_owned())
            .parse::<SocketAddr>()
            .map_err(|_| ConfigError("LISTEN_ADDR must be host:port, e.g. 0.0.0.0:8080".into()))?;

        let database_url = get("DATABASE_URL").map(Secret::new);
        let secret_key = get("CONNECT_SECRET_KEY").map(Secret::new);
        if database_url.is_some() && secret_key.is_none() {
            return Err(ConfigError(
                "CONNECT_SECRET_KEY is required with DATABASE_URL (generate one with \
                 `openssl rand -base64 32`)"
                    .into(),
            ));
        }

        let lab_events = match (
            get("LAB_URL"),
            get("LAB_INSTANCE_ID"),
            get("LAB_INSTANCE_SECRET"),
        ) {
            (None, None, None) => None,
            (Some(url), Some(instance_id), Some(secret)) => {
                let url = url.trim().trim_end_matches('/').to_owned();
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return Err(ConfigError("LAB_URL must be an http(s) URL".into()));
                }
                Some(LabEventsConfig {
                    url,
                    instance_id: instance_id.trim().to_owned(),
                    instance_secret: Secret::new(secret),
                })
            }
            _ => {
                return Err(ConfigError(
                    "LAB_URL, LAB_INSTANCE_ID and LAB_INSTANCE_SECRET go together: set all three \
                     or none"
                        .into(),
                ));
            }
        };

        let log_format = match get("LOG_FORMAT").as_deref() {
            None | Some("pretty") | Some("text") => LogFormat::Pretty,
            Some("json") => LogFormat::Json,
            Some(_) => return Err(ConfigError("LOG_FORMAT must be `pretty` or `json`".into())),
        };
        let log_filter = get("RUST_LOG")
            .or_else(|| get("LOG_LEVEL"))
            .unwrap_or_else(|| "info".to_owned());

        let lab_service_token = get("LAB_SERVICE_TOKEN").map(Secret::new);
        let admin_api_key = get("ADMIN_API_KEY").map(Secret::new);
        let allow_open_mode = match get("ALLOW_OPEN_MODE").as_deref().map(str::trim) {
            None | Some("false") | Some("0") => false,
            Some("true") | Some("1") => true,
            Some(_) => return Err(ConfigError("ALLOW_OPEN_MODE must be true or false".into())),
        };
        let open_mode = lab_service_token.is_none() && admin_api_key.is_none();
        if open_mode && !allow_open_mode && !listen_addr.ip().is_loopback() {
            return Err(ConfigError(format!(
                "no management auth is configured and the API would listen on {listen_addr}: \
                 set ADMIN_API_KEY (or LAB_SERVICE_TOKEN), listen on a loopback address \
                 (LISTEN_ADDR=127.0.0.1:8080), or set ALLOW_OPEN_MODE=true for local \
                 development"
            )));
        }
        let default_mode = if lab_service_token.is_some() {
            PolicyMode::Public
        } else {
            PolicyMode::Any
        };
        let mut host_policy = HostPolicy::parse(
            get(host_policy::MODE_ENV).as_deref(),
            get(host_policy::ALLOWLIST_ENV).as_deref(),
            default_mode,
        )
        .map_err(ConfigError)?;
        if let Some(url) = &database_url {
            for endpoint in state_db_endpoints(url.expose()) {
                host_policy = host_policy.forbid_origin(endpoint);
            }
        }

        let shutdown_grace = secs(&get, "SHUTDOWN_GRACE_SECS", 30, 1, 600)?;
        Ok(Self {
            listen_addr,
            database_url,
            secret_key,
            admin_api_key,
            lab_service_token,
            open_mode,
            host_policy,
            lab_events,
            log_format,
            log_filter,
            usage_flush_interval: secs(&get, "USAGE_FLUSH_INTERVAL_SECS", 60, 1, 3600)?,
            lab_send_interval: secs(&get, "LAB_SEND_INTERVAL_SECS", 2, 1, 300)?,
            source_probe_timeout: secs(&get, "SOURCE_PROBE_TIMEOUT_SECS", 30, 1, 600)?,
            shutdown_grace,
            limits: Limits {
                stop_grace: shutdown_grace,
                max_syncs_per_tenant: num(&get, "MAX_SYNCS_PER_TENANT", 20, 1, 1_000_000)?,
                probe_concurrency: usize::try_from(num(&get, "PROBE_CONCURRENCY", 16, 1, 1024)?)
                    .unwrap_or(16),
                probe_concurrency_per_tenant: usize::try_from(num(
                    &get,
                    "PROBE_CONCURRENCY_PER_TENANT",
                    4,
                    1,
                    1024,
                )?)
                .unwrap_or(4),
                request_timeout: secs(&get, "REQUEST_TIMEOUT_SECS", 180, 5, 3600)?,
                dead_letters_max_per_sync: num(
                    &get,
                    "DEAD_LETTERS_MAX_PER_SYNC",
                    10_000,
                    0,
                    10_000_000,
                )?,
                outbox_max_pending: num(&get, "LAB_OUTBOX_MAX_PENDING", 100_000, 1, 100_000_000)?,
            },
        })
    }
}

/// The servers a libpq URL names: every `host[:port]` of the authority
/// (comma-separated lists included) and a `host=` query parameter. Unix
/// socket paths are skipped (a source can never name one).
pub fn state_db_endpoints(url: &str) -> Vec<Endpoint> {
    let Some((_, rest)) = url.trim().split_once("://") else {
        return Vec::new();
    };
    let (authority, query) = match rest.split_once('/') {
        Some((a, path)) => (a, path.split_once('?').map(|(_, q)| q)),
        None => match rest.split_once('?') {
            Some((a, q)) => (a, Some(q)),
            None => (rest, None),
        },
    };
    let hosts = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let mut port_param = None;
    let mut host_param = None;
    for pair in query.unwrap_or_default().split('&') {
        match pair.split_once('=') {
            Some(("host", v)) => host_param = Some(v.to_owned()),
            Some(("port", v)) => port_param = v.parse::<u16>().ok(),
            _ => {}
        }
    }
    let default_port = port_param.unwrap_or(5432);
    let mut out: Vec<Endpoint> = hosts
        .split(',')
        .filter_map(|h| Endpoint::parse(h, default_port))
        .collect();
    if let Some(h) = host_param.filter(|h| !h.starts_with('/') && !h.starts_with("%2F")) {
        out.extend(
            h.split(',')
                .filter_map(|h| Endpoint::parse(h, default_port)),
        );
    }
    out.retain(|e| !e.host.is_empty());
    out
}

fn num(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, ConfigError> {
    let Some(raw) = get(name) else {
        return Ok(default);
    };
    match raw.trim().parse::<u64>() {
        Ok(v) if (min..=max).contains(&v) => Ok(v),
        _ => Err(ConfigError(format!(
            "{name} must be a number between {min} and {max}"
        ))),
    }
}

fn secs(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<Duration, ConfigError> {
    let Some(raw) = get(name) else {
        return Ok(Duration::from_secs(default));
    };
    match raw.trim().parse::<u64>() {
        Ok(v) if (min..=max).contains(&v) => Ok(Duration::from_secs(v)),
        _ => Err(ConfigError(format!(
            "{name} must be a number of seconds between {min} and {max}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| map.get(k).cloned())
    }

    #[test]
    fn defaults_are_standalone_in_memory() {
        let c = cfg(&[("ADMIN_API_KEY", "k")]).unwrap();
        assert_eq!(c.listen_addr.to_string(), "0.0.0.0:8080");
        assert!(!c.open_mode);
        assert!(c.database_url.is_none());
        assert!(c.lab_events.is_none());
        assert!(c.lab_service_token.is_none());
        assert_eq!(c.usage_flush_interval, Duration::from_secs(60));
        assert_eq!(c.log_format, LogFormat::Pretty);
    }

    #[test]
    fn open_mode_needs_loopback_or_an_explicit_opt_in() {
        // No auth on a public address: refused.
        let err = cfg(&[]).unwrap_err();
        assert!(err.0.contains("ALLOW_OPEN_MODE"), "{err}");
        assert!(cfg(&[("LISTEN_ADDR", "0.0.0.0:9000")]).is_err());
        assert!(cfg(&[("LISTEN_ADDR", "[::]:9000")]).is_err());
        // Loopback, or the explicit opt-in.
        assert!(cfg(&[("LISTEN_ADDR", "127.0.0.1:8080")]).unwrap().open_mode);
        assert!(cfg(&[("LISTEN_ADDR", "[::1]:8080")]).unwrap().open_mode);
        assert!(cfg(&[("ALLOW_OPEN_MODE", "true")]).unwrap().open_mode);
        assert!(cfg(&[("ALLOW_OPEN_MODE", "maybe")]).is_err());
        // Any token: not open.
        assert!(!cfg(&[("LAB_SERVICE_TOKEN", "t")]).unwrap().open_mode);
    }

    #[test]
    fn a_blank_auth_variable_is_an_error_not_open_mode() {
        for name in ["ADMIN_API_KEY", "LAB_SERVICE_TOKEN"] {
            for blank in ["", "  "] {
                let err = cfg(&[(name, blank), ("ALLOW_OPEN_MODE", "true")]).unwrap_err();
                assert!(err.0.contains(name), "{err}");
            }
        }
    }

    #[test]
    fn database_needs_the_secret_key() {
        let err = cfg(&[("DATABASE_URL", "postgres://x"), ("ADMIN_API_KEY", "k")]).unwrap_err();
        assert!(err.0.contains("CONNECT_SECRET_KEY"));
        assert!(
            cfg(&[
                ("DATABASE_URL", "postgres://x"),
                ("CONNECT_SECRET_KEY", "k"),
                ("ADMIN_API_KEY", "k"),
            ])
            .unwrap()
            .database_url
            .is_some()
        );
    }

    #[test]
    fn lab_event_variables_go_together() {
        let cfg = |pairs: &[(&str, &str)]| {
            let mut all = pairs.to_vec();
            all.push(("ADMIN_API_KEY", "k"));
            cfg(&all)
        };
        assert!(cfg(&[("LAB_URL", "https://lab")]).is_err());
        assert!(cfg(&[("LAB_URL", "https://lab"), ("LAB_INSTANCE_ID", "i")]).is_err());
        let c = cfg(&[
            ("LAB_URL", "https://lab/"),
            ("LAB_INSTANCE_ID", "i"),
            ("LAB_INSTANCE_SECRET", "s"),
        ])
        .unwrap();
        let lab = c.lab_events.unwrap();
        assert_eq!(lab.events_url(), "https://lab/internal/events");
        assert!(
            cfg(&[
                ("LAB_URL", "ftp://lab"),
                ("LAB_INSTANCE_ID", "i"),
                ("LAB_INSTANCE_SECRET", "s"),
            ])
            .is_err()
        );
    }

    #[test]
    fn secrets_never_print() {
        let c = cfg(&[
            ("ADMIN_API_KEY", "admin-hunter2"),
            ("LAB_SERVICE_TOKEN", "lab-hunter2"),
            ("DATABASE_URL", "postgres://u:pw-hunter2@h/db"),
            ("CONNECT_SECRET_KEY", "key-hunter2"),
            ("LAB_URL", "https://lab"),
            ("LAB_INSTANCE_ID", "i"),
            ("LAB_INSTANCE_SECRET", "inst-hunter2"),
        ])
        .unwrap();
        assert!(!format!("{c:?}").contains("hunter2"));
    }

    #[test]
    fn host_policy_defaults_follow_the_lab_token() {
        assert!(cfg(&[("ADMIN_API_KEY", "k")]).unwrap().host_policy.is_any());
        let lab = cfg(&[("LAB_SERVICE_TOKEN", "t")]).unwrap();
        assert_eq!(lab.host_policy.mode_name(), "public");
        let explicit = cfg(&[("LAB_SERVICE_TOKEN", "t"), ("CONNECT_HOST_POLICY", "any")]).unwrap();
        assert!(explicit.host_policy.is_any());
        let list = cfg(&[
            ("CONNECT_HOST_POLICY", "allowlist"),
            ("CONNECT_HOST_ALLOWLIST", "db.internal:5432,10.0.0.0/8"),
            ("ADMIN_API_KEY", "k"),
        ])
        .unwrap();
        assert_eq!(list.host_policy.mode_name(), "allowlist");
        assert!(cfg(&[("CONNECT_HOST_POLICY", "allowlist"), ("ADMIN_API_KEY", "k")]).is_err());
        assert!(cfg(&[("CONNECT_HOST_POLICY", "open"), ("ADMIN_API_KEY", "k")]).is_err());
    }

    #[tokio::test]
    async fn the_state_database_is_never_an_origin() {
        let c = cfg(&[
            ("DATABASE_URL", "postgres://u:pw@127.0.0.1:6543/connect"),
            ("CONNECT_SECRET_KEY", "k"),
            ("ADMIN_API_KEY", "k"),
        ])
        .unwrap();
        let refused = c
            .host_policy
            .vet_origin(&Endpoint::new("localhost", 6543))
            .await;
        assert!(refused.is_err());
        assert!(
            c.host_policy
                .vet_origin(&Endpoint::new("127.0.0.1", 5432))
                .await
                .is_ok()
        );
    }

    #[test]
    fn state_db_urls_name_their_servers() {
        assert_eq!(
            state_db_endpoints("postgres://u:p@db:5433/x?sslmode=disable"),
            vec![Endpoint::new("db", 5433)]
        );
        assert_eq!(
            state_db_endpoints("postgresql://a,b:6000/x"),
            vec![Endpoint::new("a", 5432), Endpoint::new("b", 6000)]
        );
        assert_eq!(
            state_db_endpoints("postgres://u@[::1]/x"),
            vec![Endpoint::new("::1", 5432)]
        );
        assert_eq!(
            state_db_endpoints("postgres:///x?host=pg.internal&port=7000"),
            vec![Endpoint::new("pg.internal", 7000)]
        );
        assert_eq!(state_db_endpoints("postgres:///x?host=/var/run"), vec![]);
    }

    #[test]
    fn bad_values_are_refused() {
        let cfg = |pairs: &[(&str, &str)]| {
            let mut all = pairs.to_vec();
            all.push(("ADMIN_API_KEY", "k"));
            cfg(&all)
        };
        assert!(cfg(&[("LISTEN_ADDR", "nope")]).is_err());
        assert!(cfg(&[("LOG_FORMAT", "xml")]).is_err());
        assert!(cfg(&[("USAGE_FLUSH_INTERVAL_SECS", "0")]).is_err());
        assert!(cfg(&[("MAX_SYNCS_PER_TENANT", "0")]).is_err());
        assert!(cfg(&[("REQUEST_TIMEOUT_SECS", "1")]).is_err());
        let c = cfg(&[
            ("MAX_SYNCS_PER_TENANT", "5"),
            ("PROBE_CONCURRENCY", "8"),
            ("PROBE_CONCURRENCY_PER_TENANT", "2"),
            ("DEAD_LETTERS_MAX_PER_SYNC", "100"),
            ("LAB_OUTBOX_MAX_PENDING", "1000"),
            ("REQUEST_TIMEOUT_SECS", "30"),
        ])
        .unwrap();
        assert_eq!(
            c.limits,
            Limits {
                max_syncs_per_tenant: 5,
                probe_concurrency: 8,
                probe_concurrency_per_tenant: 2,
                request_timeout: Duration::from_secs(30),
                dead_letters_max_per_sync: 100,
                outbox_max_pending: 1000,
                stop_grace: Duration::from_secs(30),
            }
        );
        assert_eq!(cfg(&[]).unwrap().limits, Limits::default());
        assert_eq!(
            cfg(&[("USAGE_FLUSH_INTERVAL_SECS", "5")])
                .unwrap()
                .usage_flush_interval,
            Duration::from_secs(5)
        );
    }
}
