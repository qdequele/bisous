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
use crate::tinybird::TinybirdConfig;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// Variables whose blank value is an error rather than "unset".
const AUTH_VARS: &[&str] = &[
    "ADMIN_API_KEY",
    "LAB_SERVICE_TOKEN",
    "METRICS_TOKEN",
    "TINYBIRD_TOKEN",
];

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
    /// The bearer token of `GET /metrics` (404 without it). Never one of
    /// the management tokens.
    pub metrics_token: Option<Secret>,
    /// No management auth: allowed only on a loopback address or with
    /// `ALLOW_OPEN_MODE=true`.
    pub open_mode: bool,
    pub lab_events: Option<LabEventsConfig>,
    /// Usage analytics (`TINYBIRD_URL` + `TINYBIRD_TOKEN`).
    pub tinybird: Option<TinybirdConfig>,
    pub log_format: LogFormat,
    /// `RUST_LOG` syntax; `LOG_LEVEL` is the simple form.
    pub log_filter: String,
    /// How often accumulated usage with documents or bytes becomes events.
    pub usage_flush_interval: Duration,
    /// How long a running sync's `sync_seconds` alone (an idle sync) stay
    /// pending before they become an event.
    pub usage_idle_flush: Duration,
    /// How long a sync's new phase must hold before `sync.state_changed`
    /// reports it (`crate::sync_events`).
    pub sync_events_settle: Duration,
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
    /// Lab mode: how often the teams' credits are checked
    /// (`CREDITS_CHECK_INTERVAL_SECS`).
    pub credits_check_interval: Duration,
    /// How long a credits answer is reused (`CREDITS_CACHE_FRESH_SECS`,
    /// 30 s) and kept quiet when the Lab cannot answer
    /// (`CREDITS_CACHE_STALE_SECS`, 300 s).
    pub credits_fresh: Duration,
    pub credits_stale: Duration,
    /// When a sync that makes no progress is reported as stuck
    /// (`MAX_LAG_HOLD_BYTES`, `MAX_LAG_HOLD_SECS`; off by default).
    pub lag_guard: LagGuard,
}

/// Past these, a sync that is not making progress gets a warning in its
/// status. Nothing is dropped automatically: freeing a customer's
/// replication slot or offsets loses its position, an operator decision.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct LagGuard {
    pub max_bytes: Option<u64>,
    pub max_seconds: Option<f64>,
}

/// A sync whose last checkpoint is older than this is not making progress.
pub const LAG_STALL_AFTER: Duration = Duration::from_secs(300);

impl LagGuard {
    /// The warning for a sync `lag` behind with no checkpoint for
    /// `since_checkpoint`, if it deserves one.
    pub fn warning(
        &self,
        lag: Option<&meili_connect_core::Lag>,
        since_checkpoint: Option<Duration>,
    ) -> Option<String> {
        let lag = lag?;
        if since_checkpoint.is_some_and(|d| d < LAG_STALL_AFTER) {
            return None;
        }
        let over_bytes = matches!((lag.bytes, self.max_bytes), (Some(b), Some(max)) if b > max);
        let over_secs = matches!((lag.seconds, self.max_seconds), (Some(s), Some(max)) if s > max);
        if !(over_bytes || over_secs) {
            return None;
        }
        let what = match (over_bytes, lag.bytes, lag.seconds) {
            (true, Some(b), _) => format!("{b} bytes of origin log"),
            (_, _, Some(s)) => format!("{s:.0} s"),
            _ => "too far".to_owned(),
        };
        Some(format!(
            "the sync is {what} behind its origin and has saved no checkpoint for {} min: the \
             origin keeps its log (WAL, binlog, oplog, offsets) for it meanwhile. Nothing is \
             dropped automatically; fix the cause, or pause or delete the sync to release it",
            since_checkpoint.map_or(0, |d| d.as_secs() / 60)
        ))
    }
}

impl Config {
    /// Lab mode (`LAB_SERVICE_TOKEN`) or `CONNECT_HOST_POLICY=public`: the
    /// server runs other people's syncs, which may never use its own
    /// identity, files or environment (`crate::ambient`).
    pub fn hosted(&self) -> bool {
        self.lab_service_token.is_some() || self.host_policy.mode_name() == "public"
    }

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

        let tinybird = match (get("TINYBIRD_URL"), get("TINYBIRD_TOKEN")) {
            (None, None) => None,
            (Some(url), Some(token)) => {
                let url = url.trim().trim_end_matches('/').to_owned();
                if !(url.starts_with("http://") || url.starts_with("https://")) {
                    return Err(ConfigError("TINYBIRD_URL must be an http(s) URL".into()));
                }
                Some(TinybirdConfig {
                    url,
                    token: Secret::new(token.trim()),
                })
            }
            _ => {
                return Err(ConfigError(
                    "TINYBIRD_URL and TINYBIRD_TOKEN go together: set both or neither".into(),
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
        let metrics_token = get("METRICS_TOKEN").map(Secret::new);
        if let Some(m) = &metrics_token
            && [&lab_service_token, &admin_api_key]
                .into_iter()
                .flatten()
                .any(|t| t.expose() == m.expose())
        {
            return Err(ConfigError(
                "METRICS_TOKEN must differ from ADMIN_API_KEY and LAB_SERVICE_TOKEN: the scraper \
                 must not hold a management token"
                    .into(),
            ));
        }
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
        let credits_fresh = secs(&get, "CREDITS_CACHE_FRESH_SECS", 30, 1, 3600)?;
        let credits_stale = secs(&get, "CREDITS_CACHE_STALE_SECS", 300, 1, 86_400)?;
        if credits_stale < credits_fresh {
            return Err(ConfigError(
                "CREDITS_CACHE_STALE_SECS must be at least CREDITS_CACHE_FRESH_SECS".into(),
            ));
        }
        Ok(Self {
            listen_addr,
            database_url,
            secret_key,
            admin_api_key,
            lab_service_token,
            metrics_token,
            open_mode,
            host_policy,
            lab_events,
            tinybird,
            log_format,
            log_filter,
            usage_flush_interval: secs(&get, "USAGE_FLUSH_INTERVAL_SECS", 60, 1, 3600)?,
            usage_idle_flush: secs(&get, "USAGE_IDLE_FLUSH_SECS", 3600, 1, 86_400)?,
            sync_events_settle: secs(&get, "SYNC_EVENTS_SETTLE_SECS", 30, 0, 3600)?,
            lab_send_interval: secs(&get, "LAB_SEND_INTERVAL_SECS", 2, 1, 300)?,
            source_probe_timeout: secs(&get, "SOURCE_PROBE_TIMEOUT_SECS", 30, 1, 600)?,
            shutdown_grace,
            credits_check_interval: secs(&get, "CREDITS_CHECK_INTERVAL_SECS", 15, 1, 3600)?,
            credits_fresh,
            credits_stale,
            lag_guard: LagGuard {
                max_bytes: get("MAX_LAG_HOLD_BYTES")
                    .map(|_| num(&get, "MAX_LAG_HOLD_BYTES", 0, 1, u64::MAX))
                    .transpose()?,
                max_seconds: get("MAX_LAG_HOLD_SECS")
                    .map(|_| num(&get, "MAX_LAG_HOLD_SECS", 0, 1, 365 * 86_400))
                    .transpose()?
                    .map(|s| s as f64),
            },
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
        assert_eq!(c.usage_idle_flush, Duration::from_secs(3600));
        assert_eq!(c.log_format, LogFormat::Pretty);
        let c = cfg(&[("ADMIN_API_KEY", "k"), ("USAGE_IDLE_FLUSH_SECS", "120")]).unwrap();
        assert_eq!(c.usage_idle_flush, Duration::from_secs(120));
        assert!(cfg(&[("ADMIN_API_KEY", "k"), ("USAGE_IDLE_FLUSH_SECS", "0")]).is_err());
        assert_eq!(c.sync_events_settle, Duration::from_secs(30));
        let c = cfg(&[("ADMIN_API_KEY", "k"), ("SYNC_EVENTS_SETTLE_SECS", "0")]).unwrap();
        assert_eq!(c.sync_events_settle, Duration::ZERO);
        assert!(cfg(&[("ADMIN_API_KEY", "k"), ("SYNC_EVENTS_SETTLE_SECS", "3601")]).is_err());
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
    fn a_lab_token_or_the_public_policy_means_hosted() {
        assert!(!cfg(&[("ADMIN_API_KEY", "k")]).unwrap().hosted());
        assert!(cfg(&[("LAB_SERVICE_TOKEN", "t")]).unwrap().hosted());
        assert!(
            cfg(&[("ADMIN_API_KEY", "k"), ("CONNECT_HOST_POLICY", "public")])
                .unwrap()
                .hosted()
        );
    }

    #[test]
    fn tinybird_variables_go_together() {
        let cfg = |pairs: &[(&str, &str)]| {
            let mut all = pairs.to_vec();
            all.push(("ADMIN_API_KEY", "k"));
            cfg(&all)
        };
        assert!(cfg(&[]).unwrap().tinybird.is_none());
        assert!(cfg(&[("TINYBIRD_URL", "https://api.tinybird.co")]).is_err());
        assert!(cfg(&[("TINYBIRD_TOKEN", "t")]).is_err());
        assert!(cfg(&[("TINYBIRD_URL", "ftp://x"), ("TINYBIRD_TOKEN", "t")]).is_err());
        let tb = cfg(&[
            ("TINYBIRD_URL", "https://api.eu-central-1.aws.tinybird.co/"),
            ("TINYBIRD_TOKEN", "p.tok-hunter2"),
        ])
        .unwrap()
        .tinybird
        .unwrap();
        assert_eq!(
            tb.events_url(),
            "https://api.eu-central-1.aws.tinybird.co/v0/events?name=connect_usage&wait=true"
        );
        assert!(!format!("{tb:?}").contains("hunter2"));
    }

    #[test]
    fn the_metrics_token_is_its_own() {
        assert!(
            cfg(&[("ADMIN_API_KEY", "k")])
                .unwrap()
                .metrics_token
                .is_none()
        );
        let c = cfg(&[("ADMIN_API_KEY", "k"), ("METRICS_TOKEN", "m")]).unwrap();
        assert_eq!(c.metrics_token.unwrap().expose(), "m");
        for clash in ["ADMIN_API_KEY", "LAB_SERVICE_TOKEN"] {
            let err = cfg(&[(clash, "same"), ("METRICS_TOKEN", "same")]).unwrap_err();
            assert!(err.0.contains("METRICS_TOKEN must differ"), "{err}");
        }
        let err = cfg(&[("ADMIN_API_KEY", "k"), ("METRICS_TOKEN", " ")]).unwrap_err();
        assert!(err.0.contains("METRICS_TOKEN"), "{err}");
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
    fn the_lag_guard_warns_only_about_a_stuck_sync_past_a_threshold() {
        let off = cfg(&[("ADMIN_API_KEY", "k")]).unwrap().lag_guard;
        assert_eq!(off, LagGuard::default());
        let lag = meili_connect_core::Lag {
            seconds: Some(7200.0),
            bytes: Some(5_000_000_000),
            records: None,
        };
        let stuck = Some(Duration::from_secs(3600));
        assert_eq!(off.warning(Some(&lag), stuck), None, "off by default");

        let g = cfg(&[
            ("ADMIN_API_KEY", "k"),
            ("MAX_LAG_HOLD_BYTES", "1000000000"),
            ("MAX_LAG_HOLD_SECS", "3600"),
        ])
        .unwrap()
        .lag_guard;
        let w = g.warning(Some(&lag), stuck).unwrap();
        assert!(
            w.contains("5000000000 bytes") && w.contains("Nothing is dropped"),
            "{w}"
        );
        // Making progress: no warning, however far behind.
        assert_eq!(g.warning(Some(&lag), Some(Duration::from_secs(10))), None);
        // Under both thresholds, or nothing known: no warning.
        let small = meili_connect_core::Lag {
            seconds: Some(5.0),
            bytes: Some(10),
            records: None,
        };
        assert_eq!(g.warning(Some(&small), stuck), None);
        assert_eq!(g.warning(None, stuck), None);
        assert!(cfg(&[("ADMIN_API_KEY", "k"), ("MAX_LAG_HOLD_SECS", "x")]).is_err());
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
