//! Process configuration, read once from the environment.
//!
//! Every variable is documented in `.env.example` and the README. Secrets
//! are wrapped in [`Secret`] as soon as they are read, so a `Debug` print of
//! [`Config`] never shows them.

use std::net::SocketAddr;
use std::time::Duration;

use meili_connect_core::Secret;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct ConfigError(pub String);

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
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Reads through `get` so tests do not touch the process environment.
    /// Blank values count as unset.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
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

        Ok(Self {
            listen_addr,
            database_url,
            secret_key,
            admin_api_key: get("ADMIN_API_KEY").map(Secret::new),
            lab_service_token: get("LAB_SERVICE_TOKEN").map(Secret::new),
            lab_events,
            log_format,
            log_filter,
            usage_flush_interval: secs(&get, "USAGE_FLUSH_INTERVAL_SECS", 60, 1, 3600)?,
            lab_send_interval: secs(&get, "LAB_SEND_INTERVAL_SECS", 2, 1, 300)?,
            source_probe_timeout: secs(&get, "SOURCE_PROBE_TIMEOUT_SECS", 30, 1, 600)?,
            shutdown_grace: secs(&get, "SHUTDOWN_GRACE_SECS", 30, 1, 600)?,
        })
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
        let c = cfg(&[]).unwrap();
        assert_eq!(c.listen_addr.to_string(), "0.0.0.0:8080");
        assert!(c.database_url.is_none());
        assert!(c.lab_events.is_none());
        assert!(c.admin_api_key.is_none() && c.lab_service_token.is_none());
        assert_eq!(c.usage_flush_interval, Duration::from_secs(60));
        assert_eq!(c.log_format, LogFormat::Pretty);
    }

    #[test]
    fn database_needs_the_secret_key() {
        let err = cfg(&[("DATABASE_URL", "postgres://x")]).unwrap_err();
        assert!(err.0.contains("CONNECT_SECRET_KEY"));
        assert!(
            cfg(&[
                ("DATABASE_URL", "postgres://x"),
                ("CONNECT_SECRET_KEY", "k")
            ])
            .unwrap()
            .database_url
            .is_some()
        );
    }

    #[test]
    fn lab_event_variables_go_together() {
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
    fn bad_values_are_refused() {
        assert!(cfg(&[("LISTEN_ADDR", "nope")]).is_err());
        assert!(cfg(&[("LOG_FORMAT", "xml")]).is_err());
        assert!(cfg(&[("USAGE_FLUSH_INTERVAL_SECS", "0")]).is_err());
        assert_eq!(
            cfg(&[("USAGE_FLUSH_INTERVAL_SECS", "5")])
                .unwrap()
                .usage_flush_interval,
            Duration::from_secs(5)
        );
    }
}
