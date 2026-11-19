//! The connector's typed configuration (`SourceSpec.config`) and the
//! connection parameters resolved from it.

use meili_connect_core::{Secret, SourceError, SourceKind};
use serde::{Deserialize, Serialize};

use crate::ident;

/// TLS mode, libpq names. `prefer` and `allow` are accepted from URLs and
/// treated as `require`: a replication client cannot silently fall back to
/// plain text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    Disable,
    #[default]
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    fn parse(value: &str) -> Result<Self, SourceError> {
        match value {
            "disable" => Ok(SslMode::Disable),
            "allow" | "prefer" | "require" => Ok(SslMode::Require),
            "verify-ca" => Ok(SslMode::VerifyCa),
            "verify-full" => Ok(SslMode::VerifyFull),
            other => Err(SourceError::Config(format!(
                "unsupported sslmode '{other}': use disable, require, verify-ca or verify-full"
            ))),
        }
    }
}

/// `SourceSpec.config` for `postgres` and `supabase`. Either `url` or the
/// discrete fields; discrete fields override the URL's parts when both are
/// given.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// `postgres://user:password@host:port/database?sslmode=require`.
    #[serde(default)]
    pub url: Option<Secret>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<Secret>,
    #[serde(default)]
    pub sslmode: Option<SslMode>,
    /// PEM-encoded CA certificate(s) for `verify-ca` / `verify-full`.
    /// Without it the Mozilla roots are used.
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Override the replication slot name (default `meili_connect_<sync_id>`).
    #[serde(default)]
    pub slot_name: Option<String>,
    /// Override the publication name (default `meili_connect_<sync_id>`).
    #[serde(default)]
    pub publication_name: Option<String>,
}

/// Everything needed to open a connection, resolved and validated.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnParams {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Secret,
    pub sslmode: SslMode,
    pub ca_cert: Option<String>,
    pub supabase: bool,
}

/// The full, validated settings of one source.
#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub conn: ConnParams,
    pub slot: String,
    pub publication: String,
    /// The sync these objects belong to: their ownership marker.
    pub sync_id: String,
}

impl PostgresConfig {
    pub fn from_value(value: &serde_json::Value) -> Result<Self, SourceError> {
        // serde's message names the offending field, never its value for
        // these types, but stay conservative: no echo of the input.
        serde_json::from_value(value.clone()).map_err(|e| {
            SourceError::Config(format!(
                "invalid postgres source config: {}",
                scrub_serde(&e)
            ))
        })
    }

    pub fn resolve(&self, kind: SourceKind, sync_id: &str) -> Result<Settings, SourceError> {
        let supabase = kind == SourceKind::Supabase;
        let mut host = None;
        let mut port = None;
        let mut database = None;
        let mut user = None;
        let mut password = None;
        let mut sslmode = None;

        if let Some(url) = &self.url {
            let parsed = parse_url(url.expose())?;
            host = parsed.host;
            port = parsed.port;
            database = parsed.database;
            user = parsed.user;
            password = parsed.password;
            sslmode = parsed.sslmode;
        }
        let host = self
            .host
            .clone()
            .or(host)
            .filter(|h| !h.is_empty())
            .ok_or_else(|| SourceError::Config("postgres source: 'host' is required".into()))?;
        let port = self.port.or(port).unwrap_or(5432);
        let user = self
            .user
            .clone()
            .or(user)
            .filter(|u| !u.is_empty())
            .ok_or_else(|| SourceError::Config("postgres source: 'user' is required".into()))?;
        let database = self
            .database
            .clone()
            .or(database)
            .filter(|d| !d.is_empty())
            .unwrap_or_else(|| user.clone());
        let password = self.password.clone().or(password).unwrap_or_default();
        let sslmode = self.sslmode.or(sslmode).unwrap_or_default();

        let supabase_host = is_supabase_host(&host);
        if is_supabase_pooler(&host, port, supabase || supabase_host) {
            return Err(SourceError::Config(
                "this is a Supabase connection pooler address, which cannot carry logical \
                 replication: use the direct connection (db.<project-ref>.supabase.co, port 5432)"
                    .into(),
            ));
        }
        if (supabase || supabase_host) && sslmode == SslMode::Disable {
            return Err(SourceError::Config(
                "Supabase requires TLS: set sslmode to require or verify-full".into(),
            ));
        }
        if matches!(sslmode, SslMode::VerifyCa | SslMode::VerifyFull)
            && let Some(ca) = &self.ca_cert
            && !ca.contains("-----BEGIN CERTIFICATE-----")
        {
            return Err(SourceError::Config(
                "ca_cert must be a PEM certificate (-----BEGIN CERTIFICATE-----)".into(),
            ));
        }

        let slot = match &self.slot_name {
            Some(name) => ident::validate_slot_name(name)?,
            None => ident::default_name(sync_id),
        };
        let publication = match &self.publication_name {
            Some(name) => ident::validate_publication_name(name)?,
            None => ident::default_name(sync_id),
        };

        Ok(Settings {
            conn: ConnParams {
                host,
                port,
                database,
                user,
                password,
                sslmode,
                ca_cert: self.ca_cert.clone(),
                supabase: supabase || supabase_host,
            },
            slot,
            publication,
            sync_id: sync_id.to_owned(),
        })
    }
}

#[derive(Debug, Default)]
struct ParsedUrl {
    host: Option<String>,
    port: Option<u16>,
    database: Option<String>,
    user: Option<String>,
    password: Option<Secret>,
    sslmode: Option<SslMode>,
}

/// Parse a libpq URL. Errors never include the URL: it carries the password.
fn parse_url(raw: &str) -> Result<ParsedUrl, SourceError> {
    let invalid = || {
        SourceError::Config(
            "invalid connection URL: expected postgres://user:password@host:port/database".into(),
        )
    };
    let url = url::Url::parse(raw).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err(invalid());
    }
    let decode = |s: &str| -> Result<String, SourceError> { percent_decode(s).ok_or_else(invalid) };
    let mut out = ParsedUrl {
        host: url
            .host_str()
            .map(|h| h.trim_start_matches('[').trim_end_matches(']').to_owned()),
        port: url.port(),
        ..ParsedUrl::default()
    };
    if !url.username().is_empty() {
        out.user = Some(decode(url.username())?);
    }
    if let Some(pw) = url.password() {
        out.password = Some(Secret::new(decode(pw)?));
    }
    let path = url.path().trim_start_matches('/');
    if !path.is_empty() {
        out.database = Some(decode(path)?);
    }
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "sslmode" => out.sslmode = Some(SslMode::parse(&value)?),
            "host" => out.host = Some(value.into_owned()),
            "port" => out.port = Some(value.parse().map_err(|_| invalid())?),
            "user" => out.user = Some(value.into_owned()),
            "dbname" => out.database = Some(value.into_owned()),
            // Accepted and ignored: they do not change what we connect to.
            _ => {}
        }
    }
    Ok(out)
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// `*.supabase.co` / `*.supabase.com` hosts.
pub fn is_supabase_host(host: &str) -> bool {
    let h = host.to_ascii_lowercase();
    h.ends_with(".supabase.co") || h.ends_with(".supabase.com")
}

/// Supabase's Supavisor pooler: `*.pooler.supabase.com`, or the transaction
/// pooler port 6543 on a Supabase project.
pub fn is_supabase_pooler(host: &str, port: u16, supabase: bool) -> bool {
    let h = host.to_ascii_lowercase();
    h.ends_with(".pooler.supabase.com") || (supabase && port == 6543)
}

fn scrub_serde(e: &serde_json::Error) -> String {
    // serde_json messages look like "unknown field `x`, expected ..." or
    // "invalid type: string \"...\", expected u16". The second form can echo
    // a value: keep only the part before the first quote.
    let msg = e.to_string();
    match msg.find('"') {
        Some(i) => format!("{}…", msg[..i].trim_end()),
        None => msg,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn resolve(v: serde_json::Value, kind: SourceKind) -> Result<Settings, SourceError> {
        PostgresConfig::from_value(&v)?.resolve(kind, "sync-1")
    }

    #[test]
    fn discrete_fields() {
        let s = resolve(
            json!({"host": "db.local", "port": 5433, "database": "app", "user": "u",
                   "password": "p", "sslmode": "disable"}),
            SourceKind::Postgres,
        )
        .unwrap();
        assert_eq!(s.conn.host, "db.local");
        assert_eq!(s.conn.port, 5433);
        assert_eq!(s.conn.database, "app");
        assert_eq!(s.conn.user, "u");
        assert_eq!(s.conn.password.expose(), "p");
        assert_eq!(s.conn.sslmode, SslMode::Disable);
        assert_eq!(s.slot, "meili_connect_sync_1");
        assert_eq!(s.publication, "meili_connect_sync_1");
        assert!(!s.conn.supabase);
    }

    #[test]
    fn url_with_encoded_password_and_overrides() {
        let s = resolve(
            json!({"url": "postgresql://al%40ice:p%2Fss%3Aword@[::1]:6000/my%20db?sslmode=verify-full&application_name=x",
                   "database": "other"}),
            SourceKind::Postgres,
        )
        .unwrap();
        assert_eq!(s.conn.host, "::1");
        assert_eq!(s.conn.port, 6000);
        assert_eq!(s.conn.user, "al@ice");
        assert_eq!(s.conn.password.expose(), "p/ss:word");
        assert_eq!(s.conn.database, "other");
        assert_eq!(s.conn.sslmode, SslMode::VerifyFull);
    }

    #[test]
    fn defaults() {
        let s = resolve(json!({"host": "h", "user": "u"}), SourceKind::Postgres).unwrap();
        assert_eq!(s.conn.port, 5432);
        assert_eq!(s.conn.database, "u");
        assert_eq!(s.conn.sslmode, SslMode::Require);
        assert!(s.conn.password.is_empty());
    }

    #[test]
    fn errors_never_echo_secrets() {
        let err = resolve(
            json!({"url": "mysql://u:hunter2@h/db"}),
            SourceKind::Postgres,
        )
        .unwrap_err();
        assert!(matches!(err, SourceError::Config(_)));
        assert!(!err.to_string().contains("hunter2"));

        let err = resolve(
            json!({"host": "h", "user": "u", "port": "hunter2"}),
            SourceKind::Postgres,
        )
        .unwrap_err();
        assert!(!err.to_string().contains("hunter2"), "{err}");

        let err = resolve(
            json!({"url": "postgres://u:hunter2@h/db?sslmode=nope"}),
            SourceKind::Postgres,
        )
        .unwrap_err();
        assert!(!err.to_string().contains("hunter2"));
    }

    #[test]
    fn missing_host_or_user() {
        assert!(resolve(json!({"user": "u"}), SourceKind::Postgres).is_err());
        assert!(resolve(json!({"host": "h"}), SourceKind::Postgres).is_err());
        assert!(
            resolve(
                json!({"host": "h", "user": "u", "bogus": 1}),
                SourceKind::Postgres
            )
            .is_err()
        );
    }

    #[test]
    fn debug_redacts() {
        let c = PostgresConfig::from_value(
            &json!({"url": "postgres://u:hunter2@h/db", "password": "hunter3"}),
        )
        .unwrap();
        let s = format!("{c:?}");
        assert!(!s.contains("hunter2") && !s.contains("hunter3"));
    }

    #[test]
    fn supabase_pooler_rejected() {
        let err = resolve(
            json!({"url": "postgres://postgres.abc:pw@aws-0-eu-central-1.pooler.supabase.com:5432/postgres"}),
            SourceKind::Supabase,
        )
        .unwrap_err();
        assert!(matches!(err, SourceError::Config(m) if m.contains("direct connection")));

        let err = resolve(
            json!({"host": "db.abc.supabase.co", "port": 6543, "user": "postgres"}),
            SourceKind::Supabase,
        )
        .unwrap_err();
        assert!(matches!(err, SourceError::Config(_)));

        // Port 6543 on a non-Supabase host is just a port.
        assert!(
            resolve(
                json!({"host": "h", "port": 6543, "user": "u"}),
                SourceKind::Postgres
            )
            .is_ok()
        );

        let ok = resolve(
            json!({"host": "db.abc.supabase.co", "user": "postgres", "password": "x"}),
            SourceKind::Postgres,
        )
        .unwrap();
        assert!(ok.conn.supabase);
    }

    #[test]
    fn supabase_host_detection() {
        assert!(is_supabase_host("db.abcd.supabase.co"));
        assert!(is_supabase_host("aws-0-us-east-1.pooler.supabase.com"));
        assert!(!is_supabase_host("supabase.co.evil.com"));
        assert!(is_supabase_pooler(
            "AWS-0-us-east-1.pooler.supabase.com",
            5432,
            false
        ));
        assert!(is_supabase_pooler("db.x.supabase.co", 6543, true));
        assert!(!is_supabase_pooler("db.x.supabase.co", 5432, true));
    }

    #[test]
    fn supabase_requires_tls() {
        let err = resolve(
            json!({"host": "db.abc.supabase.co", "user": "postgres", "sslmode": "disable"}),
            SourceKind::Supabase,
        )
        .unwrap_err();
        assert!(matches!(err, SourceError::Config(_)));
    }

    #[test]
    fn name_overrides_validated() {
        let s = resolve(
            json!({"host": "h", "user": "u", "slot_name": "my_slot", "publication_name": "MyPub"}),
            SourceKind::Postgres,
        )
        .unwrap();
        assert_eq!(s.slot, "my_slot");
        assert_eq!(s.publication, "MyPub");
        assert!(
            resolve(
                json!({"host": "h", "user": "u", "slot_name": "Bad-Slot"}),
                SourceKind::Postgres
            )
            .is_err()
        );
        assert!(
            resolve(
                json!({"host": "h", "user": "u", "publication_name": "supabase_realtime"}),
                SourceKind::Postgres
            )
            .is_err()
        );
    }
}
