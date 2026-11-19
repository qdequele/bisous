//! The typed shape of `SourceSpec.config` for `kind: kafka`.

use std::time::Duration;

use meili_connect_core::{Secret, SourceError};
use serde::{Deserialize, Serialize};

/// Connector settings. Topics are not listed here: they come from the
/// stream mappings (`collection` = topic name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    /// `host:port` of one or more brokers.
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub security: Security,
    /// How record values are decoded, for every topic of the sync.
    #[serde(default)]
    pub format: Format,
    /// Where a fresh sync starts reading. Ignored when resuming.
    #[serde(default)]
    pub start_from: StartFrom,
    /// Value fields that form the document key, in order. When empty, the
    /// record key is used. Set it when record keys are missing or not
    /// meaningful. Deletes without a value (tombstones) are then keyed from
    /// the record key parsed as a JSON object holding these fields.
    #[serde(default)]
    pub key_fields: Vec<String>,
    /// Emit a checkpoint after this many records.
    #[serde(default = "default_checkpoint_every_records")]
    pub checkpoint_every_records: u64,
    /// Emit a checkpoint at least this often while positions move.
    #[serde(default = "default_checkpoint_interval_ms")]
    pub checkpoint_interval_ms: u64,
    /// How often the topics are re-read to pick up new partitions.
    #[serde(default = "default_partition_discovery_interval_ms")]
    pub partition_discovery_interval_ms: u64,
    /// Timeout of metadata and offset requests.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Upper bound of the client's prefetch buffer, in KiB. Bounds memory.
    #[serde(default = "default_max_buffer_kbytes")]
    pub max_buffer_kbytes: u32,
}

fn default_checkpoint_every_records() -> u64 {
    1_000
}
fn default_checkpoint_interval_ms() -> u64 {
    1_000
}
fn default_partition_discovery_interval_ms() -> u64 {
    30_000
}
fn default_request_timeout_ms() -> u64 {
    10_000
}
fn default_max_buffer_kbytes() -> u32 {
    16 * 1024
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    /// The record value is the document (a JSON object). A null value
    /// (tombstone) deletes it.
    #[default]
    Json,
    /// Debezium change events: the `{before, after, op}` envelope, with or
    /// without the `{schema, payload}` wrapper, or flattened rows from the
    /// `ExtractNewRecordState` transform (`__deleted`).
    Debezium,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    #[default]
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl Protocol {
    fn as_librdkafka(self) -> &'static str {
        match self {
            Protocol::Plaintext => "plaintext",
            Protocol::Ssl => "ssl",
            Protocol::SaslPlaintext => "sasl_plaintext",
            Protocol::SaslSsl => "sasl_ssl",
        }
    }

    fn uses_tls(self) -> bool {
        matches!(self, Protocol::Ssl | Protocol::SaslSsl)
    }

    fn uses_sasl(self) -> bool {
        matches!(self, Protocol::SaslPlaintext | Protocol::SaslSsl)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Security {
    #[serde(default)]
    pub protocol: Protocol,
    /// Only with `ssl` / `sasl_ssl`.
    #[serde(default)]
    pub tls: Option<Tls>,
    /// Required with `sasl_plaintext` / `sasl_ssl`.
    #[serde(default)]
    pub sasl: Option<Sasl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Tls {
    /// PEM CA bundle. The system trust store is used when absent.
    #[serde(default)]
    pub ca_pem: Option<String>,
    /// PEM client certificate, for mTLS. Requires `key_pem`.
    #[serde(default)]
    pub cert_pem: Option<String>,
    /// PEM private key of the client certificate.
    #[serde(default)]
    pub key_pem: Option<Secret>,
    /// Password of an encrypted `key_pem`.
    #[serde(default)]
    pub key_password: Option<Secret>,
    /// Check that the broker certificate matches its host name.
    #[serde(default = "default_true")]
    pub verify_hostname: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN", alias = "plain")]
    Plain,
    #[serde(
        rename = "SCRAM-SHA-256",
        alias = "scram-sha-256",
        alias = "scram_sha_256"
    )]
    ScramSha256,
    #[serde(
        rename = "SCRAM-SHA-512",
        alias = "scram-sha-512",
        alias = "scram_sha_512"
    )]
    ScramSha512,
}

impl SaslMechanism {
    fn as_librdkafka(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sasl {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: Secret,
}

impl KafkaConfig {
    /// Parses and validates `SourceSpec.config`. Error messages never echo
    /// values, only field paths.
    pub fn from_value(value: &serde_json::Value) -> Result<Self, SourceError> {
        let config: KafkaConfig = serde_json::from_value(value.clone())
            .map_err(|e| SourceError::Config(format!("invalid kafka config: {}", redact(&e))))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), SourceError> {
        let bad = |m: &str| Err(SourceError::Config(m.to_owned()));
        if self.bootstrap_servers.is_empty()
            || self.bootstrap_servers.iter().any(|s| s.trim().is_empty())
        {
            return bad("bootstrap_servers must list at least one host:port");
        }
        if self
            .bootstrap_servers
            .iter()
            .any(|s| s.contains(',') || s.contains(char::is_whitespace))
        {
            return bad("bootstrap_servers entries must be single host:port values");
        }
        if self.key_fields.iter().any(|f| f.is_empty()) {
            return bad("key_fields must not contain empty names");
        }
        if self.checkpoint_every_records == 0 {
            return bad("checkpoint_every_records must be at least 1");
        }
        if self.checkpoint_interval_ms == 0
            || self.partition_discovery_interval_ms == 0
            || self.request_timeout_ms == 0
        {
            return bad("intervals and timeouts must be greater than 0");
        }
        if self.max_buffer_kbytes < 1024 {
            return bad("max_buffer_kbytes must be at least 1024");
        }
        let sec = &self.security;
        match (sec.protocol.uses_sasl(), &sec.sasl) {
            (true, None) => return bad("security.sasl is required with a sasl_* protocol"),
            (false, Some(_)) => {
                return bad("security.sasl is only allowed with sasl_plaintext or sasl_ssl");
            }
            (true, Some(sasl)) if sasl.username.is_empty() || sasl.password.is_empty() => {
                return bad("security.sasl.username and password must not be empty");
            }
            _ => {}
        }
        if let Some(tls) = &sec.tls {
            if !sec.protocol.uses_tls() {
                return bad("security.tls is only allowed with ssl or sasl_ssl");
            }
            if tls.cert_pem.is_some() != tls.key_pem.is_some() {
                return bad("security.tls.cert_pem and key_pem must be set together");
            }
            if tls.key_password.is_some() && tls.key_pem.is_none() {
                return bad("security.tls.key_password requires key_pem");
            }
        }
        Ok(())
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub fn checkpoint_interval(&self) -> Duration {
        Duration::from_millis(self.checkpoint_interval_ms)
    }

    pub fn partition_discovery_interval(&self) -> Duration {
        Duration::from_millis(self.partition_discovery_interval_ms)
    }

    /// Every secret value, to scrub them from error messages.
    pub(crate) fn secret_values(&self) -> Vec<&str> {
        let mut out = Vec::new();
        if let Some(sasl) = &self.security.sasl {
            out.push(sasl.password.expose());
        }
        if let Some(tls) = &self.security.tls {
            out.extend(tls.key_pem.iter().map(Secret::expose));
            out.extend(tls.key_password.iter().map(Secret::expose));
        }
        out
    }

    /// librdkafka properties, secrets included. Never log the result.
    pub(crate) fn client_properties(&self, sync_id: &str) -> Vec<(&'static str, String)> {
        let mut props = vec![
            ("bootstrap.servers", self.bootstrap_servers.join(",")),
            ("client.id", format!("meili-connect-{sync_id}")),
            ("group.id", group_id(sync_id)),
            ("enable.auto.commit", "false".to_owned()),
            ("enable.auto.offset.store", "false".to_owned()),
            // A position the broker no longer has is reported, never
            // silently reset: it becomes `PositionLost`.
            ("auto.offset.reset", "error".to_owned()),
            ("enable.partition.eof", "false".to_owned()),
            ("allow.auto.create.topics", "false".to_owned()),
            ("socket.timeout.ms", self.request_timeout_ms.to_string()),
            (
                "queued.max.messages.kbytes",
                self.max_buffer_kbytes.to_string(),
            ),
            (
                "security.protocol",
                self.security.protocol.as_librdkafka().to_owned(),
            ),
        ];
        if let Some(sasl) = &self.security.sasl {
            props.push(("sasl.mechanism", sasl.mechanism.as_librdkafka().to_owned()));
            props.push(("sasl.username", sasl.username.clone()));
            props.push(("sasl.password", sasl.password.expose().to_owned()));
        }
        if let Some(tls) = &self.security.tls {
            if let Some(ca) = &tls.ca_pem {
                props.push(("ssl.ca.pem", ca.clone()));
            }
            if let Some(cert) = &tls.cert_pem {
                props.push(("ssl.certificate.pem", cert.clone()));
            }
            if let Some(key) = &tls.key_pem {
                props.push(("ssl.key.pem", key.expose().to_owned()));
            }
            if let Some(pw) = &tls.key_password {
                props.push(("ssl.key.password", pw.expose().to_owned()));
            }
            if !tls.verify_hostname {
                props.push(("ssl.endpoint.identification.algorithm", "none".to_owned()));
            }
        }
        props
    }
}

/// The consumer group offsets are committed under, for lag monitoring only.
pub fn group_id(sync_id: &str) -> String {
    format!("meili-connect-{sync_id}")
}

/// serde_json errors may quote the offending value (`invalid value: string
/// "hunter2"`). Keep only the category and position.
fn redact(e: &serde_json::Error) -> String {
    let msg = e.to_string();
    if msg.contains("unknown field") || msg.contains("missing field") {
        // `unknown field `x`, expected one of ...` names fields, not values.
        msg
    } else {
        format!(
            "{:?} error at line {} column {}",
            e.classify(),
            e.line(),
            e.column()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_config_has_defaults() {
        let c = KafkaConfig::from_value(&json!({"bootstrap_servers": ["b:9092"]})).unwrap();
        assert_eq!(c.format, Format::Json);
        assert_eq!(c.start_from, StartFrom::Earliest);
        assert_eq!(c.security.protocol, Protocol::Plaintext);
        assert!(c.key_fields.is_empty());
        assert_eq!(c.checkpoint_every_records, 1000);
    }

    #[test]
    fn full_config_parses() {
        let c = KafkaConfig::from_value(&json!({
            "bootstrap_servers": ["a:9093", "b:9093"],
            "format": "debezium",
            "start_from": "latest",
            "key_fields": ["tenant", "id"],
            "security": {
                "protocol": "sasl_ssl",
                "sasl": {"mechanism": "SCRAM-SHA-512", "username": "u", "password": "p"},
                "tls": {"ca_pem": "CA", "cert_pem": "CERT", "key_pem": "KEY", "verify_hostname": false}
            }
        }))
        .unwrap();
        assert_eq!(c.format, Format::Debezium);
        assert_eq!(c.start_from, StartFrom::Latest);
        let props = c.client_properties("s1");
        let get = |k: &str| props.iter().find(|(p, _)| *p == k).map(|(_, v)| v.as_str());
        assert_eq!(get("bootstrap.servers"), Some("a:9093,b:9093"));
        assert_eq!(get("group.id"), Some("meili-connect-s1"));
        assert_eq!(get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(get("sasl.password"), Some("p"));
        assert_eq!(get("ssl.key.pem"), Some("KEY"));
        assert_eq!(get("ssl.endpoint.identification.algorithm"), Some("none"));
        assert_eq!(get("auto.offset.reset"), Some("error"));
        assert_eq!(get("enable.auto.commit"), Some("false"));
    }

    #[test]
    fn mechanism_aliases() {
        for m in [
            "PLAIN",
            "plain",
            "scram-sha-256",
            "SCRAM-SHA-256",
            "scram_sha_512",
        ] {
            let v = json!({"bootstrap_servers": ["b:1"], "security": {"protocol": "sasl_plaintext",
                "sasl": {"mechanism": m, "username": "u", "password": "p"}}});
            KafkaConfig::from_value(&v).unwrap();
        }
    }

    #[test]
    fn secrets_are_redacted_in_debug() {
        let c = KafkaConfig::from_value(&json!({"bootstrap_servers": ["b:1"], "security": {
            "protocol": "sasl_ssl",
            "sasl": {"mechanism": "PLAIN", "username": "u", "password": "hunter2"},
            "tls": {"cert_pem": "C", "key_pem": "topsecretkey", "key_password": "kpw"}}}))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("hunter2"));
        assert!(!dbg.contains("topsecretkey"));
        assert!(!dbg.contains("kpw"));
    }

    #[test]
    fn invalid_configs_are_config_errors_without_values() {
        let cases = [
            json!({"bootstrap_servers": []}),
            json!({"bootstrap_servers": ["a:1,b:2"]}),
            json!({"bootstrap_servers": ["a:1"], "security": {"protocol": "sasl_ssl"}}),
            json!({"bootstrap_servers": ["a:1"], "security": {"protocol": "plaintext",
                "sasl": {"mechanism": "PLAIN", "username": "u", "password": "p"}}}),
            json!({"bootstrap_servers": ["a:1"], "security": {"protocol": "plaintext", "tls": {}}}),
            json!({"bootstrap_servers": ["a:1"], "security": {"protocol": "ssl", "tls": {"cert_pem": "x"}}}),
            json!({"bootstrap_servers": ["a:1"], "key_fields": [""]}),
            json!({"bootstrap_servers": ["a:1"], "checkpoint_every_records": 0}),
            json!({"bootstrap_servers": ["a:1"], "format": "avro"}),
            json!({"bootstrap_servers": ["a:1"], "bogus": 1}),
            json!({"bootstrap_servers": ["a:1"], "security": {"protocol": "sasl_ssl",
                "sasl": {"mechanism": "GSSAPI", "username": "u", "password": "hunter2"}}}),
        ];
        for case in cases {
            match KafkaConfig::from_value(&case) {
                Err(SourceError::Config(m)) => assert!(!m.contains("hunter2"), "{m}"),
                other => panic!("{case}: {other:?}"),
            }
        }
    }
}
