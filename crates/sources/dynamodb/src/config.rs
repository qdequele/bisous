//! Typed connector settings, read from `SourceSpec.config`.

use std::time::Duration;

use meili_connect_core::{Secret, SourceError};
use serde::{Deserialize, Serialize};

/// Highest number of parallel Scan segments we accept. Each segment holds
/// one page (≤ 1 MB) in memory, so this also bounds snapshot memory.
pub const MAX_SEGMENTS: u32 = 64;

/// The DynamoDB connector configuration.
///
/// ```json
/// {
///   "region": "eu-west-1",
///   "endpoint_url": "http://localhost:8000",
///   "credentials": { "type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/meili", "external_id": "..." },
///   "scan": { "segments": 4, "page_limit": 500, "max_rcu_per_second": 200 },
///   "stream": { "max_concurrent_shards": 32 }
/// }
/// ```
///
/// Tables are not configured here: they come from the sync's stream
/// mappings (`collection` = table name).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoConfig {
    pub region: String,
    /// Custom endpoint (DynamoDB Local, LocalStack). Applied to DynamoDB and
    /// DynamoDB Streams only, never to STS.
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub credentials: CredentialsConfig,
    #[serde(default)]
    pub scan: ScanOptions,
    #[serde(default)]
    pub stream: StreamOptions,
}

/// How the connector authenticates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialsConfig {
    /// The AWS default provider chain of the host (env, profile, IMDS,
    /// ECS/EKS web identity).
    #[default]
    Default,
    /// A static access key.
    Static {
        access_key_id: String,
        secret_access_key: Secret,
        #[serde(default)]
        session_token: Option<Secret>,
    },
    /// Assume a role in the customer's account (cross-account, the hosted
    /// default). The base credentials are the host's default chain.
    AssumeRole {
        role_arn: String,
        #[serde(default)]
        external_id: Option<Secret>,
        /// Defaults to `meili-connect-<sync id>`.
        #[serde(default)]
        session_name: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ScanOptions {
    /// Parallel Scan segments (1 = sequential), at most [`MAX_SEGMENTS`].
    pub segments: u32,
    /// `Limit` per Scan page. `None` lets DynamoDB fill 1 MB pages.
    pub page_limit: Option<u32>,
    /// Cap on read capacity consumed by the snapshot, per table, in RCU/s.
    pub max_rcu_per_second: Option<f64>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            segments: 1,
            page_limit: None,
            max_rcu_per_second: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct StreamOptions {
    /// Shards read at the same time, across all tables.
    pub max_concurrent_shards: usize,
    /// Wait between two `GetRecords` on a shard that returned nothing.
    /// At least 1000 ms (DynamoDB Streams read limits).
    pub idle_poll_ms: u64,
    /// How often the shard list is re-described to find child shards.
    pub shard_refresh_secs: u64,
    /// Minimum gap between two emitted checkpoints.
    pub checkpoint_interval_ms: u64,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            max_concurrent_shards: 32,
            idle_poll_ms: 1000,
            shard_refresh_secs: 30,
            checkpoint_interval_ms: 1000,
        }
    }
}

impl StreamOptions {
    pub fn idle_poll(&self) -> Duration {
        Duration::from_millis(self.idle_poll_ms)
    }
    pub fn shard_refresh(&self) -> Duration {
        Duration::from_secs(self.shard_refresh_secs)
    }
    pub fn checkpoint_interval(&self) -> Duration {
        Duration::from_millis(self.checkpoint_interval_ms)
    }
}

impl DynamoConfig {
    /// Parse and validate. Errors are `Config` and never echo the input
    /// (it may hold secrets).
    pub fn from_value(value: &serde_json::Value) -> Result<Self, SourceError> {
        let config: DynamoConfig = serde_json::from_value(value.clone()).map_err(|e| {
            // serde messages name fields and expected types, never values.
            SourceError::Config(format!("invalid dynamodb source config: {e}"))
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), SourceError> {
        let bad = |m: &str| Err(SourceError::Config(m.to_owned()));
        if self.region.trim().is_empty() {
            return bad("`region` is required");
        }
        if let Some(url) = &self.endpoint_url
            && !(url.starts_with("http://") || url.starts_with("https://"))
        {
            return bad("`endpoint_url` must start with http:// or https://");
        }
        match &self.credentials {
            CredentialsConfig::Default => {}
            CredentialsConfig::Static {
                access_key_id,
                secret_access_key,
                ..
            } => {
                if access_key_id.trim().is_empty() || secret_access_key.is_empty() {
                    return bad("static credentials need `access_key_id` and `secret_access_key`");
                }
            }
            CredentialsConfig::AssumeRole { role_arn, .. } => {
                if !role_arn.starts_with("arn:") || !role_arn.contains(":role/") {
                    return bad(
                        "`role_arn` must be an IAM role ARN (arn:aws:iam::<account>:role/<name>)",
                    );
                }
            }
        }
        if self.scan.segments == 0 || self.scan.segments > MAX_SEGMENTS {
            return Err(SourceError::Config(format!(
                "`scan.segments` must be between 1 and {MAX_SEGMENTS}"
            )));
        }
        if self.scan.page_limit == Some(0) {
            return bad("`scan.page_limit` must be positive");
        }
        if let Some(rate) = self.scan.max_rcu_per_second
            && !(rate.is_finite() && rate > 0.0)
        {
            return bad("`scan.max_rcu_per_second` must be a positive number");
        }
        if self.stream.max_concurrent_shards == 0 {
            return bad("`stream.max_concurrent_shards` must be positive");
        }
        if self.stream.idle_poll_ms < 1000 {
            return bad("`stream.idle_poll_ms` must be at least 1000 (DynamoDB Streams limits)");
        }
        if self.stream.shard_refresh_secs == 0 || self.stream.shard_refresh_secs > 3600 {
            return bad("`stream.shard_refresh_secs` must be between 1 and 3600");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minimal_config_uses_defaults() {
        let c = DynamoConfig::from_value(&json!({"region": "eu-west-1"})).unwrap();
        assert_eq!(c.credentials, CredentialsConfig::Default);
        assert_eq!(c.scan, ScanOptions::default());
        assert_eq!(c.stream, StreamOptions::default());
        assert_eq!(c.endpoint_url, None);
    }

    #[test]
    fn static_and_assume_role_credentials() {
        let c = DynamoConfig::from_value(&json!({
            "region": "us-east-1",
            "endpoint_url": "http://localhost:8000",
            "credentials": {"type": "static", "access_key_id": "AKIA", "secret_access_key": "s3cr3t", "session_token": "tok"},
            "scan": {"segments": 4, "page_limit": 100, "max_rcu_per_second": 50.0},
            "stream": {"max_concurrent_shards": 4, "idle_poll_ms": 2000}
        }))
        .unwrap();
        match &c.credentials {
            CredentialsConfig::Static {
                secret_access_key,
                session_token,
                ..
            } => {
                assert_eq!(secret_access_key.expose(), "s3cr3t");
                assert_eq!(session_token.as_ref().map(|t| t.expose()), Some("tok"));
            }
            other => panic!("unexpected {other:?}"),
        }
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("s3cr3t") && !dbg.contains("tok\""), "{dbg}");
        assert_eq!(c.scan.segments, 4);
        assert_eq!(c.stream.idle_poll(), Duration::from_secs(2));

        let c = DynamoConfig::from_value(&json!({
            "region": "us-east-1",
            "credentials": {"type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/meili", "external_id": "ext-1"}
        }))
        .unwrap();
        assert!(matches!(
            c.credentials,
            CredentialsConfig::AssumeRole { .. }
        ));
        assert!(!format!("{c:?}").contains("ext-1"));
    }

    #[test]
    fn rejects_bad_configs_without_echoing_secrets() {
        let cases = [
            json!({}),
            json!({"region": ""}),
            json!({"region": "x", "endpoint_url": "localhost:8000"}),
            json!({"region": "x", "credentials": {"type": "static", "access_key_id": "", "secret_access_key": "hunter2"}}),
            json!({"region": "x", "credentials": {"type": "assume_role", "role_arn": "nope"}}),
            json!({"region": "x", "scan": {"segments": 0}}),
            json!({"region": "x", "scan": {"segments": 65}}),
            json!({"region": "x", "scan": {"max_rcu_per_second": -1.0}}),
            json!({"region": "x", "stream": {"idle_poll_ms": 10}}),
            json!({"region": "x", "stream": {"max_concurrent_shards": 0}}),
            json!({"region": "x", "unknown": true}),
            json!({"region": "x", "credentials": {"type": "magic", "secret_access_key": "hunter2"}}),
        ];
        for case in cases {
            match DynamoConfig::from_value(&case) {
                Err(SourceError::Config(m)) => assert!(!m.contains("hunter2"), "{m}"),
                other => panic!("{case} gave {other:?}"),
            }
        }
    }
}
