//! librdkafka plumbing: client context, client creation, error mapping and
//! the blocking metadata calls (run on the blocking pool, never on the
//! async runtime).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use meili_connect_core::SourceError;
use rdkafka::ClientConfig;
use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;

use crate::checkpoint::Watermarks;
use crate::config::KafkaConfig;

pub type KafkaConsumer = StreamConsumer<Ctx>;

/// Routes librdkafka logs to `tracing` and remembers the last global error,
/// so a metadata timeout can be explained (authentication failed, TLS
/// handshake failed, brokers unreachable).
#[derive(Default)]
pub struct Ctx {
    last_error: Mutex<Option<RDKafkaErrorCode>>,
}

impl Ctx {
    pub fn last_error(&self) -> Option<RDKafkaErrorCode> {
        *self.last_error.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl ClientContext for Ctx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => {
                tracing::error!(target: "librdkafka", facility = fac, "{message}")
            }
            RDKafkaLogLevel::Warning => {
                tracing::warn!(target: "librdkafka", facility = fac, "{message}")
            }
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                tracing::info!(target: "librdkafka", facility = fac, "{message}")
            }
            RDKafkaLogLevel::Debug => {
                tracing::debug!(target: "librdkafka", facility = fac, "{message}")
            }
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if let Some(code) = error.rdkafka_error_code() {
            *self.last_error.lock().unwrap_or_else(|e| e.into_inner()) = Some(code);
        }
        tracing::warn!(target: "librdkafka", "{error}: {reason}");
    }
}

impl ConsumerContext for Ctx {}

/// Builds a consumer. Creating it does not connect: librdkafka connects in
/// the background on the first request.
pub fn consumer(config: &KafkaConfig, sync_id: &str) -> Result<Arc<KafkaConsumer>, SourceError> {
    let consumer: KafkaConsumer = client_config(config, sync_id)
        .create_with_context(Ctx::default())
        .map_err(|e| map_error(config, "create the Kafka client", &e, None))?;
    Ok(Arc::new(consumer))
}

pub fn client_config(config: &KafkaConfig, sync_id: &str) -> ClientConfig {
    let mut cc = ClientConfig::new();
    for (k, v) in config.client_properties(sync_id) {
        cc.set(k, v);
    }
    cc
}

/// What the engine must do about a client error. Secrets are scrubbed from
/// the message.
pub fn map_error(
    config: &KafkaConfig,
    what: &str,
    error: &KafkaError,
    last_global: Option<RDKafkaErrorCode>,
) -> SourceError {
    let message = match error {
        // The librdkafka description may quote the property value.
        KafkaError::ClientConfig(_, _, key, _) => {
            return SourceError::Config(format!("{what}: invalid client property `{key}`"));
        }
        KafkaError::ClientCreation(msg) => {
            return SourceError::Config(scrub(config, &format!("{what}: {msg}")));
        }
        other => other.to_string(),
    };
    let code = error.rdkafka_error_code();
    // A timeout or transport error during a request is often the visible
    // face of an authentication or TLS failure reported globally.
    let effective = match (code, last_global) {
        (Some(c), Some(g)) if is_generic(c) && classify(g) != Class::Transient => Some(g),
        (None, Some(g)) if classify(g) != Class::Transient => Some(g),
        _ => code,
    };
    let detail = match effective {
        Some(c) if Some(c) != code => format!("{message} ({c})"),
        _ => message,
    };
    let text = scrub(config, &format!("{what}: {detail}"));
    match effective.map(classify).unwrap_or(Class::Transient) {
        Class::Config => SourceError::Config(text),
        Class::PositionLost => SourceError::PositionLost(text),
        Class::Transient => SourceError::Transient(text),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    Config,
    PositionLost,
    Transient,
}

pub fn classify(code: RDKafkaErrorCode) -> Class {
    use RDKafkaErrorCode as E;
    match code {
        E::Authentication
        | E::SaslAuthenticationFailed
        | E::UnsupportedSASLMechanism
        | E::IllegalSASLState
        | E::TopicAuthorizationFailed
        | E::GroupAuthorizationFailed
        | E::ClusterAuthorizationFailed
        | E::UnknownTopicOrPartition
        | E::UnknownTopic
        | E::UnknownPartition
        | E::InvalidTopic
        | E::SSL
        | E::InvalidArgument
        | E::InvalidConfig => Class::Config,
        E::OffsetOutOfRange | E::AutoOffsetReset => Class::PositionLost,
        _ => Class::Transient,
    }
}

fn is_generic(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode as E;
    matches!(
        code,
        E::OperationTimedOut
            | E::RequestTimedOut
            | E::BrokerTransportFailure
            | E::AllBrokersDown
            | E::TimedOutQueue
    )
}

/// Errors librdkafka recovers from on its own while consuming: logged, not
/// returned.
pub fn is_self_healing(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode as E;
    matches!(
        code,
        E::BrokerTransportFailure
            | E::Resolve
            | E::LeaderNotAvailable
            | E::NotLeaderForPartition
            | E::RequestTimedOut
            | E::OperationTimedOut
            | E::TimedOutQueue
            | E::PartitionEOF
            | E::CoordinatorNotAvailable
            | E::CoordinatorLoadInProgress
            | E::NotCoordinator
            | E::WaitingForCoordinator
    )
}

/// Replaces any secret value that could have slipped into a message.
pub fn scrub(config: &KafkaConfig, text: &str) -> String {
    let mut out = text.to_owned();
    for secret in config.secret_values() {
        if secret.len() >= 3 {
            out = out.replace(secret, "<redacted>");
        }
    }
    out
}

/// Partitions of each topic, from a metadata request per topic. A topic
/// that does not exist or cannot be read is `Config`.
pub fn topic_partitions(
    consumer: &KafkaConsumer,
    config: &KafkaConfig,
    topics: &[String],
) -> Result<BTreeMap<String, Vec<i32>>, SourceError> {
    let timeout = config.request_timeout();
    let mut out = BTreeMap::new();
    for topic in topics {
        out.insert(
            topic.clone(),
            partitions_of(consumer, config, topic, timeout)?,
        );
    }
    Ok(out)
}

pub fn partitions_of(
    consumer: &KafkaConsumer,
    config: &KafkaConfig,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, SourceError> {
    let last = || consumer.context().last_error();
    let md = consumer.fetch_metadata(Some(topic), timeout).map_err(|e| {
        map_error(
            config,
            &format!("read metadata of topic `{topic}`"),
            &e,
            last(),
        )
    })?;
    let Some(t) = md.topics().iter().find(|t| t.name() == topic) else {
        return Err(SourceError::Config(format!(
            "topic `{topic}` does not exist"
        )));
    };
    if let Some(err) = t.error() {
        let code = RDKafkaErrorCode::from(err);
        return Err(match code {
            RDKafkaErrorCode::UnknownTopicOrPartition | RDKafkaErrorCode::UnknownTopic => {
                SourceError::Config(format!("topic `{topic}` does not exist"))
            }
            RDKafkaErrorCode::TopicAuthorizationFailed => {
                SourceError::Config(format!("not authorized to read topic `{topic}`"))
            }
            other => match classify(other) {
                Class::Config => SourceError::Config(format!("topic `{topic}`: {other}")),
                _ => SourceError::Transient(format!("topic `{topic}`: {other}")),
            },
        });
    }
    let mut partitions: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
    if partitions.is_empty() {
        return Err(SourceError::Transient(format!(
            "topic `{topic}` has no partitions yet"
        )));
    }
    partitions.sort_unstable();
    Ok(partitions)
}

pub fn watermarks(
    consumer: &KafkaConsumer,
    config: &KafkaConfig,
    topic: &str,
    partition: i32,
) -> Result<Watermarks, SourceError> {
    let (low, high) = consumer
        .fetch_watermarks(topic, partition, config.request_timeout())
        .map_err(|e| {
            map_error(
                config,
                &format!("read offsets of {topic}/{partition}"),
                &e,
                consumer.context().last_error(),
            )
        })?;
    Ok(Watermarks { low, high })
}

/// Watermarks of every partition of the given topics.
pub fn all_watermarks(
    consumer: &KafkaConsumer,
    config: &KafkaConfig,
    topics: &[String],
) -> Result<BTreeMap<String, BTreeMap<i32, Watermarks>>, SourceError> {
    let mut out = BTreeMap::new();
    for (topic, partitions) in topic_partitions(consumer, config, topics)? {
        let mut wms = BTreeMap::new();
        for p in partitions {
            wms.insert(p, watermarks(consumer, config, &topic, p)?);
        }
        out.insert(topic, wms);
    }
    Ok(out)
}

/// Runs a blocking librdkafka call on the blocking pool.
pub async fn blocking<T, F>(f: F) -> Result<T, SourceError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SourceError> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| SourceError::Fatal(format!("blocking Kafka call panicked: {e}")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> KafkaConfig {
        KafkaConfig::from_value(&json!({"bootstrap_servers": ["b:1"], "security": {
            "protocol": "sasl_plaintext",
            "sasl": {"mechanism": "PLAIN", "username": "u", "password": "hunter2"}}}))
        .unwrap()
    }

    #[test]
    fn classification() {
        use RDKafkaErrorCode as E;
        for c in [
            E::Authentication,
            E::SaslAuthenticationFailed,
            E::TopicAuthorizationFailed,
            E::UnknownTopicOrPartition,
        ] {
            assert_eq!(classify(c), Class::Config, "{c:?}");
        }
        for c in [E::OffsetOutOfRange, E::AutoOffsetReset] {
            assert_eq!(classify(c), Class::PositionLost);
        }
        for c in [
            E::AllBrokersDown,
            E::LeaderNotAvailable,
            E::RequestTimedOut,
            E::OperationTimedOut,
            E::BrokerTransportFailure,
        ] {
            assert_eq!(classify(c), Class::Transient);
        }
    }

    #[test]
    fn timeouts_explained_by_a_global_auth_error_are_config() {
        let c = cfg();
        let e = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        assert!(matches!(
            map_error(&c, "x", &e, None),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            map_error(&c, "x", &e, Some(RDKafkaErrorCode::Authentication)),
            SourceError::Config(_)
        ));
        assert!(matches!(
            map_error(&c, "x", &e, Some(RDKafkaErrorCode::BrokerTransportFailure)),
            SourceError::Transient(_)
        ));
        let e = KafkaError::MessageConsumption(RDKafkaErrorCode::AutoOffsetReset);
        assert!(matches!(
            map_error(&c, "x", &e, None),
            SourceError::PositionLost(_)
        ));
    }

    #[test]
    fn client_config_errors_never_echo_values() {
        let c = cfg();
        let e = KafkaError::ClientConfig(
            rdkafka::types::RDKafkaConfRes::RD_KAFKA_CONF_INVALID,
            "Invalid value hunter2".into(),
            "sasl.password".into(),
            "hunter2".into(),
        );
        let SourceError::Config(m) = map_error(&c, "create", &e, None) else {
            panic!()
        };
        assert!(!m.contains("hunter2"));
        let e = KafkaError::ClientCreation("bad thing hunter2".into());
        let SourceError::Config(m) = map_error(&c, "create", &e, None) else {
            panic!()
        };
        assert!(!m.contains("hunter2"), "{m}");
    }
}
