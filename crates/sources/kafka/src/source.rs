//! `KafkaSource`: one consumer per sync, manually assigned at the
//! checkpoint offsets. The checkpoint in our store is the source of truth;
//! offsets committed to the broker only serve lag monitoring.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use meili_connect_core::{
    AckReceiver, Capabilities, CheckReport, CheckStatus, Checkpoint, CollectionInfo, EventSender,
    Opened, Source, SourceError, SourceEvent,
};
use rdkafka::Message;
use rdkafka::Offset;
use rdkafka::TopicPartitionList;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::checkpoint::{self, Positions};
use crate::client::{self, Ctx, KafkaConsumer};
use crate::config::{KafkaConfig, StartFrom, group_id};
use crate::decode::{DecodeError, Decoded, Decoder};

/// Shown as the key of every topic by `discover`: Kafka has no schema, the
/// key is whatever the producer put in the record key.
pub const RECORD_KEY: &str = "<record key>";

pub struct KafkaSource {
    sync_id: String,
    config: KafkaConfig,
    topics: Vec<String>,
    decoder: Decoder,
    consumer: Option<Arc<KafkaConsumer>>,
    /// Set by `open`: the next offset to read per partition.
    positions: Option<Positions>,
    /// Positions changed by `open` (new partitions) and not yet saved.
    pending_checkpoint: bool,
    skipped: Arc<AtomicU64>,
}

impl KafkaSource {
    pub fn new(sync_id: &str, config: KafkaConfig, topics: Vec<String>) -> Self {
        let decoder = Decoder {
            format: config.format,
            key_fields: config.key_fields.clone(),
        };
        Self {
            sync_id: sync_id.to_owned(),
            config,
            topics,
            decoder,
            consumer: None,
            positions: None,
            pending_checkpoint: false,
            skipped: Arc::default(),
        }
    }

    /// Records skipped because they could not be decoded, since this value
    /// was built.
    pub fn skipped_records(&self) -> u64 {
        self.skipped.load(Ordering::Relaxed)
    }

    /// A shared handle on the skipped-records counter.
    pub fn skipped_counter(&self) -> Arc<AtomicU64> {
        self.skipped.clone()
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    fn consumer(&mut self) -> Result<Arc<KafkaConsumer>, SourceError> {
        if let Some(c) = &self.consumer {
            return Ok(c.clone());
        }
        let c = client::consumer(&self.config, &self.sync_id)?;
        self.consumer = Some(c.clone());
        Ok(c)
    }
}

#[async_trait]
impl Source for KafkaSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: false,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        let mut report = CheckReport::default();
        let consumer = match self.consumer() {
            Ok(c) => c,
            Err(e) => {
                report.push("client", CheckStatus::Failed, e.to_string());
                return Ok(report);
            }
        };
        let config = self.config.clone();
        let topics = self.topics.clone();
        let auth = match &config.security.sasl {
            Some(s) => format!("authenticated as `{}`", s.username),
            None => "no SASL authentication configured".to_owned(),
        };
        let items = client::blocking(move || {
            let mut items = Vec::new();
            match consumer.fetch_metadata(None, config.request_timeout()) {
                Ok(md) => {
                    items.push((
                        "brokers",
                        CheckStatus::Ok,
                        format!("{} broker(s) reachable", md.brokers().len()),
                    ));
                    items.push(("authentication", CheckStatus::Ok, auth));
                }
                Err(e) => {
                    let last = consumer.context().last_error();
                    let (name, msg) = explain_connect_failure(&config, &e, last);
                    items.push((name, CheckStatus::Failed, msg));
                    return Ok(items);
                }
            }
            for topic in &topics {
                let name = "topic";
                match client::partitions_of(&consumer, &config, topic, config.request_timeout()) {
                    Ok(p) => items.push((
                        name,
                        CheckStatus::Ok,
                        format!("`{topic}`: {} partition(s)", p.len()),
                    )),
                    Err(e) => items.push((name, CheckStatus::Failed, e.to_string())),
                }
            }
            Ok(items)
        })
        .await?;
        for (name, status, msg) in items {
            report.push(name, status, msg);
        }
        Ok(report)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let consumer = self.consumer()?;
        let config = self.config.clone();
        let mut names = client::blocking(move || {
            let md = consumer
                .fetch_metadata(None, config.request_timeout())
                .map_err(|e| {
                    client::map_error(&config, "list topics", &e, consumer.context().last_error())
                })?;
            Ok(md
                .topics()
                .iter()
                .filter(|t| !t.name().starts_with("__") && t.error().is_none())
                .map(|t| t.name().to_owned())
                .collect::<Vec<_>>())
        })
        .await?;
        names.sort();
        let key = if self.config.key_fields.is_empty() {
            vec![RECORD_KEY.to_owned()]
        } else {
            self.config.key_fields.clone()
        };
        Ok(names
            .into_iter()
            .map(|name| CollectionInfo {
                name,
                key: key.clone(),
                fields: vec![],
                estimated_rows: None,
            })
            .collect())
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        let consumer = self.consumer()?;
        let config = self.config.clone();
        let topics = self.topics.clone();
        let current =
            client::blocking(move || client::all_watermarks(&consumer, &config, &topics)).await?;
        match resume {
            None => {
                let positions: Positions = current
                    .iter()
                    .map(|(topic, parts)| {
                        let start = parts
                            .iter()
                            .map(|(&p, wm)| match self.config.start_from {
                                StartFrom::Earliest => (p, wm.low),
                                StartFrom::Latest => (p, wm.high),
                            })
                            .collect();
                        (topic.clone(), start)
                    })
                    .collect();
                let start = checkpoint::encode(&positions)?;
                self.positions = Some(positions);
                self.pending_checkpoint = false;
                Ok(Opened::Fresh { start })
            }
            Some(cp) => {
                let saved = checkpoint::decode(&cp)?;
                let plan = checkpoint::plan_resume(&saved, &current)?;
                for (topic, partition) in &plan.added {
                    tracing::info!(
                        sync_id = %self.sync_id, topic, partition,
                        "partition not in the checkpoint, reading it from the start"
                    );
                }
                self.pending_checkpoint = !plan.added.is_empty();
                self.positions = Some(plan.positions);
                Ok(Opened::Resumed)
            }
        }
    }

    async fn snapshot(
        &mut self,
        _events: EventSender,
        _cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        // No snapshot capability: a Kafka topic is its own history, read
        // from `start_from`.
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        mut acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let Some(positions) = self.positions.clone() else {
            return Err(SourceError::Fatal("stream called before open".to_owned()));
        };
        let consumer = self.consumer()?;
        let mut run = Run {
            source_id: self.sync_id.clone(),
            config: self.config.clone(),
            topics: self.topics.clone(),
            decoder: self.decoder.clone(),
            consumer: consumer.clone(),
            positions,
            dirty: self.pending_checkpoint,
            since_checkpoint: 0,
            skipped: self.skipped.clone(),
            leads: HashMap::new(),
        };
        let tpl = run.assignment(None)?;
        consumer
            .assign(&tpl)
            .map_err(|e| client::map_error(&self.config, "assign partitions", &e, None))?;

        let mut checkpoint_tick = tokio::time::interval(self.config.checkpoint_interval());
        checkpoint_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let discovery_every = self.config.partition_discovery_interval();
        let mut discovery_tick = tokio::time::interval_at(
            tokio::time::Instant::now() + discovery_every,
            discovery_every,
        );
        discovery_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut acks_open = true;

        let result = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break Ok(()),
                changed = acks.changed(), if acks_open => {
                    if changed.is_err() {
                        acks_open = false;
                        continue;
                    }
                    let acked = acks.borrow_and_update().clone();
                    if let Some(cp) = acked {
                        run.commit_for_monitoring(&cp);
                    }
                }
                received = consumer.recv() => {
                    let step = match received {
                        Ok(message) => Step::Record(run.decode(&message)),
                        Err(e) => Step::Error(e),
                    };
                    match step {
                        Step::Record(record) => {
                            if !run.handle(record, &events, &cancel).await? {
                                break Ok(());
                            }
                        }
                        Step::Error(e) => run.on_consumer_error(&e)?,
                    }
                }
                _ = checkpoint_tick.tick() => {
                    if run.dirty && !run.emit_checkpoint(&events, &cancel).await? {
                        break Ok(());
                    }
                }
                _ = discovery_tick.tick() => {
                    match run.discover_partitions(&cancel).await {
                        Ok(()) => {}
                        Err(SourceError::Transient(m)) => {
                            tracing::warn!(sync_id = %run.source_id, "partition discovery failed: {m}");
                        }
                        Err(e) => break Err(e),
                    }
                }
            }
        };
        // Keep what was read, so a later `stream` on this value (not done by
        // the engine, which rebuilds after errors) would continue.
        self.positions = Some(run.positions);
        self.pending_checkpoint = run.dirty;
        consumer.unassign().ok();
        result
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        let group = group_id(&self.sync_id);
        let admin: AdminClient<Ctx> = match client::client_config(&self.config, &self.sync_id)
            .create_with_context(Ctx::default())
        {
            Ok(a) => a,
            Err(e) => {
                let e = client::map_error(&self.config, "create the admin client", &e, None);
                tracing::warn!(sync_id = %self.sync_id, "teardown skipped: {e}");
                return Ok(());
            }
        };
        let timeout = self.config.request_timeout();
        let opts = AdminOptions::new().request_timeout(Some(timeout));
        let outcome =
            tokio::time::timeout(timeout * 2, admin.delete_groups(&[group.as_str()], &opts)).await;
        match outcome {
            Ok(Ok(results)) => {
                for r in results {
                    match r {
                        Ok(_) => {
                            tracing::info!(sync_id = %self.sync_id, group, "consumer group deleted")
                        }
                        Err((_, RDKafkaErrorCode::GroupIdNotFound)) => {}
                        Err((_, code)) => tracing::warn!(
                            sync_id = %self.sync_id, group, "consumer group not deleted: {code}"
                        ),
                    }
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(sync_id = %self.sync_id, group, "consumer group not deleted: {}",
                    client::scrub(&self.config, &e.to_string()))
            }
            Err(_) => {
                tracing::warn!(sync_id = %self.sync_id, group, "consumer group deletion timed out")
            }
        }
        Ok(())
    }
}

/// Explains a failed connection for the setup UI.
fn explain_connect_failure(
    config: &KafkaConfig,
    e: &KafkaError,
    last: Option<RDKafkaErrorCode>,
) -> (&'static str, String) {
    use RDKafkaErrorCode as E;
    let mapped = client::map_error(config, "connect", e, last);
    match last {
        Some(
            E::Authentication
            | E::SaslAuthenticationFailed
            | E::UnsupportedSASLMechanism
            | E::IllegalSASLState,
        ) => (
            "authentication",
            format!(
                "authentication failed, check the SASL mechanism, username and password ({mapped})"
            ),
        ),
        Some(E::SSL) => (
            "tls",
            format!("TLS handshake failed, check the protocol and certificates ({mapped})"),
        ),
        Some(E::Resolve) => (
            "brokers",
            format!("broker host names do not resolve ({mapped})"),
        ),
        _ => ("brokers", format!("brokers unreachable ({mapped})")),
    }
}

enum Step {
    Record(Record),
    Error(KafkaError),
}

/// One consumed record, owned, so nothing borrowed from librdkafka lives
/// across an `await`.
struct Record {
    topic: String,
    partition: i32,
    offset: i64,
    outcome: Result<Decoded, DecodeError>,
    /// The value starts with the Confluent Schema Registry framing.
    schema_registry: bool,
}

/// Whether a value is framed for the Confluent Schema Registry: magic byte
/// `0`, then a 4-byte schema id. No JSON document starts with a NUL byte.
fn is_schema_registry_framed(payload: Option<&[u8]>) -> bool {
    payload.is_some_and(|p| p.len() >= 5 && p[0] == 0)
}

/// Decoding state of one partition since the stream started.
#[derive(Debug, Default)]
struct Lead {
    /// A record of this partition decoded: later failures are sporadic.
    proven: bool,
    /// Leading failures `(offset, reason)`, not yet reported nor
    /// checkpointed past. At most `max_leading_decode_failures`.
    held: Vec<(i64, String)>,
}

/// State of one `stream` call.
struct Run {
    source_id: String,
    config: KafkaConfig,
    topics: Vec<String>,
    decoder: Decoder,
    consumer: Arc<KafkaConsumer>,
    positions: Positions,
    /// Positions moved since the last emitted checkpoint.
    dirty: bool,
    since_checkpoint: u64,
    skipped: Arc<AtomicU64>,
    leads: HashMap<(String, i32), Lead>,
}

impl Run {
    fn decode<M: Message>(&self, m: &M) -> Record {
        let topic = m.topic();
        Record {
            topic: topic.to_owned(),
            partition: m.partition(),
            offset: m.offset(),
            outcome: self.decoder.decode(topic, m.key(), m.payload()),
            schema_registry: is_schema_registry_framed(m.payload()),
        }
    }

    fn skipped_event(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        reason: String,
    ) -> SourceEvent {
        let total = self.skipped.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::warn!(
            sync_id = %self.source_id,
            topic, partition, offset,
            skipped_total = total,
            "skipping undecodable record: {reason}"
        );
        SourceEvent::Skipped {
            collection: topic.to_owned(),
            position: format!("{topic}/{partition}@{offset}"),
            reason,
        }
    }

    fn wrong_format(&self, topic: &str, partition: i32, detail: &str) -> SourceError {
        let format = match self.config.format {
            crate::config::Format::Json => "json",
            crate::config::Format::Debezium => "debezium",
        };
        SourceError::Config(format!(
            "topic `{topic}` (partition {partition}) does not look like the configured \
             `{format}` format: {detail}; nothing past these records was checkpointed"
        ))
    }

    /// Emits what the record produced and moves its partition. Returns
    /// `false` when the engine is gone or the run is cancelled.
    async fn handle(
        &mut self,
        record: Record,
        events: &EventSender,
        cancel: &CancellationToken,
    ) -> Result<bool, SourceError> {
        let lead_key = (record.topic.clone(), record.partition);
        let lead = self.leads.entry(lead_key.clone()).or_default();
        let proven = lead.proven;
        let mut out: Vec<SourceEvent> = Vec::new();
        match record.outcome {
            Ok(decoded) => {
                // The format is right: report the leading failures held so
                // far, then the record.
                let held = std::mem::take(&mut lead.held);
                lead.proven = true;
                for (offset, reason) in held {
                    out.push(self.skipped_event(&record.topic, record.partition, offset, reason));
                }
                match decoded {
                    Decoded::Change(change) => out.push(SourceEvent::Change(change)),
                    Decoded::Truncate => out.push(SourceEvent::Truncate {
                        collection: record.topic.clone(),
                    }),
                    Decoded::Ignored => {}
                }
            }
            Err(_) if record.schema_registry => {
                return Err(self.wrong_format(
                    &record.topic,
                    record.partition,
                    &format!(
                        "the record at offset {} uses the Confluent Schema Registry wire format \
                         (magic byte 0 and a schema id); Avro, Protobuf and JSON Schema through \
                         the Schema Registry are not supported, produce plain JSON values",
                        record.offset
                    ),
                ));
            }
            Err(DecodeError(reason)) if proven => {
                out.push(self.skipped_event(
                    &record.topic,
                    record.partition,
                    record.offset,
                    reason,
                ));
            }
            Err(DecodeError(reason)) => {
                lead.held.push((record.offset, reason.clone()));
                let held = lead.held.len();
                if held >= self.config.max_leading_decode_failures as usize {
                    return Err(self.wrong_format(
                        &record.topic,
                        record.partition,
                        &format!(
                            "its first {held} records since the sync started could not be \
                             decoded (last: {reason})"
                        ),
                    ));
                }
                // Held: the position does not move past it until a record
                // of this partition decodes.
                return Ok(true);
            }
        }
        for event in out {
            if !send(events, event, cancel).await {
                return Ok(false);
            }
        }
        self.positions
            .entry(record.topic)
            .or_default()
            .insert(record.partition, record.offset + 1);
        self.dirty = true;
        self.since_checkpoint += 1;
        if self.since_checkpoint >= self.config.checkpoint_every_records {
            return self.emit_checkpoint(events, cancel).await;
        }
        Ok(true)
    }

    async fn emit_checkpoint(
        &mut self,
        events: &EventSender,
        cancel: &CancellationToken,
    ) -> Result<bool, SourceError> {
        let cp = checkpoint::encode(&self.positions)?;
        if !send(events, SourceEvent::Checkpoint(cp), cancel).await {
            return Ok(false);
        }
        self.dirty = false;
        self.since_checkpoint = 0;
        Ok(true)
    }

    fn on_consumer_error(&self, e: &KafkaError) -> Result<(), SourceError> {
        let code = e.rdkafka_error_code();
        if let Some(code) = code
            && client::is_self_healing(code)
        {
            tracing::warn!(sync_id = %self.source_id, "kafka consumer: {code} (retrying)");
            return Ok(());
        }
        // Authorization, unknown topic → Config; offset out of range →
        // PositionLost; all brokers down and anything else → Transient.
        let last = self.consumer.context().last_error();
        Err(client::map_error(&self.config, "consume", e, last))
    }

    /// The assignment for every known partition, or only for `only`.
    fn assignment(
        &self,
        only: Option<&[(String, i32)]>,
    ) -> Result<TopicPartitionList, SourceError> {
        let mut tpl = TopicPartitionList::new();
        for (topic, parts) in &self.positions {
            for (&partition, &offset) in parts {
                if only.is_some_and(|o| !o.iter().any(|(t, p)| t == topic && *p == partition)) {
                    continue;
                }
                tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
                    .map_err(|e| client::map_error(&self.config, "build assignment", &e, None))?;
            }
        }
        Ok(tpl)
    }

    /// Picks up partitions added to the topics since `open`, from their log
    /// start.
    async fn discover_partitions(&mut self, cancel: &CancellationToken) -> Result<(), SourceError> {
        let consumer = self.consumer.clone();
        let config = self.config.clone();
        let topics = self.topics.clone();
        let known: Vec<(String, i32)> = self
            .positions
            .iter()
            .flat_map(|(t, ps)| ps.keys().map(move |p| (t.clone(), *p)))
            .collect();
        let lookup = client::blocking(move || {
            let mut added = Vec::new();
            for topic in &topics {
                for p in client::partitions_of(&consumer, &config, topic, config.request_timeout())?
                {
                    if !known.iter().any(|(t, kp)| t == topic && *kp == p) {
                        let wm = client::watermarks(&consumer, &config, topic, p)?;
                        added.push((topic.clone(), p, wm.low));
                    }
                }
            }
            Ok(added)
        });
        let added = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            added = lookup => added?,
        };
        if added.is_empty() {
            return Ok(());
        }
        let mut new = Vec::with_capacity(added.len());
        for (topic, partition, start) in added {
            tracing::info!(sync_id = %self.source_id, topic, partition, start, "new partition");
            self.positions
                .entry(topic.clone())
                .or_default()
                .insert(partition, start);
            new.push((topic, partition));
        }
        let tpl = self.assignment(Some(&new))?;
        self.consumer
            .incremental_assign(&tpl)
            .map_err(|e| client::map_error(&self.config, "assign new partitions", &e, None))?;
        self.dirty = true;
        Ok(())
    }

    /// Commits the acknowledged offsets under `meili-connect-<sync_id>` so
    /// the user's tooling can watch lag. Asynchronous and best effort.
    fn commit_for_monitoring(&self, acked: &Checkpoint) {
        let Ok(positions) = checkpoint::decode(acked) else {
            return;
        };
        let mut tpl = TopicPartitionList::new();
        for (topic, parts) in &positions {
            for (&partition, &offset) in parts {
                if tpl
                    .add_partition_offset(topic, partition, Offset::Offset(offset))
                    .is_err()
                {
                    return;
                }
            }
        }
        if tpl.count() == 0 {
            return;
        }
        if let Err(e) = self.consumer.commit(&tpl, CommitMode::Async) {
            tracing::debug!(sync_id = %self.source_id, "offset commit for monitoring failed: {e}");
        }
    }
}

/// Sends unless cancelled. `false` when the engine is gone or cancelled.
async fn send(events: &EventSender, event: SourceEvent, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        sent = events.send(event) => sent.is_ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_connect_core::{Change, Op};
    use serde_json::json;
    use tokio::sync::mpsc;

    fn config(extra: serde_json::Value) -> KafkaConfig {
        let mut v = json!({"bootstrap_servers": ["127.0.0.1:1"], "checkpoint_every_records": 1000});
        if let (serde_json::Value::Object(base), serde_json::Value::Object(extra)) = (&mut v, extra)
        {
            base.extend(extra);
        }
        KafkaConfig::from_value(&v).unwrap()
    }

    fn run(extra: serde_json::Value) -> Run {
        let config = config(extra);
        let consumer = client::consumer(&config, "t").unwrap();
        Run {
            source_id: "t".into(),
            topics: vec!["t".into()],
            decoder: Decoder {
                format: config.format,
                key_fields: vec![],
            },
            consumer,
            positions: Positions::from([("t".to_owned(), [(0, 0), (1, 0)].into())]),
            dirty: false,
            since_checkpoint: 0,
            skipped: Arc::default(),
            leads: HashMap::new(),
            config,
        }
    }

    fn rec(run: &Run, partition: i32, offset: i64, value: &[u8]) -> Record {
        Record {
            topic: "t".into(),
            partition,
            offset,
            outcome: run.decoder.decode("t", Some(b"1"), Some(value)),
            schema_registry: is_schema_registry_framed(Some(value)),
        }
    }

    const GOOD: &[u8] = br#"{"id": 1}"#;
    const BAD: &[u8] = b"not json";

    async fn feed(
        run: &mut Run,
        records: Vec<Record>,
    ) -> (Result<bool, SourceError>, Vec<SourceEvent>) {
        let (tx, mut rx) = mpsc::channel(256);
        let cancel = CancellationToken::new();
        let mut result = Ok(true);
        for r in records {
            result = run.handle(r, &tx, &cancel).await;
            if result.is_err() {
                break;
            }
        }
        drop(tx);
        let mut events = Vec::new();
        while let Some(e) = rx.recv().await {
            events.push(e);
        }
        (result, events)
    }

    fn skipped_positions(events: &[SourceEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|e| match e {
                SourceEvent::Skipped {
                    collection,
                    position,
                    reason,
                } => {
                    assert_eq!(collection, "t");
                    assert!(!reason.contains("not json"), "{reason}");
                    Some(position.clone())
                }
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn leading_failures_are_reported_once_the_format_is_confirmed() {
        let mut r = run(json!({}));
        let records = vec![
            rec(&r, 0, 0, BAD),
            rec(&r, 0, 1, BAD),
            rec(&r, 0, 2, GOOD),
            rec(&r, 0, 3, BAD),
        ];
        let (result, events) = feed(&mut r, records).await;
        assert!(result.unwrap());
        assert_eq!(skipped_positions(&events), vec!["t/0@0", "t/0@1", "t/0@3"]);
        assert!(matches!(
            &events[2],
            SourceEvent::Change(Change {
                op: Op::Upsert { .. },
                ..
            })
        ));
        assert_eq!(r.positions["t"][&0], 4);
        assert_eq!(r.skipped.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn a_partition_that_never_decodes_is_a_config_error() {
        let mut r = run(json!({"max_leading_decode_failures": 3, "checkpoint_every_records": 1}));
        let records = (0..3).map(|o| rec(&r, 0, o, BAD)).collect();
        let (result, events) = feed(&mut r, records).await;
        match result {
            Err(SourceError::Config(m)) => {
                assert!(
                    m.contains("does not look like") && m.contains("`json`"),
                    "{m}"
                );
            }
            other => panic!("{other:?}"),
        }
        // Nothing reported, nothing checkpointed past the records.
        assert!(events.is_empty(), "{events:?}");
        assert_eq!(r.positions["t"][&0], 0);
    }

    #[tokio::test]
    async fn sporadic_failures_after_a_success_are_skipped() {
        let mut r = run(json!({"max_leading_decode_failures": 2}));
        let mut records = vec![rec(&r, 0, 0, GOOD)];
        records.extend((1..6).map(|o| rec(&r, 0, o, BAD)));
        // Another partition has its own count.
        records.push(rec(&r, 1, 0, BAD));
        let (result, events) = feed(&mut r, records).await;
        assert!(result.unwrap());
        assert_eq!(skipped_positions(&events).len(), 5);
        assert_eq!(r.positions["t"][&0], 6);
        assert_eq!(r.positions["t"][&1], 0, "held, not checkpointed past");
    }

    #[tokio::test]
    async fn schema_registry_framing_is_a_config_error() {
        let mut r = run(json!({}));
        let avro = [0u8, 0, 0, 0, 7, 2, 104, 105];
        let records = vec![rec(&r, 0, 0, &avro)];
        let (result, events) = feed(&mut r, records).await;
        match result {
            Err(SourceError::Config(m)) => assert!(
                m.contains("Schema Registry") && m.contains("not supported"),
                "{m}"
            ),
            other => panic!("{other:?}"),
        }
        assert!(events.is_empty());
        assert_eq!(r.positions["t"][&0], 0);
        assert!(!is_schema_registry_framed(Some(GOOD)));
        assert!(!is_schema_registry_framed(Some(&[0, 1])));
    }
}
