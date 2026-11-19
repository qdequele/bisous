//! Tests against a live broker. Set `KAFKA_TEST_BROKERS=host:port` to run
//! them; without it they pass with a notice.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use meili_connect_core::SourceFactory;
use meili_connect_core::{
    AckSender, Change, CheckStatus, Checkpoint, Op, Opened, Source, SourceError, SourceEvent,
    SourceKind, SourceSpec, StreamMapping, ack_channel,
};
use meili_connect_source_kafka::{KafkaSourceFactory, build_source};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewPartitions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn brokers() -> Option<String> {
    match std::env::var("KAFKA_TEST_BROKERS") {
        Ok(b) if !b.is_empty() => Some(b),
        _ => {
            eprintln!("KAFKA_TEST_BROKERS is not set: skipping live Kafka test");
            None
        }
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

fn admin(brokers: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap()
}

async fn create_topic(brokers: &str, name: &str, partitions: i32) {
    let r = admin(brokers)
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(10))),
        )
        .await
        .unwrap();
    r.into_iter().for_each(|r| {
        r.unwrap();
    });
    // Let the metadata settle.
    tokio::time::sleep(Duration::from_millis(500)).await;
}

async fn produce(
    brokers: &str,
    topic: &str,
    partition: i32,
    key: Option<&str>,
    value: Option<&str>,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    let mut rec: FutureRecord<'_, str, str> = FutureRecord::to(topic).partition(partition);
    if let Some(k) = key {
        rec = rec.key(k);
    }
    if let Some(v) = value {
        rec = rec.payload(v);
    }
    producer
        .send(rec, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)
        .unwrap();
}

async fn produce_docs(brokers: &str, topic: &str, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        let v = json!({"id": id, "name": format!("n{id}")}).to_string();
        produce(brokers, topic, 0, Some(&id.to_string()), Some(&v)).await;
    }
}

fn spec(brokers: &str, extra: Value) -> SourceSpec {
    let mut config = json!({
        "bootstrap_servers": brokers.split(',').collect::<Vec<_>>(),
        "checkpoint_interval_ms": 200,
        "partition_discovery_interval_ms": 500,
        "request_timeout_ms": 5000,
    });
    if let (Value::Object(base), Value::Object(extra)) = (&mut config, extra) {
        base.extend(extra);
    }
    SourceSpec {
        kind: SourceKind::Kafka,
        config,
    }
}

fn mappings(topics: &[&str]) -> Vec<StreamMapping> {
    topics
        .iter()
        .map(|t| serde_json::from_value(json!({"collection": t, "index": "idx"})).unwrap())
        .collect()
}

fn source(brokers: &str, topics: &[&str], extra: Value) -> Box<dyn Source> {
    KafkaSourceFactory
        .build(&unique("sync"), &spec(brokers, extra), &mappings(topics))
        .unwrap()
}

struct Running {
    rx: mpsc::Receiver<SourceEvent>,
    /// Positions of the `Skipped` events seen so far.
    skipped: Vec<String>,
    acks: AckSender,
    cancel: CancellationToken,
    handle: JoinHandle<(Box<dyn Source>, Result<(), SourceError>)>,
}

fn spawn_stream(mut source: Box<dyn Source>) -> Running {
    let (tx, rx) = mpsc::channel(64);
    let (acks, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let handle = tokio::spawn(async move {
        let r = source.stream(tx, ack_rx, c).await;
        (source, r)
    });
    Running {
        rx,
        skipped: Vec::new(),
        acks,
        cancel,
        handle,
    }
}

impl Running {
    /// Collects `n` changes, then waits for a checkpoint covering them.
    async fn changes(&mut self, n: usize) -> (Vec<Change>, Checkpoint) {
        let mut out = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let ev = tokio::time::timeout_at(deadline, self.rx.recv())
                .await
                .expect("timed out waiting for events")
                .expect("stream ended");
            match ev {
                SourceEvent::Change(c) => out.push(c),
                SourceEvent::Skipped { position, .. } => self.skipped.push(position),
                SourceEvent::Checkpoint(cp) if out.len() >= n => return (out, cp),
                _ => {}
            }
        }
    }

    async fn stop(self) -> Box<dyn Source> {
        self.cancel.cancel();
        let (source, r) = self.handle.await.unwrap();
        r.unwrap();
        source
    }
}

fn keys(changes: &[Change]) -> Vec<Value> {
    changes.iter().map(|c| c.key[0].clone()).collect()
}

#[tokio::test]
async fn check_reports_brokers_auth_and_topics() {
    let Some(b) = brokers() else { return };
    let topic = unique("check");
    create_topic(&b, &topic, 2).await;
    let missing = unique("missing");

    let mut ok = source(&b, &[&topic], json!({}));
    let report = ok.check().await.unwrap();
    assert!(report.passed(), "{report:?}");
    assert!(
        report
            .items
            .iter()
            .any(|i| i.message.contains("2 partition(s)"))
    );

    let mut bad = source(&b, &[&topic, &missing], json!({}));
    let report = bad.check().await.unwrap();
    assert!(!report.passed());
    let failed: Vec<_> = report
        .items
        .iter()
        .filter(|i| i.status == CheckStatus::Failed)
        .collect();
    assert_eq!(failed.len(), 1, "{report:?}");
    assert!(failed[0].message.contains(&missing));

    // check creates nothing: the missing topic still does not exist.
    let report = bad.check().await.unwrap();
    assert!(!report.passed());

    let mut unreachable = source(
        "127.0.0.1:1",
        &[&topic],
        json!({"request_timeout_ms": 1500}),
    );
    let report = unreachable.check().await.unwrap();
    assert!(!report.passed());
    assert_eq!(report.items[0].name, "brokers");
}

/// No topic picked yet: a connection-level check that passes, with one
/// `topics` item in place of the per-topic checks.
#[tokio::test]
async fn check_without_topics_is_a_connection_check() {
    let Some(b) = brokers() else { return };
    let mut s = source(&b, &[], json!({}));
    let report = s.check().await.unwrap();
    assert!(report.passed(), "{report:?}");
    let names: Vec<_> = report.items.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, ["brokers", "authentication", "topics"], "{report:?}");
    let topics = report.items.last().unwrap();
    assert_eq!(topics.status, CheckStatus::Ok);
    assert_eq!(
        topics.message,
        "no topic selected yet: topic checks run once you pick them"
    );
    // An unreachable cluster still fails, without a `topics` item.
    let mut unreachable = source("127.0.0.1:1", &[], json!({"request_timeout_ms": 1500}));
    let report = unreachable.check().await.unwrap();
    assert!(!report.passed());
    assert!(
        report.items.iter().all(|i| i.name != "topics"),
        "{report:?}"
    );
}

#[tokio::test]
async fn discover_lists_topics_but_not_internal_ones() {
    let Some(b) = brokers() else { return };
    let topic = unique("disc");
    create_topic(&b, &topic, 1).await;
    let found = source(&b, &[&topic], json!({})).discover().await.unwrap();
    let mine = found
        .iter()
        .find(|c| c.name == topic)
        .expect("topic listed");
    assert_eq!(mine.key, vec!["<record key>"]);
    assert!(mine.fields.is_empty());
    assert!(found.iter().all(|c| !c.name.starts_with("__")));
}

#[tokio::test]
async fn open_unknown_topic_is_config() {
    let Some(b) = brokers() else { return };
    let mut s = source(&b, &[&unique("nope")], json!({}));
    assert!(matches!(s.open(None).await, Err(SourceError::Config(_))));
}

#[tokio::test]
async fn fresh_earliest_reads_existing_records() {
    let Some(b) = brokers() else { return };
    let topic = unique("early");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=3).await;

    let mut s = source(&b, &[&topic], json!({"start_from": "earliest"}));
    let Opened::Fresh { start } = s.open(None).await.unwrap() else {
        panic!("expected fresh")
    };
    assert_eq!(start.0, json!({ &topic: {"0": 0} }));
    let mut run = spawn_stream(s);
    let (changes, cp) = run.changes(3).await;
    assert_eq!(keys(&changes), vec![json!(1), json!(2), json!(3)]);
    let Op::Upsert { doc } = &changes[0].op else {
        panic!()
    };
    assert_eq!(doc.get("name"), Some(&json!("n1")));
    assert_eq!(changes[0].collection, topic);
    assert_eq!(cp.0, json!({ &topic: {"0": 3} }));
    run.stop().await;
}

#[tokio::test]
async fn fresh_latest_skips_existing_records() {
    let Some(b) = brokers() else { return };
    let topic = unique("late");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=2).await;

    let mut s = source(&b, &[&topic], json!({"start_from": "latest"}));
    let Opened::Fresh { start } = s.open(None).await.unwrap() else {
        panic!()
    };
    assert_eq!(start.0, json!({ &topic: {"0": 2} }));
    let mut run = spawn_stream(s);
    produce_docs(&b, &topic, 3..=3).await;
    let (changes, cp) = run.changes(1).await;
    assert_eq!(keys(&changes), vec![json!(3)]);
    assert_eq!(cp.0, json!({ &topic: {"0": 3} }));
    run.stop().await;
}

#[tokio::test]
async fn resume_starts_exactly_at_the_checkpoint() {
    let Some(b) = brokers() else { return };
    let topic = unique("resume");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=5).await;

    let mut s = source(&b, &[&topic], json!({}));
    let cp = Checkpoint(json!({ &topic: {"0": 2} }));
    assert_eq!(s.open(Some(cp)).await.unwrap(), Opened::Resumed);
    let mut run = spawn_stream(s);
    let (changes, cp) = run.changes(3).await;
    assert_eq!(keys(&changes), vec![json!(3), json!(4), json!(5)]);
    assert_eq!(cp.0, json!({ &topic: {"0": 5} }));
    run.stop().await;

    // Resuming from the last checkpoint replays nothing.
    let mut s = source(&b, &[&topic], json!({}));
    assert_eq!(s.open(Some(cp)).await.unwrap(), Opened::Resumed);
    let mut run = spawn_stream(s);
    produce_docs(&b, &topic, 6..=6).await;
    let (changes, _) = run.changes(1).await;
    assert_eq!(keys(&changes), vec![json!(6)]);
    run.stop().await;
}

#[tokio::test]
async fn tombstones_delete_and_bad_records_are_skipped() {
    let Some(b) = brokers() else { return };
    let topic = unique("tomb");
    create_topic(&b, &topic, 1).await;
    produce(&b, &topic, 0, Some("7"), Some("not json")).await;
    produce(&b, &topic, 0, None, Some(r#"{"id": 1}"#)).await; // no key
    produce(&b, &topic, 0, Some("7"), None).await; // tombstone

    let mut s = build_source(&unique("sync"), &spec(&b, json!({})), &mappings(&[&topic])).unwrap();
    let skipped = s.skipped_counter();
    s.open(None).await.unwrap();
    let mut run = spawn_stream(Box::new(s));
    let (changes, cp) = run.changes(1).await;
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].key, vec![json!(7)]);
    assert_eq!(changes[0].op, Op::Delete);
    assert_eq!(cp.0, json!({ &topic: {"0": 3} }));
    assert_eq!(skipped.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(
        run.skipped,
        vec![format!("{topic}/0@0"), format!("{topic}/0@1")]
    );
    run.stop().await;
}

/// Runs a stream that must fail; returns its error and the furthest
/// checkpointed offset of partition 0.
async fn stream_failure(mut run: Running, topic: &str) -> (SourceError, Option<i64>) {
    let mut furthest = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while let Ok(Some(ev)) = tokio::time::timeout_at(deadline, run.rx.recv()).await {
        match ev {
            SourceEvent::Checkpoint(cp) => furthest = cp.0[topic]["0"].as_i64(),
            SourceEvent::Change(_) | SourceEvent::Skipped { .. } => {
                panic!("nothing is emitted for a topic in the wrong format: {ev:?}")
            }
            _ => {}
        }
    }
    let (_, r) = tokio::time::timeout(Duration::from_secs(5), run.handle)
        .await
        .expect("stream did not fail")
        .unwrap();
    (r.expect_err("stream must fail"), furthest)
}

#[tokio::test]
async fn a_topic_in_another_format_stops_the_sync() {
    let Some(b) = brokers() else { return };
    let topic = unique("wrongfmt");
    create_topic(&b, &topic, 1).await;
    for i in 0..5 {
        produce(&b, &topic, 0, Some(&i.to_string()), Some("plain text")).await;
    }
    let mut s = source(&b, &[&topic], json!({"max_leading_decode_failures": 5}));
    s.open(None).await.unwrap();
    let (err, furthest) = stream_failure(spawn_stream(s), &topic).await;
    match err {
        SourceError::Config(m) => assert!(m.contains("does not look like"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(
        furthest.is_none_or(|o| o == 0),
        "checkpointed past: {furthest:?}"
    );
}

#[tokio::test]
async fn schema_registry_records_stop_the_sync() {
    let Some(b) = brokers() else { return };
    let topic = unique("avro");
    create_topic(&b, &topic, 1).await;
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &b)
        .create()
        .unwrap();
    let value: &[u8] = &[0, 0, 0, 0, 42, 6, 102, 111, 111];
    producer
        .send(
            FutureRecord::<str, [u8]>::to(&topic)
                .partition(0)
                .key("1")
                .payload(value),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
    let mut s = source(&b, &[&topic], json!({}));
    s.open(None).await.unwrap();
    let (err, furthest) = stream_failure(spawn_stream(s), &topic).await;
    match err {
        SourceError::Config(m) => assert!(m.contains("Schema Registry"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert!(
        furthest.is_none_or(|o| o == 0),
        "checkpointed past: {furthest:?}"
    );
}

#[tokio::test]
async fn new_partitions_are_picked_up_from_the_start() {
    let Some(b) = brokers() else { return };
    let topic = unique("grow");
    create_topic(&b, &topic, 1).await;
    let mut s = source(&b, &[&topic], json!({}));
    s.open(None).await.unwrap();
    let mut run = spawn_stream(s);
    produce_docs(&b, &topic, 1..=1).await;
    let (changes, _) = run.changes(1).await;
    assert_eq!(keys(&changes), vec![json!(1)]);

    let r = admin(&b)
        .create_partitions(&[NewPartitions::new(&topic, 2)], &AdminOptions::new())
        .await
        .unwrap();
    r.into_iter().for_each(|r| {
        r.unwrap();
    });
    // Produced before the source notices the partition: must still be read.
    produce(&b, &topic, 1, Some("2"), Some(r#"{"id": 2}"#)).await;
    let (changes, cp) = run.changes(1).await;
    assert_eq!(keys(&changes), vec![json!(2)]);
    assert_eq!(cp.0, json!({ &topic: {"0": 1, "1": 1} }));
    run.stop().await;
}

#[tokio::test]
async fn resume_outside_the_log_is_position_lost() {
    let Some(b) = brokers() else { return };
    let topic = unique("lost");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=5).await;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(3))
        .unwrap();
    admin(&b)
        .delete_records(&tpl, &AdminOptions::new())
        .await
        .unwrap();

    let mut s = source(&b, &[&topic], json!({}));
    let r = s.open(Some(Checkpoint(json!({ &topic: {"0": 1} })))).await;
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");
    let r = s.open(Some(Checkpoint(json!({ &topic: {"0": 99} })))).await;
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");
    // The log start itself is fine.
    let r = s.open(Some(Checkpoint(json!({ &topic: {"0": 3} })))).await;
    assert_eq!(r.unwrap(), Opened::Resumed);
}

#[tokio::test(flavor = "multi_thread")]
async fn acks_are_committed_for_monitoring_and_teardown_removes_them() {
    let Some(b) = brokers() else { return };
    let topic = unique("ack");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=3).await;
    let sync_id = unique("sync");
    let mut s = KafkaSourceFactory
        .build(&sync_id, &spec(&b, json!({})), &mappings(&[&topic]))
        .unwrap();
    s.open(None).await.unwrap();
    let mut run = spawn_stream(s);
    let (_, cp) = run.changes(3).await;
    run.acks.send(Some(cp)).unwrap();

    let group = format!("meili-connect-{sync_id}");
    let watcher: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &b)
        .set("group.id", &group)
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition(&topic, 0);
    let mut committed = Offset::Invalid;
    for _ in 0..40 {
        let t = tpl.clone();
        let w = &watcher;
        committed = tokio::task::block_in_place(|| {
            w.committed_offsets(t, Duration::from_secs(5))
                .unwrap()
                .find_partition(&topic, 0)
                .unwrap()
                .offset()
        });
        if committed == Offset::Offset(3) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(committed, Offset::Offset(3));
    drop(watcher);

    let mut s = run.stop().await;
    s.teardown().await.unwrap();
    s.teardown().await.unwrap(); // idempotent
    let a = admin(&b);
    let mut listed = None;
    for _ in 0..10 {
        let r = tokio::task::block_in_place(|| {
            a.inner()
                .fetch_group_list(Some(&group), Duration::from_secs(5))
        });
        if let Ok(groups) = r {
            listed = Some(
                groups
                    .groups()
                    .iter()
                    .all(|g| g.name() != group || g.state() == "Dead"),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(listed, Some(true), "group still present or not listable");
}

#[tokio::test]
async fn retention_passing_while_streaming_is_position_lost() {
    let Some(b) = brokers() else { return };
    let topic = unique("lost-stream");
    create_topic(&b, &topic, 1).await;
    produce_docs(&b, &topic, 1..=5).await;
    let mut s = source(&b, &[&topic], json!({}));
    assert_eq!(
        s.open(Some(Checkpoint(json!({ &topic: {"0": 0} }))))
            .await
            .unwrap(),
        Opened::Resumed
    );
    // The log start moves past the position between open and stream.
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&topic, 0, Offset::Offset(4))
        .unwrap();
    admin(&b)
        .delete_records(&tpl, &AdminOptions::new())
        .await
        .unwrap();
    let run = spawn_stream(s);
    let (_, r) = tokio::time::timeout(Duration::from_secs(20), run.handle)
        .await
        .expect("stream did not fail")
        .unwrap();
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");
}
