//! Integration tests against DynamoDB Local (or LocalStack).
//!
//! Set `DYNAMODB_TEST_ENDPOINT` (e.g. `http://127.0.0.1:8000`) to run them:
//!
//! ```bash
//! docker run -d --rm --name mc-test-ddb -p 127.0.0.1:8000:8000 amazon/dynamodb-local \
//!   -jar DynamoDBLocal.jar -inMemory -sharedDb
//! DYNAMODB_TEST_ENDPOINT=http://127.0.0.1:8000 cargo test -p meili-connect-source-dynamodb
//! ```
//!
//! Shard splits cannot be provoked locally; the lineage scheduler's unit
//! tests cover them.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue as Av, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType, TableStatus,
};
use meili_connect_core::{
    Change, CheckStatus, Checkpoint, Op, Opened, Source, SourceError, SourceEvent, SourceFactory,
    SourceKind, SourceSpec, StreamMapping, ack_channel, document_id,
};
use meili_connect_source_dynamodb::convert;
use meili_connect_source_dynamodb::{DynamoSourceFactory, StreamCheckpoint};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn endpoint() -> Option<String> {
    let e = std::env::var("DYNAMODB_TEST_ENDPOINT")
        .ok()
        .filter(|e| !e.is_empty());
    if e.is_none() {
        eprintln!("DYNAMODB_TEST_ENDPOINT is not set: skipping DynamoDB Local integration test");
    }
    e
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{:08x}", rand::random::<u32>())
}

async fn admin(endpoint: &str) -> Client {
    let creds = aws_credential_types::Credentials::new("dummy", "dummy", None, None, "test");
    let conf = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(aws_sdk_dynamodb::config::BehaviorVersion::latest())
        .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .endpoint_url(endpoint)
        .build();
    Client::from_conf(conf)
}

/// `pk` (N), plus `sk` (N) when `sort` is set.
async fn create_table(client: &Client, name: &str, sort: bool, view: Option<StreamViewType>) {
    let mut req = client
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        );
    if sort {
        req = req
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sk")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            );
    }
    if let Some(view) = view {
        req = req.stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(view)
                .build()
                .unwrap(),
        );
    }
    req.send().await.unwrap();
    for _ in 0..100 {
        let d = client
            .describe_table()
            .table_name(name)
            .send()
            .await
            .unwrap();
        if d.table.unwrap().table_status == Some(TableStatus::Active) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("table {name} never became ACTIVE");
}

async fn put(client: &Client, table: &str, pk: i64, v: i64) {
    client
        .put_item()
        .table_name(table)
        .item("pk", Av::N(pk.to_string()))
        .item("v", Av::N(v.to_string()))
        .item("name", Av::S(format!("item {pk}")))
        .item("tags", Av::Ss(vec!["a".into(), format!("t{v}")]))
        .send()
        .await
        .unwrap();
}

async fn delete(client: &Client, table: &str, pk: i64) {
    client
        .delete_item()
        .table_name(table)
        .key("pk", Av::N(pk.to_string()))
        .send()
        .await
        .unwrap();
}

fn source(endpoint: &str, tables: &[&str]) -> Box<dyn Source> {
    let spec = SourceSpec {
        kind: SourceKind::Dynamodb,
        config: json!({
            "region": "us-east-1",
            "endpoint_url": endpoint,
            "credentials": {"type": "static", "access_key_id": "dummy", "secret_access_key": "dummy"},
            "scan": {"segments": 3, "page_limit": 10},
            "stream": {"idle_poll_ms": 1000, "checkpoint_interval_ms": 200, "shard_refresh_secs": 2}
        }),
    };
    let mappings: Vec<StreamMapping> = tables
        .iter()
        .map(|t| StreamMapping {
            collection: (*t).to_owned(),
            index: (*t).to_owned(),
            primary_key: "id".into(),
            fields: Default::default(),
            rename: Default::default(),
            vector: None,
        })
        .collect();
    DynamoSourceFactory.build("it", &spec, &mappings).unwrap()
}

/// What the engine would have written to Meilisearch.
#[derive(Default)]
struct Applied {
    docs: HashMap<String, Value>,
    last_checkpoint: Option<Checkpoint>,
    deletes: Vec<Vec<Value>>,
    changes: usize,
}

type Shared = Arc<Mutex<Applied>>;

/// Drains a source channel into `Applied`.
fn collector(applied: Shared) -> (mpsc::Sender<SourceEvent>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<SourceEvent>(64);
    let handle = tokio::spawn(async move {
        while let Some(ev) = rx.recv().await {
            let mut a = applied.lock().unwrap();
            match ev {
                SourceEvent::Change(Change { key, op, .. }) => {
                    a.changes += 1;
                    let id = document_id(&key);
                    match op {
                        Op::Upsert { doc } => {
                            a.docs.insert(id, Value::Object(doc));
                        }
                        Op::Delete => {
                            a.docs.remove(&id);
                            a.deletes.push(key);
                        }
                        Op::Patch { .. } => panic!("dynamodb never emits patches"),
                    }
                }
                SourceEvent::Checkpoint(cp) => a.last_checkpoint = Some(cp),
                other => panic!("unexpected event {other:?}"),
            }
        }
    });
    (tx, handle)
}

/// The table's current content, keyed like the engine would.
async fn table_docs(client: &Client, table: &str, keys: &[String]) -> HashMap<String, Value> {
    let mut out = HashMap::new();
    let mut start = None;
    loop {
        let page = client
            .scan()
            .table_name(table)
            .consistent_read(true)
            .set_exclusive_start_key(start)
            .send()
            .await
            .unwrap();
        for item in page.items() {
            let key = convert::table::key_values(table, keys, item).unwrap();
            let doc = convert::table::item_to_document(item).unwrap();
            out.insert(document_id(&key), Value::Object(doc));
        }
        match page.last_evaluated_key {
            Some(k) if !k.is_empty() => start = Some(k),
            _ => return out,
        }
    }
}

async fn wait_converged(client: &Client, table: &str, applied: &Shared) {
    let keys = vec!["pk".to_owned()];
    let mut last_diff = String::new();
    for _ in 0..120 {
        let expected = table_docs(client, table, &keys).await;
        {
            let a = applied.lock().unwrap();
            if a.docs == expected {
                return;
            }
            let missing = expected.keys().filter(|k| !a.docs.contains_key(*k)).count();
            let extra = a.docs.keys().filter(|k| !expected.contains_key(*k)).count();
            let stale = expected
                .iter()
                .filter(|(k, v)| a.docs.get(*k).is_some_and(|d| d != *v))
                .count();
            last_diff = format!("missing {missing}, extra {extra}, stale {stale}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("index never converged to table `{table}`: {last_diff}");
}

#[tokio::test]
async fn check_reports_prerequisites() {
    let Some(ep) = endpoint() else { return };
    let client = admin(&ep).await;
    let good = unique("check_good");
    let keys_only = unique("check_keys");
    let no_stream = unique("check_none");
    create_table(&client, &good, false, Some(StreamViewType::NewAndOldImages)).await;
    create_table(&client, &keys_only, false, Some(StreamViewType::KeysOnly)).await;
    create_table(&client, &no_stream, false, None).await;

    let mut src = source(&ep, &[&good]);
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:?}");
    let status = |name: &str| {
        report
            .items
            .iter()
            .find(|i| i.name == name)
            .map(|i| i.status.clone())
    };
    assert_eq!(status(&format!("table:{good}")), Some(CheckStatus::Ok));
    assert_eq!(status(&format!("stream:{good}")), Some(CheckStatus::Ok));
    assert_eq!(
        status(&format!("permissions:{good}")),
        Some(CheckStatus::Ok)
    );

    let mut src = source(&ep, &[&keys_only, &no_stream, "does_not_exist_xyz"]);
    let report = src.check().await.unwrap();
    assert!(!report.passed());
    let item = |name: String| {
        report
            .items
            .iter()
            .find(|i| i.name == name)
            .unwrap()
            .clone()
    };
    let k = item(format!("stream:{keys_only}"));
    assert_eq!(k.status, CheckStatus::Failed);
    assert!(
        k.message.contains("KEYS_ONLY") && k.message.contains("NEW_AND_OLD_IMAGES"),
        "{}",
        k.message
    );
    assert_eq!(
        item(format!("stream:{no_stream}")).status,
        CheckStatus::Failed
    );
    let missing = item("table:does_not_exist_xyz".into());
    assert_eq!(missing.status, CheckStatus::Failed);
    assert!(
        missing.message.contains("does not exist"),
        "{}",
        missing.message
    );

    // open() refuses the same prerequisites with Config errors.
    let mut src = source(&ep, &[&keys_only]);
    assert!(matches!(src.open(None).await, Err(SourceError::Config(_))));
    let mut src = source(&ep, &[&no_stream]);
    assert!(matches!(src.open(None).await, Err(SourceError::Config(_))));
}

/// No table picked yet: the report passes with one `Ok` `tables` item.
#[tokio::test]
async fn check_without_tables_is_a_connection_check() {
    let Some(ep) = endpoint() else { return };
    let mut src = source(&ep, &[]);
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:?}");
    let tables = report.items.iter().find(|i| i.name == "tables").unwrap();
    assert_eq!(tables.status, CheckStatus::Ok);
    assert!(
        report
            .items
            .iter()
            .all(|i| !i.name.contains(':') && i.status != CheckStatus::Failed),
        "{report:?}"
    );
}

#[tokio::test]
async fn discover_lists_keys_and_counts() {
    let Some(ep) = endpoint() else { return };
    let client = admin(&ep).await;
    let name = unique("discover");
    create_table(&client, &name, true, Some(StreamViewType::NewImage)).await;
    let mut src = source(&ep, &[&name]);
    let found = src.discover().await.unwrap();
    let info = found.iter().find(|c| c.name == name).expect("table listed");
    assert_eq!(info.key, vec!["pk", "sk"]);
    let fields: BTreeMap<_, _> = info
        .fields
        .iter()
        .map(|f| (f.name.as_str(), (f.source_type.as_str(), f.nullable)))
        .collect();
    assert_eq!(fields.get("pk"), Some(&("N", false)));
    assert_eq!(fields.get("sk"), Some(&("S", false)));
    assert!(info.estimated_rows.is_some());
}

/// Fresh open, Scan while another task writes, then stream: the result
/// must equal the table. Then resume from the last checkpoint after more
/// writes, including deletes.
#[tokio::test]
async fn gapless_snapshot_stream_and_resume() {
    let Some(ep) = endpoint() else { return };
    let client = admin(&ep).await;
    let table = unique("gapless");
    create_table(
        &client,
        &table,
        false,
        Some(StreamViewType::NewAndOldImages),
    )
    .await;
    for pk in 0..150 {
        put(&client, &table, pk, 1).await;
    }

    // 1. Open captures positions before reading anything.
    let mut src = source(&ep, &[&table]);
    let start = match src.open(None).await.unwrap() {
        Opened::Fresh { start } => start,
        Opened::Resumed => panic!("no checkpoint was given"),
    };
    let decoded: StreamCheckpoint = start.decode().unwrap();
    assert!(decoded.0[&table].stream_arn.contains("/stream/"));
    assert!(!decoded.0[&table].shards.is_empty());

    // 2. Writes race the snapshot: inserts, updates and deletes.
    let writer = {
        let client = client.clone();
        let table = table.clone();
        tokio::spawn(async move {
            for i in 0..60 {
                put(&client, &table, 150 + i, 1).await;
                put(&client, &table, i, 2).await;
                if i % 3 == 0 {
                    delete(&client, &table, 100 + i).await;
                }
            }
        })
    };

    let applied: Shared = Arc::default();
    let (tx, collected) = collector(applied.clone());
    let cancel = CancellationToken::new();
    src.snapshot(tx.clone(), cancel.clone()).await.unwrap();
    assert!(
        applied.lock().unwrap().last_checkpoint.is_none(),
        "snapshots emit no checkpoint"
    );
    writer.await.unwrap();

    // 3. Stream from the captured start.
    let (_ack_tx, ack_rx) = ack_channel();
    let stream_cancel = CancellationToken::new();
    let stream = {
        let tx = tx.clone();
        let c = stream_cancel.clone();
        tokio::spawn(async move {
            let r = src.stream(tx, ack_rx, c).await;
            (src, r)
        })
    };
    wait_converged(&client, &table, &applied).await;
    {
        let a = applied.lock().unwrap();
        assert!(
            a.deletes.iter().any(|k| k == &vec![json!(100)]),
            "REMOVE records become deletes keyed by pk"
        );
    }

    // Let a checkpoint covering everything land, then stop.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stream_cancel.cancel();
    let (_src, result) = stream.await.unwrap();
    result.unwrap();
    let checkpoint = applied
        .lock()
        .unwrap()
        .last_checkpoint
        .clone()
        .expect("a checkpoint");
    let cp: StreamCheckpoint = checkpoint.decode().unwrap();
    assert!(
        cp.0[&table].shards.values().any(|s| s.seq.is_some()),
        "positions advanced: {cp:?}"
    );

    // 4. More writes while stopped, then resume from the checkpoint.
    for pk in 300..320 {
        put(&client, &table, pk, 3).await;
    }
    for pk in 0..10 {
        delete(&client, &table, pk).await;
    }
    put(&client, &table, 20, 9).await;

    let mut src = source(&ep, &[&table]);
    assert_eq!(
        src.open(Some(checkpoint.clone())).await.unwrap(),
        Opened::Resumed
    );
    let changes_before = applied.lock().unwrap().changes;
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel2 = CancellationToken::new();
    let stream = {
        let tx = tx.clone();
        let c = cancel2.clone();
        tokio::spawn(async move { src.stream(tx, ack_rx, c).await })
    };
    wait_converged(&client, &table, &applied).await;
    let replayed = applied.lock().unwrap().changes - changes_before;
    // 20 puts + 10 deletes + 1 update, without replaying the whole history.
    assert!(replayed >= 31, "replayed {replayed}");
    assert!(replayed < 200, "resume replayed too much: {replayed}");
    cancel2.cancel();
    stream.await.unwrap().unwrap();
    drop(tx);
    collected.await.unwrap();

    // 5. Positions that cannot be honoured.
    let mut wrong_arn = cp.clone();
    wrong_arn
        .0
        .get_mut(&table)
        .unwrap()
        .stream_arn
        .push_str("-old");
    let mut src = source(&ep, &[&table]);
    let r = src.open(Some(Checkpoint::new(&wrong_arn).unwrap())).await;
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");

    let mut gone_shard = cp.clone();
    gone_shard.0.get_mut(&table).unwrap().shards.insert(
        "shardId-00000000000000000000-deadbeef".into(),
        meili_connect_source_dynamodb::ShardCheckpoint {
            seq: Some("1".into()),
            done: false,
            since: None,
        },
    );
    let r = src.open(Some(Checkpoint::new(&gone_shard).unwrap())).await;
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");

    let other = unique("unmapped");
    create_table(&client, &other, false, Some(StreamViewType::NewImage)).await;
    let mut src = source(&ep, &[&table, &other]);
    let r = src.open(Some(checkpoint)).await;
    assert!(
        matches!(r, Err(SourceError::PositionLost(_))),
        "newly mapped table: {r:?}"
    );
}

/// Composite keys arrive in key-schema order on both paths.
#[tokio::test]
async fn composite_keys_and_remove() {
    let Some(ep) = endpoint() else { return };
    let client = admin(&ep).await;
    let table = unique("composite");
    create_table(&client, &table, true, Some(StreamViewType::NewImage)).await;
    let put_c = |pk: i64, sk: &str| {
        client
            .put_item()
            .table_name(&table)
            .item("pk", Av::N(pk.to_string()))
            .item("sk", Av::S(sk.to_owned()))
            .item(
                "payload",
                Av::M(HashMap::from([("n".into(), Av::N("1.5".into()))])),
            )
            .send()
    };
    put_c(1, "a").await.unwrap();

    let mut src = source(&ep, &[&table]);
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    let applied: Shared = Arc::default();
    let (tx, collected) = collector(applied.clone());
    let cancel = CancellationToken::new();
    src.snapshot(tx.clone(), cancel.clone()).await.unwrap();

    let (_ack, ack_rx) = ack_channel();
    let stream = {
        let tx = tx.clone();
        let c = cancel.clone();
        tokio::spawn(async move { src.stream(tx, ack_rx, c).await })
    };
    put_c(2, "b").await.unwrap();
    client
        .delete_item()
        .table_name(&table)
        .key("pk", Av::N("1".into()))
        .key("sk", Av::S("a".into()))
        .send()
        .await
        .unwrap();

    let mut ok = false;
    for _ in 0..60 {
        {
            let a = applied.lock().unwrap();
            let id2 = document_id(&[json!(2), json!("b")]);
            if a.deletes.contains(&vec![json!(1), json!("a")]) && a.docs.contains_key(&id2) {
                assert_eq!(a.docs[&id2]["payload"], json!({"n": 1.5}));
                assert!(!a.docs.contains_key(&document_id(&[json!(1), json!("a")])));
                ok = true;
            }
        }
        if ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    cancel.cancel();
    stream.await.unwrap().unwrap();
    drop(tx);
    collected.await.unwrap();
    assert!(ok, "stream did not deliver the insert and the delete");
}
