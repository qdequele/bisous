//! Integration tests against the Firestore emulator.
//!
//! Set `FIRESTORE_TEST_EMULATOR_HOST` (`host:port`) to run them:
//!
//! ```bash
//! docker run -d --rm --name mc-test-firestore -p 127.0.0.1:8086:8080 \
//!   gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
//!   gcloud emulators firestore start --host-port=0.0.0.0:8080
//! FIRESTORE_TEST_EMULATOR_HOST=127.0.0.1:8086 cargo test -p meili-connect-source-firestore
//! ```
//!
//! The emulator answers every resume, and every change on a live stream,
//! with a `RESET` and the whole result set (Google's service sends deltas):
//! these tests cover the reset path; the delta path is covered by the
//! state machine's unit tests.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::time::Duration;

use gcloud_sdk::google::firestore::v1::{
    ArrayValue, CommitRequest, Document, MapValue, Value, Write, value::ValueType, write,
};
use gcloud_sdk::google::r#type::LatLng;
use meili_connect_core::{
    Change, CheckStatus, Checkpoint, Op, Opened, SourceError, SourceEvent, SourceFactory,
    SourceKind, SourceSpec, StreamMapping, ack_channel,
};
use meili_connect_source_firestore::client::Firestore;
use meili_connect_source_firestore::{FirestoreConfig, FirestoreSourceFactory, ListenCheckpoint};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn emulator() -> Option<String> {
    let host = std::env::var("FIRESTORE_TEST_EMULATOR_HOST")
        .ok()
        .filter(|h| !h.is_empty());
    if host.is_none() {
        eprintln!(
            "FIRESTORE_TEST_EMULATOR_HOST is not set: skipping Firestore emulator integration test"
        );
    }
    host
}

/// A project per test: the emulator keeps projects apart.
fn project() -> String {
    format!("demo-mc-{:08x}", rand::random::<u32>())
}

fn config(host: &str, project: &str) -> Json {
    json!({"emulator_host": host, "project_id": project, "snapshot": {"page_size": 2}})
}

fn spec(host: &str, project: &str) -> SourceSpec {
    SourceSpec {
        kind: SourceKind::Firestore,
        config: config(host, project),
    }
}

fn mapping(c: &str) -> StreamMapping {
    serde_json::from_value(json!({"collection": c, "index": "i"})).unwrap()
}

async fn admin(host: &str, project: &str) -> Firestore {
    let c = FirestoreConfig::from_value(&config(host, project)).unwrap();
    Firestore::connect(&c).await.unwrap()
}

fn int(i: i64) -> Value {
    Value {
        value_type: Some(ValueType::IntegerValue(i)),
    }
}

async fn commit(fs: &Firestore, writes: Vec<Write>) {
    fs.get()
        .commit(CommitRequest {
            database: fs.database.clone(),
            writes,
            transaction: vec![],
            request_options: None,
        })
        .await
        .unwrap();
}

fn put_write(fs: &Firestore, path: &str, fields: HashMap<String, Value>) -> Write {
    Write {
        operation: Some(write::Operation::Update(Document {
            name: format!("{}/{path}", fs.documents_root()),
            fields,
            create_time: None,
            update_time: None,
        })),
        ..Default::default()
    }
}

async fn put(fs: &Firestore, path: &str, v: i64) {
    let mut fields = HashMap::new();
    fields.insert("v".to_owned(), int(v));
    commit(fs, vec![put_write(fs, path, fields)]).await;
}

async fn delete(fs: &Firestore, path: &str) {
    commit(
        fs,
        vec![Write {
            operation: Some(write::Operation::Delete(format!(
                "{}/{path}",
                fs.documents_root()
            ))),
            ..Default::default()
        }],
    )
    .await;
}

fn changes(events: &[SourceEvent]) -> Vec<&Change> {
    events
        .iter()
        .filter_map(|e| match e {
            SourceEvent::Change(c) => Some(c),
            _ => None,
        })
        .collect()
}

/// `collection key op v` lines, sorted.
fn summary(events: &[SourceEvent]) -> Vec<String> {
    let mut out: Vec<String> = changes(events)
        .into_iter()
        .map(|c| {
            let key = c.key[0].as_str().unwrap().to_owned();
            match &c.op {
                Op::Upsert { doc } => format!("{} {key} upsert {}", c.collection, doc["v"]),
                Op::Delete => format!("{} {key} delete", c.collection),
                Op::Patch { .. } => format!("{} {key} patch", c.collection),
            }
        })
        .collect();
    out.sort();
    out
}

/// Receive until `done` holds for what was received, or panic after 20 s.
async fn receive_until(
    rx: &mut mpsc::Receiver<SourceEvent>,
    seen: &mut Vec<SourceEvent>,
    done: impl Fn(&[SourceEvent]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done(seen) {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) => seen.push(ev),
            Ok(None) => panic!("the source closed its channel; got {:?}", summary(seen)),
            Err(_) => panic!("timed out; got {:?}", summary(seen)),
        }
    }
}

#[tokio::test]
async fn snapshot_pages_converts_types_then_streams_changes() {
    let Some(host) = emulator() else { return };
    let project = project();
    let fs = admin(&host, &project).await;

    // 5 products (3 pages of 2), and orders under two users.
    for i in 1..=5 {
        put(&fs, &format!("products/p{i}"), i).await;
    }
    put(&fs, "users/a/orders/o1", 10).await;
    put(&fs, "users/b/orders/o1", 20).await;
    let mut typed = HashMap::new();
    typed.insert("v".to_owned(), int(6));
    typed.insert(
        "at".to_owned(),
        Value {
            value_type: Some(ValueType::TimestampValue(
                gcloud_sdk::prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                },
            )),
        },
    );
    typed.insert(
        "where".to_owned(),
        Value {
            value_type: Some(ValueType::GeoPointValue(LatLng {
                latitude: 48.85,
                longitude: 2.35,
            })),
        },
    );
    typed.insert(
        "owner".to_owned(),
        Value {
            value_type: Some(ValueType::ReferenceValue(format!(
                "{}/users/a",
                fs.documents_root()
            ))),
        },
    );
    typed.insert(
        "tags".to_owned(),
        Value {
            value_type: Some(ValueType::ArrayValue(ArrayValue {
                values: vec![Value {
                    value_type: Some(ValueType::StringValue("x".into())),
                }],
            })),
        },
    );
    let mut dims = HashMap::new();
    dims.insert("w".to_owned(), int(3));
    typed.insert(
        "dims".to_owned(),
        Value {
            value_type: Some(ValueType::MapValue(MapValue { fields: dims })),
        },
    );
    commit(&fs, vec![put_write(&fs, "products/p6", typed)]).await;

    let mut source = FirestoreSourceFactory
        .build(
            "s1",
            &spec(&host, &project),
            &[mapping("products"), mapping("**/orders")],
        )
        .unwrap();
    let start = match source.open(None).await.unwrap() {
        Opened::Fresh { start } => start,
        other => panic!("{other:?}"),
    };
    let cp: ListenCheckpoint = start.decode().unwrap();
    assert!(cp.resume_token.is_none());
    assert_eq!(cp.collections, ["products", "**/orders"]);

    // Written after the start position and before the snapshot reads: the
    // stream replays it.
    put(&fs, "products/p7", 7).await;
    delete(&fs, "products/p5").await;

    let (tx, mut rx) = mpsc::channel(1000);
    source.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut snap = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        snap.push(ev);
    }
    assert_eq!(
        summary(&snap),
        [
            "**/orders users/a/orders/o1 upsert 10",
            "**/orders users/b/orders/o1 upsert 20",
            "products p1 upsert 1",
            "products p2 upsert 2",
            "products p3 upsert 3",
            "products p4 upsert 4",
            "products p6 upsert 6",
            "products p7 upsert 7",
        ]
    );
    assert!(snap.iter().all(|e| matches!(e, SourceEvent::Change(_))));
    let p6 = changes(&snap)
        .into_iter()
        .find(|c| c.key[0] == json!("p6"))
        .unwrap();
    let Op::Upsert { doc } = &p6.op else { panic!() };
    assert_eq!(
        Json::Object(doc.clone()),
        json!({
            "v": 6,
            "at": "2023-11-14T22:13:20.000000Z",
            "where": {"lat": 48.85, "lng": 2.35},
            "owner": "users/a",
            "tags": ["x"],
            "dims": {"w": 3},
            "_path": "products/p6",
        })
    );

    // Stream: catches up from the start position, then follows changes.
    let (tx, mut rx) = mpsc::channel(1000);
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let task = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let r = source.stream(tx, ack_rx, cancel).await;
            (source, r)
        })
    };
    let mut seen = Vec::new();
    receive_until(&mut rx, &mut seen, |s| {
        s.iter().any(|e| matches!(e, SourceEvent::Checkpoint(_)))
    })
    .await;

    put(&fs, "products/p8", 8).await;
    put(&fs, "products/p1", 100).await;
    delete(&fs, "products/p2").await;
    delete(&fs, "users/a/orders/o1").await;
    receive_until(&mut rx, &mut seen, |s| {
        let lines = summary(s);
        [
            "products p8 upsert 8",
            "products p1 upsert 100",
            "products p2 delete",
            "**/orders users/a/orders/o1 delete",
        ]
        .iter()
        .all(|want| lines.iter().any(|l| l == want))
    })
    .await;
    // Every change is followed by a checkpoint eventually.
    receive_until(&mut rx, &mut seen, |s| {
        matches!(s.last(), Some(SourceEvent::Checkpoint(_)))
    })
    .await;
    let last: ListenCheckpoint = match seen.last() {
        Some(SourceEvent::Checkpoint(cp)) => cp.decode().unwrap(),
        _ => unreachable!(),
    };
    assert!(last.resume_token.is_some());
    // p5 was deleted before the snapshot: never upserted, and a delete of
    // it is harmless. p3 was never touched: never deleted.
    assert!(
        !summary(&seen)
            .iter()
            .any(|l| l.starts_with("products p3 delete"))
    );

    cancel.cancel();
    let (_source, r) = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    r.unwrap();
}

#[tokio::test]
async fn resuming_needs_a_delta_or_resyncs() {
    let Some(host) = emulator() else { return };
    let project = project();
    let fs = admin(&host, &project).await;
    put(&fs, "items/a", 1).await;

    let factory = FirestoreSourceFactory;
    let streams = [mapping("items")];
    let mut source = factory
        .build("s1", &spec(&host, &project), &streams)
        .unwrap();
    let Opened::Fresh { start } = source.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(100);
    source.snapshot(tx, CancellationToken::new()).await.unwrap();
    let (tx, mut rx) = mpsc::channel(100);
    let cancel = CancellationToken::new();
    let task = {
        let cancel = cancel.clone();
        tokio::spawn(async move { source.stream(tx, ack_channel().1, cancel).await })
    };
    let mut seen = Vec::new();
    receive_until(&mut rx, &mut seen, |s| {
        s.iter().any(|e| matches!(e, SourceEvent::Checkpoint(_)))
    })
    .await;
    let checkpoint = seen
        .iter()
        .rev()
        .find_map(|e| match e {
            SourceEvent::Checkpoint(cp) => Some(cp.clone()),
            _ => None,
        })
        .unwrap();
    cancel.cancel();
    task.await.unwrap().unwrap();

    // A restarted source does not know which documents the index holds.
    // The emulator re-sends everything on resume: deletes made meanwhile
    // cannot be told apart, so the sync resyncs.
    delete(&fs, "items/a").await;
    let mut resumed = factory
        .build("s1", &spec(&host, &project), &streams)
        .unwrap();
    assert_eq!(
        resumed.open(Some(checkpoint.clone())).await.unwrap(),
        Opened::Resumed
    );
    let (tx, _rx) = mpsc::channel(100);
    let r = tokio::time::timeout(
        Duration::from_secs(20),
        resumed.stream(tx, ack_channel().1, CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(matches!(r, Err(SourceError::PositionLost(_))), "{r:?}");

    // A checkpoint of other collections is not resumable.
    let mut other = factory
        .build(
            "s1",
            &spec(&host, &project),
            &[mapping("items"), mapping("more")],
        )
        .unwrap();
    assert!(matches!(
        other.open(Some(checkpoint)).await,
        Err(SourceError::PositionLost(_))
    ));
    // Neither is garbage.
    assert!(matches!(
        other.open(Some(Checkpoint(json!({"what": 1})))).await,
        Err(SourceError::PositionLost(_))
    ));
    // The start of a fresh sync (a read time, no token) is.
    let mut again = factory
        .build("s1", &spec(&host, &project), &streams)
        .unwrap();
    assert_eq!(again.open(Some(start)).await.unwrap(), Opened::Resumed);
}

#[tokio::test]
async fn check_and_discover() {
    let Some(host) = emulator() else { return };
    let project = project();
    let fs = admin(&host, &project).await;
    put(&fs, "products/p1", 1).await;
    put(&fs, "products/p2", 2).await;
    put(&fs, "products/p1/reviews/r1", 5).await;

    let factory = FirestoreSourceFactory;
    let mut probe = factory.build("s1", &spec(&host, &project), &[]).unwrap();
    let report = probe.check().await.unwrap();
    assert!(report.passed(), "{report:?}");
    assert_eq!(report.items[0].name, "connection");
    assert_eq!(report.items[1].name, "collections");

    let mut source = factory
        .build(
            "s1",
            &spec(&host, &project),
            &[mapping("products"), mapping("empty")],
        )
        .unwrap();
    let report = source.check().await.unwrap();
    let status = |name: &str| {
        report
            .items
            .iter()
            .find(|i| i.name == name)
            .unwrap()
            .status
            .clone()
    };
    assert_eq!(status("collection:products"), CheckStatus::Ok);
    assert_eq!(status("collection:empty"), CheckStatus::Warning);
    assert!(report.passed());

    let found = probe.discover().await.unwrap();
    let products = found.iter().find(|c| c.name == "products").unwrap();
    assert_eq!(products.estimated_rows, Some(2));
    assert_eq!(products.fields[0].name, "v");
    assert_eq!(products.fields[0].source_type, "integer");
    let reviews = found.iter().find(|c| c.name == "**/reviews").unwrap();
    assert_eq!(reviews.estimated_rows, Some(1));
}

#[tokio::test]
async fn a_bad_key_is_a_config_error() {
    // Needs no emulator: the key is refused before any call.
    let spec = SourceSpec {
        kind: SourceKind::Firestore,
        config: json!({"service_account": "{\"type\": \"service_account\", \"client_email\": \"a@b.c\", \"private_key_id\": \"k\", \"private_key\": \"not a key\", \"project_id\": \"demo-x\"}"}),
    };
    let mut source = FirestoreSourceFactory
        .build("s1", &spec, &[mapping("x")])
        .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(60), source.check())
        .await
        .unwrap()
        .unwrap();
    assert!(!report.passed(), "{report:?}");
    assert!(!format!("{report:?}").contains("not a key"));
}
