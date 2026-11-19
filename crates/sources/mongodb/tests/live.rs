//! Tests against a live replica set. Set `MONGO_TEST_URL`, e.g.
//! `mongodb://127.0.0.1:27017/?directConnection=true`; skipped otherwise.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use futures::TryStreamExt as _;
use meili_connect_core::{
    CheckStatus, Checkpoint, Op, Opened, Source, SourceError, SourceEvent, SourceFactory,
    SourceKind, SourceSpec, StreamMapping, ack_channel,
};
use meili_connect_source_mongodb::MongoSourceFactory;
use meili_connect_source_mongodb::convert::document_to_json;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn url() -> Option<String> {
    match std::env::var("MONGO_TEST_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("MONGO_TEST_URL is not set: skipping live MongoDB test");
            None
        }
    }
}

fn unique_db() -> String {
    format!("mc_test_{:08x}", rand::random::<u32>())
}

fn mappings(colls: &[&str]) -> Vec<StreamMapping> {
    colls
        .iter()
        .map(|c| {
            serde_json::from_value(json!({"collection": c, "index": c.replace('.', "_")})).unwrap()
        })
        .collect()
}

fn build(url: &str, extra: Value, colls: &[&str]) -> Box<dyn Source> {
    let mut config = json!({"uri": url});
    if let (Some(obj), Value::Object(extra)) = (config.as_object_mut(), extra) {
        obj.extend(extra);
    }
    let spec = SourceSpec {
        kind: SourceKind::Mongodb,
        config,
    };
    MongoSourceFactory
        .build("test", &spec, &mappings(colls))
        .unwrap()
}

async fn client(url: &str) -> Client {
    Client::with_uri_str(url).await.unwrap()
}

/// What Meilisearch would hold: `(collection, key)` → document.
#[derive(Default, Debug)]
struct Mirror {
    docs: BTreeMap<(String, String), serde_json::Map<String, Value>>,
    truncated: Vec<String>,
    checkpoints: Vec<Checkpoint>,
    changes: usize,
}

impl Mirror {
    fn apply(&mut self, ev: SourceEvent) {
        match ev {
            SourceEvent::Change(c) => {
                self.changes += 1;
                let k = (c.collection, serde_json::to_string(&c.key).unwrap());
                match c.op {
                    Op::Upsert { doc } => {
                        self.docs.insert(k, doc);
                    }
                    Op::Patch { doc, unset } => {
                        let entry = self.docs.entry(k).or_default();
                        entry.extend(doc);
                        for f in unset {
                            entry.remove(&f);
                        }
                    }
                    Op::Delete => {
                        self.docs.remove(&k);
                    }
                }
            }
            SourceEvent::Checkpoint(cp) => self.checkpoints.push(cp),
            SourceEvent::Truncate { collection } => {
                self.docs.retain(|(c, _), _| *c != collection);
                self.truncated.push(collection);
            }
            SourceEvent::SchemaChanged { .. } | SourceEvent::Skipped { .. } => {}
        }
    }
}

async fn db_state(
    client: &Client,
    colls: &[&str],
) -> BTreeMap<(String, String), serde_json::Map<String, Value>> {
    let mut out = BTreeMap::new();
    for name in colls {
        let (db, coll) = name.split_once('.').unwrap();
        let docs: Vec<Document> = client
            .database(db)
            .collection::<Document>(coll)
            .find(doc! {})
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        for d in docs {
            let json = document_to_json(&d);
            let key = serde_json::to_string(&vec![json["_id"].clone()]).unwrap();
            out.insert(((*name).to_owned(), key), json);
        }
    }
    out
}

/// Run `stream` in the background, feeding a mirror until `done` holds or
/// the timeout expires. Returns the mirror and the stream's result.
async fn stream_until(
    mut source: Box<dyn Source>,
    mut mirror: Mirror,
    timeout: Duration,
    mut done: impl FnMut(&Mirror) -> bool,
) -> (Mirror, Result<(), SourceError>, Box<dyn Source>) {
    let (tx, mut rx) = mpsc::channel(64);
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        let r = source.stream(tx, ack_rx, task_cancel).await;
        (source, r)
    });
    let deadline = tokio::time::Instant::now() + timeout;
    while !done(&mirror) {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) => mirror.apply(ev),
            Ok(None) | Err(_) => break,
        }
    }
    cancel.cancel();
    while let Ok(ev) = rx.try_recv() {
        mirror.apply(ev);
    }
    let (source, r) = handle.await.unwrap();
    (mirror, r, source)
}

#[tokio::test]
async fn check_and_discover() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    c.database(&db)
        .collection::<Document>("orders")
        .insert_many([doc! {"total": 1_i32, "who": "a"}, doc! {"total": 2_i64}])
        .await
        .unwrap();
    c.database(&db)
        .collection::<Document>("users")
        .insert_one(doc! {"_id": "u1", "name": "x"})
        .await
        .unwrap();

    let orders = format!("{db}.orders");
    let users = format!("{db}.users");
    let mut source = build(&url, json!({}), &[&orders, &users]);
    let report = source.check().await.unwrap();
    assert!(report.passed(), "{report:#?}");
    let status = |n: &str| {
        report
            .items
            .iter()
            .find(|i| i.name == n)
            .unwrap()
            .status
            .clone()
    };
    assert_eq!(status("topology"), CheckStatus::Ok);
    assert_eq!(status("server_version"), CheckStatus::Ok);
    assert_eq!(status(&format!("collection {orders}")), CheckStatus::Ok);
    // A fresh single-node replica set has a short oplog window (or the
    // check is skipped): never Failed.
    eprintln!("{report:#?}");

    let missing = format!("{db}.nope");
    let mut source = build(&url, json!({}), &[&missing]);
    let report = source.check().await.unwrap();
    assert!(!report.passed());
    assert_eq!(
        report
            .items
            .iter()
            .find(|i| i.name == format!("collection {missing}"))
            .unwrap()
            .status,
        CheckStatus::Failed
    );

    let mut source = build(&url, json!({"database": db}), &[]);
    let found = source.discover().await.unwrap();
    let names: Vec<_> = found.iter().map(|c| c.name.clone()).collect();
    assert_eq!(names, vec![orders.clone(), users.clone()]);
    let o = &found[0];
    assert_eq!(o.key, vec!["_id".to_owned()]);
    assert_eq!(o.estimated_rows, Some(2));
    let total = o.fields.iter().find(|f| f.name == "total").unwrap();
    assert_eq!(total.source_type, "int|long");
    assert!(o.fields.iter().find(|f| f.name == "who").unwrap().nullable);

    c.database(&db).drop().await.unwrap();
}

#[tokio::test]
async fn wrong_credentials_fail_the_check_without_leaking() {
    let Some(url) = url() else { return };
    let Some(rest) = url.strip_prefix("mongodb://") else {
        eprintln!("MONGO_TEST_URL is not a mongodb:// URL: skipping");
        return;
    };
    let bad = format!("mongodb://nobody:s3cr3tpass@{rest}");
    let mut source = build(&bad, json!({}), &["a.b"]);
    let report = source.check().await.unwrap();
    assert!(!report.passed(), "{report:?}");
    assert_eq!(report.items[0].name, "connection");
    assert!(!format!("{report:?}").contains("s3cr3tpass"));
    let err = source.open(None).await.unwrap_err();
    assert!(matches!(err, SourceError::Config(_)), "{err:?}");
    assert!(!err.to_string().contains("s3cr3tpass"));
}

#[tokio::test]
async fn snapshot_then_stream_is_gapless_under_concurrent_writes() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    let a = c.database(&db).collection::<Document>("a");
    let b = c.database(&db).collection::<Document>("b");
    a.insert_many((0..300).map(|i| doc! {"_id": i, "n": i, "kind": "seed"}))
        .await
        .unwrap();
    b.insert_many((0..300).map(|i| doc! {"n": i, "nested": {"x": i}}))
        .await
        .unwrap();
    let names = [format!("{db}.a"), format!("{db}.b")];
    let colls = [names[0].as_str(), names[1].as_str()];

    let mut source = build(&url, json!({"snapshot_batch_size": 50}), &colls);
    let Opened::Fresh { start } = source.open(None).await.unwrap() else {
        panic!("expected a fresh start")
    };
    assert!(start.0.get("resume_token").is_some(), "{start:?}");

    // Writer: runs while the snapshot is read.
    let (wa, wb) = (a.clone(), b.clone());
    let writer = tokio::spawn(async move {
        for i in 0..200_i32 {
            wa.insert_one(doc! {"_id": 1000 + i, "n": i, "kind": "live"})
                .await
                .unwrap();
            wb.insert_one(doc! {"n": 1000 + i}).await.unwrap();
            if i % 3 == 0 {
                wa.update_one(
                    doc! {"_id": i},
                    doc! {"$set": {"kind": "updated"}, "$unset": {"n": ""}},
                )
                .await
                .unwrap();
            }
            if i % 5 == 0 {
                wa.delete_one(doc! {"_id": 150 + i}).await.unwrap();
                wb.update_one(doc! {"n": i}, doc! {"$set": {"nested.x": -1}})
                    .await
                    .unwrap();
            }
            if i % 50 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    let (tx, mut rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let snap_cancel = cancel.clone();
    let snap = tokio::spawn(async move {
        let r = source.snapshot(tx, snap_cancel).await;
        (source, r)
    });
    let mut mirror = Mirror::default();
    while let Some(ev) = rx.recv().await {
        assert!(!matches!(ev, SourceEvent::Checkpoint(_)));
        mirror.apply(ev);
    }
    let (source, r) = snap.await.unwrap();
    r.unwrap();
    writer.await.unwrap();
    assert!(mirror.changes >= 600);

    let expected = db_state(&c, &colls).await;
    let (mirror, r, _) = stream_until(source, mirror, Duration::from_secs(60), |m| {
        m.docs == expected && !m.checkpoints.is_empty()
    })
    .await;
    r.unwrap();
    assert_eq!(mirror.docs.len(), expected.len());
    assert!(mirror.docs == expected, "mirror diverged from the database");
    assert!(!mirror.checkpoints.is_empty());
    c.database(&db).drop().await.unwrap();
}

#[tokio::test]
async fn resume_from_checkpoint_and_patches() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    let a = c.database(&db).collection::<Document>("a");
    a.insert_one(doc! {"_id": 1, "x": 1, "addr": {"city": "Paris", "zip": "75"}})
        .await
        .unwrap();
    let name = format!("{db}.a");

    // full_document "default": updates come as patches unless dotted.
    let mut source = build(&url, json!({"full_document": "default"}), &[&name]);
    let Opened::Fresh { .. } = source.open(None).await.unwrap() else {
        panic!()
    };
    let w = a.clone();
    let writes = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        w.update_one(
            doc! {"_id": 1},
            doc! {"$set": {"x": 2}, "$unset": {"gone": ""}},
        )
        .await
        .unwrap();
        w.update_one(
            doc! {"_id": 1},
            doc! {"$set": {"y": 3}, "$unset": {"x": ""}},
        )
        .await
        .unwrap();
        w.update_one(doc! {"_id": 1}, doc! {"$set": {"addr.city": "Lyon"}})
            .await
            .unwrap();
    });
    let mut mirror = Mirror::default();
    let (tx, mut rx) = mpsc::channel(16);
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        let r = source.stream(tx, ack_rx, task_cancel).await;
        (source, r)
    });
    let mut changes = Vec::new();
    while changes.len() < 3 {
        let ev = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if let SourceEvent::Change(ch) = &ev {
            changes.push(ch.clone());
        }
        mirror.apply(ev);
    }
    // Wait for the checkpoint that covers the three changes.
    let before = mirror.checkpoints.len();
    while mirror.checkpoints.len() == before {
        let ev = tokio::time::timeout(Duration::from_secs(20), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!matches!(ev, SourceEvent::Change(_)), "unexpected change");
        mirror.apply(ev);
    }
    cancel.cancel();
    let (_, r) = handle.await.unwrap();
    r.unwrap();
    writes.await.unwrap();

    match &changes[0].op {
        Op::Patch { doc, unset } => {
            assert_eq!(Value::Object(doc.clone()), json!({"x": 2}));
            assert!(unset.is_empty());
        }
        other => panic!("expected a patch, got {other:?}"),
    }
    match &changes[1].op {
        Op::Patch { doc, unset } => {
            assert_eq!(Value::Object(doc.clone()), json!({"y": 3}));
            assert_eq!(unset, &vec!["x".to_owned()]);
        }
        other => panic!("expected a patch, got {other:?}"),
    }
    match &changes[2].op {
        Op::Upsert { doc } => {
            assert_eq!(doc["addr"], json!({"city": "Lyon", "zip": "75"}));
        }
        other => panic!("dotted path must become an upsert via lookup, got {other:?}"),
    }
    let cp = mirror.checkpoints.last().unwrap().clone();

    // Writes while the sync is down are delivered after resuming.
    a.insert_one(doc! {"_id": 2, "x": "after"}).await.unwrap();
    let mut source = build(&url, json!({}), &[&name]);
    assert_eq!(source.open(Some(cp)).await.unwrap(), Opened::Resumed);
    let (mirror, r, _) = stream_until(source, Mirror::default(), Duration::from_secs(20), |m| {
        m.docs.len() == 1 && m.changes >= 1
    })
    .await;
    r.unwrap();
    let (_, doc) = mirror.docs.iter().next().unwrap();
    assert_eq!(doc["x"], json!("after"));
    assert_eq!(
        mirror.changes, 1,
        "nothing before the checkpoint is replayed"
    );

    // A token the server cannot decode is refused. Only the history-lost
    // codes trigger a resync; a corrupt token (Overflow, 15) is not one of
    // them and keeps its own classification instead of being swallowed.
    let mut source = build(&url, json!({}), &[&name]);
    let bogus =
        Checkpoint(json!({"resume_token": {"_data": "8265A1F0C2000000012B022C0100296E5A1004"}}));
    let err = source.open(Some(bogus)).await.unwrap_err();
    assert!(!matches!(err, SourceError::PositionLost(_)), "{err:?}");

    c.database(&db).drop().await.unwrap();
}

#[tokio::test]
async fn mixed_id_types_get_distinct_ids_in_snapshot_and_stream() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    let a = c.database(&db).collection::<Document>("a");
    let oid = mongodb::bson::oid::ObjectId::new();
    a.insert_many([
        doc! {"_id": 1, "t": "int"},
        doc! {"_id": "1", "t": "string"},
        doc! {"_id": oid, "t": "oid"},
        doc! {"_id": oid.to_hex(), "t": "hex"},
    ])
    .await
    .unwrap();
    let name = format!("{db}.a");
    let mut source = build(&url, json!({}), &[&name]);
    source.open(None).await.unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    source.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut ids = std::collections::BTreeSet::new();
    while let Some(SourceEvent::Change(ch)) = rx.recv().await {
        ids.insert(meili_connect_core::document_id(&ch.key));
    }
    assert_eq!(ids.len(), 4, "{ids:?}");

    // Deleting one of a colliding pair targets only its own id.
    let w = a.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        w.delete_one(doc! {"_id": "1"}).await.unwrap();
    });
    let (tx2, mut rx2) = mpsc::channel(16);
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move { source.stream(tx2, ack_rx, task_cancel).await });
    let deleted = loop {
        match tokio::time::timeout(Duration::from_secs(20), rx2.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SourceEvent::Change(ch) if ch.op == Op::Delete => {
                break meili_connect_core::document_id(&ch.key);
            }
            _ => {}
        }
    };
    cancel.cancel();
    handle.await.unwrap().unwrap();
    assert!(ids.contains(&deleted));
    assert_ne!(
        deleted, "1",
        "the string _id must not delete the integer one"
    );
    c.database(&db).drop().await.unwrap();
}

#[tokio::test]
async fn drop_and_rename_truncate() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    let a = c.database(&db).collection::<Document>("a");
    let b = c.database(&db).collection::<Document>("b");
    a.insert_one(doc! {"_id": 1}).await.unwrap();
    b.insert_one(doc! {"_id": 1}).await.unwrap();
    let names = [format!("{db}.a"), format!("{db}.b")];
    let mut source = build(&url, json!({}), &[&names[0], &names[1]]);
    source.open(None).await.unwrap();

    let admin = c.database("admin");
    let (a2, db2) = (a.clone(), db.clone());
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        a2.drop().await.unwrap();
        admin
            .run_command(doc! {
                "renameCollection": format!("{db2}.b"),
                "to": format!("{db2}.b_old"),
            })
            .await
            .unwrap();
    });
    let (mirror, r, _) = stream_until(source, Mirror::default(), Duration::from_secs(20), |m| {
        m.truncated.len() >= 2
    })
    .await;
    r.unwrap();
    assert_eq!(mirror.truncated, vec![names[0].clone(), names[1].clone()]);

    // dropDatabase on a database-level stream: truncates, then the
    // invalidate is handled by reopening (the stream keeps running).
    let x = c.database(&db).collection::<Document>("x");
    x.insert_one(doc! {"_id": 1}).await.unwrap();
    let xname = format!("{db}.x");
    let mut source = build(&url, json!({}), &[&xname]);
    source.open(None).await.unwrap();
    let (cdb, db3) = (c.clone(), db.clone());
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cdb.database(&db3).drop().await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        cdb.database(&db3)
            .collection::<Document>("x")
            .insert_one(doc! {"_id": 7})
            .await
            .unwrap();
    });
    let (mirror, r, _) = stream_until(source, Mirror::default(), Duration::from_secs(30), |m| {
        m.docs.len() == 1
    })
    .await;
    r.unwrap();
    assert!(mirror.truncated.contains(&xname), "{mirror:?}");
    assert_eq!(mirror.docs.len(), 1, "stream reopened after invalidate");

    c.database(&db).drop().await.unwrap();
}

#[tokio::test]
async fn idle_stream_emits_keepalive_checkpoints_and_cancels() {
    let Some(url) = url() else { return };
    let mut source = build(&url, json!({}), &[&format!("{}.idle", unique_db())]);
    source.open(None).await.unwrap();
    let c = client(&url).await;
    // Unrelated writes advance the cluster time.
    let other = unique_db();
    let noise = c.database(&other).collection::<Document>("noise");
    let n2 = noise.clone();
    let bg = tokio::spawn(async move {
        for i in 0..40 {
            n2.insert_one(doc! {"i": i}).await.unwrap();
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
    let started = std::time::Instant::now();
    let (mirror, r, _) = stream_until(source, Mirror::default(), Duration::from_secs(15), |m| {
        !m.checkpoints.is_empty()
    })
    .await;
    r.unwrap();
    assert!(!mirror.checkpoints.is_empty(), "idle keep-alive checkpoint");
    assert_eq!(mirror.changes, 0);
    assert!(started.elapsed() < Duration::from_secs(15));
    bg.abort();
    c.database(&other).drop().await.unwrap();
}

#[tokio::test]
async fn snapshot_blocked_on_a_full_channel_returns_ok_on_cancel() {
    let Some(url) = url() else { return };
    let c = client(&url).await;
    let db = unique_db();
    c.database(&db)
        .collection::<Document>("a")
        .insert_many((0..500).map(|i| doc! {"_id": i}))
        .await
        .unwrap();
    let mut source = build(&url, json!({}), &[&format!("{db}.a")]);
    source.open(None).await.unwrap();
    // Nobody reads: the snapshot blocks on back-pressure.
    let (tx, _rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        c2.cancel();
    });
    let r = tokio::time::timeout(Duration::from_secs(3), source.snapshot(tx, cancel))
        .await
        .expect("snapshot ignored the cancellation");
    assert!(r.is_ok(), "{r:?}");
    c.database(&db).drop().await.unwrap();
}

/// `MONGO_STANDALONE_TEST_URL`: a mongod started without `--replSet`.
#[tokio::test]
async fn standalone_is_reported_with_the_fix() {
    let url = match std::env::var("MONGO_STANDALONE_TEST_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("MONGO_STANDALONE_TEST_URL is not set: skipping standalone test");
            return;
        }
    };
    let mut source = build(&url, json!({}), &["shop.orders"]);
    let report = source.check().await.unwrap();
    let topology = report.items.iter().find(|i| i.name == "topology").unwrap();
    assert_eq!(topology.status, CheckStatus::Failed);
    assert!(topology.message.contains("--replSet"), "{topology:?}");
    let err = source.open(None).await.unwrap_err();
    assert!(matches!(err, SourceError::Config(_)), "{err:?}");
}
