//! Against a live Postgres with `wal_level=logical`. Set `PG_TEST_URL`
//! (e.g. `postgres://postgres:pw@127.0.0.1:5432/postgres?sslmode=disable`,
//! a superuser or a role with REPLICATION + CREATE); skipped otherwise.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::time::Duration;

use meili_connect_core::{
    AckSender, Change, CheckStatus, Checkpoint, Document, Op, Opened, Source, SourceError,
    SourceEvent, SourceFactory, SourceKind, SourceSpec, StreamMapping, ack_channel, document_id,
};
use meili_connect_source_postgres::PostgresSourceFactory;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn url() -> Option<String> {
    match std::env::var("PG_TEST_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("PG_TEST_URL is not set: skipping live Postgres test");
            None
        }
    }
}

fn unique(prefix: &str) -> String {
    format!("{prefix}_{:08x}", rand::random::<u32>())
}

/// Plain-text admin connection for fixtures (the source under test may use
/// TLS on the same server).
async fn admin(url: &str) -> tokio_postgres::Client {
    let url = url
        .replace("sslmode=require", "sslmode=disable")
        .replace("sslmode=verify-ca", "sslmode=disable")
        .replace("sslmode=verify-full", "sslmode=disable");
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    client
}

fn source(url: &str, sync_id: &str, tables: &[&str]) -> Box<dyn Source> {
    let spec = SourceSpec {
        kind: SourceKind::Postgres,
        config: json!({ "url": url }),
    };
    let streams: Vec<StreamMapping> = tables
        .iter()
        .map(|t| serde_json::from_value(json!({"collection": t, "index": t})).unwrap())
        .collect();
    PostgresSourceFactory::new()
        .build(sync_id, &spec, &streams)
        .expect("build")
}

/// Applies events like the engine would and remembers checkpoints.
#[derive(Default, Debug)]
struct Model {
    docs: HashMap<(String, String), Document>,
    checkpoints: Vec<Checkpoint>,
    truncates: Vec<String>,
    schema_changes: Vec<String>,
    changes: Vec<Change>,
}

impl Model {
    fn apply(&mut self, ev: SourceEvent) {
        match ev {
            SourceEvent::Change(c) => {
                let id = (c.collection.clone(), document_id(&c.key));
                match &c.op {
                    Op::Upsert { doc } => {
                        self.docs.insert(id, doc.clone());
                    }
                    Op::Patch { doc, unset } => {
                        let entry = self.docs.entry(id).or_default();
                        for (k, v) in doc {
                            entry.insert(k.clone(), v.clone());
                        }
                        for k in unset {
                            entry.remove(k);
                        }
                    }
                    Op::Delete => {
                        self.docs.remove(&id);
                    }
                }
                self.changes.push(c);
            }
            SourceEvent::Checkpoint(cp) => self.checkpoints.push(cp),
            SourceEvent::Truncate { collection } => {
                self.docs.retain(|(c, _), _| c != &collection);
                self.truncates.push(collection);
            }
            SourceEvent::SchemaChanged { collection } => self.schema_changes.push(collection),
            SourceEvent::Skipped { .. } => panic!("postgres never skips: {ev:?}"),
        }
    }

    fn table(&self, collection: &str) -> HashMap<String, Document> {
        self.docs
            .iter()
            .filter(|((c, _), _)| c == collection)
            .map(|((_, id), d)| (id.clone(), d.clone()))
            .collect()
    }
}

/// Rows of `table` as id → {id, n, body}.
async fn rows(db: &tokio_postgres::Client, table: &str) -> HashMap<String, Document> {
    let rows = db
        .query(&format!("SELECT id, n, body FROM {table}"), &[])
        .await
        .unwrap();
    rows.iter()
        .map(|r| {
            let id: i64 = r.get(0);
            let n: Option<i32> = r.get(1);
            let body: Option<String> = r.get(2);
            let mut d = Document::new();
            d.insert("id".into(), json!(id));
            d.insert("n".into(), json!(n));
            d.insert("body".into(), json!(body));
            (id.to_string(), d)
        })
        .collect()
}

async fn create_items(db: &tokio_postgres::Client, table: &str) {
    db.batch_execute(&format!(
        "CREATE TABLE {table} (id int8 PRIMARY KEY, n int4, body text)"
    ))
    .await
    .unwrap();
}

/// Read events until `pred` holds on the model or the deadline passes.
async fn pump_until(
    rx: &mut mpsc::Receiver<SourceEvent>,
    model: &mut Model,
    timeout: Duration,
    mut pred: impl FnMut(&Model) -> bool,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if pred(model) {
            return true;
        }
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(ev)) => model.apply(ev),
            _ => return pred(model),
        }
    }
}

struct Streaming {
    rx: mpsc::Receiver<SourceEvent>,
    ack: AckSender,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<(Box<dyn Source>, Result<(), SourceError>)>,
}

fn start_stream(mut src: Box<dyn Source>) -> Streaming {
    let (tx, rx) = mpsc::channel(64);
    let (ack, acks) = ack_channel();
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move {
        let r = src.stream(tx, acks, c).await;
        (src, r)
    });
    Streaming {
        rx,
        ack,
        cancel,
        task,
    }
}

impl Streaming {
    async fn stop(self) -> Box<dyn Source> {
        self.cancel.cancel();
        let (src, r) = tokio::time::timeout(Duration::from_secs(20), self.task)
            .await
            .expect("stream stops on cancel")
            .unwrap();
        r.expect("stream returns Ok on cancel");
        src
    }
}

async fn slot_confirmed(db: &tokio_postgres::Client, slot: &str) -> Option<String> {
    db.query_opt(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
        &[&slot],
    )
    .await
    .unwrap()
    .and_then(|r| r.get(0))
}

fn lsn_of(cp: &Checkpoint) -> u64 {
    let text = cp.0["lsn"].as_str().unwrap();
    let (hi, lo) = text.split_once('/').unwrap();
    (u64::from_str_radix(hi, 16).unwrap() << 32) | u64::from_str_radix(lo, 16).unwrap()
}

fn parse_lsn(text: &str) -> u64 {
    lsn_of(&Checkpoint(json!({ "lsn": text })))
}

#[tokio::test]
async fn check_and_discover() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("disc");
    db.batch_execute(&format!(
        "CREATE TABLE {table} (b text, a int4, v float8[], e jsonb, PRIMARY KEY (b, a));
         CREATE TABLE {table}_nokey (x int4);
         INSERT INTO {table} VALUES ('x', 1, '{{1.5}}', '{{}}');
         ANALYZE {table};"
    ))
    .await
    .unwrap();

    let mut src = source(&url, &unique("chk"), &[&format!("public.{table}")]);
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:#?}");
    for name in [
        "wal_level",
        "replication_privilege",
        "replication_slots",
        "wal_senders",
    ] {
        assert!(
            report
                .items
                .iter()
                .any(|i| i.name == name && i.status == CheckStatus::Ok),
            "{name}: {report:#?}"
        );
    }

    let mut bad = source(
        &url,
        &unique("chk"),
        &[&format!("public.{table}_nokey"), "public.does_not_exist"],
    );
    let report = bad.check().await.unwrap();
    assert!(!report.passed());
    let failed: Vec<_> = report
        .items
        .iter()
        .filter(|i| i.status == CheckStatus::Failed)
        .map(|i| i.name.clone())
        .collect();
    assert_eq!(failed.len(), 2, "{report:#?}");

    let all = src.discover().await.unwrap();
    let info = all
        .iter()
        .find(|c| c.name == format!("public.{table}"))
        .expect("table listed");
    assert_eq!(info.key, vec!["b", "a"]);
    assert_eq!(info.estimated_rows, Some(1));
    let types: Vec<_> = info.fields.iter().map(|f| f.source_type.as_str()).collect();
    assert_eq!(types, ["text", "integer", "double precision[]", "jsonb"]);
    assert!(!info.fields[0].nullable && info.fields[2].nullable);
    let nokey = all
        .iter()
        .find(|c| c.name == format!("public.{table}_nokey"))
        .unwrap();
    assert!(nokey.key.is_empty());

    // check and discover created nothing.
    let slots: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'meili_connect_chk%'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(slots, 0);
}

#[tokio::test]
async fn gapless_first_sync_under_concurrent_writes_then_resume() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("items");
    let collection = format!("public.{table}");
    create_items(&db, &table).await;
    db.batch_execute(&format!(
        "INSERT INTO {table} SELECT g, g, 'row ' || g FROM generate_series(1, 3000) g"
    ))
    .await
    .unwrap();

    // Writer: inserts, updates and deletes all through open + snapshot.
    let stop = CancellationToken::new();
    let writer = {
        let url = url.clone();
        let table = table.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let db = admin(&url).await;
            let mut i = 3001i64;
            while !stop.is_cancelled() {
                db.batch_execute(&format!(
                    "INSERT INTO {table} VALUES ({i}, 0, 'new');
                     UPDATE {table} SET n = n + 1 WHERE id = {};
                     DELETE FROM {table} WHERE id = {};",
                    i % 3000 + 1,
                    (i * 7) % 3000 + 1
                ))
                .await
                .unwrap();
                i += 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;

    let sync_id = unique("gap");
    let mut src = source(&url, &sync_id, &[&collection]);
    let opened = src.open(None).await.unwrap();
    let Opened::Fresh { start } = opened else {
        panic!("expected fresh")
    };

    let mut model = Model::default();
    let (tx, mut rx) = mpsc::channel(16);
    let collect = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(ev) = rx.recv().await {
            events.push(ev);
        }
        events
    });
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let snap = collect.await.unwrap();
    assert!(snap.len() >= 2900, "snapshot read {} rows", snap.len());
    assert!(snap.iter().all(|e| matches!(
        e,
        SourceEvent::Change(Change {
            op: Op::Upsert { .. },
            ..
        })
    )));
    for ev in snap {
        model.apply(ev);
    }

    let mut s = start_stream(src);
    s.ack.send(Some(start.clone())).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.cancel();
    writer.await.unwrap();
    // A marker transaction after the writer: once it is seen, everything
    // before it has been streamed.
    db.batch_execute(&format!("INSERT INTO {table} VALUES (-1, -1, 'marker')"))
        .await
        .unwrap();
    let seen = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.table(&collection).contains_key("-1") && !m.checkpoints.is_empty()
    })
    .await;
    assert!(seen, "marker not streamed");
    // Drain to the marker's commit checkpoint.
    let n = model.checkpoints.len();
    pump_until(&mut s.rx, &mut model, Duration::from_secs(2), |m| {
        m.checkpoints.len() > n
    })
    .await;

    let streamed = model.changes.len().saturating_sub(3000);
    assert!(streamed >= 30, "only {streamed} changes streamed");
    let expected = rows(&db, &table).await;
    let got = model.table(&collection);
    assert_eq!(got.len(), expected.len());
    for (id, doc) in &expected {
        assert_eq!(got.get(id), Some(doc), "row {id}");
    }
    let last = model.checkpoints.last().cloned().unwrap();
    assert!(lsn_of(&last) >= lsn_of(&start));

    // Ack the last checkpoint, stop, write while disconnected, resume.
    s.ack.send(Some(last.clone())).unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let src = s.stop().await;
    drop(src);
    db.batch_execute(&format!(
        "UPDATE {table} SET body = 'after' WHERE id = 5; DELETE FROM {table} WHERE id = 6;
         INSERT INTO {table} VALUES (-2, 0, 'resumed')"
    ))
    .await
    .unwrap();

    let mut src = source(&url, &sync_id, &[&collection]);
    assert_eq!(src.open(Some(last.clone())).await.unwrap(), Opened::Resumed);
    let mut s = start_stream(src);
    s.ack.send(Some(last)).unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.table(&collection).contains_key("-2")
    })
    .await;
    assert!(ok, "changes made while disconnected were not replayed");
    let got = model.table(&collection);
    let expected = rows(&db, &table).await;
    assert_eq!(got, expected);
    let src = s.stop().await;
    let mut src = src;
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn change_kinds_toast_key_change_truncate_types() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("kinds");
    let mood = unique("mood");
    let collection = format!("public.{table}");
    db.batch_execute(&format!(
        "CREATE TYPE {mood} AS ENUM ('happy', 'sad');
         CREATE TABLE {table} (
            id int4 PRIMARY KEY, big text, b bool, num numeric, f float8, ts timestamptz,
            j jsonb, bytes bytea, arr int4[], m {mood}, u uuid, d date);
         ALTER TABLE {table} ALTER COLUMN big SET STORAGE EXTERNAL;"
    ))
    .await
    .unwrap();

    let sync_id = unique("kinds");
    let mut src = source(&url, &sync_id, &[&collection]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut s = start_stream(src);
    s.ack.send(Some(start)).unwrap();
    let mut model = Model::default();

    db.batch_execute(&format!(
        "INSERT INTO {table} VALUES (1, repeat('x', 100000), true, 12.50, 'NaN',
            '2024-01-02 03:04:05.5+02', '{{\"a\": [1]}}', '\\x0102', '{{1,NULL,3}}', 'sad',
            'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '2024-02-29');
         UPDATE {table} SET b = false WHERE id = 1;
         UPDATE {table} SET id = 2 WHERE id = 1;"
    ))
    .await
    .unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.changes.len() >= 4
    })
    .await;
    assert!(ok, "{model:#?}");
    let c = &model.changes;
    let Op::Upsert { doc } = &c[0].op else {
        panic!("{:?}", c[0])
    };
    assert_eq!(doc["big"].as_str().map(str::len), Some(100_000));
    assert_eq!(doc["b"], json!(true));
    assert_eq!(doc["num"], json!(12.5));
    assert_eq!(doc["f"], Value::Null);
    assert_eq!(doc["ts"], json!("2024-01-02T01:04:05.5+00:00"));
    assert_eq!(doc["j"], json!({"a": [1]}));
    assert_eq!(doc["bytes"], json!("AQI="));
    assert_eq!(doc["arr"], json!([1, null, 3]));
    assert_eq!(doc["m"], json!("sad"));
    assert_eq!(doc["u"], json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"));
    assert_eq!(doc["d"], json!("2024-02-29"));
    // UPDATE leaving the TOASTed column alone → Patch without it.
    match &c[1].op {
        Op::Patch { doc, unset } => {
            assert!(!doc.contains_key("big"));
            assert_eq!(doc["b"], json!(false));
            assert!(unset.is_empty());
        }
        other => panic!("expected patch, got {other:?}"),
    }
    // Key change → Delete(old) + new row.
    assert_eq!(c[2].op, Op::Delete);
    assert_eq!(c[2].key, vec![json!(1)]);
    assert_eq!(c[3].key, vec![json!(2)]);

    db.batch_execute(&format!("TRUNCATE {table}"))
        .await
        .unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        !m.truncates.is_empty()
    })
    .await;
    assert!(ok);
    assert_eq!(model.truncates, vec![collection.clone()]);

    db.batch_execute(&format!(
        "ALTER TABLE {table} ADD COLUMN extra text; INSERT INTO {table} (id, extra) VALUES (9, 'e')"
    ))
    .await
    .unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.changes.iter().any(|c| c.key == vec![json!(9)])
    })
    .await;
    assert!(ok);
    assert_eq!(model.schema_changes, vec![collection.clone()]);
    let src = s.stop().await;
    let mut src = src;
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn ack_moves_confirmed_flush_and_never_past_it() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("acks");
    create_items(&db, &table).await;
    let sync_id = unique("acks");
    let slot = format!("meili_connect_{sync_id}");
    let mut src = source(&url, &sync_id, &[&format!("public.{table}")]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut s = start_stream(src);
    let mut model = Model::default();

    for i in 0..3 {
        db.batch_execute(&format!("INSERT INTO {table} VALUES ({i}, {i}, 'x')"))
            .await
            .unwrap();
    }
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.checkpoints.len() >= 3
    })
    .await;
    assert!(ok);
    let first = model.checkpoints[0].clone();
    let third = model.checkpoints[2].clone();

    // Nothing acked: the slot stays at the start position.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let confirmed = parse_lsn(&slot_confirmed(&db, &slot).await.unwrap());
    assert!(confirmed <= lsn_of(&start), "slot moved without an ack");

    // Ack the first commit: the slot follows it, not further.
    s.ack.send(Some(first.clone())).unwrap();
    let mut confirmed = 0;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        confirmed = parse_lsn(&slot_confirmed(&db, &slot).await.unwrap());
        if confirmed == lsn_of(&first) {
            break;
        }
    }
    assert_eq!(confirmed, lsn_of(&first));
    assert!(confirmed < lsn_of(&third));

    s.ack.send(Some(third.clone())).unwrap();
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        confirmed = parse_lsn(&slot_confirmed(&db, &slot).await.unwrap());
        if confirmed == lsn_of(&third) {
            break;
        }
    }
    assert_eq!(confirmed, lsn_of(&third));
    let src = s.stop().await;
    let mut src = src;
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn dropped_slot_is_position_lost_and_teardown_is_idempotent() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("lost");
    create_items(&db, &table).await;
    let sync_id = unique("lost");
    let slot = format!("meili_connect_{sync_id}");
    let collection = format!("public.{table}");
    let mut src = source(&url, &sync_id, &[&collection]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    drop(src);

    // A slot that confirmed past the checkpoint cannot honour it either.
    let mut src = source(&url, &sync_id, &[&collection]);
    let ahead = Checkpoint(json!({"lsn": "0/1"}));
    assert!(matches!(
        src.open(Some(ahead)).await,
        Err(SourceError::PositionLost(_))
    ));

    db.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .await
        .unwrap();
    let mut src = source(&url, &sync_id, &[&collection]);
    let err = src.open(Some(start)).await.unwrap_err();
    assert!(matches!(err, SourceError::PositionLost(_)), "{err:?}");

    // Re-open fresh (a resync), then tear down twice.
    let mut src = source(&url, &sync_id, &[&collection]);
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    src.teardown().await.unwrap();
    src.teardown().await.unwrap();
    let slots: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    let pubs: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = $1",
            &[&slot],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((slots, pubs), (0, 0));
}

#[tokio::test]
async fn teardown_while_streaming_and_bad_credentials() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("td");
    create_items(&db, &table).await;
    let sync_id = unique("td");
    let mut src = source(&url, &sync_id, &[&format!("public.{table}")]);
    let Opened::Fresh { .. } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let s = start_stream(src);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Another source of the same sync tears down while the stream holds
    // the slot.
    let mut other = source(&url, &sync_id, &[&format!("public.{table}")]);
    other.teardown().await.unwrap();
    s.cancel.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(20), s.task).await;

    // Wrong password → Config, and the password never shows.
    let bad = url.replacen("://postgres:", "://postgres:wrong-hunter2", 1);
    if bad != url {
        let mut src = source(&bad, &unique("bad"), &[&format!("public.{table}")]);
        let err = src.open(None).await.unwrap_err();
        assert!(matches!(err, SourceError::Config(_)), "{err:?}");
        assert!(!err.to_string().contains("hunter2"));
    }
}
