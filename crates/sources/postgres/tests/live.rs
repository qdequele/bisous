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

/// Tests that change server-wide settings (`ALTER SYSTEM`) also need
/// `PG_TEST_ALTER_SYSTEM=1`: run them alone, on a throwaway server.
fn alter_system_url() -> Option<String> {
    let url = url()?;
    if std::env::var("PG_TEST_ALTER_SYSTEM").as_deref() == Ok("1") {
        Some(url)
    } else {
        eprintln!("PG_TEST_ALTER_SYSTEM is not 1: skipping a test that changes server settings");
        None
    }
}

/// A Postgres older than 14 (`PG_OLD_TEST_URL`), for the version gate.
fn old_url() -> Option<String> {
    match std::env::var("PG_OLD_TEST_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("PG_OLD_TEST_URL is not set: skipping old-Postgres test");
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
    source_with(url, sync_id, tables, json!({}))
}

/// `extra` is merged into the config (`slot_name`, `publication_name`).
fn source_with(url: &str, sync_id: &str, tables: &[&str], extra: Value) -> Box<dyn Source> {
    let mut config = json!({ "url": url });
    if let (Some(c), Some(e)) = (config.as_object_mut(), extra.as_object()) {
        c.extend(e.clone());
    }
    let spec = SourceSpec {
        kind: SourceKind::Postgres,
        config,
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

/// A table nobody analyzed yet (`reltuples` = -1) still gets an estimate,
/// from the live-row statistics: the setup UI prices its first sync. So
/// does a partitioned table, summed over its partitions; an empty one is 0.
#[tokio::test]
async fn row_estimate_without_analyze() {
    let Some(url) = url() else { return };
    let table = unique("est");
    {
        // Its own connection: closing it flushes its statistics.
        let db = admin(&url).await;
        db.batch_execute(&format!(
            "CREATE TABLE {table} (id int4 PRIMARY KEY, v text);
             INSERT INTO {table} SELECT g, 'x' FROM generate_series(1, 3) g;
             CREATE TABLE {table}_empty (id int4 PRIMARY KEY);
             CREATE TABLE {table}_parts (id int4, k int4, PRIMARY KEY (id, k)) PARTITION BY LIST (k);
             CREATE TABLE {table}_p1 PARTITION OF {table}_parts FOR VALUES IN (1);
             CREATE TABLE {table}_p2 PARTITION OF {table}_parts FOR VALUES IN (2);
             INSERT INTO {table}_parts SELECT g, 1 + g % 2 FROM generate_series(1, 5) g;"
        ))
        .await
        .unwrap();
    }
    let db = admin(&url).await;
    let reltuples: f32 = db
        .query_one(
            &format!("SELECT reltuples FROM pg_class WHERE relname = '{table}'"),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        reltuples < 0.0,
        "the fixture must not be analyzed: {reltuples}"
    );

    let mut src = source(&url, &unique("est"), &[]);
    let want = [
        (format!("public.{table}"), Some(3)),
        (format!("public.{table}_empty"), Some(0)),
        (format!("public.{table}_parts"), Some(5)),
    ];
    // Statistics reach the shared counters asynchronously: give them a moment.
    let mut got = Vec::new();
    for _ in 0..50 {
        let all = src.discover().await.unwrap();
        got = want
            .iter()
            .map(|(name, _)| {
                let info = all.iter().find(|c| &c.name == name).expect("listed");
                (name.clone(), info.estimated_rows)
            })
            .collect();
        if got == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(got, want);
    db.batch_execute(&format!("DROP TABLE {table}, {table}_empty, {table}_parts"))
        .await
        .unwrap();
}

/// No table picked yet (the setup UI tests the connection first): every
/// server-level check runs, the table checks are one `Ok` item, and the
/// report passes. Nothing is created.
#[tokio::test]
async fn check_without_tables_is_a_connection_check() {
    let Some(url) = url() else { return };
    let sync_id = unique("chk");
    let mut src = source(&url, &sync_id, &[]);
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:#?}");
    let names: Vec<_> = report.items.iter().map(|i| i.name.as_str()).collect();
    for name in [
        "connection",
        "server_version",
        "wal_level",
        "replication_privilege",
        "replication_slots",
        "wal_senders",
        "max_slot_wal_keep_size",
        "publication",
    ] {
        assert!(names.contains(&name), "{name}: {report:#?}");
    }
    let tables = report.items.last().unwrap();
    assert_eq!(tables.name, "tables");
    assert_eq!(tables.status, CheckStatus::Ok);
    assert_eq!(
        tables.message,
        "no table selected yet: table checks run once you pick them"
    );
    assert!(
        report.items.iter().all(|i| i.status != CheckStatus::Failed),
        "{report:#?}"
    );
    // A sync still needs a table.
    assert!(matches!(src.open(None).await, Err(SourceError::Config(_))));
    let db = admin(&url).await;
    let slots: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE $1",
            &[&format!("%{sync_id}%")],
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
    // Key change → Delete(old) + the whole new row, TOASTed column
    // included: a Patch would land on a document that does not exist.
    assert_eq!(c[2].op, Op::Delete);
    assert_eq!(c[2].key, vec![json!(1)]);
    assert_eq!(c[3].key, vec![json!(2)]);
    match &c[3].op {
        Op::Upsert { doc } => {
            assert_eq!(doc["big"].as_str().map(str::len), Some(100_000));
            assert_eq!(doc["b"], json!(false));
            assert_eq!(doc["m"], json!("sad"));
        }
        other => panic!("expected upsert, got {other:?}"),
    }

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

    // A checkpoint ahead of the server's WAL (restored from a backup, a
    // failover to a server that is behind): the changes in between are
    // not there to stream.
    let mut src = source(&url, &sync_id, &[&collection]);
    let future = Checkpoint(json!({"lsn": "FFFF/0"}));
    let err = src.open(Some(future)).await.unwrap_err();
    assert!(matches!(err, SourceError::PositionLost(_)), "{err:?}");

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

#[tokio::test]
async fn servers_older_than_14_are_refused_before_anything_is_created() {
    let Some(url) = old_url() else { return };
    let db = admin(&url).await;
    let version: i32 = db
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await
        .unwrap()
        .get(0);
    assert!(version < 140_000, "PG_OLD_TEST_URL is Postgres {version}");
    let table = unique("old");
    create_items(&db, &table).await;
    let sync_id = unique("old");
    let name = format!("meili_connect_{sync_id}");
    let mut src = source(&url, &sync_id, &[&format!("public.{table}")]);

    let report = src.check().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.name == "server_version")
        .unwrap();
    assert_eq!(item.status, CheckStatus::Failed, "{report:#?}");
    assert!(item.message.contains("14"), "{}", item.message);

    let err = src.open(None).await.unwrap_err();
    assert!(matches!(err, SourceError::Config(_)), "{err:?}");
    let slots: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    let pubs: i64 = db
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((slots, pubs), (0, 0));
}

/// A stream that grabs the slot back as soon as it loses it, like an engine
/// still running the sync would.
fn stubborn_holder(
    url: &str,
    sync_id: &str,
    collection: &str,
    from: Checkpoint,
) -> CancellationToken {
    let stop = CancellationToken::new();
    let (url, sync_id, collection, s) = (
        url.to_owned(),
        sync_id.to_owned(),
        collection.to_owned(),
        stop.clone(),
    );
    tokio::spawn(async move {
        while !s.is_cancelled() {
            let mut src = source(&url, &sync_id, &[&collection]);
            if src.open(Some(from.clone())).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let (tx, mut rx) = mpsc::channel(64);
            tokio::spawn(async move { while rx.recv().await.is_some() {} });
            let (_ack, acks) = ack_channel();
            let _ = src.stream(tx, acks, s.clone()).await;
        }
    });
    stop
}

#[tokio::test]
async fn teardown_never_reports_success_while_the_slot_survives() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("held");
    create_items(&db, &table).await;
    let sync_id = unique("held");
    let name = format!("meili_connect_{sync_id}");
    let collection = format!("public.{table}");
    let mut src = source(&url, &sync_id, &[&collection]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    drop(src);
    let stop = stubborn_holder(&url, &sync_id, &collection, start);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut other = source(&url, &sync_id, &[&collection]);
    let result = other.teardown().await;
    let count = |q: &'static str| {
        let db = &db;
        let name = name.clone();
        async move { db.query_one(q, &[&name]).await.unwrap().get::<_, i64>(0) }
    };
    let slots = count("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1").await;
    let pubs = count("SELECT count(*) FROM pg_publication WHERE pubname = $1").await;
    if slots > 0 {
        assert!(
            matches!(result, Err(SourceError::Transient(_))),
            "slot survived but teardown said {result:?}"
        );
        assert_eq!(pubs, 1, "publication dropped while the slot survived");
    } else {
        result.unwrap();
    }

    // Once nothing holds it any more, teardown finishes.
    stop.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;
    other.teardown().await.unwrap();
    assert_eq!(
        count("SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1").await,
        0
    );
    assert_eq!(
        count("SELECT count(*) FROM pg_publication WHERE pubname = $1").await,
        0
    );
}

#[tokio::test]
async fn names_taken_by_others_are_never_adopted_or_dropped() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("own");
    let other = unique("own_other");
    create_items(&db, &table).await;
    create_items(&db, &other).await;
    let collection = format!("public.{table}");
    let foreign_pub = unique("foreign_pub");
    let foreign_slot = unique("foreign_slot");
    db.batch_execute(&format!(
        "CREATE PUBLICATION {foreign_pub} FOR TABLE {other}"
    ))
    .await
    .unwrap();
    db.execute(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        &[&foreign_slot],
    )
    .await
    .unwrap();
    let pub_tables = || async {
        db.query(
            "SELECT tablename::text FROM pg_publication_tables WHERE pubname = $1",
            &[&foreign_pub],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect::<Vec<_>>()
    };
    let slot_exists = || async {
        db.query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
            &[&foreign_slot],
        )
        .await
        .unwrap()
        .get::<_, i64>(0)
            == 1
    };

    for extra in [
        json!({ "publication_name": foreign_pub }),
        json!({ "slot_name": foreign_slot }),
    ] {
        let sync_id = unique("own");
        let mut src = source_with(&url, &sync_id, &[&collection], extra.clone());
        let report = src.check().await.unwrap();
        assert!(!report.passed(), "{extra}: {report:#?}");
        let err = src.open(None).await.unwrap_err();
        assert!(
            matches!(&err, SourceError::Config(m) if m.contains("taken")),
            "{extra}: {err:?}"
        );
        src.teardown().await.unwrap();
        assert_eq!(pub_tables().await, vec![other.clone()], "{extra}");
        assert!(slot_exists().await, "{extra}");
    }

    // What we create carries our marker; a later open adopts it again.
    let sync_id = unique("own");
    let name = format!("meili_connect_{sync_id}");
    let mut src = source(&url, &sync_id, &[&collection]);
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    drop(src);
    let comment: Option<String> = db
        .query_one(
            "SELECT obj_description(oid, 'pg_publication') FROM pg_publication WHERE pubname = $1",
            &[&name],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(comment, Some(format!("meili-connect:{sync_id}")));
    let mut src = source(&url, &sync_id, &[&collection]);
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    src.teardown().await.unwrap();

    // A publication of ours paired with a slot name override: adopted.
    let sync_id = unique("own");
    let slot = unique("own_slot");
    let mut src = source_with(&url, &sync_id, &[&collection], json!({ "slot_name": slot }));
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    drop(src);
    let mut src = source_with(&url, &sync_id, &[&collection], json!({ "slot_name": slot }));
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    src.teardown().await.unwrap();

    db.batch_execute(&format!(
        "DROP PUBLICATION {foreign_pub}; SELECT pg_drop_replication_slot('{foreign_slot}');"
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn snapshot_survives_role_timeouts_under_back_pressure() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let role = unique("slow");
    let table = unique("slow");
    db.batch_execute(&format!(
        "CREATE ROLE {role} LOGIN REPLICATION PASSWORD 'pw';
         ALTER ROLE {role} SET idle_in_transaction_session_timeout = '1s';
         ALTER ROLE {role} SET statement_timeout = '1s';
         ALTER ROLE {role} SET lock_timeout = '1s';
         GRANT CREATE ON DATABASE {} TO {role};
         CREATE TABLE {table} (id int8 PRIMARY KEY, n int4, body text);
         ALTER TABLE {table} OWNER TO {role};
         INSERT INTO {table} SELECT g, g, 'row' FROM generate_series(1, 1200) g;",
        db.query_one("SELECT current_database()::text", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    ))
    .await
    .unwrap();
    let mut role_url = url::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(Some("pw")).unwrap();
    let role_url = role_url.to_string();

    let sync_id = unique("slow");
    let mut src = source(&role_url, &sync_id, &[&format!("public.{table}")]);
    assert!(matches!(
        src.open(None).await.unwrap(),
        Opened::Fresh { .. }
    ));
    // One slot of buffer and a consumer that stalls: the snapshot
    // transaction sits idle well past the role's 1 s timeouts.
    let (tx, mut rx) = mpsc::channel(1);
    let consumer = tokio::spawn(async move {
        let mut n = 0usize;
        while let Some(ev) = rx.recv().await {
            assert!(matches!(ev, SourceEvent::Change(_)));
            n += 1;
            if n == 1 || n == 700 {
                tokio::time::sleep(Duration::from_millis(2500)).await;
            }
        }
        n
    });
    let result = src.snapshot(tx, CancellationToken::new()).await;
    let n = consumer.await.unwrap();
    result.unwrap();
    assert_eq!(n, 1200);
    src.teardown().await.unwrap();
    db.batch_execute(&format!(
        "DROP TABLE {table}; REVOKE CREATE ON DATABASE {} FROM {role}; DROP ROLE {role};",
        db.query_one("SELECT current_database()::text", &[])
            .await
            .unwrap()
            .get::<_, String>(0)
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn inheritance_is_refused_partitions_stream_under_the_root() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let parent = unique("inh");
    let part = unique("part");
    db.batch_execute(&format!(
        "CREATE TABLE {parent} (id int8 PRIMARY KEY, n int4, body text);
         CREATE TABLE {parent}_child () INHERITS ({parent});
         CREATE TABLE {part} (id int8 PRIMARY KEY, n int4, body text) PARTITION BY RANGE (id);
         CREATE TABLE {part}_a PARTITION OF {part} FOR VALUES FROM (0) TO (100);
         CREATE TABLE {part}_b PARTITION OF {part} FOR VALUES FROM (100) TO (200);
         INSERT INTO {part} VALUES (1, 1, 'a'), (150, 1, 'b');"
    ))
    .await
    .unwrap();

    let mut src = source(&url, &unique("inh"), &[&format!("public.{parent}")]);
    let report = src.check().await.unwrap();
    let item = report
        .items
        .iter()
        .find(|i| i.name == format!("table public.{parent}"))
        .unwrap();
    assert_eq!(item.status, CheckStatus::Failed, "{report:#?}");
    assert!(item.message.contains("inherit"), "{}", item.message);
    let err = src.open(None).await.unwrap_err();
    assert!(matches!(err, SourceError::Config(_)), "{err:?}");

    let collection = format!("public.{part}");
    let sync_id = unique("part");
    let mut src = source(&url, &sync_id, &[&collection]);
    assert!(src.check().await.unwrap().passed());
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let via_root: bool = db
        .query_one(
            "SELECT pubviaroot FROM pg_publication WHERE pubname = $1",
            &[&format!("meili_connect_{sync_id}")],
        )
        .await
        .unwrap()
        .get(0);
    assert!(via_root);
    let mut model = Model::default();
    let (tx, mut rx) = mpsc::channel(16);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    while let Some(ev) = rx.recv().await {
        model.apply(ev);
    }
    assert_eq!(model.table(&collection).len(), 2);
    let mut s = start_stream(src);
    s.ack.send(Some(start)).unwrap();
    db.batch_execute(&format!(
        "INSERT INTO {part} VALUES (2, 2, 'c'), (151, 2, 'd'); UPDATE {part}_b SET n = 9 WHERE id = 150;"
    ))
    .await
    .unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.table(&collection).len() == 4 && m.table(&collection)["150"]["n"] == json!(9)
    })
    .await;
    assert!(ok, "{model:#?}");
    let mut src = s.stop().await;
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn renaming_a_mapped_table_mid_stream_is_a_config_error() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let table = unique("ren");
    create_items(&db, &table).await;
    let sync_id = unique("ren");
    let mut src = source(&url, &sync_id, &[&format!("public.{table}")]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut s = start_stream(src);
    s.ack.send(Some(start)).unwrap();
    let mut model = Model::default();
    db.batch_execute(&format!(
        "INSERT INTO {table} VALUES (1, 1, 'a');
         ALTER TABLE {table} RENAME TO {table}_new;
         INSERT INTO {table}_new VALUES (2, 2, 'b');"
    ))
    .await
    .unwrap();
    pump_until(&mut s.rx, &mut model, Duration::from_secs(10), |_| false).await;
    let (mut src, result) = tokio::time::timeout(Duration::from_secs(20), s.task)
        .await
        .expect("the stream stops by itself")
        .unwrap();
    assert!(
        matches!(&result, Err(SourceError::Config(m)) if m.contains("renamed")),
        "{result:?}"
    );
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn covering_indexes_key_on_their_key_columns_only() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let pk = unique("cpk");
    let ri = unique("cri");
    db.batch_execute(&format!(
        "CREATE TABLE {pk} (id int4, name text, PRIMARY KEY (id) INCLUDE (name));
         CREATE TABLE {ri} (code text NOT NULL, name text, n int4);
         CREATE UNIQUE INDEX {ri}_code ON {ri} (code) INCLUDE (name);
         ALTER TABLE {ri} REPLICA IDENTITY USING INDEX {ri}_code;"
    ))
    .await
    .unwrap();
    let (cp, cr) = (format!("public.{pk}"), format!("public.{ri}"));
    let sync_id = unique("cov");
    let mut src = source(&url, &sync_id, &[&cp, &cr]);
    let all = src.discover().await.unwrap();
    let key = |c: &str| all.iter().find(|i| i.name == c).unwrap().key.clone();
    assert_eq!(key(&cp), vec!["id"]);
    assert_eq!(key(&cr), vec!["code"]);

    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut s = start_stream(src);
    s.ack.send(Some(start)).unwrap();
    let mut model = Model::default();
    db.batch_execute(&format!(
        "INSERT INTO {pk} VALUES (1, 'a'); UPDATE {pk} SET name = 'b' WHERE id = 1;
         INSERT INTO {pk} VALUES (2, 'x'); DELETE FROM {pk} WHERE id = 2;
         INSERT INTO {ri} VALUES ('c1', 'a', 0); UPDATE {ri} SET name = 'b' WHERE code = 'c1';
         INSERT INTO {ri} VALUES ('c2', 'x', 0); DELETE FROM {ri} WHERE code = 'c2';"
    ))
    .await
    .unwrap();
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.changes.len() >= 8
    })
    .await;
    assert!(ok, "{model:#?}");
    assert_eq!(
        model.table(&cp),
        HashMap::from([(
            "1".to_owned(),
            json!({"id": 1, "name": "b"}).as_object().cloned().unwrap()
        )])
    );
    assert_eq!(
        model.table(&cr),
        HashMap::from([(
            "c1".to_owned(),
            json!({"code": "c1", "name": "b", "n": 0})
                .as_object()
                .cloned()
                .unwrap()
        )])
    );
    assert!(model.changes.iter().all(|c| c.key.len() == 1), "{model:#?}");
    let mut src = s.stop().await;
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn key_change_with_unchanged_toast_gives_whole_documents() {
    let Some(url) = url() else { return };
    let db = admin(&url).await;
    let def = unique("ktd");
    let full = unique("ktf");
    for t in [&def, &full] {
        db.batch_execute(&format!(
            "CREATE TABLE {t} (id int4 PRIMARY KEY, n int4, big text);
             ALTER TABLE {t} ALTER COLUMN big SET STORAGE EXTERNAL;"
        ))
        .await
        .unwrap();
    }
    db.batch_execute(&format!("ALTER TABLE {full} REPLICA IDENTITY FULL"))
        .await
        .unwrap();
    let (cd, cf) = (format!("public.{def}"), format!("public.{full}"));
    let sync_id = unique("kt");
    let mut src = source(&url, &sync_id, &[&cd, &cf]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    let (tx, _rx) = mpsc::channel(4);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let mut s = start_stream(src);
    s.ack.send(Some(start)).unwrap();
    let mut model = Model::default();

    for t in [&def, &full] {
        db.batch_execute(&format!(
            "INSERT INTO {t} VALUES (1, 0, repeat('x', 100000));
             UPDATE {t} SET id = 2, n = 1 WHERE id = 1;
             INSERT INTO {t} VALUES (10, 0, repeat('y', 100000));
             BEGIN; UPDATE {t} SET id = 11 WHERE id = 10; DELETE FROM {t} WHERE id = 11; COMMIT;"
        ))
        .await
        .unwrap();
    }
    let ok = pump_until(&mut s.rx, &mut model, Duration::from_secs(30), |m| {
        m.changes
            .iter()
            .filter(|c| c.key == vec![json!(11)] && c.op == Op::Delete)
            .count()
            == 2
    })
    .await;
    assert!(ok, "{model:#?}");
    for c in [&cd, &cf] {
        let docs = model.table(c);
        assert_eq!(docs.len(), 1, "{c}: {docs:?}");
        let doc = &docs["2"];
        assert_eq!(doc["n"], json!(1), "{c}");
        assert_eq!(doc["big"].as_str().map(str::len), Some(100_000), "{c}");
        assert!(
            model
                .changes
                .iter()
                .filter(|ch| &ch.collection == c)
                .all(|ch| !matches!(ch.op, Op::Patch { .. })),
            "{c}: a key change produced a patch"
        );
    }
    let mut src = s.stop().await;
    src.teardown().await.unwrap();
}

/// The slot is invalidated after `open` checked it and before
/// START_REPLICATION: the server says only "can no longer get changes"
/// (SQLSTATE 55000), the reason being in the DETAIL.
#[tokio::test]
async fn slot_invalidated_before_start_replication_is_position_lost() {
    let Some(url) = alter_system_url() else {
        return;
    };
    let db = admin(&url).await;
    let table = unique("inv");
    create_items(&db, &table).await;
    let sync_id = unique("inv");
    let slot = format!("meili_connect_{sync_id}");
    let collection = format!("public.{table}");
    let mut src = source(&url, &sync_id, &[&collection]);
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!()
    };
    drop(src);
    let mut src = source(&url, &sync_id, &[&collection]);
    assert_eq!(src.open(Some(start)).await.unwrap(), Opened::Resumed);

    db.batch_execute("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
        .await
        .unwrap();
    db.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    let mut lost = false;
    for i in 0..10 {
        db.batch_execute(&format!(
            "INSERT INTO {table} VALUES ({i}, 0, repeat('x', 10000))"
        ))
        .await
        .unwrap();
        db.batch_execute("SELECT pg_switch_wal()").await.unwrap();
        db.batch_execute("CHECKPOINT").await.unwrap();
        let status: Option<String> = db
            .query_one(
                "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
                &[&slot],
            )
            .await
            .unwrap()
            .get(0);
        if status.as_deref() == Some("lost") {
            lost = true;
            break;
        }
    }
    db.batch_execute("ALTER SYSTEM RESET max_slot_wal_keep_size")
        .await
        .unwrap();
    db.batch_execute("SELECT pg_reload_conf()").await.unwrap();
    assert!(lost, "could not invalidate the slot");

    let (tx, _rx) = mpsc::channel(4);
    let (_ack, acks) = ack_channel();
    let err = tokio::time::timeout(
        Duration::from_secs(20),
        src.stream(tx, acks, CancellationToken::new()),
    )
    .await
    .expect("stream fails fast")
    .unwrap_err();
    assert!(matches!(err, SourceError::PositionLost(_)), "{err:?}");
    src.teardown().await.unwrap();
}
