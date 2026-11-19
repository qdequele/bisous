//! Live tests against a MySQL 8 server with GTIDs and FULL row images.
//!
//! ```text
//! docker run -d --name mc-test-mysql -p 127.0.0.1:3306:3306 -e MYSQL_ROOT_PASSWORD=pw \
//!   mysql:8.4 --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-row-image=FULL
//! MYSQL_TEST_URL=mysql://root:pw@127.0.0.1:3306 cargo test -p meili-connect-source-mysql
//! ```
//!
//! Skipped (passing, with a notice) when `MYSQL_TEST_URL` is unset. The
//! tests share one server and one of them purges binlogs, so they run one
//! at a time.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::time::Duration;

use meili_connect_core::{
    Checkpoint, Op, Opened, Source, SourceError, SourceEvent, SourceKind, SourceSpec,
    StreamMapping, ack_channel,
};
use meili_connect_source_mysql::{GtidSet, MysqlSource};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

static SERIAL: Mutex<()> = Mutex::const_new(());

fn url() -> Option<String> {
    match std::env::var("MYSQL_TEST_URL") {
        Ok(u) if !u.is_empty() => Some(u),
        _ => {
            eprintln!("MYSQL_TEST_URL is unset: skipping live MySQL test");
            None
        }
    }
}

fn config(url: &str, database: &str) -> Value {
    let opts = Opts::from_url(url).unwrap();
    json!({
        "host": opts.ip_or_hostname(),
        "port": opts.tcp_port(),
        "user": opts.user().unwrap_or("root"),
        "password": opts.pass().unwrap_or(""),
        "database": database,
        "tls": {"mode": "required"},
        "snapshot_batch_size": 7,
    })
}

fn source(url: &str, db: &str, tables: &[&str], sync_id: &str) -> MysqlSource {
    let streams: Vec<StreamMapping> = tables
        .iter()
        .map(|t| {
            serde_json::from_value(json!({"collection": format!("{db}.{t}"), "index": t})).unwrap()
        })
        .collect();
    MysqlSource::new(
        sync_id,
        &SourceSpec {
            kind: SourceKind::Mysql,
            config: config(url, db),
        },
        &streams,
    )
    .unwrap()
}

async fn admin(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).unwrap()).await.unwrap()
}

async fn gtid_on(conn: &mut Conn) -> bool {
    let mode: Option<String> = conn.query_first("SELECT @@GLOBAL.gtid_mode").await.unwrap();
    mode.as_deref() == Some("ON")
}

async fn fresh_db(conn: &mut Conn, db: &str) {
    conn.query_drop(format!("DROP DATABASE IF EXISTS `{db}`"))
        .await
        .unwrap();
    conn.query_drop(format!("CREATE DATABASE `{db}`"))
        .await
        .unwrap();
}

/// A running `stream` and its event channel.
struct Running {
    rx: mpsc::Receiver<SourceEvent>,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), SourceError>>,
}

impl Running {
    fn start(mut src: MysqlSource) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let (_ack_tx, ack_rx) = ack_channel();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            let _keep = _ack_tx;
            src.stream(tx, ack_rx, c).await
        });
        Self { rx, cancel, task }
    }

    /// Receives events until `done` says stop, failing after 30 s.
    async fn until(&mut self, mut done: impl FnMut(&SourceEvent) -> bool) -> Vec<SourceEvent> {
        let mut out = Vec::new();
        loop {
            let ev = tokio::time::timeout(Duration::from_secs(30), self.rx.recv())
                .await
                .expect("timed out waiting for binlog events")
                .expect("stream ended");
            let stop = done(&ev);
            out.push(ev);
            if stop {
                return out;
            }
        }
    }

    async fn stop(self) {
        self.cancel.cancel();
        let res = tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("stream did not honour cancellation")
            .unwrap();
        assert!(res.is_ok(), "{res:?}");
    }
}

/// Applies changes to an in-memory index keyed by the JSON key.
fn apply(model: &mut BTreeMap<String, Value>, events: &[SourceEvent]) {
    for ev in events {
        if let SourceEvent::Change(c) = ev {
            let key = serde_json::to_string(&c.key).unwrap();
            match &c.op {
                Op::Upsert { doc } => {
                    model.insert(key, Value::Object(doc.clone()));
                }
                Op::Delete => {
                    model.remove(&key);
                }
                Op::Patch { .. } => panic!("mysql never emits patches"),
            }
        }
    }
}

fn last_checkpoint(events: &[SourceEvent]) -> Option<Checkpoint> {
    events.iter().rev().find_map(|e| match e {
        SourceEvent::Checkpoint(c) => Some(c.clone()),
        _ => None,
    })
}

fn is_change_with(ev: &SourceEvent, id: i64) -> bool {
    matches!(ev, SourceEvent::Change(c) if c.key == vec![json!(id)])
}

#[tokio::test]
async fn check_and_discover() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_check").await;
    conn.query_drop(
        "CREATE TABLE mc_check.items (id INT UNSIGNED PRIMARY KEY, name VARCHAR(20) NOT NULL)",
    )
    .await
    .unwrap();
    conn.query_drop(
        "CREATE TABLE mc_check.pairs (a INT, b VARCHAR(5), v TEXT, PRIMARY KEY (b, a))",
    )
    .await
    .unwrap();
    conn.query_drop("CREATE TABLE mc_check.nokey (v INT)")
        .await
        .unwrap();

    let mut src = source(&url, "mc_check", &["items", "pairs"], "check-sync");
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:#?}");
    for name in ["log_bin", "binlog_format", "binlog_row_image"] {
        let item = report.items.iter().find(|i| i.name == name).unwrap();
        assert_eq!(item.status, meili_connect_core::CheckStatus::Ok, "{item:?}");
    }
    let gtid = report.items.iter().find(|i| i.name == "gtid_mode").unwrap();
    let want = if gtid_on(&mut conn).await {
        meili_connect_core::CheckStatus::Ok
    } else {
        meili_connect_core::CheckStatus::Warning
    };
    assert_eq!(gtid.status, want, "{gtid:?}");

    let mut bad = source(&url, "mc_check", &["nokey", "missing"], "check-sync");
    let report = bad.check().await.unwrap();
    assert!(!report.passed());
    let failed: Vec<_> = report
        .items
        .iter()
        .filter(|i| i.status == meili_connect_core::CheckStatus::Failed)
        .map(|i| i.name.as_str())
        .collect();
    assert_eq!(failed, vec!["mc_check.missing", "mc_check.nokey"]);

    let mut wrong_pw = config(&url, "mc_check");
    wrong_pw["password"] = json!("definitely-wrong");
    let mut src_bad = MysqlSource::new(
        "x",
        &SourceSpec {
            kind: SourceKind::Mysql,
            config: wrong_pw,
        },
        &[],
    )
    .unwrap();
    let report = src_bad.check().await.unwrap();
    assert!(!report.passed());
    assert!(
        !format!("{report:?}").contains("definitely-wrong"),
        "password leaked"
    );

    let found = src.discover().await.unwrap();
    let names: Vec<_> = found.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["mc_check.items", "mc_check.nokey", "mc_check.pairs"]
    );
    let pairs = &found[2];
    assert_eq!(pairs.key, vec!["b", "a"]);
    assert_eq!(pairs.fields.len(), 3);
    assert_eq!(pairs.fields[1].source_type, "varchar(5)");
    assert!(found[1].key.is_empty());
    assert_eq!(found[0].fields[0].source_type, "int unsigned");
    assert!(!found[0].fields[0].nullable);

    // Nothing was created on the origin.
    src.teardown().await.unwrap();
    src.teardown().await.unwrap();
}

#[tokio::test]
async fn fresh_snapshot_then_stream_is_gapless_and_resumable() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_gapless").await;
    conn.query_drop(
        "CREATE TABLE mc_gapless.items (id BIGINT PRIMARY KEY, n INT NOT NULL, label VARCHAR(30))",
    )
    .await
    .unwrap();
    conn.query_drop("CREATE TABLE mc_gapless.other (id INT PRIMARY KEY)")
        .await
        .unwrap();
    for i in 1..=200 {
        conn.exec_drop(
            "INSERT INTO mc_gapless.items VALUES (?, 0, ?)",
            (i, format!("row {i}")),
        )
        .await
        .unwrap();
    }

    let mut src = source(&url, "mc_gapless", &["items"], "gapless-sync");
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!("expected a fresh open");
    };
    let expected_mode = if gtid_on(&mut conn).await {
        "gtid"
    } else {
        "file"
    };
    assert_eq!(start.0["mode"], expected_mode);

    // Concurrent writer during the snapshot: inserts, updates, deletes,
    // a key change, writes to an unmapped table, then a sentinel row.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        let mut c = admin(&writer_url).await;
        for i in 201..=260_i64 {
            c.exec_drop("INSERT INTO mc_gapless.items VALUES (?, 1, 'new')", (i,))
                .await
                .unwrap();
            c.exec_drop(
                "UPDATE mc_gapless.items SET n = n + 1 WHERE id = ?",
                (i - 200,),
            )
            .await
            .unwrap();
            if i % 10 == 0 {
                c.exec_drop("DELETE FROM mc_gapless.items WHERE id = ?", (i - 150,))
                    .await
                    .unwrap();
                c.exec_drop("INSERT INTO mc_gapless.other VALUES (?)", (i,))
                    .await
                    .unwrap();
            }
        }
        // Multi-row transaction and a primary key change.
        let mut tx = c.start_transaction(Default::default()).await.unwrap();
        tx.query_drop("UPDATE mc_gapless.items SET id = 1000 WHERE id = 199")
            .await
            .unwrap();
        tx.query_drop("UPDATE mc_gapless.items SET label = NULL WHERE id BETWEEN 120 AND 125")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        c.query_drop("INSERT INTO mc_gapless.items VALUES (999999, 0, 'sentinel')")
            .await
            .unwrap();
    });

    let (tx, mut rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let collector = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(ev) = rx.recv().await {
            out.push(ev);
        }
        out
    });
    src.snapshot(tx, cancel.clone()).await.unwrap();
    let snapshot_events = collector.await.unwrap();
    writer.await.unwrap();
    assert!(
        snapshot_events
            .iter()
            .all(|e| matches!(e, SourceEvent::Change(c) if matches!(c.op, Op::Upsert { .. }))),
        "snapshot emits upserts only"
    );
    assert!(snapshot_events.len() >= 200);

    let mut model = BTreeMap::new();
    apply(&mut model, &snapshot_events);

    let mut run = Running::start(src);
    let mut seen_sentinel = false;
    let streamed = run
        .until(|ev| {
            if is_change_with(ev, 999_999) {
                seen_sentinel = true;
            }
            seen_sentinel && matches!(ev, SourceEvent::Checkpoint(_))
        })
        .await;
    run.stop().await;
    assert!(
        streamed.iter().all(|e| match e {
            SourceEvent::Change(c) => c.collection == "mc_gapless.items",
            _ => true,
        }),
        "unmapped tables are filtered"
    );
    // The key change is a delete of 199 then an upsert of 1000.
    let pos199 = streamed
        .iter()
        .position(|e| matches!(e, SourceEvent::Change(c) if c.key == vec![json!(199)] && c.op == Op::Delete))
        .expect("delete of the old key");
    assert!(is_change_with(&streamed[pos199 + 1], 1000));
    apply(&mut model, &streamed);

    // The model equals the table.
    let rows: Vec<(i64, i32, Option<String>)> = conn
        .query("SELECT id, n, label FROM mc_gapless.items ORDER BY id")
        .await
        .unwrap();
    let expected: BTreeMap<String, Value> = rows
        .into_iter()
        .map(|(id, n, label)| {
            (
                serde_json::to_string(&vec![json!(id)]).unwrap(),
                json!({"id": id, "n": n, "label": label}),
            )
        })
        .collect();
    assert_eq!(model, expected);

    // Resume from the last checkpoint: only later writes come back.
    let cp = last_checkpoint(&streamed).expect("a checkpoint");
    conn.query_drop("INSERT INTO mc_gapless.items VALUES (5000, 5, 'after')")
        .await
        .unwrap();
    conn.query_drop("DELETE FROM mc_gapless.items WHERE id = 1")
        .await
        .unwrap();
    let mut src = source(&url, "mc_gapless", &["items"], "gapless-sync");
    assert_eq!(src.open(Some(cp)).await.unwrap(), Opened::Resumed);
    let mut run = Running::start(src);
    let resumed = run
        .until(|ev| matches!(ev, SourceEvent::Change(c) if c.op == Op::Delete))
        .await;
    run.stop().await;
    let changes: Vec<_> = resumed
        .iter()
        .filter_map(|e| match e {
            SourceEvent::Change(c) => Some((c.key.clone(), matches!(c.op, Op::Delete))),
            _ => None,
        })
        .collect();
    assert_eq!(
        changes,
        vec![(vec![json!(5000)], false), (vec![json!(1)], true)],
        "nothing before the checkpoint is replayed"
    );
}

#[tokio::test]
async fn snapshot_and_binlog_decode_types_identically() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_types").await;
    conn.query_drop(
        "CREATE TABLE mc_types.t (
            id INT PRIMARY KEY,
            ti TINYINT, tu TINYINT UNSIGNED, mi MEDIUMINT UNSIGNED, bu BIGINT UNSIGNED,
            de DECIMAL(10,2), dbig DECIMAL(30,10), f FLOAT, d DOUBLE,
            b BIT(12), vc VARCHAR(20), lat VARCHAR(20) CHARACTER SET latin1,
            tx TEXT, bl BLOB, vb VARBINARY(8),
            j JSON, dt DATE, dtm DATETIME(6), ts TIMESTAMP(3) NULL, tm TIME, y YEAR,
            e ENUM('small','medium','large'), s SET('read','write','admin'),
            g POINT
        ) CHARACTER SET utf8mb4",
    )
    .await
    .unwrap();
    let values = "255, 16777215, 18446744073709551615, \
        12.50, 12345678901234567890.0123456789, 0.5, 1.25e300, \
        b'101010101010', 'héllo', 'café', 'long text', X'00FF10', X'0102', \
        '{\"a\": [1, 2.5, \"x\", null, true], \"n\": {\"k\": \"v\"}}', \
        '2024-02-29', '2024-01-02 03:04:05.000060', '2024-01-02 03:04:05.123', '-26:03:04', 2024, \
        'medium', 'read,admin', ST_GeomFromText('POINT(1 2)')";
    conn.query_drop(format!("INSERT INTO mc_types.t VALUES (1, -5, {values})"))
        .await
        .unwrap();

    let mut src = source(&url, "mc_types", &["t"], "types-sync");
    src.open(None).await.unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let Some(SourceEvent::Change(snap)) = rx.recv().await else {
        panic!("one snapshot row");
    };

    let mut run = Running::start(src);
    conn.query_drop(format!("INSERT INTO mc_types.t VALUES (2, -5, {values})"))
        .await
        .unwrap();
    let evs = run.until(|e| is_change_with(e, 2)).await;
    run.stop().await;
    let SourceEvent::Change(streamed) = evs.last().unwrap() else {
        unreachable!()
    };

    let (Op::Upsert { doc: a }, Op::Upsert { doc: b }) = (&snap.op, &streamed.op) else {
        panic!("upserts expected");
    };
    let mut a = a.clone();
    let mut b = b.clone();
    a.remove("id");
    b.remove("id");
    assert_eq!(a, b, "snapshot and binlog disagree");
    let expected = json!({
        "ti": -5, "tu": 255, "mi": 16_777_215, "bu": 18_446_744_073_709_551_615_u64,
        "de": 12.5, "dbig": "12345678901234567890.0123456789", "f": 0.5, "d": 1.25e300,
        "b": 2730, "vc": "héllo", "lat": "café", "tx": "long text", "bl": "AP8Q", "vb": "AQI=",
        "j": {"a": [1, 2.5, "x", null, true], "n": {"k": "v"}},
        "dt": "2024-02-29", "dtm": "2024-01-02T03:04:05.000060",
        "ts": "2024-01-02T03:04:05.123000Z", "tm": "-26:03:04", "y": 2024,
        "e": "medium", "s": "read,admin",
    });
    for (k, v) in expected.as_object().unwrap() {
        assert_eq!(a.get(k), Some(v), "column {k}");
    }
    assert!(
        a["g"].as_str().is_some_and(|s| !s.is_empty()),
        "geometry as base64"
    );
}

#[tokio::test]
async fn ddl_mid_stream_and_truncate() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_ddl").await;
    conn.query_drop("CREATE TABLE mc_ddl.items (id INT PRIMARY KEY, a VARCHAR(5))")
        .await
        .unwrap();

    let mut src = source(&url, "mc_ddl", &["items"], "ddl-sync");
    src.open(None).await.unwrap();
    let mut run = Running::start(src);
    conn.query_drop("INSERT INTO mc_ddl.items VALUES (1, 'x')")
        .await
        .unwrap();
    run.until(|e| is_change_with(e, 1)).await;

    conn.query_drop("ALTER TABLE mc_ddl.items ADD COLUMN extra INT DEFAULT 7")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO mc_ddl.items (id, a) VALUES (2, 'y')")
        .await
        .unwrap();
    let evs = run.until(|e| is_change_with(e, 2)).await;
    assert!(
        evs.iter().any(|e| matches!(e, SourceEvent::SchemaChanged { collection } if collection == "mc_ddl.items")),
        "{evs:?}"
    );
    let Some(SourceEvent::Change(c)) = evs.last() else {
        unreachable!()
    };
    assert_eq!(
        c.op,
        Op::Upsert {
            doc: json!({"id": 2, "a": "y", "extra": 7})
                .as_object()
                .unwrap()
                .clone()
        }
    );

    conn.query_drop("TRUNCATE TABLE mc_ddl.items")
        .await
        .unwrap();
    let evs = run
        .until(|e| matches!(e, SourceEvent::Truncate { .. }))
        .await;
    assert!(matches!(
        evs.last(),
        Some(SourceEvent::Truncate { collection }) if collection == "mc_ddl.items"
    ));
    run.stop().await;
}

#[tokio::test]
async fn purged_position_is_lost() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_purge").await;
    conn.query_drop("CREATE TABLE mc_purge.items (id INT PRIMARY KEY)")
        .await
        .unwrap();

    let mut src = source(&url, "mc_purge", &["items"], "purge-sync");
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!("fresh");
    };
    conn.query_drop("INSERT INTO mc_purge.items VALUES (1)")
        .await
        .unwrap();
    conn.query_drop("FLUSH BINARY LOGS").await.unwrap();
    let row: mysql_async::Row = conn
        .query_first("SHOW BINARY LOG STATUS")
        .await
        .unwrap()
        .unwrap();
    let file: String = row.get(0).unwrap();
    conn.query_drop(format!("PURGE BINARY LOGS TO '{file}'"))
        .await
        .unwrap();

    let mut src = source(&url, "mc_purge", &["items"], "purge-sync");
    let err = src.open(Some(start)).await.unwrap_err();
    assert!(matches!(err, SourceError::PositionLost(_)), "{err:?}");

    // A file checkpoint on a purged file is lost too.
    let gone = Checkpoint(json!({"mode": "file", "file": "binlog.000001", "pos": 4}));
    let err = src.open(Some(gone)).await.unwrap_err();
    assert!(matches!(err, SourceError::PositionLost(_)), "{err:?}");

    // And a checkpoint from another connector is not ours.
    let foreign = Checkpoint(json!({"lsn": "0/16B3748"}));
    assert!(matches!(
        src.open(Some(foreign)).await,
        Err(SourceError::PositionLost(_))
    ));
}

#[tokio::test]
async fn idle_checkpoints_and_cancellation() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_idle").await;
    conn.query_drop("CREATE TABLE mc_idle.items (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE mc_idle.other (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let mut src = source(&url, "mc_idle", &["items"], "idle-sync");
    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!("fresh");
    };
    let mut run = Running::start(src);
    // Only unmapped writes: the position still moves and is reported.
    conn.query_drop("INSERT INTO mc_idle.other VALUES (1)")
        .await
        .unwrap();
    let evs = run
        .until(|e| matches!(e, SourceEvent::Checkpoint(c) if *c != start))
        .await;
    assert!(evs.iter().all(|e| matches!(e, SourceEvent::Checkpoint(_))));
    run.stop().await;
}

#[tokio::test]
async fn composite_key_snapshot_pages() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_pages").await;
    conn.query_drop(
        "CREATE TABLE mc_pages.items (k VARCHAR(10), n INT, v INT, PRIMARY KEY (k, n))",
    )
    .await
    .unwrap();
    for i in 0..50 {
        conn.exec_drop(
            "INSERT INTO mc_pages.items VALUES (?, ?, ?)",
            (format!("k{}", i % 4), i, i * 2),
        )
        .await
        .unwrap();
    }
    let mut src = source(&url, "mc_pages", &["items"], "pages-sync");
    src.open(None).await.unwrap();
    let (tx, mut rx) = mpsc::channel(4);
    let snap = tokio::spawn(async move { src.snapshot(tx, CancellationToken::new()).await });
    let mut keys = Vec::new();
    while let Some(ev) = rx.recv().await {
        let SourceEvent::Change(c) = ev else {
            panic!("snapshot emits changes only");
        };
        keys.push(c.key);
    }
    snap.await.unwrap().unwrap();
    assert_eq!(keys.len(), 50);
    let mut sorted = keys.clone();
    sorted.sort_by_key(|k| (k[0].as_str().unwrap().to_owned(), k[1].as_i64().unwrap()));
    sorted.dedup();
    assert_eq!(sorted.len(), 50, "no duplicates across pages");
    assert_eq!(keys, sorted, "keyset order");
}

#[tokio::test]
async fn snapshot_honours_cancellation() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_cancel").await;
    conn.query_drop("CREATE TABLE mc_cancel.items (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO mc_cancel.items VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    let mut src = source(&url, "mc_cancel", &["items"], "cancel-sync");
    src.open(None).await.unwrap();
    // A channel nobody reads: the snapshot blocks on back-pressure.
    let (tx, _rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { src.snapshot(tx, c).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    let res = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("snapshot did not honour cancellation")
        .unwrap();
    assert!(res.is_ok(), "{res:?}");
}

/// Binlog images carry text in the column's charset; the snapshot reads it
/// as utf8mb4. Both must give the same document.
#[tokio::test]
async fn non_utf8_charsets_decode_like_the_snapshot() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    fresh_db(&mut conn, "mc_charset").await;
    conn.query_drop(
        "CREATE TABLE mc_charset.t (
            id INT PRIMARY KEY,
            lat VARCHAR(40) CHARACTER SET latin1,
            cyr VARCHAR(40) CHARACTER SET cp1251,
            gb VARCHAR(40) CHARACTER SET gbk,
            sj VARCHAR(40) CHARACTER SET cp932,
            u16 VARCHAR(40) CHARACTER SET utf16
        ) CHARACTER SET utf8mb4",
    )
    .await
    .unwrap();
    let values = "'€ “œ” Ÿ café', 'Привет, мир', '中文', 'あ①', 'é😀'";
    conn.query_drop("SET NAMES utf8mb4").await.unwrap();
    conn.query_drop(format!("INSERT INTO mc_charset.t VALUES (1, {values})"))
        .await
        .unwrap();

    let mut src = source(&url, "mc_charset", &["t"], "charset-sync");
    src.open(None).await.unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    src.snapshot(tx, CancellationToken::new()).await.unwrap();
    let Some(SourceEvent::Change(snap)) = rx.recv().await else {
        panic!("one snapshot row");
    };
    let mut run = Running::start(src);
    conn.query_drop(format!("INSERT INTO mc_charset.t VALUES (2, {values})"))
        .await
        .unwrap();
    let evs = run.until(|e| is_change_with(e, 2)).await;
    run.stop().await;
    let SourceEvent::Change(streamed) = evs.last().unwrap() else {
        unreachable!()
    };
    let (Op::Upsert { doc: a }, Op::Upsert { doc: b }) = (&snap.op, &streamed.op) else {
        panic!("upserts expected");
    };
    let mut a = a.clone();
    let mut b = b.clone();
    a.remove("id");
    b.remove("id");
    assert_eq!(a, b, "snapshot and binlog disagree");
    assert_eq!(a["lat"], json!("€ “œ” Ÿ café"));
    assert_eq!(a["cyr"], json!("Привет, мир"));
}

/// The GTID set of a checkpoint (GTID mode only).
fn gtid_set(cp: &Checkpoint) -> GtidSet {
    cp.0["gtid_set"].as_str().unwrap().parse().unwrap()
}

async fn gtid_executed(conn: &mut Conn) -> GtidSet {
    let s: String = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    s.replace(['\n', ' '], "").parse().unwrap()
}

/// MySQL 8.4 logs an XA transaction as two groups:
/// `GTID n, Query(XA START), Table_map, Rows, Query(XA END), XA_prepare`
/// then `GTID n+1, Query(XA COMMIT | XA ROLLBACK)`; `XA COMMIT … ONE PHASE`
/// is one group ending in an `XA_prepare` event with the one-phase flag.
/// XA on unmapped tables is skipped with its position checkpointed; XA on
/// a mapped table stops the stream with a configuration error before any
/// of its rows are emitted.
#[tokio::test]
async fn xa_transactions() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    if !gtid_on(&mut conn).await {
        eprintln!("gtid_mode is not ON: skipping");
        return;
    }
    // A prepared XA left by an interrupted run would hold its locks.
    let _ = conn.query_drop("XA ROLLBACK 'd'").await;
    fresh_db(&mut conn, "mc_xa").await;
    conn.query_drop("CREATE TABLE mc_xa.items (id INT PRIMARY KEY, v VARCHAR(10))")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE mc_xa.other (id INT PRIMARY KEY)")
        .await
        .unwrap();
    let mut src = source(&url, "mc_xa", &["items"], "xa-sync");
    src.open(None).await.unwrap();
    let mut run = Running::start(src);

    for sql in [
        "XA START 'a'; INSERT INTO mc_xa.other VALUES (1); XA END 'a'; XA PREPARE 'a'; XA COMMIT 'a'",
        "XA START 'b'; INSERT INTO mc_xa.other VALUES (2); XA END 'b'; XA PREPARE 'b'; XA ROLLBACK 'b'",
        "XA START 'c'; INSERT INTO mc_xa.other VALUES (3); XA END 'c'; XA COMMIT 'c' ONE PHASE",
        "INSERT INTO mc_xa.items VALUES (1, 'plain')",
    ] {
        conn.query_drop(sql).await.unwrap();
    }
    let executed = gtid_executed(&mut conn).await;
    let evs = run
        .until(|e| matches!(e, SourceEvent::Checkpoint(c) if gtid_set(c) == executed))
        .await;
    let changes: Vec<_> = evs
        .iter()
        .filter_map(|e| match e {
            SourceEvent::Change(c) => Some((c.collection.clone(), c.key.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        changes,
        vec![("mc_xa.items".to_owned(), vec![json!(1)])],
        "unmapped XA transactions are skipped"
    );

    // XA on a mapped table: refused before its rows are emitted, and its
    // GTID never reaches a checkpoint.
    let before = gtid_executed(&mut conn).await;
    conn.query_drop(
        "XA START 'd'; INSERT INTO mc_xa.items VALUES (2, 'xa'); XA END 'd'; XA PREPARE 'd'",
    )
    .await
    .unwrap();
    let rest = tokio::time::timeout(Duration::from_secs(30), async {
        let mut seen = Vec::new();
        while let Some(ev) = run.rx.recv().await {
            seen.push(ev);
        }
        seen
    })
    .await
    .expect("the stream did not stop");
    for ev in &rest {
        match ev {
            SourceEvent::Change(c) => panic!("XA row emitted: {c:?}"),
            SourceEvent::Checkpoint(c) => assert!(before.contains_set(&gtid_set(c))),
            _ => {}
        }
    }
    let err = run.task.await.unwrap().unwrap_err();
    assert!(
        matches!(&err, SourceError::Config(m) if m.contains("XA transactions on mapped tables are not supported")),
        "{err:?}"
    );
    conn.query_drop("XA ROLLBACK 'd'").await.unwrap();
}

/// MySQL 8.4 logs `CREATE TABLE … SELECT` as one group:
/// `GTID, Query(BEGIN), Query(CREATE TABLE … START TRANSACTION),
/// Table_map, Write_rows, Xid`. Its rows are emitted before any checkpoint
/// that covers its GTID.
#[tokio::test]
async fn create_table_select_rows_precede_its_checkpoint() {
    let Some(url) = url() else { return };
    let _g = SERIAL.lock().await;
    let mut conn = admin(&url).await;
    if !gtid_on(&mut conn).await {
        eprintln!("gtid_mode is not ON: skipping");
        return;
    }
    fresh_db(&mut conn, "mc_cts").await;
    conn.query_drop("CREATE TABLE mc_cts.src (id INT PRIMARY KEY, v VARCHAR(10))")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO mc_cts.src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await
        .unwrap();
    conn.query_drop("CREATE TABLE mc_cts.copy (id INT PRIMARY KEY, v VARCHAR(10))")
        .await
        .unwrap();
    let mut src = source(&url, "mc_cts", &["copy"], "cts-sync");
    src.open(None).await.unwrap();
    let mut run = Running::start(src);
    conn.query_drop("DROP TABLE mc_cts.copy").await.unwrap();
    conn.query_drop("CREATE TABLE mc_cts.copy (PRIMARY KEY (id)) SELECT id, v FROM mc_cts.src")
        .await
        .unwrap();
    let executed = gtid_executed(&mut conn).await;
    let evs = run
        .until(|e| matches!(e, SourceEvent::Checkpoint(c) if gtid_set(c) == executed))
        .await;
    run.stop().await;
    let last_row = evs
        .iter()
        .rposition(|e| matches!(e, SourceEvent::Change(_)))
        .expect("rows of CREATE … SELECT");
    let first_covering = evs
        .iter()
        .position(|e| matches!(e, SourceEvent::Checkpoint(c) if gtid_set(c) == executed))
        .unwrap();
    assert!(first_covering > last_row, "{evs:#?}");
    let keys: Vec<_> = evs
        .iter()
        .filter_map(|e| match e {
            SourceEvent::Change(c) if matches!(c.op, Op::Upsert { .. }) => Some(c.key.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(keys, vec![vec![json!(1)], vec![json!(2)], vec![json!(3)]]);
    assert!(
        evs.iter().any(
            |e| matches!(e, SourceEvent::Truncate { collection } if collection == "mc_cts.copy")
        )
    );
}
