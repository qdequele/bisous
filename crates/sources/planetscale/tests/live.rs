//! Against a live Vitess. Set `VITESS_TEST_CONFIG` to a `planetscale`
//! config JSON; skipped otherwise. A local `vttestserver` (two shards):
//!
//! ```text
//! docker run -d -p 33574-33580:33574-33580 -e PORT=33574 -e KEYSPACES=shop \
//!   -e NUM_SHARDS=2 -e MYSQL_BIND_HOST=0.0.0.0 vitess/vttestserver:mysql80
//! VITESS_TEST_CONFIG='{"host":"127.0.0.1","database":"shop","user":"root",
//!   "grpc_port":33575,"mysql_port":33577,"tls":false}'
//! ```
//!
//! The tables get a hash vindex, so their rows spread over the shards.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::time::Duration;

use meili_connect_core::{
    CheckStatus, Checkpoint, Document, Op, Opened, Source, SourceEvent, SourceKind, SourceSpec,
    StreamMapping, ack_channel, document_id,
};
use meili_connect_source_planetscale::PlanetScaleSourceFactory;
use mysql_async::prelude::Queryable;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn config() -> Option<Value> {
    match std::env::var("VITESS_TEST_CONFIG") {
        Ok(c) if !c.is_empty() => Some(serde_json::from_str(&c).expect("VITESS_TEST_CONFIG")),
        _ => {
            eprintln!("VITESS_TEST_CONFIG is not set: skipping live Vitess test");
            None
        }
    }
}

async fn admin(cfg: &Value) -> mysql_async::Conn {
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(cfg["host"].as_str().unwrap().to_owned())
        .tcp_port(cfg["mysql_port"].as_u64().unwrap_or(3306) as u16)
        .user(cfg["user"].as_str().map(str::to_owned))
        .pass(cfg["password"].as_str().map(str::to_owned))
        .db_name(cfg["database"].as_str().map(str::to_owned));
    mysql_async::Conn::new(opts)
        .await
        .expect("admin connection")
}

fn source(cfg: &Value, tables: &[&str], extra: Value) -> Box<dyn Source> {
    let mut config = cfg.clone();
    if let (Some(c), Some(e)) = (config.as_object_mut(), extra.as_object()) {
        c.extend(e.clone());
    }
    let db = cfg["database"].as_str().unwrap();
    let streams: Vec<StreamMapping> = tables
        .iter()
        .map(|t| {
            serde_json::from_value(json!({"collection": format!("{db}.{t}"), "index": t})).unwrap()
        })
        .collect();
    let spec = SourceSpec {
        kind: SourceKind::Planetscale,
        config,
    };
    use meili_connect_core::SourceFactory;
    PlanetScaleSourceFactory::new()
        .build("s", &spec, &streams)
        .expect("build")
}

#[derive(Default, Debug)]
struct Model {
    docs: HashMap<String, Document>,
    checkpoints: Vec<Checkpoint>,
}

impl Model {
    fn apply(&mut self, ev: SourceEvent) {
        match ev {
            SourceEvent::Change(c) => {
                let id = document_id(&c.key);
                match c.op {
                    Op::Upsert { doc } => {
                        self.docs.insert(id, doc);
                    }
                    Op::Delete => {
                        self.docs.remove(&id);
                    }
                    Op::Patch { .. } => panic!("no patches from VStream"),
                }
            }
            SourceEvent::Checkpoint(cp) => self.checkpoints.push(cp),
            SourceEvent::Skipped { reason, .. } => panic!("skipped: {reason}"),
            _ => {}
        }
    }
}

/// Streams until `done` holds, then stops the stream. Panics with what was
/// received when 120 s pass first.
async fn stream_until(
    src: &mut Box<dyn Source>,
    model: &mut Model,
    mut done: impl FnMut(&Model) -> bool,
    during: impl std::future::Future<Output = ()>,
) {
    let (tx, mut rx) = mpsc::channel(256);
    let (_ack_tx, ack_rx) = ack_channel();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let pump = async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        let mut reached = true;
        while !done(model) {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(ev)) => model.apply(ev),
                Ok(None) | Err(_) => {
                    reached = false;
                    break;
                }
            }
        }
        stop.cancel();
        reached
    };
    let (r, reached, ()) = tokio::join!(src.stream(tx, ack_rx, cancel), pump, during);
    r.unwrap();
    assert!(
        reached,
        "the expected changes did not arrive within 120 s: {} documents, {} checkpoints, ids {:?}",
        model.docs.len(),
        model.checkpoints.len(),
        {
            let mut ids: Vec<_> = model.docs.keys().cloned().collect();
            ids.sort();
            ids
        }
    );
}

#[tokio::test]
async fn check_snapshot_stream_and_resume_over_two_shards() {
    let Some(cfg) = config() else { return };
    let mut db = admin(&cfg).await;
    let table = format!("orders_{:08x}", rand::random::<u32>());
    db.query_drop(format!(
        "CREATE TABLE {table} (id bigint PRIMARY KEY, status enum('new','paid'), \
         total decimal(10,2), at datetime(3), doc json, note varchar(20))"
    ))
    .await
    .unwrap();
    db.query_drop(format!(
        "ALTER VSCHEMA ON {table} ADD VINDEX hash(id) USING hash"
    ))
    .await
    .unwrap();
    // Vitess needs the schema tracked before it routes the new table.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let values: Vec<String> = (1..=25)
        .map(|i| {
            format!("({i}, 'new', {i}.50, '2024-01-02 03:04:05.120', '{{\"n\": {i}}}', 'row {i}')")
        })
        .collect();
    db.query_drop(format!(
        "INSERT INTO {table} (id, status, total, at, doc, note) VALUES {}",
        values.join(", ")
    ))
    .await
    .unwrap();

    let small = json!({"snapshot_batch_size": 10});
    let mut src = source(&cfg, &[&table], small.clone());
    let report = src.check().await.unwrap();
    assert!(report.passed(), "{report:#?}");
    assert!(
        report.items.iter().any(|i| i.name == "vstream"
            && i.status == CheckStatus::Ok
            && i.message.contains("2 shards")),
        "{report:#?}"
    );
    let found = src.discover().await.unwrap();
    let mine = found.iter().find(|c| c.name.ends_with(&table)).unwrap();
    assert_eq!(mine.key, vec!["id"]);

    let Opened::Fresh { start } = src.open(None).await.unwrap() else {
        panic!("a first open is fresh")
    };
    // A write between the start position and the snapshot: in both.
    db.query_drop(format!(
        "INSERT INTO {table} (id, status) VALUES (100, 'paid')"
    ))
    .await
    .unwrap();

    let mut model = Model::default();
    let (tx, mut rx) = mpsc::channel(64);
    let (got, snap) = tokio::join!(
        async {
            let mut m = Model::default();
            while let Some(ev) = rx.recv().await {
                assert!(
                    !matches!(ev, SourceEvent::Checkpoint(_)),
                    "no checkpoint in a snapshot"
                );
                m.apply(ev);
            }
            m
        },
        src.snapshot(tx, CancellationToken::new())
    );
    snap.unwrap();
    assert_eq!(got.docs.len(), 26, "every row, over pages of 10");
    assert_eq!(
        Value::Object(got.docs["7"].clone()),
        json!({"id": 7, "status": "new", "total": 7.5, "at": "2024-01-02T03:04:05.120000",
               "doc": {"n": 7}, "note": "row 7"})
    );
    model.docs = got.docs;

    // Stream from the start: the overlap, then live changes on both shards.
    let t = table.clone();
    let writes = async {
        let mut db = admin(&cfg).await;
        db.query_drop(format!(
            "INSERT INTO {t} (id, status, note) VALUES (200, 'new', 'live')"
        ))
        .await
        .unwrap();
        db.query_drop(format!(
            "UPDATE {t} SET status = 'paid', note = 'upd' WHERE id IN (1, 2, 3, 4)"
        ))
        .await
        .unwrap();
        db.query_drop(format!("DELETE FROM {t} WHERE id IN (5, 6)"))
            .await
            .unwrap();
    };
    stream_until(
        &mut src,
        &mut model,
        |m| {
            m.docs.contains_key("200")
                && !m.docs.contains_key("5")
                && !m.docs.contains_key("6")
                && (1..=4).all(|i| {
                    m.docs.get(&i.to_string()).and_then(|d| d.get("note")) == Some(&json!("upd"))
                })
                && !m.checkpoints.is_empty()
        },
        writes,
    )
    .await;
    assert_eq!(model.docs.len(), 25, "26 + 1 inserted - 2 deleted");
    assert_eq!(model.docs["2"]["status"], json!("paid"));
    assert_eq!(model.docs["100"]["status"], json!("paid"));

    // Resume from the last checkpoint: only what came after.
    let last = model.checkpoints.last().unwrap().clone();
    assert_ne!(last, start);
    let mut src = source(&cfg, &[&table], small);
    assert_eq!(src.open(Some(last)).await.unwrap(), Opened::Resumed);
    let mut after = Model::default();
    let t = table.clone();
    let writes = async {
        let mut db = admin(&cfg).await;
        db.query_drop(format!("INSERT INTO {t} (id, note) VALUES (300, 'after')"))
            .await
            .unwrap();
    };
    stream_until(&mut src, &mut after, |m| m.docs.contains_key("300"), writes).await;
    assert!(after.docs.contains_key("300"));
    assert!(
        !after.docs.contains_key("200"),
        "nothing before the checkpoint again"
    );

    // A checkpoint of another database is refused.
    let mut src = source(&cfg, &[&table], json!({}));
    let alien = Checkpoint(
        json!({"shards": [{"keyspace": "elsewhere", "shard": "-", "gtid": "MySQL56/x:1"}]}),
    );
    assert!(src.open(Some(alien)).await.is_err());

    db.query_drop(format!("DROP TABLE {table}")).await.unwrap();
}

#[tokio::test]
async fn bad_credentials_and_missing_tables_are_reported() {
    let Some(cfg) = config() else { return };
    let mut src = source(&cfg, &["no_such_table"], json!({}));
    let report = src.check().await.unwrap();
    assert!(!report.passed());
    assert!(
        report
            .items
            .iter()
            .any(|i| i.name.ends_with("no_such_table") && i.status == CheckStatus::Failed)
    );
    assert!(src.open(None).await.is_err());
}
