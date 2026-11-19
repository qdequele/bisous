//! Against a real Meilisearch. Set `MEILI_TEST_URL` (and `MEILI_TEST_KEY`
//! when the instance has a master key); skipped otherwise.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use meili_connect_core::{DocOp, Document, IndexTarget, MeiliDestination, Secret, Sink, SinkError};
use meili_connect_sink::{MeiliSink, SinkOptions};
use serde_json::{Value, json};

struct Live {
    url: String,
    key: String,
}

fn live() -> Option<Live> {
    match std::env::var("MEILI_TEST_URL") {
        Ok(url) if !url.is_empty() => Some(Live {
            url,
            key: std::env::var("MEILI_TEST_KEY").unwrap_or_default(),
        }),
        _ => {
            eprintln!("MEILI_TEST_URL is unset: skipping live Meilisearch test");
            None
        }
    }
}

fn sink(l: &Live, opts: SinkOptions) -> MeiliSink {
    let dest = MeiliDestination {
        url: l.url.clone(),
        api_key: Secret::new(l.key.clone()),
    };
    MeiliSink::new(&dest, opts).unwrap()
}

fn opts() -> SinkOptions {
    SinkOptions {
        poll_interval: Duration::from_millis(10),
        task_timeout: Duration::from_secs(60),
        ..SinkOptions::default()
    }
}

fn uid(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("mc_sink_{prefix}_{nanos}")
}

fn doc(v: Value) -> Document {
    v.as_object().unwrap().clone()
}

fn replace(id: &str, v: Value) -> DocOp {
    DocOp::Replace {
        id: id.into(),
        doc: doc(v),
    }
}

async fn get_doc(l: &Live, index: &str, id: &str) -> Option<Value> {
    let resp = reqwest::Client::new()
        .get(format!("{}/indexes/{index}/documents/{id}", l.url))
        .bearer_auth(&l.key)
        .send()
        .await
        .unwrap();
    if resp.status() == 404 {
        return None;
    }
    Some(resp.json().await.unwrap())
}

async fn wait_task(l: &Live, uid: u64) {
    for _ in 0..600 {
        let t: Value = reqwest::Client::new()
            .get(format!("{}/tasks/{uid}", l.url))
            .bearer_auth(&l.key)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match t["status"].as_str() {
            Some("succeeded") => return,
            Some("failed" | "canceled") => panic!("task failed: {t}"),
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("task {uid} did not finish");
}

#[tokio::test]
async fn apply_lifecycle() {
    let Some(l) = live() else { return };
    for gzip in [true, false] {
        let s = sink(&l, SinkOptions { gzip, ..opts() });
        let target = IndexTarget {
            uid: uid("apply"),
            primary_key: "pk".into(),
        };
        s.ensure_index(&target).await.unwrap();
        s.ensure_index(&target).await.unwrap();
        let other = IndexTarget {
            primary_key: "other".into(),
            ..target.clone()
        };
        assert!(matches!(
            s.ensure_index(&other).await,
            Err(SinkError::Config(_))
        ));

        let mut ops: Vec<DocOp> = (0..50)
            .map(|i| replace(&i.to_string(), json!({"title": format!("t{i}"), "n": i})))
            .collect();
        ops.push(replace("a b", json!({"title": "bad id"})));
        ops.push(replace("vec", json!({"_vectors": "not an object"})));
        ops.push(DocOp::Delete { id: "3".into() });
        ops.push(DocOp::Merge {
            id: "4".into(),
            doc: doc(json!({"title": "merged"})),
            unset: vec!["n".into()],
        });
        ops.push(DocOp::Delete { id: "5".into() });
        ops.push(replace("5", json!({"title": "back"})));
        let report = s.apply(&target, ops).await.unwrap();
        assert_eq!(report.written, 52, "{report:?}");
        assert_eq!(report.deleted, 2);
        let mut rejected: Vec<&str> = report.rejected.iter().map(|r| r.op.id()).collect();
        rejected.sort();
        assert_eq!(rejected, vec!["a b", "vec"]);
        assert!(report.rejected.iter().all(|r| !r.reason.is_empty()));

        assert_eq!(s.document_count(&target.uid).await.unwrap(), Some(49));
        assert_eq!(get_doc(&l, &target.uid, "3").await, None);
        assert_eq!(
            get_doc(&l, &target.uid, "4").await.unwrap(),
            json!({"pk": "4", "title": "merged", "n": null})
        );
        assert_eq!(
            get_doc(&l, &target.uid, "5").await.unwrap(),
            json!({"pk": "5", "title": "back"})
        );
        s.delete_index(&target.uid).await.unwrap();
        assert_eq!(s.document_count(&target.uid).await.unwrap(), None);
    }
}

#[tokio::test]
async fn splits_and_rejects_oversized() {
    let Some(l) = live() else { return };
    let s = sink(
        &l,
        SinkOptions {
            max_payload_bytes: 4096,
            ..opts()
        },
    );
    let target = IndexTarget {
        uid: uid("split"),
        primary_key: "id".into(),
    };
    s.ensure_index(&target).await.unwrap();
    let mut ops: Vec<DocOp> = (0..200)
        .map(|i| replace(&format!("d{i}"), json!({"body": "lorem ipsum ".repeat(10)})))
        .collect();
    ops.push(replace("huge", json!({"body": "x".repeat(10_000)})));
    ops.extend((0..100).map(|i| DocOp::Delete {
        id: format!("d{i}"),
    }));
    let report = s.apply(&target, ops).await.unwrap();
    assert_eq!(report.written, 200);
    assert_eq!(report.deleted, 100);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(s.document_count(&target.uid).await.unwrap(), Some(100));
    s.delete_index(&target.uid).await.unwrap();
}

#[tokio::test]
async fn index_operations() {
    let Some(l) = live() else { return };
    let s = sink(&l, opts());
    let live_ix = IndexTarget {
        uid: uid("live"),
        primary_key: "id".into(),
    };
    let shadow = IndexTarget {
        uid: format!("{}__connect_shadow", live_ix.uid),
        primary_key: "id".into(),
    };
    s.ensure_index(&live_ix).await.unwrap();
    s.apply(&live_ix, vec![replace("old", json!({"v": 1}))])
        .await
        .unwrap();
    let settings_url = format!("{}/indexes/{}/settings", l.url, live_ix.uid);
    let http = reqwest::Client::new();
    let resp = http
        .patch(&settings_url)
        .bearer_auth(&l.key)
        .json(&json!({"searchableAttributes": ["v"], "filterableAttributes": ["v"]}))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let task: Value = resp.json().await.unwrap();
    wait_task(&l, task["taskUid"].as_u64().unwrap()).await;

    // Shadow: settings copied first (this creates it without a primary
    // key), then ensure_index sets the key.
    s.copy_settings(&live_ix.uid, &shadow.uid).await.unwrap();
    s.ensure_index(&shadow).await.unwrap();
    let copied: Value = http
        .get(format!("{}/indexes/{}/settings", l.url, shadow.uid))
        .bearer_auth(&l.key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(copied["filterableAttributes"], json!(["v"]));
    assert_eq!(copied["searchableAttributes"], json!(["v"]));

    s.apply(
        &shadow,
        vec![
            replace("new1", json!({"v": 2})),
            replace("new2", json!({"v": 3})),
        ],
    )
    .await
    .unwrap();
    s.swap(&live_ix.uid, &shadow.uid).await.unwrap();
    assert_eq!(s.document_count(&live_ix.uid).await.unwrap(), Some(2));
    assert_eq!(s.document_count(&shadow.uid).await.unwrap(), Some(1));
    s.delete_index(&shadow.uid).await.unwrap();

    // Missing indexes.
    let missing = uid("missing");
    assert_eq!(s.document_count(&missing).await.unwrap(), None);
    s.delete_index(&missing).await.unwrap();
    s.copy_settings(&missing, &uid("never")).await.unwrap();
    assert!(matches!(
        s.swap(&live_ix.uid, &missing).await,
        Err(SinkError::Config(_))
    ));
    // Deleting documents from a missing index is a no-op.
    let r = s
        .apply(
            &IndexTarget {
                uid: missing.clone(),
                primary_key: "id".into(),
            },
            vec![DocOp::Delete { id: "x".into() }],
        )
        .await
        .unwrap();
    assert_eq!(r.deleted, 1);
    s.delete_index(&missing).await.unwrap();
    s.delete_index(&live_ix.uid).await.unwrap();
}

#[tokio::test]
async fn delete_all_documents_keeps_settings() {
    let Some(l) = live() else { return };
    let s = sink(&l, opts());
    let target = IndexTarget {
        uid: uid("clear"),
        primary_key: "id".into(),
    };
    s.ensure_index(&target).await.unwrap();
    let resp = reqwest::Client::new()
        .patch(format!("{}/indexes/{}/settings", l.url, target.uid))
        .bearer_auth(&l.key)
        .json(&json!({"filterableAttributes": ["v"]}))
        .send()
        .await
        .unwrap();
    let task: Value = resp.json().await.unwrap();
    wait_task(&l, task["taskUid"].as_u64().unwrap()).await;
    s.apply(
        &target,
        (0..10)
            .map(|i| replace(&i.to_string(), json!({"v": i})))
            .collect(),
    )
    .await
    .unwrap();
    assert_eq!(s.document_count(&target.uid).await.unwrap(), Some(10));

    s.delete_all_documents(&target.uid).await.unwrap();
    assert_eq!(s.document_count(&target.uid).await.unwrap(), Some(0));
    let settings: Value = reqwest::Client::new()
        .get(format!("{}/indexes/{}/settings", l.url, target.uid))
        .bearer_auth(&l.key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(settings["filterableAttributes"], json!(["v"]));

    s.delete_all_documents(&uid("missing")).await.unwrap();
    s.delete_index(&target.uid).await.unwrap();
}

#[tokio::test]
async fn wrong_key_is_config() {
    let Some(l) = live() else { return };
    if l.key.is_empty() {
        eprintln!("MEILI_TEST_KEY is unset: skipping auth test");
        return;
    }
    let s = sink(
        &Live {
            url: l.url.clone(),
            key: "definitely-wrong".into(),
        },
        opts(),
    );
    let e = s.document_count("anything").await.unwrap_err();
    assert!(matches!(e, SinkError::Config(_)), "{e:?}");
    assert!(!e.to_string().contains("definitely-wrong"));
}
