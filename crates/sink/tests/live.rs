//! Against a real Meilisearch. Set `MEILI_TEST_URL` (and `MEILI_TEST_KEY`
//! when the instance has a master key); skipped otherwise. The test where
//! Meilisearch calls an embedding server back also needs
//! `MEILI_TEST_CALLBACK_HOST` (see `callback_host`).

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
        embedder_keys: Default::default(),
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

/// An index the sink creates starts with the indexing defaults, whatever
/// the version: a Meilisearch that does not know `wordPosition` still gets
/// `proximityPrecision`.
#[tokio::test]
async fn a_created_index_has_the_indexing_defaults() {
    let Some(l) = live() else { return };
    let s = sink(&l, opts());
    let target = IndexTarget {
        uid: uid("defaults"),
        primary_key: "pk".into(),
    };
    s.ensure_index(&target).await.unwrap();

    let settings = get_settings(&l, &target.uid).await;
    assert_eq!(
        settings["proximityPrecision"],
        json!("byAttribute"),
        "{settings}"
    );
    if let Some(word_position) = settings.get("wordPosition") {
        assert_eq!(word_position["enabled"], json!(false), "{settings}");
    }
    // Created through its settings, then given its primary key.
    s.ensure_index(&target).await.unwrap();
    s.apply(&target, vec![replace("1", json!({"title": "t"}))])
        .await
        .unwrap();
    assert!(get_doc(&l, &target.uid, "1").await.is_some());

    // Settings somebody chose are never put back to the defaults.
    let http = reqwest::Client::new();
    let patched: Value = http
        .patch(format!("{}/indexes/{}/settings", l.url, target.uid))
        .bearer_auth(&l.key)
        .json(&json!({"proximityPrecision": "byWord"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(patched["taskUid"].is_u64(), "{patched}");
    s.delete_all_documents(&target.uid).await.unwrap(); // waits behind the settings task
    s.ensure_index(&target).await.unwrap();
    let settings = get_settings(&l, &target.uid).await;
    assert_eq!(
        settings["proximityPrecision"],
        json!("byWord"),
        "{settings}"
    );

    s.delete_index(&target.uid).await.unwrap();
}

/// The engine's order for a rebuild: the shadow is created (with the
/// defaults), then given the live index's settings. Nothing of the defaults
/// may survive the copy, or a resync would change the settings of an index
/// somebody set up on purpose.
#[tokio::test]
async fn a_shadow_takes_the_live_settings_over_the_defaults() {
    let Some(l) = live() else { return };
    let s = sink(&l, opts());
    // A live index somebody created with Meilisearch's own defaults, not
    // through the sink.
    let live_uid = uid("their_live");
    let http = reqwest::Client::new();
    let created: Value = http
        .post(format!("{}/indexes", l.url))
        .bearer_auth(&l.key)
        .json(&json!({"uid": live_uid, "primaryKey": "pk"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(created["taskUid"].is_u64(), "{created}");
    let live_ix = IndexTarget {
        uid: live_uid.clone(),
        primary_key: "pk".into(),
    };
    s.ensure_index(&live_ix).await.unwrap(); // waits for the creation
    let theirs = get_settings(&l, &live_uid).await;
    assert_eq!(theirs["proximityPrecision"], json!("byWord"), "{theirs}");

    let shadow = IndexTarget {
        uid: uid("shadow"),
        primary_key: "pk".into(),
    };
    s.ensure_index(&shadow).await.unwrap();
    s.copy_settings(&live_uid, &shadow.uid).await.unwrap();
    assert_eq!(get_settings(&l, &shadow.uid).await, theirs);

    s.delete_index(&shadow.uid).await.unwrap();
    s.delete_index(&live_uid).await.unwrap();
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

async fn patch_settings(l: &Live, index: &str, settings: Value) {
    let resp = reqwest::Client::new()
        .patch(format!("{}/indexes/{index}/settings", l.url))
        .bearer_auth(&l.key)
        .json(&settings)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());
    let task: Value = resp.json().await.unwrap();
    wait_task(l, task["taskUid"].as_u64().unwrap()).await;
}

/// Documents Meilisearch refuses for good are rejected, including those it
/// reports as `internal` or `vector_embedding_error`; good ones are written.
#[tokio::test]
async fn poison_vectors_are_rejected() {
    let Some(l) = live() else { return };
    let s = sink(
        &l,
        SinkOptions {
            suspect_backoff: Duration::from_millis(10),
            ..opts()
        },
    );
    let target = IndexTarget {
        uid: uid("vectors"),
        primary_key: "id".into(),
    };
    s.ensure_index(&target).await.unwrap();
    patch_settings(
        &l,
        &target.uid,
        json!({"embedders": {"up": {"source": "userProvided", "dimensions": 3}}}),
    )
    .await;

    // Alone in its run: `internal` from Meilisearch.
    let report = s
        .apply(&target, vec![replace("alone", json!({"_vectors": "x"}))])
        .await
        .unwrap();
    assert_eq!(report.rejected.len(), 1, "{report:?}");

    let mut deep = json!(1);
    for _ in 0..200 {
        deep = json!([deep]);
    }
    let good = |i: u32| replace(&format!("g{i}"), json!({"_vectors": {"up": [1, 2, 3]}}));
    let ops = vec![
        good(0),
        replace("str", json!({"_vectors": "not an object"})),
        good(1),
        replace("arr", json!({"_vectors": [1, 2, 3]})),
        replace("dims", json!({"_vectors": {"up": [1, 2]}})),
        good(2),
        replace("type", json!({"_vectors": {"up": "x"}})),
        replace("none", json!({"title": "no vectors"})),
        replace("deep", json!({"_vectors": {"up": [1, 2, 3]}, "x": deep})),
        good(3),
    ];
    let report = s.apply(&target, ops).await.unwrap();
    let mut rejected: Vec<&str> = report.rejected.iter().map(|r| r.op.id()).collect();
    rejected.sort();
    assert_eq!(
        rejected,
        vec!["arr", "deep", "dims", "none", "str", "type"],
        "{report:?}"
    );
    assert_eq!(report.written, 4);
    assert_eq!(s.document_count(&target.uid).await.unwrap(), Some(4));
    s.delete_index(&target.uid).await.unwrap();
}

async fn get_settings(l: &Live, index: &str) -> Value {
    reqwest::Client::new()
        .get(format!("{}/indexes/{index}/settings", l.url))
        .bearer_auth(&l.key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Meilisearch returns embedder API keys redacted (`"sk-XXXXX..."`): such
/// settings are refused, keyless ones are copied verbatim onto an index
/// that does not exist yet.
#[tokio::test]
async fn copy_settings_with_embedders() {
    let Some(l) = live() else { return };
    let s = sink(&l, opts());
    let rest = |key: Option<&str>| {
        let mut e = json!({
            "source": "rest",
            "url": "http://127.0.0.1:9/embed",
            "dimensions": 3,
            "request": {"input": "{{text}}"},
            "response": {"embedding": "{{embedding}}"},
            "headers": {"X-Custom": "v"},
        });
        if let Some(k) = key {
            e["apiKey"] = json!(k);
        }
        e
    };

    let keyed = uid("keyed");
    patch_settings(
        &l,
        &keyed,
        json!({"embedders": {"remote": rest(Some("secret-api-key-123456"))}}),
    )
    .await;
    let raw = get_settings(&l, &keyed).await;
    let shown = raw["embedders"]["remote"]["apiKey"].as_str().unwrap();
    assert_ne!(shown, "secret-api-key-123456");
    assert!(shown.contains("XXX"), "{shown}");
    let target = uid("keyed_copy");
    let e = s.copy_settings(&keyed, &target).await.unwrap_err();
    assert!(
        matches!(&e, SinkError::Config(m) if m.contains("`remote`")),
        "{e:?}"
    );
    assert_eq!(s.document_count(&target).await.unwrap(), None);

    let keyless = uid("keyless");
    patch_settings(
        &l,
        &keyless,
        json!({
            "filterableAttributes": ["a", {"attributePatterns": ["b*"],
                "features": {"facetSearch": true, "filter": {"equality": true, "comparison": true}}}],
            "localizedAttributes": [{"attributePatterns": ["t"], "locales": ["fra"]}],
            "embedders": {
                "up": {"source": "userProvided", "dimensions": 3, "binaryQuantized": true},
                "remote": rest(None),
            },
        }),
    )
    .await;
    let copy = uid("keyless_copy");
    s.copy_settings(&keyless, &copy).await.unwrap();
    assert_eq!(
        get_settings(&l, &copy).await,
        get_settings(&l, &keyless).await
    );

    for ix in [keyed, keyless, copy] {
        s.delete_index(&ix).await.unwrap();
    }
}

/// The name under which Meilisearch reaches this test process (for
/// example `host.docker.internal`, or `127.0.0.1` on the same host): set
/// `MEILI_TEST_CALLBACK_HOST` to run the tests where Meilisearch calls an
/// embedding server back. Meilisearch refuses embedders at internal
/// addresses unless started with `MEILI_EXPERIMENTAL_ALLOWED_IP_NETWORKS`
/// (for example `0.0.0.0/0`): without it the embedding never ends.
fn callback_host() -> Option<String> {
    let host = std::env::var("MEILI_TEST_CALLBACK_HOST")
        .ok()
        .filter(|h| !h.is_empty());
    if host.is_none() {
        eprintln!("MEILI_TEST_CALLBACK_HOST is unset: skipping live embedder test");
    }
    host
}

/// An index whose embedder has a key is rebuilt aside when the destination
/// names that key: the shadow gets the real key, not the redacted one
/// Meilisearch returns, so it embeds.
#[tokio::test]
async fn a_shadow_embeds_with_the_named_embedder_key() {
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let Some(l) = live() else { return };
    let Some(host) = callback_host() else { return };
    const REAL: &str = "embedder-key-0123456789abcdef";
    // An embedding server that only knows the real key.
    let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = MockServer::builder().listener(listener).start().await;
    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .and(header("authorization", format!("Bearer {REAL}").as_str()))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"embedding": [0.1, 0.2, 0.3]})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"error": "bad key"})))
        .mount(&server)
        .await;
    let embedder_url = format!("http://{host}:{port}/v1/embeddings");

    let live_ix = uid("embedder_live");
    patch_settings(
        &l,
        &live_ix,
        json!({"embedders": {"remote": {
            "source": "rest",
            "url": embedder_url,
            "apiKey": REAL,
            "dimensions": 3,
            "request": {"input": "{{text}}"},
            "response": {"embedding": "{{embedding}}"},
            "documentTemplate": "{{doc.title}}",
        }}}),
    )
    .await;
    let shown = get_settings(&l, &live_ix).await["embedders"]["remote"]["apiKey"].clone();
    assert_ne!(shown, REAL, "Meilisearch redacts the key");

    let dest = MeiliDestination {
        url: l.url.clone(),
        api_key: Secret::new(l.key.clone()),
        embedder_keys: [(embedder_url.clone(), Secret::new(REAL))].into(),
    };
    let s = MeiliSink::new(&dest, opts()).unwrap();
    let shadow = IndexTarget {
        uid: uid("embedder_shadow"),
        primary_key: "id".into(),
    };
    s.copy_settings(&live_ix, &shadow.uid).await.unwrap();
    // Redacted again on the way out, like the live one.
    assert_eq!(
        get_settings(&l, &shadow.uid).await,
        get_settings(&l, &live_ix).await
    );

    // Nothing was embedded yet (`dimensions` spares the probe): the shadow
    // embeds this document, with the key it was given.
    assert!(server.received_requests().await.unwrap().is_empty());
    s.ensure_index(&shadow).await.unwrap();
    let r = s
        .apply(
            &shadow,
            vec![replace("1", json!({"id": "1", "title": "Alien"}))],
        )
        .await
        .unwrap();
    assert_eq!((r.written, r.rejected.len()), (1, 0), "{:?}", r.rejected);
    let calls = server.received_requests().await.unwrap();
    assert!(!calls.is_empty());
    for call in &calls {
        assert_eq!(
            call.headers.get("authorization").unwrap(),
            &format!("Bearer {REAL}")
        );
    }

    // The same index without the key named is still refused.
    let e = sink(&l, opts())
        .copy_settings(&live_ix, &uid("embedder_refused"))
        .await
        .unwrap_err();
    assert!(
        matches!(&e, SinkError::Config(m) if m.contains("`remote`")),
        "{e:?}"
    );

    for ix in [live_ix, shadow.uid] {
        s.delete_index(&ix).await.unwrap();
    }
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
