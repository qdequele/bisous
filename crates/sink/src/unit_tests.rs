#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Read as _;
use std::sync::{Arc, Mutex};

use meili_connect_core::{DocOp, IndexTarget, MeiliDestination, Secret, Sink, SinkError};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::*;
use crate::client::{MeiliError, classify_doc_failure};

const KEY: &str = "super-secret-key";

fn opts() -> SinkOptions {
    SinkOptions {
        poll_interval: Duration::from_millis(1),
        max_poll_interval: Duration::from_millis(5),
        task_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(5),
        gzip: false,
        suspect_backoff: Duration::from_millis(1),
        request_backoff: Duration::from_millis(1),
        ..SinkOptions::default()
    }
}

fn sink(server: &MockServer, opts: SinkOptions) -> MeiliSink {
    let dest = MeiliDestination {
        url: server.uri(),
        api_key: Secret::new(KEY),
    };
    MeiliSink::new(&dest, opts).unwrap()
}

fn target() -> IndexTarget {
    IndexTarget {
        uid: "movies".into(),
        primary_key: "id".into(),
    }
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

fn delete(id: &str) -> DocOp {
    DocOp::Delete { id: id.into() }
}

fn err(status: u16, code: &str, kind: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(json!({
        "message": format!("message for {code}"), "code": code, "type": kind, "link": ""
    }))
}

/// What the fake recorded for one accepted request.
#[derive(Debug, Clone, PartialEq)]
struct Call {
    method: String,
    route: &'static str,
    /// Documents (NDJSON) or ids (delete-batch).
    items: Vec<Value>,
    succeeded: bool,
}

#[derive(Default)]
struct FakeState {
    tasks: Vec<Value>,
    calls: Vec<Call>,
    /// Body limit, like `http_payload_size_limit`.
    payload_limit: Option<usize>,
    /// Polls that answer `processing` before the terminal state.
    processing_polls: usize,
    polls: usize,
    gzip_seen: bool,
    /// Times each document id was sent.
    attempts: std::collections::HashMap<String, usize>,
}

/// A small in-memory Meilisearch. A document makes its task fail with:
/// `"bad": true` → `invalid_document_id`; `"embed": "fail"` →
/// `vector_embedding_error`; `"flaky": n` → `vector_embedding_error` the
/// first `n` times it is sent; `"internal": true` or a non-object
/// `_vectors` → `internal`; `"needs_vectors": true` without `_vectors.up`
/// → `vector_embedding_error`.
#[derive(Clone, Default)]
struct Fake(Arc<Mutex<FakeState>>);

impl Fake {
    fn calls(&self) -> Vec<Call> {
        self.0.lock().unwrap().calls.clone()
    }

    fn succeeded_ids(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter(|c| c.succeeded)
            .flat_map(|c| c.items)
            .map(|v| match v {
                Value::String(s) => s,
                Value::Object(o) => o["id"].as_str().unwrap().to_owned(),
                other => other.to_string(),
            })
            .collect()
    }

    async fn mount(&self, server: &MockServer) {
        Mock::given(path_regex(r"^/indexes/[^/]+/documents(/delete-batch)?$"))
            .respond_with(self.clone())
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/tasks/\d+$"))
            .respond_with(self.clone())
            .mount(server)
            .await;
    }
}

impl Respond for Fake {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut st = self.0.lock().unwrap();
        if req.url.path().starts_with("/tasks/") {
            let uid: usize = req.url.path()[7..].parse().unwrap();
            st.polls += 1;
            if st.polls <= st.processing_polls {
                return ResponseTemplate::new(200)
                    .set_body_json(json!({"uid": uid, "status": "processing"}));
            }
            return ResponseTemplate::new(200).set_body_json(st.tasks[uid].clone());
        }
        let mut body = req.body.clone();
        if req
            .headers
            .get("content-encoding")
            .is_some_and(|v| v == "gzip")
        {
            st.gzip_seen = true;
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(&body[..])
                .read_to_end(&mut out)
                .unwrap();
            body = out;
        }
        if st.payload_limit.is_some_and(|l| body.len() > l) {
            return err(413, "payload_too_large", "invalid_request");
        }
        let (route, items): (&'static str, Vec<Value>) = if req.url.path().ends_with("delete-batch")
        {
            ("delete-batch", serde_json::from_slice(&body).unwrap())
        } else {
            assert_eq!(
                req.url
                    .query_pairs()
                    .find(|(k, _)| k == "primaryKey")
                    .unwrap()
                    .1,
                "id"
            );
            let docs = body
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_slice(l).unwrap())
                .collect();
            ("documents", docs)
        };
        let mut failures = Vec::new();
        for d in &items {
            let id = d.get("id").map(Value::to_string).unwrap_or_default();
            let attempt = {
                let n = st.attempts.entry(id).or_default();
                *n += 1;
                *n
            };
            let flaky = d.get("flaky").and_then(Value::as_u64).unwrap_or(0);
            let vectors = d.get("_vectors");
            if d.get("bad") == Some(&json!(true)) {
                failures.push((
                    "invalid_document_id",
                    "invalid_request",
                    format!("bad document {}", d["id"]),
                ));
            } else if d.get("embed") == Some(&json!("fail")) || attempt as u64 <= flaky {
                failures.push((
                    "vector_embedding_error",
                    "invalid_request",
                    "embedder down".to_owned(),
                ));
            } else if d.get("internal") == Some(&json!(true))
                || vectors.is_some_and(|v| !v.is_object() && !v.is_null())
            {
                failures.push(("internal", "internal", "invalid type".to_owned()));
            } else if d.get("needs_vectors") == Some(&json!(true))
                && vectors.and_then(|v| v.get("up")).is_none()
            {
                failures.push((
                    "vector_embedding_error",
                    "invalid_request",
                    "no vectors provided".to_owned(),
                ));
            }
        }
        let failure = failures.into_iter().next();
        let uid = st.tasks.len();
        let task = match &failure {
            None => json!({"uid": uid, "status": "succeeded"}),
            Some((code, kind, msg)) => json!({"uid": uid, "status": "failed",
                "error": {"message": msg, "code": code, "type": kind, "link": ""}}),
        };
        st.tasks.push(task);
        st.calls.push(Call {
            method: req.method.to_string(),
            route,
            items,
            succeeded: failure.is_none(),
        });
        ResponseTemplate::new(202).set_body_json(json!({"taskUid": uid, "status": "enqueued"}))
    }
}

#[test]
fn runs_group_consecutive_kinds() {
    let ops = vec![
        replace("1", json!({})),
        replace("2", json!({})),
        delete("3"),
        DocOp::Merge {
            id: "4".into(),
            doc: doc(json!({})),
            unset: vec![],
        },
        replace("5", json!({})),
    ];
    let r = runs(&ops);
    assert_eq!(
        r,
        vec![
            (Kind::Replace, 0..2),
            (Kind::Delete, 2..3),
            (Kind::Merge, 3..4),
            (Kind::Replace, 4..5)
        ]
    );
    assert!(runs(&[]).is_empty());
}

#[test]
fn doc_line_injects_id_and_nulls() {
    let op = DocOp::Merge {
        id: "7".into(),
        doc: doc(json!({"id": "wrong", "title": "x", "gone": 1})),
        unset: vec!["old".into(), "old".into(), "gone".into(), "id".into()],
    };
    let line: Value = serde_json::from_slice(&encode_op(&op, "id").unwrap()).unwrap();
    assert_eq!(
        line,
        json!({"id": "7", "title": "x", "gone": 1, "old": null})
    );
    let raw = String::from_utf8(encode_op(&op, "id").unwrap()).unwrap();
    assert_eq!(raw.matches("\"old\"").count(), 1);
    assert_eq!(encode_op(&delete("a"), "id").unwrap(), b"\"a\"");
}

#[test]
fn classification() {
    let e = |code: &str, kind: &str| MeiliError {
        message: "m".into(),
        code: code.into(),
        kind: kind.into(),
    };
    assert!(matches!(
        classify_doc_failure(&e("invalid_document_id", "invalid_request")),
        DocFailure::PerDocument(_)
    ));
    assert!(matches!(
        classify_doc_failure(&e("invalid_vector_dimensions", "invalid_request")),
        DocFailure::PerDocument(_)
    ));
    assert!(matches!(
        classify_doc_failure(&e("vector_embedding_error", "invalid_request")),
        DocFailure::Suspect(_)
    ));
    for code in [
        "malformed_payload",
        "invalid_vectors_type",
        "max_fields_limit_exceeded",
        "missing_document_id",
    ] {
        assert!(
            matches!(
                classify_doc_failure(&e(code, "invalid_request")),
                DocFailure::PerDocument(_)
            ),
            "{code}"
        );
    }
    assert!(matches!(
        classify_doc_failure(&e(
            "index_primary_key_multiple_candidates_found",
            "invalid_request"
        )),
        DocFailure::Global(SinkError::Config(_))
    ));
    assert!(matches!(
        classify_doc_failure(&e("internal", "internal")),
        DocFailure::Suspect(_)
    ));
    assert!(matches!(
        classify_doc_failure(&e("no_space_left_on_device", "system")),
        DocFailure::Global(SinkError::Transient(_))
    ));
}

#[tokio::test]
async fn apply_preserves_order_across_kinds() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let ops = vec![
        replace("1", json!({"t": 1})),
        replace("2", json!({"t": 2})),
        delete("3"),
        DocOp::Merge {
            id: "4".into(),
            doc: doc(json!({"t": 4})),
            unset: vec!["x".into()],
        },
        DocOp::Merge {
            id: "5".into(),
            doc: doc(json!({})),
            unset: vec![],
        },
        delete("1"),
        replace("1", json!({"t": 9})),
    ];
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 5);
    assert_eq!(report.deleted, 2);
    assert!(report.rejected.is_empty());
    let calls = fake.calls();
    let shape: Vec<(&str, &str, usize)> = calls
        .iter()
        .map(|c| (c.method.as_str(), c.route, c.items.len()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("POST", "documents", 2),
            ("POST", "delete-batch", 1),
            ("PUT", "documents", 2),
            ("POST", "delete-batch", 1),
            ("POST", "documents", 1),
        ]
    );
    assert_eq!(calls[0].items[0], json!({"id": "1", "t": 1}));
    // Every accepted document's wire form plus one separator, once.
    let wire: u64 = calls
        .iter()
        .flat_map(|c| c.items.iter())
        .map(|v| v.to_string().len() as u64 + 1)
        .sum();
    assert_eq!(report.bytes, wire);
    assert_eq!(calls[1].items, vec![json!("3")]);
    assert_eq!(calls[2].items[0], json!({"id": "4", "t": 4, "x": null}));
    assert_eq!(calls[4].items[0], json!({"id": "1", "t": 9}));
}

#[tokio::test]
async fn sends_ndjson_with_bearer_key() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/indexes/movies/documents"))
        .and(header("authorization", format!("Bearer {KEY}").as_str()))
        .and(header("content-type", "application/x-ndjson"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"taskUid": 0})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/0"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"uid": 0, "status": "succeeded"})),
        )
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    let report = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap();
    assert_eq!(report.written, 1);
    let dbg = format!("{s:?}");
    assert!(!dbg.contains(KEY), "{dbg}");
}

#[tokio::test]
async fn gzip_bodies_are_compressed() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(
        &server,
        SinkOptions {
            gzip: true,
            ..opts()
        },
    );
    let ops = (0..100)
        .map(|i| replace(&i.to_string(), json!({"text": "x".repeat(2000)})))
        .chain([delete("a")])
        .collect();
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!((report.written, report.deleted), (100, 1));
    assert!(fake.0.lock().unwrap().gzip_seen);
    assert_eq!(fake.calls()[1].items, vec![json!("a")]);
}

#[tokio::test]
async fn splits_runs_over_the_payload_limit() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let limit = 300;
    let s = sink(
        &server,
        SinkOptions {
            max_payload_bytes: limit,
            ..opts()
        },
    );
    let mut ops: Vec<DocOp> = (0..20)
        .map(|i| replace(&i.to_string(), json!({"text": "abcdefghij".repeat(4)})))
        .collect();
    ops.insert(5, replace("huge", json!({"text": "z".repeat(1000)})));
    let deletes: Vec<DocOp> = (0..100).map(|i| delete(&format!("d{i}"))).collect();
    ops.extend(deletes);
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 20);
    assert_eq!(report.deleted, 100);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].op.id(), "huge");
    assert!(report.rejected[0].reason.contains("payload limit"));
    let calls = fake.calls();
    assert!(
        calls.len() > 4,
        "expected several requests, got {}",
        calls.len()
    );
    for c in &calls {
        let size: usize = c.items.iter().map(|v| v.to_string().len() + 1).sum();
        assert!(size <= limit, "request of {size} bytes");
    }
    let ids = fake.succeeded_ids();
    let expected: Vec<String> = (0..20)
        .map(|i| i.to_string())
        .chain((0..100).map(|i| format!("d{i}")))
        .collect();
    assert_eq!(ids, expected, "order is preserved across chunks");
}

#[tokio::test]
async fn bisection_isolates_bad_documents() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let ops: Vec<DocOp> = (0..16)
        .map(|i| {
            let bad = i == 3 || i == 12;
            replace(&i.to_string(), json!({"bad": bad}))
        })
        .collect();
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 14);
    let rejected: Vec<&str> = report.rejected.iter().map(|r| r.op.id()).collect();
    assert_eq!(rejected, vec!["3", "12"]);
    // Only the accepted documents' bytes count, each once although the
    // bisection sent most of them several times.
    let line = |i: usize| json!({"id": i.to_string(), "bad": false}).to_string().len() as u64 + 1;
    let expected: u64 = (0..16).filter(|i| *i != 3 && *i != 12).map(line).sum();
    assert_eq!(report.bytes, expected);
    assert_eq!(report.rejected[0].reason, "bad document \"3\"");
    let ids = fake.succeeded_ids();
    let expected: Vec<String> = (0..16)
        .filter(|i| *i != 3 && *i != 12)
        .map(|i| i.to_string())
        .collect();
    assert_eq!(ids, expected);
    // 1 + 2 + 4 + 4 + 4 requests at most for two bad documents among 16.
    assert!(fake.calls().len() <= 15);
}

#[tokio::test]
async fn payload_too_large_from_server_is_bisected() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.0.lock().unwrap().payload_limit = Some(250);
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let mut ops: Vec<DocOp> = (0..10)
        .map(|i| replace(&i.to_string(), json!({"t": "0123456789".repeat(4)})))
        .collect();
    ops.push(replace("big", json!({"t": "z".repeat(400)})));
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 10);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].op.id(), "big");
    assert!(report.rejected[0].reason.contains("payload_too_large"));
}

async fn mount_embedders(server: &MockServer, embedders: Value) {
    Mock::given(method("GET"))
        .and(path("/indexes/movies/settings/embedders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(embedders))
        .mount(server)
        .await;
}

#[tokio::test]
async fn embedder_outage_is_transient() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    mount_embedders(&server, json!({})).await;
    let s = sink(&server, opts());
    let all_fail: Vec<DocOp> = (0..4)
        .map(|i| replace(&i.to_string(), json!({"embed": "fail"})))
        .collect();
    let e = s.apply(&target(), all_fail).await.unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
}

#[tokio::test]
async fn a_document_that_keeps_failing_with_an_embedder_error_is_never_rejected() {
    // Others got through, but nothing proves this one is at fault: the
    // embedder may be failing on it only. Rejecting it would dead-letter a
    // good document and advance the checkpoint past it.
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    mount_embedders(&server, json!({"remote": {"source": "rest"}})).await;
    let s = sink(&server, opts());
    let one_fails: Vec<DocOp> = (0..4)
        .map(|i| {
            let v = if i == 2 { "fail" } else { "ok" };
            replace(&i.to_string(), json!({"embed": v}))
        })
        .collect();
    let e = s.apply(&target(), one_fails).await.unwrap_err();
    assert!(
        matches!(&e, SinkError::Transient(m) if m.contains("`2`")),
        "{e:?}"
    );
    // Sent three times by bisection (4 → 2 → 1), then `suspect_retries`
    // more times.
    assert_eq!(fake.0.lock().unwrap().attempts["\"2\""], 3 + 3);
}

#[tokio::test]
async fn intermittent_embedder_failures_are_retried_not_rejected() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    // Doc 5 fails the first 4 times it is sent: in the full batch, its
    // half, its quarter, then once isolated; it succeeds on a retry.
    let ops: Vec<DocOp> = (0..8)
        .map(|i| {
            let flaky = if i == 5 { 4 } else { 0 };
            replace(&i.to_string(), json!({"flaky": flaky}))
        })
        .collect();
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 8);
    assert!(report.rejected.is_empty(), "{report:?}");
    let mut ids = fake.succeeded_ids();
    ids.sort();
    assert_eq!(ids, ["0", "1", "2", "3", "4", "5", "6", "7"]);
}

#[tokio::test]
async fn internal_error_on_a_lone_document_is_transient() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let e = s
        .apply(&target(), vec![replace("1", json!({"internal": true}))])
        .await
        .unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
    assert_eq!(fake.calls().len(), 4);
}

#[tokio::test]
async fn malformed_vectors_are_rejected_even_alone() {
    // Meilisearch reports a non-object `_vectors` as `internal`; the
    // document itself shows it can never succeed.
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let report = s
        .apply(
            &target(),
            vec![replace("v", json!({"_vectors": [1.0, 2.0]}))],
        )
        .await
        .unwrap();
    assert_eq!(report.written, 0);
    assert_eq!(report.rejected.len(), 1);
    assert!(
        report.rejected[0]
            .reason
            .contains("`_vectors` must be an object")
    );
    // No pointless retries.
    assert_eq!(fake.calls().len(), 1);

    let ops = vec![
        replace("a", json!({})),
        replace("v", json!({"_vectors": "x"})),
        replace("b", json!({})),
    ];
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 2);
    assert_eq!(report.rejected[0].op.id(), "v");
}

#[tokio::test]
async fn missing_user_provided_vectors_are_rejected() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    mount_embedders(
        &server,
        json!({"up": {"source": "userProvided", "dimensions": 3}}),
    )
    .await;
    let s = sink(&server, opts());
    let ops = vec![
        replace(
            "ok",
            json!({"needs_vectors": true, "_vectors": {"up": [1, 2, 3]}}),
        ),
        replace("none", json!({"needs_vectors": true})),
    ];
    let report = s.apply(&target(), ops).await.unwrap();
    assert_eq!(report.written, 1);
    assert_eq!(report.rejected.len(), 1);
    assert_eq!(report.rejected[0].op.id(), "none");
    assert!(report.rejected[0].reason.contains("`_vectors.up`"));
}

#[test]
fn shape_problems() {
    assert_eq!(
        shape_problem(&replace("1", json!({"_vectors": null}))),
        None
    );
    assert_eq!(
        shape_problem(&replace("1", json!({"_vectors": {"a": [1]}}))),
        None
    );
    assert!(shape_problem(&replace("1", json!({"_vectors": 3}))).is_some());
    assert_eq!(shape_problem(&delete("1")), None);
    let nested = |levels: usize| {
        let mut v = json!(1);
        for _ in 0..levels {
            v = json!([v]);
        }
        replace("1", json!({ "x": v }))
    };
    // The document object plus 126 arrays is the deepest Meilisearch takes.
    assert_eq!(shape_problem(&nested(126)), None);
    assert!(shape_problem(&nested(127)).is_some());
}

#[test]
fn jitter_stays_in_range() {
    for _ in 0..100 {
        let d = client::jitter(Duration::from_millis(100));
        assert!(d >= Duration::from_millis(50) && d <= Duration::from_millis(100));
    }
    assert_eq!(client::jitter(Duration::ZERO), Duration::ZERO);
}

#[tokio::test]
async fn dropping_calls_at_any_point_leaves_the_sink_usable() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    mount_embedders(&server, json!({})).await;
    let s = sink(
        &server,
        SinkOptions {
            gzip: true,
            suspect_backoff: Duration::from_millis(5),
            ..opts()
        },
    );
    // Big enough to gzip on the blocking pool, with a document that needs
    // bisection and resends: the drops land in every await point.
    let batch = || -> Vec<DocOp> {
        let mut ops: Vec<DocOp> = (0..20)
            .map(|i| replace(&i.to_string(), json!({"t": "x".repeat(8 * 1024)})))
            .collect();
        ops.push(replace("flaky", json!({"flaky": 6})));
        ops.push(delete("3"));
        ops
    };
    for micros in [0, 50, 200, 500, 1_000, 2_000, 5_000, 10_000] {
        let _ =
            tokio::time::timeout(Duration::from_micros(micros), s.apply(&target(), batch())).await;
    }
    // A call that never finishes (task stuck processing) is dropped too.
    fake.0.lock().unwrap().processing_polls = usize::MAX;
    let dropped =
        tokio::time::timeout(Duration::from_millis(30), s.apply(&target(), batch())).await;
    assert!(dropped.is_err());
    {
        let mut st = fake.0.lock().unwrap();
        st.processing_polls = 0;
        st.attempts.clear();
    }
    let report = s.apply(&target(), batch()).await.unwrap();
    assert_eq!(report.written, 21);
    assert_eq!(report.deleted, 1);
    assert!(report.rejected.is_empty());
}

#[tokio::test]
async fn polls_until_terminal() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.0.lock().unwrap().processing_polls = 3;
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let report = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap();
    assert_eq!(report.written, 1);
    assert_eq!(fake.0.lock().unwrap().polls, 4);
}

#[tokio::test]
async fn task_timeout_is_transient() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.0.lock().unwrap().processing_polls = usize::MAX;
    fake.mount(&server).await;
    let s = sink(
        &server,
        SinkOptions {
            task_timeout: Duration::from_millis(50),
            ..opts()
        },
    );
    let e = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap_err();
    assert!(
        matches!(&e, SinkError::Transient(m) if m.contains("not finished")),
        "{e:?}"
    );
}

#[tokio::test]
async fn transient_poll_errors_are_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"taskUid": 0})))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/0"))
        .respond_with(err(503, "", ""))
        .up_to_n_times(2)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/0"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"uid": 0, "status": "succeeded"})),
        )
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    let report = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap();
    assert_eq!(report.written, 1);
}

async fn apply_with_enqueue_reply(reply: ResponseTemplate) -> SinkError {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(reply)
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    s.apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap_err()
}

#[tokio::test]
async fn http_errors_map_to_reactions() {
    let cases = [
        (err(401, "missing_authorization_header", "auth"), "config"),
        (err(403, "invalid_api_key", "auth"), "config"),
        (err(408, "", ""), "transient"),
        (err(429, "too_many_requests", "system"), "transient"),
        (err(500, "internal", "internal"), "transient"),
        (
            ResponseTemplate::new(502).set_body_string("<html>bad gateway</html>"),
            "transient",
        ),
        (err(400, "malformed_payload", "invalid_request"), "fatal"),
        (
            err(400, "index_primary_key_already_exists", "invalid_request"),
            "config",
        ),
        (err(404, "something_new", "invalid_request"), "fatal"),
    ];
    for (reply, want) in cases {
        let e = apply_with_enqueue_reply(reply).await;
        let got = match e {
            SinkError::Transient(_) => "transient",
            SinkError::Config(_) => "config",
            SinkError::Fatal(_) => "fatal",
        };
        assert_eq!(got, want, "{e:?}");
        assert!(!e.to_string().contains(KEY));
    }
}

#[tokio::test]
async fn write_blips_are_retried() {
    for blip in [
        err(503, "", ""),
        err(500, "internal", "internal"),
        err(408, "", ""),
        ResponseTemplate::new(502).set_body_string("<html>bad gateway</html>"),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(blip)
            .up_to_n_times(3)
            .with_priority(1)
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(task_ok(0))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/tasks/0"))
            .respond_with(task_state(0, "succeeded", None))
            .mount(&server)
            .await;
        let report = sink(&server, opts())
            .apply(&target(), vec![replace("1", json!({}))])
            .await
            .unwrap();
        assert_eq!(report.written, 1);
    }
}

#[tokio::test]
async fn write_retries_are_bounded() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(err(503, "", ""))
        .expect(1 + 2)
        .mount(&server)
        .await;
    let s = sink(
        &server,
        SinkOptions {
            request_retries: 2,
            ..opts()
        },
    );
    let e = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
}

#[tokio::test]
async fn retry_after_is_honoured_and_capped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(err(429, "too_many_requests", "system").insert_header("Retry-After", "1"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(task_ok(0))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/0"))
        .respond_with(task_state(0, "succeeded", None))
        .mount(&server)
        .await;
    let started = std::time::Instant::now();
    sink(&server, opts())
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(900));

    // A huge Retry-After is capped by max_request_backoff.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(err(429, "too_many_requests", "system").insert_header("Retry-After", "3600"))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(task_ok(0))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/0"))
        .respond_with(task_state(0, "succeeded", None))
        .mount(&server)
        .await;
    let started = std::time::Instant::now();
    sink(
        &server,
        SinkOptions {
            max_request_backoff: Duration::from_millis(20),
            ..opts()
        },
    )
    .apply(&target(), vec![replace("1", json!({}))])
    .await
    .unwrap();
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn client_errors_and_swaps_are_not_resent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/indexes/movies/documents"))
        .respond_with(err(400, "malformed_payload", "invalid_request"))
        .expect(1)
        .mount(&server)
        .await;
    // The swap may have been enqueued before the 502: resending it could
    // swap the indexes back.
    Mock::given(method("POST"))
        .and(path("/swap-indexes"))
        .respond_with(err(502, "", ""))
        .expect(1)
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    let e = s
        .apply(&target(), vec![replace("1", json!({}))])
        .await
        .unwrap_err();
    assert!(matches!(e, SinkError::Fatal(_)), "{e:?}");
    let e = s.swap("a", "b").await.unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
}

#[tokio::test]
async fn failed_task_errors_map_to_reactions() {
    let cases = [
        ("internal", "internal", "transient"),
        (
            "index_primary_key_multiple_candidates_found",
            "invalid_request",
            "config",
        ),
        ("invalid_api_key", "auth", "config"),
        ("weird", "unknown_kind", "fatal"),
    ];
    for (code, kind, want) in cases {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({"taskUid": 0})))
            .mount(&server)
            .await;
        Mock::given(path("/tasks/0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "uid": 0, "status": "failed",
                "error": {"message": "m", "code": code, "type": kind, "link": ""}
            })))
            .mount(&server)
            .await;
        let s = sink(&server, opts());
        let e = s
            .apply(&target(), vec![replace("1", json!({}))])
            .await
            .unwrap_err();
        let got = match e {
            SinkError::Transient(_) => "transient",
            SinkError::Config(_) => "config",
            SinkError::Fatal(_) => "fatal",
        };
        assert_eq!(got, want, "{code}: {e:?}");
    }
}

#[tokio::test]
async fn connection_refused_is_transient() {
    // A port nothing listens on (wiremock pools its servers, so a dropped
    // MockServer may still be listening).
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let dest = MeiliDestination {
        url: format!("http://127.0.0.1:{port}"),
        api_key: Secret::new(KEY),
    };
    let s = MeiliSink::new(&dest, opts()).unwrap();
    let e = s.document_count("x").await.unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
    assert!(!e.to_string().contains(KEY));
}

fn policed(url: String, policy: HostPolicy) -> Result<MeiliSink, SinkError> {
    let dest = MeiliDestination {
        url,
        api_key: Secret::new(KEY),
    };
    MeiliSink::new(
        &dest,
        SinkOptions {
            host_policy: Arc::new(policy),
            request_retries: 0,
            ..opts()
        },
    )
}

#[tokio::test]
async fn a_restrictive_policy_refuses_internal_destinations() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"numberOfDocuments": 1})))
        .mount(&server)
        .await;
    let port = server.address().port();
    // A literal address is refused when the sink is built.
    for url in [
        format!("http://127.0.0.1:{port}"),
        "http://169.254.169.254".to_owned(),
        "http://[::1]:7700".to_owned(),
        "http://10.0.0.1:7700".to_owned(),
    ] {
        let e = policed(url.clone(), HostPolicy::public()).unwrap_err();
        assert!(matches!(e, SinkError::Config(_)), "{url}: {e:?}");
    }
    // A name is resolved through the policy at connect time: `localhost`
    // never reaches the server.
    let s = policed(format!("http://localhost:{port}"), HostPolicy::public()).unwrap();
    let e = s.document_count("x").await.unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
    assert!(server.received_requests().await.unwrap().is_empty());
    // The same sink under `any` gets through.
    let s = policed(format!("http://localhost:{port}"), HostPolicy::any()).unwrap();
    assert_eq!(s.document_count("x").await.unwrap(), Some(1));
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let elsewhere = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"numberOfDocuments": 1})))
        .mount(&elsewhere)
        .await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/indexes/x/stats", elsewhere.uri())),
        )
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    assert!(s.document_count("x").await.is_err());
    assert!(elsewhere.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_body_that_is_not_a_meilisearch_error_is_never_echoed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(403)
                .set_body_string("<html>instance-id: i-0123 ami-SECRETBODY</html>"),
        )
        .mount(&server)
        .await;
    let e = sink(&server, opts()).document_count("x").await.unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("403"), "{msg}");
    assert!(
        !msg.contains("SECRETBODY") && !msg.contains("html"),
        "{msg}"
    );
}

#[test]
fn bad_urls_are_config_errors() {
    for url in ["not a url", "ftp://host", "mailto:x@y"] {
        let dest = MeiliDestination {
            url: url.into(),
            api_key: Secret::new(KEY),
        };
        assert!(matches!(
            MeiliSink::new(&dest, opts()),
            Err(SinkError::Config(_))
        ));
    }
}

fn task_ok(uid: u64) -> ResponseTemplate {
    ResponseTemplate::new(202).set_body_json(json!({"taskUid": uid}))
}

fn task_state(uid: u64, status: &str, code: Option<&str>) -> ResponseTemplate {
    let mut t = json!({"uid": uid, "status": status});
    if let Some(code) = code {
        t["error"] =
            json!({"message": format!("m {code}"), "code": code, "type": "invalid_request"});
    }
    ResponseTemplate::new(200).set_body_json(t)
}

#[tokio::test]
async fn ensure_index_cases() {
    // Exists with the same primary key: nothing to do.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"uid": "movies", "primaryKey": "id"})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(task_ok(0))
        .expect(0)
        .mount(&server)
        .await;
    // Its settings are its owner's: never touched.
    Mock::given(method("PATCH"))
        .respond_with(task_ok(0))
        .expect(0)
        .mount(&server)
        .await;
    sink(&server, opts()).ensure_index(&target()).await.unwrap();

    // Exists with another primary key: Config.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"uid": "movies", "primaryKey": "sku"})),
        )
        .mount(&server)
        .await;
    let e = sink(&server, opts())
        .ensure_index(&target())
        .await
        .unwrap_err();
    assert!(matches!(e, SinkError::Config(_)), "{e:?}");

    // Exists without a primary key: set it.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"uid": "movies", "primaryKey": null})),
        )
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies"))
        .and(wiremock::matchers::body_json(json!({"primaryKey": "id"})))
        .respond_with(task_ok(1))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/1"))
        .respond_with(task_state(1, "succeeded", None))
        .mount(&server)
        .await;
    sink(&server, opts()).ensure_index(&target()).await.unwrap();
}

// ------------------------------------------------- creating an index

const DEFAULTS: &str =
    r#"{"proximityPrecision": "byAttribute", "wordPosition": {"enabled": false}}"#;

fn defaults() -> Value {
    serde_json::from_str(DEFAULTS).unwrap()
}

/// What a Meilisearch that does not know `field` answers to a settings
/// update carrying it.
fn unknown_field(field: &str) -> ResponseTemplate {
    ResponseTemplate::new(400).set_body_json(json!({
        "message": format!("Unknown field `{field}`: expected one of `displayedAttributes`, `rankingRules`"),
        "code": "bad_request", "type": "invalid_request", "link": ""
    }))
}

async fn mount_missing(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .mount(server)
        .await;
}

/// `GET /indexes/movies`: missing at first, then there with `primary_key`
/// (`null`: as a settings update leaves an index it created).
async fn mount_missing_then(server: &MockServer, primary_key: Value) {
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .up_to_n_times(1)
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"uid": "movies", "primaryKey": primary_key})),
        )
        .mount(server)
        .await;
}

/// `GET /tasks?indexUids=movies&statuses=enqueued,processing`: the uids of
/// the tasks still queued on the index.
async fn mount_pending(server: &MockServer, uids: &[u64]) {
    let results: Vec<Value> = uids.iter().map(|uid| json!({"uid": uid})).collect();
    Mock::given(method("GET"))
        .and(path("/tasks"))
        .and(wiremock::matchers::query_param("indexUids", "movies"))
        .and(wiremock::matchers::query_param(
            "statuses",
            "enqueued,processing",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results": results})))
        .mount(server)
        .await;
}

async fn mount_task(server: &MockServer, uid: u64) {
    Mock::given(path(format!("/tasks/{uid}")))
        .respond_with(task_state(uid, "succeeded", None))
        .mount(server)
        .await;
}

async fn mount_primary_key(server: &MockServer, uid: u64) {
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies"))
        .and(wiremock::matchers::body_json(json!({"primaryKey": "id"})))
        .respond_with(task_ok(uid))
        .expect(1)
        .mount(server)
        .await;
    mount_task(server, uid).await;
}

/// No request may create the index bare, or touch its settings.
async fn forbid(server: &MockServer, verb: &str, route: &str) {
    Mock::given(method(verb))
        .and(path(route))
        .respond_with(task_ok(99))
        .expect(0)
        .mount(server)
        .await;
}

/// An index the sink creates starts with the settings that make indexing
/// lighter, set in the request that creates it: it never exists without
/// them.
#[tokio::test]
async fn a_created_index_starts_with_the_indexing_defaults() {
    let server = MockServer::start().await;
    mount_missing_then(&server, Value::Null).await;
    mount_pending(&server, &[]).await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies/settings"))
        .and(wiremock::matchers::body_json(defaults()))
        .respond_with(task_ok(4))
        .expect(1)
        .mount(&server)
        .await;
    mount_task(&server, 4).await;
    mount_primary_key(&server, 5).await;
    forbid(&server, "POST", "/indexes").await;

    sink(&server, opts()).ensure_index(&target()).await.unwrap();
}

/// A Meilisearch from before a setting existed refuses the whole request
/// (400 `bad_request` naming the field): the newest settings are left out,
/// one at a time, and the index is still created.
#[tokio::test]
async fn a_setting_this_meilisearch_does_not_know_is_left_out() {
    let server = MockServer::start().await;
    mount_missing_then(&server, Value::Null).await;
    mount_pending(&server, &[]).await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies/settings"))
        .and(wiremock::matchers::body_json(defaults()))
        .respond_with(unknown_field("wordPosition"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies/settings"))
        .and(wiremock::matchers::body_json(
            json!({"proximityPrecision": "byAttribute"}),
        ))
        .respond_with(task_ok(4))
        .expect(1)
        .mount(&server)
        .await;
    mount_task(&server, 4).await;
    mount_primary_key(&server, 5).await;

    sink(&server, opts()).ensure_index(&target()).await.unwrap();
}

/// None of them known: created the way it always was, with its primary key.
#[tokio::test]
async fn an_index_is_created_bare_when_no_default_is_known() {
    let server = MockServer::start().await;
    mount_missing(&server).await;
    mount_pending(&server, &[]).await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/movies/settings"))
        .respond_with(unknown_field("proximityPrecision"))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/indexes"))
        .and(wiremock::matchers::body_json(
            json!({"uid": "movies", "primaryKey": "id"}),
        ))
        .respond_with(task_ok(4))
        .expect(1)
        .mount(&server)
        .await;
    mount_task(&server, 4).await;

    sink(&server, opts()).ensure_index(&target()).await.unwrap();
}

/// Only "unknown setting" is tolerated. A refused key, a rejected value, a
/// `bad_request` that names none of the settings (a body damaged in
/// transit), an outage: each stops the creation like any other request,
/// and nothing is created bare behind its back.
#[tokio::test]
async fn any_other_refusal_of_the_defaults_stops_the_creation() {
    let cases = [
        (err(403, "invalid_api_key", "auth"), "config"),
        (
            err(
                400,
                "invalid_settings_proximity_precision",
                "invalid_request",
            ),
            "fatal",
        ),
        (err(400, "bad_request", "invalid_request"), "fatal"),
        (err(503, "internal", "internal"), "transient"),
    ];
    for (reply, expected) in cases {
        let server = MockServer::start().await;
        mount_missing(&server).await;
        mount_pending(&server, &[]).await;
        Mock::given(method("PATCH"))
            .and(path("/indexes/movies/settings"))
            .respond_with(reply)
            .mount(&server)
            .await;
        forbid(&server, "POST", "/indexes").await;

        let e = sink(&server, opts())
            .ensure_index(&target())
            .await
            .unwrap_err();
        let class = match e {
            SinkError::Config(_) => "config",
            SinkError::Fatal(_) => "fatal",
            SinkError::Transient(_) => "transient",
        };
        assert_eq!(class, expected, "{e:?}");
    }
}

/// The settings update is accepted, then its task fails or is cancelled:
/// an error, and no bare index either.
#[tokio::test]
async fn a_failed_or_cancelled_creation_task_is_an_error() {
    let cases = [
        // What a key without `indexes.create` gets from a settings update.
        (task_state(4, "failed", Some("index_not_found")), "config"),
        (task_state(4, "canceled", None), "transient"),
    ];
    for (state, expected) in cases {
        let server = MockServer::start().await;
        mount_missing(&server).await;
        mount_pending(&server, &[]).await;
        Mock::given(method("PATCH"))
            .and(path("/indexes/movies/settings"))
            .respond_with(task_ok(4))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(path("/tasks/4"))
            .respond_with(state)
            .mount(&server)
            .await;
        forbid(&server, "POST", "/indexes").await;

        let e = sink(&server, opts())
            .ensure_index(&target())
            .await
            .unwrap_err();
        let class = match e {
            SinkError::Config(_) => "config",
            SinkError::Fatal(_) => "fatal",
            SinkError::Transient(_) => "transient",
        };
        assert_eq!(class, expected, "{e:?}");
    }
}

/// Somebody is creating the index: their task is still queued, so the index
/// does not show yet. It is waited for, and their index, with their
/// settings, is left as it is.
#[tokio::test]
async fn an_index_being_created_by_somebody_else_is_waited_for_not_overwritten() {
    let server = MockServer::start().await;
    mount_missing_then(&server, json!("id")).await;
    mount_pending(&server, &[7]).await;
    mount_task(&server, 7).await;
    forbid(&server, "PATCH", "/indexes/movies/settings").await;
    forbid(&server, "PATCH", "/indexes/movies").await;
    forbid(&server, "POST", "/indexes").await;

    sink(&server, opts()).ensure_index(&target()).await.unwrap();
}

/// Work that never stops arriving on the uid is not waited for forever: the
/// engine retries.
#[tokio::test]
async fn a_creation_that_does_not_settle_is_transient() {
    let server = MockServer::start().await;
    mount_missing(&server).await;
    mount_pending(&server, &[7]).await;
    mount_task(&server, 7).await;
    forbid(&server, "PATCH", "/indexes/movies/settings").await;

    let e = sink(&server, opts())
        .ensure_index(&target())
        .await
        .unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");
}

#[tokio::test]
async fn swap_missing_index_is_config() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/swap-indexes"))
        .and(wiremock::matchers::body_json(
            json!([{"indexes": ["a", "b"]}]),
        ))
        .respond_with(task_ok(2))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/2"))
        .respond_with(task_state(2, "failed", Some("index_not_found")))
        .mount(&server)
        .await;
    let e = sink(&server, opts()).swap("a", "b").await.unwrap_err();
    assert!(matches!(e, SinkError::Config(_)), "{e:?}");
}

#[tokio::test]
async fn delete_missing_index_is_ok() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/indexes/gone"))
        .respond_with(task_ok(3))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/3"))
        .respond_with(task_state(3, "failed", Some("index_not_found")))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/indexes/gone404"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    s.delete_index("gone").await.unwrap();
    s.delete_index("gone404").await.unwrap();
}

#[tokio::test]
async fn delete_all_documents_cases() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/indexes/movies/documents"))
        .respond_with(task_ok(7))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/7"))
        .respond_with(task_state(7, "succeeded", None))
        .mount(&server)
        .await;
    // Missing index, reported on enqueue or by the task: nothing to delete.
    Mock::given(method("DELETE"))
        .and(path("/indexes/gone404/documents"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/indexes/gone/documents"))
        .respond_with(task_ok(8))
        .mount(&server)
        .await;
    Mock::given(path("/tasks/8"))
        .respond_with(task_state(8, "failed", Some("index_not_found")))
        .mount(&server)
        .await;
    // Any other failure is mapped.
    Mock::given(method("DELETE"))
        .and(path("/indexes/locked/documents"))
        .respond_with(err(403, "invalid_api_key", "auth"))
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    s.delete_all_documents("movies").await.unwrap();
    s.delete_all_documents("gone404").await.unwrap();
    s.delete_all_documents("gone").await.unwrap();
    let e = s.delete_all_documents("locked").await.unwrap_err();
    assert!(matches!(e, SinkError::Config(_)), "{e:?}");
}

#[tokio::test]
async fn copy_settings_cases() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/indexes/missing/settings"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .mount(&server)
        .await;
    let settings = json!({"searchableAttributes": ["title"], "rankingRules": ["words"]});
    Mock::given(method("GET"))
        .and(path("/indexes/live/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(settings.clone()))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/shadow/settings"))
        .and(wiremock::matchers::body_json(settings))
        .respond_with(task_ok(5))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/5"))
        .respond_with(task_state(5, "succeeded", None))
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    s.copy_settings("missing", "shadow").await.unwrap();
    s.copy_settings("live", "shadow").await.unwrap();
}

#[tokio::test]
async fn copy_settings_refuses_redacted_api_keys() {
    let server = MockServer::start().await;
    // What Meilisearch 1.57 returns for embedders configured with keys.
    let cases = [
        (
            "oa",
            json!({"oa": {"source": "openAi", "model": "m", "apiKey": "sk-XXXXX..."}}),
        ),
        (
            "mix",
            json!({"mix": {"source": "composite",
                "searchEmbedder": {"source": "userProvided", "dimensions": 3},
                "indexingEmbedder": {"source": "rest", "apiKey": "XXX..."}}}),
        ),
    ];
    for (i, (name, embedders)) in cases.into_iter().enumerate() {
        let from = format!("live{i}");
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{from}/settings")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "searchableAttributes": ["*"],
                "embedders": embedders,
            })))
            .mount(&server)
            .await;
        let e = sink(&server, opts())
            .copy_settings(&from, "shadow")
            .await
            .unwrap_err();
        assert!(
            matches!(&e, SinkError::Config(m) if m.contains(&format!("`{name}`"))),
            "{e:?}"
        );
        assert!(!e.to_string().contains("XXX"));
    }
    // Embedders without a key are copied.
    let settings = json!({"embedders": {
        "up": {"source": "userProvided", "dimensions": 3},
        "local": {"source": "ollama", "url": "http://x", "apiKey": null},
    }});
    Mock::given(method("GET"))
        .and(path("/indexes/keyless/settings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(settings.clone()))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/indexes/shadow/settings"))
        .and(wiremock::matchers::body_json(settings))
        .respond_with(task_ok(9))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/tasks/9"))
        .respond_with(task_state(9, "succeeded", None))
        .mount(&server)
        .await;
    sink(&server, opts())
        .copy_settings("keyless", "shadow")
        .await
        .unwrap();
}

#[tokio::test]
async fn document_count_cases() {
    let server = MockServer::start().await;
    Mock::given(path("/indexes/movies/stats"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"numberOfDocuments": 42, "isIndexing": false})),
        )
        .mount(&server)
        .await;
    Mock::given(path("/indexes/nope/stats"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
        .mount(&server)
        .await;
    let s = sink(&server, opts());
    assert_eq!(s.document_count("movies").await.unwrap(), Some(42));
    assert_eq!(s.document_count("nope").await.unwrap(), None);
}

#[tokio::test]
async fn url_prefix_and_escaping() {
    let server = MockServer::start().await;
    Mock::given(path("/meili/indexes/a%2Fb/stats"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"numberOfDocuments": 1})))
        .mount(&server)
        .await;
    let dest = MeiliDestination {
        url: format!("{}/meili/", server.uri()),
        api_key: Secret::new(""),
    };
    let s = MeiliSink::new(&dest, opts()).unwrap();
    assert_eq!(s.document_count("a/b").await.unwrap(), Some(1));
}
