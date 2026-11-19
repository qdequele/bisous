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
}

/// A small in-memory Meilisearch: documents with `"bad": true` make their
/// task fail with `invalid_document_id`, with `"embed": "fail"` with
/// `vector_embedding_error`.
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
        let failure = items.iter().find_map(|d| {
            if d.get("bad") == Some(&json!(true)) {
                Some(("invalid_document_id", format!("bad document {}", d["id"])))
            } else if d.get("embed") == Some(&json!("fail")) {
                Some(("vector_embedding_error", "embedder down".to_owned()))
            } else {
                None
            }
        });
        let uid = st.tasks.len();
        let task = match &failure {
            None => json!({"uid": uid, "status": "succeeded"}),
            Some((code, msg)) => json!({"uid": uid, "status": "failed",
                "error": {"message": msg, "code": code, "type": "invalid_request", "link": ""}}),
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

#[tokio::test]
async fn embedder_outage_is_transient_but_one_bad_embedding_is_rejected() {
    let server = MockServer::start().await;
    let fake = Fake::default();
    fake.mount(&server).await;
    let s = sink(&server, opts());
    let all_fail: Vec<DocOp> = (0..4)
        .map(|i| replace(&i.to_string(), json!({"embed": "fail"})))
        .collect();
    let e = s.apply(&target(), all_fail).await.unwrap_err();
    assert!(matches!(e, SinkError::Transient(_)), "{e:?}");

    let one_fails: Vec<DocOp> = (0..4)
        .map(|i| {
            let v = if i == 2 { "fail" } else { "ok" };
            replace(&i.to_string(), json!({"embed": v}))
        })
        .collect();
    let report = s.apply(&target(), one_fails).await.unwrap();
    assert_eq!(report.written, 3);
    assert_eq!(report.rejected[0].op.id(), "2");
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

    // Missing: created with the primary key.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/indexes/movies"))
        .respond_with(err(404, "index_not_found", "invalid_request"))
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
    Mock::given(path("/tasks/4"))
        .respond_with(task_state(4, "succeeded", None))
        .mount(&server)
        .await;
    sink(&server, opts()).ensure_index(&target()).await.unwrap();

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
