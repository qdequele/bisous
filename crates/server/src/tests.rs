//! API tests against fake sources and sinks, on the in-memory store.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use meili_connect_core::{
    AckReceiver, ApplyReport, Capabilities, Change, CheckReport, CheckStatus, Checkpoint,
    CollectionInfo, DocOp, EventSender, IndexTarget, MeiliDestination, Op, Opened, Sink, SinkError,
    Source, SourceError, SourceEvent, SourceFactory, SourceSpec, StreamMapping, SyncPhase,
};
use meili_connect_engine::SupervisorConfig;
use meili_connect_store::{LabOutbox, MemStore, SyncRepository};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::auth::{AuthConfig, H_TENANT_ID};
use crate::manager::SinkFactory;
use crate::routes::ROUTES;
use crate::state::{AppState, Backend};
use crate::{Wiring, boot_syncs, build_state, router};

const LAB: &str = "lab-token";
const ADMIN: &str = "admin-key";
const SECRET_MARK: &str = "hunter2";

// ------------------------------------------------------------- fakes

#[derive(Default)]
struct Probe {
    teardown_fails: AtomicBool,
    teardowns: AtomicUsize,
}

struct FakeSources(Arc<Probe>);

/// Config knobs: `invalid` (build fails), `slow` (check/discover hang),
/// `rows` (snapshot emits that many upserts).
struct FakeSource {
    probe: Arc<Probe>,
    config: Value,
    collection: String,
}

impl SourceFactory for FakeSources {
    fn build(
        &self,
        _sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        if spec.config.get("invalid").is_some() {
            return Err(SourceError::Config("the config is invalid".into()));
        }
        Ok(Box::new(FakeSource {
            probe: Arc::clone(&self.0),
            config: spec.config.clone(),
            collection: streams
                .first()
                .map(|s| s.collection.clone())
                .unwrap_or_default(),
        }))
    }
}

#[async_trait]
impl Source for FakeSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        if self.config.get("slow").is_some() {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        let mut r = CheckReport::default();
        r.push("connect", CheckStatus::Ok, "connected");
        r.push("wal_level", CheckStatus::Failed, "set wal_level to logical");
        Ok(r)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        if self.config.get("slow").is_some() {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        Ok(vec![CollectionInfo {
            name: "public.items".into(),
            key: vec!["id".into()],
            fields: vec![],
            estimated_rows: Some(3),
        }])
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        Ok(match resume {
            Some(_) => Opened::Resumed,
            None => Opened::Fresh {
                start: Checkpoint(json!(0)),
            },
        })
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        _cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let rows = self.config.get("rows").and_then(Value::as_u64).unwrap_or(0);
        for i in 0..rows {
            let mut doc = serde_json::Map::new();
            doc.insert("n".into(), json!(i));
            let change = Change {
                collection: self.collection.clone(),
                key: vec![json!(i)],
                op: Op::Upsert { doc },
            };
            if events.send(SourceEvent::Change(change)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn stream(
        &mut self,
        _events: EventSender,
        _acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        cancel.cancelled().await;
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        self.probe.teardowns.fetch_add(1, Ordering::SeqCst);
        if self.probe.teardown_fails.load(Ordering::SeqCst) {
            Err(SourceError::Transient("origin unreachable".into()))
        } else {
            Ok(())
        }
    }
}

struct FakeSink;

#[async_trait]
impl Sink for FakeSink {
    async fn ensure_index(&self, _: &IndexTarget) -> Result<(), SinkError> {
        Ok(())
    }
    async fn apply(&self, _: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError> {
        let deleted = ops
            .iter()
            .filter(|o| matches!(o, DocOp::Delete { .. }))
            .count() as u64;
        Ok(ApplyReport {
            written: ops.len() as u64 - deleted,
            deleted,
            rejected: vec![],
        })
    }
    async fn swap(&self, _: &str, _: &str) -> Result<(), SinkError> {
        Ok(())
    }
    async fn delete_all_documents(&self, _: &str) -> Result<(), SinkError> {
        Ok(())
    }
    async fn delete_index(&self, _: &str) -> Result<(), SinkError> {
        Ok(())
    }
    async fn copy_settings(&self, _: &str, _: &str) -> Result<(), SinkError> {
        Ok(())
    }
    async fn document_count(&self, _: &str) -> Result<Option<u64>, SinkError> {
        Ok(Some(0))
    }
}

fn fake_sinks() -> SinkFactory {
    Arc::new(|_: &MeiliDestination| Ok(Arc::new(FakeSink) as Arc<dyn Sink>))
}

// ------------------------------------------------------------- harness

struct App {
    router: Router,
    state: AppState,
    store: Arc<MemStore>,
    probe: Arc<Probe>,
}

fn app_with(auth: AuthConfig, store: Arc<MemStore>) -> App {
    let probe = Arc::new(Probe::default());
    let state = build_state(
        Arc::clone(&store) as Arc<dyn Backend>,
        Wiring {
            sources: Arc::new(FakeSources(Arc::clone(&probe))),
            sinks: fake_sinks(),
            supervisor: SupervisorConfig::default(),
            auth,
            probe_timeout: Duration::from_millis(200),
            lab_events: true,
        },
        CancellationToken::new(),
    );
    App {
        router: router(state.clone()),
        state,
        store,
        probe,
    }
}

fn app(auth: AuthConfig) -> App {
    app_with(auth, Arc::new(MemStore::new()))
}

fn locked() -> AuthConfig {
    AuthConfig {
        lab_service_token: Some(LAB.into()),
        admin_api_key: Some(ADMIN.into()),
    }
}

fn lab(tenant: &str) -> Vec<(&'static str, String)> {
    vec![
        ("authorization", format!("Bearer {LAB}")),
        (H_TENANT_ID, tenant.to_owned()),
    ]
}

fn admin() -> Vec<(&'static str, String)> {
    vec![("authorization", format!("Bearer {ADMIN}"))]
}

async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    headers: &[(&'static str, String)],
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .method(Method::from_bytes(method.as_bytes()).unwrap())
        .uri(uri);
    for (k, v) in headers {
        req = req.header(*k, v);
    }
    let req = match body {
        Some(b) => req
            .header("content-type", "application/json")
            .body(Body::from(b.to_string()))
            .unwrap(),
        None => req.body(Body::empty()).unwrap(),
    };
    let resp = router.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    (status, value)
}

fn sync_body(name: &str) -> Value {
    json!({
        "name": name,
        "source": {"kind": "postgres", "config": {
            "url": format!("postgres://app:{SECRET_MARK}@db:5432/app"),
            "password": SECRET_MARK,
            "rows": 3,
            "extra": {"client_secret": SECRET_MARK},
        }},
        "destination": {"url": "http://meili:7700", "api_key": format!("meili-{SECRET_MARK}")},
        "streams": [{"collection": "public.items", "index": "items"}],
    })
}

async fn create(app: &App, headers: &[(&'static str, String)], name: &str) -> String {
    let (status, body) = call(
        &app.router,
        "POST",
        "/syncs",
        headers,
        Some(sync_body(name)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    body["id"].as_str().unwrap().to_owned()
}

fn assert_no_secret(body: &Value) {
    assert!(
        !body.to_string().contains(SECRET_MARK),
        "secret leaked: {body}"
    );
}

async fn wait_for(mut cond: impl AsyncFnMut() -> bool) {
    for _ in 0..200 {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached in time");
}

// ------------------------------------------------------------- auth

#[tokio::test]
async fn auth_matrix() {
    // Open mode: everything is global, no token needed.
    let open = app(AuthConfig::default());
    assert_eq!(
        call(&open.router, "GET", "/syncs", &[], None).await.0,
        StatusCode::OK
    );

    let a = app(locked());
    for (headers, expected, code) in [
        (vec![], StatusCode::UNAUTHORIZED, "unauthorized"),
        (
            vec![("authorization", "Bearer wrong".to_owned())],
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        ),
        (
            vec![("authorization", format!("Bearer {LAB}x"))],
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        ),
        (
            vec![("authorization", format!("Bearer {LAB}"))],
            StatusCode::BAD_REQUEST,
            "invalid_tenant",
        ),
    ] {
        let (status, body) = call(&a.router, "GET", "/syncs", &headers, None).await;
        assert_eq!(status, expected, "{headers:?}");
        assert_eq!(body["code"], code);
        assert!(body["message"].is_string());
    }
    assert_eq!(
        call(&a.router, "GET", "/syncs", &lab("t1"), None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        call(&a.router, "GET", "/syncs", &admin(), None).await.0,
        StatusCode::OK
    );
    // /health is always open.
    let (status, body) = call(&a.router, "GET", "/health", &[], None).await;
    assert_eq!(
        (status, body["status"].as_str()),
        (StatusCode::OK, Some("ok"))
    );

    // Every authenticated route refuses an unauthenticated call.
    for r in ROUTES.iter().filter(|r| r.authenticated) {
        let uri = r.path.replace("{id}", "x");
        let (status, _) = call(&a.router, r.method, &uri, &[], Some(json!({}))).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{} {}", r.method, r.path);
    }
}

#[tokio::test]
async fn admin_is_global_and_can_act_for_a_tenant() {
    let a = app(locked());
    let tenant_sync = create(&a, &lab("team-a"), "s").await;
    let global_sync = create(&a, &admin(), "s").await;
    let (_, list) = call(&a.router, "GET", "/syncs", &admin(), None).await;
    let ids: Vec<&str> = list["syncs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![global_sync.as_str()]);
    let mut as_team = admin();
    as_team.push((H_TENANT_ID, "team-a".to_owned()));
    let (status, body) = call(
        &a.router,
        "GET",
        &format!("/syncs/{tenant_sync}"),
        &as_team,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["tenant"], "team-a");
    // The Lab acting for team-a does not see the global sync.
    let (status, _) = call(
        &a.router,
        "GET",
        &format!("/syncs/{global_sync}"),
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ------------------------------------------------------------- tenancy

#[tokio::test]
async fn a_tenant_never_reaches_another_tenants_sync() {
    let a = app(locked());
    let id = create(&a, &lab("team-a"), "orders").await;
    let b = lab("team-b");

    let (_, list) = call(&a.router, "GET", "/syncs", &b, None).await;
    assert_eq!(list["syncs"], json!([]));

    for r in ROUTES.iter().filter(|r| r.path.contains("{id}")) {
        let uri = r.path.replace("{id}", &id);
        let body = (r.method == "PUT").then(|| sync_body("stolen"));
        let (status, resp) = call(&a.router, r.method, &uri, &b, body).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{} {}: {resp}",
            r.method,
            r.path
        );
        assert_eq!(resp["code"], "not_found");
    }
    let uri = format!("/syncs/{id}?force=true");
    assert_eq!(
        call(&a.router, "DELETE", &uri, &b, None).await.0,
        StatusCode::NOT_FOUND
    );

    // Untouched for its owner: same name, still running, not torn down.
    let (status, body) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}"),
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "orders");
    assert_eq!(a.probe.teardowns.load(Ordering::SeqCst), 0);
    let (_, st) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/status"),
        &lab("team-a"),
        None,
    )
    .await;
    assert_ne!(st["phase"], "paused");
}

// ------------------------------------------------------------- secrets

#[tokio::test]
async fn secrets_are_redacted_on_every_response() {
    let a = app(AuthConfig::default());
    let (status, created) = call(&a.router, "POST", "/syncs", &[], Some(sync_body("s"))).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_no_secret(&created);
    assert_eq!(created["destination"]["api_key"], "<redacted>");
    assert_eq!(created["source"]["config"]["url"], "<redacted>");
    assert_eq!(created["source"]["config"]["password"], "<redacted>");
    assert_eq!(
        created["source"]["config"]["extra"]["client_secret"],
        "<redacted>"
    );
    assert_eq!(created["source"]["config"]["rows"], 3);
    assert_eq!(created["destination"]["url"], "http://meili:7700");
    let id = created["id"].as_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).unwrap().get_version_num() == 7);

    for (method, uri) in [
        ("GET", "/syncs".to_owned()),
        ("GET", format!("/syncs/{id}")),
        ("GET", format!("/syncs/{id}/status")),
        ("POST", format!("/syncs/{id}/pause")),
        ("POST", format!("/syncs/{id}/resume")),
    ] {
        let (status, body) = call(&a.router, method, &uri, &[], None).await;
        assert_eq!(status, StatusCode::OK, "{method} {uri}");
        assert_no_secret(&body);
    }
}

#[tokio::test]
async fn put_keeps_redacted_secrets() {
    let a = app(AuthConfig::default());
    let id = create(&a, &[], "s").await;
    let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    // Edit a plain field, send the redacted placeholders back.
    current["name"] = json!("renamed");
    current["source"]["config"]["rows"] = json!(5);
    let (status, updated) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(current.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_no_secret(&updated);
    assert_eq!(updated["name"], "renamed");

    let stored = a.store.get_sync(None, &id).await.unwrap().unwrap().spec;
    assert_eq!(
        stored.destination.api_key.expose(),
        format!("meili-{SECRET_MARK}")
    );
    assert_eq!(stored.source.config["password"], SECRET_MARK);
    assert_eq!(stored.source.config["extra"]["client_secret"], SECRET_MARK);
    assert_eq!(stored.source.config["rows"], 5);

    // A new value replaces the stored one.
    current["source"]["config"]["password"] = json!("new-pass");
    let (status, _) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(current.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let stored = a.store.get_sync(None, &id).await.unwrap().unwrap().spec;
    assert_eq!(stored.source.config["password"], "new-pass");

    // A placeholder with nothing stored behind it is refused.
    current["source"]["config"]["new_token"] = json!("<redacted>");
    let (status, body) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(current),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("source.config.new_token")
    );

    // A create never accepts placeholders.
    let mut body = sync_body("other");
    body["destination"]["api_key"] = json!("<redacted>");
    let (status, _) = call(&a.router, "POST", "/syncs", &[], Some(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ------------------------------------------------------------- validation

#[tokio::test]
async fn invalid_specs_are_refused() {
    let a = app(AuthConfig::default());
    let cases: Vec<(Value, StatusCode, &str)> = vec![
        (
            {
                let mut b = sync_body("s");
                b["streams"] = json!([]);
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["streams"][0]["index"] = json!("bad uid");
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["mass_delete_guard"] = json!(3.0);
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["name"] = json!("");
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["destination"]["url"] = json!("meili:7700");
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["source"]["kind"] = json!("oracle");
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["stream"] = json!([]);
                b
            },
            StatusCode::BAD_REQUEST,
            "invalid_request",
        ),
        (
            {
                let mut b = sync_body("s");
                b["source"]["config"]["invalid"] = json!(true);
                b
            },
            StatusCode::UNPROCESSABLE_ENTITY,
            "source_config",
        ),
    ];
    for (body, status, code) in cases {
        let (got, resp) = call(&a.router, "POST", "/syncs", &[], Some(body.clone())).await;
        assert_eq!(
            (got, resp["code"].as_str().unwrap()),
            (status, code),
            "{body}: {resp}"
        );
        assert_no_secret(&resp);
    }
    // Malformed JSON.
    let req = Request::builder()
        .method("POST")
        .uri("/syncs")
        .header("content-type", "application/json")
        .body(Body::from("{nope"))
        .unwrap();
    let resp = a.router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: Value =
        serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["code"], "invalid_request");
    // Nothing was stored.
    let (_, list) = call(&a.router, "GET", "/syncs", &[], None).await;
    assert_eq!(list["syncs"], json!([]));
    // Duplicate names in a scope conflict.
    create(&a, &[], "dup").await;
    let (status, body) = call(&a.router, "POST", "/syncs", &[], Some(sync_body("dup"))).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::CONFLICT, Some("conflict"))
    );
    // Bad query strings.
    let id = create(&a, &[], "q").await;
    let (status, _) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/progress?from=nope"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/progress?from=2026-02-01&to=2026-01-01"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/progress?from=2020-01-01&to=2026-01-01"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ------------------------------------------------------------- sources

#[tokio::test]
async fn check_and_discover_store_nothing_and_time_out() {
    let a = app(AuthConfig::default());
    let probe = json!({"source": {"kind": "postgres", "config": {"password": "x"}}});
    let (status, body) = call(
        &a.router,
        "POST",
        "/sources/check",
        &[],
        Some(probe.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["passed"], false);
    assert_eq!(body["items"][1]["status"], "failed");
    let (status, body) = call(&a.router, "POST", "/sources/discover", &[], Some(probe)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["collections"][0]["name"], "public.items");
    assert_eq!(a.store.list_syncs(None).await.unwrap().len(), 0);

    let slow = json!({"source": {"kind": "postgres", "config": {"slow": true}}});
    for uri in ["/sources/check", "/sources/discover"] {
        let (status, body) = call(&a.router, "POST", uri, &[], Some(slow.clone())).await;
        assert_eq!(
            (status, body["code"].as_str()),
            (StatusCode::GATEWAY_TIMEOUT, Some("source_timeout"))
        );
    }
    let bad = json!({"source": {"kind": "postgres", "config": {"invalid": true}}});
    let (status, body) = call(&a.router, "POST", "/sources/check", &[], Some(bad)).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("source_config"))
    );
}

// ------------------------------------------------------------- lifecycle

#[tokio::test]
async fn delete_tears_down_and_keeps_the_sync_when_teardown_fails() {
    let a = app(AuthConfig::default());
    let id = create(&a, &[], "s").await;
    a.probe.teardown_fails.store(true, Ordering::SeqCst);
    let (status, body) = call(&a.router, "DELETE", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::BAD_GATEWAY, Some("teardown_failed"))
    );
    let (status, _) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(status, StatusCode::OK, "kept for a retry");
    // A retry once the origin is back succeeds.
    a.probe.teardown_fails.store(false, Ordering::SeqCst);
    let (status, _) = call(&a.router, "DELETE", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(a.probe.teardowns.load(Ordering::SeqCst), 2);
    let (status, _) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // force deletes even when the teardown fails.
    let id = create(&a, &[], "s2").await;
    a.probe.teardown_fails.store(true, Ordering::SeqCst);
    let (status, _) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{id}?force=true"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(a.store.get_sync(None, &id).await.unwrap().is_none());
    assert!(a.state.manager.running_ids().is_empty());
}

#[tokio::test]
async fn pause_resume_status_and_release() {
    let a = app(AuthConfig::default());
    let id = create(&a, &[], "s").await;
    let status_uri = format!("/syncs/{id}/status");
    wait_for(async || {
        call(&a.router, "GET", &status_uri, &[], None).await.1["phase"] == "streaming"
    })
    .await;
    let (_, st) = call(&a.router, "GET", &status_uri, &[], None).await;
    assert_eq!(st["running"], true);
    assert_eq!(st["has_checkpoint"], true);
    assert_eq!(st["progress"]["written"], 3);

    let (_, st) = call(&a.router, "POST", &format!("/syncs/{id}/pause"), &[], None).await;
    assert_eq!(
        (st["phase"].as_str(), st["running"].as_bool()),
        (Some("paused"), Some(false))
    );
    let (_, st) = call(&a.router, "POST", &format!("/syncs/{id}/resume"), &[], None).await;
    assert_eq!(st["running"], true);
    wait_for(async || {
        call(&a.router, "GET", &status_uri, &[], None).await.1["phase"] == "streaming"
    })
    .await;

    // Only a held sync can be released.
    let (status, body) = call(
        &a.router,
        "POST",
        &format!("/syncs/{id}/release"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::CONFLICT, Some("conflict"))
    );

    let (status, st) = call(&a.router, "POST", &format!("/syncs/{id}/resync"), &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(st["running"], true);

    let (status, body) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/progress"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["days"].as_array().unwrap()[0]["written"]
            .as_u64()
            .unwrap()
            >= 3
    );
    let (status, body) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/dead-letters?limit=5"),
        &[],
        None,
    )
    .await;
    assert_eq!((status, body["items"].clone()), (StatusCode::OK, json!([])));
    let (status, body) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{id}/dead-letters"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        (status, body["deleted"].as_u64()),
        (StatusCode::OK, Some(0))
    );
}

#[tokio::test]
async fn dead_letters_are_paged() {
    let a = app(AuthConfig::default());
    let id = create(&a, &[], "s").await;
    let letters: Vec<_> = (0..5)
        .map(|i| meili_connect_core::DeadLetter {
            sync_id: id.clone(),
            index: "items".into(),
            document_id: format!("d{i}"),
            payload: json!({"kind": "replace"}),
            reason: "invalid".into(),
            created_at: chrono::Utc::now(),
        })
        .collect();
    meili_connect_core::StateStore::push_dead_letters(&*a.store, &letters)
        .await
        .unwrap();
    let (_, page) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/dead-letters?limit=3"),
        &[],
        None,
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 3);
    let next = page["next_after"].as_i64().unwrap();
    let (_, page) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/dead-letters?limit=3&after={next}"),
        &[],
        None,
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
    assert!(page["next_after"].is_null());
    assert_eq!(page["items"][1]["document_id"], "d4");
    let (_, body) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{id}/dead-letters"),
        &[],
        None,
    )
    .await;
    assert_eq!(body["deleted"], 5);
}

#[tokio::test]
async fn boot_starts_every_sync_but_the_paused_ones() {
    let store = Arc::new(MemStore::new());
    let first = app_with(AuthConfig::default(), Arc::clone(&store));
    let running = create(&first, &[], "running").await;
    let paused = create(&first, &[], "paused").await;
    call(
        &first.router,
        "POST",
        &format!("/syncs/{paused}/pause"),
        &[],
        None,
    )
    .await;
    first.state.manager.shutdown(Duration::from_secs(5)).await;

    let second = app_with(AuthConfig::default(), store);
    assert_eq!(boot_syncs(&second.state).await.unwrap(), 1);
    let mut ids = second.state.manager.running_ids();
    ids.sort();
    assert_eq!(ids, vec![running]);
    let (_, st) = call(
        &second.router,
        "GET",
        &format!("/syncs/{paused}/status"),
        &[],
        None,
    )
    .await;
    assert_eq!(st["phase"], "paused");
}

// ------------------------------------------------------------- usage

#[tokio::test]
async fn engine_progress_becomes_usage_events_for_tenant_syncs() {
    let a = app(locked());
    let id = create(&a, &lab("team-a"), "s").await;
    let global = create(&a, &admin(), "g").await;
    let meter = Arc::clone(a.state.manager.meter());
    wait_for(async || meter.pending(&id) == 3 && meter.pending(&global) == 3).await;
    assert_eq!(
        meter.flush(&*a.store).await.unwrap(),
        1,
        "global syncs are not billed"
    );
    let lease = a
        .store
        .lease_events(10, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(lease.events.len(), 1);
    let e = &lease.events[0].payload;
    assert_eq!(e["account_id"], "team-a");
    assert_eq!(e["data"]["job_id"], id.as_str());
    assert_eq!(e["data"]["units"]["documents"], 3);

    // Deleting a sync enqueues what it had not reported yet.
    meter.add(&id, 2);
    let (status, _) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{id}"),
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let lease = a
        .store
        .lease_events(10, Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(lease.events.len(), 1);
    assert_eq!(lease.events[0].payload["data"]["units"]["documents"], 2);
}

// ------------------------------------------------------------- routes

fn normalize(path: &str) -> String {
    let mut out = String::new();
    let mut in_param = false;
    for c in path.chars() {
        match c {
            '{' => {
                in_param = true;
                out.push_str("{}");
            }
            '}' => in_param = false,
            _ if in_param => {}
            _ => out.push(c),
        }
    }
    out
}

fn table() -> BTreeSet<(String, String)> {
    ROUTES
        .iter()
        .map(|r| (r.method.to_owned(), normalize(r.path)))
        .collect()
}

#[test]
fn the_table_lists_every_path_in_lib_rs() {
    let source = include_str!("lib.rs");
    let mut mounted = BTreeSet::new();
    for chunk in source.split(".route(").skip(1) {
        let start = chunk.find('"').unwrap() + 1;
        let end = start + chunk[start..].find('"').unwrap();
        mounted.insert(normalize(&chunk[start..end]));
    }
    let listed: BTreeSet<String> = ROUTES.iter().map(|r| normalize(r.path)).collect();
    assert_eq!(mounted, listed, "lib.rs router() and ROUTES disagree");
}

#[tokio::test]
async fn every_listed_route_is_mounted_with_its_method() {
    let a = app(AuthConfig::default());
    for r in ROUTES {
        let uri = r
            .path
            .replace("{id}", "00000000-0000-0000-0000-000000000000");
        let (status, body) = call(&a.router, r.method, &uri, &[], Some(json!({}))).await;
        assert_ne!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{} {}",
            r.method,
            r.path
        );
        assert_ne!(body["message"], "no such route", "{} {}", r.method, r.path);
    }
    let (status, body) = call(&a.router, "GET", "/nope", &[], None).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::NOT_FOUND, Some("not_found"))
    );
}

fn openapi() -> serde_yaml::Value {
    serde_yaml::from_str(include_str!("../../../docs/openapi.yaml")).unwrap()
}

#[test]
fn the_table_matches_the_openapi_spec() {
    let spec = openapi();
    let mut documented = BTreeSet::new();
    for (path, item) in spec["paths"].as_mapping().unwrap() {
        for (method, _) in item.as_mapping().unwrap() {
            let method = method.as_str().unwrap();
            if ["get", "post", "put", "patch", "delete"].contains(&method) {
                documented.insert((method.to_uppercase(), normalize(path.as_str().unwrap())));
            }
        }
    }
    assert_eq!(documented, table(), "docs/openapi.yaml and ROUTES disagree");
}

#[test]
fn the_openapi_security_follows_the_route() {
    let spec = openapi();
    for r in ROUTES {
        let op = &spec["paths"][r.path][r.method.to_lowercase().as_str()];
        assert!(!op.is_null(), "{} {} missing", r.method, r.path);
        let open = op["security"].as_sequence().is_some_and(|s| s.is_empty());
        assert_eq!(open, !r.authenticated, "{} {} security", r.method, r.path);
    }
}

#[tokio::test]
async fn phase_names_match_the_spec() {
    // The status phase enum documented in the spec is the engine's.
    let spec = openapi();
    let documented: BTreeSet<String> = spec["components"]["schemas"]["SyncPhase"]["enum"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect();
    let all = [
        SyncPhase::Pending,
        SyncPhase::Snapshotting,
        SyncPhase::Streaming,
        SyncPhase::Paused,
        SyncPhase::SourcePaused,
        SyncPhase::Held,
        SyncPhase::Failed,
    ];
    let actual: BTreeSet<String> = all
        .iter()
        .map(|p| {
            serde_json::to_value(p)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(documented, actual);
}
