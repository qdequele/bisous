//! API tests against fake sources and sinks, on the in-memory store.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use meili_connect_core::StateStore;
use meili_connect_core::{
    AckReceiver, ApplyReport, Capabilities, Change, CheckReport, CheckStatus, Checkpoint,
    CollectionInfo, DocOp, Endpoint, EventSender, HostPolicy, IndexTarget, MeiliDestination, Op,
    Opened, Sink, SinkError, Source, SourceError, SourceEvent, SourceFactory, SourceSpec,
    StreamMapping, SyncPhase,
};
use meili_connect_engine::SupervisorConfig;
use meili_connect_store::{LabOutbox, Lane, MemStore, SyncRepository, UsageLedger};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::auth::{AuthConfig, H_TENANT_ID};
use crate::limits::Limits;
use crate::manager::SinkFactory;
use crate::routes::ROUTES;
use crate::state::{AppState, Backend};
use crate::{Wiring, boot_syncs, build_state, router};

const LAB: &str = "lab-token";
const ADMIN: &str = "admin-key";
const METRICS: &str = "metrics-token";
const SECRET_MARK: &str = "hunter2";
/// Bytes the fake sink reports per accepted document.
const FAKE_BYTES_PER_DOC: u64 = 100;

// ------------------------------------------------------------- fakes

#[derive(Default)]
struct Probe {
    teardown_fails: AtomicBool,
    teardowns: AtomicUsize,
    /// Each teardown takes this long.
    teardown_delay_ms: AtomicU64,
    /// The config of the last source the engine opened.
    last_opened: std::sync::Mutex<Option<Value>>,
    /// The configs torn down, in order.
    torn_down: std::sync::Mutex<Vec<Value>>,
}

struct FakeSources(Arc<Probe>);

/// Config knobs: `invalid` (build fails), `slow` (check/discover hang),
/// `rows` (snapshot emits that many upserts), `host`/`port` (its endpoint),
/// `check_error` (check fails with that transient text), `open_error`
/// (open fails with that configuration text).
struct FakeSource {
    probe: Arc<Probe>,
    config: Value,
    collection: String,
    lag: Option<meili_connect_core::LagReporter>,
}

#[async_trait]
impl SourceFactory for FakeSources {
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        Ok(match spec.config.get("host").and_then(Value::as_str) {
            Some(host) => vec![Endpoint::new(
                host,
                spec.config
                    .get("port")
                    .and_then(Value::as_u64)
                    .and_then(|p| u16::try_from(p).ok())
                    .unwrap_or(5432),
            )],
            None => vec![],
        })
    }

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
            lag: None,
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
        if let Some(e) = self.config.get("check_error").and_then(Value::as_str) {
            return Err(SourceError::Transient(e.to_owned()));
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
        if let Some(e) = self.config.get("open_error").and_then(Value::as_str) {
            return Err(SourceError::Config(e.to_owned()));
        }
        *self.probe.last_opened.lock().unwrap() = Some(self.config.clone());
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
        if self.config.get("stubborn").is_some() {
            // Ignores cancellation: only an abort stops it.
            std::future::pending::<()>().await;
        }
        if let Some(lag) = &self.lag {
            lag.report(meili_connect_core::Lag {
                seconds: Some(1.5),
                bytes: Some(2048),
                records: None,
            });
        }
        cancel.cancelled().await;
        Ok(())
    }

    fn report_lag(&mut self, reporter: meili_connect_core::LagReporter) {
        self.lag = Some(reporter);
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        self.probe.teardowns.fetch_add(1, Ordering::SeqCst);
        self.probe
            .torn_down
            .lock()
            .unwrap()
            .push(self.config.clone());
        let delay = self.probe.teardown_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
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
            bytes: ops.len() as u64 * FAKE_BYTES_PER_DOC,
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
    app_full(auth, store, HostPolicy::any())
}

fn app_full(auth: AuthConfig, store: Arc<MemStore>, policy: HostPolicy) -> App {
    app_build(
        auth,
        store,
        policy,
        Limits::default(),
        Duration::from_millis(200),
    )
}

fn app_limits(auth: AuthConfig, limits: Limits, probe_timeout: Duration) -> App {
    app_build(
        auth,
        Arc::new(MemStore::new()),
        HostPolicy::any(),
        limits,
        probe_timeout,
    )
}

fn app_build(
    auth: AuthConfig,
    store: Arc<MemStore>,
    policy: HostPolicy,
    limits: Limits,
    probe_timeout: Duration,
) -> App {
    app_tweaked(auth, store, policy, limits, probe_timeout, |_| {})
}

/// [`app_build`], with a last say on the wiring.
fn app_tweaked(
    auth: AuthConfig,
    store: Arc<MemStore>,
    policy: HostPolicy,
    limits: Limits,
    probe_timeout: Duration,
    tweak: impl FnOnce(&mut Wiring),
) -> App {
    let probe = Arc::new(Probe::default());
    let mut wiring = Wiring {
        sources: Arc::new(FakeSources(Arc::clone(&probe))),
        sinks: fake_sinks(),
        policy: Arc::new(policy),
        supervisor: SupervisorConfig::default(),
        auth,
        probe_timeout,
        lab_events: true,
        credits: None,
        metrics_token: Some(METRICS.into()),
        lag_guard: Default::default(),
        tinybird: None,
        hosted: false,
        limits,
    };
    tweak(&mut wiring);
    let state = build_state(
        Arc::clone(&store) as Arc<dyn Backend>,
        wiring,
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
        open_mode: false,
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
    let open = app(AuthConfig::open());
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
    let a = app(AuthConfig::open());
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
    let a = app(AuthConfig::open());
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

    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
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
    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
    assert_eq!(stored.source.config["password"], "new-pass");

    // A placeholder with nothing stored behind it is refused.
    current["source"]["config"]["new_token"] = json!("<redacted>");
    let (status, body) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(current.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("source.config.new_token")
    );

    // A placeholder never follows a new destination or a new source host.
    current["source"]["config"]["new_token"] = json!("t");
    let mut moved = current.clone();
    moved["destination"]["url"] = json!("http://attacker.example:7700");
    let (status, body) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(moved.clone()),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, Some("secret_required")),
        "{body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("destination.api_key")
    );
    let mut moved_source = current.clone();
    moved_source["source"]["config"]["host"] = json!("attacker.example");
    let (status, body) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(moved_source),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "secret_required");
    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
    assert_eq!(
        stored.destination.url, "http://meili:7700",
        "nothing changed"
    );
    // Sending the key again moves it.
    moved["destination"]["api_key"] = json!("other-key");
    let (status, _) = call(&a.router, "PUT", &format!("/syncs/{id}"), &[], Some(moved)).await;
    assert_eq!(status, StatusCode::OK);
    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
    assert_eq!(stored.destination.url, "http://attacker.example:7700");
    assert_eq!(stored.destination.api_key.expose(), "other-key");

    // A create never accepts placeholders.
    let mut body = sync_body("other");
    body["destination"]["api_key"] = json!("<redacted>");
    let (status, _) = call(&a.router, "POST", "/syncs", &[], Some(body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ------------------------------------------------------------- validation

#[tokio::test]
async fn invalid_specs_are_refused() {
    let a = app(AuthConfig::open());
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
    let a = app(AuthConfig::open());
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

// ------------------------------------------------------------- host policy

fn origin(host: &str, port: u16) -> Value {
    json!({"kind": "postgres", "config": {"host": host, "port": port, "rows": 1}})
}

fn body_with(source: Value, destination: &str, name: &str) -> Value {
    let mut b = sync_body(name);
    b["source"] = source;
    b["destination"]["url"] = json!(destination);
    b
}

#[tokio::test]
async fn the_public_policy_refuses_internal_origins_everywhere() {
    let a = app_full(locked(), Arc::new(MemStore::new()), HostPolicy::public());
    for host in [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "::1",
        "localhost",
    ] {
        for uri in ["/sources/check", "/sources/discover"] {
            let (status, body) = call(
                &a.router,
                "POST",
                uri,
                &lab("t1"),
                Some(json!({"source": origin(host, 5432)})),
            )
            .await;
            assert_eq!(
                (status, body["code"].as_str()),
                (StatusCode::UNPROCESSABLE_ENTITY, Some("host_not_allowed")),
                "{uri} {host}: {body}"
            );
            let msg = body["message"].as_str().unwrap();
            assert!(msg.contains(host), "{msg}");
            if host == "localhost" {
                assert!(!msg.contains("127.0.0.1") && !msg.contains("::1"), "{msg}");
            }
        }
        let (status, body) = call(
            &a.router,
            "POST",
            "/syncs",
            &lab("t1"),
            Some(body_with(origin(host, 5432), "https://8.8.8.8", host)),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{host}: {body}");
        assert_eq!(body["code"], "host_not_allowed");
    }
    // A Meilisearch destination on loopback is refused too.
    for dest in [
        "http://127.0.0.1:7700",
        "http://localhost:7700",
        "http://[::1]:7700",
    ] {
        let (status, body) = call(
            &a.router,
            "POST",
            "/syncs",
            &lab("t1"),
            Some(body_with(origin("8.8.8.8", 5432), dest, "d")),
        )
        .await;
        assert_eq!(
            (status, body["code"].as_str()),
            (StatusCode::UNPROCESSABLE_ENTITY, Some("host_not_allowed")),
            "{dest}: {body}"
        );
    }
    let (_, list) = call(&a.router, "GET", "/syncs", &lab("t1"), None).await;
    assert_eq!(list["syncs"], json!([]));
    // A public origin and destination pass.
    let (status, body) = call(
        &a.router,
        "POST",
        "/sources/check",
        &lab("t1"),
        Some(json!({"source": origin("8.8.8.8", 5432)})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

/// With the production sinks (which refuse a literal internal address
/// themselves) the answer is still `422 host_not_allowed`.
#[tokio::test]
async fn a_loopback_destination_is_host_not_allowed_with_the_real_sinks() {
    let policy = Arc::new(HostPolicy::public());
    let state = build_state(
        Arc::new(MemStore::new()) as Arc<dyn Backend>,
        Wiring {
            sources: Arc::new(FakeSources(Arc::new(Probe::default()))),
            sinks: crate::manager::meili_sinks(Arc::clone(&policy)),
            policy,
            supervisor: SupervisorConfig::default(),
            auth: AuthConfig::open(),
            probe_timeout: Duration::from_millis(200),
            lab_events: false,
            credits: None,
            metrics_token: Some(METRICS.into()),
            lag_guard: Default::default(),
            tinybird: None,
            hosted: false,
            limits: Limits::default(),
        },
        CancellationToken::new(),
    );
    let router = router(state);
    for dest in [
        "http://127.0.0.1:7700",
        "http://[::1]:7700",
        "http://localhost:7700",
    ] {
        let (status, body) = call(
            &router,
            "POST",
            "/syncs",
            &[],
            Some(body_with(origin("8.8.8.8", 5432), dest, "d")),
        )
        .await;
        assert_eq!(
            (status, body["code"].as_str()),
            (StatusCode::UNPROCESSABLE_ENTITY, Some("host_not_allowed")),
            "{dest}: {body}"
        );
    }
}

#[tokio::test]
async fn the_state_database_is_refused_even_under_any() {
    let policy = HostPolicy::any().forbid_origin(Endpoint::new("127.0.0.1", 6543));
    let a = app_full(AuthConfig::open(), Arc::new(MemStore::new()), policy);
    for host in ["127.0.0.1", "localhost"] {
        let (status, body) = call(
            &a.router,
            "POST",
            "/sources/check",
            &[],
            Some(json!({"source": origin(host, 6543)})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{host}: {body}");
        assert_eq!(body["code"], "host_not_allowed");
    }
    // Another port of the same host is a different server.
    let (status, _) = call(
        &a.router,
        "POST",
        "/sources/check",
        &[],
        Some(json!({"source": origin("127.0.0.1", 5432)})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn every_start_vets_the_origin_again() {
    let store = Arc::new(MemStore::new());
    let first = app_with(AuthConfig::open(), Arc::clone(&store));
    let mut body = sync_body("s");
    body["source"] = origin("127.0.0.1", 5432);
    let (status, created) = call(&first.router, "POST", "/syncs", &[], Some(body)).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap().to_owned();
    first.state.manager.shutdown(Duration::from_secs(5)).await;

    // The operator tightens the policy: the stored sync is held at boot.
    let second = app_full(AuthConfig::open(), store, HostPolicy::public());
    boot_syncs(&second.state).await.unwrap();
    let uri = format!("/syncs/{id}/status");
    wait_for(async || call(&second.router, "GET", &uri, &[], None).await.1["phase"] == "held")
        .await;
    let (_, st) = call(&second.router, "GET", &uri, &[], None).await;
    assert!(
        st["message"].as_str().unwrap().contains("not allowed"),
        "{st}"
    );
}

#[tokio::test]
async fn lab_callers_and_tenant_syncs_get_coarse_errors() {
    let a = app(locked());
    let probe = json!({"source": {"kind": "postgres", "config": {
        "check_error": "connect 10.9.9.9:5432: DRIVER-DETAIL"}}});
    let (status, body) = call(
        &a.router,
        "POST",
        "/sources/check",
        &lab("t1"),
        Some(probe.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!body.to_string().contains("DRIVER-DETAIL"), "{body}");
    // The operator sees the detail.
    let (_, body) = call(&a.router, "POST", "/sources/check", &admin(), Some(probe)).await;
    assert!(body.to_string().contains("DRIVER-DETAIL"), "{body}");

    // Persisted messages of a tenant's sync are coarse too.
    let mut body = sync_body("held");
    body["source"]["config"]["open_error"] = json!("relation 10.9.9.9 DRIVER-DETAIL");
    let (status, created) = call(&a.router, "POST", "/syncs", &lab("t1"), Some(body.clone())).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["id"].as_str().unwrap().to_owned();
    let uri = format!("/syncs/{id}/status");
    wait_for(async || call(&a.router, "GET", &uri, &lab("t1"), None).await.1["phase"] == "held")
        .await;
    let (_, st) = call(&a.router, "GET", &uri, &lab("t1"), None).await;
    assert!(!st.to_string().contains("DRIVER-DETAIL"), "{st}");
    assert!(
        st["message"].as_str().unwrap().contains("configuration"),
        "{st}"
    );
    // A global sync keeps the detail for the operator.
    let (status, created) = call(&a.router, "POST", "/syncs", &admin(), Some(body)).await;
    assert_eq!(status, StatusCode::CREATED);
    let id = created["id"].as_str().unwrap().to_owned();
    let uri = format!("/syncs/{id}/status");
    wait_for(async || call(&a.router, "GET", &uri, &admin(), None).await.1["phase"] == "held")
        .await;
    let (_, st) = call(&a.router, "GET", &uri, &admin(), None).await;
    assert!(
        st["message"].as_str().unwrap().contains("DRIVER-DETAIL"),
        "{st}"
    );
}

// ------------------------------------------------------------- quotas

#[tokio::test]
async fn a_tenant_owns_a_bounded_number_of_syncs() {
    let a = app_limits(
        locked(),
        Limits {
            max_syncs_per_tenant: 2,
            ..Limits::default()
        },
        Duration::from_millis(200),
    );
    create(&a, &lab("t1"), "one").await;
    let two = create(&a, &lab("t1"), "two").await;
    let (status, body) = call(
        &a.router,
        "POST",
        "/syncs",
        &lab("t1"),
        Some(sync_body("three")),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::CONFLICT, Some("sync_quota_exceeded")),
        "{body}"
    );
    // Other tenants and the global scope are not affected.
    create(&a, &lab("t2"), "one").await;
    for i in 0..3 {
        create(&a, &admin(), &format!("g{i}")).await;
    }
    // Deleting one frees a slot.
    let (status, _) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{two}"),
        &lab("t1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    create(&a, &lab("t1"), "three").await;
}

#[tokio::test]
async fn concurrent_creates_respect_the_quota() {
    let a = app_limits(
        locked(),
        Limits {
            max_syncs_per_tenant: 3,
            ..Limits::default()
        },
        Duration::from_millis(200),
    );
    let calls = (0..8).map(|i| {
        let router = a.router.clone();
        async move {
            call(
                &router,
                "POST",
                "/syncs",
                &lab("t1"),
                Some(sync_body(&format!("s{i}"))),
            )
            .await
            .0
        }
    });
    let statuses = futures_join_all(calls).await;
    assert_eq!(
        statuses
            .iter()
            .filter(|s| **s == StatusCode::CREATED)
            .count(),
        3
    );
    let (_, list) = call(&a.router, "GET", "/syncs", &lab("t1"), None).await;
    assert_eq!(list["syncs"].as_array().unwrap().len(), 3);
}

async fn futures_join_all<F: std::future::Future + Send + 'static>(
    futs: impl Iterator<Item = F>,
) -> Vec<F::Output>
where
    F::Output: Send + 'static,
{
    let handles: Vec<_> = futs.map(tokio::spawn).collect();
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

#[tokio::test]
async fn probes_are_bounded_per_tenant_and_in_all() {
    let a = app_limits(
        locked(),
        Limits {
            probe_concurrency: 2,
            probe_concurrency_per_tenant: 1,
            ..Limits::default()
        },
        Duration::from_millis(400),
    );
    let slow = json!({"source": {"kind": "postgres", "config": {"slow": true}}});
    let fast = json!({"source": {"kind": "postgres", "config": {}}});
    let router = a.router.clone();
    let s = slow.clone();
    let first = tokio::spawn(async move {
        call(&router, "POST", "/sources/check", &lab("t1"), Some(s))
            .await
            .0
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The same tenant has no slot left.
    let (status, body) = call(
        &a.router,
        "POST",
        "/sources/discover",
        &lab("t1"),
        Some(fast.clone()),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("too_many_probes"))
    );
    // Another tenant has one, until the global pool is used up.
    let router = a.router.clone();
    let second = tokio::spawn(async move {
        call(&router, "POST", "/sources/check", &lab("t2"), Some(slow))
            .await
            .0
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _) = call(
        &a.router,
        "POST",
        "/sources/check",
        &lab("t3"),
        Some(fast.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(first.await.unwrap(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(second.await.unwrap(), StatusCode::GATEWAY_TIMEOUT);
    // Slots come back.
    let (status, _) = call(&a.router, "POST", "/sources/check", &lab("t1"), Some(fast)).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn requests_are_bounded_in_time() {
    let a = app_limits(
        AuthConfig::open(),
        Limits {
            request_timeout: Duration::from_millis(150),
            ..Limits::default()
        },
        Duration::from_secs(20),
    );
    let started = std::time::Instant::now();
    let (status, body) = call(
        &a.router,
        "POST",
        "/sources/check",
        &[],
        Some(json!({"source": {"kind": "postgres", "config": {"slow": true}}})),
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("request_timeout"))
    );
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn dead_letters_overflow_is_reported() {
    let store = Arc::new(MemStore::new().with_dead_letter_limits(
        meili_connect_store::DeadLetterLimits {
            max_per_sync: 2,
            ..Default::default()
        },
    ));
    let a = app_with(AuthConfig::open(), store);
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
    let (_, st) = call(&a.router, "GET", &format!("/syncs/{id}/status"), &[], None).await;
    assert_eq!(st["dead_letters_overflow"], 3, "{st}");
    let (_, page) = call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/dead-letters"),
        &[],
        None,
    )
    .await;
    assert_eq!(page["items"].as_array().unwrap().len(), 2);
}

// ------------------------------------------------------------- stopping

/// A sync that does not stop holds its lock at most the grace period:
/// update, pause and delete answer instead of waiting for it.
#[tokio::test]
async fn a_sync_that_does_not_stop_is_abandoned_after_the_grace() {
    let a = app_limits(
        AuthConfig::open(),
        Limits {
            stop_grace: Duration::from_millis(200),
            ..Limits::default()
        },
        Duration::from_millis(200),
    );
    let mut body = sync_body("s");
    body["source"]["config"]["stubborn"] = json!(true);
    let (_, created) = call(&a.router, "POST", "/syncs", &[], Some(body)).await;
    let id = created["id"].as_str().unwrap().to_owned();
    streaming(&a, &id).await;
    let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    current["name"] = json!("renamed");
    for (method, uri, body) in [
        ("PUT", format!("/syncs/{id}"), Some(current)),
        ("POST", format!("/syncs/{id}/pause"), None),
    ] {
        streaming(&a, &id).await;
        let started = std::time::Instant::now();
        let (status, resp) = call(&a.router, method, &uri, &[], body).await;
        assert_eq!(status, StatusCode::OK, "{method} {uri}: {resp}");
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "{method} {uri} took {:?}",
            started.elapsed()
        );
        if method == "POST" {
            assert_eq!(resp["phase"], "paused");
        }
        call(&a.router, "POST", &format!("/syncs/{id}/resume"), &[], None).await;
    }
}

// ------------------------------------------------------------- unreadable rows

#[tokio::test]
async fn an_unreadable_row_never_hides_the_others() {
    let a = app(AuthConfig::open());
    let good = create(&a, &[], "a-good").await;
    let bad = create(&a, &[], "b-bad").await;
    a.store.make_unreadable(&bad);

    let (status, list) = call(&a.router, "GET", "/syncs", &[], None).await;
    assert_eq!(status, StatusCode::OK, "{list}");
    let syncs = list["syncs"].as_array().unwrap();
    assert_eq!(syncs.len(), 2);
    assert_eq!(syncs[0]["id"], good.as_str());
    assert_eq!(syncs[0]["destination"]["api_key"], "<redacted>");
    assert_eq!(
        (syncs[1]["id"].as_str(), syncs[1]["unreadable"].as_bool()),
        (Some(bad.as_str()), Some(true))
    );
    assert_eq!(syncs[1]["name"], "b-bad");
    assert_eq!(syncs[1]["source"]["kind"], "postgres");

    let (status, one) = call(&a.router, "GET", &format!("/syncs/{bad}"), &[], None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(one["unreadable"], true);
    // Its status still answers; changes are refused; force deletes it.
    let (status, _) = call(&a.router, "GET", &format!("/syncs/{bad}/status"), &[], None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = call(
        &a.router,
        "POST",
        &format!("/syncs/{bad}/resume"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::CONFLICT, Some("sync_unreadable"))
    );
    let (status, _) = call(&a.router, "DELETE", &format!("/syncs/{bad}"), &[], None).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = call(
        &a.router,
        "DELETE",
        &format!("/syncs/{bad}?force=true"),
        &[],
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, list) = call(&a.router, "GET", "/syncs", &[], None).await;
    assert_eq!(list["syncs"].as_array().unwrap().len(), 1);
}

// ------------------------------------------------------------- create

/// A sync that cannot start is not kept: the client gets the error and
/// nothing starts unannounced at the next boot.
#[tokio::test]
async fn a_sync_that_cannot_start_is_rolled_back() {
    let store = Arc::new(MemStore::new());
    let a = app_with(AuthConfig::open(), Arc::clone(&store));
    // The supervisors refuse to start once the server shuts down.
    a.state.manager.shutdown(Duration::from_secs(1)).await;
    let (status, body) = call(&a.router, "POST", "/syncs", &[], Some(sync_body("s"))).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("shutting_down")),
        "{body}"
    );
    assert!(store.list_syncs(None).await.unwrap().is_empty());
    let next = app_with(AuthConfig::open(), store);
    assert_eq!(boot_syncs(&next.state).await.unwrap(), 0);
}

// ------------------------------------------------------------- concurrency

/// A `PUT` that read the sync before a `DELETE` removed it must not start a
/// supervisor for the deleted sync.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_racing_delete_never_resurrects_the_sync() {
    let a = app(AuthConfig::open());
    let id = create(&a, &[], "s").await;
    let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    current["source"]["config"]["rows"] = json!(7);
    // The DELETE holds the sync in its teardown while the PUT comes in.
    a.probe.teardown_delay_ms.store(300, Ordering::SeqCst);
    let router = a.router.clone();
    let uri = format!("/syncs/{id}");
    let delete = tokio::spawn(async move { call(&router, "DELETE", &uri, &[], None).await.0 });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let (put, body) = call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}"),
        &[],
        Some(current),
    )
    .await;
    assert_eq!(delete.await.unwrap(), StatusCode::NO_CONTENT);
    assert_eq!(put, StatusCode::NOT_FOUND, "{body}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(a.state.manager.running_ids().is_empty());
    assert!(a.store.get_sync(None, &id).await.unwrap().is_none());
    assert!(
        meili_connect_core::StateStore::load(&*a.store, &id)
            .await
            .unwrap()
            .is_none()
    );
}

/// Whatever the interleaving of a `resume` and a `PUT`, the sync ends up
/// running the stored (new) spec.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_racing_put_runs_the_stored_spec() {
    let a = app(AuthConfig::open());
    let id = create(&a, &[], "s").await;
    for round in 0..15u64 {
        let (status, _) = call(&a.router, "POST", &format!("/syncs/{id}/pause"), &[], None).await;
        assert_eq!(status, StatusCode::OK);
        let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
        current["source"]["config"]["rows"] = json!(100 + round);
        let (r1, r2) = (a.router.clone(), a.router.clone());
        let resume_uri = format!("/syncs/{id}/resume");
        let put_uri = format!("/syncs/{id}");
        let resume = tokio::spawn(async move { call(&r1, "POST", &resume_uri, &[], None).await.0 });
        let put =
            tokio::spawn(async move { call(&r2, "PUT", &put_uri, &[], Some(current)).await.0 });
        assert_eq!(resume.await.unwrap(), StatusCode::OK);
        assert_eq!(put.await.unwrap(), StatusCode::OK);
        // The running sync must be on the new spec: resume it if the PUT
        // left it paused, then wait for its open.
        call(&a.router, "POST", &format!("/syncs/{id}/resume"), &[], None).await;
        let want = json!(100 + round);
        wait_for(async || {
            a.probe
                .last_opened
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|c| c["rows"] == want)
        })
        .await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let opened = a.probe.last_opened.lock().unwrap().clone().unwrap();
        assert_eq!(opened["rows"], want, "round {round}");
    }
}

// ------------------------------------------------------------- put

async fn written(a: &App, id: &str) -> u64 {
    let (_, st) = call(&a.router, "GET", &format!("/syncs/{id}/status"), &[], None).await;
    st["progress"]["written"].as_u64().unwrap()
}

async fn streaming(a: &App, id: &str) {
    let uri = format!("/syncs/{id}/status");
    wait_for(async || {
        let st = call(&a.router, "GET", &uri, &[], None).await.1;
        st["phase"] == "streaming" && st["has_checkpoint"] == true
    })
    .await;
}

async fn put(a: &App, id: &str, query: &str, body: Value) -> (StatusCode, Value) {
    call(
        &a.router,
        "PUT",
        &format!("/syncs/{id}{query}"),
        &[],
        Some(body),
    )
    .await
}

#[tokio::test]
async fn a_new_origin_is_torn_down_with_the_old_spec_and_synced_fresh() {
    let a = app(AuthConfig::open());
    let mut body = sync_body("s");
    body["source"]["config"]["host"] = json!("old-db");
    let (_, created) = call(&a.router, "POST", "/syncs", &[], Some(body.clone())).await;
    let id = created["id"].as_str().unwrap().to_owned();
    streaming(&a, &id).await;
    assert_eq!(written(&a, &id).await, 3);

    body["source"]["config"]["host"] = json!("new-db");
    body["source"]["config"]["rows"] = json!(5);
    let (status, resp) = put(&a, &id, "", body.clone()).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let torn: Vec<Value> = a.probe.torn_down.lock().unwrap().clone();
    assert_eq!(torn.len(), 1);
    assert_eq!(torn[0]["host"], "old-db", "torn down with the old spec");
    // The checkpoint was cleared: the new origin is snapshotted.
    wait_for(async || written(&a, &id).await == 8).await;
    streaming(&a, &id).await;

    // A teardown that fails changes nothing, unless forced.
    a.probe.teardown_fails.store(true, Ordering::SeqCst);
    body["source"]["config"]["host"] = json!("third-db");
    let (status, resp) = put(&a, &id, "", body.clone()).await;
    assert_eq!(
        (status, resp["code"].as_str()),
        (StatusCode::BAD_GATEWAY, Some("teardown_failed")),
        "{resp}"
    );
    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
    assert_eq!(stored.source.config["host"], "new-db");
    assert_eq!(a.state.manager.running_ids(), vec![id.clone()]);
    streaming(&a, &id).await;
    let (status, _) = put(&a, &id, "?force=true", body).await;
    assert_eq!(status, StatusCode::OK);
    let stored = a
        .store
        .get_sync(None, &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap()
        .spec;
    assert_eq!(stored.source.config["host"], "third-db");
}

#[tokio::test]
async fn changed_streams_are_backfilled_and_a_rename_only_restarts() {
    let a = app(AuthConfig::open());
    let id = create(&a, &[], "s").await;
    streaming(&a, &id).await;
    assert_eq!(written(&a, &id).await, 3);
    let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;

    // Name and guard only: no new snapshot, no teardown.
    current["name"] = json!("renamed");
    current["mass_delete_guard"] = json!(0.9);
    let (status, _) = put(&a, &id, "", current.clone()).await;
    assert_eq!(status, StatusCode::OK);
    streaming(&a, &id).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(written(&a, &id).await, 3);

    // Another index, primary key or field selection: everything again.
    current["streams"][0]["index"] = json!("items_v2");
    current["streams"][0]["fields"] = json!({"include": ["n"]});
    let (status, _) = put(&a, &id, "", current.clone()).await;
    assert_eq!(status, StatusCode::OK);
    wait_for(async || written(&a, &id).await == 6).await;
    // A new collection too.
    current["streams"]
        .as_array_mut()
        .unwrap()
        .push(json!({"collection": "public.other", "index": "other"}));
    let (status, _) = put(&a, &id, "", current).await;
    assert_eq!(status, StatusCode::OK);
    wait_for(async || written(&a, &id).await == 9).await;
    assert_eq!(a.probe.teardowns.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn put_on_a_paused_sync_leaves_it_paused() {
    let a = app(AuthConfig::open());
    let id = create(&a, &[], "s").await;
    streaming(&a, &id).await;
    call(&a.router, "POST", &format!("/syncs/{id}/pause"), &[], None).await;
    let (_, mut current) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    for edit in ["name", "streams"] {
        if edit == "name" {
            current["name"] = json!("still paused");
        } else {
            current["streams"][0]["index"] = json!("elsewhere");
        }
        let (status, resp) = put(&a, &id, "", current.clone()).await;
        assert_eq!(status, StatusCode::OK, "{resp}");
        let (_, st) = call(&a.router, "GET", &format!("/syncs/{id}/status"), &[], None).await;
        assert_eq!(
            (st["phase"].as_str(), st["running"].as_bool()),
            (Some("paused"), Some(false)),
            "{edit}: {st}"
        );
    }
    // Resuming runs the new spec from scratch (its streams changed).
    call(&a.router, "POST", &format!("/syncs/{id}/resume"), &[], None).await;
    wait_for(async || written(&a, &id).await == 6).await;
}

// ------------------------------------------------------------- lifecycle

#[tokio::test]
async fn delete_tears_down_and_keeps_the_sync_when_teardown_fails() {
    let a = app(AuthConfig::open());
    let id = create(&a, &[], "s").await;
    a.probe.teardown_fails.store(true, Ordering::SeqCst);
    let (status, body) = call(&a.router, "DELETE", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(
        (status, body["code"].as_str()),
        (StatusCode::BAD_GATEWAY, Some("teardown_failed"))
    );
    let (status, _) = call(&a.router, "GET", &format!("/syncs/{id}"), &[], None).await;
    assert_eq!(status, StatusCode::OK, "kept for a retry");
    // Kept stopped, durably: paused with the reason, not started at boot.
    let (_, st) = call(&a.router, "GET", &format!("/syncs/{id}/status"), &[], None).await;
    assert_eq!(
        (st["phase"].as_str(), st["running"].as_bool()),
        (Some("paused"), Some(false)),
        "{st}"
    );
    assert!(
        st["message"].as_str().unwrap().contains("deletion failed"),
        "{st}"
    );
    let rebooted = app_with(AuthConfig::open(), Arc::clone(&a.store));
    assert_eq!(boot_syncs(&rebooted.state).await.unwrap(), 0);
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
    let a = app(AuthConfig::open());
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
    // The metered units: a first sync is a snapshot.
    assert_eq!(st["documents"]["documents_snapshot"], 3);
    assert_eq!(st["documents"]["documents_streamed"], 0);
    assert_eq!(st["documents"]["documents_resync"], 0);
    assert_eq!(st["bytes"], 3 * FAKE_BYTES_PER_DOC);
    assert_eq!(st["dead_letters"], 0);
    // The fake source reports its lag while streaming.
    assert_eq!(st["lag"], json!({"seconds": 1.5, "bytes": 2048}));
    assert!(st["lag_warning"].is_null(), "the guard is off");

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
    let a = app(AuthConfig::open());
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
    let first = app_with(AuthConfig::open(), Arc::clone(&store));
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

    let second = app_with(AuthConfig::open(), store);
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

#[tokio::test]
async fn boot_keeps_a_sync_held_for_credits_until_they_are_back() {
    let store = Arc::new(MemStore::new());
    let first = app_with(locked(), Arc::clone(&store));
    let id = create(&first, &lab(TEAM), "s").await;
    first.state.manager.shutdown(Duration::from_secs(5)).await;
    let mut state = store.load(&id).await.unwrap().unwrap();
    state.phase = SyncPhase::Held;
    state.message = Some("out_of_credits".into());
    store.save(&state).await.unwrap();

    let lab_server = wiremock::MockServer::start().await;
    lab_answers(&lab_server, true).await;
    let second = app_tweaked(
        locked(),
        store,
        HostPolicy::any(),
        Limits::default(),
        Duration::from_millis(200),
        |w| {
            w.credits = Some(Arc::new(
                crate::credits::CreditCache::new(crate::config::LabEventsConfig {
                    url: lab_server.uri(),
                    instance_id: "inst".into(),
                    instance_secret: "secret".into(),
                })
                .unwrap(),
            ))
        },
    );
    assert_eq!(boot_syncs(&second.state).await.unwrap(), 0);
    assert!(second.state.manager.is_credit_held(&id));
    second.state.manager.enforce_credits().await;
    wait_for(async || status_of(&second, &id).await["phase"] == "streaming").await;
}

// ------------------------------------------------------------- usage

#[tokio::test]
async fn engine_progress_becomes_usage_events_for_tenant_syncs() {
    let a = app(locked());
    let id = create(&a, &lab("team-a"), "s").await;
    let global = create(&a, &admin(), "g").await;
    let usage = Arc::clone(a.state.manager.usage());
    let snapshot = async |id: &str| -> u64 {
        a.store
            .progress(None, id, chrono::NaiveDate::MIN, chrono::NaiveDate::MAX)
            .await
            .map_or(0, |d| d.iter().map(|d| d.usage.documents_snapshot).sum())
    };
    // Both ran their snapshot: the tenant's is pending for the Lab, the
    // global one never is.
    wait_for(async || {
        a.store
            .pending_usage(Lane::Lab)
            .await
            .unwrap()
            .iter()
            .any(|p| p.sync_id == id && p.usage.documents_snapshot == 3)
            && a.store
                .progress(
                    Some("team-a"),
                    &id,
                    chrono::NaiveDate::MIN,
                    chrono::NaiveDate::MAX,
                )
                .await
                .is_ok()
    })
    .await;
    wait_for(async || snapshot(&global).await == 3).await;
    assert_eq!(a.store.pending_usage(Lane::Lab).await.unwrap().len(), 1);
    assert_eq!(
        usage.flush(&*a.store).await.unwrap(),
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
    assert_eq!(e["data"]["units"]["documents_snapshot"], 3);
    assert_eq!(e["data"]["units"]["documents_streamed"], 0);
    assert_eq!(e["data"]["units"]["bytes"], 3 * FAKE_BYTES_PER_DOC);
    assert_eq!(e["data"]["description"], "postgres → items");

    // Deleting a sync enqueues what it had not reported yet.
    a.store
        .record_progress(
            &id,
            &meili_connect_core::Progress {
                written: 2,
                usage: meili_connect_core::Usage {
                    documents_streamed: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
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
    assert_eq!(
        lease.events[0].payload["data"]["units"]["documents_streamed"],
        2
    );
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
    let a = app(AuthConfig::open());
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

// ------------------------------------------------------------- credits

const TEAM: &str = "11111111-2222-3333-4444-555555555555";

/// The fake Lab answers the account lookup with `can_spend`.
async fn lab_answers(server: &wiremock::MockServer, can_spend: bool) {
    use wiremock::matchers::{method, path};
    server.reset().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{TEAM}")))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_json(json!({"active": true, "can_spend": can_spend})),
        )
        .mount(server)
        .await;
}

fn app_with_credits(server: &wiremock::MockServer) -> App {
    let credits = crate::credits::CreditCache::new(crate::config::LabEventsConfig {
        url: server.uri(),
        instance_id: "inst".into(),
        instance_secret: "secret".into(),
    })
    .unwrap()
    // Always ask: the test flips the answer.
    .with_windows(Duration::ZERO, Duration::from_secs(300));
    app_tweaked(
        locked(),
        Arc::new(MemStore::new()),
        HostPolicy::any(),
        Limits::default(),
        Duration::from_millis(200),
        |w| w.credits = Some(Arc::new(credits)),
    )
}

async fn status_of(a: &App, id: &str) -> Value {
    call(
        &a.router,
        "GET",
        &format!("/syncs/{id}/status"),
        &lab(TEAM),
        None,
    )
    .await
    .1
}

#[tokio::test]
async fn a_team_out_of_credits_is_held_and_resumes_with_its_credits() {
    let lab_server = wiremock::MockServer::start().await;
    lab_answers(&lab_server, true).await;
    let a = app_with_credits(&lab_server);
    let id = create(&a, &lab(TEAM), "s").await;
    wait_for(async || status_of(&a, &id).await["phase"] == "streaming").await;
    let other_team = create(&a, &lab("team-b"), "other").await;

    // The Lab says the team cannot spend: its running syncs are held,
    // nothing torn down; another team is untouched.
    lab_answers(&lab_server, false).await;
    a.state.manager.enforce_credits().await;
    let st = status_of(&a, &id).await;
    assert_eq!(
        (
            st["phase"].as_str(),
            st["message"].as_str(),
            st["running"].as_bool()
        ),
        (Some("held"), Some("out_of_credits"), Some(false)),
        "{st}"
    );
    assert_eq!(a.probe.teardowns.load(Ordering::SeqCst), 0);
    let (_, other) = call(
        &a.router,
        "GET",
        &format!("/syncs/{other_team}/status"),
        &lab("team-b"),
        None,
    )
    .await;
    assert_eq!(other["running"], true);

    // Releasing, resuming or resyncing it is refused with a clear code.
    for action in ["release", "resume", "resync"] {
        let (status, body) = call(
            &a.router,
            "POST",
            &format!("/syncs/{id}/{action}"),
            &lab(TEAM),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{action}: {body}");
        assert_eq!(body["code"], "out_of_credits");
    }
    // A new sync can be created; it starts held.
    let new = create(&a, &lab(TEAM), "new").await;
    let st = status_of(&a, &new).await;
    assert_eq!(
        (st["phase"].as_str(), st["message"].as_str()),
        (Some("held"), Some("out_of_credits"))
    );

    // Credits are back: both resume by themselves.
    lab_answers(&lab_server, true).await;
    a.state.manager.enforce_credits().await;
    for sync in [&id, &new] {
        wait_for(async || status_of(&a, sync).await["phase"] == "streaming").await;
        let st = status_of(&a, sync).await;
        assert_eq!(st["running"], true);
        assert!(st["message"].is_null(), "{st}");
    }
}

#[tokio::test]
async fn a_paused_sync_stays_paused_through_a_credit_hold() {
    let lab_server = wiremock::MockServer::start().await;
    lab_answers(&lab_server, true).await;
    let a = app_with_credits(&lab_server);
    let id = create(&a, &lab(TEAM), "s").await;
    wait_for(async || status_of(&a, &id).await["phase"] == "streaming").await;
    lab_answers(&lab_server, false).await;
    a.state.manager.enforce_credits().await;
    assert!(a.state.manager.is_credit_held(&id));
    let (status, _) = call(
        &a.router,
        "POST",
        &format!("/syncs/{id}/pause"),
        &lab(TEAM),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    lab_answers(&lab_server, true).await;
    a.state.manager.enforce_credits().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(status_of(&a, &id).await["phase"], "paused");
}

#[tokio::test]
async fn without_the_lab_credits_are_never_checked() {
    let a = app(locked());
    let id = create(&a, &lab(TEAM), "s").await;
    a.state.manager.enforce_credits().await;
    wait_for(async || status_of(&a, &id).await["phase"] == "streaming").await;
}

// ------------------------------------------------------------- metrics

#[tokio::test]
async fn metrics_take_only_the_metrics_token() {
    let a = app(locked());
    let id = create(&a, &lab("team-a"), "s").await;
    wait_for(async || {
        call(
            &a.router,
            "GET",
            &format!("/syncs/{id}/status"),
            &lab("team-a"),
            None,
        )
        .await
        .1["phase"]
            == "streaming"
    })
    .await;
    let bearer = |t: &str| vec![("authorization", format!("Bearer {t}"))];
    for headers in [vec![], bearer("wrong"), bearer(ADMIN), lab("team-a")] {
        let (status, body) = call(&a.router, "GET", "/metrics", &headers, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{headers:?}");
        assert_eq!(body["code"], "unauthorized");
    }
    let req = Request::builder()
        .uri("/metrics")
        .header("authorization", format!("Bearer {METRICS}"))
        .body(Body::empty())
        .unwrap();
    let resp = a.router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    let text = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    for name in [
        "connect_syncs",
        "connect_documents_total",
        "connect_bytes_total",
        "connect_batch_seconds",
        "connect_sink_errors_total",
        "connect_source_errors_total",
        "connect_lag_seconds",
        "connect_dead_letters_total",
        "connect_outbox_backlog",
        "connect_lab_events_sent_total",
    ] {
        assert!(
            text.contains(&format!("# TYPE {name} ")),
            "{name} not declared in\n{text}"
        );
    }
    assert!(
        text.contains("connect_syncs{phase=\"streaming\"} 1"),
        "{text}"
    );
    assert!(
        text.contains("connect_documents_total{kind=\"snapshot\"} 3"),
        "{text}"
    );
    assert!(text.contains(&format!("connect_bytes_total {}", 3 * FAKE_BYTES_PER_DOC)));
    assert!(text.contains("connect_batch_seconds_count 1"), "{text}");
    // One running sync reporting 1.5 s of lag.
    assert!(text.contains("connect_lag_seconds_count 1"), "{text}");
    assert!(
        text.contains("connect_lag_seconds_bucket{le=\"5\"} 1"),
        "{text}"
    );
    // Never a tenant or a sync id.
    assert!(!text.contains("team-a") && !text.contains(&id));

    // Without METRICS_TOKEN the route does not exist for anyone.
    let off = app_tweaked(
        locked(),
        Arc::new(MemStore::new()),
        HostPolicy::any(),
        Limits::default(),
        Duration::from_millis(200),
        |w| w.metrics_token = None,
    );
    for headers in [vec![], bearer(METRICS), bearer(ADMIN)] {
        let (status, _) = call(&off.router, "GET", "/metrics", &headers, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}

// ------------------------------------------------------------- GET /usage

async fn snapshot_done(a: &App, headers: &[(&'static str, String)], id: &str) {
    wait_for(async || {
        call(
            &a.router,
            "GET",
            &format!("/syncs/{id}/status"),
            headers,
            None,
        )
        .await
        .1["documents"]["documents_snapshot"]
            == 3
    })
    .await;
}

#[tokio::test]
async fn usage_is_per_day_or_per_sync_and_scoped_to_the_tenant() {
    let a = app(locked());
    let s1 = create(&a, &lab("team-a"), "first").await;
    let s2 = create(&a, &lab("team-a"), "second").await;
    let other = create(&a, &lab("team-b"), "theirs").await;
    for id in [&s1, &s2] {
        snapshot_done(&a, &lab("team-a"), id).await;
    }
    snapshot_done(&a, &lab("team-b"), &other).await;
    let today = chrono::Utc::now().date_naive();

    let (status, body) = call(&a.router, "GET", "/usage", &lab("team-a"), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source"], "state");
    assert_eq!(body["group_by"], "day");
    let units = json!({
        "documents_streamed": 0,
        "documents_snapshot": 6,
        "documents_resync": 0,
        "bytes": 6 * FAKE_BYTES_PER_DOC,
        "sync_seconds": body["totals"]["sync_seconds"],
    });
    assert_eq!(body["totals"], units);
    assert_eq!(
        body["rows"],
        json!([{ "day": today.to_string(), "units": units }])
    );

    let (_, by_sync) = call(
        &a.router,
        "GET",
        &format!("/usage?group_by=sync&from={today}&to={today}"),
        &lab("team-a"),
        None,
    )
    .await;
    let rows = by_sync["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{by_sync}");
    let mut seen: Vec<(String, String, u64)> = rows
        .iter()
        .map(|r| {
            (
                r["sync_id"].as_str().unwrap().to_owned(),
                r["name"].as_str().unwrap().to_owned(),
                r["units"]["documents_snapshot"].as_u64().unwrap(),
            )
        })
        .collect();
    seen.sort();
    let mut expected = vec![(s1, "first".to_owned(), 3), (s2, "second".to_owned(), 3)];
    expected.sort();
    assert_eq!(seen, expected);
    assert_eq!(by_sync["totals"]["documents_snapshot"], 6);

    // Another tenant sees its own only.
    let (_, theirs) = call(&a.router, "GET", "/usage", &lab("team-b"), None).await;
    assert_eq!(theirs["totals"]["documents_snapshot"], 3);
    // A range without activity is empty, with zero totals.
    let (_, empty) = call(
        &a.router,
        "GET",
        "/usage?from=2020-01-01&to=2020-01-31",
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(empty["rows"], json!([]));
    assert_eq!(empty["totals"]["documents_snapshot"], 0);
}

#[tokio::test]
async fn usage_validates_its_range() {
    let a = app(locked());
    for query in [
        "from=2026-02-01&to=2026-01-01",
        "from=2025-01-01&to=2026-01-02",
        "group_by=week",
        "from=yesterday",
    ] {
        let (status, body) = call(
            &a.router,
            "GET",
            &format!("/usage?{query}"),
            &lab("team-a"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["code"], "invalid_request");
    }
    // Exactly 366 days is allowed.
    let (status, _) = call(
        &a.router,
        "GET",
        "/usage?from=2025-01-01&to=2026-01-01",
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn usage_comes_from_tinybird_when_configured_with_the_same_shape() {
    use wiremock::matchers::{method, path, query_param};
    let tb = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path("/v0/pipes/connect_tenant_usage.json"))
        .and(query_param("tenant_id", "team-a"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"day": "2026-11-17", "sync_id": "s1", "documents_streamed": 10,
                 "documents_snapshot": 0, "documents_resync": 0, "bytes": 1000, "sync_seconds": 60},
                {"day": "2026-11-18", "sync_id": "s1", "documents_streamed": 5,
                 "documents_snapshot": 2, "documents_resync": 0, "bytes": 700, "sync_seconds": 60},
                {"day": "2026-11-18", "sync_id": "s2", "documents_streamed": 0,
                 "documents_snapshot": 0, "documents_resync": 4, "bytes": 400, "sync_seconds": 30}
            ]
        })))
        .mount(&tb)
        .await;
    let a = app_tweaked(
        locked(),
        Arc::new(MemStore::new()),
        HostPolicy::any(),
        Limits::default(),
        Duration::from_millis(200),
        |w| {
            w.tinybird = Some(crate::tinybird::TinybirdConfig {
                url: tb.uri(),
                token: "tb".into(),
            })
        },
    );
    let (_, body) = call(
        &a.router,
        "GET",
        "/usage?from=2026-11-01&to=2026-11-30",
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(body["source"], "tinybird");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(body["rows"][1]["day"], "2026-11-18");
    assert_eq!(body["rows"][1]["units"]["documents_streamed"], 5);
    assert_eq!(body["rows"][1]["units"]["documents_resync"], 4);
    assert_eq!(
        body["totals"],
        json!({"documents_streamed": 15, "documents_snapshot": 2, "documents_resync": 4,
               "bytes": 2100, "sync_seconds": 150})
    );
    let (_, by_sync) = call(
        &a.router,
        "GET",
        "/usage?from=2026-11-01&to=2026-11-30&group_by=sync",
        &lab("team-a"),
        None,
    )
    .await;
    assert_eq!(by_sync["rows"][0]["sync_id"], "s1");
    assert_eq!(by_sync["rows"][0]["units"]["documents_streamed"], 15);
    assert!(by_sync["rows"][0]["name"].is_null(), "unknown here");

    // Tinybird down: the state database answers, same shape.
    tb.reset().await;
    let (status, body) = call(&a.router, "GET", "/usage", &lab("team-a"), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "state");
    assert!(body["totals"].is_object());
}

// ------------------------------------------------------------- ambient identity

fn dynamo_body(name: &str, credentials: Value) -> Value {
    json!({
        "name": name,
        "source": {"kind": "dynamodb", "config": {"region": "eu-west-1", "credentials": credentials}},
        "destination": {"url": "http://meili:7700", "api_key": "k"},
        "streams": [{"collection": "orders", "index": "orders"}],
    })
}

fn hosted_app(store: Arc<MemStore>) -> App {
    app_tweaked(
        locked(),
        store,
        HostPolicy::any(),
        Limits::default(),
        Duration::from_millis(200),
        |w| w.hosted = true,
    )
}

#[tokio::test]
async fn a_tenant_never_borrows_the_servers_own_identity_on_a_hosted_server() {
    let a = hosted_app(Arc::new(MemStore::new()));
    let default = json!({"type": "default"});
    let (status, body) = call(
        &a.router,
        "POST",
        "/syncs",
        &lab("team-a"),
        Some(dynamo_body("d", default.clone())),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "ambient_credentials");
    assert!(body["message"].as_str().unwrap().contains("assume_role"));
    // Neither may it probe with them (listing the server's own tables).
    for route in ["/sources/check", "/sources/discover"] {
        let (status, body) = call(
            &a.router,
            "POST",
            route,
            &lab("team-a"),
            Some(json!({"source": {"kind": "dynamodb", "config": {"region": "eu-west-1"}}})),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{route}: {body}");
        assert_eq!(body["code"], "ambient_credentials");
    }
    // A MongoDB URI reading a local file, for the admin acting for a tenant.
    let mut tenant_admin = admin();
    tenant_admin.push((H_TENANT_ID, "team-a".to_owned()));
    let (status, body) = call(
        &a.router,
        "POST",
        "/syncs",
        &tenant_admin,
        Some(json!({
            "name": "m",
            "source": {"kind": "mongodb", "config": {"uri": "mongodb://h/?tlsCAFile=/etc/passwd"}},
            "destination": {"url": "http://meili:7700", "api_key": "k"},
            "streams": [{"collection": "db.c", "index": "c"}],
        })),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    // A role to assume is fine, and the standalone admin keeps `default`.
    let (status, _) = call(
        &a.router,
        "POST",
        "/syncs",
        &lab("team-a"),
        Some(dynamo_body(
            "r",
            json!({"type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/r"}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        &a.router,
        "POST",
        "/syncs",
        &admin(),
        Some(dynamo_body("g", default.clone())),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Standalone (not hosted): allowed for tenants too.
    let standalone = app(locked());
    let (status, _) = call(
        &standalone.router,
        "POST",
        "/syncs",
        &lab("team-a"),
        Some(dynamo_body("d", default)),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn a_stored_tenant_sync_with_the_servers_identity_is_held_on_a_hosted_server() {
    let store = Arc::new(MemStore::new());
    let before = app(locked());
    let id = {
        let (status, body) = call(
            &before.router,
            "POST",
            "/syncs",
            &lab("team-a"),
            Some(dynamo_body("d", json!({"type": "default"}))),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        body["id"].as_str().unwrap().to_owned()
    };
    // Same row, now on a hosted server.
    let row = before
        .store
        .get_sync(Some("team-a"), &id)
        .await
        .unwrap()
        .unwrap()
        .readable()
        .unwrap();
    before.state.manager.shutdown(Duration::from_secs(5)).await;
    store.create_sync(Some("team-a"), &row.spec).await.unwrap();
    let a = hosted_app(Arc::clone(&store));
    boot_syncs(&a.state).await.unwrap();
    wait_for(async || {
        call(
            &a.router,
            "GET",
            &format!("/syncs/{id}/status"),
            &lab("team-a"),
            None,
        )
        .await
        .1["phase"]
            == "held"
    })
    .await;
    assert_eq!(
        a.probe.last_opened.lock().unwrap().as_ref(),
        None,
        "never opened"
    );
}
