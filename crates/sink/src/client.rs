//! Thin HTTP layer over the Meilisearch API: requests, error mapping, task
//! polling. Never logs the API key (sent as a sensitive header) nor request
//! bodies.

use std::hash::{BuildHasher as _, Hasher as _};
use std::io::Write as _;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use meili_connect_core::{HostPolicy, MeiliDestination, SinkError};
use reqwest::header::{
    AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER,
};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;

use crate::SinkOptions;

/// Bodies above this size are gzipped on the blocking pool.
const INLINE_GZIP_LIMIT: usize = 64 * 1024;

/// Resolves the destination's name through the [`HostPolicy`] and hands
/// hyper only the vetted addresses, so the connection goes where the policy
/// looked (no second lookup for a DNS rebinding to change).
struct PolicyResolver {
    policy: Arc<HostPolicy>,
    /// The destination URL's port: the client only ever talks to it.
    port: u16,
}

impl reqwest::dns::Resolve for PolicyResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = Arc::clone(&self.policy);
        let host = name.as_str().to_owned();
        let port = self.port;
        Box::pin(async move {
            let addrs = policy.resolve(&host, port).await.map_err(|e| {
                tracing::warn!(error = %e, "Meilisearch destination refused by the host policy");
                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
            })?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Meilisearch's error object (`{message, code, type, link}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub(crate) struct MeiliError {
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub code: String,
    #[serde(rename = "type", default)]
    pub kind: String,
}

impl std::fmt::Display for MeiliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.code.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{} ({})", self.message, self.code)
        }
    }
}

/// A non-2xx reply.
#[derive(Debug, Clone)]
pub(crate) struct ApiError {
    pub status: StatusCode,
    pub error: MeiliError,
    /// `Retry-After` in seconds, when the server sent one.
    pub retry_after: Option<Duration>,
}

/// Which failures of one request are resent before surfacing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retry {
    /// Safe to repeat: 408, 429, 5xx and transport errors are retried.
    Idempotent,
    /// Repeating may apply it twice (an index swap swaps back): only
    /// failures to connect, where nothing reached the server, are retried.
    ConnectOnly,
    /// The caller has its own retry loop.
    Never,
}

/// A request body, serialized (and compressed) once and resent as is.
struct Prepared {
    body: Bytes,
    content_type: &'static str,
    gzip: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum TaskStatus {
    Enqueued,
    Processing,
    Succeeded,
    Failed,
    Canceled,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Task {
    pub uid: u64,
    pub status: TaskStatus,
    #[serde(default)]
    pub error: Option<MeiliError>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Enqueued {
    task_uid: u64,
}

pub(crate) enum Payload {
    Json(serde_json::Value),
    /// Already serialized body with its content type.
    Raw {
        body: Vec<u8>,
        content_type: &'static str,
    },
}

/// Outcome of one call: either a 2xx body or Meilisearch's error.
pub(crate) type Reply = Result<Bytes, ApiError>;

pub(crate) struct Client {
    http: reqwest::Client,
    base: Url,
    pub opts: SinkOptions,
}

impl Client {
    pub fn new(destination: &MeiliDestination, opts: SinkOptions) -> Result<Self, SinkError> {
        let base = Url::parse(destination.url.trim())
            .map_err(|e| SinkError::Config(format!("invalid Meilisearch URL: {e}")))?;
        if base.cannot_be_a_base() || !matches!(base.scheme(), "http" | "https") {
            return Err(SinkError::Config(
                "invalid Meilisearch URL: expected http(s)://host[:port][/prefix]".into(),
            ));
        }
        let mut headers = HeaderMap::new();
        if !destination.api_key.is_empty() {
            let mut value =
                HeaderValue::from_str(&format!("Bearer {}", destination.api_key.expose()))
                    .map_err(|_| {
                        SinkError::Config("Meilisearch API key has invalid characters".into())
                    })?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let policy = Arc::clone(&opts.host_policy);
        let port = base.port_or_known_default().unwrap_or(80);
        let host = base
            .host_str()
            .ok_or_else(|| SinkError::Config("invalid Meilisearch URL: no host".into()))?;
        // hyper connects to a literal address without asking the resolver:
        // vet it here, once (the URL never changes).
        if let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        {
            policy
                .check_ip(ip, Some(port))
                .map_err(|e| SinkError::Config(format!("Meilisearch URL: {e}")))?;
        }
        let mut builder = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(opts.request_timeout)
            .connect_timeout(opts.connect_timeout.min(opts.request_timeout))
            .user_agent(concat!("meili-connect/", env!("CARGO_PKG_VERSION")))
            // A redirect would send the API key and the documents to a host
            // nobody vetted.
            .redirect(reqwest::redirect::Policy::none());
        if !policy.is_any() {
            // A proxy would resolve the name itself.
            builder = builder
                .no_proxy()
                .dns_resolver(Arc::new(PolicyResolver { policy, port }));
        }
        let http = builder
            .build()
            .map_err(|e| SinkError::Config(format!("cannot build HTTP client: {e}")))?;
        Ok(Self { http, base, opts })
    }

    fn url(&self, segments: &[&str]) -> Result<Url, SinkError> {
        let mut url = self.base.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|()| SinkError::Config("invalid Meilisearch URL".into()))?;
            path.pop_if_empty();
            path.extend(segments);
        }
        Ok(url)
    }

    /// Sends one idempotent request, see [`Client::call_with`].
    pub async fn call(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Reply, SinkError> {
        self.call_with(Retry::Idempotent, method, segments, query, payload)
            .await
    }

    /// Sends one request, resending it per `retry` up to `request_retries`
    /// times with jittered exponential backoff (or the server's
    /// `Retry-After`, capped at `max_request_backoff`). Transport failures
    /// are `Err(SinkError::Transient)`; HTTP failures come back as
    /// `Ok(Err(ApiError))` for the caller to map.
    pub async fn call_with(
        &self,
        retry: Retry,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Reply, SinkError> {
        let url = self.url(segments)?;
        let path = segments.join("/");
        let prepared = match payload {
            None => None,
            Some(Payload::Json(value)) => Some(Prepared {
                body: serde_json::to_vec(&value)
                    .map_err(|e| SinkError::Fatal(format!("cannot serialize request: {e}")))?
                    .into(),
                content_type: "application/json",
                gzip: false,
            }),
            Some(Payload::Raw { body, content_type }) => Some(if self.opts.gzip {
                Prepared {
                    body: gzip(body).await?.into(),
                    content_type,
                    gzip: true,
                }
            } else {
                Prepared {
                    body: body.into(),
                    content_type,
                    gzip: false,
                }
            }),
        };
        let max_delay = self.opts.max_request_backoff;
        let mut delay = self.opts.request_backoff.min(max_delay);
        let mut attempt = 0;
        loop {
            let outcome = self
                .send_once(method.clone(), url.clone(), query, prepared.as_ref())
                .await;
            let wait = match &outcome {
                Err((_, connect)) if retry == Retry::Idempotent || *connect => None,
                Ok(Err(api)) if retry == Retry::Idempotent && retryable_status(api.status) => {
                    api.retry_after
                }
                _ => return outcome.map_err(|(e, _)| transport_error(&path, e)),
            };
            if attempt >= self.opts.request_retries || retry == Retry::Never {
                return outcome.map_err(|(e, _)| transport_error(&path, e));
            }
            attempt += 1;
            let wait = wait.unwrap_or_else(|| jitter(delay)).min(max_delay);
            tracing::debug!(%path, attempt, ?wait, "retrying a failed request");
            tokio::time::sleep(wait).await;
            delay = delay.saturating_mul(2).min(max_delay);
        }
    }

    /// One attempt. A transport error comes with whether it failed to
    /// connect (nothing reached the server).
    async fn send_once(
        &self,
        method: Method,
        url: Url,
        query: &[(&str, &str)],
        prepared: Option<&Prepared>,
    ) -> Result<Reply, (reqwest::Error, bool)> {
        let mut req = self.http.request(method, url);
        if !query.is_empty() {
            req = req.query(query);
        }
        if let Some(p) = prepared {
            req = req
                .header(CONTENT_TYPE, p.content_type)
                .body(p.body.clone());
            if p.gzip {
                req = req.header(CONTENT_ENCODING, "gzip");
            }
        }
        let resp = req.send().await.map_err(|e| {
            let connect = e.is_connect();
            (e, connect)
        })?;
        let status = resp.status();
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let body = resp.bytes().await.map_err(|e| (e, false))?;
        if status.is_success() {
            return Ok(Ok(body));
        }
        // A body that is not Meilisearch's error object is never echoed: the
        // URL may point at something else, whose replies must not reach the
        // tenant through error messages.
        let error = serde_json::from_slice::<MeiliError>(&body).unwrap_or_else(|_| MeiliError {
            message: format!("HTTP {status}: the reply is not a Meilisearch error"),
            ..MeiliError::default()
        });
        Ok(Err(ApiError {
            status,
            error,
            retry_after,
        }))
    }

    /// An idempotent `call` for routes that enqueue a task; returns the
    /// task uid.
    pub async fn enqueue(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Result<u64, ApiError>, SinkError> {
        self.enqueue_with(Retry::Idempotent, method, segments, query, payload)
            .await
    }

    pub async fn enqueue_with(
        &self,
        retry: Retry,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Result<u64, ApiError>, SinkError> {
        match self
            .call_with(retry, method, segments, query, payload)
            .await?
        {
            Ok(body) => {
                let e: Enqueued = serde_json::from_slice(&body).map_err(|e| {
                    SinkError::Fatal(format!("unexpected reply from Meilisearch: {e}"))
                })?;
                Ok(Ok(e.task_uid))
            }
            Err(e) => Ok(Err(e)),
        }
    }

    /// Polls `GET /tasks/{uid}` until the task is terminal, backing off from
    /// `poll_interval` to `max_poll_interval`. Transient polling errors are
    /// retried until `task_timeout`.
    pub async fn wait_task(&self, uid: u64) -> Result<Task, SinkError> {
        let deadline = Instant::now() + self.opts.task_timeout;
        let mut delay = self.opts.poll_interval;
        let uid_s = uid.to_string();
        loop {
            let last_error = match self
                .call_with(Retry::Never, Method::GET, &["tasks", &uid_s], &[], None)
                .await
            {
                Ok(Ok(body)) => {
                    let task: Task = serde_json::from_slice(&body).map_err(|e| {
                        SinkError::Fatal(format!("unexpected task from Meilisearch: {e}"))
                    })?;
                    match task.status {
                        TaskStatus::Succeeded | TaskStatus::Failed | TaskStatus::Canceled => {
                            return Ok(task);
                        }
                        _ => None,
                    }
                }
                Ok(Err(api)) => match map_api_error("polling task", &api) {
                    SinkError::Transient(m) => Some(m),
                    other => return Err(other),
                },
                Err(SinkError::Transient(m)) => Some(m),
                Err(other) => return Err(other),
            };
            let now = Instant::now();
            if now >= deadline {
                let why = last_error.map(|m| format!(": {m}")).unwrap_or_default();
                return Err(SinkError::Transient(format!(
                    "task {uid} not finished after {:?}{why}",
                    self.opts.task_timeout
                )));
            }
            tokio::time::sleep(delay.min(deadline - now)).await;
            delay = (delay * 2).min(self.opts.max_poll_interval.max(self.opts.poll_interval));
        }
    }

    /// Enqueue then wait; a failed task becomes an error via
    /// [`map_task_error`]. Used by the index-level operations.
    pub async fn run_task(
        &self,
        context: &str,
        retry: Retry,
        method: Method,
        segments: &[&str],
        payload: Option<Payload>,
    ) -> Result<Task, SinkError> {
        let uid = match self
            .enqueue_with(retry, method, segments, &[], payload)
            .await?
        {
            Ok(uid) => uid,
            Err(api) => return Err(map_api_error(context, &api)),
        };
        self.wait_task(uid).await
    }
}

fn transport_error(path: &str, e: reqwest::Error) -> SinkError {
    let e = e.without_url();
    if e.is_builder() {
        SinkError::Config(format!("{path}: {e}"))
    } else {
        // Timeouts, connection refused/reset, DNS, truncated bodies.
        SinkError::Transient(format!("{path}: {e}"))
    }
}

async fn gzip(body: Vec<u8>) -> Result<Vec<u8>, SinkError> {
    fn encode(body: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut enc = flate2::write::GzEncoder::new(
            Vec::with_capacity(body.len() / 4),
            flate2::Compression::fast(),
        );
        enc.write_all(body)?;
        enc.finish()
    }
    let out = if body.len() <= INLINE_GZIP_LIMIT {
        encode(&body)
    } else {
        tokio::task::spawn_blocking(move || encode(&body))
            .await
            .map_err(|e| SinkError::Fatal(format!("gzip task panicked: {e}")))?
    };
    out.map_err(|e| SinkError::Fatal(format!("gzip failed: {e}")))
}

/// Statuses worth resending an idempotent request for.
fn retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

/// A delay drawn uniformly in `[d/2, d]`, so the retries of many clients
/// spread out.
pub(crate) fn jitter(d: Duration) -> Duration {
    // A fresh RandomState is randomly keyed: enough randomness for this.
    let r = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let half = d / 2;
    let span = u64::try_from((d - half).as_nanos()).unwrap_or(u64::MAX);
    half + Duration::from_nanos(r % span.saturating_add(1))
}

fn is_config_code(code: &str) -> bool {
    code.starts_with("index_primary_key_")
        || matches!(
            code,
            "invalid_api_key" | "missing_authorization_header" | "invalid_index_primary_key"
        )
}

/// HTTP status + Meilisearch error → the engine's reaction.
pub(crate) fn map_api_error(context: &str, api: &ApiError) -> SinkError {
    let msg = format!("{context}: {}", api.error);
    let s = api.status.as_u16();
    if s == 401 || s == 403 || is_config_code(&api.error.code) || api.error.kind == "auth" {
        SinkError::Config(msg)
    } else if s == 408 || s == 429 || api.status.is_server_error() {
        SinkError::Transient(msg)
    } else {
        SinkError::Fatal(msg)
    }
}

/// A failed index-level task (create, swap, delete, settings) → reaction.
pub(crate) fn map_task_error(context: &str, error: &MeiliError) -> SinkError {
    let msg = format!("{context}: {error}");
    match error.kind.as_str() {
        "internal" | "system" => SinkError::Transient(msg),
        "auth" => SinkError::Config(msg),
        _ if is_config_code(&error.code) || error.code == "index_not_found" => {
            SinkError::Config(msg)
        }
        _ => SinkError::Fatal(msg),
    }
}

/// How a failed document task should be handled.
#[derive(Debug)]
pub(crate) enum DocFailure {
    /// One or more documents in the payload are at fault: bisect.
    PerDocument(String),
    /// Could be one document or an outage: `vector_embedding_error` (bad
    /// document or embedder down) and `internal` (Meilisearch reports some
    /// malformed documents, e.g. a non-object `_vectors`, as internal
    /// errors). Bisected like a per-document error; an isolated document is
    /// resent, and rejected only when it is provably at fault.
    Suspect(String),
    /// Not the documents' fault.
    Global(SinkError),
}

pub(crate) fn classify_doc_failure(error: &MeiliError) -> DocFailure {
    let msg = || format!("document task failed: {error}");
    match error.kind.as_str() {
        "system" => DocFailure::Global(SinkError::Transient(msg())),
        "internal" => DocFailure::Suspect(error.message.clone()),
        "auth" => DocFailure::Global(SinkError::Config(msg())),
        "invalid_request" => {
            if is_config_code(&error.code) {
                DocFailure::Global(SinkError::Config(msg()))
            } else if error.code == "vector_embedding_error" {
                DocFailure::Suspect(error.message.clone())
            } else if matches!(
                error.code.as_str(),
                "index_not_found"
                    | "index_already_exists"
                    | "index_creation_failed"
                    | "payload_too_large"
            ) {
                DocFailure::Global(SinkError::Fatal(msg()))
            } else {
                DocFailure::PerDocument(error.message.clone())
            }
        }
        _ => DocFailure::Global(SinkError::Fatal(msg())),
    }
}
