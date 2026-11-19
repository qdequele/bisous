//! Thin HTTP layer over the Meilisearch API: requests, error mapping, task
//! polling. Never logs the API key (sent as a sensitive header) nor request
//! bodies.

use std::hash::{BuildHasher as _, Hasher as _};
use std::io::Write as _;
use std::time::{Duration, Instant};

use bytes::Bytes;
use meili_connect_core::{MeiliDestination, SinkError};
use reqwest::header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, HeaderValue};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;

use crate::SinkOptions;

/// Bodies above this size are gzipped on the blocking pool.
const INLINE_GZIP_LIMIT: usize = 64 * 1024;
/// Longest excerpt of an unparseable error body kept in a message.
const RAW_ERROR_EXCERPT: usize = 200;

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
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(opts.request_timeout)
            .connect_timeout(opts.connect_timeout.min(opts.request_timeout))
            .user_agent(concat!("meili-connect/", env!("CARGO_PKG_VERSION")))
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

    /// Sends one request. Transport failures are `Err(SinkError::Transient)`;
    /// HTTP failures come back as `Ok(Err(ApiError))` for the caller to map.
    pub async fn call(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Reply, SinkError> {
        let url = self.url(segments)?;
        let mut req = self.http.request(method, url);
        if !query.is_empty() {
            req = req.query(query);
        }
        match payload {
            None => {}
            Some(Payload::Json(value)) => {
                let body = serde_json::to_vec(&value)
                    .map_err(|e| SinkError::Fatal(format!("cannot serialize request: {e}")))?;
                req = req.header(CONTENT_TYPE, "application/json").body(body);
            }
            Some(Payload::Raw { body, content_type }) => {
                req = req.header(CONTENT_TYPE, content_type);
                if self.opts.gzip {
                    req = req.header(CONTENT_ENCODING, "gzip").body(gzip(body).await?);
                } else {
                    req = req.body(body);
                }
            }
        }
        let path = segments.join("/");
        let resp = req.send().await.map_err(|e| transport_error(&path, e))?;
        let status = resp.status();
        let body = resp.bytes().await.map_err(|e| transport_error(&path, e))?;
        if status.is_success() {
            return Ok(Ok(body));
        }
        let error = serde_json::from_slice::<MeiliError>(&body).unwrap_or_else(|_| {
            let text = String::from_utf8_lossy(&body);
            let excerpt: String = text.chars().take(RAW_ERROR_EXCERPT).collect();
            MeiliError {
                message: format!("HTTP {status}: {excerpt}"),
                ..MeiliError::default()
            }
        });
        Ok(Err(ApiError { status, error }))
    }

    /// `call` for routes that enqueue a task; returns the task uid.
    pub async fn enqueue(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, &str)],
        payload: Option<Payload>,
    ) -> Result<Result<u64, ApiError>, SinkError> {
        match self.call(method, segments, query, payload).await? {
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
            let last_error = match self.call(Method::GET, &["tasks", &uid_s], &[], None).await {
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
        method: Method,
        segments: &[&str],
        payload: Option<Payload>,
    ) -> Result<Task, SinkError> {
        let uid = match self.enqueue(method, segments, &[], payload).await? {
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
