//! Meilisearch writer: batching, task tracking, poison-document bisection, index swap.
//!
//! [`MeiliSink`] implements [`meili_connect_core::Sink`] over the Meilisearch
//! HTTP API. See `docs/ARCHITECTURE.md`, "Meilisearch writes".

mod client;

/// What the refusal of [`MeiliSink`]'s `copy_settings` says when an embedder
/// of the index to rebuild holds an API key. The server gives that refusal a
/// description of its own (`server::reason`): its usual one for a refused
/// request talks about the primary key.
pub const EMBEDDER_KEY_NOT_COPIED: &str = "which Meilisearch only returns redacted";

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use meili_connect_core::{
    ApplyReport, DocOp, Document, HostPolicy, IndexTarget, MeiliDestination, RejectedDoc, Sink,
    SinkError,
};
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::json;

use client::{Client, DocFailure, Payload, Retry, TaskStatus, classify_doc_failure, map_api_error};

/// Meilisearch's default `http_payload_size_limit` is 100 MiB; stay under.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 90 * 1024 * 1024;

/// Tunables of [`MeiliSink`].
#[derive(Debug, Clone)]
pub struct SinkOptions {
    /// Upper bound of one request body before compression. Runs are split
    /// to fit; a single document over it is rejected.
    pub max_payload_bytes: usize,
    /// First delay between two `GET /tasks/{uid}`; doubled up to
    /// `max_poll_interval`.
    pub poll_interval: Duration,
    pub max_poll_interval: Duration,
    /// How long to wait for one task before giving up with a transient
    /// error (the engine then replays the batch).
    pub task_timeout: Duration,
    /// Timeout of one HTTP request, body included.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// Send document payloads with `Content-Encoding: gzip`.
    pub gzip: bool,
    /// How many more times a single document is sent after a failure that
    /// may be transient (`internal`, `vector_embedding_error`) before the
    /// whole `apply` fails with `SinkError::Transient`.
    pub suspect_retries: u32,
    /// First delay before resending such a document; doubled each time,
    /// jittered.
    pub suspect_backoff: Duration,
    /// How many more times one HTTP request is sent after a 408, 429, 5xx
    /// or transport error before the call fails with `Transient`. Document
    /// writes are idempotent by id, so resending them is safe.
    pub request_retries: u32,
    /// First delay between two attempts; doubled each time, jittered.
    pub request_backoff: Duration,
    /// Cap of one delay, `Retry-After` included.
    pub max_request_backoff: Duration,
    /// Which addresses the sink may connect to. Unless it is `any`, the
    /// HTTP client resolves names through the policy and connects only to
    /// the vetted addresses (no second lookup a DNS rebinding could
    /// change), never through a proxy; a literal IP in the URL is vetted
    /// when the sink is built. Redirects are never followed, whatever the
    /// policy. Default: `any` (tests, trusted standalone use).
    pub host_policy: Arc<HostPolicy>,
}

impl Default for SinkOptions {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            poll_interval: Duration::from_millis(50),
            max_poll_interval: Duration::from_secs(1),
            task_timeout: Duration::from_secs(15 * 60),
            request_timeout: Duration::from_secs(120),
            connect_timeout: Duration::from_secs(10),
            gzip: true,
            suspect_retries: 3,
            suspect_backoff: Duration::from_secs(1),
            request_retries: 5,
            request_backoff: Duration::from_millis(200),
            max_request_backoff: Duration::from_secs(30),
            host_policy: Arc::new(HostPolicy::any()),
        }
    }
}

/// The Meilisearch [`Sink`].
///
/// Cancellation: every method may be dropped at any await point. The sink
/// holds no mutable state (only the HTTP client and its options), so a
/// dropped call leaves nothing behind but tasks already enqueued on
/// Meilisearch, which a replay rewrites idempotently. A gzip running on the
/// blocking pool finishes and is discarded.
pub struct MeiliSink {
    client: Client,
}

impl std::fmt::Debug for MeiliSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeiliSink")
            .field("options", &self.client.opts)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Replace,
    Merge,
    Delete,
}

impl Kind {
    fn of(op: &DocOp) -> Self {
        match op {
            DocOp::Replace { .. } => Kind::Replace,
            DocOp::Merge { .. } => Kind::Merge,
            DocOp::Delete { .. } => Kind::Delete,
        }
    }
}

/// Splits `ops` into maximal runs of one kind, in order.
fn runs(ops: &[DocOp]) -> Vec<(Kind, std::ops::Range<usize>)> {
    let mut out: Vec<(Kind, std::ops::Range<usize>)> = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let kind = Kind::of(op);
        match out.last_mut() {
            Some((k, r)) if *k == kind => r.end = i + 1,
            _ => out.push((kind, i..i + 1)),
        }
    }
    out
}

/// A document with its id injected under `primary_key` and `unset` fields
/// written as `null`, serialized without cloning the body.
struct DocLine<'a> {
    primary_key: &'a str,
    id: &'a str,
    doc: &'a Document,
    unset: &'a [String],
}

impl Serialize for DocLine<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(None)?;
        map.serialize_entry(self.primary_key, self.id)?;
        for (k, v) in self.doc {
            if k != self.primary_key {
                map.serialize_entry(k, v)?;
            }
        }
        for (i, field) in self.unset.iter().enumerate() {
            if field != self.primary_key
                && !self.doc.contains_key(field)
                && !self.unset[..i].contains(field)
            {
                map.serialize_entry(field, &serde_json::Value::Null)?;
            }
        }
        map.end()
    }
}

/// The wire form of one op: an NDJSON line, or a JSON string for deletes.
fn encode_op(op: &DocOp, primary_key: &str) -> Result<Vec<u8>, SinkError> {
    let res = match op {
        DocOp::Replace { id, doc } => serde_json::to_vec(&DocLine {
            primary_key,
            id,
            doc,
            unset: &[],
        }),
        DocOp::Merge { id, doc, unset } => serde_json::to_vec(&DocLine {
            primary_key,
            id,
            doc,
            unset,
        }),
        DocOp::Delete { id } => serde_json::to_vec(id),
    };
    res.map_err(|e| SinkError::Fatal(format!("cannot serialize document: {e}")))
}

/// Bytes a delete-batch array adds around its items (`[` and `]`).
const ARRAY_OVERHEAD: usize = 2;

enum WriteOutcome {
    Written,
    /// Meilisearch refused the payload in a way bisection can narrow down.
    Refused {
        reason: String,
        suspect: bool,
    },
}

/// The ops of one run currently being written, with their wire form.
struct Chunk {
    /// Index into the run of each line.
    idx: Vec<usize>,
    lines: Vec<Vec<u8>>,
    bytes: usize,
}

impl Chunk {
    fn new() -> Self {
        Self {
            idx: Vec::new(),
            lines: Vec::new(),
            bytes: 0,
        }
    }
}

/// Successes and deterministic rejections (index into the run, reason) of
/// one run.
#[derive(Default)]
struct RunTally {
    ok: u64,
    /// Wire bytes of the accepted lines, one separator each.
    ok_bytes: u64,
    rejected: Vec<(usize, String)>,
}

/// Wire bytes of some lines, one separator (newline or comma) each.
fn wire_bytes(lines: &[Vec<u8>]) -> u64 {
    lines.iter().map(|l| l.len() as u64 + 1).sum()
}

/// Deepest JSON nesting Meilisearch accepts in a document, the top-level
/// object counting as one (serde_json's recursion limit is 128).
const MAX_DOC_DEPTH: usize = 127;

fn json_depth(v: &serde_json::Value) -> usize {
    match v {
        serde_json::Value::Array(a) => 1 + a.iter().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Object(o) => 1 + o.values().map(json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

/// Why Meilisearch will always refuse this document although it reports
/// the failure as `internal` (no stable error code). Checked on the
/// document itself, never on the error message.
fn shape_problem(op: &DocOp) -> Option<String> {
    let doc = match op {
        DocOp::Replace { doc, .. } | DocOp::Merge { doc, .. } => doc,
        DocOp::Delete { .. } => return None,
    };
    match doc.get("_vectors") {
        None | Some(serde_json::Value::Null | serde_json::Value::Object(_)) => {}
        Some(_) => return Some("`_vectors` must be an object".into()),
    }
    let depth = 1 + doc.values().map(json_depth).max().unwrap_or(0);
    (depth > MAX_DOC_DEPTH).then(|| {
        format!("document nests {depth} levels deep, over Meilisearch's limit of {MAX_DOC_DEPTH}")
    })
}

/// The first embedder of `settings` that holds an `apiKey`, at any depth
/// (composite embedders nest theirs). `GET /settings` always redacts API
/// keys (`"sk-XXXXX..."`), so any key found there cannot be copied.
fn embedder_with_api_key(settings: &serde_json::Value) -> Option<&str> {
    fn has_key(v: &serde_json::Value) -> bool {
        match v {
            serde_json::Value::Object(o) => o
                .iter()
                .any(|(k, v)| (k == "apiKey" && !v.is_null()) || has_key(v)),
            serde_json::Value::Array(a) => a.iter().any(has_key),
            _ => false,
        }
    }
    settings
        .get("embedders")?
        .as_object()?
        .iter()
        .find(|(_, e)| has_key(e))
        .map(|(name, _)| name.as_str())
}

/// What became of a document isolated with a possibly transient failure.
enum Settled {
    Written,
    Rejected(String),
}

impl MeiliSink {
    pub fn new(destination: &MeiliDestination, options: SinkOptions) -> Result<Self, SinkError> {
        Ok(Self {
            client: Client::new(destination, options)?,
        })
    }

    fn limit(&self, kind: Kind) -> usize {
        match kind {
            Kind::Delete => self
                .client
                .opts
                .max_payload_bytes
                .saturating_sub(ARRAY_OVERHEAD),
            _ => self.client.opts.max_payload_bytes,
        }
    }

    async fn apply_run(
        &self,
        target: &IndexTarget,
        kind: Kind,
        run: &[DocOp],
        report: &mut ApplyReport,
    ) -> Result<(), SinkError> {
        let limit = self.limit(kind);
        let mut tally = RunTally::default();
        let mut chunk = Chunk::new();
        for (i, op) in run.iter().enumerate() {
            let line = encode_op(op, &target.primary_key)?;
            // One separator per line (newline or comma).
            let cost = line.len() + 1;
            if cost > limit {
                tally.rejected.push((
                    i,
                    format!(
                        "document is {} bytes, over the payload limit of {} bytes",
                        line.len(),
                        self.client.opts.max_payload_bytes
                    ),
                ));
                continue;
            }
            if chunk.bytes + cost > limit {
                let full = std::mem::replace(&mut chunk, Chunk::new());
                self.write_bisect(target, kind, run, full, &mut tally)
                    .await?;
            }
            chunk.idx.push(i);
            chunk.lines.push(line);
            chunk.bytes += cost;
        }
        if !chunk.lines.is_empty() {
            self.write_bisect(target, kind, run, chunk, &mut tally)
                .await?;
        }
        match kind {
            Kind::Delete => report.deleted += tally.ok,
            _ => report.written += tally.ok,
        }
        report.bytes += tally.ok_bytes;
        if !tally.rejected.is_empty() {
            tracing::warn!(
                index = %target.uid,
                rejected = tally.rejected.len(),
                "Meilisearch rejected documents"
            );
        }
        for (i, reason) in tally.rejected {
            if let Some(op) = run.get(i) {
                report.rejected.push(RejectedDoc {
                    op: op.clone(),
                    reason,
                });
            }
        }
        Ok(())
    }

    /// Writes a chunk; on refusal, halves it until each refused document is
    /// isolated. Left halves go first so the run keeps its order. An
    /// isolated document whose failure may be transient is settled on the
    /// spot, before anything after it in the batch is written, so the order
    /// across runs holds.
    async fn write_bisect(
        &self,
        target: &IndexTarget,
        kind: Kind,
        run: &[DocOp],
        chunk: Chunk,
        tally: &mut RunTally,
    ) -> Result<(), SinkError> {
        let mut stack = vec![(0usize, chunk.lines.len())];
        while let Some((a, b)) = stack.pop() {
            match self.write_once(target, kind, &chunk.lines[a..b]).await? {
                WriteOutcome::Written => {
                    tally.ok += (b - a) as u64;
                    tally.ok_bytes += wire_bytes(&chunk.lines[a..b]);
                }
                WriteOutcome::Refused { reason, suspect } => {
                    if b - a == 1 {
                        let i = chunk.idx[a];
                        let settled = match run.get(i) {
                            Some(op) if suspect => {
                                self.settle_suspect(target, kind, op, &chunk.lines[a..b], reason)
                                    .await?
                            }
                            _ => Settled::Rejected(reason),
                        };
                        match settled {
                            Settled::Written => {
                                tally.ok += 1;
                                tally.ok_bytes += wire_bytes(&chunk.lines[a..b]);
                            }
                            Settled::Rejected(reason) => tally.rejected.push((i, reason)),
                        }
                    } else {
                        let mid = a + (b - a) / 2;
                        tracing::debug!(index = %target.uid, size = b - a, "bisecting refused payload");
                        stack.push((mid, b));
                        stack.push((a, mid));
                    }
                }
            }
        }
        Ok(())
    }

    /// A single document failed with an error that may be transient (an
    /// embedder or Meilisearch hiccup) or may be its own fault. Resend it a
    /// few times with backoff; if it keeps failing, reject it only when the
    /// document itself is provably at fault, otherwise fail the whole call
    /// with `Transient` so nothing is dead-lettered by mistake.
    async fn settle_suspect(
        &self,
        target: &IndexTarget,
        kind: Kind,
        op: &DocOp,
        line: &[Vec<u8>],
        mut reason: String,
    ) -> Result<Settled, SinkError> {
        if let Some(why) = shape_problem(op) {
            return Ok(Settled::Rejected(format!("{reason} ({why})")));
        }
        let opts = &self.client.opts;
        let mut delay = opts.suspect_backoff;
        for attempt in 1..=opts.suspect_retries {
            tracing::debug!(index = %target.uid, attempt, "resending a document after a possibly transient failure");
            tokio::time::sleep(client::jitter(delay)).await;
            delay = delay.saturating_mul(2);
            match self.write_once(target, kind, line).await? {
                WriteOutcome::Written => return Ok(Settled::Written),
                WriteOutcome::Refused {
                    reason: r,
                    suspect: false,
                } => return Ok(Settled::Rejected(r)),
                WriteOutcome::Refused {
                    reason: r,
                    suspect: true,
                } => reason = r,
            }
        }
        if let Some(embedder) = self.missing_user_provided_vectors(&target.uid, op).await? {
            return Ok(Settled::Rejected(format!(
                "{reason} (no `_vectors.{embedder}` for the userProvided embedder `{embedder}`)"
            )));
        }
        Err(SinkError::Transient(format!(
            "document `{}` still fails after {} retries with a possibly transient error: {reason}",
            op.id(),
            opts.suspect_retries
        )))
    }

    /// The name of a `userProvided` embedder of the index for which this
    /// full document has no `_vectors` entry: Meilisearch then fails with
    /// `vector_embedding_error`, the same code as an embedder outage.
    async fn missing_user_provided_vectors(
        &self,
        uid: &str,
        op: &DocOp,
    ) -> Result<Option<String>, SinkError> {
        // A partial update may rely on the vectors already stored.
        let DocOp::Replace { doc, .. } = op else {
            return Ok(None);
        };
        let body = match self
            .client
            .call(
                Method::GET,
                &["indexes", uid, "settings", "embedders"],
                &[],
                None,
            )
            .await?
        {
            Ok(body) => body,
            Err(api) if api.status == StatusCode::NOT_FOUND => return Ok(None),
            Err(api) => return Err(map_api_error("reading embedders", &api)),
        };
        let embedders =
            serde_json::from_slice::<Option<serde_json::Map<String, serde_json::Value>>>(&body)
                .map_err(|e| {
                    SinkError::Fatal(format!("unexpected embedders from Meilisearch: {e}"))
                })?
                .unwrap_or_default();
        let vectors = doc.get("_vectors").and_then(|v| v.as_object());
        Ok(embedders
            .iter()
            .filter(|(_, e)| e.get("source").and_then(|s| s.as_str()) == Some("userProvided"))
            .map(|(name, _)| name)
            .find(|name| !vectors.is_some_and(|v| v.contains_key(name.as_str())))
            .cloned())
    }

    /// One request + its task.
    async fn write_once(
        &self,
        target: &IndexTarget,
        kind: Kind,
        lines: &[Vec<u8>],
    ) -> Result<WriteOutcome, SinkError> {
        let size: usize = lines.iter().map(|l| l.len() + 1).sum();
        let mut body = Vec::with_capacity(size + ARRAY_OVERHEAD);
        let (method, content_type) = match kind {
            Kind::Delete => {
                body.push(b'[');
                for (i, l) in lines.iter().enumerate() {
                    if i > 0 {
                        body.push(b',');
                    }
                    body.extend_from_slice(l);
                }
                body.push(b']');
                (Method::POST, "application/json")
            }
            Kind::Replace | Kind::Merge => {
                for l in lines {
                    body.extend_from_slice(l);
                    body.push(b'\n');
                }
                let m = if kind == Kind::Replace {
                    Method::POST
                } else {
                    Method::PUT
                };
                (m, "application/x-ndjson")
            }
        };
        let payload = Some(Payload::Raw { body, content_type });
        let enqueued = match kind {
            Kind::Delete => {
                self.client
                    .enqueue(
                        method,
                        &["indexes", &target.uid, "documents", "delete-batch"],
                        &[],
                        payload,
                    )
                    .await?
            }
            _ => {
                self.client
                    .enqueue(
                        method,
                        &["indexes", &target.uid, "documents"],
                        &[("primaryKey", target.primary_key.as_str())],
                        payload,
                    )
                    .await?
            }
        };
        let uid = match enqueued {
            Ok(uid) => uid,
            Err(api) if api.status == StatusCode::PAYLOAD_TOO_LARGE => {
                return Ok(WriteOutcome::Refused {
                    reason: format!("over the instance's payload limit: {}", api.error),
                    suspect: false,
                });
            }
            Err(api) => return Err(map_api_error("writing documents", &api)),
        };
        let task = self.client.wait_task(uid).await?;
        tracing::debug!(index = %target.uid, task = task.uid, docs = lines.len(), status = ?task.status, "document task finished");
        match task.status {
            TaskStatus::Succeeded => Ok(WriteOutcome::Written),
            TaskStatus::Canceled => Err(SinkError::Transient(format!(
                "task {uid} was canceled by another client"
            ))),
            _ => {
                let error = task.error.unwrap_or_default();
                if kind == Kind::Delete && error.code == "index_not_found" {
                    // Nothing to delete from an index that does not exist.
                    return Ok(WriteOutcome::Written);
                }
                match classify_doc_failure(&error) {
                    DocFailure::PerDocument(reason) => Ok(WriteOutcome::Refused {
                        reason,
                        suspect: false,
                    }),
                    DocFailure::Suspect(reason) => Ok(WriteOutcome::Refused {
                        reason,
                        suspect: true,
                    }),
                    DocFailure::Global(e) => Err(e),
                }
            }
        }
    }

    /// Enqueue + wait; a failed task is mapped with `map_task_error`, except
    /// codes in `ok_codes`, which count as success.
    async fn index_task(
        &self,
        context: &str,
        retry: Retry,
        method: Method,
        segments: &[&str],
        payload: Option<Payload>,
        ok_codes: &[&str],
    ) -> Result<(), SinkError> {
        let task = self
            .client
            .run_task(context, retry, method, segments, payload)
            .await?;
        match task.status {
            TaskStatus::Succeeded => Ok(()),
            TaskStatus::Canceled => Err(SinkError::Transient(format!(
                "{context}: task {} was canceled by another client",
                task.uid
            ))),
            _ => {
                let error = task.error.unwrap_or_default();
                if ok_codes.contains(&error.code.as_str()) {
                    Ok(())
                } else {
                    Err(client::map_task_error(context, &error))
                }
            }
        }
    }

    /// Enqueue + wait for a payload-less task on an index; a missing index
    /// (404 on enqueue or `index_not_found` on the task) counts as success.
    async fn task_allow_missing(
        &self,
        context: &str,
        method: Method,
        segments: &[&str],
    ) -> Result<(), SinkError> {
        let task_uid = match self.client.enqueue(method, segments, &[], None).await? {
            Ok(t) => t,
            Err(api) if api.status == StatusCode::NOT_FOUND => return Ok(()),
            Err(api) => return Err(map_api_error(context, &api)),
        };
        let task = self.client.wait_task(task_uid).await?;
        match task.status {
            TaskStatus::Succeeded => Ok(()),
            TaskStatus::Canceled => Err(SinkError::Transient(format!(
                "{context}: task {task_uid} was canceled"
            ))),
            _ => {
                let error = task.error.unwrap_or_default();
                if error.code == "index_not_found" {
                    Ok(())
                } else {
                    Err(client::map_task_error(context, &error))
                }
            }
        }
    }

    /// `GET /indexes/{uid}` → `Some(primaryKey)`, `None` when missing.
    async fn index_primary_key(&self, uid: &str) -> Result<Option<Option<String>>, SinkError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct IndexInfo {
            primary_key: Option<String>,
        }
        match self
            .client
            .call(Method::GET, &["indexes", uid], &[], None)
            .await?
        {
            Ok(body) => {
                let info: IndexInfo = serde_json::from_slice(&body).map_err(|e| {
                    SinkError::Fatal(format!("unexpected index from Meilisearch: {e}"))
                })?;
                Ok(Some(info.primary_key))
            }
            Err(api) if api.status == StatusCode::NOT_FOUND => Ok(None),
            Err(api) => Err(map_api_error("reading index", &api)),
        }
    }
}

#[async_trait]
impl Sink for MeiliSink {
    async fn ensure_index(&self, target: &IndexTarget) -> Result<(), SinkError> {
        let uid = target.uid.as_str();
        let pk = target.primary_key.as_str();
        // Two rounds: a concurrent creation between GET and POST is re-read.
        for _ in 0..2 {
            match self.index_primary_key(uid).await? {
                Some(Some(existing)) if existing == pk => return Ok(()),
                Some(Some(existing)) => {
                    return Err(SinkError::Config(format!(
                        "index `{uid}` exists with primary key `{existing}`, expected `{pk}`"
                    )));
                }
                Some(None) => {
                    // Created without a primary key (e.g. by a settings
                    // update): set it.
                    return self
                        .index_task(
                            "setting index primary key",
                            Retry::Idempotent,
                            Method::PATCH,
                            &["indexes", uid],
                            Some(Payload::Json(json!({ "primaryKey": pk }))),
                            &[],
                        )
                        .await;
                }
                None => {
                    let task = self
                        .client
                        .run_task(
                            "creating index",
                            // A duplicate fails with `index_already_exists`,
                            // handled below.
                            Retry::Idempotent,
                            Method::POST,
                            &["indexes"],
                            Some(Payload::Json(json!({ "uid": uid, "primaryKey": pk }))),
                        )
                        .await?;
                    match task.status {
                        TaskStatus::Succeeded => return Ok(()),
                        TaskStatus::Failed
                            if task
                                .error
                                .as_ref()
                                .is_some_and(|e| e.code == "index_already_exists") =>
                        {
                            continue;
                        }
                        TaskStatus::Canceled => {
                            return Err(SinkError::Transient(format!(
                                "creating index: task {} was canceled",
                                task.uid
                            )));
                        }
                        _ => {
                            let error = task.error.unwrap_or_default();
                            return Err(client::map_task_error("creating index", &error));
                        }
                    }
                }
            }
        }
        Err(SinkError::Transient(format!(
            "index `{uid}` is being created concurrently"
        )))
    }

    async fn apply(&self, target: &IndexTarget, ops: Vec<DocOp>) -> Result<ApplyReport, SinkError> {
        let mut report = ApplyReport::default();
        for (kind, range) in runs(&ops) {
            self.apply_run(target, kind, &ops[range], &mut report)
                .await?;
        }
        Ok(report)
    }

    async fn swap(&self, a: &str, b: &str) -> Result<(), SinkError> {
        self.index_task(
            "swapping indexes",
            // A swap enqueued twice swaps back.
            Retry::ConnectOnly,
            Method::POST,
            &["swap-indexes"],
            Some(Payload::Json(json!([{ "indexes": [a, b] }]))),
            &[],
        )
        .await
    }

    async fn delete_all_documents(&self, uid: &str) -> Result<(), SinkError> {
        self.task_allow_missing(
            "deleting all documents",
            Method::DELETE,
            &["indexes", uid, "documents"],
        )
        .await
    }

    async fn delete_index(&self, uid: &str) -> Result<(), SinkError> {
        self.task_allow_missing("deleting index", Method::DELETE, &["indexes", uid])
            .await
    }

    async fn copy_settings(&self, from: &str, to: &str) -> Result<(), SinkError> {
        let settings = match self
            .client
            .call(Method::GET, &["indexes", from, "settings"], &[], None)
            .await?
        {
            Ok(body) => serde_json::from_slice::<serde_json::Value>(&body).map_err(|e| {
                SinkError::Fatal(format!("unexpected settings from Meilisearch: {e}"))
            })?,
            Err(api) if api.status == StatusCode::NOT_FOUND => return Ok(()),
            Err(api) => return Err(map_api_error("reading settings", &api)),
        };
        if let Some(embedder) = embedder_with_api_key(&settings) {
            return Err(SinkError::Config(format!(
                "cannot copy the settings of index `{from}` to `{to}`: embedder `{embedder}` has \
                 an API key, {EMBEDDER_KEY_NOT_COPIED}; copying it would install a broken key. \
                 Empty `{from}` (its settings are kept) or remove the key, then resume"
            )));
        }
        // Every key of `GET /settings` is accepted back by `PATCH` (checked
        // against Meilisearch 1.57, embedders included), and the PATCH
        // creates `to` when it does not exist yet.
        self.index_task(
            "copying settings",
            Retry::Idempotent,
            Method::PATCH,
            &["indexes", to, "settings"],
            Some(Payload::Json(settings)),
            &[],
        )
        .await
    }

    async fn document_count(&self, uid: &str) -> Result<Option<u64>, SinkError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Stats {
            number_of_documents: u64,
        }
        match self
            .client
            .call(Method::GET, &["indexes", uid, "stats"], &[], None)
            .await?
        {
            Ok(body) => {
                let stats: Stats = serde_json::from_slice(&body).map_err(|e| {
                    SinkError::Fatal(format!("unexpected stats from Meilisearch: {e}"))
                })?;
                Ok(Some(stats.number_of_documents))
            }
            Err(api) if api.status == StatusCode::NOT_FOUND => Ok(None),
            Err(api) => Err(map_api_error("reading index stats", &api)),
        }
    }
}

#[cfg(test)]
mod unit_tests;
