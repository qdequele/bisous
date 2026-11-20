//! `FirestoreSource`: the `Source` implementation.

use std::collections::BTreeSet;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use gcloud_sdk::google::firestore::v1::structured_aggregation_query::aggregation::{
    Count, Operator,
};
use gcloud_sdk::google::firestore::v1::structured_aggregation_query::{self, Aggregation};
use gcloud_sdk::google::firestore::v1::{
    Cursor, ListCollectionIdsRequest, ListenRequest, RunAggregationQueryRequest, RunQueryRequest,
    StructuredAggregationQuery, StructuredQuery, Target, Value, listen_request,
    run_aggregation_query_request, run_query_request, target, value::ValueType,
};
use gcloud_sdk::prost_types::Timestamp;
use meili_connect_core::{
    AckReceiver, Capabilities, CheckReport, CheckStatus, Checkpoint, CollectionInfo, Endpoint,
    EventSender, FieldInfo, Lag, LagReporter, Opened, Source, SourceError, SourceEvent,
    SourceFactory, SourceKind, SourceSpec, StreamMapping,
};
use tokio_util::sync::CancellationToken;

use crate::client::{CALL_TIMEOUT, Firestore};
use crate::config::{FirestoreConfig, host_port};
use crate::convert::relative_path;
use crate::errors::status_error;
use crate::listen::{ListenCheckpoint, ListenState, Mapping};
use crate::target::{CollectionRef, GROUP_PREFIX};

/// Collections listed by `discover`.
const MAX_LISTED_COLLECTIONS: usize = 1000;
/// Collection groups `discover` proposes (subcollections of sampled documents).
const MAX_LISTED_GROUPS: usize = 100;
/// Collections described concurrently by `discover`.
const DISCOVER_CONCURRENCY: usize = 8;
/// How long `discover` waits for one collection's document count.
const COUNT_TIMEOUT: Duration = Duration::from_secs(5);
/// Reconnects of the `Listen` stream in a row (without a response in
/// between) before giving up to the engine.
const MAX_RECONNECTS: u32 = 5;
const RECONNECT_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Builds [`FirestoreSource`]s. Register it for [`SourceKind::Firestore`].
#[derive(Debug, Clone, Copy, Default)]
pub struct FirestoreSourceFactory;

#[async_trait]
impl SourceFactory for FirestoreSourceFactory {
    /// `emulator_host` when set. Without it the connector only talks to
    /// Google's own endpoints (`firestore.googleapis.com`, and
    /// `oauth2.googleapis.com` for tokens, whatever the key says).
    async fn endpoints(&self, spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
        if spec.kind != SourceKind::Firestore {
            return Err(SourceError::Config(format!(
                "the firestore connector cannot read a `{}` config",
                spec.kind.as_str()
            )));
        }
        let config = FirestoreConfig::from_value(&spec.config)?;
        match &config.emulator_host {
            Some(host) => {
                let (host, port) = host_port(host).ok_or_else(|| {
                    SourceError::Config("`emulator_host` must be host:port".into())
                })?;
                Ok(vec![Endpoint::new(host, port)])
            }
            None => Ok(Vec::new()),
        }
    }

    fn build(
        &self,
        _sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        if spec.kind != SourceKind::Firestore {
            return Err(SourceError::Config(format!(
                "the firestore connector cannot build a `{}` source",
                spec.kind.as_str()
            )));
        }
        let config = FirestoreConfig::from_value(&spec.config)?;
        let mut mappings: Vec<Mapping> = Vec::new();
        for m in streams {
            if mappings.iter().any(|x| x.name == m.collection) {
                continue;
            }
            mappings.push(Mapping {
                name: m.collection.clone(),
                cref: CollectionRef::parse(&m.collection)?,
            });
        }
        Ok(Box::new(FirestoreSource::new(config, mappings)))
    }
}

/// A Firestore origin: paginated `RunQuery` snapshot, `Listen` changes.
///
/// It creates nothing in Firestore, so `teardown` is a no-op, and holds
/// nothing there, so acks are not needed.
pub struct FirestoreSource {
    config: FirestoreConfig,
    mappings: Vec<Mapping>,
    client: Option<Firestore>,
    /// Set by `open`.
    state: Option<ListenState>,
    resume: Option<ListenCheckpoint>,
    lag: Option<LagReporter>,
}

impl std::fmt::Debug for FirestoreSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirestoreSource")
            .field("config", &self.config)
            .field(
                "collections",
                &self.mappings.iter().map(|m| &m.name).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl FirestoreSource {
    pub fn new(config: FirestoreConfig, mappings: Vec<Mapping>) -> Self {
        Self {
            config,
            mappings,
            client: None,
            state: None,
            resume: None,
            lag: None,
        }
    }

    async fn client(&mut self) -> Result<Firestore, SourceError> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let c = Firestore::connect(&self.config).await?;
        self.client = Some(c.clone());
        Ok(c)
    }

    fn who(&self) -> Option<String> {
        self.config.client_email()
    }

    fn err(&self, op: &str, status: &tonic::Status) -> SourceError {
        status_error(op, status, self.who().as_deref())
    }

    /// Collection ids under `parent` (a database root or a document), at
    /// most `max`.
    async fn list_collection_ids(
        &self,
        fs: &Firestore,
        parent: &str,
        max: usize,
    ) -> Result<Vec<String>, SourceError> {
        let mut ids = Vec::new();
        let mut page_token = String::new();
        loop {
            let mut client = fs.get();
            let call = client.list_collection_ids(ListCollectionIdsRequest {
                parent: parent.to_owned(),
                page_size: 300,
                page_token: std::mem::take(&mut page_token),
                request_options: None,
                consistency_selector: None,
            });
            let out = tokio::time::timeout(CALL_TIMEOUT, call)
                .await
                .map_err(|_| SourceError::Transient("ListCollectionIds timed out".into()))?
                .map_err(|s| self.err("ListCollectionIds", &s))?
                .into_inner();
            ids.extend(out.collection_ids);
            if out.next_page_token.is_empty() || ids.len() >= max {
                ids.truncate(max);
                return Ok(ids);
            }
            page_token = out.next_page_token;
        }
    }

    /// The first `limit` documents of a collection, and the read time.
    async fn first_documents(
        &self,
        fs: &Firestore,
        cref: &CollectionRef,
        limit: i32,
    ) -> Result<
        (
            Vec<gcloud_sdk::google::firestore::v1::Document>,
            Option<Timestamp>,
        ),
        SourceError,
    > {
        let mut query = cref.query();
        query.limit = Some(limit);
        let mut stream = run_query(fs, cref, query)
            .await
            .map_err(|s| self.err("RunQuery", &s))?;
        let mut docs = Vec::new();
        let mut read_time = None;
        loop {
            let next = tokio::time::timeout(CALL_TIMEOUT, stream.next())
                .await
                .map_err(|_| SourceError::Transient("RunQuery timed out".into()))?;
            match next {
                None => return Ok((docs, read_time)),
                Some(Err(s)) => return Err(self.err("RunQuery", &s)),
                Some(Ok(resp)) => {
                    if resp.read_time.is_some() {
                        read_time = resp.read_time;
                    }
                    docs.extend(resp.document);
                }
            }
        }
    }

    /// Documents in a collection, when Firestore counts them in time.
    async fn count(&self, fs: &Firestore, cref: &CollectionRef) -> Option<u64> {
        let req = RunAggregationQueryRequest {
            parent: cref.query_parent(&fs.documents_root()),
            explain_options: None,
            request_options: None,
            query_type: Some(
                run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                    StructuredAggregationQuery {
                        aggregations: vec![Aggregation {
                            alias: "n".into(),
                            operator: Some(Operator::Count(Count { up_to: None })),
                        }],
                        query_type: Some(structured_aggregation_query::QueryType::StructuredQuery(
                            StructuredQuery {
                                from: cref.query().from,
                                ..Default::default()
                            },
                        )),
                    },
                ),
            ),
            consistency_selector: None,
        };
        let count = async {
            let mut stream = fs.get().run_aggregation_query(req).await.ok()?.into_inner();
            while let Some(Ok(resp)) = stream.next().await {
                if let Some(Value {
                    value_type: Some(ValueType::IntegerValue(n)),
                }) = resp
                    .result
                    .and_then(|r| r.aggregate_fields.get("n").cloned())
                {
                    return u64::try_from(n).ok();
                }
            }
            None
        };
        tokio::time::timeout(COUNT_TIMEOUT, count)
            .await
            .ok()
            .flatten()
    }

    /// One entry of `discover`: a sample document's fields, a count, and
    /// the subcollections of the sample.
    async fn describe(
        &self,
        fs: &Firestore,
        name: String,
    ) -> Result<(CollectionInfo, Vec<String>), SourceError> {
        let cref = CollectionRef::parse(&name)?;
        let (docs, _) = self.first_documents(fs, &cref, 1).await?;
        let mut fields: Vec<FieldInfo> = Vec::new();
        let mut subcollections = Vec::new();
        if let Some(doc) = docs.first() {
            let mut names: Vec<_> = doc.fields.iter().collect();
            names.sort_by(|a, b| a.0.cmp(b.0));
            fields = names
                .into_iter()
                .map(|(k, v)| FieldInfo {
                    name: k.clone(),
                    source_type: type_name(v).to_owned(),
                    nullable: true,
                })
                .collect();
            if !cref.is_group() {
                subcollections = self
                    .list_collection_ids(fs, &doc.name, MAX_LISTED_GROUPS)
                    .await
                    .unwrap_or_default();
            }
        }
        let estimated_rows = self.count(fs, &cref).await;
        Ok((
            CollectionInfo {
                name,
                key: vec!["__name__".into()],
                fields,
                estimated_rows,
            },
            subcollections,
        ))
    }

    fn resume_type(&self) -> Result<target::ResumeType, SourceError> {
        if let Some(token) = self.state.as_ref().and_then(|s| s.last_token.clone()) {
            return Ok(target::ResumeType::ResumeToken(token));
        }
        let cp = self
            .resume
            .as_ref()
            .ok_or_else(|| SourceError::Fatal("stream called before open".into()))?;
        Ok(match cp.token_bytes()? {
            Some(token) => target::ResumeType::ResumeToken(token),
            None => target::ResumeType::ReadTime(cp.read_timestamp()?),
        })
    }

    /// One `Listen` stream, until it ends, fails, or the token fires.
    /// Returns whether at least one response was received.
    async fn listen_once(
        &mut self,
        fs: &Firestore,
        events: &EventSender,
        cancel: &CancellationToken,
    ) -> Result<ListenEnd, SourceError> {
        let resume = self.resume_type()?;
        let mappings = self
            .state
            .as_ref()
            .map(|s| s.mappings().to_vec())
            .unwrap_or_default();
        let (tx, rx) = tokio::sync::mpsc::channel(mappings.len().max(1));
        for (i, m) in mappings.iter().enumerate() {
            let target_id = i32::try_from(i + 1)
                .map_err(|_| SourceError::Config("too many collections in one sync".into()))?;
            let req = ListenRequest {
                database: fs.database.clone(),
                labels: Default::default(),
                request_options: None,
                target_change: Some(listen_request::TargetChange::AddTarget(Target {
                    target_id,
                    once: false,
                    expected_count: None,
                    target_type: Some(target::TargetType::Query(target::QueryTarget {
                        parent: m.cref.query_parent(&fs.documents_root()),
                        query_type: Some(target::query_target::QueryType::StructuredQuery(
                            StructuredQuery {
                                from: m.cref.query().from,
                                ..Default::default()
                            },
                        )),
                    })),
                    resume_type: Some(resume.clone()),
                })),
            };
            // The channel holds every target: this never waits.
            if tx.send(req).await.is_err() {
                return Err(SourceError::Fatal("listen request channel closed".into()));
            }
        }
        // `tx` stays alive until this function returns: dropping it would
        // half-close the stream.
        let mut client = fs.get();
        let opened = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(ListenEnd::Cancelled),
            r = tokio::time::timeout(CALL_TIMEOUT, client.listen(tokio_stream::wrappers::ReceiverStream::new(rx))) => r,
        };
        let mut stream = opened
            .map_err(|_| SourceError::Transient("opening the Listen stream timed out".into()))?
            .map_err(|s| self.err("Listen", &s))?
            .into_inner();
        let mut progressed = false;
        loop {
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(ListenEnd::Cancelled),
                n = stream.next() => n,
            };
            let resp = match next {
                None => {
                    return Ok(ListenEnd::Closed { progressed });
                }
                Some(Err(s)) => {
                    let e = self.err("Listen", &s);
                    return match e {
                        SourceError::Transient(m) => Ok(ListenEnd::Failed {
                            progressed,
                            message: m,
                        }),
                        other => Err(other),
                    };
                }
                Some(Ok(resp)) => resp,
            };
            progressed = true;
            let state = self
                .state
                .as_mut()
                .ok_or_else(|| SourceError::Fatal("stream called before open".into()))?;
            let out = state.on_response(resp)?;
            for ev in out {
                if let (SourceEvent::Checkpoint(_), Some(lag), Some(rt)) =
                    (&ev, &self.lag, state.last_read_time.as_ref())
                {
                    lag.report(Lag {
                        seconds: Some(meili_connect_core::seconds_since_unix_millis(
                            rt.seconds.saturating_mul(1000) + i64::from(rt.nanos / 1_000_000),
                        )),
                        bytes: None,
                        records: None,
                    });
                }
                if !emit(events, ev, cancel).await {
                    return Ok(ListenEnd::Cancelled);
                }
            }
        }
    }
}

enum ListenEnd {
    Cancelled,
    /// The server ended the stream.
    Closed {
        progressed: bool,
    },
    /// A transient failure.
    Failed {
        progressed: bool,
        message: String,
    },
}

/// Send an event; `false` when the token fired or the engine went away.
async fn emit(events: &EventSender, ev: SourceEvent, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        r = events.send(ev) => r.is_ok(),
    }
}

async fn run_query(
    fs: &Firestore,
    cref: &CollectionRef,
    query: StructuredQuery,
) -> Result<tonic::Streaming<gcloud_sdk::google::firestore::v1::RunQueryResponse>, tonic::Status> {
    let req = RunQueryRequest {
        parent: cref.query_parent(&fs.documents_root()),
        explain_options: None,
        request_options: None,
        query_type: Some(run_query_request::QueryType::StructuredQuery(query)),
        consistency_selector: None,
    };
    match tokio::time::timeout(CALL_TIMEOUT, fs.get().run_query(req)).await {
        Ok(r) => r.map(|r| r.into_inner()),
        Err(_) => Err(tonic::Status::deadline_exceeded("RunQuery timed out")),
    }
}

/// The Firestore type name of a value, for `discover`.
fn type_name(v: &Value) -> &'static str {
    match &v.value_type {
        None | Some(ValueType::NullValue(_)) => "null",
        Some(ValueType::BooleanValue(_)) => "boolean",
        Some(ValueType::IntegerValue(_)) => "integer",
        Some(ValueType::DoubleValue(_)) => "double",
        Some(ValueType::TimestampValue(_)) => "timestamp",
        Some(ValueType::StringValue(_)) => "string",
        Some(ValueType::BytesValue(_)) => "bytes",
        Some(ValueType::ReferenceValue(_)) => "reference",
        Some(ValueType::GeoPointValue(_)) => "geopoint",
        Some(ValueType::ArrayValue(_)) => "array",
        Some(ValueType::MapValue(_)) => "map",
        Some(_) => "expression",
    }
}

fn now() -> Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(d.as_secs()).unwrap_or(0),
        nanos: i32::try_from(d.subsec_nanos()).unwrap_or(0),
    }
}

#[async_trait]
impl Source for FirestoreSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        let mut report = CheckReport::default();
        let fs = match self.client().await {
            Ok(fs) => fs,
            Err(e) => {
                report.push("connection", CheckStatus::Failed, e.to_string());
                return Ok(report);
            }
        };
        let who = self.who().unwrap_or_else(|| "the emulator".into());
        match self.list_collection_ids(&fs, &fs.documents_root(), 1).await {
            Ok(_) => report.push(
                "connection",
                CheckStatus::Ok,
                format!("connected to {} as {who}", fs.database),
            ),
            Err(e) => {
                report.push("connection", CheckStatus::Failed, e.to_string());
                return Ok(report);
            }
        }
        if self.mappings.is_empty() {
            report.push_none_selected("collections", "collection");
            return Ok(report);
        }
        for m in self.mappings.clone() {
            let name = format!("collection:{}", m.name);
            match self.first_documents(&fs, &m.cref, 1).await {
                Ok((docs, _)) if docs.is_empty() => report.push(
                    &name,
                    CheckStatus::Warning,
                    "no document yet: the collection syncs once documents are written to it",
                ),
                Ok(_) => report.push(&name, CheckStatus::Ok, "documents are readable"),
                Err(e) => report.push(&name, CheckStatus::Failed, e.to_string()),
            }
        }
        Ok(report)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let fs = self.client().await?;
        let root = fs.documents_root();
        let top = self
            .list_collection_ids(&fs, &root, MAX_LISTED_COLLECTIONS)
            .await?;
        let this = &*self;
        let described: Vec<_> = futures::stream::iter(top.clone())
            .map(|id| {
                let fs = fs.clone();
                async move { this.describe(&fs, id).await }
            })
            .buffered(DISCOVER_CONCURRENCY)
            .collect()
            .await;
        let mut out = Vec::new();
        let mut groups = BTreeSet::new();
        for d in described {
            let (info, subs) = d?;
            out.push(info);
            groups.extend(subs);
        }
        let mut extra: Vec<String> = groups
            .into_iter()
            .take(MAX_LISTED_GROUPS)
            .map(|id| format!("{GROUP_PREFIX}{id}"))
            .collect();
        // Mapped collections that were not listed (subcollection paths).
        for m in &self.mappings {
            if !top.contains(&m.name) && !extra.contains(&m.name) {
                extra.push(m.name.clone());
            }
        }
        let described: Vec<_> = futures::stream::iter(extra)
            .map(|name| {
                let fs = fs.clone();
                async move { this.describe(&fs, name).await }
            })
            .buffered(DISCOVER_CONCURRENCY)
            .collect()
            .await;
        for d in described {
            out.push(d?.0);
        }
        Ok(out)
    }

    async fn open(&mut self, resume: Option<Checkpoint>) -> Result<Opened, SourceError> {
        if self.mappings.is_empty() {
            return Err(SourceError::Config(
                "a Firestore sync needs at least one collection".into(),
            ));
        }
        let fs = self.client().await?;
        let max_tracked = self.config.stream.max_tracked_documents;
        match resume {
            Some(cp) => {
                let cp: ListenCheckpoint = cp.decode().map_err(|e| {
                    SourceError::PositionLost(format!("unreadable Firestore checkpoint: {e}"))
                })?;
                let mapped: Vec<String> = self.mappings.iter().map(|m| m.name.clone()).collect();
                if cp.collections != mapped {
                    return Err(SourceError::PositionLost(
                        "the mapped collections changed since the checkpoint".into(),
                    ));
                }
                cp.token_bytes()?;
                cp.read_timestamp()?;
                // Surface a revoked key or a missing database here.
                self.list_collection_ids(&fs, &fs.documents_root(), 1)
                    .await?;
                self.state = Some(ListenState::new(self.mappings.clone(), false, max_tracked));
                self.resume = Some(cp);
                Ok(Opened::Resumed)
            }
            None => {
                // The server's time, before anything is read: changes after
                // it are replayed by the stream.
                let first = self.mappings[0].cref.clone();
                let (_, read_time) = self.first_documents(&fs, &first, 1).await?;
                let read_time = read_time.unwrap_or_else(now);
                let state = ListenState::new(self.mappings.clone(), true, max_tracked);
                let start = state.checkpoint(None, &read_time)?;
                self.resume = Some(
                    start
                        .decode()
                        .map_err(|e| SourceError::Fatal(e.to_string()))?,
                );
                self.state = Some(state);
                Ok(Opened::Fresh { start })
            }
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let fs = self.client().await?;
        let page_size = i32::try_from(self.config.snapshot.page_size).unwrap_or(300);
        for (index, m) in self.mappings.clone().into_iter().enumerate() {
            let mut last: Option<String> = None;
            loop {
                let mut query = m.cref.query();
                query.limit = Some(page_size);
                if let Some(name) = last.take() {
                    query.start_at = Some(Cursor {
                        values: vec![Value {
                            value_type: Some(ValueType::ReferenceValue(name)),
                        }],
                        before: false,
                    });
                }
                let mut stream = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    s = run_query(&fs, &m.cref, query) => s.map_err(|s| self.err("RunQuery", &s))?,
                };
                let mut read = 0;
                loop {
                    let next = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        n = tokio::time::timeout(CALL_TIMEOUT, stream.next()) => n,
                    };
                    let resp = match next {
                        Err(_) => return Err(SourceError::Transient("RunQuery timed out".into())),
                        Ok(None) => break,
                        Ok(Some(Err(s))) => return Err(self.err("RunQuery", &s)),
                        Ok(Some(Ok(resp))) => resp,
                    };
                    let Some(doc) = resp.document else { continue };
                    read += 1;
                    let state = self
                        .state
                        .as_mut()
                        .ok_or_else(|| SourceError::Fatal("snapshot called before open".into()))?;
                    let ev = state.upsert(index, &doc);
                    state.record_snapshot(index, relative_path(&doc.name));
                    last = Some(doc.name);
                    if !emit(&events, ev, &cancel).await {
                        return Ok(());
                    }
                }
                if read < page_size || last.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        _acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let fs = self.client().await?;
        let mut failures: u32 = 0;
        loop {
            let end = self.listen_once(&fs, &events, &cancel).await?;
            let (progressed, message) = match end {
                ListenEnd::Cancelled => return Ok(()),
                ListenEnd::Closed { progressed } => {
                    (progressed, "the server closed the Listen stream".to_owned())
                }
                ListenEnd::Failed {
                    progressed,
                    message,
                } => (progressed, message),
            };
            if events.is_closed() {
                return Ok(());
            }
            failures = if progressed { 1 } else { failures + 1 };
            if failures > MAX_RECONNECTS {
                return Err(SourceError::Transient(message));
            }
            // Reconnect in place: the document sets survive, so a reset
            // that follows can still be applied.
            let backoff = Duration::from_secs(1 << failures.min(5)).min(RECONNECT_BACKOFF_CAP);
            tracing::debug!(%message, ?backoff, "reconnecting the Firestore Listen stream");
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                _ = tokio::time::sleep(backoff) => {}
            }
        }
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        Ok(())
    }

    fn report_lag(&mut self, reporter: LagReporter) {
        self.lag = Some(reporter);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(config: serde_json::Value) -> SourceSpec {
        SourceSpec {
            kind: SourceKind::Firestore,
            config,
        }
    }

    fn mapping(c: &str) -> StreamMapping {
        serde_json::from_value(json!({"collection": c, "index": "i"})).unwrap()
    }

    #[tokio::test]
    async fn endpoints_are_the_emulator_or_none() {
        let emulator =
            spec(json!({"emulator_host": "fs.example.com:8080", "project_id": "demo-x"}));
        assert_eq!(
            FirestoreSourceFactory.endpoints(&emulator).await.unwrap(),
            vec![Endpoint::new("fs.example.com", 8080)]
        );
        let google = spec(json!({"service_account": crate::config::tests::key_json()}));
        assert!(
            FirestoreSourceFactory
                .endpoints(&google)
                .await
                .unwrap()
                .is_empty()
        );
        let wrong = SourceSpec {
            kind: SourceKind::Mongodb,
            config: json!({}),
        };
        assert!(FirestoreSourceFactory.endpoints(&wrong).await.is_err());
    }

    #[test]
    fn build_validates_collections() {
        let s = spec(json!({"emulator_host": "localhost:8080", "project_id": "demo-x"}));
        assert!(FirestoreSourceFactory.build("s", &s, &[]).is_ok());
        assert!(
            FirestoreSourceFactory
                .build(
                    "s",
                    &s,
                    &[
                        mapping("products"),
                        mapping("**/orders"),
                        mapping("users/a/carts")
                    ]
                )
                .is_ok()
        );
        assert!(matches!(
            FirestoreSourceFactory.build("s", &s, &[mapping("users/a")]),
            Err(SourceError::Config(_))
        ));
    }
}
