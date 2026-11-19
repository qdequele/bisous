//! `MongoSource`: check, discover, open, snapshot, stream.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::TryStreamExt as _;
use meili_connect_core::{
    AckReceiver, Capabilities, Change, CheckReport, CheckStatus, CollectionInfo, EventSender,
    FieldInfo, Op, Opened, Source, SourceError, SourceEvent, StreamMapping,
};
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::ResumeToken;
use mongodb::options::{ClientOptions, FullDocumentType};
use mongodb::results::CollectionType;
use mongodb::{Client, Database};
use tokio_util::sync::CancellationToken;

use crate::config::{FullDocumentMode, MongoConfig, Target, resolve_targets};
use crate::convert::{bson_to_json, document_to_json};
use crate::error::{Scrubber, codes, error_code, map_error, map_resume_error};
use crate::event::{Mapped, Namespaces, map_event};
use crate::position::Position;

/// Longest a `getMore` waits for new events; also the cancellation latency
/// of an idle stream.
const MAX_AWAIT: Duration = Duration::from_secs(1);
/// Emit an idle keep-alive checkpoint at most this often.
const IDLE_CHECKPOINT: Duration = Duration::from_secs(5);
/// Emit a checkpoint at least every this many events, even mid-batch.
const EVENTS_PER_CHECKPOINT: usize = 1_000;
/// Change stream batch size (bounded memory).
const STREAM_BATCH: u32 = 1_000;
/// Documents sampled per collection by `discover`.
const DISCOVER_SAMPLE: i64 = 20;
/// Oplog windows shorter than this are reported as a warning.
const SHORT_OPLOG: Duration = Duration::from_secs(24 * 3600);

pub struct MongoSource {
    sync_id: String,
    config: MongoConfig,
    targets: Vec<Target>,
    names: Namespaces,
    scrub: Scrubber,
    client: Option<Client>,
    position: Option<Position>,
    /// The stream `open(Some(..))` validated, reused by `stream`. In a
    /// mutex only to make the source `Sync` (it is never contended: every
    /// access goes through `&mut self`).
    stream: std::sync::Mutex<Option<Held>>,
    idle_checkpoint: Duration,
}

impl std::fmt::Debug for MongoSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoSource")
            .field("sync_id", &self.sync_id)
            .field("config", &self.config)
            .field("targets", &self.targets)
            .field("position", &self.position)
            .finish_non_exhaustive()
    }
}

/// A stream opened and validated by `open`, with the event (if any) its
/// validating read returned.
struct Held {
    stream: ChangeStream<Document>,
    first: Option<(Document, Option<ResumeToken>)>,
}

enum Flow {
    Continue,
    Stop,
}

async fn send(events: &EventSender, cancel: &CancellationToken, event: SourceEvent) -> Flow {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Flow::Stop,
        sent = events.send(event) => if sent.is_ok() { Flow::Continue } else { Flow::Stop },
    }
}

impl MongoSource {
    pub fn new(
        sync_id: &str,
        config: MongoConfig,
        streams: &[StreamMapping],
    ) -> Result<Self, SourceError> {
        let targets = resolve_targets(&config, streams)?;
        let names = Namespaces::new(&targets);
        let scrub = Scrubber::new(&config.uri);
        Ok(Self {
            sync_id: sync_id.to_owned(),
            config,
            targets,
            names,
            scrub,
            client: None,
            position: None,
            stream: std::sync::Mutex::new(None),
            idle_checkpoint: IDLE_CHECKPOINT,
        })
    }

    fn held_stream(&mut self) -> &mut Option<Held> {
        self.stream
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Shorten the idle keep-alive interval (tests).
    #[doc(hidden)]
    pub fn set_idle_checkpoint_interval(&mut self, every: Duration) {
        self.idle_checkpoint = every;
    }

    fn err(&self, e: &mongodb::error::Error, context: &str) -> SourceError {
        map_error(e, context, &self.scrub)
    }

    async fn client(&mut self) -> Result<Client, SourceError> {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }
        let mut opts = ClientOptions::parse(self.config.uri.expose())
            .await
            .map_err(|e| self.err(&e, "parsing the connection string"))?;
        if opts.app_name.is_none() {
            opts.app_name = Some("meili-connect".into());
        }
        if opts.server_selection_timeout.is_none() {
            opts.server_selection_timeout = Some(Duration::from_secs(15));
        }
        let client = Client::with_options(opts).map_err(|e| self.err(&e, "creating the client"))?;
        self.client = Some(client.clone());
        Ok(client)
    }

    /// One database when every mapping lives in it, else the deployment.
    fn single_db(&self) -> Option<&str> {
        let first = self.targets.first()?.db.as_str();
        self.targets.iter().all(|t| t.db == first).then_some(first)
    }

    /// `$match` restricting the stream to the mapped namespaces (and the
    /// events that affect them: renames into them, dropped databases,
    /// invalidations).
    fn pipeline(&self) -> Vec<Document> {
        let mut or: Vec<Bson> = Vec::with_capacity(self.targets.len() * 2 + 2);
        for t in &self.targets {
            or.push(Bson::Document(doc! {"ns.db": &t.db, "ns.coll": &t.coll}));
            or.push(Bson::Document(doc! {"to.db": &t.db, "to.coll": &t.coll}));
        }
        let dbs: BTreeSet<&str> = self.targets.iter().map(|t| t.db.as_str()).collect();
        or.push(Bson::Document(doc! {
            "operationType": "dropDatabase",
            "ns.db": {"$in": dbs.into_iter().collect::<Vec<_>>()},
        }));
        or.push(Bson::Document(doc! {"operationType": "invalidate"}));
        vec![doc! {"$match": {"$or": or}}]
    }

    async fn open_stream(
        &self,
        client: &Client,
        from: Option<&Position>,
    ) -> Result<ChangeStream<Document>, SourceError> {
        let db: Option<Database> = self.single_db().map(|d| client.database(d));
        let mut watch = match &db {
            Some(db) => db.watch(),
            None => client.watch(),
        }
        .pipeline(self.pipeline())
        .max_await_time(MAX_AWAIT)
        .batch_size(STREAM_BATCH);
        let full_document = match self.config.full_document {
            FullDocumentMode::UpdateLookup => Some(FullDocumentType::UpdateLookup),
            FullDocumentMode::WhenAvailable => Some(FullDocumentType::WhenAvailable),
            FullDocumentMode::Required => Some(FullDocumentType::Required),
            FullDocumentMode::Default => None,
        };
        if let Some(fd) = full_document {
            watch = watch.full_document(fd);
        }
        if let Some(pos) = from {
            // `startAfter` (4.2+) also accepts the token of an
            // `invalidate` event, unlike `resumeAfter`.
            if let Some(token) = pos.token()? {
                watch = watch.start_after(token);
            } else if let Some(ts) = pos.timestamp() {
                watch = watch.start_at_operation_time(ts);
            }
        }
        let stream = watch
            .await
            .map_err(|e| self.err(&e, "opening the change stream"))?;
        Ok(stream.with_type::<Document>())
    }

    async fn current_operation_time(&self, client: &Client) -> Result<Position, SourceError> {
        let reply = client
            .database("admin")
            .run_command(doc! {"ping": 1})
            .await
            .map_err(|e| self.err(&e, "reading the cluster time"))?;
        match reply.get("operationTime") {
            Some(Bson::Timestamp(ts)) => Ok(Position::from_timestamp(*ts)),
            _ => Err(SourceError::Config(
                "the server returned neither a resume token nor an operation time; \
                 change streams need a replica set or sharded cluster"
                    .into(),
            )),
        }
    }

    async fn lookup(
        &self,
        client: &Client,
        db: &str,
        coll: &str,
        id: &Bson,
    ) -> Result<Option<Document>, SourceError> {
        client
            .database(db)
            .collection::<Document>(coll)
            .find_one(doc! {"_id": id.clone()})
            .await
            .map_err(|e| self.err(&e, "looking up an updated document"))
    }

    async fn check_inner(&mut self, report: &mut CheckReport) -> Result<(), SourceError> {
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => {
                report.push("connection", CheckStatus::Failed, e.to_string());
                return Ok(());
            }
        };
        let admin = client.database("admin");
        let hello = match admin.run_command(doc! {"hello": 1}).await {
            Ok(h) => h,
            Err(e) => {
                report.push(
                    "connection",
                    CheckStatus::Failed,
                    self.err(&e, "connecting").to_string(),
                );
                return Ok(());
            }
        };
        report.push("connection", CheckStatus::Ok, "connected");

        let sharded = hello.get_str("msg").is_ok_and(|m| m == "isdbgrid");
        let replica_set = hello.get_str("setName").ok().map(str::to_owned);
        let change_streams_possible = sharded || replica_set.is_some();
        match (&replica_set, sharded) {
            (_, true) => report.push("topology", CheckStatus::Ok, "sharded cluster (mongos)"),
            (Some(name), false) => {
                report.push("topology", CheckStatus::Ok, format!("replica set {name:?}"))
            }
            (None, false) => report.push(
                "topology",
                CheckStatus::Failed,
                "standalone server: change streams need a replica set or a sharded cluster. \
                 Convert it to a single-node replica set: restart mongod with `--replSet rs0` \
                 and run `rs.initiate()` once",
            ),
        }

        match admin.run_command(doc! {"buildInfo": 1}).await {
            Ok(info) => {
                let version = info.get_str("version").unwrap_or("unknown").to_owned();
                let mut parts = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
                let (major, minor) = (parts.next().unwrap_or(0), parts.next().unwrap_or(0));
                if (major, minor) < (4, 2) {
                    report.push(
                        "server_version",
                        CheckStatus::Failed,
                        format!("MongoDB {version}: version 4.2 or later is required"),
                    );
                } else {
                    report.push(
                        "server_version",
                        CheckStatus::Ok,
                        format!("MongoDB {version}"),
                    );
                }
            }
            Err(e) => report.push(
                "server_version",
                CheckStatus::Warning,
                self.err(&e, "buildInfo").to_string(),
            ),
        }

        if self.targets.is_empty() {
            report.push(
                "collections",
                CheckStatus::Warning,
                "no collection is mapped",
            );
        }
        for t in &self.targets {
            let name = format!("collection {}.{}", t.db, t.coll);
            let db = client.database(&t.db);
            match db.list_collections().filter(doc! {"name": &t.coll}).await {
                Ok(cursor) => {
                    let specs: Vec<_> = match cursor.try_collect().await {
                        Ok(s) => s,
                        Err(e) => {
                            report.push(
                                &name,
                                CheckStatus::Failed,
                                self.err(&e, "listCollections").to_string(),
                            );
                            continue;
                        }
                    };
                    match specs.first().map(|s| &s.collection_type) {
                        None => {
                            report.push(&name, CheckStatus::Failed, "collection does not exist");
                            continue;
                        }
                        Some(CollectionType::View) => {
                            report.push(
                                &name,
                                CheckStatus::Failed,
                                "is a view: change streams cannot watch views",
                            );
                            continue;
                        }
                        Some(_) => {}
                    }
                }
                Err(e) => {
                    report.push(
                        &name,
                        CheckStatus::Failed,
                        self.err(&e, "listCollections").to_string(),
                    );
                    continue;
                }
            }
            let coll = db.collection::<Document>(&t.coll);
            if let Err(e) = coll.find_one(doc! {}).await {
                report.push(&name, CheckStatus::Failed, self.err(&e, "find").to_string());
                continue;
            }
            if change_streams_possible {
                // Opening a change stream creates nothing on the server.
                if let Err(e) = coll.watch().max_await_time(Duration::from_millis(1)).await {
                    report.push(
                        &name,
                        CheckStatus::Failed,
                        self.err(&e, "changeStream").to_string(),
                    );
                    continue;
                }
                report.push(&name, CheckStatus::Ok, "find and changeStream allowed");
            } else {
                report.push(&name, CheckStatus::Ok, "find allowed");
            }
        }

        if replica_set.is_some() {
            self.check_oplog(&client, report).await;
        }
        Ok(())
    }

    async fn check_oplog(&self, client: &Client, report: &mut CheckReport) {
        let oplog = client.database("local").collection::<Document>("oplog.rs");
        let first = oplog
            .find_one(doc! {})
            .sort(doc! {"$natural": 1})
            .projection(doc! {"ts": 1})
            .await;
        let last = oplog
            .find_one(doc! {})
            .sort(doc! {"$natural": -1})
            .projection(doc! {"ts": 1})
            .await;
        match (first, last) {
            (Ok(Some(first)), Ok(Some(last))) => {
                let (Ok(a), Ok(b)) = (first.get_timestamp("ts"), last.get_timestamp("ts")) else {
                    return;
                };
                let window = Duration::from_secs(u64::from(b.time.saturating_sub(a.time)));
                let hours = window.as_secs_f64() / 3600.0;
                if window < SHORT_OPLOG {
                    report.push(
                        "oplog_window",
                        CheckStatus::Warning,
                        format!(
                            "the oplog covers {hours:.1} h: a sync stopped longer than that \
                             must resync from scratch. Grow the oplog or set a minimum \
                             retention (Atlas: Additional Settings > Set Minimum Oplog Window)"
                        ),
                    );
                } else {
                    report.push(
                        "oplog_window",
                        CheckStatus::Ok,
                        format!("the oplog covers {hours:.1} h"),
                    );
                }
            }
            (Err(e), _) | (_, Err(e)) if error_code(&e) == Some(codes::UNAUTHORIZED) => {
                report.push(
                    "oplog_window",
                    CheckStatus::Ok,
                    "not readable with these credentials (needs read on `local`); skipped",
                );
            }
            _ => {}
        }
    }
}

fn bson_type_name(v: &Bson) -> &'static str {
    match v {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binData",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "maxKey",
        Bson::MinKey => "minKey",
        Bson::DbPointer(_) => "dbPointer",
    }
}

/// Top-level fields seen in a sample, with every BSON type seen (`int|long`).
/// A field is nullable when it is missing from or null in any sampled
/// document.
pub(crate) fn infer_fields(sample: &[Document]) -> Vec<FieldInfo> {
    let mut seen: BTreeMap<&str, (BTreeSet<&'static str>, usize, bool)> = BTreeMap::new();
    for doc in sample {
        for (k, v) in doc {
            let entry = seen.entry(k.as_str()).or_default();
            entry.1 += 1;
            if matches!(v, Bson::Null | Bson::Undefined) {
                entry.2 = true;
            } else {
                entry.0.insert(bson_type_name(v));
            }
        }
    }
    let mut fields: Vec<FieldInfo> = seen
        .into_iter()
        .map(|(name, (types, count, null))| FieldInfo {
            name: name.to_owned(),
            source_type: if types.is_empty() {
                "null".to_owned()
            } else {
                types.into_iter().collect::<Vec<_>>().join("|")
            },
            nullable: null || count < sample.len(),
        })
        .collect();
    // `_id` first, like MongoDB shows it.
    fields.sort_by_key(|f| f.name != "_id");
    fields
}

#[async_trait]
impl Source for MongoSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: true,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        let mut report = CheckReport::default();
        self.check_inner(&mut report).await?;
        Ok(report)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let client = self.client().await?;
        let dbs: Vec<String> = match &self.config.database {
            Some(db) => vec![db.clone()],
            None => client
                .list_database_names()
                .await
                .map_err(|e| self.err(&e, "listing databases"))?
                .into_iter()
                .filter(|d| !matches!(d.as_str(), "admin" | "local" | "config"))
                .collect(),
        };
        let mut out = Vec::new();
        for db_name in dbs {
            let db = client.database(&db_name);
            let specs: Vec<_> = db
                .list_collections()
                .await
                .map_err(|e| self.err(&e, "listing collections"))?
                .try_collect()
                .await
                .map_err(|e| self.err(&e, "listing collections"))?;
            let mut names: Vec<String> = specs
                .into_iter()
                .filter(|s| s.collection_type == CollectionType::Collection)
                .map(|s| s.name)
                .filter(|n| !n.starts_with("system."))
                .collect();
            names.sort();
            for name in names {
                let coll = db.collection::<Document>(&name);
                let estimated_rows = coll.estimated_document_count().await.ok();
                let sample: Vec<Document> = match coll
                    .aggregate(vec![doc! {"$sample": {"size": DISCOVER_SAMPLE}}])
                    .await
                {
                    Ok(cursor) => cursor.try_collect().await.unwrap_or_default(),
                    Err(_) => Vec::new(),
                };
                out.push(CollectionInfo {
                    name: format!("{db_name}.{name}"),
                    key: vec!["_id".to_owned()],
                    fields: infer_fields(&sample),
                    estimated_rows,
                });
            }
        }
        Ok(out)
    }

    async fn open(
        &mut self,
        resume: Option<meili_connect_core::Checkpoint>,
    ) -> Result<Opened, SourceError> {
        if self.targets.is_empty() {
            return Err(SourceError::Config("no collection is mapped".into()));
        }
        let client = self.client().await?;
        *self.held_stream() = None;
        match resume {
            None => {
                // The position is taken from a stream opened *now*, before
                // any snapshot read: everything the snapshot might miss is
                // after it.
                let stream = self.open_stream(&client, None).await?;
                let position = match stream.resume_token() {
                    Some(token) => Position::from_token(&token)?,
                    None => self.current_operation_time(&client).await?,
                };
                drop(stream);
                let start = position.to_checkpoint()?;
                tracing::info!(sync_id = %self.sync_id, "mongodb: fresh start position captured");
                self.position = Some(position);
                Ok(Opened::Fresh { start })
            }
            Some(cp) => {
                let position = Position::from_checkpoint(&cp)?;
                // The server checks the resume point lazily, on the first
                // read: open the stream and read once (at most one event,
                // kept for `stream`). A token older than the oplog fails
                // with ChangeStreamHistoryLost (→ PositionLost). Any other
                // error keeps its own classification: a blip is retried,
                // not turned into a full resync.
                let mut stream = self.open_stream(&client, Some(&position)).await?;
                let first = stream
                    .next_if_any()
                    .await
                    .map_err(|e| map_resume_error(&e, "resuming the change stream", &self.scrub))?
                    .map(|ev| (ev, stream.resume_token()));
                tracing::info!(sync_id = %self.sync_id, "mongodb: resumed from checkpoint");
                *self.held_stream() = Some(Held { stream, first });
                self.position = Some(position);
                Ok(Opened::Resumed)
            }
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let client = self.client().await?;
        let batch = self.config.snapshot_batch_size;
        for t in self.targets.clone() {
            let coll = client.database(&t.db).collection::<Document>(&t.coll);
            let mut cursor = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                c = coll.find(doc! {}).batch_size(batch) => c.map_err(|e| self.err(&e, "snapshot find"))?,
            };
            let mut count: u64 = 0;
            loop {
                let next = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    n = cursor.try_next() => n.map_err(|e| self.err(&e, "snapshot read"))?,
                };
                let Some(doc) = next else { break };
                let id = doc.get("_id").ok_or_else(|| {
                    SourceError::Fatal(format!("document without _id in {}", t.name))
                })?;
                let change = Change {
                    collection: t.name.clone(),
                    key: vec![bson_to_json(id)],
                    op: Op::Upsert {
                        doc: document_to_json(&doc),
                    },
                };
                if let Flow::Stop = send(&events, &cancel, SourceEvent::Change(change)).await {
                    return Ok(());
                }
                count += 1;
            }
            tracing::info!(sync_id = %self.sync_id, collection = %t.name, documents = count, "mongodb: snapshot of collection done");
        }
        Ok(())
    }

    async fn stream(
        &mut self,
        events: EventSender,
        acks: AckReceiver,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        // Resume tokens hold nothing on the server: acks need no action.
        drop(acks);
        let client = self.client().await?;
        let mut position = self
            .position
            .clone()
            .ok_or_else(|| SourceError::Fatal("stream called before open".into()))?;
        let held = self.held_stream().take();
        let (mut stream, mut pending) = match held {
            Some(h) => (h.stream, h.first),
            None => (
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    s = self.open_stream(&client, Some(&position)) => s?,
                },
                None,
            ),
        };
        // The open position is already durable (the engine saved it or
        // resumed from it).
        let mut emitted = position.clone();
        let mut since_checkpoint: usize = 0;
        let mut last_checkpoint = Instant::now();

        loop {
            let (event, token): (Option<Document>, Option<ResumeToken>) =
                if let Some((ev, token)) = pending.take() {
                    (Some(ev), token)
                } else {
                    let next = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        n = stream.next_if_any() => n,
                    };
                    match next {
                        Ok(e) => (e, stream.resume_token()),
                        Err(e) => return Err(self.err(&e, "reading the change stream")),
                    }
                };

            let Some(event) = event else {
                // The batch is exhausted: the post-batch resume token covers
                // everything seen, including events filtered out by $match.
                if let Some(t) = &token {
                    position = Position::from_token(t)?;
                }
                let moved = position != emitted;
                if moved
                    && (since_checkpoint > 0 || last_checkpoint.elapsed() >= self.idle_checkpoint)
                {
                    if let Flow::Stop = send(
                        &events,
                        &cancel,
                        SourceEvent::Checkpoint(position.to_checkpoint()?),
                    )
                    .await
                    {
                        return Ok(());
                    }
                    emitted = position.clone();
                    last_checkpoint = Instant::now();
                    since_checkpoint = 0;
                }
                if !stream.is_alive() {
                    stream = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        s = self.open_stream(&client, Some(&position)) => s?,
                    };
                }
                continue;
            };

            match map_event(&event, &self.names)? {
                Mapped::Events(out) => {
                    for e in out {
                        if let Flow::Stop = send(&events, &cancel, e).await {
                            return Ok(());
                        }
                    }
                }
                Mapped::Lookup {
                    collection,
                    db,
                    coll,
                    id,
                } => {
                    let found = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        f = self.lookup(&client, &db, &coll, &id) => f?,
                    };
                    // Missing: deleted since; its delete event follows.
                    if let Some(doc) = found {
                        let change = Change {
                            collection,
                            key: vec![bson_to_json(&id)],
                            op: Op::Upsert {
                                doc: document_to_json(&doc),
                            },
                        };
                        if let Flow::Stop =
                            send(&events, &cancel, SourceEvent::Change(change)).await
                        {
                            return Ok(());
                        }
                    }
                }
                Mapped::Invalidate => {
                    let Some(t) = &token else {
                        return Err(SourceError::Fatal(
                            "invalidate event without a resume token".into(),
                        ));
                    };
                    position = Position::from_token(t)?;
                    if let Flow::Stop = send(
                        &events,
                        &cancel,
                        SourceEvent::Checkpoint(position.to_checkpoint()?),
                    )
                    .await
                    {
                        return Ok(());
                    }
                    emitted = position.clone();
                    last_checkpoint = Instant::now();
                    since_checkpoint = 0;
                    tracing::info!(sync_id = %self.sync_id, "mongodb: change stream invalidated, reopening");
                    drop(stream);
                    stream = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        s = self.open_stream(&client, Some(&position)) => s?,
                    };
                    continue;
                }
            }

            if let Some(t) = &token {
                position = Position::from_token(t)?;
            }
            since_checkpoint += 1;
            if since_checkpoint >= EVENTS_PER_CHECKPOINT && position != emitted {
                if let Flow::Stop = send(
                    &events,
                    &cancel,
                    SourceEvent::Checkpoint(position.to_checkpoint()?),
                )
                .await
                {
                    return Ok(());
                }
                emitted = position.clone();
                last_checkpoint = Instant::now();
                since_checkpoint = 0;
            }
            self.position = Some(position.clone());
        }
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        // A change stream leaves nothing behind on the server.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    #[test]
    fn field_inference() {
        let sample = vec![
            doc! {"_id": ObjectId::new(), "name": "a", "n": 1_i32, "tags": ["x"]},
            doc! {"_id": ObjectId::new(), "name": Bson::Null, "n": 2_i64},
        ];
        let fields = infer_fields(&sample);
        let get = |n: &str| fields.iter().find(|f| f.name == n).unwrap().clone();
        assert_eq!(fields[0].name, "_id");
        assert_eq!(get("_id").source_type, "objectId");
        assert!(!get("_id").nullable);
        assert!(get("name").nullable);
        assert_eq!(get("name").source_type, "string");
        assert_eq!(get("n").source_type, "int|long");
        assert!(!get("n").nullable);
        assert!(get("tags").nullable);
    }

    #[test]
    fn pipeline_scope() {
        let config = MongoConfig::from_json(&serde_json::json!({"uri": "mongodb://h"})).unwrap();
        let streams: Vec<StreamMapping> = serde_json::from_value(serde_json::json!([
            {"collection": "a.x", "index": "x"},
            {"collection": "a.y", "index": "y"},
        ]))
        .unwrap();
        let s = MongoSource::new("s", config.clone(), &streams).unwrap();
        assert_eq!(s.single_db(), Some("a"));
        let p = s.pipeline();
        assert_eq!(p.len(), 1);
        let or = p[0]
            .get_document("$match")
            .unwrap()
            .get_array("$or")
            .unwrap();
        assert_eq!(or.len(), 6);

        let streams: Vec<StreamMapping> = serde_json::from_value(serde_json::json!([
            {"collection": "a.x", "index": "x"},
            {"collection": "b.y", "index": "y"},
        ]))
        .unwrap();
        let s = MongoSource::new("s", config, &streams).unwrap();
        assert_eq!(s.single_db(), None);
    }
}
