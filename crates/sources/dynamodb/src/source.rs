//! `DynamoSource`: the `Source` implementation.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_dynamodb::types::{
    KeyType, ScalarAttributeType, StreamViewType, TableDescription, TableStatus,
};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use futures::StreamExt;
use meili_connect_core::{
    AckReceiver, Capabilities, CheckReport, CheckStatus, CollectionInfo, EventSender, FieldInfo,
    Opened, Source, SourceError, SourceFactory, SourceKind, SourceSpec, StreamMapping,
};
use tokio_util::sync::CancellationToken;

use crate::client::Clients;
use crate::config::DynamoConfig;
use crate::errors::{ErrClass, classify, to_source_error};
use crate::lineage::{Lineage, StreamCheckpoint};
use crate::snapshot::scan_table;
use crate::stream::{self, checkpoint_of, describe_stream, is_disabled};
use crate::util::unix_now;

/// Tables described concurrently by `discover`.
const DISCOVER_CONCURRENCY: usize = 8;
/// Upper bound on tables listed by `discover`.
const MAX_LISTED_TABLES: usize = 10_000;

/// What the stream and snapshot need to know about a table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub name: String,
    /// Partition key, then sort key when there is one.
    pub key_names: Vec<String>,
}

/// One table after `open`.
#[derive(Debug, Clone)]
pub struct TableState {
    pub info: Arc<TableInfo>,
    pub stream_arn: String,
    pub lineage: Lineage,
}

/// Builds [`DynamoSource`]s. Register it for [`SourceKind::Dynamodb`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DynamoSourceFactory;

impl SourceFactory for DynamoSourceFactory {
    fn build(
        &self,
        sync_id: &str,
        spec: &SourceSpec,
        streams: &[StreamMapping],
    ) -> Result<Box<dyn Source>, SourceError> {
        if spec.kind != SourceKind::Dynamodb {
            return Err(SourceError::Config(format!(
                "the dynamodb connector cannot build a `{}` source",
                spec.kind.as_str()
            )));
        }
        let config = DynamoConfig::from_value(&spec.config)?;
        let mut tables: Vec<String> = Vec::new();
        for m in streams {
            if m.collection.trim().is_empty() {
                return Err(SourceError::Config(
                    "a stream mapping has an empty table name".into(),
                ));
            }
            if !tables.contains(&m.collection) {
                tables.push(m.collection.clone());
            }
        }
        Ok(Box::new(DynamoSource::new(sync_id, config, tables)))
    }
}

/// A DynamoDB origin: `Scan` snapshot, DynamoDB Streams changes.
///
/// It creates nothing on AWS (it never enables a stream), so `teardown` is
/// a no-op, and it holds nothing on the origin, so the ack channel is not
/// needed: stream records expire on their own after 24 h.
pub struct DynamoSource {
    sync_id: String,
    config: DynamoConfig,
    tables: Vec<String>,
    clients: Option<Clients>,
    opened: Option<BTreeMap<String, TableState>>,
}

impl std::fmt::Debug for DynamoSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoSource")
            .field("sync_id", &self.sync_id)
            .field("config", &self.config)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl DynamoSource {
    pub fn new(sync_id: &str, config: DynamoConfig, tables: Vec<String>) -> Self {
        Self {
            sync_id: sync_id.to_owned(),
            config,
            tables,
            clients: None,
            opened: None,
        }
    }

    async fn clients(&mut self) -> Clients {
        match &self.clients {
            Some(c) => c.clone(),
            None => {
                let c = Clients::build(&self.config, &self.sync_id).await;
                self.clients = Some(c.clone());
                c
            }
        }
    }

    async fn describe_table(
        &self,
        dynamo: &aws_sdk_dynamodb::Client,
        table: &str,
    ) -> Result<TableDescription, SourceError> {
        let out = dynamo
            .describe_table()
            .table_name(table)
            .send()
            .await
            .map_err(|e| match classify("DescribeTable", &e) {
                (ErrClass::NotFound, _) => SourceError::Config(format!(
                    "table `{table}` does not exist in region {}",
                    self.config.region
                )),
                (class, msg) => to_source_error(class, msg),
            })?;
        out.table.ok_or_else(|| {
            SourceError::Fatal(format!("DescribeTable `{table}` returned no description"))
        })
    }

    /// `check`'s report for one table.
    async fn check_table(&self, clients: &Clients, table: &str, report: &mut CheckReport) {
        let name = format!("table:{table}");
        let desc = match self.describe_table(&clients.dynamo, table).await {
            Ok(d) => d,
            Err(e) => {
                report.push(
                    &name,
                    CheckStatus::Failed,
                    hint(&e.to_string(), "dynamodb:DescribeTable"),
                );
                return;
            }
        };
        match desc.table_status() {
            Some(TableStatus::Active) => report.push(&name, CheckStatus::Ok, "table is ACTIVE"),
            Some(TableStatus::Updating) => report.push(
                &name,
                CheckStatus::Warning,
                "table is UPDATING; it can be read meanwhile",
            ),
            other => report.push(
                &name,
                CheckStatus::Failed,
                format!("table status is {other:?}, it must be ACTIVE"),
            ),
        }

        let stream_name = format!("stream:{table}");
        let arn = match stream_arn(table, &desc) {
            Ok(arn) => {
                report.push(
                    &stream_name,
                    CheckStatus::Ok,
                    "stream enabled with new images",
                );
                arn
            }
            Err(e) => {
                report.push(&stream_name, CheckStatus::Failed, e.to_string());
                return;
            }
        };

        // Exercise each read the sync needs. All of them are read-only.
        let mut missing = Vec::new();
        let mut other = Vec::new();
        let mut note = |action: &str, class: ErrClass, msg: String| {
            if class == ErrClass::Config {
                missing.push(action.to_owned());
            } else {
                other.push(msg);
            }
        };
        if let Err(e) = clients
            .dynamo
            .scan()
            .table_name(table)
            .limit(1)
            .send()
            .await
        {
            let (class, msg) = classify("Scan", &e);
            note("dynamodb:Scan", class, msg);
        }
        let mut shard = None;
        match clients
            .streams
            .describe_stream()
            .stream_arn(&arn)
            .limit(1)
            .send()
            .await
        {
            Ok(out) => {
                shard = out
                    .stream_description
                    .and_then(|d| d.shards)
                    .and_then(|s| s.into_iter().next())
                    .and_then(|s| s.shard_id);
            }
            Err(e) => {
                let (class, msg) = classify("DescribeStream", &e);
                note("dynamodb:DescribeStream", class, msg);
            }
        }
        if let Some(shard) = shard {
            match clients
                .streams
                .get_shard_iterator()
                .stream_arn(&arn)
                .shard_id(shard)
                .shard_iterator_type(ShardIteratorType::Latest)
                .send()
                .await
            {
                Ok(out) => {
                    if let Some(it) = out.shard_iterator
                        && let Err(e) = clients
                            .streams
                            .get_records()
                            .shard_iterator(it)
                            .limit(1)
                            .send()
                            .await
                    {
                        let (class, msg) = classify("GetRecords", &e);
                        note("dynamodb:GetRecords", class, msg);
                    }
                }
                Err(e) => {
                    let (class, msg) = classify("GetShardIterator", &e);
                    note("dynamodb:GetShardIterator", class, msg);
                }
            }
        }
        let perm = format!("permissions:{table}");
        if !missing.is_empty() {
            report.push(
                &perm,
                CheckStatus::Failed,
                format!(
                    "missing permissions: {} (on the table and `{}/stream/*`)",
                    missing.join(", "),
                    desc.table_arn().unwrap_or(table)
                ),
            );
        } else if !other.is_empty() {
            report.push(&perm, CheckStatus::Warning, other.join("; "));
        } else {
            report.push(
                &perm,
                CheckStatus::Ok,
                "Scan, DescribeStream, GetShardIterator and GetRecords allowed",
            );
        }
    }

    async fn list_tables(
        &self,
        dynamo: &aws_sdk_dynamodb::Client,
    ) -> Result<Vec<String>, SourceError> {
        let mut names = Vec::new();
        let mut start: Option<String> = None;
        loop {
            let out = match dynamo
                .list_tables()
                .set_exclusive_start_table_name(start.take())
                .send()
                .await
            {
                Ok(out) => out,
                Err(e) => {
                    let (class, msg) = classify("ListTables", &e);
                    if class == ErrClass::Config {
                        // No dynamodb:ListTables: show the mapped tables only.
                        tracing::debug!(error = %msg, "ListTables denied, discovering mapped tables only");
                        return Ok(self.tables.clone());
                    }
                    return Err(to_source_error(class, msg));
                }
            };
            names.extend(out.table_names.unwrap_or_default());
            match out.last_evaluated_table_name {
                Some(last) if names.len() < MAX_LISTED_TABLES => start = Some(last),
                _ => return Ok(names),
            }
        }
    }
}

/// Extend a failure message with the IAM action to grant when it is a
/// permission problem.
fn hint(msg: &str, action: &str) -> String {
    if msg.contains("AccessDenied") || msg.contains("not authorized") {
        format!("{msg} (grant {action})")
    } else {
        msg.to_owned()
    }
}

/// Partition key then sort key.
fn key_names(desc: &TableDescription) -> Vec<String> {
    let mut keys: Vec<(u8, String)> = desc
        .key_schema()
        .iter()
        .map(|k| {
            let rank = match k.key_type() {
                KeyType::Hash => 0,
                _ => 1,
            };
            (rank, k.attribute_name().to_owned())
        })
        .collect();
    keys.sort_by_key(|(rank, _)| *rank);
    keys.into_iter().map(|(_, name)| name).collect()
}

/// The table's stream ARN, or why it cannot be used.
fn stream_arn(table: &str, desc: &TableDescription) -> Result<String, SourceError> {
    let fix = format!(
        "aws dynamodb update-table --table-name {table} --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES"
    );
    let spec = desc.stream_specification();
    let enabled = spec.is_some_and(|s| s.stream_enabled());
    if !enabled {
        return Err(SourceError::Config(format!(
            "DynamoDB Streams is not enabled on table `{table}`; enable it with view type NEW_AND_OLD_IMAGES: {fix}"
        )));
    }
    match spec.and_then(|s| s.stream_view_type()) {
        Some(StreamViewType::NewImage) | Some(StreamViewType::NewAndOldImages) => {}
        other => {
            let view = other
                .map(|v| v.as_str().to_owned())
                .unwrap_or_else(|| "unknown".into());
            return Err(SourceError::Config(format!(
                "the stream of table `{table}` has view type {view}, which does not carry item images; \
                 disable the stream and re-enable it with NEW_AND_OLD_IMAGES (or NEW_IMAGE): {fix}"
            )));
        }
    }
    desc.latest_stream_arn().map(str::to_owned).ok_or_else(|| {
        SourceError::Config(format!(
            "table `{table}` reports no stream ARN yet; wait for the stream to be enabled"
        ))
    })
}

fn scalar_type(t: &ScalarAttributeType) -> String {
    t.as_str().to_owned()
}

#[async_trait]
impl Source for DynamoSource {
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            snapshot: true,
            patches: false,
        }
    }

    async fn check(&mut self) -> Result<CheckReport, SourceError> {
        let clients = self.clients().await;
        let mut report = CheckReport::default();
        if clients.custom_endpoint {
            report.push(
                "credentials",
                CheckStatus::Warning,
                "custom endpoint: identity not verified with STS",
            );
        } else {
            match clients.sts.get_caller_identity().send().await {
                Ok(id) => report.push(
                    "credentials",
                    CheckStatus::Ok,
                    format!(
                        "authenticated as {}",
                        id.arn().unwrap_or("unknown identity")
                    ),
                ),
                Err(e) => {
                    let (_, msg) = classify("GetCallerIdentity", &e);
                    report.push("credentials", CheckStatus::Failed, msg);
                    return Ok(report);
                }
            }
        }
        if self.tables.is_empty() {
            report.push("tables", CheckStatus::Warning, "no table is mapped yet");
        }
        for table in self.tables.clone() {
            self.check_table(&clients, &table, &mut report).await;
        }
        Ok(report)
    }

    async fn discover(&mut self) -> Result<Vec<CollectionInfo>, SourceError> {
        let clients = self.clients().await;
        let names = self.list_tables(&clients.dynamo).await?;
        let this = &*self;
        let described: Vec<Result<Option<CollectionInfo>, SourceError>> =
            futures::stream::iter(names)
                .map(|name| {
                    let dynamo = clients.dynamo.clone();
                    async move {
                        let desc = match this.describe_table(&dynamo, &name).await {
                            Ok(d) => d,
                            // Deleted between list and describe.
                            Err(SourceError::Config(m)) if m.contains("does not exist") => {
                                return Ok(None);
                            }
                            Err(e) => return Err(e),
                        };
                        let key = key_names(&desc);
                        let fields = desc
                            .attribute_definitions()
                            .iter()
                            .map(|a| FieldInfo {
                                name: a.attribute_name().to_owned(),
                                source_type: scalar_type(a.attribute_type()),
                                nullable: !key.iter().any(|k| k == a.attribute_name()),
                            })
                            .collect();
                        Ok(Some(CollectionInfo {
                            name,
                            key,
                            fields,
                            estimated_rows: desc.item_count().and_then(|n| u64::try_from(n).ok()),
                        }))
                    }
                })
                .buffered(DISCOVER_CONCURRENCY)
                .collect()
                .await;
        let mut out = Vec::with_capacity(described.len());
        for r in described {
            if let Some(info) = r? {
                out.push(info);
            }
        }
        Ok(out)
    }

    async fn open(
        &mut self,
        resume: Option<meili_connect_core::Checkpoint>,
    ) -> Result<Opened, SourceError> {
        self.opened = None;
        if self.tables.is_empty() {
            return Err(SourceError::Config("no DynamoDB table is mapped".into()));
        }
        let clients = self.clients().await;
        let now = unix_now();

        // Describe everything before deciding: the positions below are
        // captured before any Scan (see `lineage` for the reasoning).
        let mut described = Vec::with_capacity(self.tables.len());
        for table in &self.tables {
            let desc = self.describe_table(&clients.dynamo, table).await?;
            let arn = stream_arn(table, &desc)?;
            let info = describe_stream(&clients.streams, &arn)
                .await
                .map_err(|(class, msg)| to_source_error(class, msg))?;
            if is_disabled(info.status.as_ref()) {
                return Err(SourceError::Config(format!(
                    "the stream of table `{table}` is being disabled; re-enable it with view type NEW_AND_OLD_IMAGES"
                )));
            }
            let table_info = Arc::new(TableInfo {
                name: table.clone(),
                key_names: key_names(&desc),
            });
            described.push((table_info, arn, info.shards));
        }

        match resume {
            None => {
                let mut states = BTreeMap::new();
                for (info, arn, shards) in described {
                    tracing::info!(table = %info.name, shards = shards.len(), "capturing stream start positions");
                    states.insert(
                        info.name.clone(),
                        TableState {
                            info,
                            stream_arn: arn,
                            lineage: Lineage::fresh(&shards, now),
                        },
                    );
                }
                let start = checkpoint_of(&states)?;
                self.opened = Some(states);
                Ok(Opened::Fresh { start })
            }
            Some(cp) => {
                let cp: StreamCheckpoint = cp.decode().map_err(|e| {
                    SourceError::PositionLost(format!("unreadable dynamodb checkpoint: {e}"))
                })?;
                let mut states = BTreeMap::new();
                for (info, arn, shards) in described {
                    let table = &info.name;
                    let Some(saved) = cp.0.get(table) else {
                        return Err(SourceError::PositionLost(format!(
                            "table `{table}` has no saved position (newly mapped)"
                        )));
                    };
                    if saved.stream_arn != arn {
                        return Err(SourceError::PositionLost(format!(
                            "the stream of table `{table}` changed (disabled and re-enabled): {} -> {arn}",
                            saved.stream_arn
                        )));
                    }
                    let lineage = Lineage::resume(saved, &shards, now)
                        .map_err(|m| SourceError::PositionLost(format!("table `{table}`: {m}")))?;
                    states.insert(
                        table.clone(),
                        TableState {
                            info,
                            stream_arn: arn,
                            lineage,
                        },
                    );
                }
                self.opened = Some(states);
                Ok(Opened::Resumed)
            }
        }
    }

    async fn snapshot(
        &mut self,
        events: EventSender,
        cancel: CancellationToken,
    ) -> Result<(), SourceError> {
        let Some(opened) = &self.opened else {
            return Err(SourceError::Fatal("snapshot called before open".into()));
        };
        let tables: Vec<Arc<TableInfo>> = opened.values().map(|s| s.info.clone()).collect();
        let clients = self.clients().await;
        for table in tables {
            tracing::info!(table = %table.name, "scanning table");
            let complete =
                scan_table(&clients.dynamo, &table, &self.config.scan, &events, &cancel).await?;
            if !complete {
                if events.is_closed() {
                    return Ok(());
                }
                // Never report an interrupted snapshot as complete.
                return Err(SourceError::Transient(
                    "snapshot cancelled before completion".into(),
                ));
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
        let clients = self.clients().await;
        let opts = self.config.stream.clone();
        let Some(opened) = self.opened.as_mut() else {
            return Err(SourceError::Fatal("stream called before open".into()));
        };
        stream::run(clients.streams, &opts, opened, events, cancel).await
    }

    async fn teardown(&mut self) -> Result<(), SourceError> {
        // Nothing was created on AWS.
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{KeySchemaElement, StreamSpecification};
    use serde_json::json;

    fn table(view: Option<StreamViewType>, enabled: bool) -> TableDescription {
        let mut spec = StreamSpecification::builder().stream_enabled(enabled);
        if let Some(v) = view {
            spec = spec.stream_view_type(v);
        }
        TableDescription::builder()
            .table_name("t")
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sk")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .stream_specification(spec.build().unwrap())
            .latest_stream_arn("arn:aws:dynamodb:x:1:table/t/stream/1")
            .build()
    }

    #[test]
    fn key_order_is_partition_then_sort() {
        assert_eq!(key_names(&table(None, false)), vec!["pk", "sk"]);
    }

    #[test]
    fn stream_view_types() {
        for v in [StreamViewType::NewImage, StreamViewType::NewAndOldImages] {
            assert!(stream_arn("t", &table(Some(v), true)).is_ok());
        }
        for v in [StreamViewType::KeysOnly, StreamViewType::OldImage] {
            match stream_arn("t", &table(Some(v.clone()), true)) {
                Err(SourceError::Config(m)) => {
                    assert!(
                        m.contains(v.as_str()) && m.contains("NEW_AND_OLD_IMAGES"),
                        "{m}"
                    )
                }
                other => panic!("{other:?}"),
            }
        }
        assert!(matches!(
            stream_arn("t", &table(None, false)),
            Err(SourceError::Config(_))
        ));
    }

    #[test]
    fn factory_validates_kind_and_config() {
        let f = DynamoSourceFactory;
        let mapping = |c: &str| StreamMapping {
            collection: c.into(),
            index: "i".into(),
            primary_key: "id".into(),
            fields: Default::default(),
            rename: Default::default(),
            vector: None,
        };
        let spec = SourceSpec {
            kind: SourceKind::Dynamodb,
            config: json!({"region": "eu-west-1"}),
        };
        assert!(
            f.build("s1", &spec, &[mapping("a"), mapping("a"), mapping("b")])
                .is_ok()
        );
        let wrong = SourceSpec {
            kind: SourceKind::Postgres,
            config: json!({"region": "eu-west-1"}),
        };
        assert!(matches!(
            f.build("s1", &wrong, &[]),
            Err(SourceError::Config(_))
        ));
        let bad = SourceSpec {
            kind: SourceKind::Dynamodb,
            config: json!({"region": 3}),
        };
        assert!(matches!(
            f.build("s1", &bad, &[]),
            Err(SourceError::Config(_))
        ));
        assert!(matches!(
            f.build("s1", &spec, &[mapping(" ")]),
            Err(SourceError::Config(_))
        ));
    }

    #[tokio::test]
    async fn open_without_tables_is_a_config_error() {
        let config = DynamoConfig::from_value(&json!({"region": "eu-west-1"})).unwrap();
        let mut s = DynamoSource::new("s", config, vec![]);
        assert!(matches!(s.open(None).await, Err(SourceError::Config(_))));
        assert!(s.teardown().await.is_ok());
        assert!(s.teardown().await.is_ok());
    }
}
