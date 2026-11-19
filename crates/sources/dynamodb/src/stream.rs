//! Reading DynamoDB Streams.
//!
//! One coordinator loop owns every table's [`Lineage`] and runs one
//! `GetRecords` step per ready shard as a task, with at most
//! `max_concurrent_shards` steps in flight across tables. A step's records
//! are forwarded to the engine by the coordinator, then the shard position
//! advances, so a `Checkpoint` event always follows the changes it covers.
//!
//! Ordering: DynamoDB only orders the records of one item, along a shard
//! lineage. Parents are read to their end before their children, so
//! per-item order is kept; order *across* items (and so across shards) is
//! not preserved, which is fine for idempotent keyed upserts and deletes.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_dynamodbstreams::types::{ShardIteratorType, StreamStatus};
use meili_connect_core::{
    Change, Checkpoint, EventSender, Lag, LagReporter, SourceError, SourceEvent,
    seconds_since_unix_millis,
};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::StreamOptions;
use crate::convert::record_to_change;
use crate::errors::{ErrClass, classify, to_source_error};
use crate::lineage::{ShardDesc, StartAt, StreamCheckpoint};
use crate::source::{TableInfo, TableState};
use crate::util::{backoff, send, sleep, unix_now};

/// `GetRecords` returns at most 1000 records / 1 MB per call.
const RECORDS_PER_CALL: i32 = 1000;
/// Gap between two calls on a busy shard: DynamoDB Streams allows 5
/// `GetRecords` per second per shard.
const BUSY_POLL: Duration = Duration::from_millis(200);
/// Re-describe soon after a shard ends, to pick up its children.
const REFRESH_AFTER_CLOSE: Duration = Duration::from_secs(2);
/// Consecutive throttled or failed steps on one shard before giving up.
const MAX_SHARD_FAILURES: u32 = 12;
/// Cap on `DescribeStream` pages, a guard against a runaway pagination.
const MAX_DESCRIBE_PAGES: usize = 10_000;

/// A stream as `DescribeStream` reports it.
#[derive(Debug, Clone)]
pub struct StreamInfo {
    pub status: Option<StreamStatus>,
    pub shards: Vec<ShardDesc>,
}

/// Describe a stream, following `LastEvaluatedShardId`.
pub async fn describe_stream(
    client: &aws_sdk_dynamodbstreams::Client,
    arn: &str,
) -> Result<StreamInfo, (ErrClass, String)> {
    let mut shards = Vec::new();
    let mut start: Option<String> = None;
    for _ in 0..MAX_DESCRIBE_PAGES {
        let out = client
            .describe_stream()
            .stream_arn(arn)
            .set_exclusive_start_shard_id(start.take())
            .send()
            .await
            .map_err(|e| classify("DescribeStream", &e))?;
        let Some(desc) = out.stream_description else {
            return Err((
                ErrClass::Fatal,
                "DescribeStream returned no description".into(),
            ));
        };
        let status = desc.stream_status.clone();
        for s in desc.shards.unwrap_or_default() {
            let Some(id) = s.shard_id else { continue };
            shards.push(ShardDesc {
                id,
                parent: s.parent_shard_id,
                closed: s
                    .sequence_number_range
                    .as_ref()
                    .and_then(|r| r.ending_sequence_number())
                    .is_some(),
            });
        }
        match desc.last_evaluated_shard_id {
            Some(last) => start = Some(last),
            None => return Ok(StreamInfo { status, shards }),
        }
    }
    Err((
        ErrClass::Fatal,
        format!("DescribeStream returned more than {MAX_DESCRIBE_PAGES} pages"),
    ))
}

pub fn is_disabled(status: Option<&StreamStatus>) -> bool {
    matches!(
        status,
        Some(StreamStatus::Disabled) | Some(StreamStatus::Disabling)
    )
}

/// The whole position map as a core checkpoint.
pub fn checkpoint_of(tables: &BTreeMap<String, TableState>) -> Result<Checkpoint, SourceError> {
    let cp = StreamCheckpoint(
        tables
            .iter()
            .map(|(name, st)| (name.clone(), st.lineage.to_checkpoint(&st.stream_arn)))
            .collect(),
    );
    Checkpoint::new(&cp).map_err(|e| SourceError::Fatal(format!("checkpoint encoding: {e}")))
}

type Key = (String, String);

#[derive(Default)]
struct Cursor {
    iterator: Option<String>,
    failures: u32,
}

enum StepOutcome {
    Records {
        changes: Vec<Change>,
        last_seq: Option<String>,
        next: Option<String>,
        polled_at: u64,
        /// `ApproximateCreationDateTime` of the newest record, unix ms.
        newest_ms: Option<i64>,
    },
    Expired,
    Retry(String),
    Cancelled,
}

struct Step {
    client: aws_sdk_dynamodbstreams::Client,
    table: Arc<TableInfo>,
    stream_arn: String,
    shard: String,
    start: StartAt,
    iterator: Option<String>,
    delay: Duration,
    cancel: CancellationToken,
}

impl Step {
    async fn run(self) -> Result<StepOutcome, SourceError> {
        if !sleep(self.delay, &self.cancel).await {
            return Ok(StepOutcome::Cancelled);
        }
        let iterator = match self.iterator.clone() {
            Some(it) => it,
            None => match self.iterator().await? {
                Ok(it) => it,
                Err(outcome) => return Ok(outcome),
            },
        };
        let polled_at = unix_now();
        let result = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Ok(StepOutcome::Cancelled),
            r = self.client.get_records().shard_iterator(iterator).limit(RECORDS_PER_CALL).send() => r,
        };
        match result {
            Ok(out) => {
                let records = out.records.unwrap_or_default();
                let mut changes = Vec::with_capacity(records.len());
                let mut last_seq = None;
                let newest_ms = records
                    .iter()
                    .filter_map(|r| r.dynamodb()?.approximate_creation_date_time())
                    .filter_map(|t| t.to_millis().ok())
                    .max();
                for r in &records {
                    let (change, seq) =
                        record_to_change(&self.table.name, &self.table.key_names, r)?;
                    changes.push(change);
                    if seq.is_some() {
                        last_seq = seq;
                    }
                }
                Ok(StepOutcome::Records {
                    changes,
                    last_seq,
                    next: out.next_shard_iterator,
                    polled_at,
                    newest_ms,
                })
            }
            Err(e) => {
                let (class, msg) = classify("GetRecords", &e);
                self.react(class, msg)
            }
        }
    }

    /// A fresh iterator, or the outcome to report instead.
    async fn iterator(&self) -> Result<Result<String, StepOutcome>, SourceError> {
        let mut req = self
            .client
            .get_shard_iterator()
            .stream_arn(&self.stream_arn)
            .shard_id(&self.shard);
        req = match &self.start {
            StartAt::TrimHorizon => req.shard_iterator_type(ShardIteratorType::TrimHorizon),
            StartAt::AfterSequence(seq) => req
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .sequence_number(seq),
        };
        let result = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => return Ok(Err(StepOutcome::Cancelled)),
            r = req.send() => r,
        };
        match result {
            Ok(out) => match out.shard_iterator {
                Some(it) => Ok(Ok(it)),
                None => Err(SourceError::Transient(format!(
                    "GetShardIterator returned no iterator for shard {}",
                    self.shard
                ))),
            },
            Err(e) => {
                let (class, msg) = classify("GetShardIterator", &e);
                self.react(class, msg).map(Err)
            }
        }
    }

    fn react(&self, class: ErrClass, msg: String) -> Result<StepOutcome, SourceError> {
        let shard = &self.shard;
        let table = &self.table.name;
        match class {
            ErrClass::ExpiredIterator => Ok(StepOutcome::Expired),
            ErrClass::Throttle | ErrClass::Transient => Ok(StepOutcome::Retry(msg)),
            ErrClass::Lost => Err(SourceError::PositionLost(format!(
                "table `{table}`, shard {shard}: records were trimmed before being read ({msg})"
            ))),
            ErrClass::NotFound => Err(SourceError::PositionLost(format!(
                "table `{table}`, shard {shard}: the shard or stream no longer exists ({msg})"
            ))),
            other => Err(to_source_error(other, msg)),
        }
    }
}

/// The lag of every shard read so far: `now − ApproximateCreationDateTime`
/// of its newest record, `0` once a read returns nothing. The stream's lag
/// is the worst shard's.
#[derive(Default)]
struct ShardLags(HashMap<Key, f64>);

impl ShardLags {
    fn observe(&mut self, key: &Key, had_records: bool, newest_ms: Option<i64>) {
        let seconds = match (had_records, newest_ms) {
            (false, _) => 0.0,
            (true, Some(ms)) => seconds_since_unix_millis(ms),
            (true, None) => return,
        };
        self.0.insert(key.clone(), seconds);
    }

    fn forget(&mut self, key: &Key) {
        self.0.remove(key);
    }

    fn worst(&self) -> Option<f64> {
        self.0.values().copied().reduce(f64::max)
    }
}

/// Run until cancelled or the engine goes away.
pub async fn run(
    client: aws_sdk_dynamodbstreams::Client,
    opts: &StreamOptions,
    tables: &mut BTreeMap<String, TableState>,
    events: EventSender,
    lag: Option<LagReporter>,
    cancel: CancellationToken,
) -> Result<(), SourceError> {
    let mut lags = ShardLags::default();
    let mut cursors: HashMap<Key, Cursor> = HashMap::new();
    // Shards queued or in flight.
    let mut scheduled: HashSet<Key> = HashSet::new();
    let mut pending: VecDeque<(Key, Duration)> = VecDeque::new();
    let mut tasks: JoinSet<(Key, Result<StepOutcome, SourceError>)> = JoinSet::new();
    let mut dirty = false;
    let mut last_emit: Option<Instant> = None;
    // The stream may have moved on during a long snapshot: describe first.
    let mut next_refresh = Instant::now();
    let mut next_tick = Instant::now() + opts.checkpoint_interval();

    loop {
        for (table, st) in tables.iter() {
            for shard in st.lineage.ready() {
                let key = (table.clone(), shard);
                if scheduled.insert(key.clone()) {
                    pending.push_back((key, Duration::ZERO));
                }
            }
        }
        while tasks.len() < opts.max_concurrent_shards {
            let Some((key, delay)) = pending.pop_front() else {
                break;
            };
            let Some(st) = tables.get(&key.0) else {
                scheduled.remove(&key);
                continue;
            };
            let Some(start) = st.lineage.start_at(&key.1) else {
                scheduled.remove(&key);
                continue;
            };
            if let Err(m) = st.lineage.check_fresh(&key.1, unix_now()) {
                return Err(SourceError::PositionLost(format!("table `{}`: {m}", key.0)));
            }
            let cursor = cursors.entry(key.clone()).or_default();
            let step = Step {
                client: client.clone(),
                table: st.info.clone(),
                stream_arn: st.stream_arn.clone(),
                shard: key.1.clone(),
                start,
                iterator: cursor.iterator.take(),
                delay,
                cancel: cancel.clone(),
            };
            tasks.spawn(async move { (key, step.run().await) });
        }

        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let Some(joined) = joined else { continue };
                let (key, outcome) = joined
                    .map_err(|e| SourceError::Fatal(format!("shard reader task failed: {e}")))?;
                match outcome? {
                    StepOutcome::Cancelled => return Ok(()),
                    StepOutcome::Expired => {
                        tracing::debug!(table = %key.0, shard = %key.1, "shard iterator expired, renewing");
                        cursors.entry(key.clone()).or_default().iterator = None;
                        pending.push_back((key, Duration::ZERO));
                    }
                    StepOutcome::Retry(msg) => {
                        let cursor = cursors.entry(key.clone()).or_default();
                        cursor.failures += 1;
                        if cursor.failures > MAX_SHARD_FAILURES {
                            return Err(SourceError::Transient(msg));
                        }
                        tracing::debug!(table = %key.0, shard = %key.1, failures = cursor.failures, "shard read throttled or failed, backing off");
                        let wait = backoff(cursor.failures, Duration::from_millis(500), Duration::from_secs(30));
                        pending.push_back((key, wait));
                    }
                    StepOutcome::Records { changes, last_seq, next, polled_at, newest_ms } => {
                        let had_records = !changes.is_empty();
                        lags.observe(&key, had_records, newest_ms);
                        if let (Some(r), Some(seconds)) = (&lag, lags.worst()) {
                            r.report(Lag { seconds: Some(seconds), bytes: None, records: None });
                        }
                        for change in changes {
                            if !send(&events, SourceEvent::Change(change), &cancel).await {
                                return Ok(());
                            }
                        }
                        let Some(st) = tables.get_mut(&key.0) else { continue };
                        if let Some(seq) = last_seq {
                            st.lineage.advance(&key.1, seq);
                            dirty = true;
                        } else if st.lineage.idle(&key.1, polled_at) {
                            dirty = true;
                        }
                        match next {
                            None => {
                                tracing::debug!(table = %key.0, shard = %key.1, "shard fully read");
                                st.lineage.finish(&key.1);
                                lags.forget(&key);
                                dirty = true;
                                scheduled.remove(&key);
                                cursors.remove(&key);
                                next_refresh = next_refresh.min(Instant::now() + REFRESH_AFTER_CLOSE);
                            }
                            Some(it) => {
                                let cursor = cursors.entry(key.clone()).or_default();
                                cursor.iterator = Some(it);
                                cursor.failures = 0;
                                let wait = if had_records { BUSY_POLL } else { opts.idle_poll() };
                                pending.push_back((key, wait));
                            }
                        }
                        let due = last_emit.is_none_or(|t| t.elapsed() >= opts.checkpoint_interval());
                        if dirty && due {
                            if !send(&events, SourceEvent::Checkpoint(checkpoint_of(tables)?), &cancel).await {
                                return Ok(());
                            }
                            dirty = false;
                            last_emit = Some(Instant::now());
                        }
                    }
                }
            }
            _ = tokio::time::sleep_until(next_refresh) => {
                if refresh(&client, tables, &cancel).await? {
                    dirty = true;
                }
                next_refresh = Instant::now() + opts.shard_refresh();
            }
            _ = tokio::time::sleep_until(next_tick) => {
                if dirty {
                    if !send(&events, SourceEvent::Checkpoint(checkpoint_of(tables)?), &cancel).await {
                        return Ok(());
                    }
                    dirty = false;
                    last_emit = Some(Instant::now());
                }
                next_tick = Instant::now() + opts.checkpoint_interval();
            }
        }
    }
}

/// Re-describe every stream and adopt new child shards. Transient
/// failures are logged and retried at the next refresh.
async fn refresh(
    client: &aws_sdk_dynamodbstreams::Client,
    tables: &mut BTreeMap<String, TableState>,
    cancel: &CancellationToken,
) -> Result<bool, SourceError> {
    let mut changed = false;
    for (name, st) in tables.iter_mut() {
        let described = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(changed),
            r = describe_stream(client, &st.stream_arn) => r,
        };
        match described {
            Ok(info) => {
                if is_disabled(info.status.as_ref()) {
                    return Err(SourceError::Config(format!(
                        "the stream of table `{name}` was disabled; re-enable it with view type NEW_AND_OLD_IMAGES (the sync then resyncs)"
                    )));
                }
                if st.lineage.refresh(&info.shards, unix_now()) {
                    changed = true;
                }
            }
            Err((ErrClass::NotFound, msg)) => {
                return Err(SourceError::PositionLost(format!(
                    "the stream of table `{name}` no longer exists ({msg})"
                )));
            }
            Err((ErrClass::Throttle | ErrClass::Transient, msg)) => {
                tracing::warn!(table = %name, error = %msg, "could not refresh the shard list, will retry");
            }
            Err((class, msg)) => return Err(to_source_error(class, msg)),
        }
    }
    Ok(changed)
}

#[cfg(test)]
mod lag_tests {
    use super::*;

    fn key(shard: &str) -> Key {
        ("t".to_owned(), shard.to_owned())
    }

    #[test]
    fn the_stream_lag_is_the_worst_shard() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let mut lags = ShardLags::default();
        assert_eq!(lags.worst(), None, "nothing read yet");
        lags.observe(&key("a"), true, Some(now_ms - 60_000));
        lags.observe(&key("b"), false, None);
        let worst = lags.worst().unwrap_or(-1.0);
        assert!((59.0..62.0).contains(&worst), "{worst}");
        // Shard a caught up.
        lags.observe(&key("a"), false, None);
        assert_eq!(lags.worst(), Some(0.0));
        // Records without a creation time leave the shard as it was.
        lags.observe(&key("c"), true, None);
        assert_eq!(lags.worst(), Some(0.0));
        lags.forget(&key("a"));
        lags.forget(&key("b"));
        assert_eq!(lags.worst(), None);
    }
}
