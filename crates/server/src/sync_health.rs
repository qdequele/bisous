//! `sync.health_changed`: tells the Lab when a tenant's sync gets dangerous
//! for its source, per cause, and when it is fine again.
//!
//! - **`lag`**: how far behind its source a *streaming* sync is, as the
//!   source already reports it ([`meili_connect_core::Lag::seconds`]; no
//!   extra query). Looked at every [`LAG_EVERY`]. Not measured in other
//!   phases: a lag warning stands until the sync streams under the
//!   threshold again.
//! - **`wal_retained`**: the WAL the replication slot of a Postgres or
//!   Supabase sync keeps on the source, measured in **every** phase where
//!   the slot exists (streaming, snapshotting, paused, held, failed, source
//!   paused): a stopped sync's slot keeps WAL until the customer's disk
//!   fills or `max_slot_wal_keep_size` invalidates the slot. Read every
//!   `SYNC_HEALTH_INTERVAL_SECS` by [`Source::retention`] on a short-lived
//!   connection of its own, behind the host policy like every connection
//!   of the sync: at most [`PROBE_CONCURRENCY`] at a time, each bounded by
//!   [`PROBE_TIMEOUT`]. A probe that fails is logged and skipped (an
//!   unreachable source is the state event's business). Other source kinds
//!   keep nothing measurable here (MySQL binlog retention is not measured).
//!
//! A cause is `warning` once its value reaches its threshold and `ok` again
//! only below [`OK_BELOW_PERCENT`] of it, so a value hovering around the
//! threshold does not flap. The WAL threshold is `SYNC_WAL_WARN_BYTES`, or
//! [`SOURCE_LIMIT_PERCENT`] of the source's own positive
//! `max_slot_wal_keep_size` when that is lower; the event carries the
//! threshold that applied and the source's limit.
//!
//! A change goes through [`crate::sync_events::SyncEvents`] like a phase change: it waits for
//! the settle window (`SYNC_EVENTS_SETTLE_SECS`), a change back to what the
//! Lab was told last cancels it, and it is enqueued in the same Lab outbox.
//! Only Lab tenants' syncs are tracked; a deleted sync drops its health
//! state with the rest. The state is in memory: after a restart every
//! cause starts `ok`, so a sync still over its threshold reports `warning`
//! once more (the Lab dedupes repeated warnings), and one that recovered
//! while the server was down sends nothing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use meili_connect_core::{Retention, Source, SourceFactory, SourceKind, SyncPhase};
use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::manager::SyncManager;
use crate::sync_events::Reported;
use crate::usage::PRODUCT;

/// The Lab event type.
pub const EVENT_TYPE: &str = "sync.health_changed";
/// `ok` again only below this share of the threshold (hysteresis).
pub const OK_BELOW_PERCENT: u64 = 80;
/// Retained WAL past this share of the source's own positive
/// `max_slot_wal_keep_size` warns, even under `SYNC_WAL_WARN_BYTES`.
pub const SOURCE_LIMIT_PERCENT: u64 = 50;
/// Retention probes running at once.
pub const PROBE_CONCURRENCY: usize = 4;
/// Longest a retention probe may take (connection included).
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
/// How often the streaming syncs' lag is looked at (in memory: cheap).
pub const LAG_EVERY: Duration = Duration::from_secs(30);

/// `SYNC_HEALTH_INTERVAL_SECS`, `SYNC_LAG_WARN_SECS`, `SYNC_WAL_WARN_BYTES`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HealthConfig {
    /// How often the slots are measured; zero turns the whole probe off.
    pub interval: Duration,
    /// Lag, in seconds, that warns.
    pub lag_warn_secs: u64,
    /// Retained WAL, in bytes, that warns.
    pub wal_warn_bytes: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(300),
            lag_warn_secs: 900,
            wal_warn_bytes: 1 << 30,
        }
    }
}

/// The contract's `cause`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Cause {
    Lag,
    WalRetained,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::Lag => "lag",
            Cause::WalRetained => "wal_retained",
        }
    }
}

/// The contract's `status`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Status {
    #[default]
    Ok,
    Warning,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Warning => "warning",
        }
    }
}

/// One measure of one cause, with the threshold that applies to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reading {
    /// Seconds (lag) or bytes (WAL).
    pub value: u64,
    pub threshold: u64,
    /// WAL: the source's own cap (`max_slot_wal_keep_size`), if any.
    pub source_limit: Option<u64>,
}

impl Reading {
    /// A streaming sync's lag.
    pub fn lag(seconds: f64, config: &HealthConfig) -> Self {
        Self {
            // Whole seconds; negative or NaN (clock skew) is 0.
            value: if seconds.is_finite() && seconds > 0.0 {
                seconds as u64
            } else {
                0
            },
            threshold: config.lag_warn_secs,
            source_limit: None,
        }
    }

    /// The WAL a slot keeps; `None` (no slot) keeps nothing.
    pub fn wal(retention: Option<&Retention>, config: &HealthConfig) -> Self {
        let limit = retention.and_then(|r| r.limit_bytes);
        Self {
            value: retention.map_or(0, |r| r.bytes),
            threshold: wal_threshold(config.wal_warn_bytes, limit),
            source_limit: limit,
        }
    }
}

/// `SYNC_WAL_WARN_BYTES`, or half the source's positive cap when lower.
pub fn wal_threshold(warn_bytes: u64, source_limit: Option<u64>) -> u64 {
    match source_limit {
        Some(limit) if limit > 0 => warn_bytes.min(share(limit, SOURCE_LIMIT_PERCENT).max(1)),
        _ => warn_bytes,
    }
}

fn share(value: u64, percent: u64) -> u64 {
    u64::try_from(u128::from(value) * u128::from(percent) / 100).unwrap_or(u64::MAX)
}

/// The status of a cause measured at `value`, `previous` before: warning
/// from the threshold, ok again below [`OK_BELOW_PERCENT`] of it, unchanged
/// in between.
pub fn judge(previous: Status, value: u64, threshold: u64) -> Status {
    if value >= threshold {
        Status::Warning
    } else if value < share(threshold, OK_BELOW_PERCENT) {
        Status::Ok
    } else {
        previous
    }
}

/// A status change waiting for the settle window.
#[derive(Debug, Clone)]
pub(crate) struct HealthChange {
    pub status: Status,
    pub reading: Reading,
    /// The sync's phase when it was measured.
    pub state: Option<SyncPhase>,
    pub at: DateTime<Utc>,
    pub since: Instant,
}

/// One cause of one sync.
#[derive(Debug, Default)]
pub(crate) struct HealthTrack {
    /// What the Lab was told last (`ok` until told otherwise).
    reported: Status,
    /// The status of the last reading.
    current: Status,
    pending: Option<HealthChange>,
}

impl HealthTrack {
    pub fn observe(&mut self, reading: Reading, state: Option<SyncPhase>) {
        let status = judge(self.current, reading.value, reading.threshold);
        self.current = status;
        if status == self.reported {
            // Back where the Lab thinks it is: nothing to tell.
            self.pending = None;
            return;
        }
        match &mut self.pending {
            // Still the same change: it keeps its time, takes the values.
            Some(p) if p.status == status => {
                p.reading = reading;
                p.state = state;
            }
            _ => {
                self.pending = Some(HealthChange {
                    status,
                    reading,
                    state,
                    at: Utc::now(),
                    since: Instant::now(),
                });
            }
        }
    }

    /// The change that settled (any with `force`), now reported.
    pub fn take_due(&mut self, settle: Duration, force: bool) -> Option<HealthChange> {
        let due = self
            .pending
            .as_ref()
            .is_some_and(|p| force || p.since.elapsed() >= settle);
        if !due {
            return None;
        }
        let change = self.pending.take()?;
        self.reported = change.status;
        Some(change)
    }
}

/// The `sync.health_changed` event of a change, without its `id`. Nothing
/// about the source but numbers: no host, no slot name.
pub(crate) fn event(
    sync_id: &str,
    tenant: &str,
    label: &str,
    cause: Cause,
    c: &HealthChange,
) -> Value {
    let mut data = Map::new();
    data.insert("sync_id".into(), json!(sync_id));
    data.insert("sync_name".into(), json!(label));
    data.insert("status".into(), json!(c.status.as_str()));
    data.insert("cause".into(), json!(cause.as_str()));
    if let Some(state) = c.state {
        data.insert("state".into(), json!(Reported::Phase(state).as_str()));
    }
    match cause {
        Cause::Lag => {
            data.insert("lag_seconds".into(), json!(c.reading.value));
        }
        Cause::WalRetained => {
            data.insert("retained_wal_bytes".into(), json!(c.reading.value));
            if let Some(limit) = c.reading.source_limit {
                data.insert("source_limit_bytes".into(), json!(limit));
            }
        }
    }
    data.insert("threshold".into(), json!(c.reading.threshold));
    json!({
        "type": EVENT_TYPE,
        "occurred_at": c.at.to_rfc3339_opts(SecondsFormat::Millis, true),
        "account_id": tenant,
        "api_key_id": null,
        "product": PRODUCT,
        "data": data,
    })
}

// ------------------------------------------------------------ the probe

/// Whether a sync of this kind keeps a slot the probe can measure.
fn has_slot(kind: SourceKind) -> bool {
    matches!(kind, SourceKind::Postgres | SourceKind::Supabase)
}

/// The lag of every streaming tracked sync.
pub async fn sample_lag(manager: &SyncManager, config: &HealthConfig) {
    let events = manager.sync_events();
    for id in manager.running_ids() {
        if !events.tracks(&id) {
            continue;
        }
        if let Ok(Some(st)) = manager.status(&id).await
            && st.phase == SyncPhase::Streaming
            && let Some(seconds) = st.lag.and_then(|l| l.seconds)
        {
            events.observe_health(
                &id,
                Cause::Lag,
                Reading::lag(seconds, config),
                Some(st.phase),
            );
        }
    }
}

/// Measure the slot of every tracked Postgres or Supabase sync, whatever
/// its phase. Returns how many were measured.
pub async fn probe_retention(
    manager: &Arc<SyncManager>,
    config: &HealthConfig,
    cancel: &CancellationToken,
) -> usize {
    let events = Arc::clone(manager.sync_events());
    let rows = match manager.backend().list_all_for_boot().await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::debug!(error = %e, "cannot list the syncs; their slots are measured next time");
            return 0;
        }
    };
    let slots = Arc::new(Semaphore::new(PROBE_CONCURRENCY));
    let mut probes = JoinSet::new();
    for row in rows {
        let Ok(spec) = row.spec else { continue };
        if spec.tenant.is_none() || !has_slot(spec.source.kind) || !events.tracks(&spec.id) {
            continue;
        }
        let (manager, events, slots, config) = (
            Arc::clone(manager),
            Arc::clone(&events),
            Arc::clone(&slots),
            *config,
        );
        probes.spawn(async move {
            let Ok(_slot) = slots.acquire_owned().await else {
                return false;
            };
            let state = manager.state_phase(&spec.id).await;
            let measure = async {
                let mut source: Box<dyn Source> =
                    manager
                        .guarded_sources(&spec)
                        .build(&spec.id, &spec.source, &spec.streams)?;
                source.retention().await
            };
            match tokio::time::timeout(PROBE_TIMEOUT, measure).await {
                Ok(Ok(retention)) => {
                    if let Some(r) = &retention {
                        tracing::debug!(sync_id = %spec.id, bytes = r.bytes,
                            wal_status = r.status.as_deref().unwrap_or("-"), "slot measured");
                    }
                    events.observe_health(
                        &spec.id,
                        Cause::WalRetained,
                        Reading::wal(retention.as_ref(), &config),
                        state,
                    );
                    true
                }
                Ok(Err(e)) => {
                    tracing::debug!(sync_id = %spec.id, error = %e,
                        "cannot measure the replication slot; skipped");
                    false
                }
                Err(_) => {
                    tracing::debug!(sync_id = %spec.id,
                        "measuring the replication slot timed out; skipped");
                    false
                }
            }
        });
    }
    let mut measured = 0;
    loop {
        tokio::select! {
            // Dropping the set aborts what still runs.
            _ = cancel.cancelled() => return measured,
            next = probes.join_next() => match next {
                Some(Ok(true)) => measured += 1,
                Some(_) => {}
                None => return measured,
            },
        }
    }
}

/// Look at the lag every [`LAG_EVERY`] and measure the slots every
/// `config.interval` (first right away), until `cancel`.
pub async fn run(manager: Arc<SyncManager>, config: HealthConfig, cancel: CancellationToken) {
    if config.interval.is_zero() || !manager.sync_events().is_enabled() {
        return;
    }
    let tick = LAG_EVERY.min(config.interval);
    let mut last_probe: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(tick) => {}
        }
        sample_lag(&manager, &config).await;
        if last_probe.is_none_or(|t| t.elapsed() >= config.interval) {
            last_probe = Some(Instant::now());
            probe_retention(&manager, &config, &cancel).await;
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    const GIB: u64 = 1 << 30;

    fn cfg() -> HealthConfig {
        HealthConfig::default()
    }

    #[test]
    fn warning_at_the_threshold_ok_only_well_below_it() {
        use Status::*;
        assert_eq!(judge(Ok, 999, 1000), Ok);
        assert_eq!(judge(Ok, 1000, 1000), Warning);
        assert_eq!(judge(Warning, 999, 1000), Warning, "hysteresis");
        assert_eq!(judge(Warning, 800, 1000), Warning, "80% is not below 80%");
        assert_eq!(judge(Warning, 799, 1000), Ok);
        assert_eq!(judge(Ok, 900, 1000), Ok, "in the band, stays ok");
        // No overflow at the top.
        assert_eq!(judge(Warning, u64::MAX - 1, u64::MAX), Warning);
        assert_eq!(judge(Ok, u64::MAX, u64::MAX), Warning);
    }

    #[test]
    fn the_source_limit_lowers_the_wal_threshold() {
        assert_eq!(wal_threshold(GIB, None), GIB);
        // max_slot_wal_keep_size = 1 GiB: warn from 512 MiB.
        assert_eq!(wal_threshold(GIB, Some(GIB)), GIB / 2);
        // A large cap: the configured threshold wins.
        assert_eq!(wal_threshold(GIB, Some(10 * GIB)), GIB);
        // 0 is not a usable cap.
        assert_eq!(wal_threshold(GIB, Some(0)), GIB);
        assert_eq!(wal_threshold(GIB, Some(1)), 1);
    }

    #[test]
    fn readings_from_the_source() {
        let r = Reading::wal(
            Some(&Retention {
                bytes: 600 << 20,
                limit_bytes: Some(GIB),
                status: Some("reserved".into()),
            }),
            &cfg(),
        );
        assert_eq!(
            r,
            Reading {
                value: 600 << 20,
                threshold: GIB / 2,
                source_limit: Some(GIB)
            }
        );
        assert_eq!(judge(Status::Ok, r.value, r.threshold), Status::Warning);
        // No slot: nothing kept.
        assert_eq!(Reading::wal(None, &cfg()).value, 0);
        assert_eq!(Reading::lag(901.7, &cfg()).value, 901);
        assert_eq!(Reading::lag(-3.0, &cfg()).value, 0);
        assert_eq!(Reading::lag(f64::NAN, &cfg()).value, 0);
    }

    fn wal(bytes: u64) -> Reading {
        Reading::wal(
            Some(&Retention {
                bytes,
                limit_bytes: None,
                status: None,
            }),
            &cfg(),
        )
    }

    #[test]
    fn a_track_reports_crossings_once_and_ignores_the_band() {
        let mut t = HealthTrack::default();
        let state = Some(SyncPhase::Paused);
        t.observe(wal(GIB / 2), state);
        assert!(t.take_due(Duration::ZERO, false).is_none(), "under: ok");
        t.observe(wal(GIB), state);
        let c = t.take_due(Duration::ZERO, false).unwrap();
        assert_eq!(c.status, Status::Warning);
        assert_eq!(c.reading.value, GIB);
        // Still over, then in the band: nothing new.
        t.observe(wal(2 * GIB), state);
        t.observe(wal(GIB * 9 / 10), state);
        assert!(t.take_due(Duration::ZERO, true).is_none());
        // Well below: ok, once.
        t.observe(wal(GIB / 10), state);
        assert_eq!(
            t.take_due(Duration::ZERO, false).unwrap().status,
            Status::Ok
        );
        t.observe(wal(0), state);
        assert!(t.take_due(Duration::ZERO, true).is_none());
    }

    #[test]
    fn a_crossing_back_inside_the_window_cancels_it() {
        let mut t = HealthTrack::default();
        let settle = Duration::from_secs(3600);
        t.observe(wal(GIB), None);
        assert!(t.take_due(settle, false).is_none(), "not settled");
        // Same change again: the latest values, the first time.
        t.observe(wal(3 * GIB), Some(SyncPhase::Held));
        let at = t.pending.as_ref().unwrap().at;
        t.observe(wal(4 * GIB), Some(SyncPhase::Failed));
        assert_eq!(t.pending.as_ref().unwrap().at, at);
        // Back under: nothing to tell.
        t.observe(wal(0), None);
        assert!(t.take_due(settle, true).is_none());
        // Over again and forced (a graceful stop): the latest values.
        t.observe(wal(5 * GIB), Some(SyncPhase::Failed));
        let c = t.take_due(settle, true).unwrap();
        assert_eq!(c.reading.value, 5 * GIB);
        assert_eq!(c.state, Some(SyncPhase::Failed));
    }

    #[test]
    fn events_carry_only_numbers() {
        let c = HealthChange {
            status: Status::Warning,
            reading: Reading {
                value: 7,
                threshold: 5,
                source_limit: Some(10),
            },
            state: Some(SyncPhase::Held),
            at: Utc::now(),
            since: Instant::now(),
        };
        let e = event("s1", "t", "postgres → p", Cause::WalRetained, &c);
        assert_eq!(
            e["data"],
            json!({"sync_id": "s1", "sync_name": "postgres → p", "status": "warning",
                   "cause": "wal_retained", "state": "held", "retained_wal_bytes": 7,
                   "threshold": 5, "source_limit_bytes": 10})
        );
        let e = event("s1", "t", "l", Cause::Lag, &c);
        assert_eq!(e["data"]["lag_seconds"], 7);
        assert!(e["data"].get("source_limit_bytes").is_none());
        assert!(e["data"].get("retained_wal_bytes").is_none());
    }
}
