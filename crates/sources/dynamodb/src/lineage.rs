//! Checkpoint format and the shard lineage scheduler.
//!
//! A DynamoDB stream is a tree of shards: when a shard closes (every few
//! hours, or when a partition splits) it gets one or more children, and
//! records of one item move from the parent to a child. Per-item order is
//! only preserved if a child is read *after* its parent has been read to
//! its end (the parent is closed and `GetRecords` returned no next
//! iterator). Siblings are independent and read in parallel.
//!
//! [`Lineage`] tracks, for one table, every shard we are responsible for
//! and decides which ones are ready to read. It does no I/O, so it is
//! tested as a pure state machine.
//!
//! # Where a fresh sync starts (gapless start)
//!
//! `open(None)` runs before the snapshot. A `LATEST` iterator cannot be
//! saved (iterators expire after 15 minutes and a snapshot can take hours),
//! and DynamoDB has no cheap "current sequence number" of an open shard. So
//! `open` records every *leaf* shard of the stream (a shard no other
//! described shard names as parent: every open shard, plus a just-closed
//! shard whose children are not listed yet) and reads each from
//! `TRIM_HORIZON`. Every write after `open` lands in a leaf or in a
//! descendant of one, and descendants are adopted as they appear, so
//! nothing written after `open` is missed. The cost is replaying what those
//! leaves already hold (at most the 24 h retention, usually a few hours,
//! since shards roll over): replays are idempotent keyed upserts and
//! deletes, applied in per-item order, so the index converges to the
//! current state.
//!
//! A shard read from `TRIM_HORIZON` that has never returned a record keeps
//! `since`, the time before which its content is known to be read. If 23 h
//! pass without progress (a snapshot longer than the retention, a long
//! outage), records may have been trimmed silently, so the position is
//! reported lost instead of skipping them. For a leaf tracked by `open`,
//! `since` starts at the open time (older records are covered by the
//! snapshot). For an adopted child it starts at the child's *creation*
//! time, read from its id, because all of a child's records are needed: a
//! child adopted late (after an outage, or deep in a catch-up) may already
//! be close to the retention limit.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Records are retained 24 h; consider a `TRIM_HORIZON` position stale an
/// hour earlier to absorb clock skew.
pub const RETENTION_GUARD_SECS: u64 = 23 * 3600;

/// Granularity of `since` updates on idle shards, to keep checkpoints from
/// changing on every empty poll.
pub const SINCE_GRANULARITY_SECS: u64 = 60;

/// 2014-01-01: no stream shard is older.
const MIN_PLAUSIBLE_SHARD_TIME: u64 = 1_388_534_400;

/// `{ table: { stream_arn, shards: { shard_id: { seq, done } } } }`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct StreamCheckpoint(pub BTreeMap<String, TableCheckpoint>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCheckpoint {
    pub stream_arn: String,
    pub shards: BTreeMap<String, ShardCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCheckpoint {
    /// Last sequence number emitted from this shard; `null` = read from
    /// `TRIM_HORIZON`.
    pub seq: Option<String>,
    /// The shard was read to its end.
    pub done: bool,
    /// Unix seconds. Only while `seq` is `null`: everything written to the
    /// shard before this instant has been read (see the module docs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<u64>,
}

/// One shard as `DescribeStream` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardDesc {
    pub id: String,
    pub parent: Option<String>,
    /// Has an `EndingSequenceNumber`: no new records will arrive.
    pub closed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Tracked {
    parent: Option<String>,
    seq: Option<String>,
    done: bool,
    since: Option<u64>,
}

/// Where to start reading a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartAt {
    TrimHorizon,
    AfterSequence(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Lineage {
    shards: BTreeMap<String, Tracked>,
}

impl Lineage {
    /// Track every leaf of the described stream, from `TRIM_HORIZON`.
    pub fn fresh(desc: &[ShardDesc], now: u64) -> Self {
        let parents: BTreeSet<&str> = desc.iter().filter_map(|d| d.parent.as_deref()).collect();
        let shards = desc
            .iter()
            .filter(|d| !parents.contains(d.id.as_str()))
            .map(|d| {
                (
                    d.id.clone(),
                    Tracked {
                        parent: d.parent.clone(),
                        seq: None,
                        done: false,
                        since: Some(now),
                    },
                )
            })
            .collect();
        Self { shards }
    }

    /// Rebuild from a checkpoint and the current description. Fails (the
    /// position is lost) when a shard we still had to read has aged out of
    /// the stream, or when a `TRIM_HORIZON` position is too old.
    pub fn resume(cp: &TableCheckpoint, desc: &[ShardDesc], now: u64) -> Result<Self, String> {
        let described: BTreeMap<&str, &ShardDesc> =
            desc.iter().map(|d| (d.id.as_str(), d)).collect();
        let mut shards = BTreeMap::new();
        for (id, s) in &cp.shards {
            let d = described.get(id.as_str());
            if !s.done && d.is_none() {
                return Err(format!(
                    "shard {id} is no longer in the stream (records older than 24 h are trimmed)"
                ));
            }
            let t = Tracked {
                parent: d.and_then(|d| d.parent.clone()),
                seq: s.seq.clone(),
                done: s.done,
                since: if s.seq.is_none() { s.since } else { None },
            };
            stale(id, &t, now)?;
            shards.insert(id.clone(), t);
        }
        let mut lineage = Self { shards };
        lineage.refresh(desc, now);
        Ok(lineage)
    }

    /// Apply a new description: adopt the descendants of tracked shards and
    /// forget finished shards that aged out. Returns whether the checkpoint
    /// changed.
    pub fn refresh(&mut self, desc: &[ShardDesc], now: u64) -> bool {
        let mut changed = false;
        for d in desc {
            if let Some(t) = self.shards.get_mut(&d.id)
                && t.parent.is_none()
                && d.parent.is_some()
            {
                t.parent = d.parent.clone();
            }
        }
        // Adopt to a fixpoint: a grandchild may be listed with its parent.
        loop {
            let adopt: Vec<&ShardDesc> = desc
                .iter()
                .filter(|d| !self.shards.contains_key(&d.id))
                .filter(|d| {
                    d.parent
                        .as_deref()
                        .is_some_and(|p| self.shards.contains_key(p))
                })
                .collect();
            if adopt.is_empty() {
                break;
            }
            for d in adopt {
                let since = adopted_since(d, now);
                self.shards.insert(
                    d.id.clone(),
                    Tracked {
                        parent: d.parent.clone(),
                        seq: None,
                        done: false,
                        since: Some(since),
                    },
                );
            }
            changed = true;
        }
        // A finished shard that left the description can no longer have
        // unadopted children (we refresh far more often than the 24 h a
        // closed shard stays listed), so it is safe to forget.
        let described: BTreeSet<&str> = desc.iter().map(|d| d.id.as_str()).collect();
        let before = self.shards.len();
        self.shards
            .retain(|id, t| !t.done || described.contains(id.as_str()));
        changed || self.shards.len() != before
    }

    /// Shards to read now: not finished, and their parent is finished or
    /// not ours (it predates the sync's start).
    pub fn ready(&self) -> Vec<String> {
        self.shards
            .iter()
            .filter(|(_, t)| !t.done)
            .filter(|(_, t)| match &t.parent {
                None => true,
                Some(p) => self.shards.get(p).is_none_or(|pt| pt.done),
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn start_at(&self, shard: &str) -> Option<StartAt> {
        let t = self.shards.get(shard)?;
        Some(match &t.seq {
            Some(seq) => StartAt::AfterSequence(seq.clone()),
            None => StartAt::TrimHorizon,
        })
    }

    /// Fails when reading `shard` from `TRIM_HORIZON` could silently skip
    /// trimmed records.
    pub fn check_fresh(&self, shard: &str, now: u64) -> Result<(), String> {
        match self.shards.get(shard) {
            Some(t) => stale(shard, t, now),
            None => Ok(()),
        }
    }

    /// Records up to `seq` were emitted.
    pub fn advance(&mut self, shard: &str, seq: String) {
        if let Some(t) = self.shards.get_mut(shard) {
            t.seq = Some(seq);
            t.since = None;
        }
    }

    /// A `GetRecords` issued at `polled_at` returned nothing on a shard that
    /// never had records: its content before `polled_at` is read. Returns
    /// whether the checkpoint changed.
    pub fn idle(&mut self, shard: &str, polled_at: u64) -> bool {
        match self.shards.get_mut(shard) {
            Some(t) if t.seq.is_none() && !t.done => match t.since {
                Some(s) if polled_at < s.saturating_add(SINCE_GRANULARITY_SECS) => false,
                _ => {
                    t.since = Some(polled_at);
                    true
                }
            },
            _ => false,
        }
    }

    /// The shard was read to its end (closed, no next iterator).
    pub fn finish(&mut self, shard: &str) {
        if let Some(t) = self.shards.get_mut(shard) {
            t.done = true;
            t.since = None;
        }
    }

    pub fn is_tracked(&self, shard: &str) -> bool {
        self.shards.contains_key(shard)
    }

    pub fn to_checkpoint(&self, stream_arn: &str) -> TableCheckpoint {
        TableCheckpoint {
            stream_arn: stream_arn.to_owned(),
            shards: self
                .shards
                .iter()
                .map(|(id, t)| {
                    (
                        id.clone(),
                        ShardCheckpoint {
                            seq: t.seq.clone(),
                            done: t.done,
                            since: t.since,
                        },
                    )
                })
                .collect(),
        }
    }
}

/// `since` of an adopted child: everything the child holds must be read,
/// so it is the child's creation time, never the adoption time (a child
/// may be adopted hours after it appeared, after an outage or during a
/// long catch-up). Without a readable creation time, the parent's is a
/// lower bound (a child is created after its parent); without either,
/// the shard is treated as old, which reports the position lost rather
/// than risk a silent gap. Capped at `now` against clock skew.
fn adopted_since(d: &ShardDesc, now: u64) -> u64 {
    let created = shard_created_at(&d.id).or_else(|| {
        let parent = d.parent.as_deref().and_then(shard_created_at);
        if parent.is_none() {
            tracing::warn!(shard = %d.id, "shard id carries no creation time, treating the shard as old");
        }
        parent
    });
    created.unwrap_or(0).min(now)
}

/// The creation time of a shard (Unix seconds), read from its id:
/// `shardId-<creation millis, zero-padded>-<suffix>`. `None` when the id
/// does not follow that format or the time is implausible.
fn shard_created_at(id: &str) -> Option<u64> {
    let rest = id.strip_prefix("shardId-")?;
    let digits = rest.split('-').next()?;
    if digits.len() < 13 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let secs = digits.parse::<u64>().ok()? / 1000;
    // DynamoDB Streams did not exist before 2014.
    (secs >= MIN_PLAUSIBLE_SHARD_TIME).then_some(secs)
}

fn stale(id: &str, t: &Tracked, now: u64) -> Result<(), String> {
    match (t.done, &t.seq, t.since) {
        (false, None, Some(since)) if now >= since.saturating_add(RETENTION_GUARD_SECS) => {
            Err(format!(
                "shard {id} has not been read for {} h; its oldest records may be trimmed",
                (now - since) / 3600
            ))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn d(id: &str, parent: Option<&str>, closed: bool) -> ShardDesc {
        ShardDesc {
            id: id.into(),
            parent: parent.map(Into::into),
            closed,
        }
    }

    const T0: u64 = 1_700_000_000;

    #[test]
    fn fresh_tracks_leaves_only() {
        // a (closed) -> b (closed) -> c (open); x is a closed root whose
        // children are not listed yet; y is an open root.
        let desc = [
            d("a", None, true),
            d("b", Some("a"), true),
            d("c", Some("b"), false),
            d("x", None, true),
            d("y", None, false),
        ];
        let l = Lineage::fresh(&desc, T0);
        assert!(!l.is_tracked("a") && !l.is_tracked("b"));
        assert_eq!(l.ready(), vec!["c", "x", "y"]);
        assert_eq!(l.start_at("c"), Some(StartAt::TrimHorizon));
        let cp = l.to_checkpoint("arn:s");
        assert_eq!(cp.shards["c"].since, Some(T0));
        assert!(cp.shards.values().all(|s| s.seq.is_none() && !s.done));
    }

    #[test]
    fn parents_before_children_and_splits() {
        let mut l = Lineage::fresh(&[d("p", None, false)], T0);
        assert_eq!(l.ready(), vec!["p"]);
        l.advance("p", "100".into());

        // p closes and splits into c1, c2; c1 already split into g.
        let desc = [
            d("p", None, true),
            d("c1", Some("p"), true),
            d("c2", Some("p"), false),
            d("g", Some("c1"), false),
        ];
        assert!(l.refresh(&desc, T0 + 10));
        assert!(l.is_tracked("g"));
        // Children wait for the parent to be read to its end.
        assert_eq!(l.ready(), vec!["p"]);
        l.finish("p");
        assert_eq!(l.ready(), vec!["c1", "c2"]);
        l.advance("c1", "200".into());
        l.finish("c1");
        assert_eq!(l.ready(), vec!["c2", "g"]);
        assert_eq!(l.start_at("g"), Some(StartAt::TrimHorizon));
        assert_eq!(l.start_at("c1"), Some(StartAt::AfterSequence("200".into())));
        // Refreshing again changes nothing.
        assert!(!l.refresh(&desc, T0 + 20));
    }

    #[test]
    fn closed_shards_are_pruned_once_aged_out() {
        let mut l = Lineage::fresh(&[d("p", None, true)], T0);
        l.finish("p");
        let desc = [d("p", None, true), d("c", Some("p"), false)];
        l.refresh(&desc, T0);
        assert_eq!(l.ready(), vec!["c"]);
        // p leaves the description 24 h later: forgotten, c stays ready.
        assert!(l.refresh(&[d("c", Some("p"), false)], T0 + 90_000));
        assert!(!l.is_tracked("p"));
        assert_eq!(l.ready(), vec!["c"]);
        // An unfinished shard is never pruned.
        let mut l2 = Lineage::fresh(&[d("q", None, false)], T0);
        l2.advance("q", "1".into());
        l2.refresh(&[], T0);
        assert!(l2.is_tracked("q"));
    }

    #[test]
    fn foreign_shards_are_not_adopted() {
        // A shard whose parent is not ours (it predates the start) is
        // ignored: its content is older than the snapshot.
        let mut l = Lineage::fresh(&[d("c", Some("old"), false)], T0);
        let desc = [
            d("old", None, true),
            d("c", Some("old"), false),
            d("other", None, true),
        ];
        assert!(!l.refresh(&desc, T0));
        assert!(!l.is_tracked("old") && !l.is_tracked("other"));
        assert_eq!(l.ready(), vec!["c"]);
    }

    #[test]
    fn resume_mid_lineage() {
        let cp = TableCheckpoint {
            stream_arn: "arn:s".into(),
            shards: BTreeMap::from([
                (
                    "p".into(),
                    ShardCheckpoint {
                        seq: Some("10".into()),
                        done: true,
                        since: None,
                    },
                ),
                (
                    "c1".into(),
                    ShardCheckpoint {
                        seq: Some("20".into()),
                        done: false,
                        since: None,
                    },
                ),
            ]),
        };
        // c2 appeared while we were down; g is c1's child (c1 not done).
        let desc = [
            d("p", None, true),
            d("c1", Some("p"), true),
            d("c2", Some("p"), false),
            d("g", Some("c1"), false),
        ];
        let l = Lineage::resume(&cp, &desc, T0).unwrap();
        assert_eq!(l.ready(), vec!["c1", "c2"]);
        assert_eq!(l.start_at("c1"), Some(StartAt::AfterSequence("20".into())));
        assert!(l.is_tracked("g"));

        // The finished parent aged out meanwhile: fine.
        let desc2 = &desc[1..];
        let l2 = Lineage::resume(&cp, desc2, T0).unwrap();
        assert!(!l2.is_tracked("p"));
        assert_eq!(l2.ready(), vec!["c1", "c2"]);
    }

    #[test]
    fn expired_shards_lose_the_position() {
        let cp = TableCheckpoint {
            stream_arn: "arn:s".into(),
            shards: BTreeMap::from([(
                "gone".into(),
                ShardCheckpoint {
                    seq: Some("5".into()),
                    done: false,
                    since: None,
                },
            )]),
        };
        let err = Lineage::resume(&cp, &[d("new", None, false)], T0).unwrap_err();
        assert!(err.contains("gone"), "{err}");

        // A TRIM_HORIZON position older than the retention guard.
        let cp = TableCheckpoint {
            stream_arn: "arn:s".into(),
            shards: BTreeMap::from([(
                "s".into(),
                ShardCheckpoint {
                    seq: None,
                    done: false,
                    since: Some(T0),
                },
            )]),
        };
        let desc = [d("s", None, false)];
        assert!(Lineage::resume(&cp, &desc, T0 + 3600).is_ok());
        assert!(Lineage::resume(&cp, &desc, T0 + RETENTION_GUARD_SECS).is_err());
        let l = Lineage::fresh(&desc, T0);
        assert!(l.check_fresh("s", T0 + RETENTION_GUARD_SECS).is_err());
    }

    #[test]
    fn idle_polls_move_since_with_granularity() {
        let mut l = Lineage::fresh(&[d("s", None, false)], T0);
        assert!(!l.idle("s", T0 + 1));
        assert!(l.idle("s", T0 + SINCE_GRANULARITY_SECS));
        assert_eq!(
            l.to_checkpoint("a").shards["s"].since,
            Some(T0 + SINCE_GRANULARITY_SECS)
        );
        // Kept fresh by idle polls, it never goes stale.
        assert!(l.check_fresh("s", T0 + RETENTION_GUARD_SECS).is_ok());
        l.advance("s", "9".into());
        assert!(!l.idle("s", T0 + 10_000));
        assert_eq!(l.to_checkpoint("a").shards["s"].since, None);
    }

    /// A real shard id: `shardId-<20-digit zero-padded creation millis>-<suffix>`.
    fn sid(created_secs: u64, suffix: &str) -> String {
        format!("shardId-{:020}-{suffix}", created_secs * 1000)
    }

    #[test]
    fn shard_creation_time_is_read_from_the_id() {
        assert_eq!(shard_created_at(&sid(T0, "ab12cd34")), Some(T0));
        assert_eq!(
            shard_created_at("shardId-00000001795099544717-e12d6529"),
            Some(1_795_099_544)
        );
        // 13 digits without padding.
        assert_eq!(shard_created_at("shardId-1700000000123-x"), Some(T0));
        for bad in [
            "c1",
            "shardId-",
            "shardId-abc-1",
            "shardId-000000000000",
            "shardId-00000000000000000000-deadbeef",
            "shardId-99999999999999999999999-x",
        ] {
            assert_eq!(shard_created_at(bad), None, "{bad}");
        }
    }

    /// Down 20 h; on resume children C and G (created early in the outage)
    /// are adopted; the sync stops again before C returns a record and
    /// resumes 10 h later. C's first hours are trimmed by then: the
    /// position must be lost, not silently skipped.
    #[test]
    fn late_adopted_children_keep_their_creation_time() {
        const H: u64 = 3600;
        let p = sid(T0 - 2 * H, "p");
        let c = sid(T0 + H, "c");
        let g = sid(T0 + 5 * H, "g");
        let cp = TableCheckpoint {
            stream_arn: "arn:s".into(),
            shards: BTreeMap::from([(
                p.clone(),
                ShardCheckpoint {
                    seq: Some("10".into()),
                    done: false,
                    since: None,
                },
            )]),
        };
        let desc = [
            d(&p, None, true),
            d(&c, Some(&p), true),
            d(&g, Some(&c), false),
        ];
        let resumed_at = T0 + 20 * H;
        let mut l = Lineage::resume(&cp, &desc, resumed_at).unwrap();
        assert!(l.is_tracked(&c) && l.is_tracked(&g));
        let saved = l.to_checkpoint("arn:s");
        assert_eq!(saved.shards[&c].since, Some(T0 + H));
        assert_eq!(saved.shards[&g].since, Some(T0 + 5 * H));
        // The parent is read to its end; C has not returned a record yet.
        l.finish(&p);
        let saved = l.to_checkpoint("arn:s");

        // Stopped, resumed 10 h later.
        let later = resumed_at + 10 * H;
        let err = Lineage::resume(&saved, &desc, later).unwrap_err();
        assert!(err.contains(&c), "{err}");

        // Same within one run when catch-up lag approaches the retention.
        assert!(l.check_fresh(&c, T0 + H + RETENTION_GUARD_SECS).is_err());
        assert!(l.check_fresh(&c, resumed_at).is_ok());
    }

    #[test]
    fn unparseable_children_fall_back_to_the_parent_or_to_old() {
        let p = sid(T0, "p");
        let mut l = Lineage::fresh(&[d(&p, None, false)], T0 + 100);
        l.advance(&p, "1".into());
        // A child id without a timestamp: created after its parent.
        l.refresh(
            &[d(&p, None, true), d("child", Some(&p), false)],
            T0 + 50_000,
        );
        assert_eq!(l.to_checkpoint("a").shards["child"].since, Some(T0));
        // Neither id carries a time: treated as old.
        let mut l = Lineage::fresh(&[d("q", None, false)], T0);
        l.advance("q", "1".into());
        l.refresh(&[d("q", None, true), d("r", Some("q"), false)], T0 + 10);
        l.finish("q");
        assert!(l.check_fresh("r", T0 + 10).is_err());
        // A creation time in the future (clock skew) is capped at now.
        let mut l = Lineage::fresh(&[d("q", None, false)], T0);
        l.refresh(
            &[d("q", None, true), d(&sid(T0 + 500, "f"), Some("q"), false)],
            T0 + 10,
        );
        assert_eq!(
            l.to_checkpoint("a").shards[&sid(T0 + 500, "f")].since,
            Some(T0 + 10)
        );
    }

    #[test]
    fn checkpoint_json_round_trip() {
        let mut l = Lineage::fresh(&[d("s1", None, false), d("s2", None, false)], T0);
        l.advance("s1", "000123".into());
        l.finish("s2");
        let cp = StreamCheckpoint(BTreeMap::from([("users".into(), l.to_checkpoint("arn:x"))]));
        let json = serde_json::to_value(&cp).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "users": {
                    "stream_arn": "arn:x",
                    "shards": {
                        "s1": {"seq": "000123", "done": false},
                        "s2": {"seq": null, "done": true}
                    }
                }
            })
        );
        let core = meili_connect_core::Checkpoint::new(&cp).unwrap();
        let back: StreamCheckpoint = core.decode().unwrap();
        assert_eq!(back, cp);
        // `since` is optional on input.
        let parsed: StreamCheckpoint = serde_json::from_value(serde_json::json!({
            "t": {"stream_arn": "a", "shards": {"s": {"seq": null, "done": false}}}
        }))
        .unwrap();
        assert_eq!(parsed.0["t"].shards["s"].since, None);
    }
}
