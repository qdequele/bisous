//! MySQL GTID sets: `uuid:1-5:7,uuid2:1-3`, with MySQL 8.4 tags
//! (`uuid:1-5:tag_a:1-2`).

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use mysql_async::binlog::events::GtidEvent;

/// Source key of a GTID: server uuid (lowercase, hyphenated) and optional tag.
type Sid = (String, Option<String>);

/// A normalized GTID set: per (uuid, tag), sorted disjoint closed intervals
/// with no two adjacent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GtidSet {
    sids: BTreeMap<Sid, Vec<(u64, u64)>>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid GTID set: {0}")]
pub struct GtidParseError(String);

impl GtidSet {
    pub fn is_empty(&self) -> bool {
        self.sids.is_empty()
    }

    /// Adds one transaction.
    pub fn add(&mut self, uuid: &str, tag: Option<&str>, gno: u64) {
        self.add_interval(
            (uuid.to_ascii_lowercase(), tag.map(str::to_ascii_lowercase)),
            gno,
            gno,
        );
    }

    pub fn add_event(&mut self, ev: &GtidEvent) {
        let uuid = uuid::Uuid::from_bytes(ev.sid()).hyphenated().to_string();
        let tag = ev.tag().map(|t| t.as_str().to_owned());
        self.add(&uuid, tag.as_deref(), ev.gno());
    }

    fn add_interval(&mut self, sid: Sid, start: u64, end: u64) {
        let list = self.sids.entry(sid).or_default();
        list.push((start, end));
        normalize(list);
    }

    /// `self ∪ other`.
    pub fn union(&mut self, other: &GtidSet) {
        for (sid, ivs) in &other.sids {
            let list = self.sids.entry(sid.clone()).or_default();
            list.extend_from_slice(ivs);
            normalize(list);
        }
    }

    pub fn contains(&self, uuid: &str, tag: Option<&str>, gno: u64) -> bool {
        let key = (uuid.to_ascii_lowercase(), tag.map(str::to_ascii_lowercase));
        self.sids
            .get(&key)
            .is_some_and(|ivs| ivs.iter().any(|&(s, e)| s <= gno && gno <= e))
    }

    /// `other ⊆ self`.
    pub fn contains_set(&self, other: &GtidSet) -> bool {
        other.sids.iter().all(|(sid, ivs)| {
            let Some(mine) = self.sids.get(sid) else {
                return ivs.is_empty();
            };
            ivs.iter()
                .all(|&(s, e)| mine.iter().any(|&(ms, me)| ms <= s && e <= me))
        })
    }

    /// The set in the form `Sid::from_str` accepts, one entry per (uuid, tag).
    pub fn sid_strings(&self) -> Vec<String> {
        self.sids
            .iter()
            .map(|((uuid, tag), ivs)| {
                let mut s = uuid.clone();
                if let Some(tag) = tag {
                    s.push(':');
                    s.push_str(tag);
                }
                for iv in ivs {
                    s.push(':');
                    push_interval(&mut s, *iv);
                }
                s
            })
            .collect()
    }
}

fn push_interval(s: &mut String, (start, end): (u64, u64)) {
    if start == end {
        s.push_str(&start.to_string());
    } else {
        s.push_str(&format!("{start}-{end}"));
    }
}

fn normalize(list: &mut Vec<(u64, u64)>) {
    list.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(list.len());
    for &(s, e) in list.iter() {
        match out.last_mut() {
            Some(last) if s <= last.1.saturating_add(1) => last.1 = last.1.max(e),
            _ => out.push((s, e)),
        }
    }
    *list = out;
}

fn is_tag(token: &str) -> bool {
    let mut chars = token.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl FromStr for GtidSet {
    type Err = GtidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut set = GtidSet::default();
        // `@@gtid_executed` separates entries with ",\n".
        for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let mut tokens = entry.split(':');
            let uuid_raw = tokens.next().unwrap_or_default().trim();
            let uuid = uuid::Uuid::parse_str(uuid_raw)
                .map_err(|_| GtidParseError(format!("bad uuid in `{entry}`")))?
                .hyphenated()
                .to_string();
            let mut tag: Option<String> = None;
            let mut saw_interval = false;
            for token in tokens {
                let token = token.trim();
                if is_tag(token) {
                    tag = Some(token.to_ascii_lowercase());
                    continue;
                }
                let (start, end) = match token.split_once('-') {
                    Some((a, b)) => (parse_gno(a, entry)?, parse_gno(b, entry)?),
                    None => {
                        let n = parse_gno(token, entry)?;
                        (n, n)
                    }
                };
                if start == 0 || end < start {
                    return Err(GtidParseError(format!("bad interval in `{entry}`")));
                }
                set.add_interval((uuid.clone(), tag.clone()), start, end);
                saw_interval = true;
            }
            if !saw_interval {
                return Err(GtidParseError(format!("no interval in `{entry}`")));
            }
        }
        Ok(set)
    }
}

fn parse_gno(s: &str, entry: &str) -> Result<u64, GtidParseError> {
    s.trim()
        .parse()
        .map_err(|_| GtidParseError(format!("bad number in `{entry}`")))
}

/// Canonical MySQL text form: `uuid:1-5:7:tag:1-2,uuid2:3`.
impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first_uuid = true;
        let mut current: Option<&str> = None;
        for ((uuid, tag), ivs) in &self.sids {
            if current != Some(uuid.as_str()) {
                if !first_uuid {
                    f.write_str(",")?;
                }
                first_uuid = false;
                current = Some(uuid);
                f.write_str(uuid)?;
            }
            if let Some(tag) = tag {
                write!(f, ":{tag}")?;
            }
            for iv in ivs {
                let mut s = String::new();
                push_interval(&mut s, *iv);
                write!(f, ":{s}")?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    const B: &str = "4d22fa47-71ca-11e1-9e33-c80aa9429562";

    #[test]
    fn parse_and_format_roundtrip() {
        let raw = format!("{A}:1-5:7:9-10,\n{B}:3");
        let set: GtidSet = raw.parse().unwrap();
        assert_eq!(set.to_string(), format!("{A}:1-5:7:9-10,{B}:3"));
        let again: GtidSet = set.to_string().parse().unwrap();
        assert_eq!(set, again);
        assert!(GtidSet::from_str("").unwrap().is_empty());
        assert_eq!(GtidSet::default().to_string(), "");
    }

    #[test]
    fn uppercase_uuid_is_normalized() {
        let set: GtidSet = format!("{}:1-2", A.to_uppercase()).parse().unwrap();
        assert!(set.contains(A, None, 2));
    }

    #[test]
    fn tagged_gtids() {
        let set: GtidSet = format!("{A}:1-3:mytag:1-2:other:5").parse().unwrap();
        assert!(set.contains(A, None, 3));
        assert!(set.contains(A, Some("mytag"), 2));
        assert!(!set.contains(A, Some("mytag"), 3));
        assert!(set.contains(A, Some("other"), 5));
        let s = set.to_string();
        assert_eq!(s, format!("{A}:1-3:mytag:1-2:other:5"));
        assert_eq!(s.parse::<GtidSet>().unwrap(), set);
        assert_eq!(
            set.sid_strings(),
            vec![
                format!("{A}:1-3"),
                format!("{A}:mytag:1-2"),
                format!("{A}:other:5")
            ]
        );
    }

    #[test]
    fn merge_adjacent_and_overlapping() {
        let mut set: GtidSet = format!("{A}:1-3:7-9").parse().unwrap();
        set.add(A, None, 4);
        assert_eq!(set.to_string(), format!("{A}:1-4:7-9"));
        set.add(A, None, 6);
        set.add(A, None, 5);
        assert_eq!(set.to_string(), format!("{A}:1-9"));
        set.add(A, None, 3);
        assert_eq!(set.to_string(), format!("{A}:1-9"));
        let other: GtidSet = format!("{A}:10-20,{B}:1").parse().unwrap();
        set.union(&other);
        assert_eq!(set.to_string(), format!("{A}:1-20,{B}:1"));
    }

    #[test]
    fn contains_and_subset() {
        let set: GtidSet = format!("{A}:1-10:20-30,{B}:1-5").parse().unwrap();
        assert!(set.contains(A, None, 1));
        assert!(set.contains(A, None, 25));
        assert!(!set.contains(A, None, 15));
        assert!(!set.contains(B, None, 6));
        assert!(!set.contains(A, Some("t"), 1));

        let sub: GtidSet = format!("{A}:2-9:20,{B}:5").parse().unwrap();
        assert!(set.contains_set(&sub));
        let not_sub: GtidSet = format!("{A}:9-21").parse().unwrap();
        assert!(!set.contains_set(&not_sub));
        let other_uuid: GtidSet = "aaaaaaaa-71ca-11e1-9e33-c80aa9429562:1".parse().unwrap();
        assert!(!set.contains_set(&other_uuid));
        assert!(set.contains_set(&GtidSet::default()));
        assert!(GtidSet::default().contains_set(&GtidSet::default()));
    }

    #[test]
    fn invalid_sets_are_rejected() {
        for bad in [
            "nope:1",
            A,
            &format!("{A}:0"),
            &format!("{A}:5-2"),
            &format!("{A}:x"),
            &format!("{A}:1-"),
        ] {
            assert!(bad.parse::<GtidSet>().is_err(), "{bad}");
        }
    }

    #[test]
    fn sid_strings_feed_mysql_common() {
        let set: GtidSet = format!("{A}:1-5:7,{B}:3").parse().unwrap();
        for s in set.sid_strings() {
            let sid: mysql_async::Sid<'static> = s.parse().unwrap();
            assert_eq!(sid.to_string(), s);
        }
    }
}
