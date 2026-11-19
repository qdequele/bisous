//! The Kafka checkpoint: `{ topic: { partition: next_offset } }`.

use std::collections::BTreeMap;

use meili_connect_core::{Checkpoint, SourceError};

/// Next offset to read, per topic and partition. Serialized as
/// `{"topic": {"0": 42}}` (JSON object keys are strings).
pub type Positions = BTreeMap<String, BTreeMap<i32, i64>>;

pub fn encode(positions: &Positions) -> Result<Checkpoint, SourceError> {
    Checkpoint::new(positions).map_err(|e| SourceError::Fatal(format!("encode checkpoint: {e}")))
}

pub fn decode(checkpoint: &Checkpoint) -> Result<Positions, SourceError> {
    let positions: Positions = checkpoint
        .decode()
        .map_err(|e| SourceError::Fatal(format!("not a kafka checkpoint: {e}")))?;
    if positions
        .values()
        .flat_map(|p| p.iter())
        .any(|(&p, &o)| p < 0 || o < 0)
    {
        return Err(SourceError::Fatal(
            "kafka checkpoint holds a negative partition or offset".to_owned(),
        ));
    }
    Ok(positions)
}

/// Where each partition of the mapped topics starts when resuming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    pub positions: Positions,
    /// Partitions the checkpoint did not know (topic grew, topic newly
    /// mapped). They start at their log start.
    pub added: Vec<(String, i32)>,
}

/// Low and high watermark of one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

/// Merges a saved checkpoint with the current partitions of the mapped
/// topics. Topics no longer mapped are dropped. A saved offset outside
/// `[low, high]` (retention passed, or the topic was recreated) or a saved
/// partition that no longer exists is `PositionLost`.
pub fn plan_resume(
    saved: &Positions,
    current: &BTreeMap<String, BTreeMap<i32, Watermarks>>,
) -> Result<ResumePlan, SourceError> {
    let mut positions = Positions::new();
    let mut added = Vec::new();
    for (topic, partitions) in current {
        let saved_topic = saved.get(topic);
        if let Some(saved_topic) = saved_topic
            && let Some(gone) = saved_topic.keys().find(|p| !partitions.contains_key(p))
        {
            return Err(SourceError::PositionLost(format!(
                "partition {topic}/{gone} in the checkpoint no longer exists"
            )));
        }
        let out = positions.entry(topic.clone()).or_default();
        for (&partition, wm) in partitions {
            match saved_topic.and_then(|t| t.get(&partition)) {
                Some(&offset) if offset < wm.low => {
                    return Err(SourceError::PositionLost(format!(
                        "offset {offset} of {topic}/{partition} is below the log start {} \
                         (retention passed)",
                        wm.low
                    )));
                }
                Some(&offset) if offset > wm.high => {
                    return Err(SourceError::PositionLost(format!(
                        "offset {offset} of {topic}/{partition} is beyond the log end {} \
                         (topic recreated or truncated)",
                        wm.high
                    )));
                }
                Some(&offset) => {
                    out.insert(partition, offset);
                }
                None => {
                    out.insert(partition, wm.low);
                    added.push((topic.clone(), partition));
                }
            }
        }
    }
    Ok(ResumePlan { positions, added })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn wm(low: i64, high: i64) -> Watermarks {
        Watermarks { low, high }
    }

    #[test]
    fn round_trip_and_shape() {
        let mut p = Positions::new();
        p.entry("orders".into()).or_default().insert(0, 42);
        p.entry("orders".into()).or_default().insert(3, 7);
        let cp = encode(&p).unwrap();
        assert_eq!(cp.0, json!({"orders": {"0": 42, "3": 7}}));
        assert_eq!(decode(&cp).unwrap(), p);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(matches!(
            decode(&Checkpoint(json!({"t": {"x": 1}}))),
            Err(SourceError::Fatal(_))
        ));
        assert!(matches!(
            decode(&Checkpoint(json!({"t": {"0": -1}}))),
            Err(SourceError::Fatal(_))
        ));
        assert!(matches!(
            decode(&Checkpoint(json!("lsn"))),
            Err(SourceError::Fatal(_))
        ));
    }

    #[test]
    fn merge_keeps_saved_and_adds_new_partitions_from_earliest() {
        let saved: Positions = [
            ("a".into(), [(0, 5)].into()),
            ("gone".into(), [(0, 1)].into()),
        ]
        .into();
        let current = [
            ("a".to_owned(), [(0, wm(0, 10)), (1, wm(3, 9))].into()),
            ("b".to_owned(), [(0, wm(2, 4))].into()),
        ]
        .into();
        let plan = plan_resume(&saved, &current).unwrap();
        let expected: Positions = [
            ("a".into(), [(0, 5), (1, 3)].into()),
            ("b".into(), [(0, 2)].into()),
        ]
        .into();
        assert_eq!(plan.positions, expected);
        assert_eq!(plan.added, vec![("a".into(), 1), ("b".into(), 0)]);
    }

    #[test]
    fn boundaries_are_valid() {
        let saved: Positions = [("a".into(), [(0, 3), (1, 9)].into())].into();
        let current = [("a".to_owned(), [(0, wm(3, 10)), (1, wm(0, 9))].into())].into();
        assert!(plan_resume(&saved, &current).unwrap().added.is_empty());
    }

    #[test]
    fn retention_passed_is_position_lost() {
        let saved: Positions = [("a".into(), [(0, 2)].into())].into();
        let current = [("a".to_owned(), [(0, wm(3, 10))].into())].into();
        assert!(matches!(
            plan_resume(&saved, &current),
            Err(SourceError::PositionLost(_))
        ));
    }

    #[test]
    fn beyond_end_or_missing_partition_is_position_lost() {
        let saved: Positions = [("a".into(), [(0, 11)].into())].into();
        let current = [("a".to_owned(), [(0, wm(0, 10))].into())].into();
        assert!(matches!(
            plan_resume(&saved, &current),
            Err(SourceError::PositionLost(_))
        ));
        let saved: Positions = [("a".into(), [(4, 1)].into())].into();
        assert!(matches!(
            plan_resume(&saved, &current),
            Err(SourceError::PositionLost(_))
        ));
    }
}
