//! The MySQL checkpoint: a GTID set, or a binlog file and offset when GTIDs
//! are off (or on MariaDB, whose GTIDs have another shape).

use meili_connect_core::{Checkpoint, SourceError};
use serde::{Deserialize, Serialize};

use crate::gtid::GtidSet;

/// Serialized form, stored by the engine as opaque JSON:
/// `{"mode":"gtid","gtid_set":"uuid:1-42"}` or
/// `{"mode":"file","file":"binlog.000003","pos":1234}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum Repr {
    Gtid { gtid_set: String },
    File { file: String, pos: u64 },
}

/// Where the stream (re)starts. Every transaction *in* the GTID set (or
/// before the file offset) is already applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Position {
    Gtid(GtidSet),
    File { file: String, pos: u64 },
}

impl Position {
    pub fn to_checkpoint(&self) -> Result<Checkpoint, SourceError> {
        let repr = match self {
            Position::Gtid(set) => Repr::Gtid {
                gtid_set: set.to_string(),
            },
            Position::File { file, pos } => Repr::File {
                file: file.clone(),
                pos: *pos,
            },
        };
        Checkpoint::new(repr).map_err(|e| SourceError::Fatal(format!("encode checkpoint: {e}")))
    }

    pub fn from_checkpoint(cp: &Checkpoint) -> Result<Self, SourceError> {
        // A checkpoint this source cannot read was not produced by it (or
        // by an incompatible version): only a resync recovers from that.
        let repr: Repr = cp
            .decode()
            .map_err(|e| SourceError::PositionLost(format!("unreadable mysql checkpoint: {e}")))?;
        match repr {
            Repr::Gtid { gtid_set } => gtid_set.parse().map(Position::Gtid).map_err(|e| {
                SourceError::PositionLost(format!("unreadable mysql checkpoint: {e}"))
            }),
            Repr::File { file, pos } if !file.is_empty() => Ok(Position::File { file, pos }),
            Repr::File { .. } => Err(SourceError::PositionLost(
                "unreadable mysql checkpoint: empty binlog file name".into(),
            )),
        }
    }

    pub fn is_gtid(&self) -> bool {
        matches!(self, Position::Gtid(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gtid_checkpoint_roundtrip() {
        let set: GtidSet = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42".parse().unwrap();
        let pos = Position::Gtid(set);
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(
            cp.0,
            json!({"mode": "gtid", "gtid_set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42"})
        );
        assert_eq!(Position::from_checkpoint(&cp).unwrap(), pos);
    }

    #[test]
    fn file_checkpoint_roundtrip() {
        let pos = Position::File {
            file: "binlog.000003".into(),
            pos: 1234,
        };
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(
            cp.0,
            json!({"mode": "file", "file": "binlog.000003", "pos": 1234})
        );
        assert_eq!(Position::from_checkpoint(&cp).unwrap(), pos);
        assert!(!pos.is_gtid());
    }

    #[test]
    fn empty_gtid_set_roundtrips() {
        let pos = Position::Gtid(GtidSet::default());
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(Position::from_checkpoint(&cp).unwrap(), pos);
    }

    #[test]
    fn foreign_checkpoints_are_position_lost() {
        for bad in [
            json!({"lsn": "0/16B3748"}),
            json!({"mode": "gtid", "gtid_set": "garbage"}),
            json!({"mode": "file", "file": "", "pos": 4}),
            json!(42),
        ] {
            let cp = Checkpoint(bad.clone());
            assert!(
                matches!(
                    Position::from_checkpoint(&cp),
                    Err(SourceError::PositionLost(_))
                ),
                "{bad}"
            );
        }
    }
}
