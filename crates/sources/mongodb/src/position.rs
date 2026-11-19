//! The checkpoint format: a change stream resume token, or a cluster time
//! when the server returned no token.

use meili_connect_core::{Checkpoint, SourceError};
use mongodb::bson::{Bson, Timestamp};
use mongodb::change_stream::event::ResumeToken;
use serde::{Deserialize, Serialize};

/// Stored as JSON in `SyncState.checkpoint`:
///
/// - `{"resume_token": {"_data": "8265…"}}` (canonical Extended JSON of the
///   token, so it round-trips losslessly), or
/// - `{"operation_time": {"t": 1700000000, "i": 1}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Position {
    ResumeToken(serde_json::Value),
    OperationTime { t: u32, i: u32 },
}

impl Position {
    pub fn from_token(token: &ResumeToken) -> Result<Self, SourceError> {
        let bson = mongodb::bson::to_bson(token)
            .map_err(|e| SourceError::Fatal(format!("cannot encode resume token: {e}")))?;
        Ok(Position::ResumeToken(bson.into_canonical_extjson()))
    }

    pub fn from_timestamp(ts: Timestamp) -> Self {
        Position::OperationTime {
            t: ts.time,
            i: ts.increment,
        }
    }

    pub fn token(&self) -> Result<Option<ResumeToken>, SourceError> {
        match self {
            Position::ResumeToken(json) => {
                let bson = Bson::try_from(json.clone())
                    .map_err(|e| invalid(format!("resume token is not Extended JSON: {e}")))?;
                let token = mongodb::bson::from_bson::<ResumeToken>(bson)
                    .map_err(|e| invalid(format!("resume token: {e}")))?;
                Ok(Some(token))
            }
            Position::OperationTime { .. } => Ok(None),
        }
    }

    pub fn timestamp(&self) -> Option<Timestamp> {
        match self {
            Position::OperationTime { t, i } => Some(Timestamp {
                time: *t,
                increment: *i,
            }),
            Position::ResumeToken(_) => None,
        }
    }

    pub fn to_checkpoint(&self) -> Result<Checkpoint, SourceError> {
        Checkpoint::new(self).map_err(|e| SourceError::Fatal(format!("encode checkpoint: {e}")))
    }

    /// A checkpoint this source cannot read is a position we cannot honour:
    /// the engine resyncs.
    pub fn from_checkpoint(cp: &Checkpoint) -> Result<Self, SourceError> {
        let pos: Position = cp
            .decode()
            .map_err(|e| invalid(format!("unreadable MongoDB checkpoint: {e}")))?;
        pos.token()?;
        Ok(pos)
    }
}

fn invalid(msg: String) -> SourceError {
    SourceError::PositionLost(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;
    use serde_json::json;

    fn token() -> ResumeToken {
        mongodb::bson::from_bson(Bson::Document(doc! {
            "_data": "8265A1F0C2000000012B022C0100296E5A1004"
        }))
        .unwrap()
    }

    #[test]
    fn resume_token_round_trip() {
        let pos = Position::from_token(&token()).unwrap();
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(
            cp.0,
            json!({"resume_token": {"_data": "8265A1F0C2000000012B022C0100296E5A1004"}})
        );
        let back = Position::from_checkpoint(&cp).unwrap();
        assert_eq!(back, pos);
        assert_eq!(back.token().unwrap(), Some(token()));
    }

    #[test]
    fn operation_time_round_trip() {
        let pos = Position::from_timestamp(Timestamp {
            time: 1_700_000_000,
            increment: 4,
        });
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(
            cp.0,
            json!({"operation_time": {"t": 1_700_000_000, "i": 4}})
        );
        let back = Position::from_checkpoint(&cp).unwrap();
        assert_eq!(
            back.timestamp().map(|t| (t.time, t.increment)),
            Some((1_700_000_000, 4))
        );
        assert_eq!(back.token().unwrap(), None);
    }

    #[test]
    fn garbage_is_position_lost() {
        let err = Position::from_checkpoint(&Checkpoint(json!({"lsn": "0/1"}))).unwrap_err();
        assert!(matches!(err, SourceError::PositionLost(_)));
    }
}
