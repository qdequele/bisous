//! LSN ⇄ checkpoint.

use meili_connect_core::{Checkpoint, SourceError};
use pgwire_replication::Lsn;
use serde::{Deserialize, Serialize};

/// The checkpoint body: `{"lsn": "16/B374D848"}`. The LSN is the end of a
/// commit record (or a keep-alive position outside any transaction):
/// restarting the slot there replays every transaction that committed after
/// it and none before.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PgCheckpoint {
    lsn: String,
}

pub fn encode(lsn: Lsn) -> Checkpoint {
    Checkpoint(serde_json::json!({ "lsn": lsn.to_string() }))
}

pub fn decode(checkpoint: &Checkpoint) -> Result<Lsn, SourceError> {
    let body: PgCheckpoint = checkpoint.decode().map_err(|_| {
        SourceError::Fatal("checkpoint is not a postgres checkpoint ({\"lsn\": \"X/Y\"})".into())
    })?;
    parse(&body.lsn)
}

pub fn parse(text: &str) -> Result<Lsn, SourceError> {
    Lsn::parse(text.trim())
        .map_err(|_| SourceError::Fatal(format!("invalid LSN '{text}' in checkpoint")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip() {
        for raw in [0u64, 1, 0x16_B374_D848, u64::MAX] {
            let cp = encode(Lsn(raw));
            assert_eq!(decode(&cp).unwrap(), Lsn(raw));
        }
        assert_eq!(encode(Lsn(0x16_B374_D848)).0, json!({"lsn": "16/B374D848"}));
    }

    #[test]
    fn parses_postgres_text() {
        assert_eq!(parse("0/16B3748").unwrap(), Lsn(0x16B_3748));
        assert_eq!(
            decode(&Checkpoint(json!({"lsn": "1/0"}))).unwrap(),
            Lsn(1 << 32)
        );
    }

    #[test]
    fn rejects_foreign_checkpoints() {
        assert!(matches!(
            decode(&Checkpoint(json!({"gtid": "x"}))),
            Err(SourceError::Fatal(_))
        ));
        assert!(decode(&Checkpoint(json!({"lsn": "nope"}))).is_err());
        assert!(decode(&Checkpoint(json!(42))).is_err());
    }

    #[test]
    fn ordering_matches_numeric() {
        assert!(parse("1/0").unwrap() > parse("0/FFFFFFFF").unwrap());
    }
}
