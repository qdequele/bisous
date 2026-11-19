//! Source key → Meilisearch document id.

use sha2::{Digest, Sha256};

/// Meilisearch ids are integers or strings of `[a-zA-Z0-9_-]`, at most 511
/// bytes. A single key column that already fits is used as is, so ids stay
/// readable. Anything else (composite keys, UUIDs with braces, binary keys,
/// long strings) becomes the hex SHA-256 of the canonical JSON of the key.
///
/// The function is pure and stable: the same key always gives the same id,
/// which is what makes replays idempotent. Changing it is a breaking change
/// that requires a full resync of every index.
pub fn document_id(key: &[serde_json::Value]) -> String {
    if let [single] = key {
        match single {
            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => return n.to_string(),
            serde_json::Value::String(s) if is_valid_id(s) => return s.clone(),
            _ => {}
        }
    }
    let canonical = serde_json::Value::Array(key.to_vec()).to_string();
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

fn is_valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 511
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn simple_keys_stay_readable() {
        assert_eq!(document_id(&[json!(42)]), "42");
        assert_eq!(document_id(&[json!("abc-DEF_1")]), "abc-DEF_1");
    }

    #[test]
    fn unsafe_or_composite_keys_are_hashed() {
        let a = document_id(&[json!("a b")]);
        assert_eq!(a.len(), 64);
        assert_ne!(a, document_id(&[json!("a_b")]));
        let c = document_id(&[json!(1), json!("x")]);
        assert_eq!(c.len(), 64);
        assert_eq!(c, document_id(&[json!(1), json!("x")]));
        assert_ne!(c, document_id(&[json!("1"), json!("x")]));
        assert_eq!(document_id(&[json!(1.5)]).len(), 64);
        assert_eq!(document_id(&[json!("")]).len(), 64);
    }
}
