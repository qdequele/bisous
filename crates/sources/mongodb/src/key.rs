//! `_id` → the JSON key handed to `core::document_id`.
//!
//! MongoDB allows `_id`s of different types in one collection, and
//! `core::document_id` keeps a single integer or id-safe string as is, so
//! the plain BSON → JSON conversion would collide: `1` and `"1"` both give
//! `"1"`, an ObjectId and the string of its hex both give the hex. This
//! encoding is injective while keeping the common cases readable:
//!
//! | `_id` | key |
//! |---|---|
//! | ObjectId | its 24-char hex string |
//! | Int32, Int64 | a JSON number |
//! | String that parses as an integer, or is 24 or 64 hex chars | `{"$string": s}` (hashed) |
//! | any other String | the string |
//! | anything else | `{"$<type>": <canonical value>}` (hashed) |
//!
//! A 64-hex string is wrapped too, because that is the shape of a hashed id.
//!
//! The same function serves snapshot, stream and delete events, so a
//! document and its delete always map to the same id. Changing it changes
//! document ids: every MongoDB index would need a full resync.

use base64::Engine as _;
use mongodb::bson::{Bson, Document};
use serde_json::{Map, Number, Value};

/// The key of a document, from its `_id`.
pub fn key_of(id: &Bson) -> Value {
    match id {
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        Bson::Int32(i) => Value::Number((*i).into()),
        Bson::Int64(i) => Value::Number((*i).into()),
        Bson::String(s) if is_ambiguous(s) => typed("string", Value::String(s.clone())),
        Bson::String(s) => Value::String(s.clone()),
        Bson::Double(f) => typed("double", double(*f)),
        Bson::Decimal128(d) => typed("decimal", Value::String(d.to_string())),
        Bson::DateTime(dt) => typed("date", Value::Number(dt.timestamp_millis().into())),
        Bson::Binary(b) => {
            let mut m = Map::with_capacity(2);
            m.insert("subtype".into(), Value::Number(u8::from(b.subtype).into()));
            m.insert(
                "base64".into(),
                Value::String(base64::engine::general_purpose::STANDARD.encode(&b.bytes)),
            );
            typed("binary", Value::Object(m))
        }
        Bson::Timestamp(ts) => {
            let mut m = Map::with_capacity(2);
            m.insert("t".into(), Value::Number(ts.time.into()));
            m.insert("i".into(), Value::Number(ts.increment.into()));
            typed("timestamp", Value::Object(m))
        }
        Bson::Boolean(b) => typed("bool", Value::Bool(*b)),
        Bson::Document(d) => typed("document", document(d)),
        Bson::Array(items) => typed("array", Value::Array(items.iter().map(key_of).collect())),
        Bson::Null => typed("null", Value::Null),
        Bson::Undefined => typed("undefined", Value::Null),
        Bson::MinKey => typed("minKey", Value::Null),
        Bson::MaxKey => typed("maxKey", Value::Null),
        Bson::RegularExpression(re) => typed(
            "regex",
            Value::Array(vec![
                Value::String(re.pattern.clone()),
                Value::String(re.options.clone()),
            ]),
        ),
        Bson::Symbol(s) => typed("symbol", Value::String(s.clone())),
        Bson::JavaScriptCode(c) => typed("code", Value::String(c.clone())),
        Bson::JavaScriptCodeWithScope(c) => typed(
            "codeWithScope",
            Value::Array(vec![Value::String(c.code.clone()), document(&c.scope)]),
        ),
        Bson::DbPointer(_) => typed("dbPointer", id.clone().into_canonical_extjson()),
    }
}

fn typed(kind: &str, value: Value) -> Value {
    let mut m = Map::with_capacity(1);
    m.insert(format!("${kind}"), value);
    Value::Object(m)
}

/// Ordered `[name, key]` pairs: field order is part of a compound `_id`'s
/// identity in MongoDB, and a JSON object would not keep it.
fn document(d: &Document) -> Value {
    Value::Array(
        d.iter()
            .map(|(k, v)| Value::Array(vec![Value::String(k.clone()), key_of(v)]))
            .collect(),
    )
}

fn double(f: f64) -> Value {
    match Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None if f.is_nan() => Value::String("NaN".into()),
        None if f > 0.0 => Value::String("Infinity".into()),
        None => Value::String("-Infinity".into()),
    }
}

/// A string `core::document_id` could confuse with another type's key: one
/// that reads as an integer, or has the shape of an ObjectId hex or of a
/// hashed id.
fn is_ambiguous(s: &str) -> bool {
    let digits = s.strip_prefix(['-', '+']).unwrap_or(s);
    let integer = !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit());
    let hex = matches!(s.len(), 24 | 64) && s.bytes().all(|b| b.is_ascii_hexdigit());
    integer || hex
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_connect_core::document_id;
    use mongodb::bson::spec::BinarySubtype;
    use mongodb::bson::{Binary, DateTime, Decimal128, Timestamp, doc, oid::ObjectId};
    use serde_json::json;

    fn id(b: Bson) -> String {
        document_id(&[key_of(&b)])
    }

    #[test]
    fn common_keys_stay_readable() {
        let oid = ObjectId::parse_str("65a1f0c2e4b0a1b2c3d4e5f6").unwrap();
        assert_eq!(id(Bson::ObjectId(oid)), "65a1f0c2e4b0a1b2c3d4e5f6");
        assert_eq!(id(Bson::Int32(7)), "7");
        assert_eq!(id(Bson::Int64(-9)), "-9");
        assert_eq!(id(Bson::String("user-42".into())), "user-42");
        assert_eq!(id(Bson::String("abc".into())), "abc");
    }

    #[test]
    fn different_types_never_share_an_id() {
        let oid = ObjectId::parse_str("65a1f0c2e4b0a1b2c3d4e5f6").unwrap();
        let date = DateTime::from_millis(1_700_000_000_123);
        let uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: hex::decode("0123456789abcdef0123456789abcdef").unwrap(),
        };
        let pairs = [
            (Bson::Int32(1), Bson::String("1".into())),
            (Bson::Int64(1), Bson::String("1".into())),
            (Bson::ObjectId(oid), Bson::String(oid.to_hex())),
            (Bson::Int32(1), Bson::Double(1.0)),
            (
                Bson::DateTime(date),
                Bson::String(date.try_to_rfc3339_string().unwrap()),
            ),
            (
                Bson::DateTime(date),
                Bson::String("2023-11-14T22:13:20.123Z".into()),
            ),
            (
                Bson::Binary(uuid),
                Bson::String("01234567-89ab-cdef-0123-456789abcdef".into()),
            ),
            (
                Bson::Decimal128("1".parse::<Decimal128>().unwrap()),
                Bson::Int32(1),
            ),
            (
                Bson::Decimal128("1".parse::<Decimal128>().unwrap()),
                Bson::String("1".into()),
            ),
            (Bson::Double(1.0), Bson::String("1.0".into())),
            (Bson::Boolean(true), Bson::String("true".into())),
            (
                Bson::Timestamp(Timestamp {
                    time: 1,
                    increment: 2,
                }),
                Bson::Document(doc! {"t": 1_i32, "i": 2_i32}),
            ),
            (
                Bson::Document(doc! {"a": 1_i32, "b": 2_i32}),
                Bson::Document(doc! {"b": 2_i32, "a": 1_i32}),
            ),
            (
                Bson::Document(doc! {"a": 1_i32}),
                Bson::Document(doc! {"a": "1"}),
            ),
            (
                Bson::Document(doc! {"a": oid}),
                Bson::Document(doc! {"a": oid.to_hex()}),
            ),
            (
                Bson::String("$string".into()),
                Bson::Document(doc! {"$string": "x"}),
            ),
            (Bson::Null, Bson::String("null".into())),
            (Bson::MinKey, Bson::MaxKey),
            (
                Bson::String("a".repeat(64)),
                Bson::Array(vec![Bson::String("a".into())]),
            ),
        ];
        for (a, b) in pairs {
            assert_ne!(id(a.clone()), id(b.clone()), "{a:?} vs {b:?}");
        }
        // A 64-hex string cannot pose as the hash of another key.
        let hashed = id(Bson::Double(1.5));
        assert_eq!(hashed.len(), 64);
        assert_ne!(id(Bson::String(hashed.clone())), hashed);
    }

    #[test]
    fn ambiguous_strings_are_wrapped() {
        assert_eq!(key_of(&Bson::String("1".into())), json!({"$string": "1"}));
        assert_eq!(
            key_of(&Bson::String("-12".into())),
            json!({"$string": "-12"})
        );
        assert_eq!(
            key_of(&Bson::String("65A1F0C2E4B0A1B2C3D4E5F6".into())),
            json!({"$string": "65A1F0C2E4B0A1B2C3D4E5F6"})
        );
        assert_eq!(key_of(&Bson::String("1a".into())), json!("1a"));
        assert_eq!(key_of(&Bson::String("-".into())), json!("-"));
        assert_eq!(key_of(&Bson::Double(f64::NAN)), json!({"$double": "NaN"}));
        assert_eq!(
            key_of(&Bson::Document(doc! {"t": 1_i32, "n": "1"})),
            json!({"$document": [["t", 1], ["n", {"$string": "1"}]]})
        );
    }
}
