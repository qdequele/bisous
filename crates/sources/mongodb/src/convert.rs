//! BSON → JSON for Meilisearch documents.
//!
//! | BSON | JSON |
//! |---|---|
//! | ObjectId | 24-char hex string |
//! | DateTime | RFC 3339 / ISO-8601 string, UTC, millisecond precision (out-of-range dates: epoch millis as a number) |
//! | Int32, Int64 | number |
//! | Double | number; NaN and ±Infinity become `null` (JSON has no such numbers) |
//! | Decimal128 | number when the value round-trips exactly through JSON, else its decimal string |
//! | Binary, UUID subtype (4) of 16 bytes | canonical UUID string |
//! | Binary, any other subtype | standard base64 string |
//! | Timestamp | `{"t": seconds, "i": increment}` |
//! | Regex | `"/pattern/options"` |
//! | JavaScript code (with or without scope), Symbol | string |
//! | DBPointer | relaxed Extended JSON |
//! | MinKey, MaxKey, Undefined, Null | `null` |
//! | Document, Array | object, array (recursively) |

use base64::Engine as _;
use meili_connect_core::Document as JsonDocument;
use mongodb::bson::{Bson, Document, spec::BinarySubtype};
use serde_json::{Number, Value};

pub fn bson_to_json(value: &Bson) -> Value {
    match value {
        Bson::Double(f) => Number::from_f64(*f).map_or(Value::Null, Value::Number),
        Bson::String(s) | Bson::Symbol(s) | Bson::JavaScriptCode(s) => Value::String(s.clone()),
        Bson::JavaScriptCodeWithScope(c) => Value::String(c.code.clone()),
        Bson::Array(items) => Value::Array(items.iter().map(bson_to_json).collect()),
        Bson::Document(doc) => Value::Object(document_to_json(doc)),
        Bson::Boolean(b) => Value::Bool(*b),
        Bson::Null | Bson::Undefined | Bson::MinKey | Bson::MaxKey => Value::Null,
        Bson::RegularExpression(re) => Value::String(format!("/{}/{}", re.pattern, re.options)),
        Bson::Int32(i) => Value::Number((*i).into()),
        Bson::Int64(i) => Value::Number((*i).into()),
        Bson::Timestamp(ts) => {
            let mut m = serde_json::Map::with_capacity(2);
            m.insert("t".into(), Value::Number(ts.time.into()));
            m.insert("i".into(), Value::Number(ts.increment.into()));
            Value::Object(m)
        }
        Bson::Binary(bin) => {
            if bin.subtype == BinarySubtype::Uuid && bin.bytes.len() == 16 {
                Value::String(format_uuid(&bin.bytes))
            } else {
                Value::String(base64::engine::general_purpose::STANDARD.encode(&bin.bytes))
            }
        }
        Bson::ObjectId(oid) => Value::String(oid.to_hex()),
        Bson::DateTime(dt) => {
            let millis = dt.timestamp_millis();
            match chrono::DateTime::from_timestamp_millis(millis) {
                Some(d) => Value::String(d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
                None => Value::Number(millis.into()),
            }
        }
        Bson::Decimal128(d) => decimal_to_json(&d.to_string()),
        Bson::DbPointer(_) => value.clone().into_relaxed_extjson(),
    }
}

pub fn document_to_json(doc: &Document) -> JsonDocument {
    let mut out = serde_json::Map::with_capacity(doc.len());
    for (k, v) in doc {
        out.insert(k.clone(), bson_to_json(v));
    }
    out
}

fn format_uuid(b: &[u8]) -> String {
    let h = hex::encode(b);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// A Decimal128 becomes a JSON number only when nothing is lost: an integer
/// that fits `i64`, or a value whose shortest `f64` rendering is the same
/// decimal. Otherwise (`0.1000000000000000055511151231257827`, `1.50`,
/// `NaN`, `1E+6144`) it stays a string.
fn decimal_to_json(s: &str) -> Value {
    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }
    if s.bytes()
        .all(|b| b.is_ascii_digit() || b == b'.' || b == b'-')
        && let Ok(f) = s.parse::<f64>()
        && let Some(n) = Number::from_f64(f)
        && n.to_string() == s
    {
        return Value::Number(n);
    }
    Value::String(s.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::{
        Binary, DateTime, Decimal128, JavaScriptCodeWithScope, Regex, Timestamp, doc, oid::ObjectId,
    };
    use serde_json::json;

    #[test]
    fn scalars() {
        let oid = ObjectId::parse_str("65a1f0c2e4b0a1b2c3d4e5f6").unwrap();
        let d = doc! {
            "oid": oid,
            "s": "x",
            "i32": 7_i32,
            "i64": 9_007_199_254_740_993_i64,
            "f": 1.5,
            "nan": f64::NAN,
            "b": true,
            "n": Bson::Null,
            "u": Bson::Undefined,
            "min": Bson::MinKey,
            "max": Bson::MaxKey,
            "date": DateTime::from_millis(1_700_000_000_123),
            "ts": Timestamp { time: 1_700_000_000, increment: 3 },
            "re": Regex { pattern: "^a.*".into(), options: "i".into() },
            "code": Bson::JavaScriptCode("function(){}".into()),
            "scoped": JavaScriptCodeWithScope { code: "f()".into(), scope: doc!{} },
            "sym": Bson::Symbol("sym".into()),
        };
        assert_eq!(
            Value::Object(document_to_json(&d)),
            json!({
                "oid": "65a1f0c2e4b0a1b2c3d4e5f6",
                "s": "x",
                "i32": 7,
                "i64": 9_007_199_254_740_993_i64,
                "f": 1.5,
                "nan": null,
                "b": true,
                "n": null,
                "u": null,
                "min": null,
                "max": null,
                "date": "2023-11-14T22:13:20.123Z",
                "ts": {"t": 1_700_000_000, "i": 3},
                "re": "/^a.*/i",
                "code": "function(){}",
                "scoped": "f()",
                "sym": "sym",
            })
        );
    }

    #[test]
    fn binary() {
        let uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: hex::decode("0123456789abcdef0123456789abcdef").unwrap(),
        };
        assert_eq!(
            bson_to_json(&Bson::Binary(uuid)),
            json!("01234567-89ab-cdef-0123-456789abcdef")
        );
        let generic = Binary {
            subtype: BinarySubtype::Generic,
            bytes: b"hello".to_vec(),
        };
        assert_eq!(bson_to_json(&Bson::Binary(generic)), json!("aGVsbG8="));
        let short_uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: vec![1, 2],
        };
        assert_eq!(bson_to_json(&Bson::Binary(short_uuid)), json!("AQI="));
    }

    #[test]
    fn decimals() {
        let dec = |s: &str| bson_to_json(&Bson::Decimal128(s.parse::<Decimal128>().unwrap()));
        assert_eq!(dec("42"), json!(42));
        assert_eq!(dec("-3"), json!(-3));
        assert_eq!(dec("1.5"), json!(1.5));
        assert_eq!(dec("0.1"), json!(0.1));
        assert_eq!(dec("1.50"), json!("1.50"));
        assert_eq!(
            dec("0.1000000000000000055511151231257827"),
            json!("0.1000000000000000055511151231257827")
        );
        assert_eq!(dec("NaN"), json!("NaN"));
        assert_eq!(dec("99999999999999999999"), json!("99999999999999999999"));
    }

    #[test]
    fn nested_and_out_of_range_dates() {
        let d = doc! {
            "a": [1_i32, {"b": [ObjectId::parse_str("65a1f0c2e4b0a1b2c3d4e5f6").unwrap()]}],
            "far": DateTime::from_millis(i64::MAX),
        };
        assert_eq!(
            Value::Object(document_to_json(&d)),
            json!({"a": [1, {"b": ["65a1f0c2e4b0a1b2c3d4e5f6"]}], "far": i64::MAX})
        );
    }
}
