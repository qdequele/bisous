//! DynamoDB `AttributeValue` → JSON, items and stream records → `Change`.
//!
//! | DynamoDB | JSON |
//! |---|---|
//! | `S` | string |
//! | `N` | number when it round-trips exactly through `i64` or `f64`, else the decimal string |
//! | `BOOL` | bool |
//! | `NULL` | null |
//! | `B` | base64 string (standard alphabet, padded) |
//! | `SS` / `NS` / `BS` | arrays of the above |
//! | `L` / `M` | arrays / objects, recursively |

use std::collections::HashMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use meili_connect_core::{Change, Document, Op, SourceError};
use serde_json::{Map, Value};

/// DynamoDB numbers have up to 38 significant digits. Keep them as JSON
/// numbers only when nothing is lost, otherwise as their exact decimal text.
pub fn number_to_json(n: &str) -> Value {
    let trimmed = n.trim();
    if let Ok(i) = trimmed.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = trimmed.parse::<f64>()
        && f.is_finite()
        && let Some(num) = serde_json::Number::from_f64(f)
        && let (Some(a), Some(b)) = (
            normalize_decimal(trimmed),
            normalize_decimal(&format!("{f:e}")),
        )
        && a == b
    {
        return Value::Number(num);
    }
    Value::String(trimmed.to_owned())
}

/// Canonical (negative, significant digits, exponent of the last digit) of
/// a decimal literal, so `"1.50"`, `"15e-1"` and `"1.5e0"` compare equal.
fn normalize_decimal(s: &str) -> Option<(bool, String, i64)> {
    let (neg, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int, frac) = match mantissa.find('.') {
        Some(i) => (&mantissa[..i], &mantissa[i + 1..]),
        None => (mantissa, ""),
    };
    if int.is_empty() && frac.is_empty() {
        return None;
    }
    if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut digits: String = format!("{int}{frac}");
    let mut exp = exp.checked_sub(i64::try_from(frac.len()).ok()?)?;
    let lead = digits.len() - digits.trim_start_matches('0').len();
    digits.drain(..lead);
    if digits.is_empty() {
        return Some((false, "0".to_owned(), 0));
    }
    while digits.ends_with('0') {
        digits.pop();
        exp = exp.checked_add(1)?;
    }
    Some((neg, digits, exp))
}

fn blob_to_json(bytes: &[u8]) -> Value {
    Value::String(BASE64.encode(bytes))
}

/// Generates the conversion for one SDK's `AttributeValue` (DynamoDB and
/// DynamoDB Streams have distinct but identical types).
macro_rules! attribute_conversions {
    ($module:ident, $av:ty) => {
        pub mod $module {
            use super::*;

            pub fn value_to_json(value: &$av) -> Result<Value, SourceError> {
                type Av = $av;
                Ok(match value {
                    Av::S(s) => Value::String(s.clone()),
                    Av::N(n) => number_to_json(n),
                    Av::Bool(b) => Value::Bool(*b),
                    Av::Null(_) => Value::Null,
                    Av::B(b) => blob_to_json(b.as_ref()),
                    Av::Ss(v) => Value::Array(v.iter().cloned().map(Value::String).collect()),
                    Av::Ns(v) => Value::Array(v.iter().map(|n| number_to_json(n)).collect()),
                    Av::Bs(v) => Value::Array(v.iter().map(|b| blob_to_json(b.as_ref())).collect()),
                    Av::L(v) => {
                        Value::Array(v.iter().map(value_to_json).collect::<Result<_, _>>()?)
                    }
                    Av::M(m) => Value::Object(item_to_document(m)?),
                    _ => {
                        return Err(SourceError::Fatal(
                            "unsupported DynamoDB attribute type (newer than this connector)"
                                .into(),
                        ));
                    }
                })
            }

            pub fn item_to_document(item: &HashMap<String, $av>) -> Result<Document, SourceError> {
                let mut doc = Map::with_capacity(item.len());
                for (k, v) in item {
                    doc.insert(k.clone(), value_to_json(v)?);
                }
                Ok(doc)
            }

            /// The key attributes, in key-schema order (partition, then sort).
            pub fn key_values(
                table: &str,
                key_names: &[String],
                item: &HashMap<String, $av>,
            ) -> Result<Vec<Value>, SourceError> {
                key_names
                    .iter()
                    .map(|name| match item.get(name) {
                        Some(v) => value_to_json(v),
                        None => Err(SourceError::Fatal(format!(
                            "an item of table `{table}` has no key attribute `{name}`"
                        ))),
                    })
                    .collect()
            }
        }
    };
}

attribute_conversions!(table, aws_sdk_dynamodb::types::AttributeValue);
attribute_conversions!(stream, aws_sdk_dynamodbstreams::types::AttributeValue);

/// A scanned item → `Upsert`.
pub fn item_to_change(
    table_name: &str,
    key_names: &[String],
    item: &HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
) -> Result<Change, SourceError> {
    Ok(Change {
        collection: table_name.to_owned(),
        key: table::key_values(table_name, key_names, item)?,
        op: Op::Upsert {
            doc: table::item_to_document(item)?,
        },
    })
}

/// A stream record → `Change`, plus its sequence number.
///
/// `INSERT`/`MODIFY` → `Upsert(NewImage)`; `REMOVE` (including TTL
/// expiries) → `Delete(Keys)`. A missing `NewImage` means the stream's view
/// type is `KEYS_ONLY` or `OLD_IMAGE`: a configuration error.
pub fn record_to_change(
    table_name: &str,
    key_names: &[String],
    record: &aws_sdk_dynamodbstreams::types::Record,
) -> Result<(Change, Option<String>), SourceError> {
    use aws_sdk_dynamodbstreams::types::OperationType;

    let Some(data) = record.dynamodb() else {
        return Err(SourceError::Fatal(format!(
            "a stream record of table `{table_name}` has no `dynamodb` payload"
        )));
    };
    let seq = data.sequence_number().map(str::to_owned);
    let keys = data.keys().ok_or_else(|| {
        SourceError::Fatal(format!(
            "a stream record of table `{table_name}` has no `Keys`"
        ))
    })?;
    let key = stream::key_values(table_name, key_names, keys)?;
    let op = match record.event_name() {
        Some(OperationType::Insert) | Some(OperationType::Modify) => {
            let image = data.new_image().ok_or_else(|| {
                SourceError::Config(format!(
                    "the stream of table `{table_name}` does not carry new images; set its view type to NEW_IMAGE or NEW_AND_OLD_IMAGES"
                ))
            })?;
            Op::Upsert {
                doc: stream::item_to_document(image)?,
            }
        }
        Some(OperationType::Remove) => Op::Delete,
        other => {
            return Err(SourceError::Fatal(format!(
                "unsupported stream event `{other:?}` on table `{table_name}`"
            )));
        }
    };
    Ok((
        Change {
            collection: table_name.to_owned(),
            key,
            op,
        },
        seq,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue as Av;
    use aws_sdk_dynamodbstreams::types::{
        AttributeValue as Sv, OperationType, Record, StreamRecord,
    };
    use aws_smithy_types::Blob;
    use serde_json::json;

    #[test]
    fn numbers_keep_precision() {
        assert_eq!(number_to_json("42"), json!(42));
        assert_eq!(number_to_json("-7"), json!(-7));
        assert_eq!(number_to_json("1.5"), json!(1.5));
        assert_eq!(number_to_json("0.1"), json!(0.1));
        assert_eq!(number_to_json("1.50"), json!(1.5));
        assert_eq!(number_to_json("1E+2"), json!(100.0));
        assert_eq!(number_to_json("-0.000123"), json!(-0.000123));
        // Beyond i64 and not exactly representable as f64.
        assert_eq!(
            number_to_json("18446744073709551617"),
            json!("18446744073709551617")
        );
        assert_eq!(
            number_to_json("3.14159265358979323846264338327950288"),
            json!("3.14159265358979323846264338327950288")
        );
        assert_eq!(
            number_to_json("9007199254740993.5"),
            json!("9007199254740993.5")
        );
        assert_eq!(number_to_json("1E+126"), json!(1e126));
    }

    #[test]
    fn normalizes_decimals() {
        assert_eq!(normalize_decimal("1.50"), normalize_decimal("15e-1"));
        assert_eq!(normalize_decimal("100"), normalize_decimal("1e2"));
        assert_eq!(normalize_decimal("0.000"), Some((false, "0".into(), 0)));
        assert_eq!(normalize_decimal("abc"), None);
        assert_eq!(normalize_decimal("."), None);
    }

    #[test]
    fn every_attribute_type() {
        let item: HashMap<String, Av> = HashMap::from([
            ("s".into(), Av::S("hi".into())),
            ("n".into(), Av::N("3".into())),
            ("f".into(), Av::N("2.25".into())),
            ("t".into(), Av::Bool(true)),
            ("z".into(), Av::Null(true)),
            ("b".into(), Av::B(Blob::new(vec![0u8, 1, 2, 255]))),
            ("ss".into(), Av::Ss(vec!["a".into(), "b".into()])),
            ("ns".into(), Av::Ns(vec!["1".into(), "1.5".into()])),
            ("bs".into(), Av::Bs(vec![Blob::new(b"x".to_vec())])),
            (
                "l".into(),
                Av::L(vec![Av::S("x".into()), Av::N("1".into()), Av::Null(true)]),
            ),
            (
                "m".into(),
                Av::M(HashMap::from([(
                    "inner".into(),
                    Av::M(HashMap::from([("deep".into(), Av::Bool(false))])),
                )])),
            ),
        ]);
        let doc = table::item_to_document(&item).unwrap();
        assert_eq!(
            Value::Object(doc),
            json!({
                "s": "hi", "n": 3, "f": 2.25, "t": true, "z": null,
                "b": "AAEC/w==", "ss": ["a", "b"], "ns": [1, 1.5], "bs": ["eA=="],
                "l": ["x", 1, null], "m": {"inner": {"deep": false}}
            })
        );
    }

    #[test]
    fn scanned_item_keys_follow_key_schema_order() {
        let item: HashMap<String, Av> = HashMap::from([
            ("sk".into(), Av::N("7".into())),
            ("pk".into(), Av::S("user#1".into())),
            ("name".into(), Av::S("Ada".into())),
        ]);
        let keys = vec!["pk".to_owned(), "sk".to_owned()];
        let c = item_to_change("users", &keys, &item).unwrap();
        assert_eq!(c.collection, "users");
        assert_eq!(c.key, vec![json!("user#1"), json!(7)]);
        assert!(matches!(c.op, Op::Upsert { ref doc } if doc.len() == 3));

        let missing = item_to_change("users", &["id".to_owned()], &item);
        assert!(matches!(missing, Err(SourceError::Fatal(_))));
    }

    fn record(event: OperationType, new_image: bool) -> Record {
        let keys = HashMap::from([
            ("pk".to_owned(), Sv::S("a".into())),
            ("sk".to_owned(), Sv::N("1".into())),
        ]);
        let mut data = StreamRecord::builder()
            .set_keys(Some(keys.clone()))
            .sequence_number("000100");
        if new_image {
            let mut img = keys;
            img.insert("v".into(), Sv::Bool(true));
            data = data.set_new_image(Some(img));
        }
        Record::builder()
            .event_name(event)
            .dynamodb(data.build())
            .build()
    }

    #[test]
    fn stream_records_to_changes() {
        let keys = vec!["pk".to_owned(), "sk".to_owned()];
        for ev in [OperationType::Insert, OperationType::Modify] {
            let (c, seq) = record_to_change("t", &keys, &record(ev, true)).unwrap();
            assert_eq!(seq.as_deref(), Some("000100"));
            assert_eq!(c.key, vec![json!("a"), json!(1)]);
            assert_eq!(
                c.op,
                Op::Upsert {
                    doc: json!({"pk": "a", "sk": 1, "v": true})
                        .as_object()
                        .cloned()
                        .unwrap()
                }
            );
        }
        let (c, _) = record_to_change("t", &keys, &record(OperationType::Remove, false)).unwrap();
        assert_eq!(c.op, Op::Delete);
        assert_eq!(c.key, vec![json!("a"), json!(1)]);

        // KEYS_ONLY / OLD_IMAGE streams: no new image on a MODIFY.
        let err = record_to_change("t", &keys, &record(OperationType::Modify, false));
        assert!(matches!(err, Err(SourceError::Config(_))));
    }
}
