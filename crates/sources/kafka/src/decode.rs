//! Record → change conversion. Pure functions, no I/O.
//!
//! Error messages describe what is wrong with a record, never its content:
//! payloads may hold personal data and must not reach the logs.

use std::fmt;

use base64::Engine as _;
use meili_connect_core::{Change, Document, Op};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::Value;

use crate::config::Format;

/// What one record turns into.
#[derive(Debug, Clone, PartialEq)]
pub enum Decoded {
    Change(Change),
    /// Debezium `op: "t"`.
    Truncate,
    /// A record that carries no row change (heartbeat, schema change,
    /// transaction marker, Debezium `op: "m"` message). Not an error.
    Ignored,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn err<T>(msg: &str) -> Result<T, DecodeError> {
    Err(DecodeError(msg.to_owned()))
}

/// The decoding rules of one sync.
#[derive(Debug, Clone)]
pub struct Decoder {
    pub format: Format,
    pub key_fields: Vec<String>,
}

impl Decoder {
    pub fn decode(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Decoded, DecodeError> {
        let value = match value {
            None => None,
            Some(bytes) => match serde_json::from_slice::<Value>(bytes) {
                Ok(Value::Null) => None,
                Ok(v) => Some(v),
                Err(_) => return err("value is not valid JSON"),
            },
        };
        let Some(value) = value else {
            // Tombstone.
            let key = self.key(key, None)?;
            return Ok(change(topic, key, Op::Delete));
        };
        match self.format {
            Format::Json => match value {
                Value::Object(doc) => {
                    let key = self.key(key, Some(&doc))?;
                    Ok(change(topic, key, Op::Upsert { doc }))
                }
                _ => err("value is not a JSON object"),
            },
            Format::Debezium => self.debezium(topic, key, value),
        }
    }

    fn debezium(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Value,
    ) -> Result<Decoded, DecodeError> {
        let Value::Object(obj) = unwrap_schema(value) else {
            return err("debezium value is not a JSON object");
        };
        if is_non_row_record(&obj) {
            return Ok(Decoded::Ignored);
        }
        if is_envelope(&obj) {
            let op = obj.get("op").and_then(Value::as_str).unwrap_or_default();
            let row = |field: &str| match obj.get(field) {
                Some(Value::Object(row)) => Some(row.clone()),
                _ => None,
            };
            return match op {
                "c" | "r" | "u" => {
                    let Some(doc) = row("after") else {
                        return err("debezium create/update without an `after` row");
                    };
                    let key = self.key(key, Some(&doc))?;
                    Ok(change(topic, key, Op::Upsert { doc }))
                }
                "d" => {
                    let before = row("before");
                    let key = self.key(key, before.as_ref())?;
                    Ok(change(topic, key, Op::Delete))
                }
                "t" => Ok(Decoded::Truncate),
                "m" => Ok(Decoded::Ignored),
                _ => err("debezium envelope with an unknown `op`"),
            };
        }
        // Flattened row (ExtractNewRecordState), possibly rewritten deletes.
        let mut doc = obj;
        let deleted = match doc.remove("__deleted") {
            Some(Value::Bool(b)) => b,
            Some(Value::String(s)) => s.eq_ignore_ascii_case("true"),
            _ => false,
        } || doc.get("__op").and_then(Value::as_str) == Some("d");
        let key = self.key(key, Some(&doc))?;
        if deleted {
            Ok(change(topic, key, Op::Delete))
        } else {
            Ok(change(topic, key, Op::Upsert { doc }))
        }
    }

    /// The document key. With `key_fields`, the named fields of the row
    /// (or, without a row, of the record key parsed as a JSON object).
    /// Otherwise the record key: JSON object → its values in field order,
    /// Debezium key envelope → its payload values, other JSON → itself,
    /// non-JSON UTF-8 → the string, anything else → base64.
    pub fn key(
        &self,
        record_key: Option<&[u8]>,
        row: Option<&Document>,
    ) -> Result<Vec<Value>, DecodeError> {
        if !self.key_fields.is_empty() {
            return match row {
                Some(row) => pick(&self.key_fields, |f| row.get(f).cloned()).ok_or_else(|| {
                    DecodeError("a key field is missing or null in the value".into())
                }),
                None => {
                    let Some(KeyJson::Object(fields)) = record_key.and_then(parse_key_json) else {
                        return err("record without a value and without a JSON object key: \
                             cannot resolve key_fields");
                    };
                    let fields = unwrap_key_envelope(fields);
                    pick(&self.key_fields, |f| {
                        fields
                            .iter()
                            .find(|(k, _)| k == f)
                            .map(|(_, v)| v.clone().into_value())
                    })
                    .ok_or_else(|| {
                        DecodeError("a key field is missing or null in the record key".into())
                    })
                }
            };
        }
        record_key_values(record_key)
    }
}

fn change(topic: &str, key: Vec<Value>, op: Op) -> Decoded {
    Decoded::Change(Change {
        collection: topic.to_owned(),
        key,
        op,
    })
}

fn pick(fields: &[String], get: impl Fn(&str) -> Option<Value>) -> Option<Vec<Value>> {
    fields
        .iter()
        .map(|f| get(f).filter(|v| !v.is_null()))
        .collect()
}

/// `{"schema": ..., "payload": X}` → `X` (JSON converter with schemas).
fn unwrap_schema(value: Value) -> Value {
    match value {
        Value::Object(mut obj)
            if obj.len() == 2 && obj.contains_key("schema") && obj.contains_key("payload") =>
        {
            obj.remove("payload").unwrap_or(Value::Null)
        }
        other => other,
    }
}

fn is_envelope(obj: &Document) -> bool {
    matches!(obj.get("op"), Some(Value::String(_)))
        && (obj.contains_key("before") || obj.contains_key("after") || obj.contains_key("source"))
}

/// Heartbeats (`{"ts_ms": ...}`), schema-change events (`ddl`,
/// `tableChanges`) and transaction metadata (`status: BEGIN|END`).
fn is_non_row_record(obj: &Document) -> bool {
    if obj.len() == 1 && obj.contains_key("ts_ms") {
        return true;
    }
    if obj.contains_key("ddl") || obj.contains_key("tableChanges") {
        return true;
    }
    let status = obj.get("status").and_then(Value::as_str);
    matches!(status, Some("BEGIN" | "END"))
        && (obj.contains_key("event_count") || obj.contains_key("data_collections"))
}

fn record_key_values(record_key: Option<&[u8]>) -> Result<Vec<Value>, DecodeError> {
    let Some(bytes) = record_key.filter(|b| !b.is_empty()) else {
        return err("record has no key; set key_fields to key documents from the value");
    };
    let values = match parse_key_json(bytes) {
        Some(KeyJson::Object(fields)) => unwrap_key_envelope(fields)
            .into_iter()
            .map(|(_, v)| v.into_value())
            .collect(),
        Some(KeyJson::Other(Value::Null)) => {
            return err("record key is JSON null; set key_fields");
        }
        Some(KeyJson::Other(Value::Array(items))) if !items.is_empty() => items,
        Some(KeyJson::Other(v)) => vec![v],
        None => match std::str::from_utf8(bytes) {
            Ok(s) => vec![Value::String(s.to_owned())],
            Err(_) => vec![Value::String(
                base64::engine::general_purpose::STANDARD.encode(bytes),
            )],
        },
    };
    let values: Vec<Value> = values;
    if values.is_empty() || values.iter().all(Value::is_null) {
        return err("record key holds no value; set key_fields");
    }
    Ok(values)
}

/// A Debezium key with schemas is `{"schema": ..., "payload": {...}}`.
fn unwrap_key_envelope(fields: Vec<(String, KeyJson)>) -> Vec<(String, KeyJson)> {
    let is_envelope = fields.len() == 2
        && fields.iter().any(|(k, _)| k == "schema")
        && fields
            .iter()
            .any(|(k, v)| k == "payload" && matches!(v, KeyJson::Object(_)));
    if !is_envelope {
        return fields;
    }
    fields
        .into_iter()
        .find_map(|(k, v)| match (k.as_str(), v) {
            ("payload", KeyJson::Object(inner)) => Some(inner),
            _ => None,
        })
        .unwrap_or_default()
}

fn parse_key_json(bytes: &[u8]) -> Option<KeyJson> {
    serde_json::from_slice(bytes).ok()
}

/// JSON that keeps the field order of objects. `serde_json::Map` sorts
/// keys unless the workspace-wide `preserve_order` feature is on, and a
/// composite key must follow the producer's order.
#[derive(Debug, Clone, PartialEq)]
enum KeyJson {
    Object(Vec<(String, KeyJson)>),
    Other(Value),
}

impl KeyJson {
    fn into_value(self) -> Value {
        match self {
            KeyJson::Other(v) => v,
            KeyJson::Object(fields) => Value::Object(
                fields
                    .into_iter()
                    .map(|(k, v)| (k, v.into_value()))
                    .collect(),
            ),
        }
    }
}

impl<'de> Deserialize<'de> for KeyJson {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = KeyJson;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_bool<E>(self, v: bool) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::Bool(v)))
            }
            fn visit_i64<E>(self, v: i64) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::from(v)))
            }
            fn visit_u64<E>(self, v: u64) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::from(v)))
            }
            fn visit_f64<E>(self, v: f64) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::from(v)))
            }
            fn visit_str<E>(self, v: &str) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::String(v.to_owned())))
            }
            fn visit_string<E>(self, v: String) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::String(v)))
            }
            fn visit_unit<E>(self) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::Null))
            }
            fn visit_none<E>(self) -> Result<KeyJson, E> {
                Ok(KeyJson::Other(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<KeyJson, A::Error> {
                let mut items = Vec::new();
                while let Some(item) = seq.next_element::<KeyJson>()? {
                    items.push(item.into_value());
                }
                Ok(KeyJson::Other(Value::Array(items)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<KeyJson, A::Error> {
                let mut fields = Vec::new();
                while let Some((k, v)) = map.next_entry::<String, KeyJson>()? {
                    fields.push((k, v));
                }
                Ok(KeyJson::Object(fields))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn json_decoder() -> Decoder {
        Decoder {
            format: Format::Json,
            key_fields: vec![],
        }
    }
    fn dbz() -> Decoder {
        Decoder {
            format: Format::Debezium,
            key_fields: vec![],
        }
    }
    fn bytes(v: Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }
    fn upsert(key: Vec<Value>, doc: Value) -> Decoded {
        let Value::Object(doc) = doc else { panic!() };
        Decoded::Change(Change {
            collection: "t".into(),
            key,
            op: Op::Upsert { doc },
        })
    }
    fn delete(key: Vec<Value>) -> Decoded {
        Decoded::Change(Change {
            collection: "t".into(),
            key,
            op: Op::Delete,
        })
    }

    // ---- keys ---------------------------------------------------------

    #[test]
    fn key_json_object_keeps_field_order() {
        let d = json_decoder();
        assert_eq!(
            d.key(Some(br#"{"z": 1, "a": "x"}"#), None).unwrap(),
            vec![json!(1), json!("x")]
        );
    }

    #[test]
    fn key_debezium_envelope_uses_payload() {
        let d = json_decoder();
        let k = br#"{"schema": {"type": "struct"}, "payload": {"tenant": "t1", "id": 7}}"#;
        assert_eq!(d.key(Some(k), None).unwrap(), vec![json!("t1"), json!(7)]);
    }

    #[test]
    fn key_scalars_strings_and_binary() {
        let d = json_decoder();
        assert_eq!(d.key(Some(b"42"), None).unwrap(), vec![json!(42)]);
        assert_eq!(d.key(Some(br#""abc""#), None).unwrap(), vec![json!("abc")]);
        assert_eq!(d.key(Some(b"user-1"), None).unwrap(), vec![json!("user-1")]);
        assert_eq!(
            d.key(Some(b"[1, 2]"), None).unwrap(),
            vec![json!(1), json!(2)]
        );
        assert_eq!(
            d.key(Some(&[0xff, 0x00, 0x10]), None).unwrap(),
            vec![json!("/wAQ")]
        );
    }

    #[test]
    fn key_missing_or_null_is_an_error() {
        let d = json_decoder();
        assert!(d.key(None, None).is_err());
        assert!(d.key(Some(b""), None).is_err());
        assert!(d.key(Some(b"null"), None).is_err());
        assert!(d.key(Some(br#"{"id": null}"#), None).is_err());
    }

    #[test]
    fn key_fields_from_value_then_from_record_key() {
        let d = Decoder {
            format: Format::Json,
            key_fields: vec!["tenant".into(), "id".into()],
        };
        let Value::Object(row) = json!({"id": 3, "tenant": "a", "x": 1}) else {
            panic!()
        };
        assert_eq!(
            d.key(Some(b"ignored"), Some(&row)).unwrap(),
            vec![json!("a"), json!(3)]
        );
        // Tombstone: key fields resolved by name from the record key.
        assert_eq!(
            d.key(Some(br#"{"id": 3, "tenant": "a"}"#), None).unwrap(),
            vec![json!("a"), json!(3)]
        );
        assert!(d.key(Some(b"plain"), None).is_err());
        let Value::Object(partial) = json!({"id": 3}) else {
            panic!()
        };
        assert!(d.key(None, Some(&partial)).is_err());
    }

    // ---- json format --------------------------------------------------

    #[test]
    fn json_value_is_the_document() {
        let got = json_decoder()
            .decode("t", Some(b"1"), Some(&bytes(json!({"id": 1, "name": "a"}))))
            .unwrap();
        assert_eq!(got, upsert(vec![json!(1)], json!({"id": 1, "name": "a"})));
    }

    #[test]
    fn json_tombstones_delete() {
        let d = json_decoder();
        assert_eq!(
            d.decode("t", Some(b"1"), None).unwrap(),
            delete(vec![json!(1)])
        );
        assert_eq!(
            d.decode("t", Some(b"1"), Some(b"null")).unwrap(),
            delete(vec![json!(1)])
        );
    }

    #[test]
    fn json_bad_values_are_errors() {
        let d = json_decoder();
        assert!(d.decode("t", Some(b"1"), Some(b"{not json")).is_err());
        assert!(d.decode("t", Some(b"1"), Some(b"[1]")).is_err());
        assert!(d.decode("t", None, Some(b"{}")).is_err());
    }

    #[test]
    fn decode_errors_never_echo_the_payload() {
        let d = json_decoder();
        let e = d
            .decode("t", Some(b"1"), Some(b"secret-ssn-123"))
            .unwrap_err();
        assert!(!e.0.contains("secret"));
    }

    // ---- debezium -----------------------------------------------------

    fn envelope(op: &str, before: Value, after: Value) -> Value {
        json!({"before": before, "after": after, "op": op,
               "source": {"connector": "postgresql", "table": "users"}, "ts_ms": 1})
    }

    #[test]
    fn debezium_create_read_update_upsert_after() {
        let key = br#"{"id": 1}"#;
        for op in ["c", "r", "u"] {
            let v = envelope(op, json!(null), json!({"id": 1, "n": op}));
            assert_eq!(
                dbz().decode("t", Some(key), Some(&bytes(v))).unwrap(),
                upsert(vec![json!(1)], json!({"id": 1, "n": op}))
            );
        }
    }

    #[test]
    fn debezium_with_schema_wrapper() {
        let key = json!({"schema": {"type": "struct"}, "payload": {"id": 9}});
        let v = json!({"schema": {"type": "struct"},
                       "payload": envelope("u", json!({"id": 9}), json!({"id": 9, "n": "b"}))});
        assert_eq!(
            dbz()
                .decode("t", Some(&bytes(key)), Some(&bytes(v)))
                .unwrap(),
            upsert(vec![json!(9)], json!({"id": 9, "n": "b"}))
        );
    }

    #[test]
    fn debezium_delete_keys_from_record_key_or_before() {
        let v = envelope("d", json!({"id": 4, "n": "x"}), json!(null));
        assert_eq!(
            dbz()
                .decode("t", Some(br#"{"id": 4}"#), Some(&bytes(v.clone())))
                .unwrap(),
            delete(vec![json!(4)])
        );
        let with_fields = Decoder {
            format: Format::Debezium,
            key_fields: vec!["id".into()],
        };
        assert_eq!(
            with_fields.decode("t", None, Some(&bytes(v))).unwrap(),
            delete(vec![json!(4)])
        );
    }

    #[test]
    fn debezium_truncate_and_ignored_records() {
        let t = envelope("t", json!(null), json!(null));
        assert_eq!(
            dbz().decode("t", None, Some(&bytes(t))).unwrap(),
            Decoded::Truncate
        );
        for v in [
            envelope("m", json!(null), json!(null)),
            json!({"ts_ms": 1700000000000_i64}),
            json!({"schema": {}, "payload": {"ts_ms": 1}}),
            json!({"source": {}, "databaseName": "db", "ddl": "ALTER TABLE x", "tableChanges": []}),
            json!({"status": "BEGIN", "id": "571:53195829", "event_count": null, "data_collections": null}),
        ] {
            assert_eq!(
                dbz().decode("t", None, Some(&bytes(v))).unwrap(),
                Decoded::Ignored
            );
        }
    }

    #[test]
    fn debezium_tombstone_deletes() {
        assert_eq!(
            dbz().decode("t", Some(br#"{"id": 4}"#), None).unwrap(),
            delete(vec![json!(4)])
        );
    }

    #[test]
    fn debezium_flattened_rows() {
        let key = br#"{"id": 2}"#;
        let v = json!({"id": 2, "n": "a", "__deleted": "false"});
        assert_eq!(
            dbz().decode("t", Some(key), Some(&bytes(v))).unwrap(),
            upsert(vec![json!(2)], json!({"id": 2, "n": "a"}))
        );
        let v = json!({"id": 2, "n": "a"});
        assert_eq!(
            dbz().decode("t", Some(key), Some(&bytes(v))).unwrap(),
            upsert(vec![json!(2)], json!({"id": 2, "n": "a"}))
        );
        for deleted in [json!("true"), json!(true)] {
            let v = json!({"id": 2, "__deleted": deleted});
            assert_eq!(
                dbz().decode("t", Some(key), Some(&bytes(v))).unwrap(),
                delete(vec![json!(2)])
            );
        }
        // Flattened with the schema wrapper.
        let v = json!({"schema": {}, "payload": {"id": 2, "__deleted": "true"}});
        assert_eq!(
            dbz().decode("t", Some(key), Some(&bytes(v))).unwrap(),
            delete(vec![json!(2)])
        );
    }

    #[test]
    fn debezium_bad_records_are_errors() {
        assert!(
            dbz()
                .decode(
                    "t",
                    Some(b"1"),
                    Some(&bytes(envelope("u", json!(null), json!(null))))
                )
                .is_err()
        );
        assert!(
            dbz()
                .decode(
                    "t",
                    Some(b"1"),
                    Some(&bytes(envelope("x", json!(null), json!({}))))
                )
                .is_err()
        );
        assert!(dbz().decode("t", Some(b"1"), Some(b"42")).is_err());
    }
}
