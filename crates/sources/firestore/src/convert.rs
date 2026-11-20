//! Firestore values → JSON.
//!
//! | Firestore | JSON |
//! |---|---|
//! | null, boolean, string | the same |
//! | integer (int64) | number |
//! | double | number (`NaN` and infinities: `null`) |
//! | timestamp | RFC 3339 string, UTC, microsecond precision (`2024-05-01T12:00:00.123456Z`) |
//! | geopoint | `{"lat": f64, "lng": f64}`, the shape of Meilisearch's `_geo` |
//! | reference | the document path relative to the database (`users/alice`) |
//! | bytes | standard base64 string |
//! | array, map | array, object (recursively) |
//!
//! Field references, functions and pipelines only appear in queries, never
//! in stored documents; they become `null`.

use base64::Engine as _;
use gcloud_sdk::google::firestore::v1::{Document, Value as FsValue, value::ValueType};
use serde_json::{Map, Number, Value};

/// The fields of a document as a JSON object.
pub fn document_fields(doc: &Document) -> Map<String, Value> {
    doc.fields
        .iter()
        .map(|(k, v)| (k.clone(), to_json(v)))
        .collect()
}

pub fn to_json(value: &FsValue) -> Value {
    let Some(v) = &value.value_type else {
        return Value::Null;
    };
    match v {
        ValueType::NullValue(_) => Value::Null,
        ValueType::BooleanValue(b) => Value::Bool(*b),
        ValueType::IntegerValue(i) => Value::Number((*i).into()),
        ValueType::DoubleValue(d) => Number::from_f64(*d).map_or(Value::Null, Value::Number),
        ValueType::TimestampValue(ts) => timestamp_to_json(ts.seconds, ts.nanos),
        ValueType::StringValue(s) => Value::String(s.clone()),
        ValueType::BytesValue(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        ValueType::ReferenceValue(r) => Value::String(relative_path(r).to_owned()),
        ValueType::GeoPointValue(p) => {
            let mut m = Map::new();
            m.insert(
                "lat".into(),
                Number::from_f64(p.latitude).map_or(Value::Null, Value::Number),
            );
            m.insert(
                "lng".into(),
                Number::from_f64(p.longitude).map_or(Value::Null, Value::Number),
            );
            Value::Object(m)
        }
        ValueType::ArrayValue(a) => Value::Array(a.values.iter().map(to_json).collect()),
        ValueType::MapValue(m) => Value::Object(
            m.fields
                .iter()
                .map(|(k, v)| (k.clone(), to_json(v)))
                .collect(),
        ),
        ValueType::FieldReferenceValue(_)
        | ValueType::VariableReferenceValue(_)
        | ValueType::FunctionValue(_)
        | ValueType::PipelineValue(_) => Value::Null,
    }
}

/// RFC 3339 in UTC with microseconds; out-of-range values (Firestore allows
/// years 1–9999, all representable) fall back to epoch seconds.
pub fn timestamp_to_json(seconds: i64, nanos: i32) -> Value {
    let nanos = u32::try_from(nanos.clamp(0, 999_999_999)).unwrap_or(0);
    match chrono::DateTime::from_timestamp(seconds, nanos) {
        Some(dt) => Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)),
        None => Value::Number(seconds.into()),
    }
}

/// `projects/p/databases/d/documents/users/alice` → `users/alice`. A name
/// of another shape is returned as is.
pub fn relative_path(name: &str) -> &str {
    match name.find("/documents/") {
        Some(i) => &name[i + "/documents/".len()..],
        None => name,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use gcloud_sdk::google::firestore::v1::{ArrayValue, MapValue};
    use gcloud_sdk::google::r#type::LatLng;
    use serde_json::json;
    use std::collections::HashMap;

    fn v(t: ValueType) -> FsValue {
        FsValue {
            value_type: Some(t),
        }
    }

    #[test]
    fn every_type_has_a_json_form() {
        let mut inner = HashMap::new();
        inner.insert("n".to_owned(), v(ValueType::IntegerValue(-3)));
        let mut fields = HashMap::new();
        fields.insert("null".to_owned(), v(ValueType::NullValue(0)));
        fields.insert("bool".to_owned(), v(ValueType::BooleanValue(true)));
        fields.insert(
            "int".to_owned(),
            v(ValueType::IntegerValue(9_007_199_254_740_993)),
        );
        fields.insert("double".to_owned(), v(ValueType::DoubleValue(1.5)));
        fields.insert("nan".to_owned(), v(ValueType::DoubleValue(f64::NAN)));
        fields.insert(
            "ts".to_owned(),
            v(ValueType::TimestampValue(
                gcloud_sdk::prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 123_456_000,
                },
            )),
        );
        fields.insert("s".to_owned(), v(ValueType::StringValue("héllo".into())));
        fields.insert("bytes".to_owned(), v(ValueType::BytesValue(vec![0, 1, 2])));
        fields.insert(
            "ref".to_owned(),
            v(ValueType::ReferenceValue(
                "projects/p/databases/(default)/documents/users/alice".into(),
            )),
        );
        fields.insert(
            "geo".to_owned(),
            v(ValueType::GeoPointValue(LatLng {
                latitude: 48.85,
                longitude: 2.35,
            })),
        );
        fields.insert(
            "arr".to_owned(),
            v(ValueType::ArrayValue(ArrayValue {
                values: vec![
                    v(ValueType::StringValue("a".into())),
                    v(ValueType::MapValue(MapValue {
                        fields: inner.clone(),
                    })),
                ],
            })),
        );
        fields.insert(
            "map".to_owned(),
            v(ValueType::MapValue(MapValue { fields: inner })),
        );
        fields.insert("unset".to_owned(), FsValue { value_type: None });
        let doc = Document {
            name: "projects/p/databases/(default)/documents/c/d".into(),
            fields,
            create_time: None,
            update_time: None,
        };
        assert_eq!(
            Value::Object(document_fields(&doc)),
            json!({
                "null": null,
                "bool": true,
                "int": 9_007_199_254_740_993_i64,
                "double": 1.5,
                "nan": null,
                "ts": "2023-11-14T22:13:20.123456Z",
                "s": "héllo",
                "bytes": "AAEC",
                "ref": "users/alice",
                "geo": {"lat": 48.85, "lng": 2.35},
                "arr": ["a", {"n": -3}],
                "map": {"n": -3},
                "unset": null,
            })
        );
    }

    #[test]
    fn paths() {
        assert_eq!(
            relative_path("projects/p/databases/(default)/documents/a/b/c/d"),
            "a/b/c/d"
        );
        assert_eq!(relative_path("weird"), "weird");
        assert_eq!(
            timestamp_to_json(0, 0),
            json!("1970-01-01T00:00:00.000000Z")
        );
    }
}
