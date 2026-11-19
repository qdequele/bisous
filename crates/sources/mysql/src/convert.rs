//! MySQL values → JSON.
//!
//! The same column can reach us in two encodings: the binary protocol of a
//! snapshot `SELECT`, and the row image of a binlog event. They differ for
//! `TIMESTAMP` (binlog: epoch seconds), `ENUM` (binlog: 1-based index), `SET`
//! (binlog: bitmask), `YEAR` (binlog: offset from 1900), `JSON` (binlog:
//! binary JSONB) and character sets (binlog: raw bytes in the column's
//! charset; snapshot: converted to utf8mb4 by the server).

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use mysql_async::Value;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::consts::ColumnType;
use serde_json::{Number, Value as Json};

use crate::schema::ColumnMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Snapshot,
    Binlog,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConvertError {
    #[error(
        "column `{0}` holds a partial JSON update; set binlog_row_value_options='' on the server"
    )]
    PartialJson(String),
    #[error("column `{column}`: {reason}")]
    Invalid { column: String, reason: String },
}

/// Converts one binlog cell.
pub fn binlog_to_json(col: &ColumnMeta, v: BinlogValue<'_>) -> Result<Json, ConvertError> {
    match v {
        BinlogValue::Value(v) => value_to_json(col, v, Origin::Binlog),
        BinlogValue::Jsonb(j) => {
            let dom = j.parse().map_err(|e| ConvertError::Invalid {
                column: col.name.clone(),
                reason: format!("bad binary JSON: {e}"),
            })?;
            Ok(Json::from(dom))
        }
        BinlogValue::JsonDiff(_) => Err(ConvertError::PartialJson(col.name.clone())),
    }
}

/// Converts one cell of a snapshot row (binary protocol) or, with
/// `Origin::Binlog`, of a binlog row image.
pub fn value_to_json(col: &ColumnMeta, v: Value, origin: Origin) -> Result<Json, ConvertError> {
    let dt = col.data_type.as_str();
    Ok(match v {
        Value::NULL => Json::Null,
        Value::Int(i) => int_to_json(col, i, origin),
        Value::UInt(u) => match dt {
            "enum" => enum_member(col, u),
            "year" if u == 0 => Json::Null,
            _ => Json::from(u),
        },
        Value::Float(f) => {
            // Through the shortest decimal form, so 0.1f32 stays 0.1.
            let d: f64 = f.to_string().parse().unwrap_or(f64::from(f));
            float(d)
        }
        Value::Double(d) => float(d),
        Value::Date(y, mo, d, h, mi, s, us) => {
            if mo == 0 || d == 0 {
                Json::Null
            } else if dt == "date" {
                Json::String(format!("{y:04}-{mo:02}-{d:02}"))
            } else {
                let mut out = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}");
                push_micros(&mut out, us);
                if dt == "timestamp" {
                    // The session runs in UTC.
                    out.push('Z');
                }
                Json::String(out)
            }
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let hours = u64::from(days) * 24 + u64::from(h);
            let mut out = format!("{}{hours:02}:{mi:02}:{s:02}", if neg { "-" } else { "" });
            push_micros(&mut out, us);
            Json::String(out)
        }
        Value::Bytes(b) => bytes_to_json(col, b, origin)?,
    })
}

fn float(d: f64) -> Json {
    Number::from_f64(d).map(Json::Number).unwrap_or(Json::Null)
}

fn push_micros(out: &mut String, us: u32) {
    if us > 0 {
        out.push_str(&format!(".{us:06}"));
    }
}

fn int_width(data_type: &str) -> u32 {
    match data_type {
        "tinyint" => 8,
        "smallint" => 16,
        "mediumint" => 24,
        "int" | "integer" => 32,
        _ => 64,
    }
}

fn int_to_json(col: &ColumnMeta, i: i64, origin: Origin) -> Json {
    match col.data_type.as_str() {
        "enum" => enum_member(col, i.max(0) as u64),
        "year" if i == 0 => Json::Null,
        // Pre-5.6 TIMESTAMP in a binlog: epoch seconds.
        "timestamp" if origin == Origin::Binlog => epoch_to_json(i, 0),
        dt if col.unsigned && i < 0 => {
            // Read as signed (no signedness metadata): reinterpret.
            let width = int_width(dt);
            let u = if width == 64 {
                i as u64
            } else {
                (i + (1_i64 << width)) as u64
            };
            Json::from(u)
        }
        _ => Json::from(i),
    }
}

fn enum_member(col: &ColumnMeta, idx: u64) -> Json {
    if col.members.is_empty() {
        return Json::from(idx);
    }
    // 0 is the "invalid value" member, shown by MySQL as ''.
    match idx.checked_sub(1).and_then(|i| col.members.get(i as usize)) {
        Some(m) => Json::String(m.clone()),
        None => Json::String(String::new()),
    }
}

fn epoch_to_json(secs: i64, micros: u32) -> Json {
    if secs == 0 && micros == 0 {
        // The zero TIMESTAMP.
        return Json::Null;
    }
    match chrono::DateTime::from_timestamp(secs, micros * 1000) {
        Some(t) => {
            let mut out = t.format("%Y-%m-%dT%H:%M:%S").to_string();
            push_micros(&mut out, micros);
            out.push('Z');
            Json::String(out)
        }
        None => Json::Null,
    }
}

fn is_binary_type(dt: &str) -> bool {
    matches!(
        dt,
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
    )
}

fn is_geometry_type(dt: &str) -> bool {
    matches!(
        dt,
        "geometry"
            | "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
            | "geomcollection"
    )
}

fn bytes_to_json(col: &ColumnMeta, b: Vec<u8>, origin: Origin) -> Result<Json, ConvertError> {
    let dt = col.data_type.as_str();
    let invalid = |reason: String| ConvertError::Invalid {
        column: col.name.clone(),
        reason,
    };
    Ok(match dt {
        "decimal" | "numeric" => decimal_to_json(&String::from_utf8_lossy(&b)),
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            let s = String::from_utf8_lossy(&b);
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                Json::from(i)
            } else if let Ok(u) = s.parse::<u64>() {
                Json::from(u)
            } else {
                return Err(invalid(format!("not an integer: {s}")));
            }
        }
        "float" | "double" | "real" => {
            let s = String::from_utf8_lossy(&b);
            s.trim()
                .parse::<f64>()
                .map(float)
                .map_err(|_| invalid("not a number".into()))?
        }
        "bit" => {
            if b.len() > 8 {
                return Err(invalid("BIT wider than 64 bits".into()));
            }
            Json::from(b.iter().fold(0_u64, |acc, &x| (acc << 8) | u64::from(x)))
        }
        "json" => {
            if b.is_empty() {
                Json::Null
            } else {
                serde_json::from_slice(&b).map_err(|e| invalid(format!("bad JSON: {e}")))?
            }
        }
        "set" if origin == Origin::Binlog => {
            let mut members = Vec::new();
            for (i, m) in col.members.iter().enumerate() {
                let byte = b.get(i / 8).copied().unwrap_or(0);
                if byte & (1 << (i % 8)) != 0 {
                    members.push(m.as_str());
                }
            }
            Json::String(members.join(","))
        }
        "timestamp" if origin == Origin::Binlog => {
            // TIMESTAMP2: "secs" or "secs.micros".
            let s = String::from_utf8_lossy(&b);
            let (secs, micros) = match s.split_once('.') {
                Some((a, f)) => (a.parse::<i64>(), f.parse::<u32>()),
                None => (s.parse::<i64>(), Ok(0)),
            };
            match (secs, micros) {
                (Ok(secs), Ok(micros)) => epoch_to_json(secs, micros),
                _ => return Err(invalid(format!("bad binlog timestamp: {s}"))),
            }
        }
        "year" => {
            let s = String::from_utf8_lossy(&b);
            match s.trim().parse::<u32>() {
                // Binlog stores YEAR as an offset from 1900; 0 is the zero year.
                Ok(1900) if origin == Origin::Binlog => Json::Null,
                Ok(0) => Json::Null,
                Ok(y) => Json::from(y),
                Err(_) => return Err(invalid(format!("bad year: {s}"))),
            }
        }
        "date" | "datetime" | "timestamp" | "time" => {
            // Text-protocol fallback: already a string.
            let s = String::from_utf8_lossy(&b).into_owned();
            if s.starts_with("0000-00-00") {
                Json::Null
            } else {
                Json::String(s)
            }
        }
        "vector" => {
            let (chunks, rest) = b.as_chunks::<4>();
            if !rest.is_empty() {
                return Err(invalid("VECTOR length is not a multiple of 4".into()));
            }
            Json::Array(
                chunks
                    .iter()
                    .map(|c| {
                        let f = f32::from_le_bytes(*c);
                        let d: f64 = f.to_string().parse().unwrap_or(f64::from(f));
                        float(d)
                    })
                    .collect(),
            )
        }
        dt if is_binary_type(dt) || is_geometry_type(dt) => Json::String(BASE64.encode(&b)),
        _ => Json::String(decode_text(col, b, origin)),
    })
}

/// Character columns. A snapshot is already utf8mb4 (`SET NAMES`); binlog
/// images carry the column's own charset.
fn decode_text(col: &ColumnMeta, b: Vec<u8>, origin: Origin) -> String {
    if origin == Origin::Binlog && col.charset.as_deref() == Some("latin1") {
        // MySQL's latin1 is cp1252; ISO-8859-1 differs only on 0x80–0x9F.
        return b.iter().map(|&x| char::from(x)).collect();
    }
    match String::from_utf8(b) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

/// `DECIMAL` text → a JSON number when the number represents it exactly,
/// else the string itself.
pub fn decimal_to_json(s: &str) -> Json {
    let s = s.trim();
    let canonical = canonical_decimal(s);
    if !canonical.contains('.') {
        if let Ok(i) = canonical.parse::<i64>() {
            return Json::from(i);
        }
        if let Ok(u) = canonical.parse::<u64>() {
            return Json::from(u);
        }
    }
    if let Ok(f) = s.parse::<f64>()
        && f.is_finite()
        && canonical_decimal(&f.to_string()) == canonical
        && let Some(n) = Number::from_f64(f)
    {
        return Json::Number(n);
    }
    Json::String(s.to_owned())
}

/// Strips a leading `+`, leading integer zeros, trailing fraction zeros and
/// a negative zero.
fn canonical_decimal(s: &str) -> String {
    let (neg, body) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let int = int.trim_start_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let frac = frac.trim_end_matches('0');
    let mut out = String::new();
    if neg && !(int == "0" && frac.is_empty()) {
        out.push('-');
    }
    out.push_str(int);
    if !frac.is_empty() {
        out.push('.');
        out.push_str(frac);
    }
    out
}

/// Fallback type name when a binlog column is not in the cached schema.
pub fn data_type_from_binlog(ct: ColumnType) -> &'static str {
    use ColumnType::*;
    match ct {
        MYSQL_TYPE_TINY => "tinyint",
        MYSQL_TYPE_SHORT => "smallint",
        MYSQL_TYPE_INT24 => "mediumint",
        MYSQL_TYPE_LONG => "int",
        MYSQL_TYPE_LONGLONG => "bigint",
        MYSQL_TYPE_FLOAT => "float",
        MYSQL_TYPE_DOUBLE => "double",
        MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => "decimal",
        MYSQL_TYPE_TIMESTAMP | MYSQL_TYPE_TIMESTAMP2 => "timestamp",
        MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => "date",
        MYSQL_TYPE_TIME | MYSQL_TYPE_TIME2 => "time",
        MYSQL_TYPE_DATETIME | MYSQL_TYPE_DATETIME2 => "datetime",
        MYSQL_TYPE_YEAR => "year",
        MYSQL_TYPE_BIT => "bit",
        MYSQL_TYPE_JSON => "json",
        MYSQL_TYPE_ENUM => "enum",
        MYSQL_TYPE_SET => "set",
        MYSQL_TYPE_GEOMETRY => "geometry",
        MYSQL_TYPE_VECTOR => "vector",
        MYSQL_TYPE_TINY_BLOB | MYSQL_TYPE_MEDIUM_BLOB | MYSQL_TYPE_LONG_BLOB | MYSQL_TYPE_BLOB => {
            "blob"
        }
        _ => "varchar",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(dt: &str, ct: &str) -> ColumnMeta {
        ColumnMeta::new("c", dt, ct, Some("utf8mb4"), true)
    }

    fn snap(c: &ColumnMeta, v: Value) -> Json {
        value_to_json(c, v, Origin::Snapshot).unwrap()
    }

    fn bin(c: &ColumnMeta, v: Value) -> Json {
        value_to_json(c, v, Origin::Binlog).unwrap()
    }

    #[test]
    fn integers_signed_and_unsigned() {
        let i = col("int", "int");
        assert_eq!(snap(&i, Value::Int(-5)), json!(-5));
        let bu = col("bigint", "bigint unsigned");
        assert_eq!(snap(&bu, Value::UInt(u64::MAX)), json!(u64::MAX));
        // Binlog without signedness metadata gives the signed reading.
        assert_eq!(bin(&bu, Value::Int(-1)), json!(u64::MAX));
        let tu = col("tinyint", "tinyint unsigned");
        assert_eq!(bin(&tu, Value::Int(-1)), json!(255));
        let iu = col("int", "int unsigned");
        assert_eq!(bin(&iu, Value::Int(-2)), json!(4_294_967_294_u64));
        let mu = col("mediumint", "mediumint unsigned");
        assert_eq!(bin(&mu, Value::Int(-1)), json!(16_777_215));
        assert_eq!(bin(&bu, Value::UInt(1 << 63)), json!(1_u64 << 63));
        // u64 beyond i64 stays a number.
        assert!(bin(&bu, Value::UInt(u64::MAX)).is_u64());
    }

    #[test]
    fn decimals() {
        assert_eq!(decimal_to_json("12.50"), json!(12.5));
        assert_eq!(decimal_to_json("100"), json!(100));
        assert_eq!(decimal_to_json("100.000"), json!(100));
        assert_eq!(decimal_to_json("-0.001"), json!(-0.001));
        assert_eq!(decimal_to_json("0.00"), json!(0));
        assert_eq!(decimal_to_json("-0.00"), json!(0));
        assert_eq!(
            decimal_to_json("12345678901234567890.123456789"),
            json!("12345678901234567890.123456789")
        );
        assert_eq!(
            decimal_to_json("99999999999999999999999999"),
            json!("99999999999999999999999999")
        );
        assert_eq!(
            decimal_to_json("18446744073709551615"),
            json!(18_446_744_073_709_551_615_u64)
        );
        let d = col("decimal", "decimal(10,2)");
        assert_eq!(snap(&d, Value::Bytes(b"3.25".to_vec())), json!(3.25));
    }

    #[test]
    fn floats() {
        let f = col("float", "float");
        assert_eq!(snap(&f, Value::Float(0.1)), json!(0.1));
        let d = col("double", "double");
        assert_eq!(snap(&d, Value::Double(1.5e300)), json!(1.5e300));
    }

    #[test]
    fn bits_and_binaries() {
        let b = col("bit", "bit(10)");
        assert_eq!(snap(&b, Value::Bytes(vec![0x02, 0x01])), json!(513));
        let blob = col("blob", "blob");
        assert_eq!(snap(&blob, Value::Bytes(vec![0, 255, 1])), json!("AP8B"));
        let g = col("point", "point");
        assert_eq!(snap(&g, Value::Bytes(vec![1, 2, 3])), json!("AQID"));
        let vb = col("varbinary", "varbinary(16)");
        assert_eq!(bin(&vb, Value::Bytes(b"ab".to_vec())), json!("YWI="));
    }

    #[test]
    fn strings_and_charsets() {
        let s = col("varchar", "varchar(10)");
        assert_eq!(
            snap(&s, Value::Bytes("héllo".as_bytes().to_vec())),
            json!("héllo")
        );
        let latin = ColumnMeta::new("l", "varchar", "varchar(10)", Some("latin1"), true);
        assert_eq!(bin(&latin, Value::Bytes(vec![0x68, 0xE9])), json!("hé"));
        // Snapshot rows are converted to utf8mb4 by the server.
        assert_eq!(
            snap(&latin, Value::Bytes("hé".as_bytes().to_vec())),
            json!("hé")
        );
    }

    #[test]
    fn json_text_and_binary() {
        let j = col("json", "json");
        assert_eq!(
            snap(&j, Value::Bytes(br#"{"a":[1,"x"]}"#.to_vec())),
            json!({"a": [1, "x"]})
        );
        assert!(matches!(
            binlog_to_json(&j, BinlogValue::JsonDiff(vec![])),
            Err(ConvertError::PartialJson(_))
        ));
    }

    #[test]
    fn dates_and_times() {
        let d = col("date", "date");
        assert_eq!(
            snap(&d, Value::Date(2024, 2, 29, 0, 0, 0, 0)),
            json!("2024-02-29")
        );
        assert_eq!(snap(&d, Value::Date(0, 0, 0, 0, 0, 0, 0)), json!(null));
        let dt = col("datetime", "datetime(6)");
        assert_eq!(
            snap(&dt, Value::Date(2024, 1, 2, 3, 4, 5, 60)),
            json!("2024-01-02T03:04:05.000060")
        );
        assert_eq!(
            bin(&dt, Value::Date(2024, 1, 2, 3, 4, 5, 0)),
            json!("2024-01-02T03:04:05")
        );
        let ts = col("timestamp", "timestamp");
        assert_eq!(
            snap(&ts, Value::Date(2024, 1, 2, 3, 4, 5, 0)),
            json!("2024-01-02T03:04:05Z")
        );
        assert_eq!(
            bin(&ts, Value::Bytes(b"1704164645".to_vec())),
            json!("2024-01-02T03:04:05Z")
        );
        assert_eq!(
            bin(&ts, Value::Bytes(b"1704164645.000123".to_vec())),
            json!("2024-01-02T03:04:05.000123Z")
        );
        assert_eq!(bin(&ts, Value::Bytes(b"0".to_vec())), json!(null));
        let t = col("time", "time");
        assert_eq!(
            snap(&t, Value::Time(true, 1, 2, 3, 4, 0)),
            json!("-26:03:04")
        );
        assert_eq!(
            snap(&t, Value::Time(false, 0, 10, 0, 0, 5)),
            json!("10:00:00.000005")
        );
        let y = col("year", "year");
        assert_eq!(snap(&y, Value::Int(2024)), json!(2024));
        assert_eq!(snap(&y, Value::UInt(0)), json!(null));
        assert_eq!(bin(&y, Value::Bytes(b"2024".to_vec())), json!(2024));
        assert_eq!(bin(&y, Value::Bytes(b"1900".to_vec())), json!(null));
    }

    #[test]
    fn enums_and_sets() {
        let e = col("enum", "enum('small','medium','large')");
        assert_eq!(bin(&e, Value::Int(2)), json!("medium"));
        assert_eq!(bin(&e, Value::Int(0)), json!(""));
        assert_eq!(snap(&e, Value::Bytes(b"large".to_vec())), json!("large"));
        let s = col("set", "set('read','write','admin')");
        assert_eq!(bin(&s, Value::Bytes(vec![0b101])), json!("read,admin"));
        assert_eq!(bin(&s, Value::Bytes(vec![0])), json!(""));
        assert_eq!(
            snap(&s, Value::Bytes(b"read,write".to_vec())),
            json!("read,write")
        );
    }

    #[test]
    fn vectors() {
        let v = col("vector", "vector(2)");
        let mut b = 1.5_f32.to_le_bytes().to_vec();
        b.extend_from_slice(&0.25_f32.to_le_bytes());
        assert_eq!(snap(&v, Value::Bytes(b)), json!([1.5, 0.25]));
    }

    #[test]
    fn nulls() {
        assert_eq!(snap(&col("int", "int"), Value::NULL), json!(null));
    }
}
