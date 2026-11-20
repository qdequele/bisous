//! MySQL text values → JSON.
//!
//! VStream rows and the snapshot (MySQL text protocol) both carry each value
//! as its MySQL text form, so one converter serves both, keyed by the
//! column's `information_schema` type. The JSON is the same as the `mysql`
//! connector's: integers and exact decimals as numbers, dates as ISO 8601
//! (`TIMESTAMP` in UTC with a `Z`), zero dates as null, binary as base64,
//! `JSON` parsed, `BIT` as a number.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Number, Value as Json};

/// What the converter needs to know about a column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    /// `information_schema.columns.data_type`, lowercase (`int`, `varchar`).
    pub data_type: String,
    /// `column_type` (`int unsigned`, `enum('a','b')`), shown in discovery.
    pub column_type: String,
    pub nullable: bool,
    /// A character column whose charset is `binary`: bytes, base64.
    pub binary_charset: bool,
    /// ENUM / SET members in declaration order.
    pub members: Vec<String>,
}

impl ColumnMeta {
    pub fn new(
        name: &str,
        data_type: &str,
        column_type: &str,
        nullable: bool,
        charset: Option<&str>,
    ) -> Self {
        let data_type = data_type.to_ascii_lowercase();
        let members = if data_type == "enum" || data_type == "set" {
            parse_members(column_type)
        } else {
            Vec::new()
        };
        Self {
            name: name.to_owned(),
            data_type,
            column_type: column_type.to_owned(),
            nullable,
            binary_charset: charset == Some("binary"),
            members,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("column `{column}`: {reason}")]
pub struct ConvertError {
    pub column: String,
    pub reason: String,
}

/// `enum('a','it''s')` → `["a", "it's"]`.
fn parse_members(column_type: &str) -> Vec<String> {
    let Some(start) = column_type.find('(') else {
        return Vec::new();
    };
    let body = &column_type[start + 1..column_type.rfind(')').unwrap_or(column_type.len())];
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (false, '\'') => quoted = true,
            (true, '\'') if chars.peek() == Some(&'\'') => {
                cur.push('\'');
                chars.next();
            }
            (true, '\'') => {
                quoted = false;
                out.push(std::mem::take(&mut cur));
            }
            (true, '\\') => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            (true, c) => cur.push(c),
            (false, _) => {}
        }
    }
    out
}

/// One cell. `enum_as_index`: ENUM and SET cells are numbers (VStream from
/// a vtgate older than Vitess 20), to be mapped to their members.
pub fn to_json(
    col: &ColumnMeta,
    cell: Option<&[u8]>,
    enum_as_index: bool,
) -> Result<Json, ConvertError> {
    let Some(b) = cell else {
        return Ok(Json::Null);
    };
    let invalid = |reason: String| ConvertError {
        column: col.name.clone(),
        reason,
    };
    let text = || String::from_utf8_lossy(b);
    Ok(match col.data_type.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => {
            let s = text();
            let s = s.trim();
            if let Ok(i) = s.parse::<i64>() {
                Json::from(i)
            } else if let Ok(u) = s.parse::<u64>() {
                Json::from(u)
            } else {
                return Err(invalid(format!("not an integer: {s}")));
            }
        }
        "decimal" | "numeric" => decimal_to_json(&text()),
        "float" | "double" | "real" => {
            let s = text();
            let f = s
                .trim()
                .parse::<f64>()
                .map_err(|_| invalid("not a number".into()))?;
            Number::from_f64(f).map(Json::Number).unwrap_or(Json::Null)
        }
        "bit" => {
            if b.len() > 8 {
                return Err(invalid("BIT wider than 64 bits".into()));
            }
            Json::from(b.iter().fold(0_u64, |acc, &x| (acc << 8) | u64::from(x)))
        }
        "year" => match text().trim().parse::<u32>() {
            Ok(0) => Json::Null,
            Ok(y) => Json::from(y),
            Err(_) => return Err(invalid(format!("bad year: {}", text()))),
        },
        "date" => {
            let s = text();
            if s.starts_with("0000-00-00") {
                Json::Null
            } else {
                Json::String(s.into_owned())
            }
        }
        "datetime" | "timestamp" => {
            let s = text();
            if s.starts_with("0000-00-00") {
                Json::Null
            } else {
                let mut out = iso_datetime(&s);
                if col.data_type == "timestamp" {
                    // Both connections run in UTC.
                    out.push('Z');
                }
                Json::String(out)
            }
        }
        "time" => Json::String(normalize_fraction(&text())),
        "json" => {
            if b.is_empty() {
                Json::Null
            } else {
                serde_json::from_slice(b).map_err(|e| invalid(format!("bad JSON: {e}")))?
            }
        }
        "enum" if enum_as_index => {
            let s = text();
            match s.trim().parse::<usize>() {
                Ok(0) => Json::String(String::new()),
                Ok(i) => Json::String(col.members.get(i - 1).cloned().unwrap_or_default()),
                Err(_) => Json::String(s.into_owned()),
            }
        }
        "set" if enum_as_index => {
            let s = text();
            match s.trim().parse::<u64>() {
                Ok(bits) => Json::String(
                    col.members
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i < 64 && bits & (1 << i) != 0)
                        .map(|(_, m)| m.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                Err(_) => Json::String(s.into_owned()),
            }
        }
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "geometry"
        | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
        | "multipolygon" | "geometrycollection" | "geomcollection" | "vector" => {
            Json::String(BASE64.encode(b))
        }
        _ if col.binary_charset => Json::String(BASE64.encode(b)),
        _ => Json::String(text().into_owned()),
    })
}

/// `2024-01-02 03:04:05.120` → `2024-01-02T03:04:05.120000` (the `mysql`
/// connector's form: microseconds only when non-zero).
fn iso_datetime(s: &str) -> String {
    normalize_fraction(&s.replacen(' ', "T", 1))
}

/// A trailing `.fff` → six digits, or nothing when zero.
fn normalize_fraction(s: &str) -> String {
    match s.rsplit_once('.') {
        Some((head, frac)) if frac.chars().all(|c| c.is_ascii_digit()) && !frac.is_empty() => {
            if frac.chars().all(|c| c == '0') {
                head.to_owned()
            } else {
                let mut f: String = frac.chars().take(6).collect();
                while f.len() < 6 {
                    f.push('0');
                }
                format!("{head}.{f}")
            }
        }
        _ => s.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(dt: &str, ct: &str) -> ColumnMeta {
        ColumnMeta::new("c", dt, ct, true, None)
    }

    fn conv(dt: &str, ct: &str, v: &str) -> Json {
        to_json(&col(dt, ct), Some(v.as_bytes()), false).unwrap()
    }

    #[test]
    fn numbers() {
        assert_eq!(conv("int", "int", "-7"), json!(-7));
        assert_eq!(
            conv("bigint", "bigint unsigned", "18446744073709551615"),
            json!(18446744073709551615_u64)
        );
        assert_eq!(conv("decimal", "decimal(10,2)", "12.50"), json!(12.5));
        assert_eq!(
            conv(
                "decimal",
                "decimal(30,10)",
                "12345678901234567890.0000000001"
            ),
            json!("12345678901234567890.0000000001")
        );
        assert_eq!(conv("double", "double", "0.1"), json!(0.1));
        assert_eq!(
            to_json(&col("bit", "bit(10)"), Some(&[0x02, 0x01]), false).unwrap(),
            json!(513)
        );
        assert!(to_json(&col("int", "int"), Some(b"x"), false).is_err());
        assert_eq!(
            to_json(&col("int", "int"), None, false).unwrap(),
            Json::Null
        );
    }

    #[test]
    fn dates_match_the_mysql_connector() {
        assert_eq!(conv("date", "date", "2024-01-02"), json!("2024-01-02"));
        assert_eq!(conv("date", "date", "0000-00-00"), Json::Null);
        assert_eq!(
            conv("datetime", "datetime(3)", "2024-01-02 03:04:05.120"),
            json!("2024-01-02T03:04:05.120000")
        );
        assert_eq!(
            conv("datetime", "datetime", "2024-01-02 03:04:05"),
            json!("2024-01-02T03:04:05")
        );
        assert_eq!(
            conv("timestamp", "timestamp(6)", "2024-01-02 03:04:05.000000"),
            json!("2024-01-02T03:04:05Z")
        );
        assert_eq!(conv("time", "time", "-838:59:59"), json!("-838:59:59"));
        assert_eq!(conv("year", "year", "2024"), json!(2024));
        assert_eq!(conv("year", "year", "0000"), Json::Null);
    }

    #[test]
    fn text_json_binary_and_enums() {
        assert_eq!(conv("varchar", "varchar(10)", "héllo"), json!("héllo"));
        assert_eq!(conv("json", "json", r#"{"a":[1,2]}"#), json!({"a": [1, 2]}));
        assert_eq!(conv("varbinary", "varbinary(4)", "ab"), json!("YWI="));
        let bin = ColumnMeta::new("c", "char", "char(2)", true, Some("binary"));
        assert_eq!(to_json(&bin, Some(b"ab"), false).unwrap(), json!("YWI="));
        let e = col("enum", "enum('small','it''s','x\\'y')");
        assert_eq!(e.members, vec!["small", "it's", "x'y"]);
        assert_eq!(to_json(&e, Some(b"small"), false).unwrap(), json!("small"));
        assert_eq!(to_json(&e, Some(b"2"), true).unwrap(), json!("it's"));
        let s = col("set", "set('a','b','c')");
        assert_eq!(to_json(&s, Some(b"5"), true).unwrap(), json!("a,c"));
        assert_eq!(to_json(&s, Some(b"a,c"), false).unwrap(), json!("a,c"));
    }
}
