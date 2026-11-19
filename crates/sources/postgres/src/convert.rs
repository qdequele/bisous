//! Postgres text output → JSON.
//!
//! Both the snapshot (simple-query protocol) and the stream (pgoutput, text
//! mode) see values in their text output form, rendered with the session
//! settings pinned in [`crate::conn::SESSION_OPTIONS`] (`DateStyle=ISO`,
//! `TimeZone=UTC`, `IntervalStyle=iso_8601`, `bytea_output=hex`). One
//! converter serves both, so a row reads the same whichever path it took.

use std::collections::HashMap;

use base64::Engine as _;
use serde_json::{Number, Value};

pub type Oid = u32;

/// How to read the text form of a type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeKind {
    Bool,
    /// int2, int4, int8, oid, xid…
    Int,
    /// float4, float8
    Float,
    Numeric,
    Bytea,
    Json,
    Timestamp,
    TimestampTz,
    /// text, varchar, uuid, date, time, enums, unknown types…
    Text,
    /// pgvector `vector` / `halfvec`: `[1,2.5,3]`.
    Vector,
    /// An array of the given element type (resolved through the registry).
    Array(Oid),
}

/// OID → kind. Built-in OIDs are fixed; others (enums, domains, extension
/// types, their arrays) are looked up in `pg_type` and inserted.
#[derive(Debug, Clone)]
pub struct TypeRegistry {
    kinds: HashMap<Oid, TypeKind>,
}

impl Default for TypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Built-in scalar types (`pg_type.dat`), and their array types.
const BUILTINS: &[(Oid, Oid, TypeKind)] = &[
    // (oid, array oid, kind)
    (16, 1000, TypeKind::Bool),
    (17, 1001, TypeKind::Bytea),
    (18, 1002, TypeKind::Text),   // "char"
    (19, 1003, TypeKind::Text),   // name
    (20, 1016, TypeKind::Int),    // int8
    (21, 1005, TypeKind::Int),    // int2
    (23, 1007, TypeKind::Int),    // int4
    (25, 1009, TypeKind::Text),   // text
    (26, 1028, TypeKind::Int),    // oid
    (28, 1011, TypeKind::Int),    // xid
    (114, 199, TypeKind::Json),   // json
    (142, 143, TypeKind::Text),   // xml
    (700, 1021, TypeKind::Float), // float4
    (701, 1022, TypeKind::Float), // float8
    (790, 791, TypeKind::Text),   // money (locale-dependent text)
    (829, 1040, TypeKind::Text),  // macaddr
    (869, 1041, TypeKind::Text),  // inet
    (650, 651, TypeKind::Text),   // cidr
    (1042, 1014, TypeKind::Text), // bpchar
    (1043, 1015, TypeKind::Text), // varchar
    (1082, 1182, TypeKind::Text), // date
    (1083, 1183, TypeKind::Text), // time
    (1114, 1115, TypeKind::Timestamp),
    (1184, 1185, TypeKind::TimestampTz),
    (1186, 1187, TypeKind::Text), // interval
    (1266, 1270, TypeKind::Text), // timetz
    (1560, 1561, TypeKind::Text), // bit
    (1562, 1563, TypeKind::Text), // varbit
    (1700, 1231, TypeKind::Numeric),
    (2950, 2951, TypeKind::Text), // uuid
    (3802, 3807, TypeKind::Json), // jsonb
    (3614, 3643, TypeKind::Text), // tsvector
    (5038, 5039, TypeKind::Text), // pg_snapshot
];

impl TypeRegistry {
    pub fn new() -> Self {
        let mut kinds = HashMap::with_capacity(BUILTINS.len() * 2);
        for &(oid, array, kind) in BUILTINS {
            kinds.insert(oid, kind);
            kinds.insert(array, TypeKind::Array(oid));
        }
        Self { kinds }
    }

    pub fn get(&self, oid: Oid) -> Option<TypeKind> {
        self.kinds.get(&oid).copied()
    }

    pub fn knows(&self, oid: Oid) -> bool {
        self.kinds.contains_key(&oid)
    }

    pub fn insert(&mut self, oid: Oid, kind: TypeKind) {
        self.kinds.insert(oid, kind);
    }

    /// Kind of `oid`, `Text` when unknown (its text form is kept as is).
    pub fn kind(&self, oid: Oid) -> TypeKind {
        self.get(oid).unwrap_or(TypeKind::Text)
    }

    /// Text output of a value of type `oid` → JSON.
    pub fn to_json(&self, oid: Oid, text: &str) -> Value {
        self.convert(self.kind(oid), text, 0)
    }

    fn convert(&self, kind: TypeKind, text: &str, depth: usize) -> Value {
        match kind {
            TypeKind::Bool => match text {
                "t" | "true" => Value::Bool(true),
                "f" | "false" => Value::Bool(false),
                other => Value::String(other.to_owned()),
            },
            TypeKind::Int => text
                .parse::<i64>()
                .map(Value::from)
                .or_else(|_| text.parse::<u64>().map(Value::from))
                .unwrap_or_else(|_| Value::String(text.to_owned())),
            TypeKind::Float => float(text),
            TypeKind::Numeric => numeric(text),
            TypeKind::Bytea => bytea(text),
            TypeKind::Json => {
                serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_owned()))
            }
            TypeKind::Timestamp => Value::String(timestamp(text, false)),
            TypeKind::TimestampTz => Value::String(timestamp(text, true)),
            TypeKind::Text => Value::String(text.to_owned()),
            TypeKind::Vector => vector(text),
            TypeKind::Array(elem) => {
                // A multidimensional array is one literal; deep nesting of
                // array types (array of domain over array) is not a thing in
                // Postgres, the guard only protects against a bad registry.
                if depth > 4 {
                    return Value::String(text.to_owned());
                }
                let elem_kind = self.kind(elem);
                match parse_array(text) {
                    Some(tree) => self.array_to_json(&tree, elem_kind, depth),
                    None => Value::String(text.to_owned()),
                }
            }
        }
    }

    fn array_to_json(&self, node: &ArrayNode, elem: TypeKind, depth: usize) -> Value {
        match node {
            ArrayNode::Null => Value::Null,
            ArrayNode::Item(s) => self.convert(elem, s, depth + 1),
            ArrayNode::List(items) => Value::Array(
                items
                    .iter()
                    .map(|i| self.array_to_json(i, elem, depth))
                    .collect(),
            ),
        }
    }
}

fn float(text: &str) -> Value {
    match text {
        "NaN" | "Infinity" | "-Infinity" => Value::Null,
        _ => text
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
    }
}

/// A number when it survives the trip through i64 or f64 exactly, else the
/// decimal string (so no precision is silently lost). NaN and infinities are
/// null, like floats.
fn numeric(text: &str) -> Value {
    match text {
        "NaN" | "Infinity" | "-Infinity" => return Value::Null,
        _ => {}
    }
    if let Ok(i) = text.parse::<i64>() {
        return Value::from(i);
    }
    if let Ok(f) = text.parse::<f64>()
        && f.is_finite()
        && normalize_decimal(text) == normalize_decimal(&format_f64(f))
        && let Some(n) = Number::from_f64(f)
    {
        return Value::Number(n);
    }
    Value::String(text.to_owned())
}

fn format_f64(f: f64) -> String {
    // `{}` for f64 is the shortest representation that round-trips and
    // never uses an exponent.
    format!("{f}")
}

/// Canonical decimal text: no leading `+`, no leading zeros, no trailing
/// fractional zeros, `-0` → `0`.
fn normalize_decimal(text: &str) -> String {
    let (neg, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let body = if frac.is_empty() {
        int.to_owned()
    } else {
        format!("{int}.{frac}")
    };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

fn bytea(text: &str) -> Value {
    let engine = base64::engine::general_purpose::STANDARD;
    if let Some(hex) = text.strip_prefix("\\x")
        && let Ok(bytes) = hex::decode(hex)
    {
        return Value::String(engine.encode(bytes));
    }
    // `bytea_output = escape` is never set by us; keep the text.
    Value::String(text.to_owned())
}

/// ISO DateStyle output → ISO-8601: `2024-01-02 03:04:05.5+00` →
/// `2024-01-02T03:04:05.5+00:00`. `infinity`, BC dates and anything
/// unexpected are kept as they are.
fn timestamp(text: &str, tz: bool) -> String {
    let Some((date, time)) = text.split_once(' ') else {
        return text.to_owned();
    };
    if time.ends_with("BC") || date.len() < 10 {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len() + 4);
    out.push_str(date);
    out.push('T');
    if !tz {
        out.push_str(time);
        return out;
    }
    // Offset: last '+' or '-' in the time part (`+00`, `+05:30`, `-03:30:12`).
    match time.rfind(['+', '-']) {
        Some(i) => {
            let (clock, offset) = time.split_at(i);
            out.push_str(clock);
            let sign = &offset[..1];
            let parts: Vec<&str> = offset[1..].split(':').collect();
            match parts.as_slice() {
                [h] => out.push_str(&format!("{sign}{h}:00")),
                [h, m] => out.push_str(&format!("{sign}{h}:{m}")),
                // Seconds in an offset (pre-1900 LMT zones): RFC 3339 has no
                // place for them; keep Postgres's form.
                _ => out.push_str(offset),
            }
        }
        None => out.push_str(time),
    }
    out
}

fn vector(text: &str) -> Value {
    let inner = text.trim();
    if let Some(body) = inner.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        if body.trim().is_empty() {
            return Value::Array(Vec::new());
        }
        let mut out = Vec::new();
        for part in body.split(',') {
            match part.trim().parse::<f64>().ok().and_then(Number::from_f64) {
                Some(n) => out.push(Value::Number(n)),
                None => return Value::String(text.to_owned()),
            }
        }
        return Value::Array(out);
    }
    Value::String(text.to_owned())
}

#[derive(Debug, Clone, PartialEq)]
enum ArrayNode {
    Null,
    Item(String),
    List(Vec<ArrayNode>),
}

/// Parse an array literal (`{1,NULL,"a \"b\""}`, nested `{{1,2},{3,4}}`,
/// optional `[0:1]=` bounds prefix). Every built-in type uses `,` as the
/// delimiter except `box` (`;`), which we treat as text anyway.
fn parse_array(text: &str) -> Option<ArrayNode> {
    let mut s = text.trim();
    if s.starts_with('[') {
        let eq = s.find('=')?;
        s = &s[eq + 1..];
    }
    let bytes = s.as_bytes();
    let mut pos = 0;
    let node = parse_list(bytes, &mut pos)?;
    if pos != bytes.len() {
        return None;
    }
    Some(node)
}

fn parse_list(b: &[u8], pos: &mut usize) -> Option<ArrayNode> {
    if b.get(*pos) != Some(&b'{') {
        return None;
    }
    *pos += 1;
    let mut items = Vec::new();
    if b.get(*pos) == Some(&b'}') {
        *pos += 1;
        return Some(ArrayNode::List(items));
    }
    loop {
        match b.get(*pos)? {
            b'{' => items.push(parse_list(b, pos)?),
            b'"' => {
                *pos += 1;
                let mut buf = Vec::new();
                loop {
                    match b.get(*pos)? {
                        b'\\' => {
                            buf.push(*b.get(*pos + 1)?);
                            *pos += 2;
                        }
                        b'"' => {
                            *pos += 1;
                            break;
                        }
                        &c => {
                            buf.push(c);
                            *pos += 1;
                        }
                    }
                }
                items.push(ArrayNode::Item(String::from_utf8(buf).ok()?));
            }
            _ => {
                let start = *pos;
                let mut buf = Vec::new();
                while let Some(&c) = b.get(*pos) {
                    if c == b',' || c == b'}' {
                        break;
                    }
                    if c == b'\\' {
                        buf.push(*b.get(*pos + 1)?);
                        *pos += 2;
                    } else {
                        buf.push(c);
                        *pos += 1;
                    }
                }
                let raw = &b[start..*pos];
                let item = String::from_utf8(buf).ok()?;
                let item = item.trim();
                if raw.eq_ignore_ascii_case(b"null") {
                    items.push(ArrayNode::Null);
                } else {
                    items.push(ArrayNode::Item(item.to_owned()));
                }
            }
        }
        match b.get(*pos)? {
            b',' => *pos += 1,
            b'}' => {
                *pos += 1;
                return Some(ArrayNode::List(items));
            }
            _ => return None,
        }
    }
}

/// Classify a non-built-in type from its `pg_type` row.
/// `typtype`: b base, c composite, d domain, e enum, p pseudo, r range,
/// m multirange. `typcategory` `A` = array. Domains resolve to their base.
pub fn classify(
    typname: &str,
    typtype: char,
    typcategory: char,
    typelem: Oid,
    typbasetype: Oid,
    registry: &TypeRegistry,
) -> TypeKind {
    match typtype {
        'e' => TypeKind::Text,
        'd' => registry.kind(typbasetype),
        _ if typcategory == 'A' && typelem != 0 => TypeKind::Array(typelem),
        _ if matches!(typname, "vector" | "halfvec") => TypeKind::Vector,
        _ => TypeKind::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conv(oid: Oid, text: &str) -> Value {
        TypeRegistry::new().to_json(oid, text)
    }

    #[test]
    fn scalars() {
        assert_eq!(conv(16, "t"), json!(true));
        assert_eq!(conv(16, "f"), json!(false));
        assert_eq!(conv(21, "-5"), json!(-5));
        assert_eq!(conv(23, "2147483647"), json!(2147483647));
        assert_eq!(conv(20, "-9223372036854775808"), json!(i64::MIN));
        assert_eq!(conv(26, "4294967295"), json!(4294967295u64));
        assert_eq!(conv(25, "hello"), json!("hello"));
        assert_eq!(conv(1043, "x"), json!("x"));
        assert_eq!(
            conv(2950, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
            json!("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        );
        assert_eq!(conv(9999999, "whatever (1,2)"), json!("whatever (1,2)"));
    }

    #[test]
    fn floats() {
        assert_eq!(conv(701, "1.5"), json!(1.5));
        assert_eq!(conv(700, "-0.25"), json!(-0.25));
        assert_eq!(conv(701, "1e+300"), json!(1e300));
        assert_eq!(conv(701, "NaN"), Value::Null);
        assert_eq!(conv(701, "Infinity"), Value::Null);
        assert_eq!(conv(700, "-Infinity"), Value::Null);
    }

    #[test]
    fn numerics() {
        assert_eq!(conv(1700, "42"), json!(42));
        assert_eq!(conv(1700, "1.50"), json!(1.5));
        assert_eq!(conv(1700, "0.1"), json!(0.1));
        assert_eq!(conv(1700, "-0.000"), json!(0.0));
        assert_eq!(
            conv(1700, "12345678901234567890"),
            json!("12345678901234567890")
        );
        assert_eq!(
            conv(1700, "0.12345678901234567890123"),
            json!("0.12345678901234567890123")
        );
        assert_eq!(conv(1700, "NaN"), Value::Null);
        assert_eq!(conv(1700, "Infinity"), Value::Null);
    }

    #[test]
    fn bytea_is_base64() {
        assert_eq!(conv(17, "\\x0102ff"), json!("AQL/"));
        assert_eq!(conv(17, "\\x"), json!(""));
    }

    #[test]
    fn json_is_embedded() {
        assert_eq!(conv(3802, r#"{"a": [1, null]}"#), json!({"a": [1, null]}));
        assert_eq!(conv(114, "3"), json!(3));
        assert_eq!(conv(114, "not json"), json!("not json"));
    }

    #[test]
    fn dates_and_times() {
        assert_eq!(
            conv(1114, "2024-01-02 03:04:05.123456"),
            json!("2024-01-02T03:04:05.123456")
        );
        assert_eq!(
            conv(1184, "2024-01-02 03:04:05+00"),
            json!("2024-01-02T03:04:05+00:00")
        );
        assert_eq!(
            conv(1184, "2024-01-02 03:04:05.5+05:30"),
            json!("2024-01-02T03:04:05.5+05:30")
        );
        assert_eq!(
            conv(1184, "1850-01-01 00:00:00+00:09:21"),
            json!("1850-01-01T00:00:00+00:09:21")
        );
        assert_eq!(conv(1184, "infinity"), json!("infinity"));
        assert_eq!(
            conv(1114, "0044-03-15 12:00:00 BC"),
            json!("0044-03-15 12:00:00 BC")
        );
        assert_eq!(conv(1082, "2024-01-02"), json!("2024-01-02"));
        assert_eq!(conv(1083, "13:45:00"), json!("13:45:00"));
        assert_eq!(conv(1186, "P1DT2H"), json!("P1DT2H"));
    }

    #[test]
    fn arrays() {
        assert_eq!(conv(1007, "{1,2,NULL}"), json!([1, 2, null]));
        assert_eq!(conv(1007, "{{1,2},{3,4}}"), json!([[1, 2], [3, 4]]));
        assert_eq!(
            conv(1009, r#"{a,"b c","q\"x","NULL",\,}"#),
            json!(["a", "b c", "q\"x", "NULL", ","])
        );
        assert_eq!(conv(1009, "{}"), json!([]));
        assert_eq!(conv(1000, "{t,f}"), json!([true, false]));
        assert_eq!(conv(1022, "{1.5,NaN}"), json!([1.5, null]));
        assert_eq!(conv(1007, "[0:1]={7,8}"), json!([7, 8]));
        assert_eq!(
            conv(3807, r#"{"{\"a\": 1}","[2]"}"#),
            json!([{"a": 1}, [2]])
        );
        assert_eq!(
            conv(1185, r#"{"2024-01-02 03:04:05+00"}"#),
            json!(["2024-01-02T03:04:05+00:00"])
        );
        assert_eq!(conv(1007, "{1,2"), json!("{1,2"));
    }

    #[test]
    fn vectors_and_custom_types() {
        let mut reg = TypeRegistry::new();
        let vec_oid = 16_400;
        let vec_array = 16_405;
        let enum_oid = 16_500;
        let enum_array = 16_501;
        let domain_oid = 16_600;
        reg.insert(vec_oid, classify("vector", 'b', 'U', 0, 0, &reg));
        reg.insert(vec_array, classify("_vector", 'b', 'A', vec_oid, 0, &reg));
        reg.insert(enum_oid, classify("mood", 'e', 'E', 0, 0, &reg));
        reg.insert(enum_array, classify("_mood", 'b', 'A', enum_oid, 0, &reg));
        reg.insert(domain_oid, classify("posint", 'd', 'N', 0, 23, &reg));
        assert_eq!(reg.to_json(vec_oid, "[1,2.5,-3]"), json!([1.0, 2.5, -3.0]));
        assert_eq!(reg.to_json(vec_oid, "[]"), json!([]));
        assert_eq!(
            reg.to_json(vec_array, r#"{"[1,2]","[3,4]"}"#),
            json!([[1.0, 2.0], [3.0, 4.0]])
        );
        assert_eq!(reg.to_json(enum_oid, "happy"), json!("happy"));
        assert_eq!(
            reg.to_json(enum_array, "{happy,sad}"),
            json!(["happy", "sad"])
        );
        assert_eq!(reg.to_json(domain_oid, "7"), json!(7));
    }

    #[test]
    fn decimal_normalisation() {
        assert_eq!(normalize_decimal("001.2300"), "1.23");
        assert_eq!(normalize_decimal("-0.0"), "0");
        assert_eq!(normalize_decimal("+5"), "5");
    }
}
