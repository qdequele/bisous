//! Origin-side names (slot, publication) and SQL quoting.

use meili_connect_core::SourceError;
use sha2::{Digest, Sha256};

/// Postgres identifiers are at most NAMEDATALEN - 1 = 63 bytes.
pub const MAX_IDENT: usize = 63;

/// Never reused: Supabase Realtime owns it.
pub const SUPABASE_REALTIME: &str = "supabase_realtime";

const PREFIX: &str = "meili_connect_";

/// `meili_connect_<sync_id>`, lowercased, every byte outside `[a-z0-9_]`
/// replaced by `_`. Slot names only allow that alphabet, and using the same
/// name for the publication keeps them easy to match. When the result is
/// longer than 63 bytes it is cut and suffixed with a hash of the full
/// sync id, so two long ids never collide.
pub fn default_name(sync_id: &str) -> String {
    let body: String = sync_id
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let full = format!("{PREFIX}{body}");
    if full.len() <= MAX_IDENT {
        return full;
    }
    let hash = hex::encode(Sha256::digest(sync_id.as_bytes()));
    let suffix = &hash[..12];
    // All bytes are ASCII, so any cut is a char boundary.
    let keep = MAX_IDENT - suffix.len() - 1;
    format!("{}_{suffix}", &full[..keep])
}

/// Slot names: `[a-z0-9_]{1,63}`.
pub fn validate_slot_name(name: &str) -> Result<String, SourceError> {
    if name.is_empty()
        || name.len() > MAX_IDENT
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(SourceError::Config(format!(
            "invalid slot name '{name}': use 1 to 63 characters among a-z, 0-9 and _"
        )));
    }
    Ok(name.to_owned())
}

/// Publication names: any identifier up to 63 bytes without NUL, and never
/// `supabase_realtime`.
pub fn validate_publication_name(name: &str) -> Result<String, SourceError> {
    if name == SUPABASE_REALTIME {
        return Err(SourceError::Config(
            "the supabase_realtime publication belongs to Supabase Realtime and is never reused: \
             pick another publication name"
                .into(),
        ));
    }
    if name.is_empty() || name.len() > MAX_IDENT || name.contains('\0') {
        return Err(SourceError::Config(format!(
            "invalid publication name '{name}': use 1 to 63 bytes"
        )));
    }
    Ok(name.to_owned())
}

/// `"ident"` with embedded quotes doubled.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `'literal'` with embedded quotes doubled. Only used for values we built
/// or validated (names, LSNs): user data always goes through parameters.
pub fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// `schema.table` → (`schema`, `table`). A name without a dot is in
/// `public`. The first dot splits, so a schema name may not contain one.
pub fn split_collection(collection: &str) -> (&str, &str) {
    match collection.split_once('.') {
        Some((schema, table)) => (schema, table),
        None => ("public", collection),
    }
}

/// `"schema"."table"`.
pub fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_name_sanitises() {
        assert_eq!(default_name("abc"), "meili_connect_abc");
        assert_eq!(
            default_name("0190F3A2-7B1C-7DEF-8000-0123456789AB"),
            "meili_connect_0190f3a2_7b1c_7def_8000_0123456789ab"
        );
        assert_eq!(default_name("a b.c/é"), "meili_connect_a_b_c__");
        for name in [default_name("x"), default_name(&"y".repeat(200))] {
            assert!(validate_slot_name(&name).is_ok(), "{name}");
        }
    }

    #[test]
    fn long_names_are_cut_and_unique() {
        let a = default_name(&format!("{}a", "x".repeat(100)));
        let b = default_name(&format!("{}b", "x".repeat(100)));
        assert_eq!(a.len(), MAX_IDENT);
        assert_eq!(b.len(), MAX_IDENT);
        assert_ne!(a, b);
        assert!(a.starts_with("meili_connect_xxx"));
        // Exactly at the limit: unchanged.
        let id = "z".repeat(MAX_IDENT - PREFIX.len());
        assert_eq!(default_name(&id), format!("{PREFIX}{id}"));
    }

    #[test]
    fn slot_and_publication_validation() {
        assert!(validate_slot_name("ok_1").is_ok());
        assert!(validate_slot_name("").is_err());
        assert!(validate_slot_name("Upper").is_err());
        assert!(validate_slot_name(&"a".repeat(64)).is_err());
        assert!(validate_publication_name("Mixed Case-pub").is_ok());
        assert!(validate_publication_name(SUPABASE_REALTIME).is_err());
        assert!(validate_publication_name(&"a".repeat(64)).is_err());
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(qualified("public", "t"), "\"public\".\"t\"");
        assert_eq!(split_collection("s.t.u"), ("s", "t.u"));
        assert_eq!(split_collection("t"), ("public", "t"));
    }
}
