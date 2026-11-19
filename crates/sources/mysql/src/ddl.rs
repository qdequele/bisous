//! Just enough DDL parsing to know which tables a binlog `QUERY` event
//! touches.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DdlKind {
    /// `TRUNCATE [TABLE] t`.
    Truncate,
    /// `DROP TABLE t[, u]`.
    Drop,
    /// `ALTER TABLE`, `CREATE TABLE`, `RENAME TABLE`, `CREATE/DROP INDEX … ON t`.
    Alter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ddl {
    pub kind: DdlKind,
    /// `(database, table)`; the database defaults to the event's schema.
    pub tables: Vec<(String, String)>,
}

/// What a `QUERY` event means for the transaction structure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryKind {
    Begin,
    /// `XA START` / `XA BEGIN`: an XA transaction begins. It ends with an
    /// `XA_PREPARE_LOG_EVENT` (two-phase or one-phase), not with a `COMMIT`.
    XaStart,
    /// `COMMIT` / `ROLLBACK` / `XA COMMIT`: transaction end.
    Commit,
    Ddl(Ddl),
    /// Anything else (statement-based leftovers, `SAVEPOINT`…).
    Other,
}

pub fn classify(query: &str, default_db: &str) -> QueryKind {
    let tokens = tokenize(query);
    let upper = |i: usize| tokens.get(i).map(|t| t.text.to_ascii_uppercase());
    let Some(first) = upper(0) else {
        return QueryKind::Other;
    };
    match first.as_str() {
        "BEGIN" => QueryKind::Begin,
        "START" if upper(1).as_deref() == Some("TRANSACTION") => QueryKind::Begin,
        "COMMIT" | "ROLLBACK" => {
            // `ROLLBACK TO SAVEPOINT x` does not end the transaction.
            if upper(1).as_deref() == Some("TO") {
                QueryKind::Other
            } else {
                QueryKind::Commit
            }
        }
        "XA" if matches!(upper(1).as_deref(), Some("START" | "BEGIN")) => QueryKind::XaStart,
        "XA" if matches!(upper(1).as_deref(), Some("COMMIT" | "ROLLBACK")) => QueryKind::Commit,
        "TRUNCATE" => {
            let mut i = 1;
            if upper(i).as_deref() == Some("TABLE") {
                i += 1;
            }
            ddl(DdlKind::Truncate, &tokens, i, default_db, false)
        }
        "DROP" => {
            let mut i = 1;
            if upper(i).as_deref() == Some("TEMPORARY") {
                return QueryKind::Other;
            }
            match upper(i).as_deref() {
                Some("TABLE") => {
                    i += 1;
                    if upper(i).as_deref() == Some("IF")
                        && upper(i + 1).as_deref() == Some("EXISTS")
                    {
                        i += 2;
                    }
                    ddl(DdlKind::Drop, &tokens, i, default_db, true)
                }
                Some("INDEX") => on_table(&tokens, default_db),
                _ => QueryKind::Other,
            }
        }
        "ALTER" => {
            // ALTER [ONLINE|IGNORE] TABLE t
            let mut i = 1;
            while let Some(t) = upper(i) {
                if t == "TABLE" {
                    return ddl(DdlKind::Alter, &tokens, i + 1, default_db, false);
                }
                if i > 3 {
                    break;
                }
                i += 1;
            }
            QueryKind::Other
        }
        "RENAME"
            if upper(1).as_deref() == Some("TABLE") || upper(1).as_deref() == Some("TABLES") =>
        {
            // RENAME TABLE a TO b, c TO d: both sides matter.
            let mut tables = Vec::new();
            let mut i = 2;
            while i < tokens.len() {
                if let Some((t, next)) = qualified(&tokens, i, default_db) {
                    tables.push(t);
                    i = next;
                } else {
                    i += 1;
                }
                while i < tokens.len()
                    && matches!(tokens[i].text.to_ascii_uppercase().as_str(), "TO" | ",")
                {
                    i += 1;
                }
            }
            QueryKind::Ddl(Ddl {
                kind: DdlKind::Alter,
                tables,
            })
        }
        "CREATE" => {
            let mut i = 1;
            while let Some(t) = upper(i) {
                match t.as_str() {
                    "TEMPORARY" => return QueryKind::Other,
                    "TABLE" => {
                        i += 1;
                        if upper(i).as_deref() == Some("IF") {
                            i += 3; // IF NOT EXISTS
                        }
                        return ddl(DdlKind::Alter, &tokens, i, default_db, false);
                    }
                    "INDEX" => return on_table(&tokens, default_db),
                    _ if i > 4 => return QueryKind::Other,
                    _ => i += 1,
                }
            }
            QueryKind::Other
        }
        _ => QueryKind::Other,
    }
}

fn ddl(kind: DdlKind, tokens: &[Token], start: usize, default_db: &str, list: bool) -> QueryKind {
    let mut tables = Vec::new();
    let mut i = start;
    while let Some((t, next)) = qualified(tokens, i, default_db) {
        tables.push(t);
        i = next;
        if list && tokens.get(i).is_some_and(|t| t.text == ",") {
            i += 1;
        } else {
            break;
        }
    }
    if tables.is_empty() {
        QueryKind::Other
    } else {
        QueryKind::Ddl(Ddl { kind, tables })
    }
}

/// `CREATE … INDEX x ON t` / `DROP INDEX x ON t`.
fn on_table(tokens: &[Token], default_db: &str) -> QueryKind {
    match tokens
        .iter()
        .position(|t| !t.quoted && t.text.eq_ignore_ascii_case("ON"))
    {
        Some(i) => ddl(DdlKind::Alter, tokens, i + 1, default_db, false),
        None => QueryKind::Other,
    }
}

/// Reads `name` or `db.name` at `i`; returns it and the next index.
fn qualified(tokens: &[Token], i: usize, default_db: &str) -> Option<((String, String), usize)> {
    let first = tokens.get(i).filter(|t| t.is_ident())?;
    if tokens.get(i + 1).is_some_and(|t| t.text == ".")
        && let Some(second) = tokens.get(i + 2).filter(|t| t.is_ident())
    {
        return Some(((first.text.clone(), second.text.clone()), i + 3));
    }
    Some(((default_db.to_owned(), first.text.clone()), i + 1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    text: String,
    quoted: bool,
}

impl Token {
    fn is_ident(&self) -> bool {
        self.quoted
            || self
                .text
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '$')
    }
}

/// Words, backtick-quoted identifiers and single punctuation characters.
/// Comments (`/* */`, `-- `, `#`) and string literals are skipped.
fn tokenize(query: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let chars: Vec<char> = query.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i < chars.len() && !(chars[i] == '*' && chars.get(i + 1) == Some(&'/')) {
                i += 1;
            }
            i += 2;
        } else if c == '#' || (c == '-' && chars.get(i + 1) == Some(&'-')) {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '`' {
            let mut s = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '`' {
                    if chars.get(i + 1) == Some(&'`') {
                        s.push('`');
                        i += 2;
                        continue;
                    }
                    break;
                }
                s.push(chars[i]);
                i += 1;
            }
            i += 1;
            out.push(Token {
                text: s,
                quoted: true,
            });
        } else if c == '\'' || c == '"' {
            let q = c;
            i += 1;
            while i < chars.len() && chars[i] != q {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1;
            out.push(Token {
                text: "''".into(),
                quoted: false,
            });
        } else if c.is_alphanumeric() || c == '_' || c == '$' {
            let mut s = String::new();
            while i < chars.len()
                && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$')
            {
                s.push(chars[i]);
                i += 1;
            }
            out.push(Token {
                text: s,
                quoted: false,
            });
        } else {
            out.push(Token {
                text: c.to_string(),
                quoted: false,
            });
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(q: &str) -> (DdlKind, Vec<(String, String)>) {
        match classify(q, "app") {
            QueryKind::Ddl(d) => (d.kind, d.tables),
            other => panic!("{q}: {other:?}"),
        }
    }

    fn t(db: &str, name: &str) -> (String, String) {
        (db.into(), name.into())
    }

    #[test]
    fn transaction_markers() {
        assert_eq!(classify("BEGIN", "x"), QueryKind::Begin);
        assert_eq!(classify("COMMIT", "x"), QueryKind::Commit);
        assert_eq!(classify("ROLLBACK", "x"), QueryKind::Commit);
        assert_eq!(classify("ROLLBACK TO SAVEPOINT a", "x"), QueryKind::Other);
        assert_eq!(classify("XA COMMIT 'x'", "x"), QueryKind::Commit);
        assert_eq!(classify("XA ROLLBACK X'62',X'',1", "x"), QueryKind::Commit);
        // As the binlog writes them (MySQL 8.4).
        assert_eq!(classify("XA START X'61',X'',1", "x"), QueryKind::XaStart);
        assert_eq!(classify("xa begin 'a'", "x"), QueryKind::XaStart);
        assert_eq!(classify("XA END X'61',X'',1", "x"), QueryKind::Other);
        assert_eq!(classify("SAVEPOINT a", "x"), QueryKind::Other);
        assert_eq!(classify("", "x"), QueryKind::Other);
    }

    #[test]
    fn truncate_and_drop() {
        assert_eq!(
            tables("TRUNCATE TABLE items"),
            (DdlKind::Truncate, vec![t("app", "items")])
        );
        assert_eq!(
            tables("truncate `shop`.`it``ems`"),
            (DdlKind::Truncate, vec![t("shop", "it`ems")])
        );
        assert_eq!(
            tables("DROP TABLE IF EXISTS `a`, shop.b /* generated by server */"),
            (DdlKind::Drop, vec![t("app", "a"), t("shop", "b")])
        );
        assert_eq!(classify("DROP TEMPORARY TABLE x", "app"), QueryKind::Other);
        assert_eq!(classify("DROP DATABASE app", "app"), QueryKind::Other);
    }

    #[test]
    fn alters() {
        assert_eq!(
            tables("/* c */ ALTER TABLE items ADD COLUMN note VARCHAR(10) DEFAULT 'x'"),
            (DdlKind::Alter, vec![t("app", "items")])
        );
        assert_eq!(
            tables("ALTER ONLINE TABLE s.i ADD c INT"),
            (DdlKind::Alter, vec![t("s", "i")])
        );
        assert_eq!(
            tables("CREATE TABLE IF NOT EXISTS s.n (id INT PRIMARY KEY)"),
            (DdlKind::Alter, vec![t("s", "n")])
        );
        assert_eq!(
            tables("RENAME TABLE a TO b, s.c TO s.d"),
            (
                DdlKind::Alter,
                vec![t("app", "a"), t("app", "b"), t("s", "c"), t("s", "d")]
            )
        );
        assert_eq!(
            tables("CREATE UNIQUE INDEX ix ON items (a)"),
            (DdlKind::Alter, vec![t("app", "items")])
        );
        assert_eq!(
            classify("CREATE TEMPORARY TABLE x (a int)", "app"),
            QueryKind::Other
        );
        assert_eq!(
            classify("INSERT INTO x VALUES (1)", "app"),
            QueryKind::Other
        );
    }
}
