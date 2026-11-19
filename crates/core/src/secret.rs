//! A string that never reaches logs.

use serde::{Deserialize, Serialize};

/// Wraps credentials (database passwords, API keys, connection strings).
/// `Debug` and `Display` print `<redacted>`. Serialization is transparent
/// because specs are stored encrypted and sent over authenticated channels;
/// never serialize a `Secret` into a log line or an error message.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The only way to read the value. Grep for `expose` to audit uses.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_printed() {
        let s = Secret::new("hunter2");
        assert_eq!(format!("{s:?} {s}"), "<redacted> <redacted>");
        assert_eq!(s.expose(), "hunter2");
    }
}
