//! Authenticated encryption of sync specs at rest.
//!
//! Layout of a sealed value: `version(1) ‖ nonce(12) ‖ ciphertext`, where the
//! version byte is `1` and the cipher is ChaCha20-Poly1305 with a fresh
//! random nonce per write. Same layout as glutony's connections.

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hmac::{Hmac, Mac};
use meili_connect_core::{Secret, StoreError, SyncSpec};
use rand::RngCore;
use sha2::Sha256;

/// Env var the caller reads the key from.
pub const SECRET_KEY_ENV: &str = "CONNECT_SECRET_KEY";

const VERSION: u8 = 1;
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

/// The 32-byte sealing key. `Debug` never prints it.
#[derive(Clone)]
pub struct SecretKey([u8; KEY_LEN]);

impl SecretKey {
    /// Decode a base64 (standard alphabet, padded or not) 32-byte key.
    pub fn from_base64(encoded: &Secret) -> Result<Self, StoreError> {
        let trimmed = encoded.expose().trim();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(trimmed)
            .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed))
            .map_err(|_| StoreError::Fatal(format!("{SECRET_KEY_ENV} is not valid base64")))?;
        let key: [u8; KEY_LEN] = bytes.try_into().map_err(|_| {
            StoreError::Fatal(format!("{SECRET_KEY_ENV} must decode to {KEY_LEN} bytes"))
        })?;
        Ok(Self(key))
    }

    /// Read and decode [`SECRET_KEY_ENV`].
    pub fn from_env() -> Result<Self, StoreError> {
        let value = std::env::var(SECRET_KEY_ENV)
            .map_err(|_| StoreError::Fatal(format!("{SECRET_KEY_ENV} is not set")))?;
        Self::from_base64(&Secret::new(value))
    }

    /// Build from raw bytes (tests, key generation tooling).
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// A fresh random key.
    pub fn generate() -> Self {
        let mut bytes = [0u8; KEY_LEN];
        rand::rng().fill_bytes(&mut bytes);
        Self(bytes)
    }

    /// The key as base64, for writing it into `CONNECT_SECRET_KEY`.
    pub fn to_base64(&self) -> Secret {
        Secret::new(base64::engine::general_purpose::STANDARD.encode(self.0))
    }

    /// A 32-byte key for another use, derived from this one and `label`
    /// (HMAC-SHA256 under a fixed prefix). Each use has its own label, so
    /// a derived key is never the sealing key nor another use's key.
    pub fn derive_subkey(&self, label: &str) -> [u8; KEY_LEN] {
        let mut mac = match <Hmac<Sha256> as Mac>::new_from_slice(&self.0) {
            Ok(m) => m,
            Err(_) => unreachable!("HMAC accepts keys of any length"),
        };
        mac.update(b"meili-connect/subkey/v1:");
        mac.update(label.as_bytes());
        mac.finalize().into_bytes().into()
    }

    fn cipher(&self) -> ChaCha20Poly1305 {
        ChaCha20Poly1305::new(Key::from_slice(&self.0))
    }

    /// Seal arbitrary bytes.
    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, StoreError> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let ciphertext = self
            .cipher()
            .encrypt(Nonce::from_slice(&nonce_bytes), plaintext)
            .map_err(|_| StoreError::Fatal("sealing failed".into()))?;
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        out.push(VERSION);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Open a value produced by [`SecretKey::seal`]. A wrong key, a tampered
    /// or truncated value is a `StoreError::Fatal`, never a panic.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, StoreError> {
        let Some((&version, rest)) = sealed.split_first() else {
            return Err(StoreError::Fatal("sealed value is empty".into()));
        };
        if version != VERSION {
            return Err(StoreError::Fatal(format!(
                "sealed value has unknown version {version}"
            )));
        }
        if rest.len() < NONCE_LEN {
            return Err(StoreError::Fatal("sealed value is truncated".into()));
        }
        let (nonce, ciphertext) = rest.split_at(NONCE_LEN);
        self.cipher()
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                StoreError::Fatal(format!(
                    "cannot unseal: wrong {SECRET_KEY_ENV} or tampered value"
                ))
            })
    }

    /// Serialize and seal a whole spec.
    pub fn seal_spec(&self, spec: &SyncSpec) -> Result<Vec<u8>, StoreError> {
        let json = serde_json::to_vec(spec)
            .map_err(|e| StoreError::Fatal(format!("cannot serialize sync spec: {e}")))?;
        self.seal(&json)
    }

    /// Unseal and deserialize a spec.
    pub fn open_spec(&self, sealed: &[u8]) -> Result<SyncSpec, StoreError> {
        let json = self.open(sealed)?;
        // The serde error can quote input; it never reaches the message.
        serde_json::from_slice(&json)
            .map_err(|_| StoreError::Fatal("unsealed sync spec is not valid".into()))
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(<redacted>)")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn is_fatal(r: Result<Vec<u8>, StoreError>) -> bool {
        matches!(r, Err(StoreError::Fatal(_)))
    }

    #[test]
    fn subkeys_are_stable_and_separated_by_label() {
        let key = SecretKey::from_bytes([7; KEY_LEN]);
        let a = key.derive_subkey("a");
        assert_eq!(a, key.derive_subkey("a"));
        assert_ne!(a, key.derive_subkey("b"));
        assert_ne!(a, [7; KEY_LEN]);
        assert_ne!(a, SecretKey::from_bytes([8; KEY_LEN]).derive_subkey("a"));
    }

    #[test]
    fn round_trip_and_random_nonce() {
        let key = SecretKey::generate();
        let a = key.seal(b"hello").unwrap();
        let b = key.seal(b"hello").unwrap();
        assert_ne!(a, b, "nonce must be random per write");
        assert_eq!(a[0], 1);
        assert_eq!(a.len(), 1 + 12 + 5 + 16);
        assert_eq!(key.open(&a).unwrap(), b"hello");
        assert_eq!(key.open(&b).unwrap(), b"hello");
    }

    #[test]
    fn wrong_key_is_fatal() {
        let sealed = SecretKey::generate().seal(b"x").unwrap();
        assert!(is_fatal(SecretKey::generate().open(&sealed)));
    }

    #[test]
    fn tampering_is_fatal() {
        let key = SecretKey::generate();
        let sealed = key.seal(b"some secret").unwrap();
        for i in 1..sealed.len() {
            let mut t = sealed.clone();
            t[i] ^= 0x01;
            assert!(is_fatal(key.open(&t)), "flip at byte {i} went unnoticed");
        }
        let mut v = sealed.clone();
        v[0] = 2;
        assert!(is_fatal(key.open(&v)));
    }

    #[test]
    fn truncated_is_fatal() {
        let key = SecretKey::generate();
        let sealed = key.seal(b"some secret").unwrap();
        for len in 0..sealed.len() {
            assert!(is_fatal(key.open(&sealed[..len])), "len {len}");
        }
    }

    #[test]
    fn key_parsing_and_debug() {
        let key = SecretKey::generate();
        let encoded = key.to_base64();
        let back = SecretKey::from_base64(&encoded).unwrap();
        let sealed = key.seal(b"k").unwrap();
        assert_eq!(back.open(&sealed).unwrap(), b"k");
        assert_eq!(format!("{key:?}"), "SecretKey(<redacted>)");
        assert!(SecretKey::from_base64(&Secret::new("not base64!")).is_err());
        assert!(SecretKey::from_base64(&Secret::new("c2hvcnQ=")).is_err());
    }
}
