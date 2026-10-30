/*
    Appellation: sign <module>
    Contrib: @FL03
*/
use crate::{Error, Result};
use base64::prelude::{BASE64_STANDARD, Engine as _};
use core::fmt;
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use std::time::{SystemTime, UNIX_EPOCH};

/// Ed25519 request signer for the Polymarket US API.
///
/// The seed is consumed by the key pair and never exposed. `Debug` prints no
/// fields, and there is no `Display` implementation.
pub struct UsSigner {
    key_id: String,
    keypair: Ed25519KeyPair,
}

impl UsSigner {
    /// Build a signer from a standard base64 secret.
    ///
    /// The secret is either a 32-byte seed or 64 bytes of seed followed by the
    /// public key. The public half must match the key derived from the seed.
    /// Surrounding whitespace is ignored.
    pub fn from_base64(key_id: impl Into<String>, secret_b64: &str) -> Result<Self> {
        let raw = BASE64_STANDARD
            .decode(secret_b64.trim())
            .map_err(|_| Error::invalid("secret key is not standard base64"))?;
        let keypair = match raw.len() {
            32 => Ed25519KeyPair::from_seed_unchecked(&raw),
            64 => Ed25519KeyPair::from_seed_and_public_key(&raw[..32], &raw[32..]),
            _ => return Err(Error::invalid("secret key must decode to 32 or 64 bytes")),
        }
        .map_err(|_| Error::invalid("secret key rejected by Ed25519"))?;
        Ok(Self {
            key_id: key_id.into(),
            keypair,
        })
    }

    /// The 32-byte Ed25519 public key derived from the seed.
    pub fn public_key(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(self.keypair.public_key().as_ref());
        out
    }

    /// Sign `timestamp_ms || METHOD || path` and return standard padded base64.
    ///
    /// `path` is the request path only: no scheme, host, query or body. The
    /// method is uppercased before signing.
    pub fn sign(&self, timestamp_ms: u64, method: &str, path: &str) -> String {
        let payload = format!("{timestamp_ms}{}{path}", method.to_ascii_uppercase());
        BASE64_STANDARD.encode(self.keypair.sign(payload.as_bytes()))
    }

    /// The three authentication headers for a request at `timestamp_ms`.
    pub fn headers(&self, timestamp_ms: u64, method: &str, path: &str) -> UsAuthHeaders {
        UsAuthHeaders {
            access_key: self.key_id.clone(),
            timestamp: timestamp_ms.to_string(),
            signature: self.sign(timestamp_ms, method, path),
        }
    }

    /// The authentication headers for a request signed at the current time.
    pub fn headers_now(&self, method: &str, path: &str) -> UsAuthHeaders {
        self.headers(now_ms(), method, path)
    }
}

impl fmt::Debug for UsSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UsSigner").finish_non_exhaustive()
    }
}

/// The `X-PM-Access-Key`, `X-PM-Timestamp` and `X-PM-Signature` header values.
///
/// A signature is replayable for 30 seconds, so `Debug` redacts it.
pub struct UsAuthHeaders {
    pub access_key: String,
    pub timestamp: String,
    pub signature: String,
}

impl fmt::Debug for UsAuthHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UsAuthHeaders")
            .field("access_key", &self.access_key)
            .field("timestamp", &self.timestamp)
            .field("signature", &"<redacted>")
            .finish()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
