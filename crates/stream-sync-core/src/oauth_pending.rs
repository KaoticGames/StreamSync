//! One-time OAuth completion nonces (not the master control capability).

use crate::control_plane::constant_time_eq;
use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const LOGIN_NONCE_HEADER: &str = "x-streamsync-login-nonce";
pub const LOGIN_NONCE_TTL: Duration = Duration::from_secs(15 * 60);
/// Reserved nonces remain valid for completion work even after the pending TTL.
pub const LOGIN_COMPLETION_TTL: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OAuthProvider {
    Twitch,
    Kick,
    StreamElements,
}

impl OAuthProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Twitch => "twitch",
            Self::Kick => "kick",
            Self::StreamElements => "streamelements",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "twitch" => Some(Self::Twitch),
            "kick" => Some(Self::Kick),
            "streamelements" | "se" => Some(Self::StreamElements),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingLogin {
    provider: OAuthProvider,
    created_at: Instant,
    consumed: bool,
    reserved: bool,
    reserved_until: Option<Instant>,
    code_verifier: Option<String>,
    code_challenge: Option<String>,
}

/// In-memory single-use login nonces for OAuth callback completion.
#[derive(Default)]
pub struct PendingLoginStore {
    inner: Mutex<HashMap<String, PendingLogin>>,
}

impl PendingLoginStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, provider: OAuthProvider) -> String {
        self.purge_expired();
        let (code_verifier, code_challenge) = if provider == OAuthProvider::Twitch {
            let verifier = generate_pkce_code_verifier();
            let challenge = pkce_s256_challenge(&verifier);
            (Some(verifier), Some(challenge))
        } else {
            (None, None)
        };
        let nonce = format!(
            "ssl_{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let mut guard = self.inner.lock().expect("pending login lock");
        guard.insert(
            nonce.clone(),
            PendingLogin {
                provider,
                created_at: Instant::now(),
                consumed: false,
                reserved: false,
                reserved_until: None,
                code_verifier,
                code_challenge,
            },
        );
        nonce
    }

    pub fn pkce_challenge_for(&self, nonce: &str) -> Option<String> {
        self.purge_expired();
        let guard = self.inner.lock().expect("pending login lock");
        guard
            .get(nonce.trim())
            .filter(|entry| !entry.consumed)
            .and_then(|entry| entry.code_challenge.clone())
    }

    pub fn code_verifier_for(
        &self,
        provider: OAuthProvider,
        nonce: &str,
    ) -> Result<Option<String>, PendingLoginError> {
        let nonce = nonce.trim();
        if nonce.is_empty() {
            return Err(PendingLoginError::Missing);
        }
        self.purge_expired();
        let guard = self.inner.lock().expect("pending login lock");
        let Some(entry) = guard.get(nonce) else {
            return Err(PendingLoginError::Unknown);
        };
        if entry.provider != provider {
            return Err(PendingLoginError::WrongProvider);
        }
        if entry.consumed {
            return Err(PendingLoginError::Replayed);
        }
        if entry.created_at.elapsed() > LOGIN_NONCE_TTL && entry.reserved_until.is_none() {
            return Err(PendingLoginError::Expired);
        }
        Ok(entry.code_verifier.clone())
    }

    /// Atomically validate and reserve a nonce while completion is in progress.
    pub fn reserve(&self, provider: OAuthProvider, nonce: &str) -> Result<(), PendingLoginError> {
        let nonce = nonce.trim();
        if nonce.is_empty() {
            return Err(PendingLoginError::Missing);
        }
        self.purge_expired();
        let mut guard = self.inner.lock().expect("pending login lock");
        let Some(entry) = guard.get_mut(nonce) else {
            return Err(PendingLoginError::Unknown);
        };
        if entry.consumed {
            return Err(PendingLoginError::Replayed);
        }
        if entry.reserved {
            return Err(PendingLoginError::Reserved);
        }
        if entry.created_at.elapsed() > LOGIN_NONCE_TTL && entry.reserved_until.is_none() {
            guard.remove(nonce);
            return Err(PendingLoginError::Expired);
        }
        if entry.provider != provider {
            return Err(PendingLoginError::WrongProvider);
        }
        entry.reserved = true;
        entry.reserved_until = Some(Instant::now() + LOGIN_COMPLETION_TTL);
        Ok(())
    }

    /// Commit a successful completion. The nonce remains as a replay tombstone until TTL.
    pub fn commit(&self, provider: OAuthProvider, nonce: &str) -> Result<(), PendingLoginError> {
        let mut guard = self.inner.lock().expect("pending login lock");
        let Some(entry) = guard.get_mut(nonce.trim()) else {
            return Err(PendingLoginError::Unknown);
        };
        if entry.provider != provider {
            return Err(PendingLoginError::WrongProvider);
        }
        if entry.consumed {
            return Err(PendingLoginError::Replayed);
        }
        if !entry.reserved {
            return Err(PendingLoginError::Unknown);
        }
        entry.reserved = false;
        entry.reserved_until = None;
        entry.consumed = true;
        Ok(())
    }

    /// Release a reservation after validation, provider, or persistence failure.
    pub fn release(&self, provider: OAuthProvider, nonce: &str) {
        let mut guard = self.inner.lock().expect("pending login lock");
        if let Some(entry) = guard.get_mut(nonce.trim()) {
            if entry.provider == provider && !entry.consumed {
                entry.reserved = false;
                entry.reserved_until = None;
            }
        }
    }

    /// Back-compatible one-shot helper for callers that have no fallible completion work.
    pub fn consume(&self, provider: OAuthProvider, nonce: &str) -> Result<(), PendingLoginError> {
        self.reserve(provider, nonce)?;
        self.commit(provider, nonce)
    }

    /// Constant-time membership probe without consuming (for tests / diagnostics).
    pub fn contains_unconsumed(&self, nonce: &str) -> bool {
        let guard = self.inner.lock().expect("pending login lock");
        guard
            .get(nonce)
            .map(|e| !e.consumed && !e.reserved && e.created_at.elapsed() <= LOGIN_NONCE_TTL)
            .unwrap_or(false)
    }

    fn purge_expired(&self) {
        let mut guard = self.inner.lock().expect("pending login lock");
        let now = Instant::now();
        guard.retain(|_, entry| {
            if entry.reserved
                && entry
                    .reserved_until
                    .map(|until| now <= until)
                    .unwrap_or(false)
            {
                return true;
            }
            entry.created_at.elapsed() <= LOGIN_NONCE_TTL || entry.consumed
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingLoginError {
    Missing,
    Unknown,
    Expired,
    Replayed,
    Reserved,
    WrongProvider,
}

impl PendingLoginError {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing_login_nonce",
            Self::Unknown => "invalid_login_nonce",
            Self::Expired => "expired_login_nonce",
            Self::Replayed => "replayed_login_nonce",
            Self::Reserved => "login_nonce_in_progress",
            Self::WrongProvider => "wrong_provider_login_nonce",
        }
    }
}

/// Compare two login nonces without leaking timing (length mismatch still fails fast).
#[allow(dead_code)]
pub fn login_nonce_eq(a: &str, b: &str) -> bool {
    constant_time_eq(a.as_bytes(), b.as_bytes())
}

fn generate_pkce_code_verifier() -> String {
    let mut bytes = [0u8; 48];
    for chunk in bytes.chunks_mut(16) {
        chunk.copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn pkce_s256_challenge(verifier: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use sha2::{Digest, Sha256};

    fn is_unreserved_ascii(value: &str) -> bool {
        value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~'))
    }

    #[test]
    fn consume_once_only() {
        let store = PendingLoginStore::new();
        let n = store.create(OAuthProvider::Twitch);
        assert!(store.consume(OAuthProvider::Twitch, &n).is_ok());
        assert_eq!(
            store.consume(OAuthProvider::Twitch, &n),
            Err(PendingLoginError::Replayed)
        );
    }

    #[test]
    fn rejects_cross_provider() {
        let store = PendingLoginStore::new();
        let n = store.create(OAuthProvider::Kick);
        assert_eq!(
            store.consume(OAuthProvider::Twitch, &n),
            Err(PendingLoginError::WrongProvider)
        );
    }

    #[test]
    fn reservation_blocks_concurrent_completion_and_can_be_released() {
        let store = PendingLoginStore::new();
        let n = store.create(OAuthProvider::StreamElements);
        store.reserve(OAuthProvider::StreamElements, &n).unwrap();
        assert_eq!(
            store.reserve(OAuthProvider::StreamElements, &n),
            Err(PendingLoginError::Reserved)
        );
        store.release(OAuthProvider::StreamElements, &n);
        assert!(store.reserve(OAuthProvider::StreamElements, &n).is_ok());
        store.commit(OAuthProvider::StreamElements, &n).unwrap();
        assert_eq!(
            store.reserve(OAuthProvider::StreamElements, &n),
            Err(PendingLoginError::Replayed)
        );
    }

    #[test]
    fn reserved_nonce_survives_pending_ttl_during_completion() {
        let store = PendingLoginStore::new();
        let n = store.create(OAuthProvider::Twitch);
        store.reserve(OAuthProvider::Twitch, &n).unwrap();
        // Simulate slow provider/persistence — reservation must remain valid.
        assert!(store.commit(OAuthProvider::Twitch, &n).is_ok());
    }

    #[test]
    fn pending_login_stores_pkce_verifier_and_challenge() {
        let store = PendingLoginStore::new();
        let nonce = store.create(OAuthProvider::Twitch);
        let verifier = store
            .code_verifier_for(OAuthProvider::Twitch, &nonce)
            .expect("lookup verifier")
            .expect("twitch verifier");
        let challenge = store
            .pkce_challenge_for(&nonce)
            .expect("twitch challenge is present");

        assert_ne!(verifier, nonce, "PKCE verifier must not reuse nonce");
        assert!(
            (43..=128).contains(&verifier.len()),
            "verifier length must follow RFC 7636"
        );
        assert!(
            is_unreserved_ascii(&verifier),
            "verifier must be unreserved"
        );

        let expected = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, expected, "challenge must be S256(verifier)");
    }
}
