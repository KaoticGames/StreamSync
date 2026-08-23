//! Scoped, revocable chat-dock credentials (not the master control capability).

use crate::control_plane::constant_time_eq;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

pub const DOCK_TOKEN_PREFIX: &str = "ssd_";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DockCredential {
    pub token: String,
    pub platform: String,
    pub profile_id: String,
    /// When false, credential is revoked.
    pub active: bool,
    pub created_at_ms: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DockCredentialFile {
    credentials: Vec<DockCredential>,
}

pub struct DockCredentialStore {
    path: std::path::PathBuf,
    inner: Mutex<HashMap<String, DockCredential>>,
}

impl DockCredentialStore {
    pub fn load_or_create(path: &Path) -> anyhow::Result<Self> {
        let store = Self {
            path: path.to_path_buf(),
            inner: Mutex::new(HashMap::new()),
        };
        if path.is_file() {
            let raw = std::fs::read_to_string(path).unwrap_or_default();
            if let Ok(file) = serde_json::from_str::<DockCredentialFile>(&raw) {
                let mut guard = store.inner.lock().expect("dock cred lock");
                for c in file.credentials {
                    if c.token.starts_with(DOCK_TOKEN_PREFIX) {
                        guard.insert(c.token.clone(), c);
                    }
                }
            }
        }
        Ok(store)
    }

    pub fn issue(&self, platform: &str, profile_id: &str) -> anyhow::Result<DockCredential> {
        let platform = normalize_platform(platform);
        let profile_id = if profile_id.trim().is_empty() {
            "chat-default".into()
        } else {
            profile_id.trim().to_string()
        };
        let token = format!(
            "{}{}{}",
            DOCK_TOKEN_PREFIX,
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let cred = DockCredential {
            token: token.clone(),
            platform,
            profile_id,
            active: true,
            created_at_ms: chrono::Utc::now().timestamp_millis(),
        };
        {
            let mut guard = self.inner.lock().expect("dock cred lock");
            guard.insert(token, cred.clone());
        }
        self.persist()?;
        Ok(cred)
    }

    pub fn revoke(&self, token: &str) -> anyhow::Result<bool> {
        let mut changed = false;
        {
            let mut guard = self.inner.lock().expect("dock cred lock");
            if let Some(c) = guard.get_mut(token) {
                if c.active {
                    c.active = false;
                    changed = true;
                }
            }
        }
        if changed {
            self.persist()?;
        }
        Ok(changed)
    }

    pub fn revoke_all(&self) -> anyhow::Result<()> {
        {
            let mut guard = self.inner.lock().expect("dock cred lock");
            for c in guard.values_mut() {
                c.active = false;
            }
        }
        self.persist()
    }

    /// Validate chat-send authority for platform/profile. Never authorizes HTTP control.
    pub fn authorize_chat_send(&self, token: &str, platform: &str, profile_id: &str) -> bool {
        let platform = normalize_platform(platform);
        let profile_id = if profile_id.trim().is_empty() {
            "chat-default".to_string()
        } else {
            profile_id.trim().to_string()
        };
        let guard = self.inner.lock().expect("dock cred lock");
        for c in guard.values() {
            if !c.active {
                continue;
            }
            if !constant_time_eq(c.token.as_bytes(), token.as_bytes()) {
                continue;
            }
            return c.platform == platform && c.profile_id == profile_id;
        }
        false
    }

    pub fn is_dock_token(token: &str) -> bool {
        token.trim().starts_with(DOCK_TOKEN_PREFIX)
    }

    fn persist(&self) -> anyhow::Result<()> {
        let guard = self.inner.lock().expect("dock cred lock");
        let file = DockCredentialFile {
            credentials: guard.values().cloned().collect(),
        };
        drop(guard);
        let data = serde_json::to_vec_pretty(&file)?;
        crate::storage::write_secret_file(&self.path, &data)?;
        Ok(())
    }
}

fn normalize_platform(platform: &str) -> String {
    match platform.trim().to_ascii_lowercase().as_str() {
        "kick" => "kick".into(),
        _ => "twitch".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmp_path() -> std::path::PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("streamsync-dock-cred-{n}.json"))
    }

    #[test]
    fn dock_token_cannot_look_like_master() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let store = DockCredentialStore::load_or_create(&path).unwrap();
        let c = store.issue("twitch", "chat-default").unwrap();
        assert!(c.token.starts_with(DOCK_TOKEN_PREFIX));
        assert!(!c.token.starts_with("ssc_"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn revoke_stops_chat_send() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let store = DockCredentialStore::load_or_create(&path).unwrap();
        let c = store.issue("kick", "chat-default").unwrap();
        assert!(store.authorize_chat_send(&c.token, "kick", "chat-default"));
        store.revoke(&c.token).unwrap();
        assert!(!store.authorize_chat_send(&c.token, "kick", "chat-default"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn wrong_platform_or_profile_fails() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let store = DockCredentialStore::load_or_create(&path).unwrap();
        let c = store.issue("twitch", "chat-default").unwrap();
        assert!(!store.authorize_chat_send(&c.token, "kick", "chat-default"));
        assert!(!store.authorize_chat_send(&c.token, "twitch", "other"));
        let _ = std::fs::remove_file(&path);
    }
}
