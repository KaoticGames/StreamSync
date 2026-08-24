//! Scoped, revocable chat-dock credentials (not the master control capability).

use crate::control_plane::constant_time_eq;
use crate::storage::{self, acquire_cross_process_lock};
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
            storage::ensure_secret_file_permissions(path)?;
            store.reload_locked()?;
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
        self.with_locked_mut(|map| {
            let mut next = map.clone();
            next.insert(token.clone(), cred.clone());
            self.persist_map(&next)?;
            *map = next;
            Ok(cred)
        })
    }

    pub fn revoke(&self, token: &str) -> anyhow::Result<bool> {
        self.with_locked_mut(|map| {
            let mut next = map.clone();
            let changed = if let Some(c) = next.get_mut(token) {
                if c.active {
                    c.active = false;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if changed {
                self.persist_map(&next)?;
                *map = next;
            }
            Ok(changed)
        })
    }

    pub fn revoke_all(&self) -> anyhow::Result<()> {
        self.with_locked_mut(|map| {
            let mut next = map.clone();
            for c in next.values_mut() {
                c.active = false;
            }
            self.persist_map(&next)?;
            *map = next;
            Ok(())
        })
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

    pub fn active_count(&self) -> usize {
        self.inner
            .lock()
            .expect("dock cred lock")
            .values()
            .filter(|credential| credential.active)
            .count()
    }

    fn lock_path(&self) -> std::path::PathBuf {
        self.path.with_extension("store.lock")
    }

    fn with_locked_mut<T>(
        &self,
        f: impl FnOnce(&mut HashMap<String, DockCredential>) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let _lock = acquire_cross_process_lock(&self.lock_path())?;
        self.reload_locked()?;
        let mut guard = self.inner.lock().expect("dock cred lock");
        let result = f(&mut guard)?;
        Ok(result)
    }

    fn reload_locked(&self) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().expect("dock cred lock");
        guard.clear();
        if !self.path.is_file() {
            return Ok(());
        }
        storage::ensure_secret_file_permissions(&self.path)?;
        let raw = std::fs::read_to_string(&self.path)?;
        if let Ok(file) = serde_json::from_str::<DockCredentialFile>(&raw) {
            for c in file.credentials {
                if c.token.starts_with(DOCK_TOKEN_PREFIX) {
                    guard.insert(c.token.clone(), c);
                }
            }
        }
        Ok(())
    }

    fn persist_map(&self, credentials: &HashMap<String, DockCredential>) -> anyhow::Result<()> {
        let file = DockCredentialFile {
            credentials: credentials.values().cloned().collect(),
        };
        let data = serde_json::to_vec_pretty(&file)?;
        storage::write_secret_file(&self.path, &data)?;
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
        std::env::temp_dir().join(format!("streamsync-dock-cred-{}-{}", std::process::id(), n))
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
        let _ = std::fs::remove_file(path.with_extension("store.lock"));
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
        let _ = std::fs::remove_file(path.with_extension("store.lock"));
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
        let _ = std::fs::remove_file(path.with_extension("store.lock"));
    }

    #[test]
    fn concurrent_issues_all_survive_reload() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let store = std::sync::Arc::new(DockCredentialStore::load_or_create(&path).unwrap());
        let mut threads = Vec::new();
        for n in 0..32 {
            let store = store.clone();
            threads.push(std::thread::spawn(move || {
                store.issue("twitch", &format!("profile-{n}")).unwrap()
            }));
        }
        let issued: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        let reloaded = DockCredentialStore::load_or_create(&path).unwrap();
        for credential in issued {
            assert!(
                reloaded.authorize_chat_send(
                    &credential.token,
                    &credential.platform,
                    &credential.profile_id
                ),
                "successful issue was lost during concurrent persistence"
            );
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("store.lock"));
    }

    #[test]
    fn stale_store_cannot_resurrect_revoked_credential() {
        let path = tmp_path();
        let _ = std::fs::remove_file(&path);
        let store_a = DockCredentialStore::load_or_create(&path).unwrap();
        let cred = store_a.issue("twitch", "chat-default").unwrap();
        store_a.revoke(&cred.token).unwrap();
        assert!(!store_a.authorize_chat_send(&cred.token, "twitch", "chat-default"));

        let store_b = DockCredentialStore::load_or_create(&path).unwrap();
        let stale = store_b.issue("kick", "other-profile").unwrap();
        assert!(store_b.authorize_chat_send(&stale.token, "kick", "other-profile"));

        let reloaded = DockCredentialStore::load_or_create(&path).unwrap();
        assert!(!reloaded.authorize_chat_send(&cred.token, "twitch", "chat-default"));
        assert!(reloaded.authorize_chat_send(&stale.token, "kick", "other-profile"));

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("store.lock"));
    }

    #[test]
    fn failed_persist_does_not_publish_memory_mutation() {
        let dir = tmp_path();
        let _ = std::fs::remove_file(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = DockCredentialStore::load_or_create(&dir).unwrap();
        assert!(store.issue("twitch", "chat-default").is_err());
        assert_eq!(store.active_count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
