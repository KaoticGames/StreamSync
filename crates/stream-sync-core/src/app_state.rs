//! Shared application state.

use crate::broadcast::FeedHub;
use crate::config_types::{
    DelegatedSessionFile, DockConfigFile, EventsDockConfig, EventsOverlayConfigFile, KickTokenFile,
    OverlayConfigFile, TwitchActiveMode, TwitchActiveModeFile, TwitchTokenFile,
};
use crate::storage::{self, StoragePaths};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

type DockControlSockets =
    HashMap<String, HashMap<uuid::Uuid, tokio::sync::mpsc::UnboundedSender<()>>>;

#[derive(Clone, Default)]
pub struct DockControlRegistry {
    inner: Arc<std::sync::Mutex<DockControlSockets>>,
}

impl DockControlRegistry {
    pub fn register(&self, token: &str) -> (uuid::Uuid, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let id = uuid::Uuid::new_v4();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.inner
            .lock()
            .expect("dock control registry lock")
            .entry(token.to_string())
            .or_default()
            .insert(id, tx);
        (id, rx)
    }

    pub fn unregister(&self, token: &str, id: uuid::Uuid) {
        let mut guard = self.inner.lock().expect("dock control registry lock");
        if let Some(sockets) = guard.get_mut(token) {
            sockets.remove(&id);
            if sockets.is_empty() {
                guard.remove(token);
            }
        }
    }

    pub fn revoke(&self, token: &str) {
        if let Some(sockets) = self
            .inner
            .lock()
            .expect("dock control registry lock")
            .remove(token)
        {
            for sender in sockets.into_values() {
                let _ = sender.send(());
            }
        }
    }

    pub fn revoke_all(&self) {
        let sockets = std::mem::take(&mut *self.inner.lock().expect("dock control registry lock"));
        for sender in sockets
            .into_values()
            .flat_map(|entries| entries.into_values())
        {
            let _ = sender.send(());
        }
    }
}

#[derive(Clone, Default)]
pub struct TwitchRuntime {
    pub tokens: TwitchTokenFile,
    pub connected: bool,
    pub channel: Option<String>,
    /// Broadcaster chat name color (#RRGGBB), from IRC GLOBALUSERSTATE or Helix /chat/color.
    pub name_color: Option<String>,
    /// Broadcaster display name from IRC USERSTATE / GLOBALUSERSTATE.
    pub display_name: Option<String>,
    /// Channel-scoped badges for the logged-in user (`name` → `version`), from IRC USERSTATE.
    pub badges_raw: HashMap<String, String>,
}

#[derive(Clone, Default)]
pub struct KickRuntime {
    pub tokens: KickTokenFile,
    pub connected: bool,
}

pub struct AppState {
    pub paths: StoragePaths,
    /// UI/static files (`shell.html`, `overlay-server/`).
    pub repo_root: PathBuf,
    /// Rust workspace (`rust/`) — config `.env` lives here.
    pub rust_root: PathBuf,
    pub overlay_server_dir: PathBuf,
    pub port: u16,
    pub readonly: bool,
    /// Bundled / env Twitch Client-ID (local OAuth).
    pub client_id: String,
    pub redirect_uri: String,
    pub dock_config: RwLock<DockConfigFile>,
    pub events_dock_config: RwLock<EventsDockConfig>,
    pub overlay_config: RwLock<OverlayConfigFile>,
    pub events_overlay_config: RwLock<EventsOverlayConfigFile>,
    pub twitch: RwLock<TwitchRuntime>,
    /// Personal Twitch OAuth tokens (always persisted separately from takeover).
    pub personal_tokens: RwLock<TwitchTokenFile>,
    /// Saved Syndicate takeover session (if any).
    pub delegated: RwLock<Option<DelegatedSessionFile>>,
    /// Monotonic delegated session generation (fences stale workers).
    pub delegated_generation: AtomicU64,
    /// Which saved identity drives IRC / EventSub.
    pub active_mode: RwLock<TwitchActiveMode>,
    pub feed: FeedHub,
    pub personal_kick: RwLock<KickTokenFile>,
    pub kick: RwLock<KickRuntime>,
    pub kick_feed_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    /// Per-installation localhost control capability (privileged routes + control socket).
    control_token: String,
    /// One-time OAuth completion nonces (never the master capability).
    pub pending_logins: crate::oauth_pending::PendingLoginStore,
    /// Scoped OBS chat-dock credentials.
    pub dock_credentials: crate::dock_capability::DockCredentialStore,
    /// Active dock control sockets, fenced immediately when credentials are revoked.
    pub dock_controls: DockControlRegistry,
}

impl AppState {
    pub fn new(
        paths: StoragePaths,
        repo_root: PathBuf,
        port: u16,
        readonly: bool,
    ) -> anyhow::Result<Arc<Self>> {
        let rust_root = storage::rust_workspace_root();
        storage::load_streamsync_dotenv(&paths.root, &rust_root);
        if !readonly {
            if let Err(e) = storage::bootstrap_twitch_env_from_rust(&paths.root, &rust_root) {
                tracing::debug!("twitch env bootstrap skipped: {e:#}");
            }
        }

        let client_id = std::env::var("TWITCH_CLIENT_ID").unwrap_or_default();
        if client_id.is_empty() && !readonly {
            tracing::warn!(
                "TWITCH_CLIENT_ID is not set — add it to {} (see rust/config/env.example) or {}",
                rust_root.join(".env").display(),
                paths.root.join(".env").display(),
            );
        }
        let redirect_uri = redirect_uri_for_port(port);

        let overlay_server_dir = repo_root.join("overlay-server");

        let mut dock =
            read_json_for_mode(&paths.dock_config, &DockConfigFile::default(), readonly)?;
        dock.profiles
            .entry("chat-default".into())
            .or_insert_with(|| crate::config_types::DockProfile {
                font_size: 13,
                show_timestamps: true,
                show_badges: true,
            });
        let mut events_dock = EventsDockConfig::default();
        if let Some(ed) = dock.events_dock.take() {
            events_dock.font_size = ed.font_size;
            events_dock.show_timestamps = ed.show_timestamps;
            events_dock.show_badges = ed.show_badges;
            events_dock.events = ed.events;
        } else {
            dock.events_dock = Some(events_dock.clone());
            if !readonly {
                storage::write_json(&paths.dock_config, &dock)?;
            }
        }

        let mut overlay = read_json_for_mode(
            &paths.overlay_config,
            &OverlayConfigFile::default(),
            readonly,
        )?;
        overlay
            .profiles
            .entry("chat-default".into())
            .or_insert_with(crate::config_types::ChatOverlayProfile::default);

        let events_overlay = read_json_for_mode(
            &paths.events_overlay_config,
            &EventsOverlayConfigFile::default(),
            readonly,
        )?;

        let personal =
            read_json_for_mode(&paths.twitch_tokens, &TwitchTokenFile::default(), readonly)?;
        let revoked_tombstone = paths.twitch_delegated_revoked.is_file();
        let delegated = if revoked_tombstone {
            if paths.twitch_delegated.is_file() {
                if let Err(e) = storage::remove_file_durable(&paths.twitch_delegated) {
                    tracing::warn!("delegated quarantine cleanup failed: {e:#}");
                }
            }
            tracing::warn!("delegated session quarantined: revoked tombstone present");
            None
        } else if paths.twitch_delegated.is_file() {
            read_json_for_mode(
                &paths.twitch_delegated,
                &DelegatedSessionFile::default(),
                readonly,
            )
            .ok()
            .filter(|d| !d.connection_key.is_empty() && !d.access_token.is_empty())
        } else {
            None
        };
        let delegated_generation =
            AtomicU64::new(delegated.as_ref().map(|d| d.generation.max(1)).unwrap_or(0));
        let saved_mode = read_json_for_mode(
            &paths.twitch_active_mode,
            &TwitchActiveModeFile::default(),
            readonly,
        )
        .map(|f| f.mode)
        .unwrap_or_default();
        let personal_ok = personal.access_token.is_some() && personal.login.is_some();
        let delegated_ok = delegated.is_some();
        let active_mode = if revoked_tombstone || !delegated_ok {
            match saved_mode {
                TwitchActiveMode::Delegated if personal_ok => TwitchActiveMode::Local,
                TwitchActiveMode::Delegated => TwitchActiveMode::Local,
                other => other,
            }
        } else {
            match saved_mode {
                TwitchActiveMode::Delegated if delegated_ok => TwitchActiveMode::Delegated,
                TwitchActiveMode::Local if personal_ok => TwitchActiveMode::Local,
                _ if delegated_ok && !personal_ok => TwitchActiveMode::Delegated,
                _ if personal_ok => TwitchActiveMode::Local,
                _ if delegated_ok => TwitchActiveMode::Delegated,
                _ => TwitchActiveMode::Local,
            }
        };
        if revoked_tombstone && active_mode == TwitchActiveMode::Local && !readonly {
            let _ = storage::write_json(
                &paths.twitch_active_mode,
                &TwitchActiveModeFile {
                    mode: TwitchActiveMode::Local,
                },
            );
        }
        let live_tokens = match active_mode {
            TwitchActiveMode::Delegated => delegated
                .as_ref()
                .map(tokens_from_delegated_session)
                .unwrap_or_default(),
            TwitchActiveMode::Local => personal.clone(),
        };

        let personal_kick =
            read_json_for_mode(&paths.kick_tokens, &KickTokenFile::default(), readonly)?;
        let live_kick = live_kick_tokens(active_mode, delegated.as_ref(), &personal_kick);
        let control_token =
            crate::control_plane::load_control_token(&paths.control_token, readonly)?;
        let dock_credentials = if paths.dock_credentials.is_file() {
            crate::dock_capability::DockCredentialStore::load(&paths.dock_credentials, !readonly)?
        } else if readonly {
            crate::dock_capability::DockCredentialStore::empty_in_memory()
        } else {
            crate::dock_capability::DockCredentialStore::load_or_create(&paths.dock_credentials)?
        };

        Ok(Arc::new(Self {
            paths: paths.clone(),
            repo_root: repo_root.clone(),
            rust_root,
            overlay_server_dir,
            port,
            readonly,
            client_id,
            redirect_uri,
            dock_config: RwLock::new(dock),
            events_dock_config: RwLock::new(events_dock),
            overlay_config: RwLock::new(overlay),
            events_overlay_config: RwLock::new(events_overlay),
            twitch: RwLock::new(TwitchRuntime {
                tokens: live_tokens,
                ..Default::default()
            }),
            personal_tokens: RwLock::new(personal),
            delegated: RwLock::new(delegated),
            delegated_generation,
            active_mode: RwLock::new(active_mode),
            feed: FeedHub::new(),
            personal_kick: RwLock::new(personal_kick),
            kick: RwLock::new(KickRuntime {
                tokens: live_kick,
                connected: false,
            }),
            kick_feed_handle: RwLock::new(None),
            control_token,
            pending_logins: crate::oauth_pending::PendingLoginStore::new(),
            dock_credentials,
            dock_controls: DockControlRegistry::default(),
        }))
    }

    pub fn control_token(&self) -> &str {
        &self.control_token
    }

    pub async fn save_dock(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let mut dock = self.dock_config.write().await;
        dock.events_dock = Some(self.events_dock_config.read().await.clone());
        storage::write_json(&self.paths.dock_config, &*dock)
    }

    pub async fn save_overlay(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let overlay = self.overlay_config.read().await;
        storage::write_json(&self.paths.overlay_config, &*overlay)
    }

    pub async fn save_events_overlay(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let cfg = self.events_overlay_config.read().await;
        storage::write_json(&self.paths.events_overlay_config, &*cfg)
    }

    pub async fn save_twitch_tokens(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        // Always persist personal OAuth separately — never write takeover tokens here.
        let personal = self.personal_tokens.read().await;
        storage::write_json(&self.paths.twitch_tokens, &*personal)
    }

    pub async fn save_kick_tokens(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let personal = self.personal_kick.read().await;
        storage::write_json(&self.paths.kick_tokens, &*personal)
    }

    pub async fn save_delegated(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let d = self.delegated.read().await;
        match d.as_ref() {
            Some(sess) => storage::write_json(&self.paths.twitch_delegated, sess),
            None => self.durable_revoke_delegated().await,
        }
    }

    /// Persist revoked tombstone and remove delegated credential file durably.
    pub async fn durable_revoke_delegated(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        storage::write_delegated_revoked_tombstone(&self.paths.twitch_delegated_revoked)?;
        storage::remove_file_durable(&self.paths.twitch_delegated)?;
        Ok(())
    }

    pub fn current_delegated_generation(&self) -> u64 {
        self.delegated_generation.load(Ordering::SeqCst)
    }

    pub fn bump_delegated_generation(&self) -> u64 {
        self.delegated_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub async fn session_still_current(&self, generation: u64) -> bool {
        if self.current_delegated_generation() != generation {
            return false;
        }
        self.delegated
            .read()
            .await
            .as_ref()
            .is_some_and(|s| s.generation == generation)
    }

    pub async fn delegated_file_exists(&self) -> bool {
        self.paths.twitch_delegated.is_file()
    }

    pub async fn save_active_mode(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let mode = *self.active_mode.read().await;
        storage::write_json(
            &self.paths.twitch_active_mode,
            &TwitchActiveModeFile { mode },
        )
    }

    /// Client-Id for Helix / EventSub: Syndicate client when takeover is the active identity.
    pub async fn helix_client_id(&self) -> String {
        if self.is_delegated_mode().await {
            if let Some(ref d) = *self.delegated.read().await {
                if !d.client_id.is_empty() {
                    return d.client_id.clone();
                }
            }
        }
        self.client_id.clone()
    }

    pub async fn is_delegated_mode(&self) -> bool {
        *self.active_mode.read().await == TwitchActiveMode::Delegated
            && self.delegated.read().await.is_some()
    }
}

/// Live Kick identity: takeover Kick when delegated and the owner linked Kick, else personal.
pub fn live_kick_tokens(
    mode: TwitchActiveMode,
    delegated: Option<&DelegatedSessionFile>,
    personal: &KickTokenFile,
) -> KickTokenFile {
    if mode == TwitchActiveMode::Delegated {
        if let Some(d) = delegated {
            let tok = d.kick_access_token.as_ref().filter(|s| !s.is_empty());
            if tok.is_some() && d.kick_id.as_ref().is_some_and(|s| !s.is_empty()) {
                return KickTokenFile {
                    access_token: d.kick_access_token.clone(),
                    refresh_token: d.kick_refresh_token.clone(),
                    expires_at: d.kick_expires_at.clone(),
                    kick_id: d.kick_id.clone(),
                    login: d.kick_login.clone(),
                    display_name: d.kick_login.clone(),
                    scopes: if d.kick_scopes.is_empty() {
                        None
                    } else {
                        Some(d.kick_scopes.clone())
                    },
                    feed_ticket: None,
                };
            }
        }
    }
    personal.clone()
}

/// Live token view of a saved takeover session (does not touch personal OAuth).
pub fn tokens_from_delegated_session(d: &DelegatedSessionFile) -> TwitchTokenFile {
    let expires_in = chrono::DateTime::parse_from_rfc3339(&d.twitch_expires_at)
        .ok()
        .map(|exp| (exp.timestamp() - chrono::Utc::now().timestamp()).max(0));
    TwitchTokenFile {
        access_token: Some(d.access_token.clone()),
        refresh_token: None,
        expires_in,
        obtainment_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        login: Some(d.channel_login.clone()),
        user_id: Some(d.channel_twitch_id.clone()),
        scopes: Some(d.scopes.clone()),
    }
}

/// OAuth redirect for this server instance. Uses `TWITCH_REDIRECT_URI` when set, but if it
/// points at localhost with a different port than the server (e.g. `.env` has 4040 while
/// Rust A/B runs on 4041), the port is aligned so Twitch callbacks reach the running server.
fn read_json_for_mode<T>(path: &std::path::Path, default: &T, readonly: bool) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned + serde::Serialize + Clone,
{
    if readonly {
        storage::read_json_if_exists(path, default)
    } else {
        storage::read_json_or_default(path, default)
    }
}

fn redirect_uri_for_port(port: u16) -> String {
    let uri = std::env::var("TWITCH_REDIRECT_URI")
        .unwrap_or_else(|_| format!("http://localhost:{port}/auth/twitch/callback"));
    align_localhost_redirect_port(&uri, port)
}

fn align_localhost_redirect_port(uri: &str, port: u16) -> String {
    const PREFIXES: &[&str] = &[
        "http://localhost:",
        "https://localhost:",
        "http://127.0.0.1:",
        "https://127.0.0.1:",
    ];
    for prefix in PREFIXES {
        let Some(after) = uri.strip_prefix(prefix) else {
            continue;
        };
        let path = after
            .find('/')
            .map(|i| &after[i..])
            .unwrap_or("/auth/twitch/callback");
        let scheme = if prefix.starts_with("https") {
            "https"
        } else {
            "http"
        };
        let host = if prefix.contains("127.0.0.1") {
            "127.0.0.1"
        } else {
            "localhost"
        };
        return format!("{scheme}://{host}:{port}{path}");
    }
    uri.to_string()
}

pub fn normalize_chat_profile_id(id: &str) -> String {
    let v = id.trim();
    if v.is_empty() || v == "default" {
        "chat-default".to_string()
    } else {
        v.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn align_localhost_redirect_port_rewrites_port() {
        let out = align_localhost_redirect_port("http://localhost:4040/auth/twitch/callback", 4041);
        assert_eq!(out, "http://localhost:4041/auth/twitch/callback");
    }

    #[test]
    fn align_localhost_redirect_port_leaves_custom_host() {
        let uri = "https://example.com/auth/twitch/callback";
        assert_eq!(align_localhost_redirect_port(uri, 4041), uri);
    }

    fn list_tree(dir: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        if !dir.is_dir() {
            return out;
        }
        for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let path = entry.path();
            let rel = path
                .strip_prefix(dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(rel.clone());
            if path.is_dir() {
                for child in list_tree(&path) {
                    out.push(format!("{rel}/{child}"));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn readonly_startup_does_not_create_persistent_files() {
        let dir = std::env::temp_dir().join(format!(
            "streamsync-readonly-startup-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::env::set_var("STREAMSYNC_USERDATA", dir.display().to_string());
        let before = list_tree(&dir);
        let repo = storage::resolve_ui_assets_root();
        let paths = storage::get_paths_readonly().unwrap();
        let _state = AppState::new(paths, repo, 14201, true).expect("readonly app state");
        let after = list_tree(&dir);
        assert_eq!(
            before, after,
            "readonly startup must not mutate userdata tree"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn readonly_startup_does_not_create_absent_userdata_root() {
        let dir = std::env::temp_dir().join(format!(
            "streamsync-readonly-absent-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!dir.exists());
        std::env::set_var("STREAMSYNC_USERDATA", dir.display().to_string());
        let repo = storage::resolve_ui_assets_root();
        let paths = storage::get_paths_readonly().unwrap();
        let built = AppState::new(paths, repo, 14202, true);
        assert!(
            !dir.exists(),
            "readonly must not create absent userdata root"
        );
        built.expect("readonly app state with absent root");
        assert!(
            !dir.exists(),
            "readonly AppState must not create userdata root"
        );
    }
}
