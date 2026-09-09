//! Shared application state.

use crate::broadcast::FeedHub;
use crate::config_types::{
    DelegatedSessionFile, DockConfigFile, EventsDockConfig, EventsOverlayConfigFile, KickTokenFile,
    OverlayConfigFile, TwitchActiveMode, TwitchActiveModeFile, TwitchTokenFile,
};
use crate::secret_store::{
    SecretStore, KICK_PERSONAL_ACCESS_KEY, KICK_PERSONAL_FEED_TICKET_KEY,
    KICK_PERSONAL_REFRESH_KEY, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_CONNECTION_KEY,
    TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
    TWITCH_PERSONAL_ACCESS_KEY, TWITCH_PERSONAL_REFRESH_KEY,
};
use crate::storage::{self, StoragePaths};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::RwLock;

type DockControlSockets =
    HashMap<String, HashMap<uuid::Uuid, tokio::sync::mpsc::UnboundedSender<()>>>;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TwitchTokenMetadataFile {
    #[serde(rename = "expiresIn", default, skip_serializing_if = "Option::is_none")]
    expires_in: Option<i64>,
    #[serde(
        rename = "obtainmentTimestamp",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    obtainment_timestamp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    login: Option<String>,
    #[serde(rename = "userId", default, skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scopes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct KickTokenMetadataFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kick_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    login: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scopes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DelegatedSessionMetadataFile {
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    channel_login: String,
    #[serde(default)]
    channel_twitch_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    scopes: Vec<String>,
    #[serde(default)]
    twitch_expires_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    connection_expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kick_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kick_login: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    kick_expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    kick_scopes: Vec<String>,
}

impl From<&TwitchTokenFile> for TwitchTokenMetadataFile {
    fn from(tokens: &TwitchTokenFile) -> Self {
        Self {
            expires_in: tokens.expires_in,
            obtainment_timestamp: tokens.obtainment_timestamp,
            login: tokens.login.clone(),
            user_id: tokens.user_id.clone(),
            scopes: tokens.scopes.clone(),
        }
    }
}

impl From<&KickTokenFile> for KickTokenMetadataFile {
    fn from(tokens: &KickTokenFile) -> Self {
        Self {
            expires_at: tokens.expires_at.clone(),
            kick_id: tokens.kick_id.clone(),
            login: tokens.login.clone(),
            display_name: tokens.display_name.clone(),
            scopes: tokens.scopes.clone(),
        }
    }
}

impl From<&DelegatedSessionFile> for DelegatedSessionMetadataFile {
    fn from(session: &DelegatedSessionFile) -> Self {
        Self {
            generation: session.generation,
            client_id: session.client_id.clone(),
            channel_login: session.channel_login.clone(),
            channel_twitch_id: session.channel_twitch_id.clone(),
            display_name: session.display_name.clone(),
            label: session.label.clone(),
            scopes: session.scopes.clone(),
            twitch_expires_at: session.twitch_expires_at.clone(),
            connection_expires_at: session.connection_expires_at.clone(),
            kick_id: session.kick_id.clone(),
            kick_login: session.kick_login.clone(),
            kick_expires_at: session.kick_expires_at.clone(),
            kick_scopes: session.kick_scopes.clone(),
        }
    }
}

fn nonempty_owned(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn nonempty_ref(value: Option<&str>) -> Option<&str> {
    value.and_then(|v| if v.trim().is_empty() { None } else { Some(v) })
}

fn write_secret_value(
    store: &dyn SecretStore,
    key: &str,
    value: Option<&str>,
) -> anyhow::Result<()> {
    match nonempty_ref(value) {
        Some(secret) => store.set(key, secret.as_bytes()),
        None => store.delete(key),
    }
}

fn read_secret_value(store: &dyn SecretStore, key: &str) -> anyhow::Result<Option<String>> {
    let Some(bytes) = store.get(key)? else {
        return Ok(None);
    };
    let decoded = String::from_utf8(bytes)
        .map_err(|_| anyhow::anyhow!("secret store value for '{key}' is not valid UTF-8"))?;
    Ok(nonempty_owned(Some(decoded)))
}

fn write_personal_twitch_metadata(
    path: &std::path::Path,
    tokens: &TwitchTokenFile,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(&TwitchTokenMetadataFile::from(tokens))?;
    storage::write_secret_file(path, &bytes)
}

fn write_personal_kick_metadata(
    path: &std::path::Path,
    tokens: &KickTokenFile,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(&KickTokenMetadataFile::from(tokens))?;
    storage::write_secret_file(path, &bytes)
}

fn write_delegated_metadata(
    path: &std::path::Path,
    session: &DelegatedSessionFile,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(&DelegatedSessionMetadataFile::from(session))?;
    storage::write_authority_bearing_secret(path, &bytes)
}

fn maybe_migrate_personal_twitch_secrets(
    path: &std::path::Path,
    readonly: bool,
    store: &dyn SecretStore,
    mut tokens: TwitchTokenFile,
) -> anyhow::Result<TwitchTokenFile> {
    let legacy_access = nonempty_owned(tokens.access_token.clone());
    let legacy_refresh = nonempty_owned(tokens.refresh_token.clone());
    let should_read_secret_store = legacy_access.is_some()
        || legacy_refresh.is_some()
        || tokens.login.as_ref().is_some_and(|v| !v.trim().is_empty())
        || tokens
            .user_id
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty())
        || tokens.expires_in.is_some()
        || tokens.obtainment_timestamp.is_some()
        || tokens.scopes.as_ref().is_some_and(|v| !v.is_empty());
    let mut migrated = false;
    if let Some(secret) = legacy_access.as_deref() {
        write_secret_value(store, TWITCH_PERSONAL_ACCESS_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_refresh.as_deref() {
        write_secret_value(store, TWITCH_PERSONAL_REFRESH_KEY, Some(secret))?;
        migrated = true;
    }
    if should_read_secret_store {
        tokens.access_token = read_secret_value(store, TWITCH_PERSONAL_ACCESS_KEY)?;
        tokens.refresh_token = read_secret_value(store, TWITCH_PERSONAL_REFRESH_KEY)?;
    } else {
        tokens.access_token = None;
        tokens.refresh_token = None;
    }
    if migrated && !readonly {
        write_personal_twitch_metadata(path, &tokens)?;
    }
    Ok(tokens)
}

fn maybe_migrate_personal_kick_secrets(
    path: &std::path::Path,
    readonly: bool,
    store: &dyn SecretStore,
    mut tokens: KickTokenFile,
) -> anyhow::Result<KickTokenFile> {
    let legacy_access = nonempty_owned(tokens.access_token.clone());
    let legacy_refresh = nonempty_owned(tokens.refresh_token.clone());
    let legacy_feed_ticket = nonempty_owned(tokens.feed_ticket.clone());
    let should_read_secret_store = legacy_access.is_some()
        || legacy_refresh.is_some()
        || legacy_feed_ticket.is_some()
        || tokens
            .kick_id
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty())
        || tokens.login.as_ref().is_some_and(|v| !v.trim().is_empty())
        || tokens
            .expires_at
            .as_ref()
            .is_some_and(|v| !v.trim().is_empty())
        || tokens.scopes.as_ref().is_some_and(|v| !v.is_empty());
    let mut migrated = false;
    if let Some(secret) = legacy_access.as_deref() {
        write_secret_value(store, KICK_PERSONAL_ACCESS_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_refresh.as_deref() {
        write_secret_value(store, KICK_PERSONAL_REFRESH_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_feed_ticket.as_deref() {
        write_secret_value(store, KICK_PERSONAL_FEED_TICKET_KEY, Some(secret))?;
        migrated = true;
    }
    if should_read_secret_store {
        tokens.access_token = read_secret_value(store, KICK_PERSONAL_ACCESS_KEY)?;
        tokens.refresh_token = read_secret_value(store, KICK_PERSONAL_REFRESH_KEY)?;
        tokens.feed_ticket = read_secret_value(store, KICK_PERSONAL_FEED_TICKET_KEY)?;
    } else {
        tokens.access_token = None;
        tokens.refresh_token = None;
        tokens.feed_ticket = None;
    }
    if migrated && !readonly {
        write_personal_kick_metadata(path, &tokens)?;
    }
    Ok(tokens)
}

fn maybe_migrate_delegated_secrets(
    path: &std::path::Path,
    readonly: bool,
    store: &dyn SecretStore,
    mut session: DelegatedSessionFile,
) -> anyhow::Result<Option<DelegatedSessionFile>> {
    let legacy_connection_key = nonempty_owned(Some(session.connection_key.clone()));
    let legacy_access_token = nonempty_owned(Some(session.access_token.clone()));
    let legacy_kick_access_token = nonempty_owned(session.kick_access_token.clone());
    let legacy_kick_refresh_token = nonempty_owned(session.kick_refresh_token.clone());
    let should_read_secret_store = session.generation > 0
        || !session.channel_login.trim().is_empty()
        || !session.channel_twitch_id.trim().is_empty()
        || legacy_connection_key.is_some()
        || legacy_access_token.is_some();
    let mut migrated = false;
    if let Some(secret) = legacy_connection_key.as_deref() {
        write_secret_value(store, TWITCH_DELEGATED_CONNECTION_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_access_token.as_deref() {
        write_secret_value(store, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_kick_access_token.as_deref() {
        write_secret_value(store, TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY, Some(secret))?;
        migrated = true;
    }
    if let Some(secret) = legacy_kick_refresh_token.as_deref() {
        write_secret_value(store, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY, Some(secret))?;
        migrated = true;
    }
    if !should_read_secret_store {
        return Ok(None);
    }
    session.connection_key =
        read_secret_value(store, TWITCH_DELEGATED_CONNECTION_KEY)?.unwrap_or_default();
    session.access_token =
        read_secret_value(store, TWITCH_DELEGATED_ACCESS_TOKEN_KEY)?.unwrap_or_default();
    session.kick_access_token = read_secret_value(store, TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
    session.kick_refresh_token = read_secret_value(store, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
    if session.connection_key.is_empty() || session.access_token.is_empty() {
        return Ok(None);
    }
    if migrated && !readonly {
        write_delegated_metadata(path, &session)?;
    }
    Ok(Some(session))
}

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

/// Deterministic failure injection for durable delegated-store operations (tests).
#[derive(Default)]
pub struct DurableFailureInject {
    pub tombstone_write: std::sync::atomic::AtomicBool,
    pub credential_remove: std::sync::atomic::AtomicBool,
    pub tombstone_clear: std::sync::atomic::AtomicBool,
    pub save_session: std::sync::atomic::AtomicBool,
    pub save_active_mode: std::sync::atomic::AtomicBool,
    pub parent_sync: std::sync::atomic::AtomicBool,
    pub pending_marker_remove: std::sync::atomic::AtomicBool,
    pub pending_marker_write: std::sync::atomic::AtomicBool,
    /// Legacy reusable `.bak` removal (authority-bearing).
    pub backup_remove: std::sync::atomic::AtomicBool,
    /// Personal Twitch token file write.
    pub save_personal_tokens: std::sync::atomic::AtomicBool,
    /// Personal Kick token file write.
    pub save_kick_tokens: std::sync::atomic::AtomicBool,
}

impl DurableFailureInject {
    pub(crate) fn fail(
        &self,
        flag: &std::sync::atomic::AtomicBool,
        op: &str,
    ) -> anyhow::Result<()> {
        if flag.load(Ordering::SeqCst) {
            anyhow::bail!("injected durable failure: {op}");
        }
        Ok(())
    }
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
    secret_store: Arc<dyn SecretStore>,
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
    /// Test-only durable failure injection hooks.
    pub durable_fail: Arc<DurableFailureInject>,
    /// Weak back-reference for delegated-authority guards on platform operations.
    twitch_services: OnceLock<Weak<crate::twitch::TwitchServices>>,
}

impl AppState {
    pub fn new(
        paths: StoragePaths,
        repo_root: PathBuf,
        port: u16,
        readonly: bool,
        secret_store: Arc<dyn SecretStore>,
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

        let personal = maybe_migrate_personal_twitch_secrets(
            &paths.twitch_tokens,
            readonly,
            secret_store.as_ref(),
            read_json_for_mode(&paths.twitch_tokens, &TwitchTokenFile::default(), readonly)?,
        )?;
        let identity_rollback_pending = paths.twitch_tokens_rollback_pending.is_file();
        let revoked_tombstone = paths.twitch_delegated_revoked.is_file();
        let revoke_pending = paths.twitch_delegated_revoke_pending.is_file();
        let quarantine = revoked_tombstone || revoke_pending;
        if !readonly && !quarantine {
            if let Err(e) = storage::inventory_delegated_startup_authority(&paths.twitch_delegated)
            {
                tracing::warn!("delegated startup inventory failed: {e:#}");
            }
        }
        let replace_pending =
            storage::delegated_replace_pending_path(&paths.twitch_delegated).is_file();
        let delegated = if quarantine {
            // Readonly must inspect/quarantine in memory only — never mutate disk.
            if !readonly && paths.twitch_delegated.is_file() {
                if let Err(e) = storage::remove_file_durable(&paths.twitch_delegated) {
                    tracing::warn!("delegated quarantine cleanup failed: {e:#}");
                }
            }
            if revoke_pending && !revoked_tombstone {
                tracing::warn!("delegated session quarantined: durable revoke still pending");
            } else {
                tracing::warn!("delegated session quarantined: revoked tombstone present");
            }
            None
        } else if replace_pending {
            if readonly {
                tracing::warn!(
                    "delegated replace-pending marker present — refusing delegated load in readonly"
                );
                None
            } else {
                tracing::warn!(
                    "delegated replace-pending marker present after startup inventory — refusing delegated load"
                );
                None
            }
        } else if paths.twitch_delegated.is_file() {
            let session = storage::committed_delegated_session_parse(&paths.twitch_delegated)?;
            match session {
                Some(session) => maybe_migrate_delegated_secrets(
                    &paths.twitch_delegated,
                    readonly,
                    secret_store.as_ref(),
                    session,
                )?,
                None => None,
            }
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
        let active_mode = if identity_rollback_pending {
            tracing::warn!(
                "identity rollback pending at {} — refusing ambiguous Twitch activation",
                paths.twitch_tokens_rollback_pending.display()
            );
            TwitchActiveMode::Local
        } else if quarantine || !delegated_ok {
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
        if quarantine && active_mode == TwitchActiveMode::Local && !readonly {
            let _ = storage::write_json(
                &paths.twitch_active_mode,
                &TwitchActiveModeFile {
                    mode: TwitchActiveMode::Local,
                },
            );
        }
        let live_tokens = if identity_rollback_pending {
            TwitchTokenFile::default()
        } else {
            match active_mode {
                TwitchActiveMode::Delegated => delegated
                    .as_ref()
                    .map(tokens_from_delegated_session)
                    .unwrap_or_default(),
                TwitchActiveMode::Local => personal.clone(),
            }
        };

        let personal_kick = maybe_migrate_personal_kick_secrets(
            &paths.kick_tokens,
            readonly,
            secret_store.as_ref(),
            read_json_for_mode(&paths.kick_tokens, &KickTokenFile::default(), readonly)?,
        )?;
        let live_kick = if identity_rollback_pending {
            KickTokenFile::default()
        } else {
            live_kick_tokens(active_mode, delegated.as_ref(), &personal_kick)
        };
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
            secret_store,
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
            durable_fail: Arc::new(DurableFailureInject::default()),
            twitch_services: OnceLock::new(),
        }))
    }

    pub fn bind_twitch_services(&self, services: &Arc<crate::twitch::TwitchServices>) {
        let _ = self.twitch_services.set(Arc::downgrade(services));
    }

    pub fn twitch_services(&self) -> Option<Arc<crate::twitch::TwitchServices>> {
        self.twitch_services.get()?.upgrade()
    }

    pub fn control_token(&self) -> &str {
        &self.control_token
    }

    pub fn secret_store(&self) -> Arc<dyn SecretStore> {
        self.secret_store.clone()
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
        self.durable_fail.fail(
            &self.durable_fail.save_personal_tokens,
            "save_personal_tokens",
        )?;
        // Always persist personal OAuth separately — never write takeover tokens here.
        let personal = self.personal_tokens.read().await.clone();
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_PERSONAL_ACCESS_KEY,
            personal.access_token.as_deref(),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_PERSONAL_REFRESH_KEY,
            personal.refresh_token.as_deref(),
        )?;
        write_personal_twitch_metadata(&self.paths.twitch_tokens, &personal)?;
        if self.identity_rollback_pending() {
            self.clear_identity_rollback_pending()?;
        }
        Ok(())
    }

    pub async fn save_kick_tokens(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.durable_fail
            .fail(&self.durable_fail.save_kick_tokens, "save_kick_tokens")?;
        let personal = self.personal_kick.read().await.clone();
        write_secret_value(
            self.secret_store.as_ref(),
            KICK_PERSONAL_ACCESS_KEY,
            personal.access_token.as_deref(),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            KICK_PERSONAL_REFRESH_KEY,
            personal.refresh_token.as_deref(),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            KICK_PERSONAL_FEED_TICKET_KEY,
            personal.feed_ticket.as_deref(),
        )?;
        write_personal_kick_metadata(&self.paths.kick_tokens, &personal)
    }

    pub async fn save_delegated(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        let d = self.delegated.read().await;
        match d.as_ref() {
            Some(sess) => self.persist_delegated_session(sess),
            None => self.durable_revoke_delegated().await,
        }
    }

    /// Write a delegated session credential file (does not clear tombstone).
    pub fn persist_delegated_session(&self, sess: &DelegatedSessionFile) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.durable_fail
            .fail(&self.durable_fail.save_session, "save_session")?;
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_DELEGATED_CONNECTION_KEY,
            Some(&sess.connection_key),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
            Some(&sess.access_token),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY,
            sess.kick_access_token.as_deref(),
        )?;
        write_secret_value(
            self.secret_store.as_ref(),
            TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
            sess.kick_refresh_token.as_deref(),
        )?;
        let bytes = serde_json::to_vec_pretty(&DelegatedSessionMetadataFile::from(sess))?;
        // Legacy `.bak` removal and atomic commit are one transaction (B5/B10).
        self.remove_delegated_backup()?;
        storage::write_authority_bearing_secret(&self.paths.twitch_delegated, &bytes)?;
        Ok(())
    }

    /// Remove legacy reusable delegated `.bak` (propagates failure).
    pub fn remove_delegated_backup(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.durable_fail
            .fail(&self.durable_fail.backup_remove, "backup_remove")?;
        let bak = self.paths.twitch_delegated.with_extension("bak");
        storage::remove_file_durable(&bak)?;
        Ok(())
    }

    /// Bounded inventory of authority-bearing delegated secret variants.
    pub fn delegated_secret_variants(&self) -> anyhow::Result<Vec<std::path::PathBuf>> {
        storage::delegated_secret_variants(&self.paths.twitch_delegated)
    }

    /// True when primary, backup, or other inventoried delegated secret files remain.
    pub fn delegated_secret_files_remain(&self) -> anyhow::Result<bool> {
        Ok(self
            .delegated_secret_variants()?
            .iter()
            .any(|p| p.is_file()))
    }

    /// True when authority-bearing secrets or a crash-persistent revoke marker remain.
    pub fn delegated_authority_artifacts_remain(&self) -> anyhow::Result<bool> {
        Ok(self.delegated_secret_files_remain()?
            || self.paths.twitch_delegated_revoke_pending.is_file())
    }

    /// Persist revoked tombstone and remove delegated credential file durably.
    pub async fn durable_revoke_delegated(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        // Crash-safe ordering: pending marker first so restart always fail-closes.
        self.durable_fail.fail(
            &self.durable_fail.pending_marker_write,
            "pending_marker_write",
        )?;
        storage::write_delegated_revoke_pending(&self.paths.twitch_delegated_revoke_pending)?;
        self.durable_fail
            .fail(&self.durable_fail.tombstone_write, "tombstone_write")?;
        storage::write_delegated_revoked_tombstone(&self.paths.twitch_delegated_revoked)?;
        self.durable_fail
            .fail(&self.durable_fail.credential_remove, "credential_remove")?;
        storage::remove_file_durable(&self.paths.twitch_delegated)?;
        // Backup + temp/revoked/committing leftovers are part of the transaction (B1/B7).
        self.remove_delegated_backup()?;
        for leftover in self.delegated_secret_variants()? {
            if leftover == self.paths.twitch_delegated
                || leftover == self.paths.twitch_delegated.with_extension("bak")
            {
                continue;
            }
            if leftover.is_file() {
                storage::remove_file_durable(&leftover)?;
            }
        }
        self.secret_store.delete(TWITCH_DELEGATED_CONNECTION_KEY)?;
        self.secret_store
            .delete(TWITCH_DELEGATED_ACCESS_TOKEN_KEY)?;
        self.secret_store
            .delete(TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
        self.secret_store
            .delete(TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
        self.durable_fail
            .fail(&self.durable_fail.parent_sync, "parent_sync")?;
        storage::sync_parent_dir(&self.paths.twitch_delegated)?;
        // Pending marker clears only when all inventoried authority-bearing files are gone.
        if self.delegated_secret_files_remain()? {
            anyhow::bail!("delegated authority-bearing artifacts remain after revoke");
        }
        self.durable_fail.fail(
            &self.durable_fail.pending_marker_remove,
            "pending_marker_remove",
        )?;
        storage::remove_file_durable(&self.paths.twitch_delegated_revoke_pending)?;
        Ok(())
    }

    /// Mark that durable revoke must complete across restarts (best-effort independent path).
    pub fn mark_durable_revoke_pending(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.durable_fail.fail(
            &self.durable_fail.pending_marker_write,
            "pending_marker_write",
        )?;
        storage::write_delegated_revoke_pending(&self.paths.twitch_delegated_revoke_pending)
    }

    pub fn durable_revoke_pending(&self) -> bool {
        self.paths.twitch_delegated_revoke_pending.is_file()
    }

    pub fn identity_rollback_pending(&self) -> bool {
        self.paths.twitch_tokens_rollback_pending.is_file()
    }

    /// Fail-closed when personal-token rollback could not restore disk coherence.
    pub fn ensure_identity_coherent_for_platform(&self) -> anyhow::Result<()> {
        if self.identity_rollback_pending() {
            anyhow::bail!("Twitch identity recovery required before platform actions");
        }
        Ok(())
    }

    pub fn identity_recovery_required(&self) -> bool {
        self.identity_rollback_pending()
    }

    /// Clear rollback marker after durable/live identity coherence is verified.
    pub fn clear_identity_rollback_pending(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        if !self.paths.twitch_tokens_rollback_pending.is_file() {
            return Ok(());
        }
        storage::remove_file_durable(&self.paths.twitch_tokens_rollback_pending)?;
        storage::sync_parent_dir(&self.paths.twitch_tokens)?;
        Ok(())
    }

    /// Durably clear the revoked tombstone after a new generation credential is on disk.
    pub fn clear_delegated_revoked_tombstone(&self) -> anyhow::Result<()> {
        if self.readonly {
            return Ok(());
        }
        if !self.paths.twitch_delegated_revoked.is_file()
            && !self.paths.twitch_delegated_revoke_pending.is_file()
        {
            return Ok(());
        }
        self.durable_fail
            .fail(&self.durable_fail.tombstone_clear, "tombstone_clear")?;
        if self.paths.twitch_delegated_revoked.is_file() {
            storage::remove_file_durable(&self.paths.twitch_delegated_revoked)?;
        }
        if self.paths.twitch_delegated_revoke_pending.is_file() {
            storage::remove_file_durable(&self.paths.twitch_delegated_revoke_pending)?;
        }
        Ok(())
    }

    pub fn current_delegated_generation(&self) -> u64 {
        self.delegated_generation.load(Ordering::SeqCst)
    }

    pub fn bump_delegated_generation(&self) -> u64 {
        self.delegated_generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn publish_delegated_generation(&self, generation: u64) {
        self.delegated_generation
            .store(generation, Ordering::SeqCst);
    }

    pub fn peek_next_delegated_generation(&self) -> u64 {
        let cur = self.current_delegated_generation();
        if cur == 0 {
            1
        } else {
            cur.saturating_add(1)
        }
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
        self.durable_fail
            .fail(&self.durable_fail.save_active_mode, "save_active_mode")?;
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
        let before = list_tree(&dir);
        let repo = storage::resolve_ui_assets_root();
        let paths = storage::paths_for_root(&dir, true).unwrap();
        let _state = AppState::new(
            paths,
            repo,
            14201,
            true,
            crate::secret_store::memory_secret_store(),
        )
        .expect("readonly app state");
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
        let repo = storage::resolve_ui_assets_root();
        let paths = storage::paths_for_root(&dir, true).unwrap();
        let built = AppState::new(
            paths,
            repo,
            14202,
            true,
            crate::secret_store::memory_secret_store(),
        );
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

    #[test]
    fn readonly_startup_with_tombstone_does_not_mutate_disk() {
        let dir = std::env::temp_dir().join(format!(
            "streamsync-readonly-tombstone-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let delegated = dir.join("twitch-delegated.json");
        let tombstone = dir.join("twitch-delegated.revoked");
        std::fs::write(
            &delegated,
            br#"{"generation":1,"connection_key":"ssk_test_placeholder_readonly","client_id":"cid","access_token":"tok","channel_login":"chan","channel_twitch_id":"1","twitch_expires_at":"2099-01-01T00:00:00Z"}"#,
        )
        .unwrap();
        std::fs::write(&tombstone, br#"{"revoked_at":"2099-01-01T00:00:00Z"}"#).unwrap();
        let before_names = list_tree(&dir);
        let before_delegated = std::fs::metadata(&delegated).unwrap();
        let before_tomb = std::fs::metadata(&tombstone).unwrap();
        let before_delegated_bytes = std::fs::read(&delegated).unwrap();
        let before_tomb_bytes = std::fs::read(&tombstone).unwrap();

        let repo = storage::resolve_ui_assets_root();
        let paths = storage::paths_for_root(&dir, true).unwrap();
        let state = AppState::new(
            paths,
            repo,
            14203,
            true,
            crate::secret_store::memory_secret_store(),
        )
        .expect("readonly with tombstone");

        let after_names = list_tree(&dir);
        assert_eq!(before_names, after_names);
        assert_eq!(before_delegated_bytes, std::fs::read(&delegated).unwrap());
        assert_eq!(before_tomb_bytes, std::fs::read(&tombstone).unwrap());
        let after_delegated = std::fs::metadata(&delegated).unwrap();
        let after_tomb = std::fs::metadata(&tombstone).unwrap();
        assert_eq!(before_delegated.len(), after_delegated.len());
        assert_eq!(before_tomb.len(), after_tomb.len());
        assert_eq!(
            before_delegated.modified().ok(),
            after_delegated.modified().ok()
        );
        assert_eq!(before_tomb.modified().ok(), after_tomb.modified().ok());

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            assert!(state.delegated.read().await.is_none());
            assert_ne!(*state.active_mode.read().await, TwitchActiveMode::Delegated);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
