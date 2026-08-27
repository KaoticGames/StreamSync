//! Stream Sync overlay server — Rust port of `overlay-server/server.js`.
//!
//! Embed in a parent broadcasting app via [`OverlayServer`], or run standalone with
//! the `stream-sync-server` binary (default port **4040**).

mod app_state;
mod broadcast;
mod config_types;
mod control_plane;
mod delegated_lifecycle;
mod dock_capability;
mod export;
mod kick;
mod oauth_pending;
mod route_manifest;
mod routes;
mod storage;
mod store_lock;
mod streamelements;
mod syndicate_connection;
mod twitch;

pub use delegated_lifecycle::{
    redact_connection_key, TeardownPhase, MAX_DELEGATED_REVOCATION_DELAY, SYNDICATE_HTTP_TIMEOUT,
    SYNDICATE_SSE_READ_TIMEOUT,
};
pub use syndicate_connection::connection_key_events_url;
pub use twitch::{disconnect_twitch, TwitchServices};

pub use streamelements::{
    clear_session as se_clear_session, load_session as se_load_session, map_overlay_to_profile,
    save_raw_overlay, save_session as se_save_session, SeClient, SeImportResult, SeOverlaySummary,
    SeSession,
};

pub use app_state::AppState;
pub use config_types::*;
pub use control_plane::{
    authorize_privileged, control_plane_middleware, cors_layer, load_or_create_control_token,
    route_inventory, route_policy, RoutePolicy, CONTROL_TOKEN_HEADER, MEDIA_UPLOAD_BODY_LIMIT,
    PRIVILEGED_JSON_BODY_LIMIT, WS_CONTROL_AUTH_TIMEOUT_MS,
};
pub use dock_capability::{DockCredential, DockCredentialStore};
pub use export::{build_backup_zip, BackupManifest};
pub use kick::sync_live_identity;
pub use oauth_pending::{OAuthProvider, PendingLoginStore, LOGIN_NONCE_HEADER};
pub use routes::BUILD_ROUTER_ROUTE_IDS;
pub use storage::{
    bootstrap_twitch_env_from_rust, get_paths, is_stream_sync_ui_bundle, is_stream_sync_workspace,
    legacy_electron_user_data, load_streamsync_dotenv, paths_for_root, resolve_repo_root,
    resolve_ui_assets_root, rust_dotenv_path, rust_workspace_root, write_secret_file, StoragePaths,
};
pub use storage::{
    remove_file_durable, write_delegated_revoke_pending, write_delegated_revoked_tombstone,
    write_json,
};

/// Back-compat alias.
pub use storage::bootstrap_twitch_env_from_rust as bootstrap_twitch_env_from_repo;

use routes::{build_router, ServerContext};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

/// Server startup options (mirrors Electron `STREAMSYNC_*` env vars).
#[derive(Debug, Clone)]
pub struct OverlayConfig {
    /// HTTP listen port (production and OBS use **4040**).
    pub port: u16,
    /// Repo root containing `overlay-server/` and static assets.
    pub repo_root: PathBuf,
    /// When true, refuse mutating writes (safe A/B against live userData).
    pub readonly: bool,
    /// Optional explicit userdata root (tests). When set, ignores `STREAMSYNC_USERDATA`.
    pub userdata_root: Option<PathBuf>,
}

impl Default for OverlayConfig {
    fn default() -> Self {
        Self {
            port: std::env::var("OVERLAY_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(4040),
            repo_root: storage::resolve_repo_root(),
            readonly: std::env::var("STREAMSYNC_READONLY")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            userdata_root: None,
        }
    }
}

/// Running overlay HTTP + WebSocket server.
pub struct OverlayServer {
    config: OverlayConfig,
}

impl OverlayServer {
    pub fn new(config: OverlayConfig) -> Self {
        Self { config }
    }

    /// Build router + state without binding (useful for tests / parent app composition).
    pub async fn build_app(
        &self,
    ) -> anyhow::Result<(axum::Router, Arc<AppState>, Arc<twitch::TwitchServices>)> {
        let paths = if let Some(root) = &self.config.userdata_root {
            storage::paths_for_root(root, self.config.readonly)?
        } else if self.config.readonly {
            storage::get_paths_readonly()?
        } else {
            storage::get_paths()?
        };
        let state = AppState::new(
            paths,
            self.config.repo_root.clone(),
            self.config.port,
            self.config.readonly,
        )?;
        let twitch = Arc::new(twitch::TwitchServices::new());
        twitch.init_teardown_worker();
        twitch.init_durable_revoke_worker();
        state.bind_twitch_services(&twitch);
        let ctx = ServerContext {
            state: state.clone(),
            twitch: twitch.clone(),
        };
        let router = build_router(ctx);
        Ok((router, state, twitch))
    }

    /// Start listening until the process is interrupted.
    pub async fn run(self) -> anyhow::Result<()> {
        let port = self.config.port;
        let (router, state, twitch) = self.build_app().await?;
        twitch::maybe_autostart(state.clone(), twitch).await;
        kick::maybe_autostart(state.clone()).await;

        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                anyhow::anyhow!(
                    "Port {port} is already in use (another Stream Sync / overlay server may be running).\n\
                     Stop the other process or set OVERLAY_PORT to a different port (e.g. 4042).\n\
                     Windows: netstat -ano | findstr :{port}  then  taskkill /PID <pid> /F"
                )
            } else {
                anyhow::Error::from(e)
            }
        })?;
        let studio = state.overlay_server_dir.join("events-studio.html");
        info!(
            "stream-sync-core listening on http://localhost:{port} (readonly={}, twitch_redirect={}, repo_root={}, overlay_server={}, events_studio_exists={})",
            self.config.readonly,
            state.redirect_uri,
            self.config.repo_root.display(),
            state.overlay_server_dir.display(),
            studio.is_file(),
        );
        axum::serve(listener, router).await?;
        Ok(())
    }
}

/// Convenience: start with default [`OverlayConfig`].
pub async fn run_default() -> anyhow::Result<()> {
    OverlayServer::new(OverlayConfig::default()).run().await
}
