//! Repo root + legacy Electron-compatible userData paths.

use std::path::{Path, PathBuf};
use tauri::{AppHandle, Manager};

/// Same folder Electron used: `%APPDATA%/Stream Sync` (productName), not the Tauri identifier path.
pub fn legacy_user_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("STREAMSYNC_USERDATA") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    #[cfg(windows)]
    {
        if let Some(roaming) = dirs::data_dir() {
            return roaming.join("Stream Sync");
        }
    }

    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".stream-sync")
}

/// UI/static asset root (workspace root in dev, Tauri resource dir when packaged).
pub fn resolve_ui_assets_root(app: &AppHandle) -> PathBuf {
    // `target/debug` often contains a copied UI bundle (shell.html + overlay-server),
    // so resource_dir would win and serve stale assets during `tauri dev`.
    // Always prefer the live workspace in debug builds.
    #[cfg(debug_assertions)]
    {
        let workspace = stream_sync_core::rust_workspace_root();
        if stream_sync_core::is_stream_sync_workspace(&workspace) {
            return workspace;
        }
    }

    if let Ok(res) = app.path().resource_dir() {
        if stream_sync_core::is_stream_sync_ui_bundle(&res) {
            return res;
        }
    }
    stream_sync_core::resolve_ui_assets_root()
}

/// Back-compat name used by older code paths.
#[allow(dead_code)]
pub fn resolve_repo_root(app: &AppHandle) -> PathBuf {
    resolve_ui_assets_root(app)
}

pub fn load_dotenv(rust_root: &Path) {
    let userdata = legacy_user_data_dir();
    stream_sync_core::load_streamsync_dotenv(&userdata, rust_root);
}
