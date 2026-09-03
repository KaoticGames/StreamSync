//! Background overlay HTTP server (stream-sync-core).

use crate::paths;
use std::path::PathBuf;
use std::time::Duration;
use stream_sync_core::{OverlayConfig, OverlayServer};
use tauri::AppHandle;
use tracing::info;

pub const DEFAULT_PORT: u16 = 4040;

pub fn configure_environment(
    app: &AppHandle,
    rust_root: &PathBuf,
    ui_assets_root: &PathBuf,
) -> u16 {
    let user_data = paths::legacy_user_data_dir();
    std::env::set_var("STREAMSYNC_USERDATA", &user_data);
    std::env::set_var("STREAMSYNC_RUST_ROOT", rust_root);
    std::env::set_var("STREAMSYNC_UI_ROOT", ui_assets_root);
    std::env::set_var("STREAMSYNC_REPO_ROOT", ui_assets_root);

    let _ = std::fs::create_dir_all(&user_data);
    let _ = std::fs::create_dir_all(user_data.join("fonts"));
    let _ = std::fs::create_dir_all(user_data.join("logs"));

    paths::load_dotenv(rust_root);
    let _ = stream_sync_core::bootstrap_twitch_env_from_rust(&user_data, rust_root);

    let port = std::env::var("OVERLAY_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT);
    std::env::set_var("OVERLAY_PORT", port.to_string());
    if std::env::var("TWITCH_CLIENT_ID")
        .map(|s| s.trim().is_empty())
        .unwrap_or(true)
    {
        tracing::warn!(
            "TWITCH_CLIENT_ID missing — set rust/.env (see config/env.example) and rebuild so bundled.env ships in the installer"
        );
    }

    info!(
        user_data = %user_data.display(),
        rust_root = %rust_root.display(),
        ui_assets_root = %ui_assets_root.display(),
        port,
        "overlay environment configured"
    );
    let _ = app;
    port
}

pub fn spawn_overlay_server(ui_assets_root: PathBuf, port: u16) {
    tauri::async_runtime::spawn(async move {
        let config = OverlayConfig {
            port,
            repo_root: ui_assets_root,
            readonly: false,
            userdata_root: None,
        };
        if let Err(e) = OverlayServer::new(config).run().await {
            tracing::error!("overlay server exited: {e:#}");
        }
    });
}

pub async fn wait_for_health(port: u16, max_wait: Duration) -> bool {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = std::time::Instant::now() + max_wait;

    while std::time::Instant::now() < deadline {
        if let Ok(res) = client.get(&url).send().await {
            if res.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    false
}

pub fn shell_url(port: u16) -> String {
    // Cache-bust shell.html so WebView2 does not keep a stale script manifest.
    format!("http://127.0.0.1:{port}/shell.html?v=1")
}
