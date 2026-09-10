//! Background overlay HTTP server (stream-sync-core).

use crate::paths;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;
use stream_sync_core::{OverlayConfig, OverlayServer};
use tauri::AppHandle;
use tracing::info;

pub const DEFAULT_PORT: u16 = 4040;
pub const HEALTH_SERVICE: &str = "overlay-server";

#[derive(Debug, Deserialize)]
pub struct OverlayHealth {
    pub ok: Option<bool>,
    pub service: Option<String>,
    pub version: Option<String>,
    #[serde(rename = "instanceNonce")]
    pub instance_nonce: Option<String>,
}

pub fn is_expected_health(
    payload: &OverlayHealth,
    expected_service: &str,
    expected_version: &str,
    expected_nonce: Option<&str>,
) -> bool {
    if payload.ok != Some(true) {
        return false;
    }
    if payload.service.as_deref() != Some(expected_service) {
        return false;
    }
    if payload.version.as_deref() != Some(expected_version) {
        return false;
    }
    match expected_nonce {
        Some(nonce) => payload.instance_nonce.as_deref() == Some(nonce),
        None => payload
            .instance_nonce
            .as_deref()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
    }
}

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

#[derive(Debug, Clone)]
pub enum OverlayStart {
    AlreadyRunningSameApp,
    ForeignOccupant { detail: String },
    BindFailed { detail: String },
}

pub fn spawn_overlay_server(
    ui_assets_root: PathBuf,
    port: u16,
) -> (String, tokio::sync::oneshot::Receiver<OverlayStart>) {
    let server = OverlayServer::new(OverlayConfig {
        port,
        repo_root: ui_assets_root,
        readonly: false,
        userdata_root: None,
        secret_store: None,
    });
    let nonce = server.instance_nonce().to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tauri::async_runtime::spawn(async move {
        if let Err(e) = server.run().await {
            let detail = format!("{e:#}");
            let occupant = classify_occupied_port(port).await;
            let _ = tx.send(occupant.unwrap_or(OverlayStart::BindFailed { detail }));
        }
    });
    (nonce, rx)
}

async fn classify_occupied_port(port: u16) -> Option<OverlayStart> {
    let health = fetch_health(port).await?;
    if is_expected_health(&health, HEALTH_SERVICE, env!("CARGO_PKG_VERSION"), None) {
        Some(OverlayStart::AlreadyRunningSameApp)
    } else {
        Some(OverlayStart::ForeignOccupant {
            detail: format!(
                "port {port} is in use by {} {}",
                health.service.unwrap_or_else(|| "unknown".into()),
                health.version.unwrap_or_else(|| "unknown".into())
            ),
        })
    }
}

async fn fetch_health(port: u16) -> Option<OverlayHealth> {
    let url = format!("http://127.0.0.1:{port}/health");
    let res = reqwest::Client::new().get(&url).send().await.ok()?;
    if !res.status().is_success() {
        return None;
    }
    res.json().await.ok()
}

pub async fn wait_for_expected_health(
    port: u16,
    expected_nonce: Option<&str>,
    max_wait: Duration,
) -> bool {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = std::time::Instant::now() + max_wait;
    let version = env!("CARGO_PKG_VERSION");

    while std::time::Instant::now() < deadline {
        if let Ok(res) = client.get(&url).send().await {
            if res.status().is_success() {
                if let Ok(payload) = res.json::<OverlayHealth>().await {
                    if is_expected_health(&payload, HEALTH_SERVICE, version, expected_nonce) {
                        return true;
                    }
                }
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

pub fn startup_error_dialog(message: &str) {
    rfd::MessageDialog::new()
        .set_title("Stream Sync")
        .set_description(message)
        .set_level(rfd::MessageLevel::Error)
        .show();
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_sync_core::health_payload;

    fn parse(json: &str) -> OverlayHealth {
        serde_json::from_str(json).expect("health json")
    }

    #[test]
    fn expected_health_validator_rejects_service_version_or_nonce_mismatch() {
        let ok = parse(&format!(
            r#"{{"ok":true,"service":"overlay-server","version":"{}","instanceNonce":"ssn_abc"}}"#,
            env!("CARGO_PKG_VERSION")
        ));
        assert!(is_expected_health(
            &ok,
            "overlay-server",
            env!("CARGO_PKG_VERSION"),
            Some("ssn_abc")
        ));
        assert!(!is_expected_health(
            &ok,
            "other-server",
            env!("CARGO_PKG_VERSION"),
            Some("ssn_abc")
        ));
        assert!(!is_expected_health(
            &ok,
            "overlay-server",
            "9.9.9",
            Some("ssn_abc")
        ));
        assert!(!is_expected_health(
            &ok,
            "overlay-server",
            env!("CARGO_PKG_VERSION"),
            Some("ssn_other")
        ));
        let http_ok_wrong = parse(r#"{"ok":true,"service":"nginx","version":"1.0"}"#);
        assert!(!is_expected_health(
            &http_ok_wrong,
            "overlay-server",
            "2.0.1",
            None
        ));
    }

    #[test]
    fn wait_for_expected_health_rejects_success_response_from_wrong_identity() {
        let payload = parse(
            r#"{"ok":true,"service":"overlay-server","version":"0.0.0","instanceNonce":"nope"}"#,
        );
        assert!(
            !is_expected_health(&payload, HEALTH_SERVICE, env!("CARGO_PKG_VERSION"), None),
            "HTTP 200 with the wrong identity must not count as ready"
        );
    }

    #[test]
    fn health_payload_includes_service_version_and_instance_nonce() {
        let a = health_payload("ssn_one");
        let b = health_payload("ssn_one");
        assert_eq!(a["service"], "overlay-server");
        assert_eq!(a["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(a["instanceNonce"], "ssn_one");
        assert_eq!(a["instanceNonce"], b["instanceNonce"]);
        let other = health_payload("ssn_two");
        assert_ne!(a["instanceNonce"], other["instanceNonce"]);
    }

    #[test]
    fn desktop_surfaces_bind_failure_as_startup_error() {
        let start = OverlayStart::BindFailed {
            detail: "Port 4040 is already in use".into(),
        };
        let message = match start {
            OverlayStart::BindFailed { detail } => detail,
            _ => String::new(),
        };
        assert!(
            message.contains("already in use"),
            "bind failure must be a user-visible string, not only a log line"
        );
    }
}
