//! Tauri commands — replaces Electron `electronAPI` / IPC.

use crate::overlay_proxy;
use crate::paths::legacy_user_data_dir;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Mutex;
use stream_sync_core::{build_backup_zip, get_paths};
use tauri::{AppHandle, Manager, State, WebviewUrl, WebviewWindow, WebviewWindowBuilder};
use tauri_plugin_opener::OpenerExt;

static SE_IMPORT_WINDOW: Mutex<()> = Mutex::new(());

#[derive(Clone)]
pub struct AppState {
    pub overlay_port: u16,
    pub logs_dir: PathBuf,
}

#[derive(Serialize)]
pub struct PurgeLogsResult {
    pub ok: bool,
    pub deleted: usize,
}

#[derive(Serialize)]
pub struct ExportBackupResult {
    pub ok: bool,
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[tauri::command]
pub fn get_overlay_base_url(state: State<'_, AppState>) -> String {
    format!("http://127.0.0.1:{}", state.overlay_port)
}

#[tauri::command]
pub fn get_overlay_port(state: State<'_, AppState>) -> u16 {
    state.overlay_port
}

#[tauri::command]
pub async fn overlay_api_request(
    window: WebviewWindow,
    state: State<'_, AppState>,
    request: overlay_proxy::OverlayApiRequest,
) -> Result<overlay_proxy::OverlayApiResponse, String> {
    overlay_proxy::execute_overlay_api_request(&window, state.overlay_port, request).await
}

#[tauri::command]
pub fn open_external(app: AppHandle, url: String) -> Result<(), String> {
    app.opener()
        .open_url(url, None::<&str>)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub fn open_logs_folder(state: State<'_, AppState>) -> Result<(), String> {
    let logs = &state.logs_dir;
    std::fs::create_dir_all(logs).map_err(|e| e.to_string())?;
    open::that(logs).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn open_discord(app: AppHandle) -> Result<(), String> {
    app.opener()
        .open_url("https://discord.gg/MR2W3gtvpw", None::<&str>)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn twitch_connect(state: State<'_, AppState>) -> Result<(), String> {
    twitch_open_auth_url(state.inner().overlay_port).await
}

#[tauri::command]
pub async fn twitch_reconnect(state: State<'_, AppState>) -> Result<(), String> {
    twitch_open_auth_url(state.inner().overlay_port).await
}

async fn twitch_open_auth_url(port: u16) -> Result<(), String> {
    open_overlay_auth_url(port, "/api/twitch/auth-url").await
}

async fn open_overlay_auth_url(port: u16, path: &str) -> Result<(), String> {
    let paths = get_paths().map_err(|e| e.to_string())?;
    let token = std::fs::read_to_string(&paths.control_token).map_err(|e| e.to_string())?;
    let token = token.trim();
    if token.len() < 32 {
        return Err("control capability unavailable".into());
    }
    let origin = format!("http://127.0.0.1:{port}");
    let res = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}{path}"))
        .header("Origin", &origin)
        .header("x-streamsync-control", token)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("HTTP {}", res.status()));
    }
    let body: serde_json::Value = res.json().await.map_err(|e| e.to_string())?;
    let url = body
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "No auth URL in response".to_string())?;
    open::that(url).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn kick_connect(state: State<'_, AppState>) -> Result<(), String> {
    open_overlay_auth_url(state.inner().overlay_port, "/api/kick/auth-url").await
}

#[tauri::command]
pub async fn twitch_disconnect(state: State<'_, AppState>) -> Result<(), String> {
    let paths = get_paths().map_err(|e| e.to_string())?;
    let token = std::fs::read_to_string(&paths.control_token).map_err(|e| e.to_string())?;
    let port = state.overlay_port;
    let origin = format!("http://127.0.0.1:{port}");
    let res = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/api/twitch/disconnect"))
        .header("Origin", &origin)
        .header("x-streamsync-control", token.trim())
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !res.status().is_success() {
        return Err(format!("HTTP {}", res.status()));
    }
    Ok(())
}

#[tauri::command]
pub fn purge_logs(state: State<'_, AppState>) -> Result<PurgeLogsResult, String> {
    let logs_dir = &state.logs_dir;
    std::fs::create_dir_all(logs_dir).map_err(|e| e.to_string())?;
    let mut deleted = 0usize;
    let entries = std::fs::read_dir(logs_dir).map_err(|e| e.to_string())?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("log") {
            continue;
        }
        if std::fs::remove_file(&path).is_ok() {
            deleted += 1;
        }
    }
    Ok(PurgeLogsResult { ok: true, deleted })
}

/// Opens StreamElements Account → Channels so the user can copy Account ID + JWT.
#[tauri::command]
pub async fn open_se_account_page(
    app: AppHandle,
    state: State<'_, AppState>,
    flow: String,
) -> Result<(), String> {
    if !flow.starts_with("ssl_")
        || flow.len() < 40
        || !flow.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err("invalid_login_flow".into());
    }
    let _guard = SE_IMPORT_WINDOW.lock().map_err(|e| e.to_string())?;

    if let Some(w) = app.get_webview_window("se-import") {
        let _ = w.close();
    }

    let initialization_script = format!(
        "window.__STREAMSYNC_OVERLAY_PORT__={};window.__STREAMSYNC_SE_FLOW__={};\n{}",
        state.overlay_port,
        serde_json::to_string(&flow).map_err(|e| e.to_string())?,
        include_str!("../../../streamelements-auth-inject.js")
    );
    WebviewWindowBuilder::new(
        &app,
        "se-import",
        WebviewUrl::External(
            tauri::Url::parse("https://streamelements.com/dashboard/account/channels")
                .map_err(|e| e.to_string())?,
        ),
    )
    .title("StreamElements — Account / Channels")
    .inner_size(1100.0, 780.0)
    .initialization_script(&initialization_script)
    .build()
    .map_err(|e| e.to_string())?;

    Ok(())
}

/// Bundle user data into a ZIP and save via the system file picker.
#[tauri::command]
pub fn export_backup(state: State<'_, AppState>) -> Result<ExportBackupResult, String> {
    let paths = get_paths().map_err(|e| e.to_string())?;
    let logs_dir = legacy_user_data_dir().join("logs");
    let zip_bytes = build_backup_zip(&paths, Some(&logs_dir)).map_err(|e| e.to_string())?;

    let default_name = format!(
        "stream-sync-backup-{}.zip",
        chrono::Utc::now().format("%Y-%m-%d")
    );

    let dest = rfd::FileDialog::new()
        .set_title("Save Stream Sync backup")
        .set_file_name(&default_name)
        .add_filter("Zip archive", &["zip"])
        .save_file();

    let Some(dest) = dest else {
        return Ok(ExportBackupResult {
            ok: false,
            cancelled: true,
            path: None,
            bytes: None,
            error: None,
        });
    };

    let mut save_path = dest;
    if save_path.extension().is_none() {
        save_path.set_extension("zip");
    }

    std::fs::write(&save_path, &zip_bytes).map_err(|e| e.to_string())?;

    let _ = &state;

    Ok(ExportBackupResult {
        ok: true,
        cancelled: false,
        path: Some(save_path.display().to_string()),
        bytes: Some(zip_bytes.len()),
        error: None,
    })
}

#[tauri::command]
pub fn check_for_updates(app: AppHandle) -> Result<serde_json::Value, String> {
    let secret = std::env::var("STREAMSYNC_UPDATE_SECRET")
        .or_else(|_| std::env::var("STREAM_SYNC_UPDATE_SECRET"))
        .unwrap_or_default();
    if secret.is_empty() {
        return Ok(serde_json::json!({ "ok": false, "error": "missing-secret" }));
    }

    let update_page = std::env::var("STREAMSYNC_UPDATE_PAGE")
        .unwrap_or_else(|_| "https://syndicateai.net/update".to_string());

    let payload = serde_json::json!({
        "app": "stream-sync",
        "v": app.package_info().version.to_string(),
        "ts": chrono::Utc::now().timestamp_millis(),
        "nonce": uuid::Uuid::new_v4().to_string(),
    });

    let p = base64_url_json(&payload);
    let sig = sign_hmac_sha256(&secret, &p);

    let mut url = reqwest::Url::parse(&update_page).map_err(|e| e.to_string())?;
    url.query_pairs_mut()
        .append_pair("p", &p)
        .append_pair("sig", &sig);

    app.opener()
        .open_url(url.as_str(), None::<&str>)
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "ok": true }))
}

fn base64_url_json(value: &serde_json::Value) -> String {
    use base64::Engine;
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn sign_hmac_sha256(secret: &str, msg: &str) -> String {
    hmac_sha256_hex(secret.as_bytes(), msg.as_bytes())
}

fn hmac_sha256_hex(key: &[u8], msg: &[u8]) -> String {
    // minimal: use reqwest doesn't help. Add `hmac` and `sha2` to workspace
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    let bytes = mac.finalize().into_bytes();
    hex::encode(bytes)
}
