//! Tauri commands — replaces Electron `electronAPI` / IPC.

use crate::overlay_proxy;
use crate::paths::legacy_user_data_dir;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Mutex;
use stream_sync_core::{build_backup_zip, get_paths, restore_backup_zip};
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
    pub kept: usize,
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

#[derive(Serialize)]
pub struct RestoreBackupResult {
    pub ok: bool,
    pub cancelled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files_written: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn require_main_window(window: &WebviewWindow, port: u16) -> Result<(), String> {
    overlay_proxy::validate_caller_window(window, port)
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
pub async fn overlay_media_upload(
    window: WebviewWindow,
    state: State<'_, AppState>,
    request: overlay_proxy::OverlayMediaUploadRequest,
) -> Result<overlay_proxy::OverlayApiResponse, String> {
    overlay_proxy::execute_overlay_media_upload(&window, state.overlay_port, request).await
}

#[tauri::command]
pub fn open_external(
    window: WebviewWindow,
    state: State<'_, AppState>,
    url: String,
) -> Result<(), String> {
    require_main_window(&window, state.overlay_port)?;
    let trimmed = url.trim();
    if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
        return Err("invalid_external_url".into());
    }
    open::that(trimmed).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn open_logs_folder(window: WebviewWindow, state: State<'_, AppState>) -> Result<(), String> {
    require_main_window(&window, state.overlay_port)?;
    let logs = &state.logs_dir;
    std::fs::create_dir_all(logs).map_err(|e| e.to_string())?;
    open::that(logs).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn pick_recording_folder(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<Option<String>, String> {
    require_main_window(&window, state.overlay_port)?;
    Ok(rfd::FileDialog::new()
        .set_title("Choose Discord recording folder")
        .set_parent(&window)
        .pick_folder()
        .map(|p| p.to_string_lossy().into_owned()))
}

#[tauri::command]
pub fn open_discord(
    window: WebviewWindow,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<(), String> {
    require_main_window(&window, state.overlay_port)?;
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
pub fn purge_logs(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<PurgeLogsResult, String> {
    require_main_window(&window, state.overlay_port)?;
    let report = stream_sync_core::purge_log_files(&state.logs_dir, chrono::Utc::now())
        .map_err(|e| e.to_string())?;
    Ok(PurgeLogsResult {
        ok: true,
        deleted: report.deleted,
        kept: report.kept,
    })
}

/// Opens StreamElements Account → Channels so the user can copy Account ID + JWT.
#[tauri::command]
pub async fn open_se_account_page(
    window: WebviewWindow,
    app: AppHandle,
    state: State<'_, AppState>,
    flow: String,
) -> Result<(), String> {
    require_main_window(&window, state.overlay_port)?;
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
pub fn export_backup(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<ExportBackupResult, String> {
    require_main_window(&window, state.overlay_port)?;
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

    Ok(ExportBackupResult {
        ok: true,
        cancelled: false,
        path: Some(save_path.display().to_string()),
        bytes: Some(zip_bytes.len()),
        error: None,
    })
}

/// Restore user data from a backup ZIP selected via system file picker.
#[tauri::command]
pub fn restore_backup(
    window: WebviewWindow,
    state: State<'_, AppState>,
) -> Result<RestoreBackupResult, String> {
    require_main_window(&window, state.overlay_port)?;
    let src = rfd::FileDialog::new()
        .set_title("Select Stream Sync backup to restore")
        .add_filter("Zip archive", &["zip"])
        .pick_file();

    let Some(src) = src else {
        return Ok(RestoreBackupResult {
            ok: false,
            cancelled: true,
            path: None,
            files_written: None,
            error: None,
        });
    };

    let zip_bytes = std::fs::read(&src).map_err(|e| e.to_string())?;
    let paths = get_paths().map_err(|e| e.to_string())?;
    let report = restore_backup_zip(&paths, &zip_bytes).map_err(|e| e.to_string())?;

    Ok(RestoreBackupResult {
        ok: true,
        cancelled: false,
        path: Some(src.display().to_string()),
        files_written: Some(report.files_written),
        error: None,
    })
}

#[tauri::command]
pub fn check_for_updates(
    window: WebviewWindow,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<serde_json::Value, String> {
    open_download_page(window, state, app)
}

/// There is no in-app updater. Open the public HTTPS download page.
#[tauri::command]
pub fn open_download_page(
    window: WebviewWindow,
    state: State<'_, AppState>,
    app: AppHandle,
) -> Result<serde_json::Value, String> {
    require_main_window(&window, state.overlay_port)?;
    let env_page = std::env::var("STREAMSYNC_UPDATE_PAGE").ok();
    let url = crate::updater::resolve_download_page(env_page.as_deref())?;
    app.opener()
        .open_url(&url, None::<&str>)
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "ok": true, "url": url }))
}
