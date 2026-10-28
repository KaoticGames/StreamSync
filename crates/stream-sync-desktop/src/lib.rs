mod commands;
mod overlay;
mod overlay_proxy;
mod paths;
mod update_service;
mod update_state;
mod updater;

use commands::AppState;
use overlay::{spawn_overlay_server, startup_error_dialog, wait_for_expected_health, OverlayStart};
use paths::{legacy_user_data_dir, resolve_ui_assets_root};
use std::sync::Arc;
use std::time::Duration;
use stream_sync_core::rust_workspace_root;
use tauri::{
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    webview::PageLoadEvent,
    Emitter, Manager, WebviewUrl, WebviewWindowBuilder, WindowEvent,
};
use tracing_subscriber::EnvFilter;
use update_service::UpdateService;

#[allow(dead_code)]
struct InstanceLock(std::fs::File);

fn emit_startup_progress(app: &tauri::AppHandle, percent: u8, stage: &str) {
    let _ = app.emit(
        "startup-progress",
        serde_json::json!({
            "percent": percent.min(100),
            "stage": stage,
        }),
    );
}

fn close_startup_window(app: &tauri::AppHandle) {
    if let Some(startup) = app.get_webview_window("startup") {
        let _ = startup.close();
    }
}

fn install_tray(app: &tauri::AppHandle) -> tauri::Result<()> {
    let show = MenuItem::with_id(app, "show", "Show Stream Sync", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
    let tray_menu = Menu::with_items(app, &[&show, &quit])?;

    let _tray = TrayIconBuilder::new()
        .icon(app.default_window_icon().unwrap().clone())
        .menu(&tray_menu)
        .tooltip("Stream Sync")
        .on_menu_event(|app, event| match event.id.as_ref() {
            "show" => {
                if let Some(w) = app.get_webview_window("main") {
                    let _ = w.show();
                    let _ = w.set_focus();
                }
            }
            "quit" => {
                app.exit(0);
            }
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            } = event
            {
                let app = tray.app_handle();
                if let Some(w) = app.get_webview_window("main") {
                    if w.is_visible().unwrap_or(false) {
                        let _ = w.hide();
                    } else {
                        let _ = w.show();
                        let _ = w.set_focus();
                    }
                }
            }
        })
        .build(app)?;

    Ok(())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let logs_dir = legacy_user_data_dir().join("logs");
    if let Err(e) = stream_sync_core::init_tracing(&logs_dir) {
        eprintln!("failed to init file tracing: {e:#}");
        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive("stream_sync_desktop_lib=info".parse().unwrap()),
            )
            .init();
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .on_window_event(|window, event| {
            if window.label() != "main" {
                return;
            }
            if let WindowEvent::CloseRequested { api, .. } = event {
                let _ = window.hide();
                api.prevent_close();
            }
        })
        .setup(|app| {
            let handle = app.handle().clone();
            let rust_root = rust_workspace_root();
            let ui_root = resolve_ui_assets_root(&handle);

            let port = overlay::configure_environment(&handle, &rust_root, &ui_root);

            let user_data = legacy_user_data_dir();
            match stream_sync_core::try_acquire_instance_lock(&user_data) {
                Ok(lock_file) => {
                    app.manage(InstanceLock(lock_file));
                }
                Err(stream_sync_core::InstanceLockError::AlreadyHeld) => {
                    startup_error_dialog(
                        "Stream Sync is already running. Use the tray icon to show the window.",
                    );
                    std::process::exit(0);
                }
                Err(stream_sync_core::InstanceLockError::Io(e)) => {
                    startup_error_dialog(&format!(
                        "Stream Sync could not start because another copy may already be running.\n\n{e}"
                    ));
                    std::process::exit(1);
                }
            }

            WebviewWindowBuilder::new(
                &handle,
                "startup",
                WebviewUrl::App("index.html".into()),
            )
            .title("Stream Sync")
            .inner_size(420.0, 300.0)
            .resizable(false)
            .center()
            .always_on_top(true)
            .on_page_load(|window, payload| {
                if payload.event() == PageLoadEvent::Finished {
                    emit_startup_progress(window.app_handle(), 10, "Preparing data…");
                }
            })
            .build()?;

            let logs_dir = user_data.join("logs");
            app.manage(AppState {
                overlay_port: port,
                logs_dir: logs_dir.clone(),
            });
            let update_service = Arc::new(UpdateService::new(user_data.clone()));
            app.manage(update_service);

            let (instance_nonce, bind_rx) = spawn_overlay_server(ui_root.clone(), port);
            install_tray(&handle)?;
            emit_startup_progress(&handle, 30, "Starting local services…");

            let handle2 = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                emit_startup_progress(&handle2, 55, "Restoring platform connections…");
                let ready = tokio::select! {
                    start = bind_rx => {
                        match start.unwrap_or(OverlayStart::BindFailed {
                            detail: "overlay server failed to start".into(),
                        }) {
                            OverlayStart::AlreadyRunningSameApp => {
                                close_startup_window(&handle2);
                                startup_error_dialog(
                                    "Stream Sync is already running. Use the tray icon to show the window.",
                                );
                                false
                            }
                            OverlayStart::ForeignOccupant { detail } => {
                                close_startup_window(&handle2);
                                startup_error_dialog(&format!(
                                    "Port {port} is already in use by another program. Stream Sync will not attach to it.\n\n{detail}"
                                ));
                                false
                            }
                            OverlayStart::BindFailed { detail } => {
                                close_startup_window(&handle2);
                                startup_error_dialog(&format!(
                                    "Stream Sync could not start its overlay server.\n\n{detail}"
                                ));
                                false
                            }
                        }
                    }
                    ok = wait_for_expected_health(port, Some(&instance_nonce), Duration::from_secs(30)) => {
                        if !ok {
                            tracing::error!(
                                "overlay server did not become healthy on port {port} — open http://127.0.0.1:{port}/health"
                            );
                            close_startup_window(&handle2);
                            startup_error_dialog(&format!(
                                "Stream Sync started but the overlay server on port {port} did not identify itself. Check that nothing else is bound to that port."
                            ));
                        }
                        ok
                    }
                };

                if !ready {
                    handle2.exit(0);
                    return;
                }

                let url = overlay::shell_url(port);
                tracing::info!("opening UI at {url}");

                emit_startup_progress(&handle2, 80, "Loading interface…");

                let handoff = handle2.clone();
                if WebviewWindowBuilder::new(
                    &handle2,
                    "main",
                    WebviewUrl::External(url.parse().expect("shell url")),
                )
                .title("Stream Sync")
                .inner_size(1280.0, 800.0)
                .min_inner_size(1024.0, 640.0)
                .visible(false)
                // Keep wry defaults + allow media autoplay (LiveKit / OBS overlays).
                .additional_browser_args(
                    "--disable-features=msWebOOUI,msPdfOOUI,msSmartScreenProtection --autoplay-policy=no-user-gesture-required",
                )
                .initialization_script(include_str!("../../../tauri-init.js"))
                .on_page_load(move |window, payload| {
                    if payload.event() != PageLoadEvent::Finished {
                        return;
                    }
                    emit_startup_progress(window.app_handle(), 100, "Ready");
                    let _ = window.show();
                    let _ = window.set_focus();
                    if let Some(startup) = handoff.get_webview_window("startup") {
                        let _ = startup.close();
                    }
                })
                .build()
                .is_err()
                {
                    tracing::error!("failed to create main window");
                    close_startup_window(&handle2);
                    startup_error_dialog(
                        "Stream Sync could not open its main window. Try restarting the app.",
                    );
                    handle2.exit(1);
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_overlay_base_url,
            commands::get_overlay_port,
            commands::overlay_api_request,
            commands::overlay_media_upload,
            commands::open_external,
            commands::open_logs_folder,
            commands::open_discord,
            commands::twitch_connect,
            commands::twitch_reconnect,
            commands::twitch_disconnect,
            commands::kick_connect,
            commands::purge_logs,
            commands::get_update_status,
            commands::check_for_updates,
            commands::check_for_updates_manual,
            commands::check_for_updates_background,
            commands::begin_update_install,
            commands::dismiss_update,
            commands::open_update_fallback_page,
            commands::open_download_page,
            commands::open_se_account_page,
            commands::export_backup,
            commands::restore_backup,
        ])
        .run(tauri::generate_context!())
        .expect("error while running Stream Sync desktop");
}
