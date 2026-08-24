mod commands;
mod overlay;
mod overlay_proxy;
mod paths;

use commands::AppState;
use overlay::{spawn_overlay_server, wait_for_health};
use paths::{legacy_user_data_dir, resolve_ui_assets_root};
use std::time::Duration;
use stream_sync_core::rust_workspace_root;
use tauri::{
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    Manager, WebviewUrl, WebviewWindowBuilder, WindowEvent,
};
use tracing_subscriber::EnvFilter;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("stream_sync_desktop_lib=info".parse().unwrap()),
        )
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_shell::init())
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
            let logs_dir = user_data.join("logs");
            app.manage(AppState {
                overlay_port: port,
                logs_dir: logs_dir.clone(),
            });

            spawn_overlay_server(ui_root.clone(), port);

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

            let handle2 = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let ready = wait_for_health(port, Duration::from_secs(30)).await;
                if !ready {
                    tracing::error!(
                        "overlay server did not become healthy on port {port} — open http://127.0.0.1:{port}/health"
                    );
                }

                let url = overlay::shell_url(port);
                tracing::info!("opening UI at {url}");

                if WebviewWindowBuilder::new(
                    &handle2,
                    "main",
                    WebviewUrl::External(url.parse().expect("shell url")),
                )
                .title("Stream Sync")
                .inner_size(1280.0, 800.0)
                .min_inner_size(1024.0, 640.0)
                // Keep wry defaults + allow media autoplay (LiveKit / OBS overlays).
                .additional_browser_args(
                    "--disable-features=msWebOOUI,msPdfOOUI,msSmartScreenProtection --autoplay-policy=no-user-gesture-required",
                )
                .initialization_script(include_str!("../../../tauri-init.js"))
                .build()
                .is_err()
                {
                    tracing::error!("failed to create main window");
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
            commands::check_for_updates,
            commands::open_se_account_page,
            commands::export_backup,
        ])
        .run(tauri::generate_context!())
        .expect("error while running Stream Sync desktop");
}
