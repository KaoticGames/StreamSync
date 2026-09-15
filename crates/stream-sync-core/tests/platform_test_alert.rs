//! Integration tests for platform-aware Events Studio test-alert API.

use axum::http::{header, StatusCode};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use stream_sync_core::{KickTokenFile, OverlayConfig, OverlayServer, CONTROL_TOKEN_HEADER};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);
static TEST_SETUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_userdata_dir() -> PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "streamsync-platform-test-alert-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

async fn spawn_test_server_mode(
    readonly: bool,
) -> (
    u16,
    std::sync::Arc<stream_sync_core::AppState>,
    tokio::task::JoinHandle<()>,
) {
    let _guard = TEST_SETUP_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test port");
    let port = listener.local_addr().expect("test listener address").port();
    let userdata = test_userdata_dir();
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let config = OverlayConfig {
        port,
        repo_root,
        readonly,
        userdata_root: Some(userdata),
        secret_store: None,
    };
    let (router, state, _) = OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app");
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (port, state, handle)
}

fn trusted_origin(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

async fn connect_feed_ws(
    port: u16,
    profile: &str,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://127.0.0.1:{port}/ws/feed?profile={profile}");
    let mut req = url.into_client_request().unwrap();
    req.headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("feed connect");
    ws
}

async fn recv_json_of_type(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout_ms: u64,
    message_type: &str,
) -> Option<Value> {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let msg = match tokio::time::timeout(remaining, ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => return None,
        };
        if let Message::Text(t) = msg {
            if let Ok(v) = serde_json::from_str::<Value>(&t) {
                if v.get("type").and_then(|x| x.as_str()) == Some(message_type) {
                    return Some(v);
                }
            }
        }
    }
    None
}

async fn drain_feed_bootstrap(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let _ = recv_json_of_type(ws, 1000, "events-dock-config").await;
}

async fn post_test_alert_http(port: u16, control_token: &str, body: Value) -> (StatusCode, Value) {
    let client = reqwest::Client::new();
    let res = client
        .post(format!("http://127.0.0.1:{port}/api/events/test-alert"))
        .header(CONTROL_TOKEN_HEADER, control_token)
        .header(header::ORIGIN, trusted_origin(port))
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("post test-alert");
    let status = res.status();
    let json: Value = res.json().await.unwrap_or(json!(null));
    (status, json)
}

async fn seed_kick_linked(state: &std::sync::Arc<stream_sync_core::AppState>) {
    let tokens = KickTokenFile {
        access_token: Some("test-kick-token".into()),
        kick_id: Some("kick123".into()),
        login: Some("kickuser".into()),
        ..Default::default()
    };
    {
        let mut k = state.kick.write().await;
        k.tokens = tokens.clone();
    }
    *state.personal_kick.write().await = tokens;
    state.save_kick_tokens().await.expect("save kick tokens");
}

async fn seed_twitch_connected(state: &std::sync::Arc<stream_sync_core::AppState>) {
    state.twitch.write().await.connected = true;
}

async fn clear_platform_links(state: &std::sync::Arc<stream_sync_core::AppState>) {
    state.twitch.write().await.connected = false;
    state.kick.write().await.tokens = KickTokenFile::default();
}

#[tokio::test]
async fn kick_follow_emits_kick_platform() {
    let (port, state, handle) = spawn_test_server_mode(false).await;
    seed_kick_linked(&state).await;

    let mut ws = connect_feed_ws(port, "default").await;
    drain_feed_bootstrap(&mut ws).await;

    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({
            "eventType": "follow",
            "data": { "name": "KickFan" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["platform"], "kick");

    let alert = recv_json_of_type(&mut ws, 1000, "event-alert")
        .await
        .expect("event-alert");
    assert_eq!(alert["platform"], "kick");
    assert_eq!(alert["eventType"], "follow");

    let dock = recv_json_of_type(&mut ws, 1000, "dock-event")
        .await
        .expect("dock-event");
    assert_eq!(dock["platform"], "kick");
    assert_eq!(dock["eventType"], "follow");

    handle.abort();
}

#[tokio::test]
async fn twitch_dock_event_platform() {
    let (port, state, handle) = spawn_test_server_mode(false).await;
    seed_twitch_connected(&state).await;

    let mut ws = connect_feed_ws(port, "default").await;
    drain_feed_bootstrap(&mut ws).await;

    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({
            "eventType": "follow",
            "platform": "twitch",
            "data": { "name": "TwitchFan" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body["platform"], "twitch");

    let alert = recv_json_of_type(&mut ws, 1000, "event-alert")
        .await
        .expect("event-alert");
    assert_eq!(alert["platform"], "twitch");

    let dock = recv_json_of_type(&mut ws, 1000, "dock-event")
        .await
        .expect("dock-event");
    assert_eq!(dock["platform"], "twitch");

    handle.abort();
}

#[tokio::test]
async fn events_studio_platform_helper_is_served() {
    let (port, _state, handle) = spawn_test_server_mode(false).await;
    let client = reqwest::Client::new();

    let studio = client
        .get(format!("http://127.0.0.1:{port}/events-studio.html"))
        .send()
        .await
        .expect("get Events Studio");
    assert_eq!(studio.status(), StatusCode::OK);
    let html = studio.text().await.expect("read Events Studio HTML");
    assert!(
        html.contains("src=\"/overlay-server/events-studio-platform.js\""),
        "Events Studio must reference the bundled helper at its served path"
    );
    assert!(
        html.contains("id=\"testModeLive\" value=\"live\" disabled"),
        "Live mode must render disabled until connection status is known"
    );
    assert!(
        html.contains("id=\"testBtnLive\" disabled"),
        "Live button must render disabled until connection status is known"
    );

    let helper = client
        .get(format!(
            "http://127.0.0.1:{port}/overlay-server/events-studio-platform.js"
        ))
        .send()
        .await
        .expect("get platform helper");
    assert_eq!(helper.status(), StatusCode::OK);
    assert!(helper
        .text()
        .await
        .expect("read platform helper")
        .contains("computeTestPlatformUi"));

    handle.abort();
}

#[tokio::test]
async fn kick_kicks_and_redeem_match_production_shapes() {
    let (port, state, handle) = spawn_test_server_mode(false).await;
    seed_kick_linked(&state).await;

    let mut ws = connect_feed_ws(port, "default").await;
    drain_feed_bootstrap(&mut ws).await;
    let (status, _) = post_test_alert_http(
        port,
        state.control_token(),
        json!({
            "eventType": "kicks",
            "platform": "kick",
            "data": { "name": "KickFan", "amount": 50 }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let alert = recv_json_of_type(&mut ws, 1000, "event-alert")
        .await
        .expect("Kick Kicks overlay alert");
    assert_eq!(alert["platform"], "kick");
    assert_eq!(alert["eventType"], "cheer");
    assert_eq!(
        alert["data"]["variables"],
        json!({ "name": "KickFan", "amount": 50 })
    );
    let dock = recv_json_of_type(&mut ws, 1000, "dock-event")
        .await
        .expect("Kick Kicks dock event");
    assert_eq!(dock["platform"], "kick");
    assert_eq!(dock["eventType"], "kicks");
    assert_eq!(dock["label"], "Kicks");

    let mut ws_alert = connect_feed_ws(port, "default").await;
    let mut ws_dock = connect_feed_ws(port, "default").await;
    drain_feed_bootstrap(&mut ws_alert).await;
    drain_feed_bootstrap(&mut ws_dock).await;
    let (status, _) = post_test_alert_http(
        port,
        state.control_token(),
        json!({
            "eventType": "redeem",
            "platform": "kick",
            "data": { "name": "KickFan", "reward": "Test Reward", "input": "hello" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(recv_json_of_type(&mut ws_alert, 300, "event-alert")
        .await
        .is_none());
    let dock = recv_json_of_type(&mut ws_dock, 1000, "dock-event")
        .await
        .expect("Kick redeem dock event");
    assert_eq!(dock["eventType"], "redeem");

    handle.abort();
}

#[tokio::test]
async fn fail_closed() {
    let (port, state, handle) = spawn_test_server_mode(false).await;
    clear_platform_links(&state).await;

    let mut ws = connect_feed_ws(port, "default").await;
    drain_feed_bootstrap(&mut ws).await;

    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "follow" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "no_platform_connected");
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    seed_kick_linked(&state).await;
    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "follow", "platform": "unknown" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unknown_platform");
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "follow", "platform": 42 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unknown_platform");
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    seed_twitch_connected(&state).await;
    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "follow" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "platform_required");
    assert!(recv_json_of_type(&mut ws, 200, "event-alert")
        .await
        .is_none());
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    clear_platform_links(&state).await;
    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "follow", "platform": "kick" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "platform_not_connected");
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    seed_kick_linked(&state).await;
    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "raid", "platform": "kick" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unsupported_event_for_platform");
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    clear_platform_links(&state).await;
    seed_twitch_connected(&state).await;
    let (status, body) = post_test_alert_http(
        port,
        state.control_token(),
        json!({ "eventType": "bits", "platform": "twitch" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"], "unsupported_event_for_platform");
    assert!(recv_json_of_type(&mut ws, 200, "event-alert")
        .await
        .is_none());
    assert!(recv_json_of_type(&mut ws, 200, "dock-event")
        .await
        .is_none());

    handle.abort();
}
