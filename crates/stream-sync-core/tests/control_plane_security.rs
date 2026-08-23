//! Phase 1 — localhost control plane regression tests.

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use stream_sync_core::{
    OverlayConfig, OverlayServer, CONTROL_TOKEN_HEADER, PRIVILEGED_JSON_BODY_LIMIT,
};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);
static TEST_SETUP_LOCK: Mutex<()> = Mutex::new(());

fn test_userdata_dir() -> PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("streamsync-control-plane-test-{n}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

async fn test_app(port: u16) -> (axum::Router, String) {
    let _guard = TEST_SETUP_LOCK.lock().expect("test setup lock");
    let userdata = test_userdata_dir();
    std::env::set_var("STREAMSYNC_USERDATA", userdata.display().to_string());
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf();
    let config = OverlayConfig {
        port,
        repo_root,
        readonly: true,
    };
    let (router, state, _) = OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app");
    (router, state.control_token().to_string())
}

async fn spawn_test_server(port: u16) -> (String, tokio::task::JoinHandle<()>) {
    let (router, token) = test_app(port).await;
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("bind test port");
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (token, handle)
}

fn trusted_origin(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn evil_origin() -> &'static str {
    "https://example.invalid"
}

async fn request_json(
    router: &axum::Router,
    method: Method,
    path: &str,
    origin: Option<&str>,
    control_token: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(origin) = origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    if let Some(token) = control_token {
        builder = builder.header(CONTROL_TOKEN_HEADER, token);
    }
    let req = if let Some(body) = body {
        builder
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .expect("request body")
    } else {
        builder.body(Body::empty()).expect("request")
    };

    let response = router.clone().oneshot(req).await.expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, json)
}

#[tokio::test]
async fn privileged_route_rejects_untrusted_origin_without_capability() {
    let port = 14040;
    let (router, _token) = test_app(port).await;

    let (status, body) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(evil_origin()),
        None,
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED, "body: {body}");
    assert_eq!(body.get("error"), Some(&json!("unauthorized")));
}

#[tokio::test]
async fn privileged_route_rejects_wrong_capability() {
    let port = 14041;
    let (router, _token) = test_app(port).await;

    let (status, body) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some("ssc_wrong_token_value"),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED, "body: {body}");
    assert_eq!(body.get("error"), Some(&json!("unauthorized")));
}

#[tokio::test]
async fn privileged_route_allows_trusted_origin_with_capability() {
    let port = 14042;
    let (router, token) = test_app(port).await;

    let (status, body) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some(&token),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "body: {body}");
    assert_eq!(body.get("ok"), Some(&json!(true)));
}

#[tokio::test]
async fn read_only_health_and_overlay_config_remain_public() {
    let port = 14043;
    let (router, _token) = test_app(port).await;

    let (health_status, health_body) = request_json(
        &router,
        Method::GET,
        "/health",
        Some(evil_origin()),
        None,
        None,
    )
    .await;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(health_body.get("ok"), Some(&json!(true)));

    let (cfg_status, cfg_body) = request_json(
        &router,
        Method::GET,
        "/api/chat/overlay-config?profile=chat-default",
        Some(&trusted_origin(port)),
        None,
        None,
    )
    .await;
    assert_eq!(cfg_status, StatusCode::OK);
    assert_eq!(cfg_body.get("profileId"), Some(&json!("chat-default")));
}

#[tokio::test]
async fn privileged_json_body_limit_rejects_oversized_payload() {
    let port = 14045;
    let (router, token) = test_app(port).await;
    let huge = "x".repeat(PRIVILEGED_JSON_BODY_LIMIT + 1024);
    let (status, _body) = request_json(
        &router,
        Method::POST,
        "/api/chat/dock-config",
        Some(&trusted_origin(port)),
        Some(&token),
        Some(json!({ "profileId": "chat-default", "padding": huge })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn unauthorized_response_does_not_leak_capability() {
    let port = 14046;
    let (router, token) = test_app(port).await;

    let (status, body) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some("ssc_wrong_token_value"),
        Some(json!({})),
    )
    .await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let body_text = body.to_string();
    assert!(!body_text.contains(&token));
}

#[tokio::test]
async fn ws_control_rejects_evil_origin_upgrade() {
    let port = 14047;
    let (_token, _handle) = spawn_test_server(port).await;
    let mut req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .expect("ws request");
    req.headers_mut().insert(
        header::ORIGIN,
        "https://example.invalid".parse().expect("origin"),
    );
    let result = tokio_tungstenite::connect_async(req).await;
    assert!(result.is_err(), "evil origin must not upgrade /ws/control");
}

#[tokio::test]
async fn ws_feed_ignores_chat_send() {
    let port = 14048;
    let (_token, _handle) = spawn_test_server(port).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!(
        "ws://127.0.0.1:{port}/ws/feed?profile=chat-default"
    ))
    .await
    .expect("feed connect");

    ws.send(Message::Text(
        json!({ "type": "chat-send", "message": "hello" })
            .to_string()
            .into(),
    ))
    .await
    .expect("send chat-send");

    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_millis(200), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).expect("json");
            let ty = v.get("type").and_then(|x| x.as_str()).unwrap_or("");
            assert_ne!(ty, "auth-ok", "feed must not authenticate chat-send");
            assert_ne!(ty, "chat-sent", "feed must not acknowledge chat-send");
        }
    }
}

#[tokio::test]
async fn ws_control_authenticates_with_capability() {
    let port = 14049;
    let (token, _handle) = spawn_test_server(port).await;
    let mut req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .expect("ws request");
    req.headers_mut().insert(
        header::ORIGIN,
        trusted_origin(port).parse().expect("origin"),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("control connect");

    ws.send(Message::Text(
        json!({ "type": "auth", "token": token }).to_string().into(),
    ))
    .await
    .expect("auth send");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_auth_ok = false;
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).expect("json");
            if v.get("type").and_then(|x| x.as_str()) == Some("auth-ok") {
                saw_auth_ok = true;
                break;
            }
        }
    }
    assert!(saw_auth_ok, "expected auth-ok from /ws/control");
}

#[tokio::test]
async fn ws_control_rejects_wrong_capability() {
    let port = 14050;
    let (_token, _handle) = spawn_test_server(port).await;
    let mut req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .expect("ws request");
    req.headers_mut().insert(
        header::ORIGIN,
        trusted_origin(port).parse().expect("origin"),
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .expect("control connect");

    ws.send(Message::Text(
        json!({ "type": "auth", "token": "ssc_wrong_token_value" })
            .to_string()
            .into(),
    ))
    .await
    .expect("auth send");

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_auth_failed = false;
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).expect("json");
            if v.get("type").and_then(|x| x.as_str()) == Some("auth-failed") {
                saw_auth_failed = true;
                break;
            }
        }
    }
    assert!(saw_auth_failed, "expected auth-failed for wrong capability");
}
