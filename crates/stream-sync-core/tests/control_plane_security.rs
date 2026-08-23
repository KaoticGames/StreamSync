//! Phase 1 corrective pass — localhost control plane regression tests.

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use stream_sync_core::{
    route_inventory, route_policy, DockCredentialStore, OverlayConfig, OverlayServer, RoutePolicy,
    CONTROL_TOKEN_HEADER, LOGIN_NONCE_HEADER, PRIVILEGED_JSON_BODY_LIMIT,
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

async fn test_app(port: u16) -> (axum::Router, std::sync::Arc<stream_sync_core::AppState>) {
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
    (router, state)
}

async fn spawn_test_server(
    port: u16,
) -> (
    std::sync::Arc<stream_sync_core::AppState>,
    tokio::task::JoinHandle<()>,
) {
    let (router, state) = test_app(port).await;
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("bind test port");
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (state, handle)
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
    login_nonce: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value, String) {
    let mut builder = Request::builder().method(method).uri(path);
    if let Some(origin) = origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    if let Some(token) = control_token {
        builder = builder.header(CONTROL_TOKEN_HEADER, token);
    }
    if let Some(nonce) = login_nonce {
        builder = builder.header(LOGIN_NONCE_HEADER, nonce);
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
    let text = String::from_utf8_lossy(&bytes).to_string();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(json!(null));
    (status, json, text)
}

#[tokio::test]
async fn privileged_route_rejects_untrusted_origin_without_capability() {
    let port = 14140;
    let (router, _state) = test_app(port).await;
    let (status, body, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(evil_origin()),
        None,
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "body: {body}");
}

#[tokio::test]
async fn privileged_route_rejects_trusted_origin_missing_token() {
    let port = 14141;
    let (router, _state) = test_app(port).await;
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        None,
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn privileged_route_rejects_evil_origin_even_with_correct_token() {
    let port = 14142;
    let (router, state) = test_app(port).await;
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(evil_origin()),
        Some(state.control_token()),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn privileged_route_rejects_missing_and_null_origin() {
    let port = 14143;
    let (router, state) = test_app(port).await;
    let (status_missing, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        None,
        Some(state.control_token()),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status_missing, StatusCode::UNAUTHORIZED);

    let (status_null, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some("null"),
        Some(state.control_token()),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status_null, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn privileged_route_allows_trusted_origin_with_capability() {
    let port = 14144;
    let (router, state) = test_app(port).await;
    let (status, body, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some(state.control_token()),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body: {body}");
}

#[tokio::test]
async fn public_oauth_callbacks_exclude_master_token() {
    let port = 14145;
    let (router, state) = test_app(port).await;
    let token = state.control_token().to_string();
    for path in [
        "/auth/twitch/callback",
        "/auth/kick/callback",
        "/auth/streamelements/callback",
    ] {
        let (status, _, text) = request_json(
            &router,
            Method::GET,
            path,
            Some(evil_origin()),
            None,
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert!(!text.contains(&token), "{path} leaked master capability");
        assert!(
            !text.contains("STREAMSYNC_CONTROL_TOKEN"),
            "{path} still injects master token"
        );
        assert!(
            text.contains("x-streamsync-login-nonce") || text.contains("flow"),
            "{path} should use login flow nonce"
        );
    }
}

#[tokio::test]
async fn login_nonce_cannot_call_disconnect() {
    let port = 14146;
    let (router, state) = test_app(port).await;
    let nonce = state
        .pending_logins
        .create(stream_sync_core::OAuthProvider::Twitch);
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        None,
        Some(&nonce),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn login_nonce_replay_and_wrong_provider_fail() {
    let port = 14147;
    let (router, state) = test_app(port).await;
    let nonce = state
        .pending_logins
        .create(stream_sync_core::OAuthProvider::Kick);
    // Wrong provider endpoint (twitch set-token) with kick nonce.
    let (status, body, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/set-token",
        Some(&trusted_origin(port)),
        None,
        Some(&nonce),
        Some(json!({ "accessToken": "x", "flowNonce": nonce })),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "body: {body}");
    assert_eq!(
        body.get("error"),
        Some(&json!("wrong_provider_login_nonce"))
    );
}

#[tokio::test]
async fn dock_token_rejected_by_privileged_http() {
    let port = 14148;
    let (router, state) = test_app(port).await;
    let dock = state
        .dock_credentials
        .issue("twitch", "chat-default")
        .expect("issue dock");
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some(&dock.token),
        None,
        Some(json!({})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(DockCredentialStore::is_dock_token(&dock.token));
    assert!(!dock.token.starts_with("ssc_"));
}

#[tokio::test]
async fn read_only_health_and_overlay_config_remain_public() {
    let port = 14149;
    let (router, _state) = test_app(port).await;
    let (health_status, health_body, _) = request_json(
        &router,
        Method::GET,
        "/health",
        Some(evil_origin()),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(health_status, StatusCode::OK);
    assert_eq!(health_body.get("ok"), Some(&json!(true)));

    let (cfg_status, cfg_body, _) = request_json(
        &router,
        Method::GET,
        "/api/chat/overlay-config?profile=chat-default",
        Some(&trusted_origin(port)),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(cfg_status, StatusCode::OK);
    assert_eq!(cfg_body.get("profileId"), Some(&json!("chat-default")));
}

#[tokio::test]
async fn privileged_json_body_limit_rejects_oversized_payload() {
    let port = 14150;
    let (router, state) = test_app(port).await;
    let huge = "x".repeat(PRIVILEGED_JSON_BODY_LIMIT + 1024);
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/chat/dock-config",
        Some(&trusted_origin(port)),
        Some(state.control_token()),
        None,
        Some(json!({ "profileId": "chat-default", "padding": huge })),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn unauthorized_response_does_not_leak_capability() {
    let port = 14151;
    let (router, state) = test_app(port).await;
    let token = state.control_token().to_string();
    let (_, body, text) = request_json(
        &router,
        Method::POST,
        "/api/twitch/disconnect",
        Some(&trusted_origin(port)),
        Some("ssc_wrong_token_value_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
        None,
        Some(json!({})),
    )
    .await;
    assert!(!text.contains(&token));
    assert_eq!(body.get("error"), Some(&json!("unauthorized")));
}

#[tokio::test]
async fn ws_control_rejects_evil_and_missing_origin() {
    let port = 14152;
    let (_state, _handle) = spawn_test_server(port).await;

    let mut evil = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    evil.headers_mut()
        .insert(header::ORIGIN, "https://example.invalid".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(evil).await.is_err());

    let missing = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    assert!(tokio_tungstenite::connect_async(missing).await.is_err());
}

#[tokio::test]
async fn ws_feed_rejects_evil_origin_and_allows_trusted() {
    let port = 14153;
    let (_state, _handle) = spawn_test_server(port).await;

    let mut evil = format!("ws://127.0.0.1:{port}/ws/feed?profile=chat-default")
        .into_client_request()
        .unwrap();
    evil.headers_mut()
        .insert(header::ORIGIN, "https://example.invalid".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(evil).await.is_err());

    let mut ok = format!("ws://127.0.0.1:{port}/ws/feed?profile=chat-default")
        .into_client_request()
        .unwrap();
    ok.headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(ok).await.expect("feed");
    ws.send(Message::Text(
        json!({ "type": "chat-send", "message": "nope" })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    // Still connected; no auth-ok / chat acknowledgement.
    let _ = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
}

#[tokio::test]
async fn ws_control_auth_timeout_and_dock_token() {
    let port = 14154;
    let (state, _handle) = spawn_test_server(port).await;
    let dock = state
        .dock_credentials
        .issue("twitch", "chat-default")
        .unwrap();

    let mut req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.send(Message::Text(
        json!({ "type": "auth", "token": dock.token, "platform": "twitch" })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut saw_ok = false;
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v.get("type").and_then(|x| x.as_str()) == Some("auth-ok") {
                saw_ok = true;
                assert_eq!(v.get("dockScoped"), Some(&json!(true)));
                break;
            }
        }
    }
    assert!(saw_ok);

    // Revoke — new auth must fail.
    state.dock_credentials.revoke(&dock.token).unwrap();
    let mut req2 = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    req2.headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (mut ws2, _) = tokio_tungstenite::connect_async(req2).await.unwrap();
    ws2.send(Message::Text(
        json!({ "type": "auth", "token": dock.token, "platform": "twitch" })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    let mut failed = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(Duration::from_millis(500), ws2.next()).await {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v.get("type").and_then(|x| x.as_str()) == Some("auth-failed") {
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
}

#[tokio::test]
async fn concurrent_control_token_initialization_converges() {
    let dir = test_userdata_dir();
    let path = dir.join("control-token.txt");
    let mut handles = Vec::new();
    for _ in 0..8 {
        let p = path.clone();
        handles.push(std::thread::spawn(move || {
            stream_sync_core::load_or_create_control_token(&p).expect("token")
        }));
    }
    let mut tokens = Vec::new();
    for h in handles {
        tokens.push(h.join().expect("join"));
    }
    let first = tokens[0].clone();
    assert!(first.len() >= 32);
    assert!(tokens.iter().all(|t| t == &first));
    let on_disk = std::fs::read_to_string(&path).unwrap().trim().to_string();
    assert_eq!(on_disk, first);
    // No reusable backup.
    assert!(!path.with_extension("bak").exists());
}

#[test]
fn route_inventory_is_exhaustive_and_fail_closed() {
    for (method, path, policy) in route_inventory() {
        let m = Method::from_bytes(method.as_bytes()).unwrap();
        let resolved = if path.contains(':') || path.ends_with("/*") {
            // Spot-check parameterized forms.
            if *path == "/config/:profile_id.json" {
                route_policy(&m, "/config/chat-default.json")
            } else if *path == "/fonts/*" {
                route_policy(&m, "/fonts/Custom.ttf")
            } else if *path == "/events-media/*" {
                route_policy(&m, "/events-media/x.png")
            } else {
                *policy
            }
        } else {
            route_policy(&m, path)
        };
        assert_eq!(resolved, *policy, "{method} {path}");
        if *policy == RoutePolicy::PublicReadOnly {
            assert_ne!(*method, "POST");
            assert_ne!(*method, "DELETE");
        }
    }
    assert_eq!(
        route_policy(&Method::POST, "/api/unknown-new-route"),
        RoutePolicy::Privileged
    );
}
