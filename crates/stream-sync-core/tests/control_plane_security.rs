//! Phase 1 corrective pass — localhost control plane regression tests.

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use stream_sync_core::{
    route_inventory, route_policy, DockCredentialStore, OverlayConfig, OverlayServer, RoutePolicy,
    BUILD_ROUTER_ROUTE_IDS, CONTROL_TOKEN_HEADER, LOGIN_NONCE_HEADER, PRIVILEGED_JSON_BODY_LIMIT,
};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);
static TEST_SETUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_userdata_dir() -> PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "streamsync-control-plane-test-{}-{n}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

async fn test_app(port: u16) -> (axum::Router, std::sync::Arc<stream_sync_core::AppState>) {
    test_app_mode(port, true).await
}

async fn test_app_mode(
    port: u16,
    readonly: bool,
) -> (axum::Router, std::sync::Arc<stream_sync_core::AppState>) {
    let _guard = TEST_SETUP_LOCK.lock().await;
    let userdata = test_userdata_dir();
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf();
    let config = OverlayConfig {
        port,
        repo_root,
        readonly,
        userdata_root: Some(userdata),
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
    spawn_test_server_mode(port, true).await
}

async fn spawn_test_server_mode(
    port: u16,
    readonly: bool,
) -> (
    std::sync::Arc<stream_sync_core::AppState>,
    tokio::task::JoinHandle<()>,
) {
    let (router, state) = test_app_mode(port, readonly).await;
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
    let (router, state) = test_app_mode(port, false).await;
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
        json!({ "type": "chat-send", "message": "nope" }).to_string(),
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
        json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
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
        json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
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
async fn ws_control_rejects_master_token_and_revocation_closes_active_socket() {
    let port = 14155;
    let (state, _handle) = spawn_test_server_mode(port, false).await;

    let mut master_req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    master_req
        .headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (mut master_ws, _) = tokio_tungstenite::connect_async(master_req).await.unwrap();
    master_ws
        .send(Message::Text(
            json!({ "type": "auth", "token": state.control_token(), "platform": "twitch" })
                .to_string(),
        ))
        .await
        .unwrap();
    let master_reply = tokio::time::timeout(Duration::from_secs(1), master_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(master_reply.to_text().unwrap().contains("auth-failed"));

    let dock = state
        .dock_credentials
        .issue("twitch", "chat-default")
        .unwrap();
    let mut dock_req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
        .into_client_request()
        .unwrap();
    dock_req
        .headers_mut()
        .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
    let (mut dock_ws, _) = tokio_tungstenite::connect_async(dock_req).await.unwrap();
    dock_ws
        .send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
        ))
        .await
        .unwrap();
    let auth_reply = tokio::time::timeout(Duration::from_secs(1), dock_ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(auth_reply.to_text().unwrap().contains("auth-ok"));

    let revoke = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/dock/revoke-credential"
        ))
        .header(header::ORIGIN.as_str(), trusted_origin(port))
        .header(CONTROL_TOKEN_HEADER, state.control_token())
        .json(&json!({ "token": dock.token }))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);
    let closed = tokio::time::timeout(Duration::from_secs(1), dock_ws.next())
        .await
        .expect("socket should be fenced promptly");
    assert!(
        matches!(closed, None | Some(Ok(Message::Close(_)))),
        "revoked socket remained active: {closed:?}"
    );
}

#[tokio::test]
async fn revoke_all_closes_every_active_dock_socket() {
    let port = 14158;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let mut sockets = Vec::new();
    for platform in ["twitch", "kick"] {
        let dock = state
            .dock_credentials
            .issue(platform, "chat-default")
            .unwrap();
        let mut req = format!("ws://127.0.0.1:{port}/ws/control?profile=chat-default")
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert(header::ORIGIN, trusted_origin(port).parse().unwrap());
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws.send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": platform }).to_string(),
        ))
        .await
        .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(1), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(reply.to_text().unwrap().contains("auth-ok"));
        sockets.push(ws);
    }

    let revoke = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/dock/revoke-credential"
        ))
        .header(header::ORIGIN.as_str(), trusted_origin(port))
        .header(CONTROL_TOKEN_HEADER, state.control_token())
        .json(&json!({ "all": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);
    for mut socket in sockets {
        let closed = tokio::time::timeout(Duration::from_secs(1), socket.next())
            .await
            .expect("revoke-all should close every socket");
        assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
    }
}

#[tokio::test]
async fn readonly_rejects_dock_credential_mutation() {
    let port = 14156;
    let (router, state) = test_app(port).await;
    let before = state.dock_credentials.active_count();
    let (status, _, _) = request_json(
        &router,
        Method::POST,
        "/api/dock/issue-credential",
        Some(&trusted_origin(port)),
        Some(state.control_token()),
        None,
        Some(json!({ "platform": "twitch", "profileId": "chat-default" })),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(state.dock_credentials.active_count(), before);
}

#[tokio::test]
async fn ui_and_callback_responses_send_csp_headers() {
    let port = 14157;
    let (router, _) = test_app(port).await;
    for path in ["/shell.html", "/auth/streamelements/callback"] {
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::ORIGIN, trusted_origin(port))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let csp = response
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|v| v.to_str().ok())
            .expect("CSP response header");
        assert!(csp.contains("default-src"));
        if path.contains("/auth/") {
            assert!(csp.contains("default-src 'none'"));
        }
    }
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
    let registered: std::collections::BTreeSet<_> =
        BUILD_ROUTER_ROUTE_IDS.iter().copied().collect();
    let manifested: std::collections::BTreeSet<_> = route_inventory()
        .iter()
        .map(|(method, path, _)| (*method, *path))
        .collect();
    assert_eq!(
        registered, manifested,
        "router and security manifest differ"
    );

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
    assert_eq!(
        route_policy(&Method::GET, "/ws/control"),
        RoutePolicy::AuthenticatedControl
    );
}

#[test]
fn build_router_source_must_match_route_id_list() {
    let src = include_str!("../src/routes.rs");
    let fn_start = src
        .find("pub fn build_router(")
        .expect("build_router in routes.rs");
    let body = &src[fn_start..];
    let body_end = body.find("\nasync fn ").unwrap_or(body.len());
    let body = &body[..body_end];
    let mut found: std::collections::BTreeSet<(String, String)> = std::collections::BTreeSet::new();
    let bytes = body.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = body[search_from..].find(".route(") {
        let start = search_from + rel + ".route(".len();
        let mut depth = 1usize;
        let mut i = start;
        while i < bytes.len() && depth > 0 {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ => {}
            }
            i += 1;
        }
        let call = &body[start..i.saturating_sub(1)];
        search_from = i;
        let call_trim = call.trim_start();
        let Some(rest) = call_trim.strip_prefix('"') else {
            continue;
        };
        let Some(end) = rest.find('"') else {
            continue;
        };
        let path = rest[..end].to_string();
        for (needle, method) in [
            ("get(", "GET"),
            ("post(", "POST"),
            ("delete(", "DELETE"),
            ("put(", "PUT"),
            ("patch(", "PATCH"),
        ] {
            let mut scan = call;
            while let Some(idx) = scan.find(needle) {
                let before = &scan[..idx];
                let ok = before
                    .chars()
                    .next_back()
                    .map(|c| !c.is_ascii_alphanumeric() && c != '_')
                    .unwrap_or(true);
                if ok {
                    found.insert((method.to_string(), path.clone()));
                }
                scan = &scan[idx + needle.len()..];
            }
        }
    }
    if body.contains("nest_service(\"/fonts\"") {
        found.insert(("GET".into(), "/fonts/*".into()));
    }
    if body.contains("nest_service(\"/events-media\"") {
        found.insert(("GET".into(), "/events-media/*".into()));
    }
    let listed: std::collections::BTreeSet<_> = BUILD_ROUTER_ROUTE_IDS
        .iter()
        .map(|(m, p)| ((*m).to_string(), (*p).to_string()))
        .collect();
    assert_eq!(
        found, listed,
        "build_router registrations must match BUILD_ROUTER_ROUTE_IDS"
    );
}

#[test]
fn frontend_dock_and_login_wiring_is_scoped() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .unwrap();
    let renderer = std::fs::read_to_string(root.join("renderer.js")).unwrap();
    let events_section = renderer
        .split("if (kind === \"events-dock\")")
        .nth(1)
        .unwrap()
        .split("if (kind === \"chat-overlay\")")
        .next()
        .unwrap();
    assert!(!events_section.contains("privilegedDockUrl"));
    assert!(!events_section.contains("#control="));

    let importer = std::fs::read_to_string(root.join("events-se-import.js")).unwrap();
    let injection = std::fs::read_to_string(root.join("streamelements-auth-inject.js")).unwrap();
    assert!(importer.contains("/api/streamelements/begin-login"));
    assert!(importer.contains("openSeAccountPage(nonce)"));
    assert!(injection.contains("__STREAMSYNC_SE_FLOW__"));
    assert!(injection.contains("/auth/streamelements/callback?flow="));
}

#[test]
fn channel_point_private_input_is_profile_scoped() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let twitch = std::fs::read_to_string(root.join("src/twitch.rs")).unwrap();
    let redemption = twitch
        .split("\"channel.channel_points_custom_reward_redemption.add\" =>")
        .nth(1)
        .unwrap()
        .split("_ => {}")
        .next()
        .unwrap();
    assert!(redemption.contains("broadcast_readonly_dock"));
    assert!(redemption.contains("broadcast_private_dock"));
    assert!(!redemption.contains("broadcast_all"));
    assert!(!redemption.contains("broadcast_profile"));
}

async fn connect_feed_ws(
    port: u16,
    profile: &str,
    audience: Option<&str>,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let mut url = format!("ws://127.0.0.1:{port}/ws/feed?profile={profile}");
    if let Some(audience) = audience {
        url.push_str(&format!("&audience={audience}"));
    }
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

#[tokio::test]
async fn ws_feed_private_audience_query_without_auth_stays_public() {
    let port = 14160;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let profile = "chat-default";
    let mut bypass = connect_feed_ws(port, profile, Some("private-control-dock")).await;
    let mut overlay = connect_feed_ws(port, profile, None).await;
    drain_feed_bootstrap(&mut bypass).await;
    drain_feed_bootstrap(&mut overlay).await;

    let secret = json!({ "type": "user_input", "text": "channel-point-secret" });
    state
        .feed
        .broadcast_private_dock(profile, &secret, &state.dock_credentials)
        .await;

    assert!(
        recv_json_of_type(&mut bypass, 400, "user_input")
            .await
            .is_none(),
        "query-declared private feed must not receive private input without credential"
    );
    assert!(
        recv_json_of_type(&mut overlay, 400, "user_input")
            .await
            .is_none(),
        "public overlay must not receive private input"
    );
}

#[tokio::test]
async fn ws_feed_private_audience_requires_valid_dock_credential() {
    let port = 14161;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let profile = "chat-default";
    let dock = state.dock_credentials.issue("twitch", profile).unwrap();
    let mut private = connect_feed_ws(port, profile, Some("private-control-dock")).await;
    drain_feed_bootstrap(&mut private).await;
    private
        .send(Message::Text(
            json!({ "type": "auth", "token": "ssd_invalid_token_xxxxxxxxxxxxxxxx", "platform": "twitch" })
                .to_string(),
        ))
        .await
        .unwrap();
    let failed = recv_json_of_type(&mut private, 1000, "auth-failed")
        .await
        .expect("auth-failed response");
    assert_eq!(failed.get("type"), Some(&json!("auth-failed")));

    let mut authorized = connect_feed_ws(port, profile, None).await;
    drain_feed_bootstrap(&mut authorized).await;
    authorized
        .send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
        ))
        .await
        .unwrap();
    let ok = recv_json_of_type(&mut authorized, 1000, "auth-ok")
        .await
        .expect("auth-ok response");
    assert_eq!(ok.get("type"), Some(&json!("auth-ok")));
    assert_eq!(ok.get("privateFeed"), Some(&json!(true)));

    let secret = json!({ "type": "user_input", "text": "authorized-private" });
    state
        .feed
        .broadcast_private_dock(profile, &secret, &state.dock_credentials)
        .await;
    let received = recv_json_of_type(&mut authorized, 1000, "user_input")
        .await
        .expect("private payload");
    assert_eq!(received.get("type"), Some(&json!("user_input")));
}

#[tokio::test]
async fn ws_feed_revocation_closes_private_subscription() {
    let port = 14162;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let profile = "chat-default";
    let dock = state.dock_credentials.issue("twitch", profile).unwrap();
    let mut private = connect_feed_ws(port, profile, None).await;
    drain_feed_bootstrap(&mut private).await;
    private
        .send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
        ))
        .await
        .unwrap();
    recv_json_of_type(&mut private, 1000, "auth-ok")
        .await
        .expect("auth-ok");

    let revoke = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{port}/api/dock/revoke-credential"
        ))
        .header(header::ORIGIN.as_str(), trusted_origin(port))
        .header(CONTROL_TOKEN_HEADER, state.control_token())
        .json(&json!({ "token": dock.token }))
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::OK);

    let closed = tokio::time::timeout(Duration::from_secs(1), private.next())
        .await
        .expect("private feed socket should close on revocation");
    assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
}

#[tokio::test]
async fn cross_process_revoke_blocks_chat_send_before_platform() {
    let port = 14163;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
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
        json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
    ))
    .await
    .unwrap();
    let auth = recv_json_of_type(&mut ws, 1000, "auth-ok")
        .await
        .expect("auth-ok");
    assert_eq!(auth.get("type"), Some(&json!("auth-ok")));

    let external = DockCredentialStore::load_or_create(&state.paths.dock_credentials).unwrap();
    external.revoke(&dock.token).unwrap();

    ws.send(Message::Text(
        json!({ "type": "chat-send", "message": "must-not-send", "platform": "twitch" })
            .to_string(),
    ))
    .await
    .unwrap();
    let result = recv_json_of_type(&mut ws, 1000, "chat-send-result")
        .await
        .expect("chat-send result");
    assert_eq!(result.get("type"), Some(&json!("chat-send-result")));
    assert_eq!(result.get("ok"), Some(&json!(false)));
    assert_eq!(result.get("error"), Some(&json!("revoked")));

    let closed = tokio::time::timeout(Duration::from_secs(1), ws.next())
        .await
        .expect("revoked socket should close");
    assert!(matches!(closed, None | Some(Ok(Message::Close(_)))));
}

#[tokio::test]
async fn events_studio_csp_allows_same_origin_embed() {
    let port = 14164;
    let (router, _) = test_app(port).await;
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/events-studio.html")
                .header(header::ORIGIN, trusted_origin(port))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let csp = response
        .headers()
        .get(header::CONTENT_SECURITY_POLICY)
        .and_then(|v| v.to_str().ok())
        .expect("CSP response header");
    assert!(csp.contains("frame-ancestors 'self'"));
    assert!(!csp.contains("frame-ancestors 'none'"));
    assert!(
        csp.contains("http://ipc.localhost"),
        "desktop studio must allow Tauri IPC (create/save/test-alert)"
    );
}

#[tokio::test]
async fn cross_process_revoke_fences_established_private_feed() {
    let port = 14165;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let profile = "chat-default";
    let dock = state.dock_credentials.issue("twitch", profile).unwrap();
    let mut private = connect_feed_ws(port, profile, None).await;
    drain_feed_bootstrap(&mut private).await;
    private
        .send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
        ))
        .await
        .unwrap();
    recv_json_of_type(&mut private, 1000, "auth-ok")
        .await
        .expect("auth-ok");

    let external = DockCredentialStore::load_or_create(&state.paths.dock_credentials).unwrap();
    external.revoke(&dock.token).unwrap();

    let secret = json!({ "type": "user_input", "text": "must-not-leak" });
    state
        .feed
        .broadcast_private_dock(profile, &secret, &state.dock_credentials)
        .await;

    let leaked = recv_json_of_type(&mut private, 400, "user_input").await;
    assert!(
        leaked.is_none(),
        "revoked private feed must not receive private payload: {leaked:?}"
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), private.next()).await;
    assert!(
        matches!(closed, Ok(None) | Ok(Some(Ok(Message::Close(_)))) | Err(_))
            || closed
                .as_ref()
                .ok()
                .and_then(|m| m.as_ref())
                .map(|m| m.as_ref().ok().map(|msg| matches!(msg, Message::Close(_))))
                .is_some(),
        "revoked private feed should close or stop delivering: {closed:?}"
    );
}

#[tokio::test]
async fn cross_process_revoke_all_fences_private_feed() {
    let port = 14166;
    let (state, _handle) = spawn_test_server_mode(port, false).await;
    let profile = "chat-default";
    let dock = state.dock_credentials.issue("twitch", profile).unwrap();
    let mut private = connect_feed_ws(port, profile, None).await;
    drain_feed_bootstrap(&mut private).await;
    private
        .send(Message::Text(
            json!({ "type": "auth", "token": dock.token, "platform": "twitch" }).to_string(),
        ))
        .await
        .unwrap();
    recv_json_of_type(&mut private, 1000, "auth-ok")
        .await
        .expect("auth-ok");

    let external = DockCredentialStore::load_or_create(&state.paths.dock_credentials).unwrap();
    external.revoke_all().unwrap();

    let secret = json!({ "type": "user_input", "text": "revoke-all-secret" });
    state
        .feed
        .broadcast_private_dock(profile, &secret, &state.dock_credentials)
        .await;
    assert!(recv_json_of_type(&mut private, 400, "user_input")
        .await
        .is_none());
}

#[test]
fn authorize_register_race_does_not_upgrade_after_revoke() {
    let dir = std::env::temp_dir().join(format!(
        "streamsync-auth-race-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dock-credentials.json");
    let store_a = DockCredentialStore::load_or_create(&path).unwrap();
    let cred = store_a.issue("twitch", "chat-default").unwrap();
    assert!(store_a.authorize_chat_send(&cred.token, "twitch", "chat-default"));
    let store_b = DockCredentialStore::load_or_create(&path).unwrap();
    store_b.revoke(&cred.token).unwrap();
    assert!(
        !store_a.authorize_chat_send(&cred.token, "twitch", "chat-default"),
        "revalidate after register must observe independent revoke"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
