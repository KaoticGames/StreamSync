//! HTTP contract tests against a running overlay server (Node :4040 or Rust :4041).
//!
//! ```text
//! # Terminal A (reference)
//! npm start
//!
//! # Terminal B (Rust under test)
//! cargo run -p stream-sync-server
//!
//! # Terminal C
//! CONTRACT_BASE_URL=http://127.0.0.1:4041 cargo test -p stream-sync-core --test contract
//! ```

use serde_json::{json, Value};

fn base_url() -> String {
    std::env::var("CONTRACT_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:4041".into())
}

async fn get(path: &str) -> (u16, Value) {
    let url = format!("{}{}", base_url(), path);
    let res = reqwest::get(&url).await.expect("GET request");
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(json!(null));
    (status, body)
}

async fn post(path: &str, body: Value) -> (u16, Value) {
    let url = format!("{}{}", base_url(), path);
    let res = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .expect("POST request");
    let status = res.status().as_u16();
    let body: Value = res.json().await.unwrap_or(json!(null));
    (status, body)
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn health_contract() {
    let (status, body) = get("/health").await;
    assert_eq!(status, 200);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert_eq!(body.get("service"), Some(&json!("overlay-server")));
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn status_contract() {
    let (status, body) = get("/api/status").await;
    assert_eq!(status, 200);
    assert!(body.get("twitch").is_some());
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn chat_overlay_config_contract() {
    let (status, body) = get("/api/chat/overlay-config?profile=chat-default").await;
    assert_eq!(status, 200);
    assert_eq!(body.get("profileId"), Some(&json!("chat-default")));
    assert!(body.get("fontSize").is_some());
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn events_dock_config_contract() {
    let (status, body) = get("/api/events/dock-config").await;
    assert_eq!(status, 200);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert!(body.get("config").is_some());
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn overlay_profiles_contract() {
    let (status, body) = get("/api/chat/overlay-profiles").await;
    assert_eq!(status, 200);
    assert_eq!(body.get("ok"), Some(&json!(true)));
    assert!(body.get("profiles").and_then(|p| p.as_array()).is_some());
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn ws_ping_pong_contract() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;

    let url = format!(
        "{}/ws/feed?profile=chat-default",
        base_url().replace("http://", "ws://")
    );
    let (mut ws, _) = connect_async(&url).await.expect("ws connect");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        json!({ "type": "ping" }).to_string(),
    ))
    .await
    .unwrap();
    // Server may push events-dock-config before pong
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    let mut saw_pong = false;
    while std::time::Instant::now() < deadline {
        let msg = match tokio::time::timeout(std::time::Duration::from_millis(500), ws.next()).await
        {
            Ok(Some(Ok(m))) => m,
            _ => break,
        };
        if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).expect("json");
            if v.get("type").and_then(|x| x.as_str()) == Some("pong") {
                saw_pong = true;
                break;
            }
        }
    }
    assert!(saw_pong, "expected pong from /ws/feed");
}

#[tokio::test]
#[ignore = "requires running overlay server; set CONTRACT_BASE_URL"]
async fn dock_config_roundtrip_contract() {
    let payload = json!({
        "profileId": "chat-default",
        "fontSize": 14,
        "showBadges": true,
        "showTimestamps": true,
    });
    let (status, body) = post("/api/chat/dock-config", payload).await;
    assert_eq!(status, 200);
    assert_eq!(body.get("ok"), Some(&json!(true)));
}
