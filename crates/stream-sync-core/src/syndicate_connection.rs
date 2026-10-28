//! Syndicate API client for Stream Sync connection keys (takeover).

use crate::delegated_lifecycle::{
    bound_remote_message, redact_connection_key, SYNDICATE_HTTP_TIMEOUT,
};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::json;
use std::sync::OnceLock;
use std::time::Duration;

const DEFAULT_API_BASE: &str = "https://api.syndicateai.net";

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeChannel {
    pub twitch_id: String,
    pub login: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeTwitch {
    pub client_id: String,
    pub access_token: String,
    pub expires_at: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConnection {
    pub expires_at: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeSuccess {
    #[serde(default)]
    #[allow(dead_code)]
    pub ok: bool,
    pub channel: ExchangeChannel,
    pub twitch: ExchangeTwitch,
    #[serde(default)]
    pub kick: Option<ExchangeKick>,
    pub connection: ExchangeConnection,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ExchangeKick {
    #[serde(default)]
    pub kick_id: Option<String>,
    #[serde(default)]
    pub login: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_at: Option<String>,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SyndicateApiError {
    pub code: String,
    pub message: String,
    #[allow(dead_code)]
    pub http_status: u16,
}

impl std::fmt::Display for SyndicateApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for SyndicateApiError {}

pub fn api_base() -> String {
    std::env::var("SYNDICATE_API_BASE")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_API_BASE.to_string())
}

pub fn syndicate_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(SYNDICATE_HTTP_TIMEOUT)
            .build()
            .expect("syndicate HTTP client")
    })
}

fn build_syndicate_streaming_http_client(connect_timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .build()
        .expect("syndicate streaming HTTP client")
}

pub fn syndicate_streaming_http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| build_syndicate_streaming_http_client(SYNDICATE_HTTP_TIMEOUT))
}

pub async fn exchange(key: &str) -> Result<ExchangeSuccess> {
    redeem(key, "exchange").await
}

pub async fn refresh(key: &str) -> Result<ExchangeSuccess> {
    redeem(key, "refresh").await
}

/// Events URL without embedding the connection key (use Authorization: Bearer instead).
pub fn connection_key_events_url() -> String {
    crate::delegated_lifecycle::connection_key_events_url(&api_base())
}

/// Kick feed URL without embedding the connection key (use Authorization: Bearer instead).
pub fn kick_feed_url() -> String {
    crate::delegated_lifecycle::kick_feed_url(&api_base())
}

async fn redeem(key: &str, action: &str) -> Result<ExchangeSuccess> {
    let key = key.trim();
    if key.is_empty() || !key.starts_with("ssk_") {
        return Err(SyndicateApiError {
            code: "invalid_key".into(),
            message: "Missing or malformed connection key.".into(),
            http_status: 401,
        }
        .into());
    }

    let url = format!("{}/api/stream-sync/connection-keys/{}", api_base(), action);
    let res = syndicate_http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&json!({ "key": key }))
        .send()
        .await
        .map_err(|e| {
            anyhow!(
                "Syndicate API request failed: {}",
                redact_connection_key(&e.to_string(), key)
            )
        })?;

    let status = res.status().as_u16();
    let body: serde_json::Value = res.json().await.unwrap_or_else(|_| json!({}));

    if status == 200 && body.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        let parsed: ExchangeSuccess = serde_json::from_value(body)
            .map_err(|e| anyhow!("Invalid Syndicate exchange response: {e}"))?;
        if parsed.twitch.client_id.is_empty() || parsed.twitch.access_token.is_empty() {
            return Err(anyhow!(
                "Syndicate exchange missing client_id or access_token"
            ));
        }
        return Ok(parsed);
    }

    let code = body
        .get("error")
        .and_then(|v| v.as_str())
        .unwrap_or(if status == 429 {
            "rate_limited"
        } else {
            "invalid_key"
        })
        .to_string();
    let raw_message = body
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("Connection key request failed");
    let message = redact_connection_key(&bound_remote_message(raw_message), key);

    Err(SyndicateApiError {
        code,
        message,
        http_status: status,
    }
    .into())
}

pub fn user_message_for_error(err: &SyndicateApiError) -> String {
    match err.code.as_str() {
        "invalid_key" => "That connection key is invalid. Ask the channel owner for a new key."
            .into(),
        "expired" => "That connection key has expired. Ask the channel owner for a new key.".into(),
        "revoked" => {
            "That connection key was revoked by the channel owner.".into()
        }
        "missing_scopes" => {
            "The channel owner must reconnect Twitch for Stream Sync on the Syndicate Dashboard first."
                .into()
        }
        "token_unavailable" => {
            "The channel owner's Twitch link is unavailable. Try again later or contact the owner."
                .into()
        }
        "rate_limited" => "Too many connection attempts. Wait a moment and try again.".into(),
        _ => "Connection key request failed. Try again or contact the channel owner.".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn user_messages_cover_handoff_codes() {
        for code in [
            "invalid_key",
            "expired",
            "revoked",
            "missing_scopes",
            "token_unavailable",
            "rate_limited",
        ] {
            let msg = user_message_for_error(&SyndicateApiError {
                code: code.into(),
                message: "internal".into(),
                http_status: 400,
            });
            assert!(!msg.is_empty());
            assert!(!msg.contains("internal"));
        }
    }

    #[test]
    fn parses_success_shape() {
        let body = json!({
            "ok": true,
            "channel": { "twitch_id": "1", "login": "chan" },
            "twitch": {
                "client_id": "cid",
                "access_token": "tok",
                "expires_at": "2030-01-01T00:00:00Z",
                "scopes": []
            },
            "connection": { "expires_at": "2030-01-01T00:00:00Z" }
        });
        let parsed: ExchangeSuccess = serde_json::from_value(body).unwrap();
        assert_eq!(parsed.channel.login, "chan");
    }

    #[tokio::test]
    async fn rejects_malformed_key_without_network() {
        let err = exchange("not-a-key").await.unwrap_err();
        let api = err.downcast_ref::<SyndicateApiError>().expect("api err");
        assert_eq!(api.code, "invalid_key");
    }

    #[test]
    fn events_url_does_not_include_connection_key() {
        let url = connection_key_events_url();
        assert!(url.ends_with("/api/stream-sync/connection-keys/events"));
        assert!(!url.contains("?key="));
        assert!(!url.contains("ssk_"));
    }

    #[test]
    fn kick_feed_url_does_not_include_connection_key() {
        let url = kick_feed_url();
        assert!(url.ends_with("/api/stream-sync/kick-feed"));
        assert!(!url.contains("?key="));
    }

    #[test]
    fn remote_error_redacts_submitted_key() {
        let key = "ssk_test_placeholder_not_a_real_key_abcdef";
        let raw = format!("Key {key} was rejected by upstream");
        let redacted = redact_connection_key(&bound_remote_message(&raw), key);
        assert!(!redacted.contains(key));
        assert!(redacted.contains("[redacted-connection-key]"));
    }

    /// Kick SSE stays open indefinitely; the syndicate streaming client must bound connect
    /// latency only, not apply a total request/body deadline like `syndicate_http_client`.
    #[tokio::test]
    async fn streaming_http_client_connect_timeout_does_not_cap_idle_sse_body() {
        use std::time::Duration;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const CONNECT_TIMEOUT: Duration = Duration::from_millis(50);
        const BODY_IDLE: Duration = Duration::from_millis(150);
        const READ_BUDGET: Duration = Duration::from_millis(500);

        async fn serve_delayed_sse(listener: &tokio::net::TcpListener) -> std::io::Result<()> {
            let (mut socket, _) = listener.accept().await?;
            let mut buf = [0u8; 2048];
            loop {
                let n = socket.read(&mut buf).await?;
                if n == 0 {
                    return Ok(());
                }
                if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let headers = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
            socket.write_all(headers).await?;
            tokio::time::sleep(BODY_IDLE).await;
            let payload = b"data: {\"type\":\"chat\",\"message\":\"after-idle\"}\n\n";
            let chunk = format!(
                "{:x}\r\n{}\r\n0\r\n\r\n",
                payload.len(),
                String::from_utf8_lossy(payload)
            );
            socket.write_all(chunk.as_bytes()).await?;
            Ok(())
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/events");
        tokio::spawn(async move {
            serve_delayed_sse(&listener).await.unwrap();
            serve_delayed_sse(&listener).await.unwrap();
        });

        let total_timeout_client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(CONNECT_TIMEOUT)
            .build()
            .expect("control client");
        let capped = tokio::time::timeout(READ_BUDGET, async {
            let response = total_timeout_client
                .get(&url)
                .header("Accept", "text/event-stream")
                .send()
                .await?;
            response.text().await
        })
        .await;
        assert!(
            capped.is_err() || capped.unwrap().is_err(),
            "total request timeout must abort an idle SSE body (control proves server timing)"
        );

        let streaming = build_syndicate_streaming_http_client(CONNECT_TIMEOUT);
        let res = tokio::time::timeout(
            READ_BUDGET,
            streaming
                .get(&url)
                .header("Accept", "text/event-stream")
                .send(),
        )
        .await
        .expect("streaming client must not hit total request timeout during idle body")
        .expect("streaming client send");
        assert!(res.status().is_success());
        let body = tokio::time::timeout(READ_BUDGET, res.text())
            .await
            .expect("streaming client must read body after idle")
            .expect("streaming body");
        assert!(
            body.contains("after-idle"),
            "expected SSE payload after connect-only timeout: {body}"
        );
    }
}
