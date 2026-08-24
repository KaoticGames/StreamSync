//! Syndicate API client for Stream Sync connection keys (takeover).

use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::json;

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
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(&json!({ "key": key }))
        .send()
        .await
        .map_err(|e| anyhow!("Syndicate API request failed: {e}"))?;

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
    let message = body
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("Connection key request failed")
        .to_string();

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
        _ => err.message.clone(),
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
            let err = SyndicateApiError {
                code: code.into(),
                message: "raw".into(),
                http_status: 401,
            };
            let msg = user_message_for_error(&err);
            assert!(!msg.is_empty());
            assert_ne!(msg, "raw");
        }
    }

    #[test]
    fn parses_success_shape() {
        let body = json!({
            "ok": true,
            "channel": {
                "twitch_id": "123",
                "login": "channelname",
                "display_name": "ChannelName"
            },
            "twitch": {
                "client_id": "cid",
                "access_token": "atok",
                "expires_at": "2026-07-15T18:00:00.000Z",
                "scopes": ["chat:read", "chat:edit"]
            },
            "connection": {
                "expires_at": "2026-07-16T12:00:00.000Z",
                "label": "Saturday takeover"
            }
        });
        let parsed: ExchangeSuccess = serde_json::from_value(body).unwrap();
        assert_eq!(parsed.channel.login, "channelname");
        assert_eq!(parsed.twitch.client_id, "cid");
        assert_eq!(
            parsed.connection.label.as_deref(),
            Some("Saturday takeover")
        );
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
}
