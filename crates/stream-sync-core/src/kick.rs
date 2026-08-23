//! Kick OAuth (via Syndicate), chat send, and Syndicate webhook feed relay.

use crate::app_state::{live_kick_tokens, AppState};
use crate::broadcast::make_platform_dock_event;
use crate::config_types::{DelegatedSessionFile, KickTokenFile};
use crate::syndicate_connection::{self, ExchangeSuccess};
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tracing::{info, warn};

const KICK_CHAT_URL: &str = "https://api.kick.com/public/v1/chat";

pub fn apply_kick_to_delegated(session: &mut DelegatedSessionFile, exchange: &ExchangeSuccess) {
    match kick_from_exchange(exchange) {
        Some(k) => {
            session.kick_id = k.kick_id.clone();
            session.kick_login = k.login.clone();
            session.kick_access_token = k.access_token.clone();
            session.kick_refresh_token = k.refresh_token.clone();
            session.kick_expires_at = k.expires_at.clone();
            session.kick_scopes = k.scopes.clone().unwrap_or_default();
        }
        None => {
            session.kick_id = None;
            session.kick_login = None;
            session.kick_access_token = None;
            session.kick_refresh_token = None;
            session.kick_expires_at = None;
            session.kick_scopes = Vec::new();
        }
    }
}

fn kick_from_exchange(exchange: &ExchangeSuccess) -> Option<KickTokenFile> {
    let k = exchange.kick.as_ref()?;
    let access = k.access_token.as_ref().filter(|s| !s.is_empty())?;
    let kick_id = k.kick_id.as_ref().filter(|s| !s.is_empty())?;
    Some(KickTokenFile {
        access_token: Some(access.clone()),
        refresh_token: k.refresh_token.clone(),
        expires_at: k.expires_at.clone(),
        kick_id: Some(kick_id.clone()),
        login: k.login.clone(),
        display_name: k.login.clone(),
        scopes: if k.scopes.is_empty() {
            None
        } else {
            Some(k.scopes.clone())
        },
        feed_ticket: None,
    })
}

pub fn auth_url(state: &AppState, flow_nonce: &str) -> String {
    let return_url = format!(
        "http://localhost:{}/auth/kick/callback?flow={}",
        state.port,
        urlencoding::encode(flow_nonce)
    );
    format!(
        "{}/api/auth/kick/stream-sync?return={}",
        syndicate_connection::api_base(),
        urlencoding::encode(&return_url)
    )
}

pub async fn sync_live_identity(state: Arc<AppState>) {
    let mode = *state.active_mode.read().await;
    let delegated = state.delegated.read().await.clone();
    let personal = state.personal_kick.read().await.clone();
    let live = live_kick_tokens(mode, delegated.as_ref(), &personal);
    {
        let mut k = state.kick.write().await;
        k.tokens = live;
        if !k.tokens.is_linked() {
            k.connected = false;
        }
    }
    restart_feed(state).await;
}

async fn restart_feed(state: Arc<AppState>) {
    if let Some(h) = state.kick_feed_handle.write().await.take() {
        h.abort();
    }
    let linked = state.kick.read().await.tokens.is_linked();
    if !linked {
        state.kick.write().await.connected = false;
        return;
    }
    let state2 = state.clone();
    let handle = tokio::spawn(async move {
        feed_loop(state2).await;
    });
    *state.kick_feed_handle.write().await = Some(handle);
}

pub async fn maybe_autostart(state: Arc<AppState>) {
    sync_live_identity(state).await;
}

pub async fn apply_personal_bundle(state: Arc<AppState>, tokens: KickTokenFile) -> Result<()> {
    if !tokens.is_linked() {
        return Err(anyhow!("Kick bundle missing access token or user id"));
    }
    *state.personal_kick.write().await = tokens;
    state.save_kick_tokens().await?;
    sync_live_identity(state).await;
    Ok(())
}

pub async fn disconnect_personal(state: Arc<AppState>) -> Result<()> {
    *state.personal_kick.write().await = KickTokenFile::default();
    state.save_kick_tokens().await?;
    sync_live_identity(state).await;
    Ok(())
}

pub async fn redeem_stream_sync_code(state: Arc<AppState>, code: &str) -> Result<KickTokenFile> {
    let url = format!(
        "{}/api/auth/kick/stream-sync/redeem",
        syndicate_connection::api_base()
    );
    let res = reqwest::Client::new()
        .post(&url)
        .json(&json!({ "code": code }))
        .send()
        .await
        .map_err(|e| anyhow!("Kick redeem failed: {e}"))?;
    let status = res.status();
    let body: Value = res.json().await.unwrap_or_else(|_| json!({}));
    if !status.is_success() || body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let err = body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("redeem_failed");
        return Err(anyhow!("Kick redeem failed: {err}"));
    }
    let tokens = kick_file_from_redeem(&body)?;
    apply_personal_bundle(state, tokens.clone()).await?;
    Ok(tokens)
}

fn kick_file_from_redeem(body: &Value) -> Result<KickTokenFile> {
    let kick = body
        .get("kick")
        .ok_or_else(|| anyhow!("missing kick blob"))?;
    let access = kick
        .get("access_token")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing access_token"))?;
    let kick_id = kick
        .get("kick_id")
        .and_then(|v| {
            v.as_str()
                .map(|s| s.to_string())
                .or_else(|| v.as_i64().map(|n| n.to_string()))
        })
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing kick_id"))?;
    let scopes = kick.get("scopes").and_then(|v| v.as_array()).map(|arr| {
        arr.iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>()
    });
    Ok(KickTokenFile {
        access_token: Some(access.to_string()),
        refresh_token: kick
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        expires_at: kick
            .get("expires_at")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        kick_id: Some(kick_id),
        login: kick
            .get("login")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        display_name: kick
            .get("display_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        scopes,
        feed_ticket: body
            .get("feed_ticket")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
    })
}

async fn ensure_fresh_personal_token(state: &AppState) -> Result<()> {
    if state.is_delegated_mode().await {
        return Ok(());
    }
    let tokens = state.kick.read().await.tokens.clone();
    let refresh = tokens.refresh_token.clone().unwrap_or_default();
    if refresh.is_empty() {
        return Ok(());
    }
    let expiring = tokens
        .expires_at
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|exp| exp.signed_duration_since(chrono::Utc::now()).num_seconds() < 120)
        .unwrap_or(true);
    if !expiring {
        return Ok(());
    }
    let url = format!(
        "{}/api/auth/kick/stream-sync/refresh",
        syndicate_connection::api_base()
    );
    let res = reqwest::Client::new()
        .post(&url)
        .json(&json!({ "refresh_token": refresh }))
        .send()
        .await?;
    if !res.status().is_success() {
        return Err(anyhow!("Kick token refresh failed"));
    }
    let body: Value = res.json().await.unwrap_or_else(|_| json!({}));
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(anyhow!("Kick token refresh rejected"));
    }
    let tokens = kick_file_from_redeem(&body)?;
    *state.personal_kick.write().await = tokens.clone();
    state.save_kick_tokens().await?;
    {
        let mut k = state.kick.write().await;
        k.tokens = tokens;
    }
    Ok(())
}

pub async fn send_chat_from_dock(state: Arc<AppState>, message: &str) -> Result<()> {
    let trimmed = message.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    ensure_fresh_personal_token(&state).await.ok();
    let tokens = state.kick.read().await.tokens.clone();
    let access = tokens
        .access_token
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("Kick is not connected"))?;
    let kick_id = tokens
        .kick_id
        .clone()
        .ok_or_else(|| anyhow!("Kick is not connected"))?;
    let broadcaster_user_id: i64 = kick_id.parse().unwrap_or(0);
    let res = reqwest::Client::new()
        .post(KICK_CHAT_URL)
        .header("Authorization", format!("Bearer {access}"))
        .header("Content-Type", "application/json")
        .json(&json!({
            "content": trimmed.chars().take(500).collect::<String>(),
            "type": "user",
            "broadcaster_user_id": broadcaster_user_id,
        }))
        .send()
        .await?;
    if !res.status().is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Kick chat send failed: {text}"));
    }
    Ok(())
}

async fn feed_url(state: &AppState) -> Option<String> {
    let delegated_mode = state.is_delegated_mode().await;
    if delegated_mode {
        let d = state.delegated.read().await;
        if let Some(sess) = d.as_ref() {
            if sess.kick_id.as_ref().is_some_and(|s| !s.is_empty())
                && !sess.connection_key.is_empty()
            {
                return Some(format!(
                    "{}/api/stream-sync/kick-feed?key={}",
                    syndicate_connection::api_base(),
                    urlencoding::encode(&sess.connection_key)
                ));
            }
        }
    }
    let tokens = state.kick.read().await.tokens.clone();
    let ticket = tokens.feed_ticket.filter(|s| !s.is_empty())?;
    Some(format!(
        "{}/api/stream-sync/kick-feed?ticket={}",
        syndicate_connection::api_base(),
        urlencoding::encode(&ticket)
    ))
}

async fn feed_loop(state: Arc<AppState>) {
    loop {
        let Some(url) = feed_url(&state).await else {
            state.kick.write().await.connected = false;
            tokio::time::sleep(Duration::from_secs(8)).await;
            continue;
        };
        match consume_sse(state.clone(), &url).await {
            Ok(()) => info!("Kick feed SSE ended"),
            Err(e) => warn!("Kick feed SSE error: {e:#}"),
        }
        state.kick.write().await.connected = false;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn consume_sse(state: Arc<AppState>, url: &str) -> Result<()> {
    let res = reqwest::Client::new()
        .get(url)
        .header("Accept", "text/event-stream")
        .send()
        .await?;
    if !res.status().is_success() {
        return Err(anyhow!("Kick feed HTTP {}", res.status()));
    }
    state.kick.write().await.connected = true;
    let mut stream = res.bytes_stream();
    let mut buf = String::new();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        buf.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(idx) = buf.find("\n\n") {
            let frame = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            if let Some(event) = parse_sse_data(&frame) {
                fanout_kick_event(&state, event).await;
            }
        }
    }
    Ok(())
}

fn parse_sse_data(frame: &str) -> Option<Value> {
    let mut data = String::new();
    for line in frame.lines() {
        let line = line.trim_end();
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(rest.trim_start());
        }
    }
    if data.is_empty() {
        return None;
    }
    serde_json::from_str(&data).ok()
}

async fn fanout_kick_event(state: &AppState, raw: Value) {
    if raw.get("type").and_then(|v| v.as_str()) == Some("chat") {
        let message_id = raw
            .get("message_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !message_id.is_empty() && mark_kick_message_seen(&message_id) {
            return;
        }
        state.feed.broadcast_all(&normalize_kick_chat(&raw)).await;
        return;
    }
    let kind = raw
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if kind.is_empty() {
        return;
    }
    let user = raw
        .get("user_login")
        .and_then(|v| v.as_str())
        .unwrap_or("someone");
    match kind.as_str() {
        "follow" => {
            emit_alert(state, "follow", json!({ "name": user })).await;
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "follow",
                    &format!("{user} followed"),
                    Some("Follow"),
                    None,
                ))
                .await;
        }
        "sub" => {
            emit_alert(state, "sub", json!({ "name": user, "amount": "1000" })).await;
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "sub",
                    &format!("{user} subscribed"),
                    Some("Sub"),
                    None,
                ))
                .await;
        }
        "sub_gift" => {
            let count = raw.get("count").cloned().unwrap_or(json!(1));
            emit_alert(state, "gift", json!({ "name": user, "amount": count })).await;
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "gift",
                    &format!("{user} gifted {count}"),
                    Some("Gift"),
                    None,
                ))
                .await;
        }
        "kicks" => {
            let amount = raw.get("amount").cloned().unwrap_or(json!(0));
            emit_alert(state, "bits", json!({ "name": user, "amount": amount })).await;
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "kicks",
                    &format!("{user} gifted {amount} Kicks"),
                    Some("Kicks"),
                    None,
                ))
                .await;
        }
        "redemption" => {
            let title = raw
                .get("title")
                .and_then(|v| v.as_str())
                .unwrap_or("Reward");
            let input = raw.get("user_input").and_then(|v| v.as_str()).unwrap_or("");
            let mut detail = format!("{title} — {user}");
            if !input.is_empty() {
                detail.push_str(&format!(": {input}"));
            }
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "redeem",
                    &detail,
                    Some("Reward"),
                    None,
                ))
                .await;
        }
        "stream" => {
            let label = raw.get("label").and_then(|v| v.as_str()).unwrap_or("live");
            state
                .feed
                .broadcast_all(&make_platform_dock_event(
                    "kick",
                    "announce",
                    &format!("Kick stream {label}"),
                    Some("Live"),
                    None,
                ))
                .await;
        }
        _ => {}
    }
}

async fn emit_alert(state: &AppState, event_type: &str, variables: Value) {
    state
        .feed
        .broadcast_all(&json!({
            "type": "event-alert",
            "platform": "kick",
            "eventType": event_type,
            "data": { "variables": variables },
        }))
        .await;
}

fn normalize_kick_chat(raw: &Value) -> Value {
    let user_name = raw
        .get("user")
        .and_then(|v| v.as_str())
        .or_else(|| raw.pointer("/user/name").and_then(|v| v.as_str()))
        .unwrap_or("");
    let color = raw
        .get("color")
        .or_else(|| raw.get("username_color"))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let kick_badges = kick_badges_from_raw(raw.get("badges"));
    let badges_raw = kick_badges_to_raw(&kick_badges);
    let message_id = raw
        .get("message_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    let ts = raw
        .get("created_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let mut user = json!({
        "name": user_name,
        "displayName": user_name,
        "kickBadges": kick_badges,
        "badgesRaw": badges_raw,
    });
    if let Some(c) = color {
        user["color"] = json!(c);
    }
    let kick_emotes = raw.get("emotes").cloned().unwrap_or(json!([]));
    let mut evt = json!({
        "type": "chat",
        "platform": "kick",
        "ts": ts,
        "user": user,
        "message": raw.get("message").cloned().unwrap_or(json!("")),
        "kickEmotes": kick_emotes,
        "self": false,
    });
    if let Some(id) = message_id {
        evt["messageId"] = json!(id);
    }
    evt
}

fn kick_badges_from_raw(raw: Option<&Value>) -> Vec<Value> {
    let Some(arr) = raw.and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|b| {
            if let Some(t) = b.as_str() {
                return Some(json!({ "type": t, "text": t }));
            }
            let t = b.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if t.is_empty() {
                return None;
            }
            let mut badge = json!({ "type": t });
            if let Some(text) = b.get("text").and_then(|v| v.as_str()) {
                badge["text"] = json!(text);
            }
            if let Some(count) = b.get("count") {
                badge["count"] = count.clone();
            }
            Some(badge)
        })
        .collect()
}

fn kick_badges_to_raw(badges: &[Value]) -> Value {
    let mut map = Map::new();
    for b in badges {
        let t = b
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_lowercase();
        if t.is_empty() {
            continue;
        }
        map.insert(t, json!("1"));
    }
    Value::Object(map)
}

fn mark_kick_message_seen(message_id: &str) -> bool {
    static SEEN: OnceLock<Mutex<(HashSet<String>, VecDeque<String>)>> = OnceLock::new();
    let lock = SEEN.get_or_init(|| Mutex::new((HashSet::new(), VecDeque::new())));
    let mut guard = lock.lock().expect("kick message dedupe lock");
    if guard.0.contains(message_id) {
        return true;
    }
    guard.0.insert(message_id.to_string());
    guard.1.push_back(message_id.to_string());
    while guard.1.len() > 500 {
        if let Some(old) = guard.1.pop_front() {
            guard.0.remove(&old);
        }
    }
    false
}

#[derive(Debug, Deserialize)]
pub struct KickRedeemBody {
    pub code: String,
    #[serde(default, alias = "flowNonce")]
    pub flow_nonce: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::syndicate_connection::ExchangeKick;

    #[test]
    fn maps_chat_payload() {
        let raw = json!({
            "type": "chat",
            "platform": "kick",
            "user": "alice",
            "message": "hi",
            "message_id": "msg-1",
            "color": "#FF5733",
            "badges": [{ "type": "moderator", "text": "Moderator" }]
        });
        let out = normalize_kick_chat(&raw);
        assert_eq!(out["platform"], json!("kick"));
        assert_eq!(out["user"]["name"], json!("alice"));
        assert_eq!(out["user"]["color"], json!("#FF5733"));
        assert_eq!(out["messageId"], json!("msg-1"));
        assert_eq!(out["user"]["kickBadges"][0]["type"], json!("moderator"));
        assert_eq!(out["user"]["badgesRaw"]["moderator"], json!("1"));
    }

    #[test]
    fn dedupes_kick_message_ids() {
        assert!(!mark_kick_message_seen("dup-test-1"));
        assert!(mark_kick_message_seen("dup-test-1"));
    }

    #[test]
    fn apply_kick_copies_exchange() {
        let exchange = ExchangeSuccess {
            ok: true,
            channel: syndicate_connection::ExchangeChannel {
                twitch_id: "1".into(),
                login: "chan".into(),
                display_name: None,
            },
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: "cid".into(),
                access_token: "atok".into(),
                expires_at: "2099-01-01T00:00:00Z".into(),
                scopes: vec![],
            },
            kick: Some(ExchangeKick {
                kick_id: Some("99".into()),
                login: Some("kickchan".into()),
                access_token: Some("ktok".into()),
                refresh_token: Some("kref".into()),
                expires_at: Some("2099-01-01T00:00:00Z".into()),
                scopes: vec!["chat:write".into()],
                error: None,
            }),
            connection: syndicate_connection::ExchangeConnection {
                expires_at: None,
                label: None,
            },
        };
        let mut session = DelegatedSessionFile {
            connection_key: "ssk_x".into(),
            client_id: "cid".into(),
            access_token: "atok".into(),
            channel_login: "chan".into(),
            channel_twitch_id: "1".into(),
            twitch_expires_at: "2099-01-01T00:00:00Z".into(),
            ..Default::default()
        };
        apply_kick_to_delegated(&mut session, &exchange);
        assert_eq!(session.kick_id.as_deref(), Some("99"));
        assert_eq!(session.kick_access_token.as_deref(), Some("ktok"));
    }
}
