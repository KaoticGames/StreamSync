//! Twitch OAuth, Helix, IRC chat, EventSub (port of overlay-server/server.js Twitch stack).

use crate::app_state::{tokens_from_delegated_session, AppState};
use crate::broadcast::{make_dock_event, FeedHub};
use crate::config_types::{DelegatedSessionFile, TwitchActiveMode, TwitchTokenFile};
use crate::syndicate_connection::{self, SyndicateApiError};
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};

type StreamSyncIrcClient = TwitchIRCClient<SecureTCPTransport, StaticLoginCredentials>;

static BADGE_TTL: Duration = Duration::from_secs(300);
static EMOTE_TTL: Duration = Duration::from_secs(300);
struct CacheEntry<T> {
    value: T,
    user_id: String,
    fetched_at: std::time::Instant,
}

pub struct TwitchServices {
    badge_cache: RwLock<Option<CacheEntry<Value>>>,
    emote_cache: RwLock<Option<CacheEntry<Vec<Value>>>>,
    irc_client: RwLock<Option<StreamSyncIrcClient>>,
    irc_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    eventsub_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    refresh_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    watch_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

impl TwitchServices {
    pub fn new() -> Self {
        Self {
            badge_cache: RwLock::new(None),
            emote_cache: RwLock::new(None),
            irc_client: RwLock::new(None),
            irc_handle: RwLock::new(None),
            eventsub_handle: RwLock::new(None),
            refresh_handle: RwLock::new(None),
            watch_handle: RwLock::new(None),
        }
    }
}

pub fn token_expired(tokens: &TwitchTokenFile) -> bool {
    if tokens.access_token.is_none() {
        return true;
    }
    let Some(ts) = tokens.obtainment_timestamp else {
        return false;
    };
    let Some(exp) = tokens.expires_in else {
        return false;
    };
    let age = (chrono::Utc::now().timestamp_millis() - ts) / 1000;
    age >= exp - 10
}

pub async fn ensure_valid_token(state: &AppState) -> Result<()> {
    let tw = state.twitch.read().await;
    if tw.tokens.access_token.is_none() {
        return Err(anyhow!(
            "No Twitch accessToken. Please connect Twitch first."
        ));
    }
    if token_expired(&tw.tokens) {
        return Err(anyhow!("Twitch token expired. Please reconnect Twitch."));
    }
    Ok(())
}

pub async fn validate_token(access_token: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let res = client
        .get("https://id.twitch.tv/oauth2/validate")
        .header("Authorization", format!("OAuth {access_token}"))
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Twitch validate failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

pub async fn helix_get(state: &AppState, path: &str) -> Result<Value> {
    ensure_valid_token(state).await?;
    let client_id = state.helix_client_id().await;
    if client_id.is_empty() {
        return Err(anyhow!("TWITCH_CLIENT_ID not configured."));
    }
    let token = state
        .twitch
        .read()
        .await
        .tokens
        .access_token
        .clone()
        .unwrap_or_default();
    let url = format!("https://api.twitch.tv/helix{path}");
    let res = reqwest::Client::new()
        .get(&url)
        .header("Client-Id", &client_id)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Helix GET {path} failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

pub async fn helix_patch(state: &AppState, path: &str, body: Value) -> Result<Value> {
    ensure_valid_token(state).await?;
    let client_id = state.helix_client_id().await;
    let token = state
        .twitch
        .read()
        .await
        .tokens
        .access_token
        .clone()
        .unwrap();
    let res = reqwest::Client::new()
        .patch(format!("https://api.twitch.tv/helix{path}"))
        .header("Client-Id", &client_id)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Helix PATCH failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

pub async fn helix_post(state: &AppState, path: &str, body: Value) -> Result<Value> {
    ensure_valid_token(state).await?;
    let client_id = state.helix_client_id().await;
    let token = state
        .twitch
        .read()
        .await
        .tokens
        .access_token
        .clone()
        .unwrap();
    let res = reqwest::Client::new()
        .post(format!("https://api.twitch.tv/helix{path}"))
        .header("Client-Id", &client_id)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await?;
    if res.status() == 409 {
        return Ok(json!({ "ok": true, "already": true }));
    }
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Helix POST failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

pub async fn update_chat_settings(state: &AppState, partial: Value) -> Result<()> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Missing userId"))?;
    let path = format!("/chat/settings?broadcaster_id={user_id}&moderator_id={user_id}");
    let _ = helix_patch(state, &path, partial).await?;
    Ok(())
}

pub async fn get_merged_badges(state: &AppState, services: &TwitchServices) -> Result<Value> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Connect Twitch first"))?;
    {
        let cache = services.badge_cache.read().await;
        if let Some(c) = cache.as_ref() {
            if c.user_id == user_id && c.fetched_at.elapsed() < BADGE_TTL {
                return Ok(c.value.clone());
            }
        }
    }
    let global = helix_get(state, "/chat/badges/global").await?;
    let channel = helix_get(state, &format!("/chat/badges?broadcaster_id={user_id}")).await?;
    let merged = merge_badge_sets(&global, &channel);
    let mut cache = services.badge_cache.write().await;
    *cache = Some(CacheEntry {
        value: merged.clone(),
        user_id,
        fetched_at: std::time::Instant::now(),
    });
    Ok(merged)
}

fn merge_badge_sets(global: &Value, channel: &Value) -> Value {
    let mut badge_sets = serde_json::Map::new();
    for source in [global, channel] {
        if let Some(data) = source.get("data").and_then(|d| d.as_array()) {
            for set in data {
                let set_id = set.get("set_id").and_then(|s| s.as_str()).unwrap_or("");
                if set_id.is_empty() {
                    continue;
                }
                let entry = badge_sets
                    .entry(set_id.to_string())
                    .or_insert_with(|| json!({ "versions": {} }));
                if let Some(versions) = set.get("versions").and_then(|v| v.as_array()) {
                    for v in versions {
                        if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                            entry["versions"][id] = v.clone();
                        }
                    }
                }
            }
        }
    }
    json!({ "badge_sets": badge_sets })
}

pub async fn get_merged_emotes(state: &AppState, services: &TwitchServices) -> Result<Vec<Value>> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Connect Twitch first"))?;
    {
        let cache = services.emote_cache.read().await;
        if let Some(c) = cache.as_ref() {
            if c.user_id == user_id && c.fetched_at.elapsed() < EMOTE_TTL {
                return Ok(c.value.clone());
            }
        }
    }
    let mut by_id: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    if let Ok(g) = helix_get(state, "/chat/emotes/global").await {
        add_emote_batch(&mut by_id, g.get("data"), Some("global"), None, &user_id);
    }
    if let Ok(c) = helix_get(state, &format!("/chat/emotes?broadcaster_id={user_id}")).await {
        add_emote_batch(
            &mut by_id,
            c.get("data"),
            Some("channel"),
            Some(&user_id),
            &user_id,
        );
    }
    let user_emotes = fetch_all_user_emotes(state, &user_id)
        .await
        .unwrap_or_default();
    // User emotes carry Helix `owner_id` for subscribed / followed channels.
    add_emote_batch(
        &mut by_id,
        Some(&Value::Array(user_emotes)),
        None,
        None,
        &user_id,
    );

    let mut list: Vec<Value> = by_id.into_values().collect();
    enrich_emote_owners(state, &mut list).await;

    let mut cache = services.emote_cache.write().await;
    *cache = Some(CacheEntry {
        value: list.clone(),
        user_id: user_id.clone(),
        fetched_at: std::time::Instant::now(),
    });
    Ok(list)
}

fn json_id_string(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if t.is_empty() || t == "0" {
            return None;
        }
        return Some(t.to_string());
    }
    if let Some(n) = v.as_u64() {
        if n == 0 {
            return None;
        }
        return Some(n.to_string());
    }
    if let Some(n) = v.as_i64() {
        if n <= 0 {
            return None;
        }
        return Some(n.to_string());
    }
    None
}

fn is_usable_owner_id(id: &str) -> bool {
    let t = id.trim();
    !t.is_empty() && t != "0"
}

fn add_emote_batch(
    by_id: &mut std::collections::HashMap<String, Value>,
    list: Option<&Value>,
    default_owner_type: Option<&str>,
    default_owner_id: Option<&str>,
    self_user_id: &str,
) {
    let Some(arr) = list.and_then(|v| v.as_array()) else {
        return;
    };
    for emote in arr {
        let Some(id) = emote.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        if by_id.contains_key(id) {
            continue;
        }

        let emote_type = emote.get("emote_type").and_then(|v| v.as_str());
        // Helix may return owner_id as "" or "0" for emotes without an owner — those
        // must not be queried via /users (they 400 the whole batch and wipe avatars).
        let owner_id = emote.get("owner_id").and_then(json_id_string).or_else(|| {
            default_owner_id
                .filter(|id| is_usable_owner_id(id))
                .map(|s| s.to_string())
        });

        let mut owner_type = default_owner_type.unwrap_or("unknown");
        if emote_type == Some("globals") {
            owner_type = "global";
        } else if matches!(
            emote_type,
            Some("subscriptions") | Some("bitstier") | Some("follower")
        ) {
            if owner_type == "unknown" {
                owner_type = "channel";
            }
        } else if owner_type == "unknown" && owner_id.is_some() {
            // Subscribed / followed channel emotes from /chat/emotes/user
            owner_type = "channel";
        }

        by_id.insert(
            id.to_string(),
            json!({
                "id": id,
                "name": emote.get("name"),
                "images": emote.get("images"),
                "emoteType": emote_type,
                "emoteSetId": emote.get("emote_set_id"),
                "ownerType": owner_type,
                "ownerId": owner_id,
                "ownerLogin": Value::Null,
                "ownerName": Value::Null,
                "ownerProfileImageUrl": Value::Null,
                "ownerIsSelf": owner_id.as_deref() == Some(self_user_id),
            }),
        );
    }
}

/// Resolve owner login / display name / avatar via Helix `/users` (chunked).
async fn enrich_emote_owners(state: &AppState, list: &mut [Value]) {
    let mut owner_ids: Vec<String> = list
        .iter()
        .filter_map(|e| e.get("ownerId").and_then(json_id_string))
        .collect();
    owner_ids.sort();
    owner_ids.dedup();
    if owner_ids.is_empty() {
        return;
    }

    let mut owners: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for chunk in owner_ids.chunks(100) {
        // Build with reqwest so repeated `id` params are encoded correctly.
        match helix_get_users(state, chunk).await {
            Ok(res) => {
                if let Some(arr) = res.get("data").and_then(|d| d.as_array()) {
                    for u in arr {
                        if let Some(id) = u.get("id").and_then(json_id_string) {
                            owners.insert(id, u.clone());
                        }
                    }
                }
            }
            Err(e) => warn!("Helix /users for emote owners failed: {e}"),
        }
    }

    info!(
        "Emote owner enrichment: {} unique owners, {} resolved",
        owner_ids.len(),
        owners.len()
    );

    for emote in list.iter_mut() {
        let Some(owner_id) = emote.get("ownerId").and_then(json_id_string) else {
            continue;
        };
        let Some(u) = owners.get(&owner_id) else {
            continue;
        };
        let login = u.get("login").cloned().unwrap_or(Value::Null);
        let name = u
            .get("display_name")
            .cloned()
            .or_else(|| u.get("login").cloned())
            .unwrap_or(Value::Null);
        let avatar = u.get("profile_image_url").cloned().unwrap_or(Value::Null);
        if let Some(obj) = emote.as_object_mut() {
            obj.insert("ownerLogin".into(), login);
            obj.insert("ownerName".into(), name);
            obj.insert("ownerProfileImageUrl".into(), avatar);
        }
    }
}

async fn helix_get_users(state: &AppState, ids: &[String]) -> Result<Value> {
    ensure_valid_token(state).await?;
    let client_id = state.helix_client_id().await;
    if client_id.is_empty() {
        return Err(anyhow!("TWITCH_CLIENT_ID not configured."));
    }
    let token = state
        .twitch
        .read()
        .await
        .tokens
        .access_token
        .clone()
        .unwrap_or_default();

    let mut url = reqwest::Url::parse("https://api.twitch.tv/helix/users")?;
    {
        let mut pairs = url.query_pairs_mut();
        for id in ids {
            pairs.append_pair("id", id);
        }
    }

    let res = reqwest::Client::new()
        .get(url)
        .header("Client-Id", &client_id)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Helix GET /users failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

async fn fetch_all_user_emotes(state: &AppState, user_id: &str) -> Result<Vec<Value>> {
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..20 {
        let mut path = format!("/chat/emotes/user?user_id={user_id}&first=100");
        if let Some(c) = &cursor {
            path.push_str(&format!("&after={c}"));
        }
        let data = helix_get(state, &path).await?;
        if let Some(items) = data.get("data").and_then(|d| d.as_array()) {
            all.extend(items.iter().cloned());
        }
        cursor = data
            .get("pagination")
            .and_then(|p| p.get("cursor"))
            .and_then(|c| c.as_str())
            .map(String::from);
        if cursor.is_none() {
            break;
        }
    }
    Ok(all)
}

pub async fn apply_set_token(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    body: Value,
) -> Result<()> {
    // Personal OAuth — keep any saved takeover session; just activate local.
    let access_token = body
        .get("accessToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing accessToken"))?;
    let validated = validate_token(access_token).await?;
    let login = validated
        .get("login")
        .and_then(|v| v.as_str())
        .map(String::from);
    let user_id = validated
        .get("user_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let expires_in = validated.get("expires_in").and_then(|v| v.as_i64());
    let tokens = TwitchTokenFile {
        access_token: Some(access_token.to_string()),
        refresh_token: None,
        expires_in,
        obtainment_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        login: login.clone(),
        user_id: user_id.clone(),
        scopes: body.get("scope").and_then(|v| v.as_array()).map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        }),
    };
    {
        *state.personal_tokens.write().await = tokens.clone();
        let mut tw = state.twitch.write().await;
        clear_live_runtime_fields(&mut tw);
        tw.tokens = tokens;
        *state.active_mode.write().await = TwitchActiveMode::Local;
    }
    state.save_twitch_tokens().await?;
    state.save_active_mode().await?;
    restart_twitch_clients(state.clone(), services.clone()).await;
    // Keep takeover tokens fresh in the background if a key is still saved.
    ensure_delegated_refresh_loop(state, services).await;
    Ok(())
}

/// Switch the live identity between saved personal OAuth and a saved takeover key.
pub async fn use_connection(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    mode: TwitchActiveMode,
) -> Result<()> {
    match mode {
        TwitchActiveMode::Local => {
            let personal = state.personal_tokens.read().await.clone();
            if personal.access_token.is_none() || personal.login.is_none() {
                return Err(anyhow!(
                    "No personal Twitch account saved. Connect with Twitch first."
                ));
            }
            {
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = personal;
                *state.active_mode.write().await = TwitchActiveMode::Local;
            }
            state.save_active_mode().await?;
            restart_twitch_clients(state.clone(), services.clone()).await;
            ensure_delegated_refresh_loop(state.clone(), services).await;
            crate::kick::sync_live_identity(state).await;
        }
        TwitchActiveMode::Delegated => {
            let session =
                state.delegated.read().await.clone().ok_or_else(|| {
                    anyhow!("No takeover connection key saved. Paste a key first.")
                })?;
            {
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = tokens_from_delegated_session(&session);
                *state.active_mode.write().await = TwitchActiveMode::Delegated;
            }
            state.save_active_mode().await?;
            restart_twitch_clients(state.clone(), services.clone()).await;
            ensure_delegated_refresh_loop(state.clone(), services).await;
            crate::kick::sync_live_identity(state).await;
        }
    }
    Ok(())
}

fn clear_live_runtime_fields(tw: &mut crate::app_state::TwitchRuntime) {
    tw.connected = false;
    tw.channel = None;
    tw.name_color = None;
    tw.display_name = None;
    tw.badges_raw.clear();
}

fn tokens_saved(t: &TwitchTokenFile) -> bool {
    t.access_token.is_some() && t.login.is_some()
}

/// Remove the currently active connection. If the other identity is still saved, activate it.
pub async fn disconnect_twitch(state: Arc<AppState>, services: Arc<TwitchServices>) -> Result<()> {
    let active = *state.active_mode.read().await;
    match active {
        TwitchActiveMode::Delegated => {
            remove_delegated_session(&state, &services).await?;
            let personal = state.personal_tokens.read().await.clone();
            if tokens_saved(&personal) {
                {
                    let mut tw = state.twitch.write().await;
                    clear_live_runtime_fields(&mut tw);
                    tw.tokens = personal;
                    *state.active_mode.write().await = TwitchActiveMode::Local;
                }
                state.save_active_mode().await?;
                restart_twitch_clients(state.clone(), services).await;
                crate::kick::sync_live_identity(state).await;
            } else {
                stop_delegated_tasks(&services).await;
                stop_twitch_clients(&services).await;
                {
                    let mut tw = state.twitch.write().await;
                    tw.tokens = TwitchTokenFile::default();
                    clear_live_runtime_fields(&mut tw);
                    *state.active_mode.write().await = TwitchActiveMode::Local;
                }
                state.save_active_mode().await?;
                crate::kick::sync_live_identity(state).await;
            }
        }
        TwitchActiveMode::Local => {
            {
                *state.personal_tokens.write().await = TwitchTokenFile::default();
            }
            state.save_twitch_tokens().await?;
            let has_delegated = state.delegated.read().await.is_some();
            if has_delegated {
                let session = state.delegated.read().await.clone().unwrap();
                {
                    let mut tw = state.twitch.write().await;
                    clear_live_runtime_fields(&mut tw);
                    tw.tokens = tokens_from_delegated_session(&session);
                    *state.active_mode.write().await = TwitchActiveMode::Delegated;
                }
                state.save_active_mode().await?;
                restart_twitch_clients(state.clone(), services.clone()).await;
                ensure_delegated_refresh_loop(state.clone(), services).await;
                crate::kick::sync_live_identity(state).await;
            } else {
                stop_delegated_tasks(&services).await;
                stop_twitch_clients(&services).await;
                {
                    let mut tw = state.twitch.write().await;
                    tw.tokens = TwitchTokenFile::default();
                    clear_live_runtime_fields(&mut tw);
                }
                crate::kick::sync_live_identity(state).await;
            }
        }
    }
    Ok(())
}

/// Remove a specific saved identity without requiring it to be active.
pub async fn remove_connection(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    mode: TwitchActiveMode,
) -> Result<()> {
    let active = *state.active_mode.read().await;
    if active == mode {
        return disconnect_twitch(state, services).await;
    }
    match mode {
        TwitchActiveMode::Local => {
            *state.personal_tokens.write().await = TwitchTokenFile::default();
            state.save_twitch_tokens().await?;
        }
        TwitchActiveMode::Delegated => {
            remove_delegated_session(&state, &services).await?;
        }
    }
    Ok(())
}

fn expires_in_from_iso(iso: &str) -> Option<i64> {
    let exp = chrono::DateTime::parse_from_rfc3339(iso).ok()?;
    let secs = exp.timestamp() - chrono::Utc::now().timestamp();
    Some(secs.max(0))
}

fn install_tokens_from_exchange(
    exchange: &syndicate_connection::ExchangeSuccess,
) -> TwitchTokenFile {
    let expires_in = expires_in_from_iso(&exchange.twitch.expires_at);
    TwitchTokenFile {
        access_token: Some(exchange.twitch.access_token.clone()),
        refresh_token: None,
        expires_in,
        obtainment_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        login: Some(exchange.channel.login.clone()),
        user_id: Some(exchange.channel.twitch_id.clone()),
        scopes: Some(exchange.twitch.scopes.clone()),
    }
}

async fn remove_delegated_session(state: &AppState, services: &TwitchServices) -> Result<()> {
    stop_delegated_tasks(services).await;
    *state.delegated.write().await = None;
    state.save_delegated().await?;
    Ok(())
}

async fn stop_delegated_tasks(services: &TwitchServices) {
    if let Some(h) = services.refresh_handle.write().await.take() {
        h.abort();
    }
    if let Some(h) = services.watch_handle.write().await.take() {
        h.abort();
    }
}

async fn stop_refresh_task(services: &TwitchServices) {
    if let Some(h) = services.refresh_handle.write().await.take() {
        h.abort();
    }
}

async fn end_delegated_session_after_key_invalid(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    code: &str,
) {
    warn!("delegated session ended: {}", code);
    let was_active = state.is_delegated_mode().await;
    let _ = remove_delegated_session(&state, &services).await;
    if was_active {
        let personal = state.personal_tokens.read().await.clone();
        if tokens_saved(&personal) {
            {
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = personal;
                *state.active_mode.write().await = TwitchActiveMode::Local;
            }
            let _ = state.save_active_mode().await;
            restart_twitch_clients(state.clone(), services.clone()).await;
        } else {
            stop_twitch_clients(&services).await;
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile::default();
            clear_live_runtime_fields(&mut tw);
            *state.active_mode.write().await = TwitchActiveMode::Local;
            let _ = state.save_active_mode().await;
        }
    }
    crate::kick::sync_live_identity(state).await;
}

/// Map a connection-key failure into `(error_code, user_message, http_status)`.
pub fn connection_key_error_parts(
    err: &anyhow::Error,
) -> Option<(String, String, axum::http::StatusCode)> {
    let api = err.downcast_ref::<SyndicateApiError>()?;
    let status = match api.code.as_str() {
        "invalid_key" | "expired" | "revoked" => axum::http::StatusCode::UNAUTHORIZED,
        "missing_scopes" => axum::http::StatusCode::FORBIDDEN,
        "rate_limited" => axum::http::StatusCode::TOO_MANY_REQUESTS,
        "token_unavailable" => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        _ => axum::http::StatusCode::BAD_GATEWAY,
    };
    Some((
        api.code.clone(),
        syndicate_connection::user_message_for_error(api),
        status,
    ))
}

/// Exchange a Syndicate connection key and start Twitch as that channel (takeover).
pub async fn apply_connection_key(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    key: &str,
) -> Result<()> {
    let exchange = syndicate_connection::exchange(key).await?;
    apply_exchange_session(state, services, key, exchange, true).await
}

/// Persist an exchanged takeover session. When `activate` is true, make it the live identity.
async fn apply_exchange_session(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    key: &str,
    exchange: syndicate_connection::ExchangeSuccess,
    activate: bool,
) -> Result<()> {
    let session = DelegatedSessionFile {
        connection_key: key.trim().to_string(),
        client_id: exchange.twitch.client_id.clone(),
        access_token: exchange.twitch.access_token.clone(),
        channel_login: exchange.channel.login.clone(),
        channel_twitch_id: exchange.channel.twitch_id.clone(),
        display_name: exchange.channel.display_name.clone(),
        label: exchange.connection.label.clone(),
        scopes: exchange.twitch.scopes.clone(),
        twitch_expires_at: exchange.twitch.expires_at.clone(),
        connection_expires_at: exchange.connection.expires_at.clone(),
        ..Default::default()
    };
    let mut session = session;
    crate::kick::apply_kick_to_delegated(&mut session, &exchange);

    *state.delegated.write().await = Some(session);
    state.save_delegated().await?;

    if activate {
        {
            let mut tw = state.twitch.write().await;
            clear_live_runtime_fields(&mut tw);
            tw.tokens = install_tokens_from_exchange(&exchange);
            *state.active_mode.write().await = TwitchActiveMode::Delegated;
        }
        state.save_active_mode().await?;
        restart_twitch_clients(state.clone(), services.clone()).await;
    }

    start_delegated_refresh_loop(state.clone(), services.clone()).await;
    start_delegated_watch_loop(state.clone(), services).await;
    crate::kick::sync_live_identity(state).await;
    Ok(())
}

async fn ensure_delegated_refresh_loop(state: Arc<AppState>, services: Arc<TwitchServices>) {
    if state.delegated.read().await.is_none() {
        return;
    }
    let running = services.refresh_handle.read().await.is_some();
    if !running {
        start_delegated_refresh_loop(state.clone(), services.clone()).await;
    }
    let watch_running = services.watch_handle.read().await.is_some();
    if !watch_running {
        start_delegated_watch_loop(state, services).await;
    }
}

async fn start_delegated_refresh_loop(state: Arc<AppState>, services: Arc<TwitchServices>) {
    stop_refresh_task(&services).await;
    let state2 = state.clone();
    let services2 = services.clone();
    let handle = tokio::spawn(async move {
        loop {
            let (key, expires_at) = {
                let d = state2.delegated.read().await;
                match d.as_ref() {
                    Some(s) => (s.connection_key.clone(), s.twitch_expires_at.clone()),
                    None => break,
                }
            };

            let sleep_for = match chrono::DateTime::parse_from_rfc3339(&expires_at) {
                Ok(exp) => {
                    let refresh_at = exp - chrono::Duration::minutes(2);
                    let now = chrono::Utc::now();
                    let wait = refresh_at.signed_duration_since(now);
                    if wait.num_seconds() > 0 {
                        Duration::from_secs(wait.num_seconds() as u64)
                    } else {
                        Duration::from_secs(5)
                    }
                }
                Err(_) => Duration::from_secs(60 * 30),
            };

            tokio::time::sleep(sleep_for).await;

            if state2.delegated.read().await.is_none() {
                break;
            }

            match syndicate_connection::refresh(&key).await {
                Ok(exchange) => {
                    let session = {
                        let mut guard = state2.delegated.write().await;
                        if let Some(ref mut s) = *guard {
                            s.access_token = exchange.twitch.access_token.clone();
                            s.client_id = exchange.twitch.client_id.clone();
                            s.twitch_expires_at = exchange.twitch.expires_at.clone();
                            s.scopes = exchange.twitch.scopes.clone();
                            if let Some(ref exp) = exchange.connection.expires_at {
                                s.connection_expires_at = Some(exp.clone());
                            }
                            crate::kick::apply_kick_to_delegated(s, &exchange);
                            s.clone()
                        } else {
                            break;
                        }
                    };
                    let _ = state2.save_delegated().await;
                    let active_delegated = state2.is_delegated_mode().await;
                    if active_delegated {
                        {
                            let mut tw = state2.twitch.write().await;
                            tw.tokens = install_tokens_from_exchange(&exchange);
                        }
                        info!(
                            "delegated Twitch token refreshed for {}",
                            session.channel_login
                        );
                        // Restart clients so IRC/EventSub pick up the new token.
                        restart_twitch_clients(state2.clone(), services2.clone()).await;
                    } else {
                        info!(
                            "delegated Twitch token refreshed (inactive) for {}",
                            session.channel_login
                        );
                    }
                    crate::kick::sync_live_identity(state2.clone()).await;
                }
                Err(e) => {
                    if let Some(api) = e.downcast_ref::<SyndicateApiError>() {
                        match api.code.as_str() {
                            "revoked" | "expired" | "invalid_key" => {
                                end_delegated_session_after_key_invalid(
                                    state2.clone(),
                                    services2.clone(),
                                    &api.code,
                                )
                                .await;
                                break;
                            }
                            "token_unavailable" | "rate_limited" => {
                                warn!("delegated refresh soft-fail: {} — retrying", api.code);
                                tokio::time::sleep(Duration::from_secs(60)).await;
                            }
                            _ => {
                                warn!("delegated refresh failed: {} — retrying", api);
                                tokio::time::sleep(Duration::from_secs(60)).await;
                            }
                        }
                    } else {
                        warn!("delegated refresh error: {e:#} — retrying");
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            }
        }
    });
    *services.refresh_handle.write().await = Some(handle);
}

fn parse_connection_key_sse_data(frame: &str) -> Option<Value> {
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

async fn consume_connection_key_events(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    key: &str,
) -> Result<bool> {
    let url = syndicate_connection::connection_key_events_url(key);
    let res = reqwest::Client::new()
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await?;
    if res.status() == reqwest::StatusCode::UNAUTHORIZED {
        end_delegated_session_after_key_invalid(state, services, "revoked").await;
        return Ok(true);
    }
    if !res.status().is_success() {
        return Err(anyhow!("connection key watch HTTP {}", res.status()));
    }
    let mut stream = res.bytes_stream();
    let mut buf = String::new();
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        buf.push_str(&String::from_utf8_lossy(&bytes));
        while let Some(idx) = buf.find("\n\n") {
            let frame = buf[..idx].to_string();
            buf = buf[idx + 2..].to_string();
            if let Some(event) = parse_connection_key_sse_data(&frame) {
                if event.get("type").and_then(|v| v.as_str()) == Some("revoked") {
                    end_delegated_session_after_key_invalid(state, services, "revoked").await;
                    return Ok(true);
                }
            }
        }
        if state.delegated.read().await.is_none() {
            return Ok(false);
        }
    }
    Ok(false)
}

async fn start_delegated_watch_loop(state: Arc<AppState>, services: Arc<TwitchServices>) {
    if let Some(h) = services.watch_handle.write().await.take() {
        h.abort();
    }
    let state2 = state.clone();
    let services2 = services.clone();
    let handle = tokio::spawn(async move {
        loop {
            let key = {
                let d = state2.delegated.read().await;
                match d.as_ref() {
                    Some(s) => s.connection_key.clone(),
                    None => break,
                }
            };

            match consume_connection_key_events(state2.clone(), services2.clone(), &key).await {
                Ok(true) => break,
                Ok(false) => {}
                Err(e) => warn!("connection key watch error: {e:#}"),
            }

            if state2.delegated.read().await.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
    *services.watch_handle.write().await = Some(handle);
}

pub async fn restart_twitch_clients(state: Arc<AppState>, services: Arc<TwitchServices>) {
    stop_twitch_clients(&services).await;
    if let Err(e) = start_irc(state.clone(), services.clone()).await {
        warn!("IRC start failed: {e}");
    }
    if let Err(e) = start_eventsub(state.clone(), services.clone()).await {
        warn!("EventSub start failed: {e}");
    }
}

async fn stop_twitch_clients(services: &TwitchServices) {
    *services.irc_client.write().await = None;
    if let Some(h) = services.irc_handle.write().await.take() {
        h.abort();
    }
    if let Some(h) = services.eventsub_handle.write().await.take() {
        h.abort();
    }
}

async fn start_irc(state: Arc<AppState>, services: Arc<TwitchServices>) -> Result<()> {
    let (login, token) = {
        let tw = state.twitch.read().await;
        let login = tw.tokens.login.clone().ok_or_else(|| anyhow!("no login"))?;
        let token = tw
            .tokens
            .access_token
            .clone()
            .ok_or_else(|| anyhow!("no token"))?;
        (login, token)
    };
    ensure_valid_token(&state).await?;

    use twitch_irc::message::ServerMessage;

    let credentials = StaticLoginCredentials::new(login.clone(), Some(token));
    let config = ClientConfig::new_simple(credentials);
    let (mut incoming, client) = StreamSyncIrcClient::new(config);

    let channel = login.clone();
    let feed = state.feed.clone();
    client.join(channel.clone()).ok();

    *services.irc_client.write().await = Some(client);

    {
        let mut tw = state.twitch.write().await;
        tw.connected = true;
        tw.channel = Some(channel.clone());
    }

    let broadcaster_login = login.clone();
    let state_irc = state.clone();
    let handle = tokio::spawn(async move {
        while let Some(message) = incoming.recv().await {
            match message {
                ServerMessage::GlobalUserState(msg) => {
                    store_broadcaster_user_state(
                        &state_irc,
                        msg.name_color.as_ref(),
                        Some(msg.user_name.as_str()),
                        &msg.badges,
                    );
                }
                ServerMessage::UserState(msg) => {
                    // Channel-scoped badges (broadcaster, mod, sub, etc.) — best source for dock sends.
                    store_broadcaster_user_state(
                        &state_irc,
                        msg.name_color.as_ref(),
                        Some(msg.user_name.as_str()),
                        &msg.badges,
                    );
                }
                ServerMessage::Privmsg(msg) => {
                    let is_self = msg.sender.login.eq_ignore_ascii_case(&broadcaster_login);
                    if is_self {
                        store_broadcaster_user_state(
                            &state_irc,
                            msg.name_color.as_ref(),
                            Some(msg.sender.name.as_str()),
                            &msg.badges,
                        );
                    }
                    let evt = privmsg_to_chat_event(&msg, is_self);
                    feed.broadcast_all(&evt).await;
                }
                ServerMessage::Notice(_) => {}
                _ => {}
            }
        }
        info!("IRC incoming ended for {channel}");
    });

    *services.irc_handle.write().await = Some(handle);
    refresh_broadcaster_chat_color(&state).await;
    Ok(())
}

async fn refresh_broadcaster_chat_color(state: &AppState) {
    if let Ok(color) = fetch_broadcaster_chat_color(state).await {
        let mut tw = state.twitch.write().await;
        tw.name_color = Some(color);
    }
}

async fn fetch_broadcaster_chat_color(state: &AppState) -> Result<String> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("no user_id"))?;
    let body = helix_get(state, &format!("/chat/color?user_id={user_id}")).await?;
    let color = body
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|row| row.get("color"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| anyhow!("no chat color in Helix response"))?;
    Ok(color.to_string())
}

fn store_broadcaster_user_state(
    state: &AppState,
    color: Option<&twitch_irc::message::RGBColor>,
    display_name: Option<&str>,
    badges: &[twitch_irc::message::Badge],
) {
    if let Ok(mut tw) = state.twitch.try_write() {
        if let Some(c) = color {
            tw.name_color = Some(c.to_string());
        }
        if let Some(name) = display_name {
            if !name.is_empty() {
                tw.display_name = Some(name.to_string());
            }
        }
        // USERSTATE after join/send is authoritative for channel badges.
        // GLOBALUSERSTATE often has an empty badge list — don't wipe channel badges with it.
        if !badges.is_empty() {
            tw.badges_raw.clear();
            for badge in badges {
                tw.badges_raw
                    .insert(badge.name.clone(), badge.version.clone());
            }
        }
    }
}

pub async fn send_chat_from_dock(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    text: &str,
) -> Result<()> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if trimmed.starts_with('/') {
        handle_dock_command(state, services, trimmed).await?;
        return Ok(());
    }
    send_plain_chat(&state, &services, trimmed).await
}

async fn irc_client_ready(services: &TwitchServices) -> Result<StreamSyncIrcClient> {
    services
        .irc_client
        .read()
        .await
        .clone()
        .ok_or_else(|| anyhow!("Twitch IRC client is not ready"))
}

/// Convert twitch-irc emotes (exclusive end index) to TMI.js shape `{ id: ["start-end"] }`.
fn emotes_to_json_map(emotes: &[twitch_irc::message::Emote]) -> Option<Value> {
    if emotes.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for emote in emotes {
        let start = emote.char_range.start;
        let end_inclusive = emote.char_range.end.saturating_sub(1);
        let range = format!("{start}-{end_inclusive}");
        let entry = map.entry(emote.id.clone()).or_insert_with(|| json!([]));
        if let Some(arr) = entry.as_array_mut() {
            arr.push(json!(range));
        }
    }
    Some(Value::Object(map))
}

fn badges_to_json(badges: &[twitch_irc::message::Badge]) -> (Vec<String>, Value) {
    let names: Vec<String> = badges.iter().map(|b| b.name.clone()).collect();
    let mut badges_raw = serde_json::Map::new();
    for badge in badges {
        badges_raw.insert(badge.name.clone(), json!(badge.version));
    }
    (names, Value::Object(badges_raw))
}

fn privmsg_to_chat_event(msg: &twitch_irc::message::PrivmsgMessage, is_self: bool) -> Value {
    let (badges, badges_raw) = badges_to_json(&msg.badges);
    let color = msg.name_color.as_ref().map(|c| c.to_string());
    let mut evt = json!({
        "type": "chat",
        "platform": "twitch",
        "ts": chrono::Utc::now().timestamp_millis(),
        "user": {
            "name": msg.sender.login.clone(),
            "displayName": msg.sender.name.clone(),
            "color": color,
            "badges": badges,
            "badgesRaw": badges_raw,
        },
        "message": msg.message_text,
        "self": is_self,
    });
    if let Some(emotes) = emotes_to_json_map(&msg.emotes) {
        evt["emotes"] = emotes;
    }
    evt
}

/// Push dock-sent chat to dock + overlay immediately (Twitch often does not IRC-echo your own sends).
/// Always includes badgesRaw + emotes when known; dock/overlay hide badges via showBadges config.
async fn broadcast_outgoing_chat(state: &AppState, services: &TwitchServices, message: &str) {
    if state.twitch.read().await.name_color.is_none() {
        refresh_broadcaster_chat_color(state).await;
    }

    let emotes = resolve_outgoing_emotes(state, services, message).await;

    let tw = state.twitch.read().await;
    let login = tw.tokens.login.clone().unwrap_or_default();
    let display = tw.display_name.clone().unwrap_or_else(|| login.clone());
    let color = tw.name_color.clone();
    let badges: Vec<String> = tw.badges_raw.keys().cloned().collect();
    let badges_raw = tw
        .badges_raw
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect::<serde_json::Map<String, Value>>();
    let mut evt = json!({
        "type": "chat",
        "platform": "twitch",
        "ts": chrono::Utc::now().timestamp_millis(),
        "user": {
            "name": login,
            "displayName": display,
            "color": color,
            "badges": badges,
            "badgesRaw": badges_raw,
        },
        "message": message,
        "self": true,
    });
    if let Some(emotes) = emotes {
        evt["emotes"] = emotes;
    }
    drop(tw);
    state.feed.broadcast_all(&evt).await;
}

/// Match whitespace-delimited tokens in `message` against the Helix emote catalog by name.
async fn resolve_outgoing_emotes(
    state: &AppState,
    services: &TwitchServices,
    message: &str,
) -> Option<Value> {
    let list = get_merged_emotes(state, services).await.ok()?;
    let mut by_name: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for emote in &list {
        let Some(name) = emote.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(id) = emote.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        by_name
            .entry(name.to_string())
            .or_insert_with(|| id.to_string());
    }
    emotes_from_message_text(message, &by_name)
}

/// Build TMI-style `{ id: ["start-end"] }` by matching whole whitespace-separated tokens.
fn emotes_from_message_text(
    message: &str,
    by_name: &std::collections::HashMap<String, String>,
) -> Option<Value> {
    if by_name.is_empty() || message.is_empty() {
        return None;
    }
    let chars: Vec<char> = message.chars().collect();
    let mut map = serde_json::Map::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() {
            i += 1;
        }
        let token: String = chars[start..i].iter().collect();
        if let Some(id) = by_name.get(&token) {
            let end_inclusive = i - 1;
            let range = format!("{start}-{end_inclusive}");
            let entry = map.entry(id.clone()).or_insert_with(|| json!([]));
            if let Some(arr) = entry.as_array_mut() {
                arr.push(json!(range));
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

async fn send_plain_chat(state: &AppState, services: &TwitchServices, trimmed: &str) -> Result<()> {
    let channel = state
        .twitch
        .read()
        .await
        .channel
        .clone()
        .ok_or_else(|| anyhow!("No Twitch channel joined"))?;
    let client = irc_client_ready(services).await?;
    client
        .say(channel.clone(), trimmed.to_string())
        .await
        .map_err(|e| anyhow!("IRC say failed: {e}"))?;
    broadcast_outgoing_chat(state, services, trimmed).await;
    info!("Sent chat to #{channel}: {trimmed}");
    Ok(())
}

async fn send_dock_privmsg(state: &AppState, services: &TwitchServices, text: &str) -> Result<()> {
    let channel = state
        .twitch
        .read()
        .await
        .channel
        .clone()
        .ok_or_else(|| anyhow!("No Twitch channel joined"))?;
    let client = irc_client_ready(services).await?;
    client
        .privmsg(channel.clone(), text.to_string())
        .await
        .map_err(|e| anyhow!("IRC privmsg failed: {e}"))?;
    info!("Sent IRC command to #{channel}: {text}");
    Ok(())
}

async fn handle_dock_command(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    text: &str,
) -> Result<()> {
    let parts: Vec<&str> = text.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("").to_lowercase();
    let args: Vec<&str> = parts.into_iter().skip(1).collect();
    match cmd.as_str() {
        "/slow" => {
            let raw = args.first().copied();
            if raw.is_none()
                || raw == Some("0")
                || raw.map(|r| r.eq_ignore_ascii_case("off")).unwrap_or(false)
                || raw
                    .map(|r| r.eq_ignore_ascii_case("disable"))
                    .unwrap_or(false)
            {
                update_chat_settings(&state, json!({ "slow_mode": false })).await?;
            } else {
                let mut wait: i64 = raw.unwrap().parse().unwrap_or(30);
                if wait <= 0 {
                    wait = 30;
                }
                wait = wait.clamp(3, 180);
                update_chat_settings(
                    &state,
                    json!({ "slow_mode": true, "slow_mode_wait_time": wait }),
                )
                .await?;
            }
        }
        "/slowoff" => {
            update_chat_settings(&state, json!({ "slow_mode": false })).await?;
        }
        "/ban" | "/unban" | "/timeout" => {
            send_dock_privmsg(&state, &services, text).await?;
        }
        _ => {
            send_dock_privmsg(&state, &services, text).await?;
        }
    }
    Ok(())
}

// ─── EventSub ───────────────────────────────────────────────────────────────

/// Twitch plan tier → display tier 1–3 (`1000`/`2000`/`3000`, Prime, or `1`/`2`/`3`).
pub fn twitch_tier_display_number(tier: &Value) -> Option<u8> {
    let s = match tier {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_string(),
        _ => return None,
    };
    if s.is_empty() {
        return None;
    }
    if s == "1000" || s.eq_ignore_ascii_case("prime") {
        return Some(1);
    }
    if s == "2000" {
        return Some(2);
    }
    if s == "3000" {
        return Some(3);
    }
    if let Ok(n) = s.parse::<u32>() {
        if (1..=3).contains(&n) {
            return Some(n as u8);
        }
        if n == 1000 {
            return Some(1);
        }
        if n == 2000 {
            return Some(2);
        }
        if n == 3000 {
            return Some(3);
        }
    }
    None
}

pub fn format_sub_dock_detail(user: &str, tier: &Value) -> String {
    match twitch_tier_display_number(tier) {
        Some(n) => format!("{user} subscribed — Tier {n}"),
        None => format!("{user} subscribed"),
    }
}

pub fn format_resub_dock_detail(user: &str, months: &Value, tier: &Value, msg: &str) -> String {
    let tn = twitch_tier_display_number(tier);
    let months_s = value_display_string(months);
    let mut detail = user.to_string();
    if !months_s.is_empty() {
        detail = format!("{user} — {months_s} months");
        if let Some(n) = tn {
            detail.push_str(&format!(" — Tier {n}"));
        }
    } else if let Some(n) = tn {
        detail.push_str(&format!(" — Tier {n}"));
    }
    if !msg.is_empty() {
        detail.push_str(&format!(": {msg}"));
    }
    detail
}

pub fn format_gift_dock_detail(
    gifter: &str,
    total: &Value,
    tier: &Value,
    recipient: &str,
) -> String {
    let tn = twitch_tier_display_number(tier);
    let qty = total
        .as_u64()
        .or_else(|| total.as_str().and_then(|s| s.trim().parse::<u64>().ok()));
    if let Some(q) = qty {
        if let Some(n) = tn {
            return format!("{gifter} gifted {q} Tier {n} subs");
        }
        return format!("{gifter} gifted {q} subs");
    }
    if !recipient.is_empty() {
        if let Some(n) = tn {
            return format!("{gifter} gifted {recipient} a Tier {n} sub");
        }
        return format!("{gifter} gifted {recipient} a sub");
    }
    if let Some(n) = tn {
        return format!("{gifter} gifted a Tier {n} sub");
    }
    format!("{gifter} gifted a sub")
}

fn value_display_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

pub fn normalize_event_variables(vars: &Value) -> Value {
    let name = vars
        .get("name")
        .or(vars.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let user = vars
        .get("user")
        .or(vars.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    json!({
        "user": user,
        "name": name,
        "amount": vars.get("amount").or(vars.get("tier")).or(vars.get("bits")).unwrap_or(&Value::Null),
        "months": vars.get("months").unwrap_or(&Value::Null),
        "reward": vars.get("reward").or(vars.get("title")).unwrap_or(&Value::Null),
        "input": vars.get("input").or(vars.get("message")).unwrap_or(&Value::Null),
        "recipient": vars.get("recipient").unwrap_or(&Value::Null),
        "tier": vars.get("tier").or(vars.get("amount")).unwrap_or(&Value::Null),
        "bits": vars.get("bits").unwrap_or(&Value::Null),
        "raiders": vars.get("raiders").or(vars.get("viewers")).unwrap_or(&Value::Null),
    })
}

async fn handle_eventsub_notification(feed: &FeedHub, sub_type: &str, event: &Value) {
    match sub_type {
        "channel.follow" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "follow",
                "data": { "variables": normalize_event_variables(&json!({ "name": user })) },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "follow",
                &format!("{user} followed"),
                Some("Follow"),
                None,
            ))
            .await;
        }
        "channel.subscribe" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "sub",
                    "data": { "variables": normalize_event_variables(&json!({ "name": user, "amount": tier })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "sub",
                &format_sub_dock_detail(user, &tier),
                Some("Sub"),
                None,
            ))
            .await;
        }
        "channel.cheer" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let bits = event.get("bits").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "cheer",
                    "data": { "variables": normalize_event_variables(&json!({ "name": user, "amount": bits })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "bits",
                &format!("{user} cheered {bits}"),
                Some("Bits"),
                None,
            ))
            .await;
        }
        "channel.subscription.message" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let months = event
                .get("cumulative_months")
                .or_else(|| event.get("streak_months"))
                .cloned()
                .unwrap_or(Value::Null);
            let msg = event
                .pointer("/message/text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            let vars = normalize_event_variables(&json!({
                "name": user,
                "months": months,
                "input": msg,
                "amount": tier,
            }));
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "resub",
                "data": { "variables": vars },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "resub",
                &format_resub_dock_detail(user, &months, &tier, msg),
                Some("Resub"),
                None,
            ))
            .await;
        }
        "channel.subscription.gift" => {
            let gifter = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("Anonymous");
            let recipient = event
                .get("recipient_user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let total = event.get("total").cloned().unwrap_or(Value::Null);
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            let vars = normalize_event_variables(&json!({
                "name": gifter,
                "recipient": recipient,
                "amount": total,
                "tier": tier,
            }));
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "gift",
                "data": { "variables": vars },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "gift",
                &format_gift_dock_detail(gifter, &total, &tier, recipient),
                Some("Gift"),
                None,
            ))
            .await;
        }
        "channel.raid" => {
            let from = event
                .get("from_broadcaster_user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let viewers = event.get("viewers").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "raid",
                    "data": { "variables": normalize_event_variables(&json!({ "name": from, "amount": viewers })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "raid",
                &format!(
                    "{from} raided{}",
                    if viewers.is_null() {
                        String::new()
                    } else {
                        format!(" with {viewers}")
                    }
                ),
                Some("Raid"),
                None,
            ))
            .await;
        }
        "channel.channel_points_custom_reward_redemption.add" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let title = event
                .pointer("/reward/title")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let input = event
                .get("user_input")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let cost = event.pointer("/reward/cost").and_then(|v| v.as_u64());
            let mut detail = format!("{title} — {user}");
            if !input.is_empty() {
                detail.push_str(&format!(": {input}"));
            }
            if let Some(c) = cost {
                detail.push_str(&format!(" ({c} pts)"));
            }
            // Channel points are dock-only — not routed through events overlays.
            feed.broadcast_profile(
                "default",
                &make_dock_event("redeem", &detail, Some("Channel Points"), None),
            )
            .await;
        }
        _ => {}
    }
}

async fn start_eventsub(state: Arc<AppState>, services: Arc<TwitchServices>) -> Result<()> {
    let client_id = state.helix_client_id().await;
    if client_id.is_empty() {
        return Err(anyhow!("TWITCH_CLIENT_ID missing"));
    }
    ensure_valid_token(&state).await?;
    let feed = state.feed.clone();
    let state2 = state.clone();
    let handle = tokio::spawn(async move {
        loop {
            if let Err(e) = eventsub_session(state2.clone(), feed.clone()).await {
                warn!("EventSub session error: {e}");
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
            let tw = state2.twitch.read().await;
            if tw.tokens.access_token.is_none() {
                break;
            }
        }
    });
    *services.eventsub_handle.write().await = Some(handle);
    Ok(())
}

async fn eventsub_session(state: Arc<AppState>, feed: FeedHub) -> Result<()> {
    let (ws, _) = connect_async("wss://eventsub.wss.twitch.tv/ws").await?;
    let (_write, mut read) = ws.split();
    let mut session_id: Option<String> = None;

    while let Some(msg) = read.next().await {
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let parsed: EventSubEnvelope = serde_json::from_str(msg.to_text()?)?;
        let message_type = parsed.metadata.message_type.as_str();
        match message_type {
            "session_welcome" => {
                session_id = parsed.payload.session.as_ref().and_then(|s| s.id.clone());
                if let Some(ref sid) = session_id {
                    subscribe_topics(&state, sid).await;
                }
            }
            "session_reconnect" => {
                session_id = parsed.payload.session.as_ref().and_then(|s| s.id.clone());
                if let Some(ref sid) = session_id {
                    subscribe_topics(&state, sid).await;
                }
            }
            "notification" => {
                let Some(sub_type) = parsed.metadata.subscription_type.as_deref() else {
                    continue;
                };
                if let Some(event) = parsed.payload.event.as_ref() {
                    handle_eventsub_notification(&feed, sub_type, event).await;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn subscribe_topics(state: &AppState, session_id: &str) {
    let user_id = match state.twitch.read().await.tokens.user_id.clone() {
        Some(id) => id,
        None => return,
    };
    let transport = json!({
        "method": "websocket",
        "session_id": session_id
    });
    let subs: Vec<(&str, &str, Value)> = vec![
        (
            "channel.follow",
            "2",
            json!({ "broadcaster_user_id": user_id, "moderator_user_id": user_id }),
        ),
        (
            "channel.subscribe",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.subscription.message",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.subscription.gift",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.cheer",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.raid",
            "1",
            json!({ "to_broadcaster_user_id": user_id }),
        ),
        (
            "channel.channel_points_custom_reward_redemption.add",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
    ];
    for (ty, ver, condition) in subs {
        let body = json!({
            "type": ty,
            "version": ver,
            "condition": condition,
            "transport": transport,
        });
        match helix_post(state, "/eventsub/subscriptions", body).await {
            Ok(_) => info!("EventSub subscribed: {ty}"),
            Err(e) => warn!("EventSub subscribe {ty}: {e}"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct EventSubEnvelope {
    metadata: EventSubMetadata,
    payload: EventSubPayload,
}

#[derive(Debug, Deserialize)]
struct EventSubMetadata {
    #[serde(rename = "message_type")]
    message_type: String,
    /// Present on `notification` only; omitted on `session_welcome` / keepalives.
    #[serde(rename = "subscription_type", default)]
    subscription_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventSubPayload {
    session: Option<EventSubSession>,
    event: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct EventSubSession {
    id: Option<String>,
}

pub async fn maybe_autostart(state: Arc<AppState>, services: Arc<TwitchServices>) {
    let active = *state.active_mode.read().await;
    let delegated_key = {
        state
            .delegated
            .read()
            .await
            .as_ref()
            .map(|d| d.connection_key.clone())
    };

    // Prefer refreshing a saved takeover key so tokens stay valid even if inactive.
    if let Some(key) = delegated_key {
        match syndicate_connection::refresh(&key).await {
            Ok(exchange) => {
                let activate = active == TwitchActiveMode::Delegated;
                match apply_exchange_session(
                    state.clone(),
                    services.clone(),
                    &key,
                    exchange,
                    activate,
                )
                .await
                {
                    Ok(()) if activate => return,
                    Ok(()) => {
                        // Inactive takeover refreshed — continue to start personal if active is local.
                    }
                    Err(e) => {
                        warn!("delegated autostart apply failed: {e:#}");
                        if activate {
                            let has_delegated_tokens = {
                                let tw = state.twitch.read().await;
                                tw.tokens.access_token.is_some() && tw.tokens.login.is_some()
                            };
                            if has_delegated_tokens {
                                restart_twitch_clients(state.clone(), services.clone()).await;
                                start_delegated_refresh_loop(state.clone(), services.clone()).await;
                                return;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if let Some(api) = e.downcast_ref::<SyndicateApiError>() {
                    match api.code.as_str() {
                        "revoked" | "expired" | "invalid_key" => {
                            warn!("delegated session invalid on launch: {}", api.code);
                            let _ = remove_delegated_session(&state, &services).await;
                            if active == TwitchActiveMode::Delegated {
                                // Fall through to personal if available.
                            }
                        }
                        _ => {
                            warn!(
                                "delegated refresh on launch failed: {api} — trying stored token"
                            );
                            if active == TwitchActiveMode::Delegated {
                                let has_delegated_tokens = {
                                    let tw = state.twitch.read().await;
                                    tw.tokens.access_token.is_some() && tw.tokens.login.is_some()
                                };
                                if has_delegated_tokens {
                                    restart_twitch_clients(state.clone(), services.clone()).await;
                                    start_delegated_refresh_loop(state.clone(), services.clone())
                                        .await;
                                    return;
                                }
                            } else {
                                ensure_delegated_refresh_loop(state.clone(), services.clone())
                                    .await;
                            }
                        }
                    }
                } else {
                    warn!("delegated refresh on launch failed: {e:#}");
                    if active == TwitchActiveMode::Delegated {
                        let has_delegated_tokens = {
                            let tw = state.twitch.read().await;
                            tw.tokens.access_token.is_some() && tw.tokens.login.is_some()
                        };
                        if has_delegated_tokens {
                            restart_twitch_clients(state.clone(), services.clone()).await;
                            start_delegated_refresh_loop(state.clone(), services.clone()).await;
                            return;
                        }
                    } else {
                        ensure_delegated_refresh_loop(state.clone(), services.clone()).await;
                    }
                }
            }
        }
    }

    // Active mode Local (or delegated cleared): start personal OAuth if present.
    let personal = state.personal_tokens.read().await.clone();
    if tokens_saved(&personal) {
        {
            let mut tw = state.twitch.write().await;
            clear_live_runtime_fields(&mut tw);
            tw.tokens = personal;
            *state.active_mode.write().await = TwitchActiveMode::Local;
        }
        let _ = state.save_active_mode().await;
        restart_twitch_clients(state.clone(), services.clone()).await;
        ensure_delegated_refresh_loop(state, services).await;
        return;
    }

    // No personal — if delegated still exists, activate it as last resort.
    if state.delegated.read().await.is_some() {
        let session = state.delegated.read().await.clone().unwrap();
        {
            let mut tw = state.twitch.write().await;
            clear_live_runtime_fields(&mut tw);
            tw.tokens = tokens_from_delegated_session(&session);
            *state.active_mode.write().await = TwitchActiveMode::Delegated;
        }
        let _ = state.save_active_mode().await;
        restart_twitch_clients(state.clone(), services.clone()).await;
        ensure_delegated_refresh_loop(state, services).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twitch_irc::message::{Badge, Emote};

    #[test]
    fn connection_key_error_parts_maps_codes() {
        let err: anyhow::Error = SyndicateApiError {
            code: "revoked".into(),
            message: "gone".into(),
            http_status: 401,
        }
        .into();
        let (code, message, status) = connection_key_error_parts(&err).expect("mapped");
        assert_eq!(code, "revoked");
        assert!(message.contains("revoked"));
        assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    }

    fn make_emote(id: &str, start: usize, end_exclusive: usize) -> Emote {
        Emote {
            id: id.to_string(),
            char_range: start..end_exclusive,
            code: String::new(),
        }
    }

    #[test]
    fn emotes_to_json_map_single_range() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5)]).unwrap();
        assert_eq!(map["25"], json!(["0-4"]));
    }

    #[test]
    fn emotes_to_json_map_groups_same_id() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5), make_emote("25", 10, 15)]).unwrap();
        assert_eq!(map["25"], json!(["0-4", "10-14"]));
    }

    #[test]
    fn emotes_to_json_map_multiple_ids() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5), make_emote("1902", 6, 11)]).unwrap();
        assert_eq!(map["25"], json!(["0-4"]));
        assert_eq!(map["1902"], json!(["6-10"]));
    }

    #[test]
    fn emotes_to_json_map_empty_returns_none() {
        assert!(emotes_to_json_map(&[]).is_none());
    }

    #[test]
    fn badges_to_json_maps_names_and_versions() {
        let badges = vec![
            Badge {
                name: "moderator".to_string(),
                version: "1".to_string(),
            },
            Badge {
                name: "subscriber".to_string(),
                version: "12".to_string(),
            },
        ];
        let (names, raw) = badges_to_json(&badges);
        assert_eq!(names, vec!["moderator", "subscriber"]);
        assert_eq!(raw["moderator"], json!("1"));
        assert_eq!(raw["subscriber"], json!("12"));
    }

    #[test]
    fn privmsg_to_chat_event_includes_emotes_and_badges() {
        let msg = twitch_irc::message::PrivmsgMessage {
            channel_login: "channel".to_string(),
            channel_id: "1".to_string(),
            sender: twitch_irc::message::TwitchUserBasics {
                id: "2".to_string(),
                login: "viewer".to_string(),
                name: "Viewer".to_string(),
            },
            badge_info: vec![],
            badges: vec![Badge {
                name: "subscriber".to_string(),
                version: "3".to_string(),
            }],
            bits: None,
            name_color: None,
            emotes: vec![make_emote("25", 0, 5)],
            message_id: "msg-1".to_string(),
            server_timestamp: chrono::Utc::now(),
            message_text: "Kappa".to_string(),
            is_action: false,
            source: twitch_irc::message::IRCMessage::parse(
                "@badges=subscriber/3;emotes=25:0-4;display-name=Viewer;user-id=2 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channel :Kappa",
            )
            .unwrap(),
        };

        let evt = privmsg_to_chat_event(&msg, false);
        assert_eq!(evt["emotes"]["25"], json!(["0-4"]));
        assert_eq!(evt["user"]["badges"], json!(["subscriber"]));
        assert_eq!(evt["user"]["badgesRaw"]["subscriber"], json!("3"));
        assert_eq!(evt["message"], json!("Kappa"));
        assert_eq!(evt["self"], json!(false));
    }

    #[test]
    fn add_emote_batch_uses_helix_owner_id_for_channel_sidebar() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([{
            "id": "em1",
            "name": "CoolEmote",
            "images": { "url_1x": "https://example.com/1.png" },
            "emote_type": "subscriptions",
            "owner_id": "999",
            "emote_set_id": "set1"
        }]);
        add_emote_batch(&mut by_id, Some(&helix), None, None, "111");
        let emote = by_id.get("em1").unwrap();
        assert_eq!(emote["ownerId"], json!("999"));
        assert_eq!(emote["ownerType"], json!("channel"));
        assert_eq!(emote["ownerIsSelf"], json!(false));
    }

    #[test]
    fn add_emote_batch_marks_own_channel_as_self() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([{
            "id": "em2",
            "name": "MyEmote",
            "images": {},
            "owner_id": "111"
        }]);
        add_emote_batch(
            &mut by_id,
            Some(&helix),
            Some("channel"),
            Some("111"),
            "111",
        );
        let emote = by_id.get("em2").unwrap();
        assert_eq!(emote["ownerId"], json!("111"));
        assert_eq!(emote["ownerType"], json!("channel"));
        assert_eq!(emote["ownerIsSelf"], json!(true));
    }

    #[test]
    fn add_emote_batch_skips_empty_and_zero_owner_ids() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([
            {
                "id": "g1",
                "name": "GlobalThing",
                "images": {},
                "emote_type": "globals",
                "owner_id": "0"
            },
            {
                "id": "g2",
                "name": "NoOwner",
                "images": {},
                "owner_id": ""
            },
            {
                "id": "c1",
                "name": "RealChannel",
                "images": {},
                "emote_type": "subscriptions",
                "owner_id": "999"
            }
        ]);
        add_emote_batch(&mut by_id, Some(&helix), None, None, "111");
        assert!(by_id.get("g1").unwrap().get("ownerId").unwrap().is_null());
        assert!(by_id.get("g2").unwrap().get("ownerId").unwrap().is_null());
        assert_eq!(by_id.get("c1").unwrap()["ownerId"], json!("999"));
        assert_eq!(by_id.get("c1").unwrap()["ownerType"], json!("channel"));
    }

    #[test]
    fn json_id_string_coerces_numbers() {
        assert_eq!(json_id_string(&json!("123")), Some("123".into()));
        assert_eq!(json_id_string(&json!(456u64)), Some("456".into()));
        assert_eq!(json_id_string(&json!("0")), None);
        assert_eq!(json_id_string(&json!("")), None);
        assert_eq!(json_id_string(&json!(0)), None);
    }

    #[test]
    fn emotes_from_message_text_matches_tokens() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        by_name.insert("PogChamp".into(), "305954156".into());
        let map = emotes_from_message_text("hi Kappa there PogChamp", &by_name).unwrap();
        assert_eq!(map["25"], json!(["3-7"]));
        assert_eq!(map["305954156"], json!(["15-22"]));
    }

    #[test]
    fn emotes_from_message_text_groups_repeats() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        let map = emotes_from_message_text("Kappa Kappa", &by_name).unwrap();
        assert_eq!(map["25"], json!(["0-4", "6-10"]));
    }

    #[test]
    fn emotes_from_message_text_no_match_returns_none() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        assert!(emotes_from_message_text("hello world", &by_name).is_none());
    }
}
