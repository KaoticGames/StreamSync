//! HTTP routes (port of overlay-server/server.js Express routes).

use crate::app_state::{normalize_chat_profile_id, AppState};
use crate::config_types::{
    normalize_display_mode, normalize_popup_duration, resolve_events_overlay_profile,
    ChatOverlayProfile,
};
use crate::kick;
use crate::storage;
use crate::broadcast::make_dock_event;
use crate::streamelements::{
    self, map_overlay_to_profile, save_raw_overlay, SeClient, SeSession,
};
use crate::twitch::{self, TwitchServices};
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State, WebSocketUpgrade},
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;

#[derive(Clone)]
pub struct ServerContext {
    pub state: Arc<AppState>,
    pub twitch: Arc<TwitchServices>,
}

pub fn build_router(ctx: ServerContext) -> Router {
    let repo = ctx.state.repo_root.clone();
    let fonts = ctx.state.paths.fonts_dir.clone();
    let events_media = ctx.state.paths.events_media_dir.clone();

    Router::new()
        .route("/", get(root_redirect))
        .route("/health", get(health))
        .route("/api/status", get(api_status))
        .route("/overlay/chat", get(overlay_chat))
        .route("/overlay/events", get(overlay_events))
        .route("/overlay/kick-chat", get(overlay_kick_chat))
        .route("/overlay/kick-events", get(overlay_kick_events))
        .route("/events-studio.html", get(events_studio))
        .route("/dock/chat", get(dock_chat))
        .route("/dock/events", get(dock_events))
        .route("/dock/kick-chat", get(dock_kick_chat))
        .route("/dock/kick-events", get(dock_kick_events))
        .route("/auth/twitch/callback", get(auth_callback))
        .route("/auth/kick/callback", get(auth_kick_callback))
        .route("/auth/streamelements/callback", get(auth_streamelements_callback))
        .route("/config/:profile_id.json", get(config_profile_json))
        .route("/api/chat/dock-config", post(post_chat_dock_config))
        .route("/api/events/dock-config", get(get_events_dock_config).post(post_events_dock_config))
        .route("/api/chat/upload-font", post(post_upload_font))
        .route("/api/events/upload-media", post(post_upload_events_media))
        .route("/api/chat/overlay-profiles", get(get_overlay_profiles))
        .route(
            "/api/chat/overlay-config",
            get(get_overlay_config)
                .post(post_overlay_config)
                .delete(delete_overlay_config),
        )
        .route("/api/twitch/auth-url", get(get_auth_url))
        .route("/api/twitch/set-token", post(post_set_token))
        .route("/api/twitch/connection-key", post(post_connection_key))
        .route("/api/twitch/use-connection", post(post_use_connection))
        .route("/api/twitch/remove-connection", post(post_remove_connection))
        .route("/api/twitch/disconnect", post(post_disconnect))
        .route("/api/kick/auth-url", get(get_kick_auth_url))
        .route("/api/kick/redeem", post(post_kick_redeem))
        .route("/api/kick/disconnect", post(post_kick_disconnect))
        .route("/api/kick/chat", post(post_kick_chat))
        .route("/api/twitch/badges/all", get(get_badges))
        .route("/api/twitch/emotes/all", get(get_emotes))
        .route("/api/events/overlay-profiles", get(get_events_profiles))
        .route(
            "/api/events/overlay-config",
            get(get_events_overlay_config)
                .post(post_events_overlay_config)
                .delete(delete_events_overlay_config),
        )
        .route("/api/events/test-alert", post(post_test_alert))
        .route(
            "/api/streamelements/session",
            get(get_se_session)
                .post(post_se_session)
                .delete(delete_se_session),
        )
        .route("/api/streamelements/overlays", get(get_se_overlays))
        .route("/api/streamelements/import", post(post_se_import))
        .route("/ws/feed", get(ws_feed))
        .route("/google-fonts.css", get(get_google_fonts_css))
        .route("/google-fonts/file", get(get_google_fonts_file))
        .nest_service("/fonts", ServeDir::new(fonts))
        .nest_service("/events-media", ServeDir::new(events_media))
        .fallback_service(ServeDir::new(repo))
        // Local overlay UI must never be sticky-cached by WebView2.
        .layer(SetResponseHeaderLayer::overriding(
            header::CACHE_CONTROL,
            header::HeaderValue::from_static("no-store"),
        ))
        .layer(CorsLayer::permissive())
        // Events Studio embeds / uploads can exceed Axum's default 2MB body limit
        // (ERR_CONNECTION_ABORTED). Media uploads go to disk; configs may still be large.
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(ctx)
}

async fn overlay_chat(State(ctx): State<ServerContext>) -> Response {
    serve_html("chat-overlay.html", &ctx).await
}
async fn overlay_events(State(ctx): State<ServerContext>) -> Response {
    serve_html("events-overlay.html", &ctx).await
}
async fn overlay_kick_chat(State(ctx): State<ServerContext>) -> Response {
    serve_html_platform("chat-overlay.html", "kick", &ctx).await
}
async fn overlay_kick_events(State(ctx): State<ServerContext>) -> Response {
    serve_html_platform("events-overlay.html", "kick", &ctx).await
}
async fn events_studio(State(ctx): State<ServerContext>) -> Response {
    serve_html("events-studio.html", &ctx).await
}
async fn dock_chat(State(ctx): State<ServerContext>) -> Response {
    serve_html("chat-dock.html", &ctx).await
}
async fn dock_events(State(ctx): State<ServerContext>) -> Response {
    serve_html("events-dock.html", &ctx).await
}
async fn dock_kick_chat(State(ctx): State<ServerContext>) -> Response {
    serve_html_platform("chat-dock.html", "kick", &ctx).await
}
async fn dock_kick_events(State(ctx): State<ServerContext>) -> Response {
    serve_html_platform("events-dock.html", "kick", &ctx).await
}

async fn serve_html(file: &'static str, ctx: &ServerContext) -> Response {
    serve_html_platform(file, "twitch", ctx).await
}

async fn serve_html_platform(file: &'static str, platform: &str, ctx: &ServerContext) -> Response {
    let path = ctx.state.overlay_server_dir.join(file);
    match tokio::fs::read_to_string(&path).await {
        Ok(mut body) => {
            if platform == "kick" {
                let inject = r#"<script>window.STREAMSYNC_DOCK_PLATFORM="kick";</script>"#;
                if let Some(i) = body.find("<head>") {
                    body.insert_str(i + 6, inject);
                } else {
                    body = format!("{inject}{body}");
                }
            }
            Response::builder()
                .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                .body(Body::from(body))
                .unwrap()
        }
        Err(_) => (
            StatusCode::NOT_FOUND,
            format!("{file} not found"),
        )
            .into_response(),
    }
}

/// Repo has `shell.html` but no `index.html`; avoid a bare `/` 404 in the browser.
async fn root_redirect() -> Redirect {
    Redirect::to("/shell.html")
}

async fn health() -> Json<Value> {
    Json(json!({ "ok": true, "service": "overlay-server", "runtime": "rust" }))
}

#[derive(Debug, Deserialize)]
struct GoogleFontQuery {
    family: String,
}

#[derive(Debug, Deserialize)]
struct GoogleFontFileQuery {
    u: String,
}

fn rewrite_gstatic_font_urls(css: &str) -> String {
    let marker = "https://fonts.gstatic.com/";
    let mut remaining = css;
    let mut out = String::with_capacity(css.len() + 128);
    while let Some(idx) = remaining.find(marker) {
        out.push_str(&remaining[..idx]);
        let after = &remaining[idx..];
        let end = after
            .find(|c: char| matches!(c, ')' | '\'' | '"' | ' ' | '\n' | '\r' | '\t'))
            .unwrap_or(after.len());
        let url = &after[..end];
        out.push_str("/google-fonts/file?u=");
        out.push_str(&urlencoding::encode(url));
        remaining = &after[end..];
    }
    out.push_str(remaining);
    out
}

/// Proxy Google Fonts CSS through localhost and rewrite gstatic URLs to a local
/// file proxy so Tauri WebView2 can load preview webfonts same-origin.
async fn get_google_fonts_css(Query(query): Query<GoogleFontQuery>) -> Result<Response, StatusCode> {
    let family = query.family.trim();
    if family.is_empty() || family.len() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }
    if family.contains('<') || family.contains('>') {
        return Err(StatusCode::BAD_REQUEST);
    }

    let google_url = format!(
        "https://fonts.googleapis.com/css2?family={}:wght@300;400;500;600;700;800&display=swap",
        urlencoding::encode(family)
    );

    let client = reqwest::Client::new();
    let res = client
        .get(&google_url)
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if !res.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }

    let css = res
        .text()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let rewritten = rewrite_gstatic_font_urls(&css);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/css; charset=utf-8")
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(rewritten))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn get_google_fonts_file(
    Query(query): Query<GoogleFontFileQuery>,
) -> Result<Response, StatusCode> {
    let url = query.u.trim();
    if !url.starts_with("https://fonts.gstatic.com/") {
        return Err(StatusCode::BAD_REQUEST);
    }

    let client = reqwest::Client::new();
    let res = client
        .get(url)
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if !res.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }

    let content_type = res
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("font/woff2")
        .to_string();
    let bytes = res
        .bytes()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CACHE_CONTROL, "public, max-age=604800")
        .body(Body::from(bytes))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn api_status(State(ctx): State<ServerContext>) -> Json<Value> {
    let tw = ctx.state.twitch.read().await;
    let personal = ctx.state.personal_tokens.read().await;
    let delegated = ctx.state.delegated.read().await;
    let active = *ctx.state.active_mode.read().await;
    let takeover = active == crate::config_types::TwitchActiveMode::Delegated && delegated.is_some();
    let personal_saved = personal.access_token.is_some() && personal.login.is_some();
    let delegated_saved = delegated.is_some();
    let se_connected = streamelements::load_session(&ctx.state.paths)
        .ok()
        .flatten()
        .map(|s| {
            json!({
                "connected": true,
                "accountId": s.account_id,
                "username": s.username,
            })
        })
        .unwrap_or_else(|| json!({ "connected": false }));
    let kick_tokens = ctx.state.kick.read().await.tokens.clone();
    let personal_kick = ctx.state.personal_kick.read().await.clone();
    let kick_via_takeover = takeover
        && ctx
            .state
            .delegated
            .read()
            .await
            .as_ref()
            .and_then(|d| d.kick_id.clone())
            .is_some();
    Json(json!({
        "twitch": {
            "connected": tw.connected,
            "channel": tw.channel,
            "login": tw.tokens.login,
            "userId": tw.tokens.user_id,
            "mode": if takeover { "delegated" } else { "local" },
            "takeover": takeover,
            "label": delegated.as_ref().and_then(|d| d.label.clone()),
            "connection_expires_at": delegated.as_ref().and_then(|d| d.connection_expires_at.clone()),
            "display_name": delegated.as_ref().and_then(|d| d.display_name.clone()),
            "accounts": {
                "local": {
                    "saved": personal_saved,
                    "active": active == crate::config_types::TwitchActiveMode::Local && personal_saved,
                    "login": personal.login,
                    "userId": personal.user_id,
                },
                "delegated": {
                    "saved": delegated_saved,
                    "active": takeover,
                    "login": delegated.as_ref().map(|d| d.channel_login.clone()),
                    "userId": delegated.as_ref().map(|d| d.channel_twitch_id.clone()),
                    "label": delegated.as_ref().and_then(|d| d.label.clone()),
                    "display_name": delegated.as_ref().and_then(|d| d.display_name.clone()),
                    "connection_expires_at": delegated.as_ref().and_then(|d| d.connection_expires_at.clone()),
                },
            },
        },
        "kick": {
            "connected": kick_tokens.is_linked(),
            "feedConnected": ctx.state.kick.read().await.connected,
            "login": kick_tokens.login,
            "kickId": kick_tokens.kick_id,
            "viaTakeover": kick_via_takeover,
            "personalSaved": personal_kick.is_linked(),
        },
        "streamelements": se_connected,
    }))
}

async fn auth_callback() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        AUTH_CALLBACK_HTML,
    )
}

async fn auth_kick_callback() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        AUTH_KICK_CALLBACK_HTML,
    )
}

async fn auth_streamelements_callback() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        SE_AUTH_CALLBACK_HTML,
    )
}

const SE_AUTH_CALLBACK_HTML: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"/><title>Stream Sync – StreamElements</title></head>
<body style="font-family:system-ui;padding:24px;background:#111;color:#eee;">
<h2>Connecting StreamElements…</h2>
<script>
(function(){
  function parseHash(hash){
    const out={};
    const h=(hash||"").replace(/^#/,"");
    if(!h) return out;
    for(const part of h.split("&")){
      const i=part.indexOf("=");
      if(i<0) continue;
      const k=decodeURIComponent(part.slice(0,i));
      const v=decodeURIComponent(part.slice(i+1));
      if(k) out[k]=v;
    }
    return out;
  }
  async function run(){
    const h=parseHash(window.location.hash);
    const jwt=h.jwt||"";
    const accountId=h.accountId||"";
    if(!jwt||!accountId){
      document.body.innerHTML+="<p style='color:#f87171;'>Missing jwt or accountId in callback.</p>";
      return;
    }
    const resp=await fetch("/api/streamelements/session",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({ jwt, accountId })
    });
    const data=await resp.json().catch(()=>({}));
    if(!resp.ok||!data.ok) throw new Error(data.error||("HTTP "+resp.status));
    document.body.innerHTML="<h2>StreamElements connected</h2><p>You can close this window.</p>";
    setTimeout(()=>{ try{ window.close(); }catch(e){} }, 600);
  }
  run().catch(e=>{
    console.error(e);
    document.body.innerHTML+="<p style='color:#f87171;'>Failed to save StreamElements session.</p>";
  });
})();
</script></body></html>"#;

const AUTH_CALLBACK_HTML: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"/><title>Stream Sync – Twitch Connect</title></head>
<body style="font-family:system-ui;padding:24px;">
<h2>Finishing Twitch connection…</h2>
<script>
(function(){
  function parseHash(hash){
    const out={};
    const h=(hash||"").replace(/^#/,"");
    if(!h) return out;
    for(const part of h.split("&")){
      const [k,v]=part.split("=");
      if(k) out[decodeURIComponent(k)]=decodeURIComponent(v||"");
    }
    return out;
  }
  async function run(){
    const h=parseHash(window.location.hash);
    const accessToken=h.access_token||"";
    if(!accessToken){ document.body.innerHTML+="<p style='color:#b91c1c;'>Missing access_token.</p>"; return; }
    const resp=await fetch("/api/twitch/set-token",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({
        accessToken,
        expiresIn:h.expires_in?Number(h.expires_in):null,
        scope:h.scope?h.scope.split(" "):null,
        tokenType:h.token_type||""
      })
    });
    const data=await resp.json().catch(()=>({}));
    if(!resp.ok||!data.ok) throw new Error(data.error||("HTTP "+resp.status));
    document.body.innerHTML="<h2>Stream Sync connected to Twitch</h2>";
    setTimeout(()=>window.close(),500);
  }
  run().catch(e=>{ console.error(e); document.body.innerHTML+="<p style='color:#b91c1c;'>Failed to finalize connection.</p>"; });
})();
</script></body></html>"#;

const AUTH_KICK_CALLBACK_HTML: &str = r#"<!doctype html>
<html><head><meta charset="utf-8"/><title>Stream Sync – Kick Connect</title></head>
<body style="font-family:system-ui;padding:24px;background:#111;color:#eee;">
<h2>Finishing Kick connection…</h2>
<script>
(function(){
  async function run(){
    const params=new URLSearchParams(window.location.search);
    const oauthErr=params.get("error");
    if(oauthErr){
      const detail=params.get("error_description")||oauthErr;
      throw new Error("Kick authorization failed: "+detail);
    }
    const code=(params.get("code")||"").trim();
    if(!code){
      document.body.innerHTML+="<p style='color:#f87171;'>Missing Kick authorization code.</p>";
      return;
    }
    const resp=await fetch("/api/kick/redeem",{
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({code})
    });
    const data=await resp.json().catch(()=>({}));
    if(!resp.ok||!data.ok) throw new Error(data.error||("HTTP "+resp.status));
    document.body.innerHTML="<h2>Stream Sync connected to Kick</h2><p>You can close this window.</p>";
    setTimeout(()=>{ try{ window.close(); }catch(e){} }, 600);
  }
  run().catch(e=>{
    console.error(e);
    document.body.innerHTML="<h2>Kick connect failed</h2><p style='color:#f87171;'>"+String(e.message||e)+"</p>";
  });
})();
</script></body></html>"#;

async fn config_profile_json(
    State(ctx): State<ServerContext>,
    Path(profile_id): Path<String>,
) -> Json<Value> {
    let dock = ctx.state.dock_config.read().await;
    let profile = dock
        .profiles
        .get(&profile_id)
        .cloned()
        .unwrap_or(crate::config_types::DockProfile {
            font_size: 13,
            show_timestamps: true,
            show_badges: true,
        });
    Json(json!({
        "id": profile_id,
        "font": { "family": "Segoe UI", "size": profile.font_size, "lineHeight": 1.35 },
        "chat": {
            "enabled": true,
            "showBadges": profile.show_badges,
            "showEmotes": true,
            "showTimestamps": profile.show_timestamps,
        }
    }))
}

async fn post_chat_dock_config(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let profile_id = body
        .get("profileId")
        .and_then(|v| v.as_str())
        .unwrap_or("chat-default")
        .to_string();
    let mut dock = ctx.state.dock_config.write().await;
    let profile = dock.profiles.entry(profile_id.clone()).or_insert_with(|| {
        crate::config_types::DockProfile {
            font_size: 13,
            show_timestamps: true,
            show_badges: true,
        }
    });
    if let Some(n) = body.get("fontSize").and_then(|v| v.as_u64()) {
        profile.font_size = n as u32;
    }
    if let Some(b) = body.get("showBadges").and_then(|v| v.as_bool()) {
        profile.show_badges = b;
    }
    if let Some(t) = body.get("showTimestamps").and_then(|v| v.as_bool()) {
        profile.show_timestamps = t;
    }
    let profile_json = json!({
        "fontSize": profile.font_size,
        "showBadges": profile.show_badges,
        "showTimestamps": profile.show_timestamps,
    });
    drop(dock);
    ctx.state.save_dock().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.state
        .feed
        .broadcast_all(&json!({
            "type": "dock-config-updated",
            "profileId": profile_id,
            "profile": profile_json,
        }))
        .await;
    Ok(Json(json!({ "ok": true, "profileId": profile_id, "profile": profile_json })))
}

async fn get_events_dock_config(State(ctx): State<ServerContext>) -> Json<Value> {
    let cfg = ctx.state.events_dock_config.read().await;
    Json(json!({ "ok": true, "config": serde_json::to_value(&*cfg).unwrap_or(json!({})) }))
}

async fn post_events_dock_config(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let mut cfg = ctx.state.events_dock_config.write().await;
    if let Some(fs) = body.get("fontSize").and_then(|v| v.as_u64()) {
        cfg.font_size = fs as u32;
    }
    if let Some(v) = body.get("showTimestamps").and_then(|v| v.as_bool()) {
        cfg.show_timestamps = v;
    }
    if let Some(v) = body.get("showBadges").and_then(|v| v.as_bool()) {
        cfg.show_badges = v;
    }
    if let Some(events) = body.get("events").and_then(|v| v.as_object()) {
        for (k, val) in events {
            if let Some(b) = val.as_bool() {
                match k.as_str() {
                    "follow" => cfg.events.follow = b,
                    "sub" => cfg.events.sub = b,
                    "resub" => cfg.events.resub = b,
                    "gift" => cfg.events.gift = b,
                    "bits" => cfg.events.bits = b,
                    "raid" => cfg.events.raid = b,
                    "redeem" => cfg.events.redeem = b,
                    "hypetrain" => cfg.events.hypetrain = b,
                    "announce" => cfg.events.announce = b,
                    _ => {}
                }
            }
        }
    }
    let out = serde_json::to_value(&*cfg).unwrap_or(json!({}));
    drop(cfg);
    ctx.state.save_dock().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.state
        .feed
        .broadcast_all(&json!({ "type": "events-dock-config", "config": out }))
        .await;
    Ok(Json(json!({ "ok": true, "config": out })))
}

async fn post_upload_font(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let profile_id = normalize_chat_profile_id(
        body.get("profileId")
            .and_then(|v| v.as_str())
            .unwrap_or("chat-default"),
    );
    let file_name = body.get("fileName").and_then(|v| v.as_str()).ok_or(StatusCode::BAD_REQUEST)?;
    let b64 = body
        .get("contentBase64")
        .and_then(|v| v.as_str())
        .ok_or(StatusCode::BAD_REQUEST)?;
    let bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        b64,
    )
    .map_err(|_| StatusCode::BAD_REQUEST)?;
    if bytes.len() < 16 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let ext = PathBuf::from(file_name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("ttf")
        .to_lowercase();
    if !["ttf", "otf", "woff", "woff2"].contains(&ext.as_str()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let stored = format!(
        "{}-{}.{}",
        file_name.trim_end_matches(&format!(".{ext}")),
        chrono::Utc::now().timestamp_millis(),
        ext
    );
    let dest = ctx.state.paths.fonts_dir.join(&stored);
    storage::write_file_atomic(&dest, &bytes).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let font_url = format!("/fonts/{stored}");
    let font_family = format!("OverlayLocal_{profile_id}");
    let settings = {
        let mut overlay = ctx.state.overlay_config.write().await;
        let profile = overlay
            .profiles
            .entry(profile_id.clone())
            .or_insert_with(ChatOverlayProfile::default);
        profile.font_family = font_family.clone();
        profile.local_font_url = Some(font_url.clone());
        overlay_profile_api_json(&profile_id, profile)
    };
    ctx.state
        .save_overlay()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.state
        .feed
        .broadcast_all(&json!({
            "type": "overlay-config-updated",
            "profileId": profile_id,
            "settings": settings,
        }))
        .await;
    Ok(Json(json!({
        "ok": true,
        "profileId": profile_id,
        "fontFamily": font_family,
        "fontUrl": font_url,
    })))
}

fn sanitize_events_profile_id(raw: &str) -> String {
    let s: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let t = s.trim_matches('_');
    if t.is_empty() {
        "default".into()
    } else {
        t.to_string()
    }
}

fn sanitize_media_filename(raw: &str) -> String {
    let path = PathBuf::from(raw);
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("asset.bin")
        .to_string();
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        "asset.bin".into()
    } else {
        cleaned
    }
}

/// Save an Events Studio visual/audio file under `events-media/{profile}/` and return a URL path.
/// Avoids embedding multi‑MB data URLs into overlay-config JSON (which hit Axum's body limit).
async fn post_upload_events_media(
    State(ctx): State<ServerContext>,
    mut multipart: Multipart,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut profile_id = String::from("default");
    let mut file_name = String::from("asset.bin");
    let mut bytes: Option<Vec<u8>> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": format!("multipart: {e}") })),
        )
    })? {
        let name = field.name().unwrap_or("").to_string();
        if name == "profileId" {
            if let Ok(text) = field.text().await {
                let t = text.trim();
                if !t.is_empty() {
                    profile_id = sanitize_events_profile_id(t);
                }
            }
            continue;
        }
        if name == "file" {
            if let Some(fname) = field.file_name().map(|s| s.to_string()) {
                file_name = sanitize_media_filename(&fname);
            }
            let data = field.bytes().await.map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "ok": false, "error": format!("read file: {e}") })),
                )
            })?;
            if data.len() > 40 * 1024 * 1024 {
                return Err((
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(json!({
                        "ok": false,
                        "error": "File too large (max 40MB). Use a smaller asset or a URL."
                    })),
                ));
            }
            if data.is_empty() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "ok": false, "error": "Empty file" })),
                ));
            }
            bytes = Some(data.to_vec());
            continue;
        }
        // Drain unknown fields
        let _ = field.bytes().await;
    }

    let Some(data) = bytes else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "Missing file field" })),
        ));
    };

    let path = PathBuf::from(&file_name);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("asset")
        .to_string();
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("bin")
        .to_lowercase();
    let stored = format!(
        "{}-{}.{}",
        sanitize_media_filename(&stem).trim_end_matches('.'),
        chrono::Utc::now().timestamp_millis(),
        ext
    );

    let dir = ctx.state.paths.events_media_dir.join(&profile_id);
    std::fs::create_dir_all(&dir).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": format!("mkdir: {e}") })),
        )
    })?;
    let dest = dir.join(&stored);
    storage::write_file_atomic(&dest, &data).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": format!("write: {e}") })),
        )
    })?;

    let url = format!("/events-media/{profile_id}/{stored}");
    Ok(Json(json!({
        "ok": true,
        "profileId": profile_id,
        "url": url,
        "fileName": file_name,
    })))
}

async fn get_overlay_profiles(State(ctx): State<ServerContext>) -> Json<Value> {
    let overlay = ctx.state.overlay_config.read().await;
    let mut ids: Vec<String> = overlay.profiles.keys().cloned().collect();
    if !ids.iter().any(|id| id == "chat-default") {
        ids.push("chat-default".into());
    }
    ids.retain(|id| {
        id == "chat-default"
            || id.starts_with("chat-")
            || (!id.starts_with("events-") && id != "events-default" && !id.starts_with("profile-"))
    });
    ids.sort();
    let profiles: Vec<Value> = ids
        .into_iter()
        .map(|id| {
            let cfg = overlay.profiles.get(&id);
            let name = cfg
                .and_then(|c| c.profile_name.clone())
                .or_else(|| cfg.and_then(|c| c.display_name.clone()))
                .or_else(|| cfg.and_then(|c| c.name.clone()))
                .unwrap_or_else(|| {
                    if id == "chat-default" {
                        "Default".into()
                    } else {
                        id.clone()
                    }
                });
            json!({ "id": id, "name": name })
        })
        .collect();
    Json(json!({ "ok": true, "profiles": profiles }))
}

#[derive(Deserialize)]
struct ProfileQuery {
    profile: Option<String>,
}

fn overlay_profile_api_json(profile_id: &str, p: &ChatOverlayProfile) -> Value {
    json!({
        "profileId": profile_id,
        "showTimestamps": p.show_timestamps,
        "showBadges": p.show_badges,
        "fontSize": p.font_size,
        "fontFamily": p.font_family,
        "fontUrl": p.local_font_url,
        "textRotate": p.text_rotate,
        "textSkew": p.text_skew,
        "feedDirection": p.feed_direction,
        "messageStyle": p.message_style,
        "bubbleRadius": p.bubble_radius,
        "bubbleColorMode": p.bubble_color_mode,
        "bubbleColor": p.bubble_color,
        "bgMode": p.bg_mode,
        "bgColor": p.bg_color,
        "bgGradient": p.bg_gradient,
        "displayMode": normalize_display_mode(&p.display_mode),
        "popupDuration": normalize_popup_duration(p.popup_duration),
        "strokeEnabled": p.stroke_enabled,
        "strokeColor": p.stroke_color,
        "strokeWidth": p.stroke_width,
        "bubbleAlpha": p.bubble_alpha,
        "popupExitStyle": p.popup_exit_style,
    })
}

async fn get_overlay_config(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
) -> Json<Value> {
    let profile_id = normalize_chat_profile_id(q.profile.as_deref().unwrap_or("chat-default"));
    let overlay = ctx.state.overlay_config.read().await;
    let p = overlay
        .profiles
        .get(&profile_id)
        .or_else(|| overlay.profiles.get("chat-default"))
        .cloned()
        .unwrap_or_default();
    Json(overlay_profile_api_json(&profile_id, &p))
}

async fn post_overlay_config(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let profile_id = normalize_chat_profile_id(
        body.get("profileId")
            .and_then(|v| v.as_str())
            .unwrap_or("chat-default"),
    );
    let mut overlay = ctx.state.overlay_config.write().await;
    let profile = overlay
        .profiles
        .entry(profile_id.clone())
        .or_insert_with(ChatOverlayProfile::default);
    merge_overlay_profile(profile, &body);
    let settings = overlay_profile_api_json(&profile_id, profile);
    drop(overlay);
    ctx.state
        .save_overlay()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.state
        .feed
        .broadcast_all(&json!({
            "type": "overlay-config-updated",
            "profileId": profile_id,
            "settings": settings,
        }))
        .await;
    Ok(Json(json!({ "ok": true, "profileId": profile_id })))
}

fn merge_overlay_profile(p: &mut ChatOverlayProfile, body: &Value) {
    if let Some(v) = body.get("showTimestamps").and_then(|v| v.as_bool()) {
        p.show_timestamps = v;
    }
    if let Some(v) = body.get("showBadges").and_then(|v| v.as_bool()) {
        p.show_badges = v;
    }
    if let Some(v) = body.get("fontSize").and_then(|v| v.as_f64()) {
        if v > 0.0 {
            p.font_size = v as u32;
        }
    }
    if let Some(v) = body.get("fontFamily").and_then(|v| v.as_str()) {
        if !v.trim().is_empty() {
            p.font_family = v.trim().to_string();
        }
    }
    // Allow clearing local font when switching back to Google fonts
    if let Some(v) = body.get("localFontUrl").or_else(|| body.get("fontUrl")) {
        if v.is_null() {
            p.local_font_url = None;
        } else if let Some(s) = v.as_str() {
            let t = s.trim();
            if t.is_empty() {
                p.local_font_url = None;
            } else {
                p.local_font_url = Some(t.to_string());
            }
        }
    }
    if let Some(v) = body.get("textRotate").and_then(|v| v.as_f64()) {
        p.text_rotate = v;
    }
    if let Some(v) = body.get("textSkew").and_then(|v| v.as_f64()) {
        p.text_skew = v;
    }
    if let Some(v) = body.get("feedDirection").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.feed_direction = v.to_string();
        }
    }
    if let Some(v) = body.get("messageStyle").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.message_style = v.to_string();
        }
    }
    if let Some(v) = body.get("bubbleRadius").and_then(|v| v.as_f64()) {
        p.bubble_radius = v;
    }
    if let Some(v) = body.get("bubbleColorMode").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.bubble_color_mode = v.to_string();
        }
    }
    if let Some(v) = body.get("bubbleColor").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.bubble_color = v.to_string();
        }
    }
    if let Some(v) = body.get("bubbleAlpha").and_then(|v| v.as_f64()) {
        if (0.0..=1.0).contains(&v) {
            p.bubble_alpha = v;
        }
    }
    if let Some(v) = body.get("strokeEnabled").and_then(|v| v.as_bool()) {
        p.stroke_enabled = v;
    }
    if let Some(v) = body.get("strokeColor").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.stroke_color = v.to_string();
        }
    }
    if let Some(v) = body.get("strokeWidth").and_then(|v| v.as_f64()) {
        if v >= 0.0 {
            p.stroke_width = v;
        }
    }
    if let Some(v) = body.get("bgMode").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.bg_mode = v.to_string();
        }
    }
    if let Some(v) = body.get("bgColor").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.bg_color = v.to_string();
        }
    }
    if let Some(v) = body.get("bgGradient").and_then(|v| v.as_str()) {
        p.bg_gradient = v.to_string();
    }
    if let Some(v) = body.get("displayMode").and_then(|v| v.as_str()) {
        p.display_mode = normalize_display_mode(v);
    }
    if let Some(v) = body.get("popupDuration").and_then(|v| v.as_f64()) {
        p.popup_duration = normalize_popup_duration(v.max(0.0) as u32);
    }
    if let Some(v) = body.get("popupExitStyle").and_then(|v| v.as_str()) {
        if !v.is_empty() {
            p.popup_exit_style = v.to_string();
        }
    }
}

async fn delete_overlay_config(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let profile_id = normalize_chat_profile_id(q.profile.as_deref().unwrap_or("chat-default"));
    if profile_id == "chat-default" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "cannot-delete-default" })),
        ));
    }
    ctx.state.overlay_config.write().await.profiles.remove(&profile_id);
    ctx.state.save_overlay().await.ok();
    Ok(Json(json!({ "ok": true, "profileId": profile_id })))
}

async fn get_auth_url(State(ctx): State<ServerContext>) -> Response {
    if ctx.state.client_id.is_empty() {
        let hint = format!(
            "TWITCH_CLIENT_ID is not configured. Add TWITCH_CLIENT_ID=... to {} (template: rust/config/env.example) or {}, then restart Stream Sync.",
            ctx.state.rust_root.join(".env").display(),
            ctx.state.paths.root.join(".env").display(),
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": hint })),
        )
            .into_response();
    }
    let scopes = [
        "chat:read", "chat:edit", "user:read:chat", "user:read:emotes",
        "moderator:read:followers", "channel:read:subscriptions", "bits:read",
        "channel:read:redemptions",
        "moderator:manage:chat_settings", "moderator:manage:banned_users",
    ];
    let url = format!(
        "https://id.twitch.tv/oauth2/authorize?client_id={}&redirect_uri={}&response_type=token&scope={}",
        urlencoding::encode(&ctx.state.client_id),
        urlencoding::encode(&ctx.state.redirect_uri),
        urlencoding::encode(&scopes.join(" ")),
    );
    Json(json!({ "ok": true, "url": url })).into_response()
}

async fn post_set_token(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    twitch::apply_set_token(ctx.state.clone(), ctx.twitch.clone(), body)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            )
        })?;
    Ok(Json(json!({ "ok": true })))
}

async fn post_connection_key(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let key = body
        .get("key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if key.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": "missing_key", "message": "Paste a connection key." })),
        ));
    }
    twitch::apply_connection_key(ctx.state.clone(), ctx.twitch.clone(), key)
        .await
        .map_err(|e| {
            if let Some((code, message, status)) = twitch::connection_key_error_parts(&e) {
                (
                    status,
                    Json(json!({ "ok": false, "error": code, "message": message })),
                )
            } else {
                let msg = e.to_string();
                (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({ "ok": false, "error": "request_failed", "message": msg })),
                )
            }
        })?;
    let login = ctx
        .state
        .twitch
        .read()
        .await
        .tokens
        .login
        .clone();
    Ok(Json(json!({ "ok": true, "login": login, "takeover": true })))
}

fn parse_connection_mode(body: &Value) -> Result<crate::config_types::TwitchActiveMode, (StatusCode, Json<Value>)> {
    let mode = body
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    match mode.as_str() {
        "local" | "personal" => Ok(crate::config_types::TwitchActiveMode::Local),
        "delegated" | "takeover" => Ok(crate::config_types::TwitchActiveMode::Delegated),
        _ => Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "ok": false,
                "error": "invalid_mode",
                "message": "mode must be \"local\" or \"delegated\"."
            })),
        )),
    }
}

async fn post_use_connection(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mode = parse_connection_mode(&body)?;
    twitch::use_connection(ctx.state.clone(), ctx.twitch.clone(), mode)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "ok": false, "error": "switch_failed", "message": e.to_string() })),
            )
        })?;
    Ok(Json(json!({
        "ok": true,
        "mode": match mode {
            crate::config_types::TwitchActiveMode::Local => "local",
            crate::config_types::TwitchActiveMode::Delegated => "delegated",
        }
    })))
}

async fn post_remove_connection(
    State(ctx): State<ServerContext>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mode = parse_connection_mode(&body)?;
    twitch::remove_connection(ctx.state.clone(), ctx.twitch.clone(), mode)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": "remove_failed", "message": e.to_string() })),
            )
        })?;
    Ok(Json(json!({ "ok": true })))
}

async fn post_disconnect(State(ctx): State<ServerContext>) -> Json<Value> {
    let _ = twitch::disconnect_twitch(ctx.state.clone(), ctx.twitch.clone()).await;
    Json(json!({ "ok": true }))
}

async fn get_kick_auth_url(State(ctx): State<ServerContext>) -> Json<Value> {
    Json(json!({ "ok": true, "url": kick::auth_url(&ctx.state) }))
}

async fn post_kick_redeem(
    State(ctx): State<ServerContext>,
    Json(body): Json<kick::KickRedeemBody>,
) -> Response {
    match kick::redeem_stream_sync_code(ctx.state.clone(), &body.code).await {
        Ok(tokens) => Json(json!({
            "ok": true,
            "login": tokens.login,
            "kickId": tokens.kick_id,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn post_kick_disconnect(State(ctx): State<ServerContext>) -> Json<Value> {
    let _ = kick::disconnect_personal(ctx.state.clone()).await;
    Json(json!({ "ok": true }))
}

#[derive(Debug, Deserialize)]
struct KickChatBody {
    message: String,
}

async fn post_kick_chat(
    State(ctx): State<ServerContext>,
    Json(body): Json<KickChatBody>,
) -> Response {
    match kick::send_chat_from_dock(ctx.state.clone(), &body.message).await {
        Ok(()) => Json(json!({ "ok": true })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_badges(State(ctx): State<ServerContext>) -> Response {
    match twitch::get_merged_badges(&ctx.state, &ctx.twitch).await {
        Ok(v) => Json(v).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_emotes(State(ctx): State<ServerContext>) -> Response {
    match twitch::get_merged_emotes(&ctx.state, &ctx.twitch).await {
        Ok(list) => Json(json!({ "ok": true, "emotes": list })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn get_events_profiles(State(ctx): State<ServerContext>) -> Json<Value> {
    let cfg = ctx.state.events_overlay_config.read().await;
    let mut profiles: Vec<Value> = cfg
        .profiles
        .iter()
        .map(|(id, profile)| {
            let name = profile
                .get("profileName")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(id.as_str());
            json!({ "id": id, "name": name })
        })
        .collect();
    profiles.sort_by(|a, b| {
        let id_a = a.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let id_b = b.get("id").and_then(|v| v.as_str()).unwrap_or("");
        if id_a == "default" {
            return std::cmp::Ordering::Less;
        }
        if id_b == "default" {
            return std::cmp::Ordering::Greater;
        }
        id_a.cmp(id_b)
    });
    Json(json!({ "ok": true, "profiles": profiles }))
}

async fn get_events_overlay_config(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
) -> Json<Value> {
    let id = q.profile.as_deref().unwrap_or("default");
    let cfg = ctx.state.events_overlay_config.read().await;
    let exists = cfg.profiles.contains_key(id);
    let stored = cfg.profiles.get(id).cloned();
    let config = resolve_events_overlay_profile(stored);
    Json(json!({
        "ok": true,
        "profileId": id,
        "exists": exists,
        "config": config,
    }))
}

async fn post_events_overlay_config(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let profile_id = q
        .profile
        .as_deref()
        .or_else(|| body.get("profileId").and_then(|v| v.as_str()))
        .unwrap_or("default")
        .to_string();
    let config = body
        .get("config")
        .or_else(|| body.get("profile"))
        .cloned()
        .ok_or(StatusCode::BAD_REQUEST)?;
    if !config.is_object() {
        return Err(StatusCode::BAD_REQUEST);
    }
    ctx.state
        .events_overlay_config
        .write()
        .await
        .profiles
        .insert(profile_id.clone(), config);
    ctx.state
        .save_events_overlay()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    ctx.state
        .feed
        .broadcast_all(&json!({
            "type": "events-overlay-config-updated",
            "profileId": profile_id,
        }))
        .await;
    Ok(Json(json!({ "ok": true, "profileId": profile_id })))
}

#[derive(Deserialize)]
struct SeSessionBody {
    jwt: String,
    #[serde(rename = "accountId")]
    account_id: String,
}

#[derive(Deserialize)]
struct SeImportBody {
    #[serde(rename = "overlayIds", default)]
    overlay_ids: Vec<String>,
}

async fn get_se_session(State(ctx): State<ServerContext>) -> Json<Value> {
    match streamelements::load_session(&ctx.state.paths) {
        Ok(Some(mut s)) => {
            let mut username = s.username.clone();
            let missing_name = username
                .as_ref()
                .map(|u| u.trim().is_empty())
                .unwrap_or(true);
            if missing_name {
                if let Ok(profile) = streamelements::fetch_channel_profile(&s).await {
                    username = streamelements::display_name_from_channel(&profile);
                    if username.is_some() {
                        s.username = username.clone();
                        let _ = streamelements::save_session(&ctx.state.paths, &s);
                    }
                }
            }
            Json(json!({
                "ok": true,
                "connected": true,
                "accountId": s.account_id,
                "username": username,
                "capturedAt": s.captured_at,
            }))
        }
        Ok(None) => Json(json!({ "ok": true, "connected": false })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn post_se_session(
    State(ctx): State<ServerContext>,
    Json(body): Json<SeSessionBody>,
) -> Result<Json<Value>, StatusCode> {
    if ctx.state.readonly {
        return Err(StatusCode::FORBIDDEN);
    }
    let jwt = body.jwt.trim().to_string();
    let account_id = body.account_id.trim().to_string();
    if jwt.is_empty() || account_id.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut session = SeSession {
        jwt,
        account_id: account_id.clone(),
        username: None,
        captured_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    if let Ok(profile) = streamelements::fetch_channel_profile(&session).await {
        session.username = streamelements::display_name_from_channel(&profile);
    }
    streamelements::save_session(&ctx.state.paths, &session).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({
        "ok": true,
        "connected": true,
        "accountId": account_id,
        "username": session.username,
    })))
}

async fn delete_se_session(State(ctx): State<ServerContext>) -> Json<Value> {
    if !ctx.state.readonly {
        let _ = streamelements::clear_session(&ctx.state.paths);
    }
    Json(json!({ "ok": true, "connected": false }))
}

async fn get_se_overlays(State(ctx): State<ServerContext>) -> Json<Value> {
    let session = match streamelements::load_session(&ctx.state.paths) {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Json(json!({
                "ok": false,
                "error": "not_connected",
                "overlays": [],
            }));
        }
        Err(e) => {
            return Json(json!({
                "ok": false,
                "error": e.to_string(),
                "overlays": [],
            }));
        }
    };
    let client = match SeClient::from_session(&session).await {
        Ok(c) => c,
        Err(e) => {
            return Json(json!({
                "ok": false,
                "error": format!("Invalid StreamElements token: {e}"),
                "overlays": [],
            }));
        }
    };
    match client.list_overlays().await {
        Ok(overlays) => Json(json!({ "ok": true, "overlays": overlays })),
        Err(e) => Json(json!({
            "ok": false,
            "error": format!("Could not list overlays: {e}"),
            "overlays": [],
        })),
    }
}

async fn post_se_import(
    State(ctx): State<ServerContext>,
    Json(body): Json<SeImportBody>,
) -> Result<Json<Value>, StatusCode> {
    if ctx.state.readonly {
        return Err(StatusCode::FORBIDDEN);
    }
    if body.overlay_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let session = streamelements::load_session(&ctx.state.paths)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let client = SeClient::from_session(&session)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    let mut results = Vec::new();
    for oid in &body.overlay_ids {
        let raw = match client.fetch_overlay_json(oid).await {
            Ok(r) => r,
            Err(e) => {
                results.push(json!({
                    "ok": false,
                    "overlayId": oid,
                    "error": e.to_string(),
                }));
                continue;
            }
        };
        let _ = save_raw_overlay(&ctx.state.paths, oid, &raw);
        let (profile_id, mut profile, mut warnings) = map_overlay_to_profile(&raw);
        let profile_name = profile
            .get("profileName")
            .and_then(|v| v.as_str())
            .unwrap_or("SE Import")
            .to_string();

        let mut unique_id = profile_id.clone();
        let mut n = 2u32;
        {
            let cfg = ctx.state.events_overlay_config.read().await;
            while cfg.profiles.contains_key(&unique_id) {
                unique_id = format!("{profile_id}-{n}");
                n += 1;
            }
        }

        let (media_warnings, media_downloaded) =
            streamelements::localize_profile_media(&ctx.state.paths, &unique_id, &mut profile)
                .await;
        warnings.extend(media_warnings);
        if media_downloaded > 0 {
            warnings.push(format!(
                "Saved {media_downloaded} media file(s) under /events-media/{unique_id}/"
            ));
        }

        ctx.state
            .events_overlay_config
            .write()
            .await
            .profiles
            .insert(unique_id.clone(), profile);
        if let Err(e) = ctx.state.save_events_overlay().await {
            results.push(json!({
                "ok": false,
                "overlayId": oid,
                "error": e.to_string(),
            }));
            continue;
        }
        ctx.state
            .feed
            .broadcast_all(&json!({
                "type": "events-overlay-config-updated",
                "profileId": unique_id,
            }))
            .await;

        results.push(json!({
            "ok": true,
            "overlayId": oid,
            "profileId": unique_id,
            "profileName": profile_name,
            "warnings": warnings,
        }));
    }

    Ok(Json(json!({ "ok": true, "results": results })))
}

async fn delete_events_overlay_config(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
) -> Json<Value> {
    let id = q.profile.as_deref().unwrap_or("default");
    if id != "default" {
        ctx.state.events_overlay_config.write().await.profiles.remove(id);
        ctx.state.save_events_overlay().await.ok();
    }
    Json(json!({ "ok": true, "profileId": id }))
}

async fn post_test_alert(
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
    Json(body): Json<Value>,
) -> Json<Value> {
    let profile_id = q
        .profile
        .as_deref()
        .or_else(|| body.get("profile").and_then(|v| v.as_str()))
        .or_else(|| body.get("profileId").and_then(|v| v.as_str()))
        .unwrap_or("default");
    let event_type = body
        .get("eventType")
        .and_then(|v| v.as_str())
        .or_else(|| body.get("eventKey").and_then(|v| v.as_str()))
        .unwrap_or("follow");
    let raw_vars = body
        .pointer("/data/variables")
        .or_else(|| body.get("variables"))
        .or_else(|| body.get("data"))
        .cloned()
        .unwrap_or_else(|| body.get("data").cloned().unwrap_or(json!({})));
    let variables = twitch::normalize_event_variables(&raw_vars);

    let mut alert = json!({
        "type": "event-alert",
        "eventType": event_type,
        "data": { "variables": variables },
    });
    if let Some(vid) = body
        .get("variationId")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        alert["variationId"] = json!(vid);
    }
    if let Some(sv) = body.get("soundVolume") {
        if !sv.is_null() {
            alert["soundVolume"] = sv.clone();
        }
    }
    ctx.state
        .feed
        .broadcast_profile(profile_id, &alert)
        .await;

    let et = event_type.to_ascii_lowercase();
    let name = variables
        .get("name")
        .or_else(|| variables.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("Someone");
    let detail = test_alert_dock_detail(&et, name, &variables);
    let dock_type = if et == "cheer" { "bits" } else { et.as_str() };
    ctx.state
        .feed
        .broadcast_all(&make_dock_event(
            dock_type,
            &detail,
            Some(event_type),
            None,
        ))
        .await;

    Json(json!({ "ok": true, "profileId": profile_id, "eventType": event_type }))
}

fn test_alert_dock_detail(et: &str, name: &str, variables: &Value) -> String {
    match et {
        "follow" => format!("{name} followed"),
        "sub" => twitch::format_sub_dock_detail(
            name,
            variables
                .get("tier")
                .or(variables.get("amount"))
                .unwrap_or(&Value::Null),
        ),
        "resub" => twitch::format_resub_dock_detail(
            name,
            variables.get("months").unwrap_or(&Value::Null),
            variables
                .get("tier")
                .or(variables.get("amount"))
                .unwrap_or(&Value::Null),
            variables
                .get("input")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ),
        "gift" => twitch::format_gift_dock_detail(
            name,
            variables.get("amount").unwrap_or(&Value::Null),
            variables.get("tier").unwrap_or(&Value::Null),
            variables
                .get("recipient")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ),
        "cheer" | "bits" => format!(
            "{name} cheered {}{}",
            variables
                .get("amount")
                .or(variables.get("bits"))
                .map(|v| v.to_string())
                .unwrap_or_default(),
            variables
                .get("input")
                .map(|i| format!(": {i}"))
                .unwrap_or_default()
        ),
        "raid" => format!(
            "{name} raided{}",
            variables
                .get("amount")
                .or(variables.get("raiders"))
                .map(|v| format!(" with {v}"))
                .unwrap_or_default()
        ),
        "redeem" => format!(
            "{} — {}{}",
            variables
                .get("reward")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "Redeem".into()),
            name,
            variables
                .get("input")
                .map(|i| format!(": {i}"))
                .unwrap_or_default()
        ),
        _ => format!("{name} triggered {et}"),
    }
}

async fn ws_feed(
    ws: WebSocketUpgrade,
    State(ctx): State<ServerContext>,
    Query(q): Query<ProfileQuery>,
) -> impl IntoResponse {
    let profile_id = q.profile.unwrap_or_else(|| "default".into());
    ws.on_upgrade(move |socket| handle_ws(socket, ctx, profile_id))
}

async fn handle_ws(socket: axum::extract::ws::WebSocket, ctx: ServerContext, profile_id: String) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(tokio::sync::RwLock::new(sender));
    ctx.state
        .feed
        .register(profile_id.clone(), sender.clone())
        .await;

    let events_cfg = ctx.state.events_dock_config.read().await.clone();
    if let Ok(text) = serde_json::to_string(&json!({
        "type": "events-dock-config",
        "config": events_cfg,
    })) {
        let mut s = sender.write().await;
        let _ = s.send(axum::extract::ws::Message::Text(text.into())).await;
    }

    while let Some(Ok(msg)) = receiver.next().await {
        if let axum::extract::ws::Message::Text(text) = msg {
            let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
                continue;
            };
            match parsed.get("type").and_then(|v| v.as_str()) {
                Some("ping") => {
                    let mut s = sender.write().await;
                    let _ = s
                        .send(axum::extract::ws::Message::Text(
                            json!({ "type": "pong", "ts": chrono::Utc::now().timestamp_millis() })
                                .to_string()
                                .into(),
                        ))
                        .await;
                }
                Some("chat-send") => {
                    if let Some(text) = parsed.get("message").and_then(|v| v.as_str()) {
                        let platform = parsed
                            .get("platform")
                            .and_then(|v| v.as_str())
                            .unwrap_or("twitch");
                        let result = if platform == "kick" {
                            kick::send_chat_from_dock(ctx.state.clone(), text).await
                        } else {
                            twitch::send_chat_from_dock(
                                ctx.state.clone(),
                                ctx.twitch.clone(),
                                text,
                            )
                            .await
                        };
                        if let Err(e) = result {
                            tracing::warn!("chat-send failed: {e:#}");
                        }
                    }
                }
                _ => {}
            }
        }
    }
    ctx.state.feed.unregister(&profile_id, &sender).await;
}
