//! Localhost control-plane policy: route classification, origin checks, capability validation.

use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::app_state::AppState;

pub const CONTROL_TOKEN_HEADER: &str = "x-streamsync-control";

/// JSON control endpoints use a small default body limit.
pub const PRIVILEGED_JSON_BODY_LIMIT: usize = 1024 * 1024;
/// Media upload endpoints keep a larger authenticated limit.
pub const MEDIA_UPLOAD_BODY_LIMIT: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutePolicy {
    /// OBS/browser read-only assets and feed rendering.
    PublicReadOnly,
    /// OAuth callback pages (GET HTML only).
    OAuthCallbackPage,
    /// Privileged control plane — origin + capability required.
    Privileged,
}

pub fn route_policy(method: &Method, path: &str) -> RoutePolicy {
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        return RoutePolicy::PublicReadOnly;
    }

    if method == Method::GET && path.starts_with("/auth/") && path.ends_with("/callback") {
        return RoutePolicy::OAuthCallbackPage;
    }

    if path == "/ws/feed" {
        return RoutePolicy::PublicReadOnly;
    }
    if path == "/ws/control" {
        // Capability is validated on the socket after upgrade (browser WS cannot set headers).
        return RoutePolicy::PublicReadOnly;
    }

    match (method, path) {
        (&Method::GET, "/health") => RoutePolicy::PublicReadOnly,
        (&Method::GET, p) if p.starts_with("/overlay/") => RoutePolicy::PublicReadOnly,
        (&Method::GET, p) if p.starts_with("/dock/") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/events-studio.html") => RoutePolicy::PublicReadOnly,
        (&Method::GET, p) if p.starts_with("/config/") && p.ends_with(".json") => {
            RoutePolicy::PublicReadOnly
        }
        (&Method::GET, "/api/chat/overlay-config") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/api/events/overlay-config") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/api/events/dock-config") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/api/chat/overlay-profiles") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/api/events/overlay-profiles") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/google-fonts.css") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/google-fonts/file") => RoutePolicy::PublicReadOnly,
        (&Method::GET, p) if p.starts_with("/fonts/") => RoutePolicy::PublicReadOnly,
        (&Method::GET, p) if p.starts_with("/events-media/") => RoutePolicy::PublicReadOnly,
        (&Method::GET, "/") => RoutePolicy::PublicReadOnly,
        (&Method::GET, _) if is_static_asset_path(path) => RoutePolicy::PublicReadOnly,
        (&Method::HEAD, _) if is_static_asset_path(path) => RoutePolicy::PublicReadOnly,
        _ => RoutePolicy::Privileged,
    }
}

fn is_static_asset_path(path: &str) -> bool {
    path.ends_with(".html")
        || path.ends_with(".js")
        || path.ends_with(".css")
        || path.ends_with(".png")
        || path.ends_with(".ico")
        || path.ends_with(".svg")
        || path.ends_with(".woff2")
        || path.ends_with(".woff")
        || path.ends_with(".ttf")
}

pub fn trusted_origin(origin: &str, port: u16) -> bool {
    let origin = origin.trim_end_matches('/');
    origin == format!("http://127.0.0.1:{port}") || origin == format!("http://localhost:{port}")
}

pub fn origin_allowed_for_privileged(headers: &HeaderMap, port: u16) -> bool {
    match headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        Some(origin) => trusted_origin(origin, port),
        None => false,
    }
}

pub fn control_token_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(CONTROL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn control_token_matches(expected: &str, provided: &str) -> bool {
    constant_time_eq(expected.as_bytes(), provided.as_bytes())
}

pub fn authorize_privileged(state: &AppState, headers: &HeaderMap) -> bool {
    if !origin_allowed_for_privileged(headers, state.port) {
        return false;
    }
    let Some(provided) = control_token_from_headers(headers) else {
        return false;
    };
    control_token_matches(state.control_token(), provided)
}

pub fn unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "ok": false,
            "error": "unauthorized",
            "message": "Control capability required."
        })),
    )
        .into_response()
}

pub fn cors_layer(port: u16) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            origin
                .to_str()
                .map(|o| trusted_origin(o, port))
                .unwrap_or(false)
        }))
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            header::ORIGIN,
            axum::http::HeaderName::from_static(CONTROL_TOKEN_HEADER),
        ])
}

pub async fn control_plane_middleware(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    match route_policy(&method, &path) {
        RoutePolicy::PublicReadOnly | RoutePolicy::OAuthCallbackPage => next.run(request).await,
        RoutePolicy::Privileged => {
            if authorize_privileged(&state, request.headers()) {
                next.run(request).await
            } else {
                unauthorized_response()
            }
        }
    }
}

pub fn load_or_create_control_token(path: &std::path::Path) -> anyhow::Result<String> {
    if path.is_file() {
        let existing = std::fs::read_to_string(path)?.trim().to_string();
        if existing.len() >= 32 {
            return Ok(existing);
        }
    }
    let token = format!(
        "ssc_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    crate::storage::write_file_atomic(path, token.as_bytes())?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privileged_routes_are_classified() {
        assert_eq!(
            route_policy(&Method::POST, "/api/twitch/disconnect"),
            RoutePolicy::Privileged
        );
        assert_eq!(
            route_policy(&Method::GET, "/api/status"),
            RoutePolicy::Privileged
        );
    }

    #[test]
    fn obs_read_routes_stay_public() {
        assert_eq!(
            route_policy(&Method::GET, "/health"),
            RoutePolicy::PublicReadOnly
        );
        assert_eq!(
            route_policy(&Method::GET, "/api/chat/overlay-config"),
            RoutePolicy::PublicReadOnly
        );
        assert_eq!(
            route_policy(&Method::GET, "/ws/feed"),
            RoutePolicy::PublicReadOnly
        );
    }

    #[test]
    fn control_ws_upgrade_is_public_then_socket_auth() {
        assert_eq!(
            route_policy(&Method::GET, "/ws/control"),
            RoutePolicy::PublicReadOnly
        );
    }
}
