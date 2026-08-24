//! Localhost control-plane policy: route inventory, origin checks, capability validation.

use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};

use crate::app_state::AppState;
use crate::dock_capability::DockCredentialStore;
use crate::oauth_pending::LOGIN_NONCE_HEADER;
pub use crate::route_manifest::RoutePolicy;

pub const CONTROL_TOKEN_HEADER: &str = "x-streamsync-control";

/// JSON control endpoints use a small default body limit.
pub const PRIVILEGED_JSON_BODY_LIMIT: usize = 1024 * 1024;
/// Media upload endpoints keep a larger authenticated limit.
pub const MEDIA_UPLOAD_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Control socket must authenticate within this window after upgrade.
pub const WS_CONTROL_AUTH_TIMEOUT_MS: u64 = 5_000;

/// Explicit security manifest. Router registration is independently listed in routes.rs.
pub fn route_inventory() -> &'static [(&'static str, &'static str, RoutePolicy)] {
    crate::route_manifest::ROUTE_MANIFEST
}

fn path_matches(pattern: &str, path: &str) -> bool {
    if let Some(base) = pattern.strip_suffix("/*") {
        let prefix = format!("{base}/");
        return path == base || path.starts_with(&prefix);
    }
    if let Some(idx) = pattern.find("/:") {
        let head = &pattern[..idx];
        if !path.starts_with(head) {
            return false;
        }
        let rest = &pattern[idx + 1..]; // ":param..." or ":param/rest"
        let after_param = rest.find('/').map(|i| &rest[i..]).unwrap_or("");
        if after_param.is_empty() {
            return path.len() > head.len();
        }
        return path[head.len()..].contains(after_param) || path.ends_with(after_param);
    }
    pattern == path
}

pub fn route_policy(method: &Method, path: &str) -> RoutePolicy {
    let path = path.trim_end_matches('/');
    let path = if path.is_empty() { "/" } else { path };
    let method_str = method.as_str();

    for (m, pattern, policy) in route_inventory() {
        if *m == method_str && path_matches(pattern, path) {
            return *policy;
        }
    }

    // Static UI assets from ServeDir fallback — GET/HEAD only.
    if (method == Method::GET || method == Method::HEAD) && is_static_asset_path(path) {
        return RoutePolicy::PublicReadOnly;
    }

    // Fail closed.
    RoutePolicy::Privileged
}

fn is_static_asset_path(path: &str) -> bool {
    // Narrow allowlist — never treat arbitrary suffixes as public control surfaces.
    const EXTS: &[&str] = &[
        ".html", ".js", ".css", ".png", ".ico", ".svg", ".woff2", ".woff", ".ttf", ".map", ".json",
    ];
    // Deny config-like JSON under /api
    if path.starts_with("/api/") {
        return false;
    }
    EXTS.iter().any(|ext| path.ends_with(ext))
}

pub fn trusted_origin(origin: &str, port: u16) -> bool {
    let origin = origin.trim_end_matches('/');
    origin == format!("http://127.0.0.1:{port}") || origin == format!("http://localhost:{port}")
}

pub fn origin_allowed_for_privileged(headers: &HeaderMap, port: u16) -> bool {
    match headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
        Some(origin) if !origin.is_empty() && origin != "null" => trusted_origin(origin, port),
        _ => false,
    }
}

/// Fail-closed WebSocket origin check (missing/malformed/null/evil rejected).
pub fn ws_origin_allowed(headers: &HeaderMap, port: u16) -> bool {
    origin_allowed_for_privileged(headers, port)
}

pub fn control_token_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(CONTROL_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

pub fn login_nonce_from_headers(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(LOGIN_NONCE_HEADER)
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
    // Dock tokens must never satisfy master-capability checks.
    if DockCredentialStore::is_dock_token(provided) {
        return false;
    }
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
            axum::http::HeaderName::from_static(LOGIN_NONCE_HEADER),
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
        RoutePolicy::PublicReadOnly
        | RoutePolicy::OAuthCallbackPage
        | RoutePolicy::AuthenticatedControl => next.run(request).await,
        RoutePolicy::OAuthCompletion => {
            // Master capability OR valid same-origin login nonce (validated in handler).
            // Middleware only enforces trusted origin; nonce/master checked by handler helpers.
            if origin_allowed_for_privileged(request.headers(), state.port)
                || authorize_privileged(&state, request.headers())
            {
                // Login-nonce completions from the callback page always send Origin.
                // Also allow privileged master-token callers (Tauri) for the same endpoints.
                if authorize_privileged(&state, request.headers())
                    || (origin_allowed_for_privileged(request.headers(), state.port)
                        && login_nonce_from_headers(request.headers()).is_some())
                {
                    next.run(request).await
                } else if origin_allowed_for_privileged(request.headers(), state.port) {
                    // Origin ok but no nonce/header yet — let handler return precise error
                    // when body carries flowNonce. Allow through for body-based nonce.
                    next.run(request).await
                } else {
                    unauthorized_response()
                }
            } else {
                unauthorized_response()
            }
        }
        RoutePolicy::Privileged => {
            if authorize_privileged(&state, request.headers()) {
                next.run(request).await
            } else {
                unauthorized_response()
            }
        }
    }
}

/// Exclusive create/load of the master control token with file locking + read-back.
pub fn load_or_create_control_token(path: &Path) -> anyhow::Result<String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    let lock_path = path.with_extension("lock");
    let _lock_guard = acquire_path_lock(&lock_path)?;

    if path.is_file() {
        let existing = std::fs::read_to_string(path)?.trim().to_string();
        if existing.len() >= 32 && !DockCredentialStore::is_dock_token(&existing) {
            crate::storage::ensure_secret_file_permissions(path)?;
            return Ok(existing);
        }
        // Malformed / too short — replace safely (no .bak retention).
        let _ = std::fs::remove_file(path);
    }
    let token = format!(
        "ssc_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    crate::storage::write_secret_file(path, token.as_bytes())?;
    // Authoritative read-back so concurrent losers converge on the winner.
    let committed = std::fs::read_to_string(path)?.trim().to_string();
    if committed.len() < 32 {
        anyhow::bail!("control token read-back failed");
    }
    Ok(committed)
}

/// Load the control token without creating persistent state when readonly.
pub fn load_control_token(path: &Path, readonly: bool) -> anyhow::Result<String> {
    if !readonly {
        return load_or_create_control_token(path);
    }
    if path.is_file() {
        let existing = std::fs::read_to_string(path)?.trim().to_string();
        if existing.len() >= 32 && !DockCredentialStore::is_dock_token(&existing) {
            return Ok(existing);
        }
    }
    if let Ok(token) = std::env::var("STREAMSYNC_READONLY_CONTROL_TOKEN") {
        let token = token.trim().to_string();
        if token.len() >= 32 && !DockCredentialStore::is_dock_token(&token) {
            return Ok(token);
        }
    }
    Ok(format!(
        "ssc_readonly_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    ))
}

struct PathLock {
    path: std::path::PathBuf,
    owner: String,
    _file: File,
}

impl Drop for PathLock {
    fn drop(&mut self) {
        if std::fs::read_to_string(&self.path)
            .map(|owner| owner == self.owner)
            .unwrap_or(false)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn acquire_path_lock(lock_path: &Path) -> anyhow::Result<PathLock> {
    let owner = format!("{} {}", std::process::id(), uuid::Uuid::new_v4());
    for attempt in 0..200 {
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path)
        {
            Ok(mut file) => {
                file.write_all(owner.as_bytes())?;
                file.flush()?;
                let _ = file.sync_all();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ =
                        std::fs::set_permissions(lock_path, std::fs::Permissions::from_mode(0o600));
                }
                return Ok(PathLock {
                    path: lock_path.to_path_buf(),
                    owner,
                    _file: file,
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Recover only an unchanged lock whose owning process is no longer alive.
                if attempt > 50 {
                    if let Ok(meta) = std::fs::metadata(lock_path) {
                        if let Ok(modified) = meta.modified() {
                            if modified.elapsed().unwrap_or_default()
                                > std::time::Duration::from_secs(5)
                            {
                                if let Ok(stale_owner) = std::fs::read_to_string(lock_path) {
                                    if !lock_owner_is_alive(&stale_owner)
                                        && std::fs::read_to_string(lock_path)
                                            .map(|current| current == stale_owner)
                                            .unwrap_or(false)
                                    {
                                        let quarantine = lock_path.with_extension(format!(
                                            "stale-{}",
                                            uuid::Uuid::new_v4().simple()
                                        ));
                                        if std::fs::rename(lock_path, &quarantine).is_ok() {
                                            let _ = std::fs::remove_file(quarantine);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("timed out waiting for control-token lock")
}

fn lock_owner_is_alive(owner: &str) -> bool {
    let Some(pid) = owner
        .split_whitespace()
        .next()
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return false;
    };
    if pid == std::process::id() {
        return true;
    }
    #[cfg(unix)]
    {
        std::path::Path::new("/proc").join(pid.to_string()).exists()
    }
    #[cfg(windows)]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .output()
            .map(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(&pid.to_string())
            })
            .unwrap_or(false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_marks_status_privileged() {
        assert_eq!(
            route_policy(&Method::GET, "/api/status"),
            RoutePolicy::Privileged
        );
    }

    #[test]
    fn set_token_is_oauth_completion_not_master_only() {
        assert_eq!(
            route_policy(&Method::POST, "/api/twitch/set-token"),
            RoutePolicy::OAuthCompletion
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
            route_policy(&Method::GET, "/api/twitch/badges/all"),
            RoutePolicy::PublicReadOnly
        );
        assert_eq!(
            route_policy(&Method::GET, "/ws/feed"),
            RoutePolicy::PublicReadOnly
        );
    }

    #[test]
    fn dock_token_never_matches_master() {
        assert!(!control_token_matches(
            "ssc_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "ssd_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ));
    }

    #[test]
    fn mutating_public_route_is_not_classified_public() {
        assert_eq!(
            route_policy(&Method::POST, "/api/chat/overlay-config"),
            RoutePolicy::Privileged
        );
        assert_eq!(
            route_policy(&Method::DELETE, "/health"),
            RoutePolicy::Privileged
        );
    }

    #[test]
    fn old_lock_owner_cannot_unlink_successor() {
        let path = std::env::temp_dir().join(format!(
            "streamsync-lock-successor-{}.lock",
            uuid::Uuid::new_v4()
        ));
        let old = acquire_path_lock(&path).unwrap();
        std::fs::write(&path, "successor-owner").unwrap();
        drop(old);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "successor-owner");
        let _ = std::fs::remove_file(path);
    }
}
