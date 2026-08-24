//! Narrow native proxy for privileged overlay HTTP — browser JS never sees the master token.

use serde::{Deserialize, Serialize};
use stream_sync_core::CONTROL_TOKEN_HEADER;
use tauri::WebviewWindow;

const ALLOWED_METHODS: &[&str] = &["GET", "POST", "DELETE", "HEAD"];

/// Explicit allowlist for main-UI proxy operations. Unknown paths are rejected.
const OVERLAY_API_ALLOWLIST: &[(&str, &str)] = &[
    ("GET", "/api/status"),
    ("GET", "/api/twitch/auth-url"),
    ("GET", "/api/twitch/badges/all"),
    ("GET", "/api/twitch/emotes/all"),
    ("GET", "/api/kick/auth-url"),
    ("GET", "/api/chat/overlay-profiles"),
    ("GET", "/api/chat/overlay-config"),
    ("GET", "/api/events/overlay-profiles"),
    ("GET", "/api/events/overlay-config"),
    ("GET", "/api/events/dock-config"),
    ("GET", "/api/streamelements/session"),
    ("GET", "/api/streamelements/begin-login"),
    ("GET", "/api/streamelements/overlays"),
    ("POST", "/api/twitch/disconnect"),
    ("POST", "/api/twitch/connection-key"),
    ("POST", "/api/twitch/use-connection"),
    ("POST", "/api/twitch/remove-connection"),
    ("POST", "/api/kick/disconnect"),
    ("POST", "/api/chat/dock-config"),
    ("POST", "/api/chat/overlay-config"),
    ("POST", "/api/events/dock-config"),
    ("POST", "/api/events/overlay-config"),
    ("POST", "/api/events/test-alert"),
    ("POST", "/api/chat/upload-font"),
    ("POST", "/api/streamelements/import"),
    ("POST", "/api/dock/issue-credential"),
    ("POST", "/api/dock/revoke-credential"),
    ("DELETE", "/api/chat/overlay-config"),
    ("DELETE", "/api/events/overlay-config"),
    ("DELETE", "/api/streamelements/session"),
];

const MEDIA_UPLOAD_MAX_BYTES: usize = 64 * 1024 * 1024;
const MEDIA_EXTENSIONS: &[&str] = &[
    "png", "jpg", "jpeg", "gif", "webp", "mp4", "webm", "mp3", "wav", "ogg",
];

#[derive(Debug, Deserialize)]
pub struct OverlayApiRequest {
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub body_base64: bool,
}

#[derive(Debug, Serialize)]
pub struct OverlayApiResponse {
    pub status: u16,
    pub body: String,
}

#[derive(Debug, Deserialize)]
pub struct OverlayMediaUploadRequest {
    pub profile_id: String,
    pub file_name: String,
    pub content_type: String,
    pub data_base64: String,
}

pub fn validate_overlay_api_path(path: &str) -> Result<String, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("empty_path".into());
    }
    if path.contains("://") || path.starts_with("//") {
        return Err("absolute_url_forbidden".into());
    }
    if path.contains('\\') {
        return Err("backslash_forbidden".into());
    }
    if path.contains('#') {
        return Err("fragment_forbidden".into());
    }
    if path.contains('@') {
        return Err("credentials_forbidden".into());
    }
    let lower = path.to_ascii_lowercase();
    if lower.contains("%2f%2f") || lower.contains("%5c") || lower.contains("..") {
        return Err("traversal_forbidden".into());
    }
    if !path.starts_with('/') {
        return Err("path_must_be_relative".into());
    }
    if !path.starts_with("/api/") {
        return Err("path_not_allowlisted".into());
    }
    let mut normalized = String::new();
    for (i, seg) in path.split('/').enumerate() {
        if seg.is_empty() {
            if i == 0 {
                normalized.push('/');
            }
            continue;
        }
        if seg == ".." {
            return Err("traversal_forbidden".into());
        }
        if !normalized.ends_with('/') {
            normalized.push('/');
        }
        normalized.push_str(seg);
    }
    if normalized.is_empty() {
        normalized.push('/');
    }
    Ok(normalized)
}

pub fn validate_allowlisted_api(method: &str, path: &str) -> Result<(), String> {
    let method = method.trim().to_ascii_uppercase();
    let path = validate_overlay_api_path(path)?;
    if OVERLAY_API_ALLOWLIST
        .iter()
        .any(|(m, p)| *m == method.as_str() && *p == path.as_str())
    {
        Ok(())
    } else {
        Err("path_not_allowlisted".into())
    }
}

pub fn validate_caller_window(window: &WebviewWindow, expected_port: u16) -> Result<(), String> {
    if window.label() != "main" {
        return Err("wrong_window".into());
    }
    let url = window.url().map_err(|e| e.to_string())?;
    let origin = format!("{}://{}", url.scheme(), url.authority());
    let ok = origin == format!("http://127.0.0.1:{expected_port}")
        || origin == format!("http://localhost:{expected_port}");
    if ok {
        Ok(())
    } else {
        Err("wrong_origin".into())
    }
}

fn read_control_token() -> Result<String, String> {
    let paths = stream_sync_core::get_paths().map_err(|e| e.to_string())?;
    let token = std::fs::read_to_string(&paths.control_token).map_err(|e| e.to_string())?;
    let token = token.trim().to_string();
    if token.len() < 32 {
        return Err("control_capability_unavailable".into());
    }
    Ok(token)
}

pub async fn execute_overlay_api_request(
    window: &WebviewWindow,
    overlay_port: u16,
    request: OverlayApiRequest,
) -> Result<OverlayApiResponse, String> {
    validate_caller_window(window, overlay_port)?;
    let path = validate_overlay_api_path(&request.path)?;
    let method = request.method.trim().to_ascii_uppercase();
    validate_allowlisted_api(&method, &path)?;
    if !ALLOWED_METHODS.contains(&method.as_str()) {
        return Err("method_not_allowed".into());
    }
    let token = read_control_token()?;
    let origin = format!("http://127.0.0.1:{overlay_port}");
    let url = format!("http://127.0.0.1:{overlay_port}{path}");
    let client = reqwest::Client::new();
    let mut builder = match method.as_str() {
        "GET" => client.get(&url),
        "HEAD" => client.head(&url),
        "POST" => client.post(&url),
        "DELETE" => client.delete(&url),
        _ => return Err("method_not_allowed".into()),
    };
    builder = builder
        .header("Origin", &origin)
        .header(CONTROL_TOKEN_HEADER, token);
    if let Some(body) = request.body {
        if request.body_base64 {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(body.trim())
                .map_err(|_| "invalid_body_base64".to_string())?;
            builder = builder.body(bytes);
        } else {
            builder = builder
                .header("Content-Type", "application/json")
                .body(body);
        }
    }
    let res = builder.send().await.map_err(|e| e.to_string())?;
    let status = res.status().as_u16();
    let body = res.text().await.map_err(|e| e.to_string())?;
    Ok(OverlayApiResponse { status, body })
}

pub async fn execute_overlay_media_upload(
    window: &WebviewWindow,
    overlay_port: u16,
    request: OverlayMediaUploadRequest,
) -> Result<OverlayApiResponse, String> {
    validate_caller_window(window, overlay_port)?;
    validate_allowlisted_api("POST", "/api/events/upload-media")?;

    let profile = request.profile_id.trim();
    if profile.is_empty()
        || profile.contains('/')
        || profile.contains('\\')
        || profile.contains("..")
    {
        return Err("invalid_profile".into());
    }

    let file_name = sanitize_upload_file_name(&request.file_name)?;
    let ext = file_name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if !MEDIA_EXTENSIONS.contains(&ext.as_str()) {
        return Err("invalid_extension".into());
    }

    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(request.data_base64.trim())
        .map_err(|_| "invalid_body_base64".to_string())?;
    if bytes.is_empty() {
        return Err("empty_upload".into());
    }
    if bytes.len() > MEDIA_UPLOAD_MAX_BYTES {
        return Err("upload_too_large".into());
    }

    let content_type = request.content_type.trim();
    if content_type.is_empty() || content_type.contains('\n') {
        return Err("invalid_content_type".into());
    }

    let token = read_control_token()?;
    let origin = format!("http://127.0.0.1:{overlay_port}");
    let url = format!("http://127.0.0.1:{overlay_port}/api/events/upload-media");

    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name(file_name)
        .mime_str(content_type)
        .map_err(|_| "invalid_content_type".to_string())?;
    let form = reqwest::multipart::Form::new()
        .text("profile", profile.to_string())
        .part("file", part);

    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .header("Origin", &origin)
        .header(CONTROL_TOKEN_HEADER, token)
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = res.status().as_u16();
    let body = res.text().await.map_err(|e| e.to_string())?;
    Ok(OverlayApiResponse { status, body })
}

fn sanitize_upload_file_name(name: &str) -> Result<String, String> {
    let base = std::path::Path::new(name)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .trim();
    if base.is_empty() || base.len() > 200 {
        return Err("invalid_file_name".into());
    }
    if base.contains("..") || base.contains('/') || base.contains('\\') {
        return Err("invalid_file_name".into());
    }
    Ok(base.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_absolute_and_traversal_paths() {
        assert!(validate_overlay_api_path("https://evil.test/api/status").is_err());
        assert!(validate_overlay_api_path("//evil/api/status").is_err());
        assert!(validate_overlay_api_path("/api/../admin").is_err());
        assert!(validate_overlay_api_path("/api/status#frag").is_err());
        assert!(validate_overlay_api_path("\\api\\status").is_err());
    }

    #[test]
    fn accepts_normalized_api_paths() {
        let p = validate_overlay_api_path("/api/status").unwrap();
        assert_eq!(p, "/api/status");
        let p2 = validate_overlay_api_path("/api//twitch//disconnect").unwrap();
        assert_eq!(p2, "/api/twitch/disconnect");
    }

    #[test]
    fn allowlist_permits_ui_routes_and_rejects_admin() {
        assert!(validate_allowlisted_api("GET", "/api/status").is_ok());
        assert!(validate_allowlisted_api("POST", "/api/events/test-alert").is_ok());
        assert!(validate_allowlisted_api("POST", "/api/twitch/set-token").is_err());
        assert!(validate_allowlisted_api("GET", "/api/admin/secret").is_err());
    }
}
