//! Narrow native proxy for privileged overlay HTTP — browser JS never sees the master token.

use serde::{Deserialize, Serialize};
use stream_sync_core::CONTROL_TOKEN_HEADER;
use tauri::WebviewWindow;

const ALLOWED_METHODS: &[&str] = &["GET", "POST", "DELETE", "HEAD"];

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

/// Validate a relative StreamSync API path. Rejects absolute URLs, traversal, fragments, etc.
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
    // Normalize: collapse duplicate slashes (except leading).
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
}
