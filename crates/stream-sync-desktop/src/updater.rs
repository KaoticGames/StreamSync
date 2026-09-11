//! Check-for-updates opens the Syndicate HTTPS page with this app's version.
//! No client-shipped HMAC secret. The page compares `v` to Syndicate's current build.

use reqwest::Url;

pub const DEFAULT_DOWNLOAD_PAGE: &str = "https://syndicateai.net/update";
pub const UPDATE_APP_ID: &str = "stream-sync";

/// Resolve the public update page. HTTPS only; reject leftover HMAC query params.
pub fn resolve_download_page(env_page: Option<&str>) -> Result<String, String> {
    let raw = env_page
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_DOWNLOAD_PAGE);
    let url = Url::parse(raw).map_err(|e| format!("invalid download page: {e}"))?;
    if url.scheme() != "https" {
        return Err("download page must use https".into());
    }
    if let Some(q) = url.query() {
        let lower = q.to_ascii_lowercase();
        if lower.contains("sig=") || lower.split('&').any(|p| p == "p" || p.starts_with("p=")) {
            return Err("download page must not carry signed query parameters".into());
        }
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn sanitize_version(version: &str) -> String {
    version
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '+'))
        .take(32)
        .collect()
}

/// Syndicate update URL including this Stream Sync version for comparison.
pub fn check_for_updates_url(env_page: Option<&str>, version: &str) -> Result<String, String> {
    let base = resolve_download_page(env_page)?;
    let version = sanitize_version(version);
    if version.is_empty() {
        return Err("missing app version".into());
    }
    let mut url = Url::parse(&base).map_err(|e| format!("invalid download page: {e}"))?;
    url.query_pairs_mut()
        .append_pair("app", UPDATE_APP_ID)
        .append_pair("v", &version);
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_download_page_is_https_without_signature() {
        let url = resolve_download_page(None).expect("default");
        assert_eq!(url, DEFAULT_DOWNLOAD_PAGE);
        assert!(url.starts_with("https://"));
        assert!(!url.contains("sig="));
    }

    #[test]
    fn env_override_must_be_https() {
        assert!(resolve_download_page(Some("http://example.com/update")).is_err());
        assert!(resolve_download_page(Some("javascript:alert(1)")).is_err());
        let url = resolve_download_page(Some("https://syndicateai.net/download")).unwrap();
        assert_eq!(url, "https://syndicateai.net/download");
    }

    #[test]
    fn rejects_pre_signed_query_string() {
        assert!(
            resolve_download_page(Some("https://syndicateai.net/update?p=abc&sig=deadbeef"))
                .is_err()
        );
    }

    #[test]
    fn check_for_updates_url_registers_app_version() {
        let url = check_for_updates_url(None, "2.0.1").unwrap();
        assert!(url.starts_with("https://syndicateai.net/update?"));
        assert!(url.contains("app=stream-sync"));
        assert!(url.contains("v=2.0.1"));
        assert!(!url.contains("sig="));
    }
}
