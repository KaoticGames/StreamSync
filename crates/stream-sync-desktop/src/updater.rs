//! Honest download-page opener. There is no in-app auto-updater yet,
//! so we must not ship a shared HMAC secret or pretend we verified an update.

use reqwest::Url;

pub const DEFAULT_DOWNLOAD_PAGE: &str = "https://syndicateai.net/update";

/// Resolve the public download page. HTTPS only; no signed query params.
pub fn resolve_download_page(env_page: Option<&str>) -> Result<String, String> {
    let raw = env_page
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_DOWNLOAD_PAGE);
    let url = Url::parse(raw).map_err(|e| format!("invalid download page: {e}"))?;
    if url.scheme() != "https" {
        return Err("download page must use https".into());
    }
    if url.query().is_some() {
        return Err("download page must not carry signed query parameters".into());
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
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
        assert!(!url.contains("p="));
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
}
