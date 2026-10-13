//! Stem GET redirect target validation (Syndicate API → private R2 presign).

use super::errors::VoiceV2ClientError;
use reqwest::Url;

pub const VOICE_V2_STEM_REDIRECT_MAX: u32 = 1;
pub const VOICE_V2_PRESIGN_RETRY_MAX: u32 = 3;
pub const VOICE_V2_PRESIGN_RETRY_BACKOFF: std::time::Duration =
    std::time::Duration::from_millis(250);

const R2_HOST_SUFFIX: &str = ".r2.cloudflarestorage.com";

/// Headers that must never be forwarded to a cross-origin presigned URL.
#[allow(dead_code)]
pub const STRIPPED_ON_STEM_REDIRECT: &[&str] =
    &["authorization", "cookie", "x-stream-sync-control-token"];

pub fn is_allowed_r2_host(host: &str) -> bool {
    let h = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if !h.ends_with(R2_HOST_SUFFIX) {
        return false;
    }
    let account = h.strip_suffix(R2_HOST_SUFFIX).unwrap_or("");
    !account.is_empty()
        && account
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn allowlist_permits_host_port(allowlist: &[String], host: &str, port: u16) -> bool {
    let host_lc = host.to_ascii_lowercase();
    for entry in allowlist {
        let e = entry.trim().to_ascii_lowercase();
        if let Some((h, p)) = e.rsplit_once(':') {
            if h == host_lc && p.parse::<u16>().ok() == Some(port) {
                return true;
            }
        } else if e == host_lc && port == 443 {
            return true;
        }
    }
    false
}

/// Validate a single stem redirect `Location` before issuing the R2 follow-up GET.
pub fn validate_stem_redirect_location(
    location: &str,
    api_base: &str,
    redirect_allowlist: &[String],
    allow_http_for_tests: bool,
) -> Result<Url, VoiceV2ClientError> {
    let loc = location.trim();
    if loc.is_empty() {
        return Err(VoiceV2ClientError::InsecureRedirect(
            "missing location".into(),
        ));
    }
    let url = Url::parse(loc)
        .map_err(|_| VoiceV2ClientError::InsecureRedirect("malformed location".into()))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(VoiceV2ClientError::InsecureRedirect(
            "userinfo in location".into(),
        ));
    }
    let scheme = url.scheme();
    if scheme == "http" {
        if !allow_http_for_tests {
            return Err(VoiceV2ClientError::InsecureRedirect(
                "http downgrade".into(),
            ));
        }
    } else if scheme != "https" {
        return Err(VoiceV2ClientError::InsecureRedirect(
            "unsupported scheme".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| VoiceV2ClientError::InsecureRedirect("missing host".into()))?;
    let port = url.port_or_known_default().unwrap_or(443);
    let r2_ok = is_allowed_r2_host(host) && port == 443;
    let allow_ok = allowlist_permits_host_port(redirect_allowlist, host, port);
    if !r2_ok && !allow_ok {
        return Err(VoiceV2ClientError::InsecureRedirect(
            "host not allowed".into(),
        ));
    }
    if is_allowed_r2_host(host) && port != 443 {
        return Err(VoiceV2ClientError::InsecureRedirect(
            "non-default port".into(),
        ));
    }
    if let Ok(api) = Url::parse(api_base) {
        if url.as_str() == api.as_str() {
            return Err(VoiceV2ClientError::InsecureRedirect("redirect loop".into()));
        }
    }
    Ok(url)
}

pub fn redirect_status(status: u16) -> bool {
    status == 307 || status == 308
}

#[cfg(test)]
mod redirect_unit_tests {
    use super::*;

    #[test]
    fn r2_host_pattern_accepts_account_subdomain() {
        assert!(is_allowed_r2_host("abc123.r2.cloudflarestorage.com"));
        assert!(!is_allowed_r2_host("evil.com"));
        assert!(!is_allowed_r2_host(".r2.cloudflarestorage.com"));
    }

    #[test]
    fn rejects_http_downgrade_without_test_flag() {
        let err = validate_stem_redirect_location(
            "http://abc.r2.cloudflarestorage.com/obj",
            "https://api.example.com",
            &[],
            false,
        )
        .unwrap_err();
        assert!(matches!(err, VoiceV2ClientError::InsecureRedirect(_)));
    }

    #[test]
    fn rejects_userinfo_and_arbitrary_host() {
        for loc in [
            "https://user:pass@abc.r2.cloudflarestorage.com/x",
            "https://evil.example/x",
        ] {
            assert!(matches!(
                validate_stem_redirect_location(loc, "https://api.example.com", &[], false),
                Err(VoiceV2ClientError::InsecureRedirect(_))
            ));
        }
    }

    #[test]
    fn allowlist_permits_local_test_host_port() {
        let url = validate_stem_redirect_location(
            "http://127.0.0.1:9123/stem",
            "http://127.0.0.1:8080",
            &["127.0.0.1:9123".into()],
            true,
        )
        .unwrap();
        assert_eq!(url.port(), Some(9123));
    }
}
