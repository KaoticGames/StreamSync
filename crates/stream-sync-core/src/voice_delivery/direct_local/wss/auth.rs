//! Outbound WSS connect request: public `wss://` URL policy and Bearer capability header.

use super::transport::DirectWssTransportError;
use crate::voice_delivery::direct_local::DirectSessionBinding;
use std::fmt;
use std::net::IpAddr;
use thiserror::Error;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http;
use url::Host;
use url::Url;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectWssConnectBuildError {
    #[error("invalid public wss url")]
    InvalidPublicWssUrl,
    #[error("forbidden url component")]
    ForbiddenUrlComponent,
    #[error("malformed capability token")]
    MalformedCapabilityToken,
}

/// HTTP upgrade request with redacted capability in `Debug`/`Display`.
pub struct DirectWssConnectRequest(http::Request<()>);

impl std::ops::Deref for DirectWssConnectRequest {
    type Target = http::Request<()>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DirectWssConnectRequest {
    pub fn into_http_request(self) -> http::Request<()> {
        self.0
    }
}

impl fmt::Debug for DirectWssConnectRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_redacted_request(f, "DirectWssConnectRequest", &self.0, true)
    }
}

impl fmt::Display for DirectWssConnectRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_redacted_request(f, "DirectWssConnectRequest", &self.0, false)
    }
}

fn write_redacted_request(
    f: &mut fmt::Formatter<'_>,
    name: &str,
    req: &http::Request<()>,
    debug: bool,
) -> fmt::Result {
    let uri = req.uri().to_string();
    if debug {
        f.debug_struct(name)
            .field("uri", &uri)
            .field("authorization", &"[REDACTED]")
            .finish()
    } else {
        write!(f, "{name} {{ uri: {uri}, authorization: [REDACTED] }}")
    }
}

pub fn validate_outbound_wss_connect_url(url_str: &str) -> Result<(), DirectWssConnectBuildError> {
    if !url_str.starts_with("wss://") {
        return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
    }
    if url_str.contains('#') {
        return Err(DirectWssConnectBuildError::ForbiddenUrlComponent);
    }
    let authority = url_str
        .strip_prefix("wss://")
        .unwrap_or("")
        .split('/')
        .next()
        .unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return Err(DirectWssConnectBuildError::ForbiddenUrlComponent);
    }
    let url = match Url::parse(url_str) {
        Ok(u) => u,
        Err(_) => return Err(DirectWssConnectBuildError::InvalidPublicWssUrl),
    };
    if url.scheme() != "wss" {
        return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(DirectWssConnectBuildError::ForbiddenUrlComponent);
    }
    if url.fragment().is_some() || url.query().is_some() {
        return Err(DirectWssConnectBuildError::ForbiddenUrlComponent);
    }
    let host = match url.host() {
        Some(h) => h,
        None => return Err(DirectWssConnectBuildError::InvalidPublicWssUrl),
    };
    match host {
        Host::Domain(domain) => {
            if domain.is_empty() || domain.eq_ignore_ascii_case("localhost") {
                return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
            }
            if let Ok(ip) = domain.parse::<IpAddr>() {
                if !is_public_ip(ip) {
                    return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
                }
            }
        }
        Host::Ipv4(ip) => {
            if !is_public_ip(IpAddr::V4(ip)) {
                return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
            }
        }
        Host::Ipv6(ip) => {
            if !is_public_ip(IpAddr::V6(ip)) {
                return Err(DirectWssConnectBuildError::InvalidPublicWssUrl);
            }
        }
    }
    Ok(())
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_loopback()
                && !v4.is_private()
                && !v4.is_link_local()
                && !v4.is_unspecified()
                && !v4.is_multicast()
                && !v4.is_broadcast()
        }
        IpAddr::V6(v6) => {
            !v6.is_loopback() && !v6.is_unspecified() && !v6.is_unique_local() && !v6.is_multicast()
        }
    }
}

pub fn build_direct_wss_connect_request(
    binding: &DirectSessionBinding,
) -> Result<DirectWssConnectRequest, DirectWssConnectBuildError> {
    validate_outbound_wss_connect_url(&binding.wss_url)?;
    if binding.token.is_empty() {
        return Err(DirectWssConnectBuildError::MalformedCapabilityToken);
    }
    let mut request = binding
        .wss_url
        .as_str()
        .into_client_request()
        .map_err(|_| DirectWssConnectBuildError::InvalidPublicWssUrl)?;
    let auth_value = http::HeaderValue::from_str(&format!("Bearer {}", binding.token))
        .map_err(|_| DirectWssConnectBuildError::MalformedCapabilityToken)?;
    request
        .headers_mut()
        .insert(http::header::AUTHORIZATION, auth_value);
    Ok(DirectWssConnectRequest(request))
}

impl From<DirectWssConnectBuildError> for DirectWssTransportError {
    fn from(_: DirectWssConnectBuildError) -> Self {
        DirectWssTransportError::ConnectFailed
    }
}
