//! Outbound WSS connect request: Authorization header only, sanitized diagnostics, URL policy.

use super::test_support::{valid_binding_json, CAPABILITY_TOKEN, WSS_URL};
use super::wss_test_support::{assert_no_capability_leak, parsed_binding};
use crate::voice_delivery::direct_local::wss::auth::{
    build_direct_wss_connect_request, validate_outbound_wss_connect_url, DirectWssConnectBuildError,
};
use crate::voice_delivery::direct_local::DirectSessionBinding;
use tokio_tungstenite::tungstenite::http;

#[test]
fn connect_request_places_capability_only_in_authorization_bearer_header() {
    let binding = parsed_binding();
    let request = build_direct_wss_connect_request(&binding).expect("connect request");
    assert_eq!(request.uri().to_string(), WSS_URL);
    let auth = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .expect("authorization header")
        .to_str()
        .expect("valid header value");
    assert_eq!(auth, format!("Bearer {}", CAPABILITY_TOKEN));
    assert_eq!(
        request
            .headers()
            .get(http::header::UPGRADE)
            .and_then(|v| v.to_str().ok()),
        Some("websocket")
    );
    assert!(request.headers().contains_key(http::header::CONNECTION));
    assert!(request.headers().contains_key("sec-websocket-key"));
    assert_eq!(
        request
            .headers()
            .get("sec-websocket-version")
            .and_then(|v| v.to_str().ok()),
        Some("13")
    );
    assert!(!request.headers().contains_key("sec-websocket-protocol"));
    assert!(!request.uri().query().is_some());
    assert!(!request.uri().path().contains(CAPABILITY_TOKEN));
}

#[test]
fn connect_request_debug_and_display_redact_capability() {
    let binding = parsed_binding();
    let request = build_direct_wss_connect_request(&binding).expect("connect request");
    let debug = format!("{request:?}");
    let display = format!("{request}");
    assert_no_capability_leak(&debug);
    assert_no_capability_leak(&display);
    assert!(debug.contains(WSS_URL));
}

#[test]
fn rejects_wss_url_with_query_for_connect() {
    assert_eq!(
        validate_outbound_wss_connect_url("wss://discord-worker.example/voice?x=1"),
        Err(DirectWssConnectBuildError::ForbiddenUrlComponent)
    );
}

#[test]
fn rejects_wss_url_with_fragment_for_connect() {
    assert_eq!(
        validate_outbound_wss_connect_url("wss://discord-worker.example/voice#frag"),
        Err(DirectWssConnectBuildError::ForbiddenUrlComponent)
    );
}

#[test]
fn rejects_wss_url_with_embedded_credentials_for_connect() {
    assert_eq!(
        validate_outbound_wss_connect_url("wss://user:pass@discord-worker.example/voice"),
        Err(DirectWssConnectBuildError::ForbiddenUrlComponent)
    );
}

#[test]
fn rejects_non_wss_scheme_for_connect() {
    assert_eq!(
        validate_outbound_wss_connect_url("https://discord-worker.example/voice"),
        Err(DirectWssConnectBuildError::InvalidPublicWssUrl)
    );
    assert_eq!(
        validate_outbound_wss_connect_url("ws://discord-worker.example/voice"),
        Err(DirectWssConnectBuildError::InvalidPublicWssUrl)
    );
}

#[test]
fn rejects_loopback_and_private_hosts_for_connect() {
    for url in [
        "wss://localhost/voice",
        "wss://127.0.0.1/voice",
        "wss://10.0.0.1/voice",
        "wss://192.168.1.1/voice",
    ] {
        assert_eq!(
            validate_outbound_wss_connect_url(url),
            Err(DirectWssConnectBuildError::InvalidPublicWssUrl),
            "expected reject for {url}"
        );
    }
}

#[test]
fn binding_with_query_wss_url_fails_before_connect_build() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://discord-worker.example/voice-direct/v1?token=leak");
    let binding = DirectSessionBinding::from_json(&json);
    assert!(binding.is_err());
    if let Ok(binding) = binding {
        assert!(build_direct_wss_connect_request(&binding).is_err());
    }
}
