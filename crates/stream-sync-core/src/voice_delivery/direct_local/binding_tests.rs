//! Strict `DirectSessionBinding` parse and device binding checks (Slice-1 capability JSON).

use super::test_support::{
    valid_binding_json, CAPABILITY_TOKEN, DEVICE_ID, DISCORD_INSTANCE_ID, EXPIRES_AT_MS, GUILD_ID,
    JS_MAX_SAFE_INTEGER, SESSION_ID, WSS_URL,
};
use crate::voice_delivery::direct_local::{DirectSessionBinding, DirectSessionBindingError};

#[test]
fn parses_required_fields() {
    let binding = DirectSessionBinding::from_json(&valid_binding_json()).expect("strict parse");
    assert_eq!(binding.session_id, SESSION_ID);
    assert_eq!(binding.guild_id, GUILD_ID);
    assert_eq!(binding.device_id, DEVICE_ID);
    assert_eq!(binding.delivery_generation, 1);
    assert_eq!(binding.discord_instance_id, DISCORD_INSTANCE_ID);
    assert_eq!(binding.leader_fence_generation, 1);
    assert_eq!(binding.token, CAPABILITY_TOKEN);
    assert_eq!(binding.expires_at_ms, EXPIRES_AT_MS);
    assert_eq!(binding.wss_url, WSS_URL);
}

#[test]
fn rejects_unknown_top_level_keys() {
    let mut json = valid_binding_json();
    json["extraField"] = serde_json::json!("surprise");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnknownField)
    );
}

#[test]
fn rejects_non_object_json() {
    assert_eq!(
        DirectSessionBinding::from_json(&serde_json::json!([])),
        Err(DirectSessionBindingError::NotJSONObject)
    );
}

#[test]
fn rejects_missing_required_field() {
    let mut json = valid_binding_json();
    json.as_object_mut().expect("object").remove("sessionId");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MissingField { field: "sessionId" })
    );
}

#[test]
fn debug_output_redacts_capability_token() {
    let binding = DirectSessionBinding::from_json(&valid_binding_json()).expect("parse");
    let debug = format!("{binding:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains(CAPABILITY_TOKEN));
}

#[test]
fn rejects_snake_case_legacy_keys() {
    let mut json = valid_binding_json();
    json.as_object_mut().expect("object").remove("sessionId");
    json["session_id"] = serde_json::json!(SESSION_ID);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnknownField)
    );
}

#[test]
fn rejects_rfc3339_expiry_instead_of_expires_at_ms() {
    let mut json = valid_binding_json();
    json.as_object_mut().expect("object").remove("expiresAtMs");
    json["expires_at"] = serde_json::json!("2026-10-21T18:00:00Z");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnknownField)
    );
}

#[test]
fn rejects_empty_session_id() {
    let mut json = valid_binding_json();
    json["sessionId"] = serde_json::json!("");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedId("sessionId".into()))
    );
}

#[test]
fn rejects_whitespace_only_session_id() {
    let mut json = valid_binding_json();
    json["sessionId"] = serde_json::json!("   ");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedId("sessionId".into()))
    );
}

#[test]
fn rejects_malformed_session_id() {
    let mut json = valid_binding_json();
    json["sessionId"] = serde_json::json!("not-a-uuid");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedId("sessionId".into()))
    );
}

#[test]
fn rejects_empty_guild_id() {
    let mut json = valid_binding_json();
    json["guildId"] = serde_json::json!("");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedId("guildId".into()))
    );
}

#[test]
fn rejects_oversized_discord_instance_id() {
    let mut json = valid_binding_json();
    json["discordInstanceId"] = serde_json::json!("x".repeat(129));
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedId(
            "discordInstanceId".into()
        ))
    );
}

#[test]
fn rejects_zero_delivery_generation() {
    let mut json = valid_binding_json();
    json["deliveryGeneration"] = serde_json::json!(0);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "deliveryGeneration",
        })
    );
}

#[test]
fn rejects_negative_delivery_generation() {
    let mut json = valid_binding_json();
    json["deliveryGeneration"] = serde_json::json!(-1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "deliveryGeneration",
        })
    );
}

#[test]
fn rejects_fractional_delivery_generation() {
    let mut json = valid_binding_json();
    json["deliveryGeneration"] = serde_json::json!(1.5);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "deliveryGeneration",
        })
    );
}

#[test]
fn rejects_string_coerced_delivery_generation() {
    let mut json = valid_binding_json();
    json["deliveryGeneration"] = serde_json::json!("1");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "deliveryGeneration",
        })
    );
}

#[test]
fn rejects_js_unsafe_delivery_generation() {
    let mut json = valid_binding_json();
    json["deliveryGeneration"] = serde_json::json!(JS_MAX_SAFE_INTEGER + 1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "deliveryGeneration",
        })
    );
}

#[test]
fn rejects_zero_leader_fence_generation() {
    let mut json = valid_binding_json();
    json["leaderFenceGeneration"] = serde_json::json!(0);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "leaderFenceGeneration",
        })
    );
}

#[test]
fn rejects_negative_leader_fence_generation() {
    let mut json = valid_binding_json();
    json["leaderFenceGeneration"] = serde_json::json!(-1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "leaderFenceGeneration",
        })
    );
}

#[test]
fn rejects_fractional_leader_fence_generation() {
    let mut json = valid_binding_json();
    json["leaderFenceGeneration"] = serde_json::json!(1.5);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "leaderFenceGeneration",
        })
    );
}

#[test]
fn rejects_string_coerced_leader_fence_generation() {
    let mut json = valid_binding_json();
    json["leaderFenceGeneration"] = serde_json::json!("1");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "leaderFenceGeneration",
        })
    );
}

#[test]
fn rejects_js_unsafe_leader_fence_generation() {
    let mut json = valid_binding_json();
    json["leaderFenceGeneration"] = serde_json::json!(JS_MAX_SAFE_INTEGER + 1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "leaderFenceGeneration",
        })
    );
}

#[test]
fn rejects_zero_expires_at_ms() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(0);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "expiresAtMs",
        })
    );
}

#[test]
fn rejects_negative_expires_at_ms() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(-1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "expiresAtMs",
        })
    );
}

#[test]
fn rejects_fractional_expires_at_ms() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(EXPIRES_AT_MS as f64 + 0.5);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "expiresAtMs",
        })
    );
}

#[test]
fn rejects_string_coerced_expires_at_ms() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(EXPIRES_AT_MS.to_string());
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "expiresAtMs",
        })
    );
}

#[test]
fn rejects_js_unsafe_expires_at_ms() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(JS_MAX_SAFE_INTEGER + 1);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::UnsafeInteger {
            field: "expiresAtMs",
        })
    );
}

#[test]
fn preserves_exact_integer_identity_across_json() {
    let mut json = valid_binding_json();
    json["expiresAtMs"] = serde_json::json!(JS_MAX_SAFE_INTEGER);
    json["deliveryGeneration"] = serde_json::json!(JS_MAX_SAFE_INTEGER);
    json["leaderFenceGeneration"] = serde_json::json!(JS_MAX_SAFE_INTEGER);
    let binding = DirectSessionBinding::from_json(&json).expect("parse safe integers");
    assert_eq!(binding.expires_at_ms, JS_MAX_SAFE_INTEGER);
    assert_eq!(binding.delivery_generation, JS_MAX_SAFE_INTEGER);
    assert_eq!(binding.leader_fence_generation, JS_MAX_SAFE_INTEGER);
}

#[test]
fn rejects_empty_capability_token() {
    let mut json = valid_binding_json();
    json["token"] = serde_json::json!("");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedCapabilityToken)
    );
}

#[test]
fn rejects_wrong_length_capability_token() {
    let mut json = valid_binding_json();
    json["token"] = serde_json::json!("A".repeat(42));
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedCapabilityToken)
    );
}

#[test]
fn rejects_padded_capability_token() {
    let mut json = valid_binding_json();
    json["token"] = serde_json::json!(format!("{}=", CAPABILITY_TOKEN));
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedCapabilityToken)
    );
}

#[test]
fn rejects_invalid_alphabet_capability_token() {
    let mut json = valid_binding_json();
    let mut bad = CAPABILITY_TOKEN.to_string();
    bad.replace_range(0..1, "+");
    json["token"] = serde_json::json!(bad);
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::MalformedCapabilityToken)
    );
}

#[test]
fn rejects_https_outbound_url() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("https://discord-worker.example/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_plain_ws_outbound_url() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("ws://discord-worker.example/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_url_with_embedded_credentials() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://user:pass@discord-worker.example/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_url_with_fragment() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://discord-worker.example/voice#frag");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_localhost_endpoint() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://localhost:443/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_loopback_literal_ip() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://127.0.0.1:443/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_private_literal_ip() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://10.0.0.5:443/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_malformed_port() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss://discord-worker.example:99999/voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_wss_empty_host_authority() {
    let mut json = valid_binding_json();
    json["wssUrl"] = serde_json::json!("wss:///voice");
    assert_eq!(
        DirectSessionBinding::from_json(&json),
        Err(DirectSessionBindingError::InvalidWssUrl)
    );
}

#[test]
fn rejects_device_mismatch_against_local_device() {
    let binding = DirectSessionBinding::from_json(&valid_binding_json()).expect("parse");
    assert_eq!(
        binding.assert_matches_local_device("other-device-id"),
        Err(DirectSessionBindingError::DeviceMismatch)
    );
    binding
        .assert_matches_local_device(DEVICE_ID)
        .expect("same device");
}
