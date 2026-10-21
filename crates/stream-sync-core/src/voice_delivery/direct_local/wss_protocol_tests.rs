//! Strict direct WSS control JSON: version, size, schema, bounded reject/revoke codes.

use super::test_support::{DEVICE_ID, DISCORD_INSTANCE_ID, GUILD_ID, SESSION_ID};
use crate::voice_delivery::direct_local::wss::protocol::{
    DirectWssControlMessage, DirectWssControlParseError, DirectWssHello, DirectWssResumeCheckpoint,
    DirectWssSessionRejectCode, DirectWssSessionRevokeCode, DIRECT_WSS_PROTOCOL_VERSION,
    MAX_DIRECT_WSS_CONTROL_JSON_BYTES,
};
use crate::voice_delivery::direct_local::DirectCumulativeCheckpoint;

#[test]
fn rejects_unknown_protocol_version() {
    let json = r#"{"type":"hello","protocolVersion":2,"sessionId":"x"}"#;
    let err = DirectWssControlMessage::parse_inbound_text(json).unwrap_err();
    assert_eq!(err, DirectWssControlParseError::UnsupportedProtocolVersion);
}

#[test]
fn rejects_control_json_over_max_bytes() {
    let pad = "a".repeat(MAX_DIRECT_WSS_CONTROL_JSON_BYTES);
    let json = format!(
        r#"{{"type":"sessionReject","protocolVersion":1,"code":"bindingMismatch","padding":"{pad}"}}"#,
    );
    assert!(json.len() > MAX_DIRECT_WSS_CONTROL_JSON_BYTES);
    let err = DirectWssControlMessage::parse_inbound_text(&json).unwrap_err();
    assert_eq!(err, DirectWssControlParseError::ControlPayloadTooLarge);
}

#[test]
fn rejects_unknown_top_level_keys() {
    let json = r#"{
        "type":"sessionAccept",
        "protocolVersion":1,
        "sessionId":"550e8400-e29b-41d4-a716-446655440000",
        "extra":true
    }"#;
    let err = DirectWssControlMessage::parse_inbound_text(json).unwrap_err();
    assert_eq!(err, DirectWssControlParseError::UnknownField);
}

#[test]
fn rejects_unknown_control_type() {
    let json = r#"{"type":"notARealMessage","protocolVersion":1}"#;
    let err = DirectWssControlMessage::parse_inbound_text(json).unwrap_err();
    assert_eq!(err, DirectWssControlParseError::UnknownMessageType);
}

#[test]
fn rejects_non_object_control_json() {
    let err = DirectWssControlMessage::parse_inbound_text("[]").unwrap_err();
    assert_eq!(err, DirectWssControlParseError::NotJsonObject);
}

#[test]
fn rejects_control_integer_above_javascript_safe_range() {
    let json = r#"{
        "type":"sessionRevoke",
        "protocolVersion":1,
        "code":"generationAdvanced",
        "deliveryGeneration":9007199254740992,
        "leaderFenceGeneration":1
    }"#;
    assert_eq!(
        DirectWssControlMessage::parse_inbound_text(json),
        Err(DirectWssControlParseError::MalformedMessage)
    );
}

#[test]
fn session_reject_code_is_bounded() {
    assert!(DirectWssSessionRejectCode::parse("bindingMismatch").is_ok());
    assert!(DirectWssSessionRejectCode::parse("capabilityExpired").is_ok());
    assert_eq!(
        DirectWssSessionRejectCode::parse("totallyMadeUp"),
        Err(DirectWssControlParseError::UnknownRejectCode)
    );
}

#[test]
fn session_revoke_code_is_bounded() {
    assert!(DirectWssSessionRevokeCode::parse("generationAdvanced").is_ok());
    assert!(DirectWssSessionRevokeCode::parse("leaderFenced").is_ok());
    assert_eq!(
        DirectWssSessionRevokeCode::parse("arbitraryReason"),
        Err(DirectWssControlParseError::UnknownRevokeCode)
    );
}

#[test]
fn hello_outbound_includes_exact_binding_and_no_checkpoint_resume() {
    use super::wss_test_support::parsed_binding;
    let binding = parsed_binding();
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let hello = DirectWssHello::from_binding_and_checkpoint(
        &binding,
        DirectWssResumeCheckpoint::from_durable_checkpoint(&checkpoint),
    );
    let json = hello.to_json_text().expect("hello json");
    let parsed = DirectWssControlMessage::parse_inbound_text(&json).expect("parse hello");
    let hello = parsed.as_hello().expect("hello type");
    assert_eq!(hello.protocol_version, DIRECT_WSS_PROTOCOL_VERSION);
    assert_eq!(hello.session_id, SESSION_ID);
    assert_eq!(hello.guild_id, GUILD_ID);
    assert_eq!(hello.device_id, DEVICE_ID);
    assert_eq!(hello.delivery_generation, 1);
    assert_eq!(hello.discord_instance_id, DISCORD_INSTANCE_ID);
    assert_eq!(hello.leader_fence_generation, 1);
    assert!(hello.resume.is_no_segments_yet());
    assert_eq!(hello.resume.highest_contiguous_sequence, None);
    assert!(json.contains("\"highestContiguousSequence\":null"));
    assert!(!json.contains("18446744073709551615"));
}

#[test]
fn cumulative_ack_outbound_mirrors_durable_checkpoint_fields() {
    use super::test_support::{sha256_lower_hex, valid_direct_receiver_limits, SESSION_ID};
    use super::wss_test_support::{default_segment_bytes, open_test_store, parsed_binding};
    use crate::voice_delivery::direct_local::{
        DirectCumulativeCheckpoint, DirectSegmentDurabilityPipeline,
    };
    use crate::voice_delivery::fs::DestRoot;

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let bytes = default_segment_bytes();
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let mut pipeline =
        DirectSegmentDurabilityPipeline::try_new(&store, valid_direct_receiver_limits())
            .expect("pipeline");
    pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
        .expect("persist");
    let durable = DirectCumulativeCheckpoint::read_durable_from_store(&store).expect("checkpoint");
    let ack = DirectWssControlMessage::cumulative_ack_from_checkpoint(&durable);
    let json = ack.to_json_text().expect("ack json");
    assert!(json.contains("\"type\":\"cumulativeAck\""));
    let reparsed = DirectWssControlMessage::parse_inbound_text(&json).expect("parse ack");
    let ack = reparsed.as_cumulative_ack().expect("ack type");
    assert_eq!(
        ack.highest_contiguous_sequence,
        durable.highest_contiguous_sequence
    );
    assert_eq!(
        ack.last_segment_byte_length,
        durable.last_segment_byte_length
    );
    assert_eq!(ack.last_segment_sha256_hex, durable.last_segment_sha256_hex);
    assert_eq!(ack.namespace_durability, durable.namespace_durability);
    assert_eq!(ack.session_id, parsed_binding().session_id);
}
