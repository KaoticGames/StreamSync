//! Happy-path ACK after durability; no ACK on invalid segment or injected durability failure.

use super::test_support::SESSION_ID;
use super::wss_test_support::{
    accept_json, binary_segment, default_segment_bytes, offer_json, open_test_store,
    parsed_binding, run_session, segment_offer_for_sequence, session_accept_for_binding,
    text_control, MockDirectWssTransport,
};
use crate::voice_delivery::direct_local::durability_pipeline::DirectDurabilityBoundary;

use crate::voice_delivery::direct_local::wss::session::DirectLocalWssStopReason;
use crate::voice_delivery::direct_local::{
    DirectCumulativeCheckpoint, DirectGenerationStore, DirectReceiverLimits,
};
use crate::voice_delivery::fs::DestRoot;
use std::time::Duration;

#[tokio::test]
async fn one_segment_happy_path_persists_then_sends_exactly_one_cumulative_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let bytes = default_segment_bytes();
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer = segment_offer_for_sequence(0, &bytes);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
        binary_segment(&bytes),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::Revoked);

    let acks = transport.cumulative_acks_sent();
    assert_eq!(acks.len(), 1);
    let durable = DirectCumulativeCheckpoint::read_durable_from_store(
        &DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("reopen"),
    )
    .expect("checkpoint");
    let ack = acks[0].as_cumulative_ack().expect("ack");
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
}

#[tokio::test]
async fn oversize_segment_sends_no_cumulative_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let bytes = default_segment_bytes();
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer = segment_offer_for_sequence(0, &bytes);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
        binary_segment(&bytes),
    ]);
    transport.push(text_control(
        r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
    ));

    let tight = DirectReceiverLimits {
        max_segment_bytes: 8,
        max_in_memory_queued_bytes: 64 * 1024,
        max_unacked_bytes: 128 * 1024,
        max_unacked_segments: 4,
        min_free_space_reserve_bytes: 1024,
        ack_deadline: Duration::from_secs(30),
        max_direct_session_duration: Duration::from_secs(3600),
    };
    let config = super::wss_test_support::session_config_with_limits(store, checkpoint, tight);
    let stop = run_session(config, &transport).await;
    assert_ne!(stop, DirectLocalWssStopReason::AcceptedIdle);
    assert!(transport.cumulative_acks_sent().is_empty());
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(
        &DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("reopen"),
    )
    .is_err());
}

#[tokio::test]
async fn invalid_synj_segment_sends_no_cumulative_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let garbage = vec![0u8; 32];
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer = segment_offer_for_sequence(0, &garbage);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
        binary_segment(&garbage),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let _ = run_session(config, &transport).await;
    assert!(transport.cumulative_acks_sent().is_empty());
}

#[tokio::test]
async fn conflicting_second_segment_identity_sends_no_second_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let bytes_a = default_segment_bytes();
    let bytes_b = vec![bytes_a[0], bytes_a[1], 0x00];
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer_a = segment_offer_for_sequence(0, &bytes_a);
    let offer_b = segment_offer_for_sequence(0, &bytes_b);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer_a)),
        binary_segment(&bytes_a),
        text_control(&offer_json(&offer_b)),
        binary_segment(&bytes_b),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let _ = run_session(config, &transport).await;
    let acks = transport.cumulative_acks_sent();
    assert_eq!(acks.len(), 1);
}

#[tokio::test]
async fn injected_durability_failure_sends_no_cumulative_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let bytes = default_segment_bytes();
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer = segment_offer_for_sequence(0, &bytes);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
        binary_segment(&bytes),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config_with_failure(
        store,
        checkpoint,
        DirectDurabilityBoundary::BeforeCheckpointAtomicReplace,
    );
    let _ = run_session(config, &transport).await;
    assert!(transport.cumulative_acks_sent().is_empty());
    let reopened = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("reopen");
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(&reopened).is_err());
}
