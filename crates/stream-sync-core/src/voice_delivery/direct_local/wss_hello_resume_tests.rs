//! Client hello resume fields: no-checkpoint vs durable checkpoint identity.

use super::test_support::{sha256_lower_hex, valid_direct_receiver_limits, SESSION_ID};
use super::wss_test_support::{
    accept_json, default_segment_bytes, hello_from_checkpoint, offer_json, open_test_store,
    parsed_binding, run_session, segment_offer_for_sequence, session_accept_for_binding,
    text_control, MockDirectWssTransport,
};
use crate::voice_delivery::direct_local::wss::protocol::DirectWssControlMessage;
use crate::voice_delivery::direct_local::wss::session::DirectLocalWssStopReason;
use crate::voice_delivery::direct_local::{
    DirectCumulativeCheckpoint, DirectSegmentDurabilityPipeline,
};
use crate::voice_delivery::fs::DestRoot;

#[tokio::test]
async fn hello_from_no_checkpoint_uses_no_segments_yet_resume() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let hello = hello_from_checkpoint(&checkpoint);
    let json = hello.to_json_text().expect("hello");
    let parsed = DirectWssControlMessage::parse_inbound_text(&json).expect("parse");
    let hello = parsed.as_hello().expect("hello");
    assert!(hello.resume.is_no_segments_yet());
    assert_eq!(hello.resume.highest_contiguous_sequence, None);
    assert_eq!(hello.resume.last_segment_byte_length, 0);
    assert!(hello.resume.last_segment_sha256_hex.is_empty());

    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::Revoked);
    let hellos = transport
        .drain_sent_text()
        .into_iter()
        .filter(|t| t.contains("\"type\":\"hello\""))
        .collect::<Vec<_>>();
    assert_eq!(hellos.len(), 1);
    assert!(hellos[0].contains("\"highestContiguousSequence\""));
}

#[tokio::test]
async fn hello_after_durable_checkpoint_repeats_exact_resume_fields() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let bytes = default_segment_bytes();
    let supplied = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let mut pipeline =
        DirectSegmentDurabilityPipeline::try_new(&store, valid_direct_receiver_limits())
            .expect("pipeline");
    pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &supplied)
        .expect("persist");
    let durable = DirectCumulativeCheckpoint::read_durable_from_store(&store).expect("checkpoint");

    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, durable.next_expected_segment_sequence());
    let offer = segment_offer_for_sequence(1, &bytes);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
        super::wss_test_support::binary_segment(&bytes),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"leaderFenced","deliveryGeneration":1,"leaderFenceGeneration":99}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, durable.clone());
    let _ = run_session(config, &transport).await;
    let hello_line = transport
        .drain_sent_text()
        .into_iter()
        .find(|t| t.contains("\"type\":\"hello\""))
        .expect("hello sent");
    let parsed_hello = DirectWssControlMessage::parse_inbound_text(&hello_line).expect("parse");
    let hello = parsed_hello.as_hello().expect("hello");
    assert_eq!(
        hello.resume.highest_contiguous_sequence,
        Some(durable.highest_contiguous_sequence)
    );
    assert_eq!(
        hello.resume.last_segment_byte_length,
        durable.last_segment_byte_length
    );
    assert_eq!(
        hello.resume.last_segment_sha256_hex,
        durable.last_segment_sha256_hex
    );
}
