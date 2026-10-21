//! ACK deadline and max direct session duration via paused Tokio clock.

use super::test_support::{valid_direct_receiver_limits, SESSION_ID};
use super::wss_test_support::{
    accept_json, binary_segment, default_segment_bytes, offer_json, open_test_store,
    parsed_binding, run_session, segment_offer_for_sequence, session_accept_for_binding,
    session_config_with_durability_delay, session_config_with_limits, text_control,
    MockDirectWssTransport,
};
use crate::voice_delivery::direct_local::wss::session::DirectLocalWssStopReason;
use crate::voice_delivery::direct_local::{DirectCumulativeCheckpoint, DirectReceiverLimits};
use crate::voice_delivery::fs::DestRoot;
use std::time::Duration;

fn short_limits() -> DirectReceiverLimits {
    DirectReceiverLimits {
        max_segment_bytes: valid_direct_receiver_limits().max_segment_bytes,
        max_in_memory_queued_bytes: valid_direct_receiver_limits().max_in_memory_queued_bytes,
        max_unacked_bytes: valid_direct_receiver_limits().max_unacked_bytes,
        max_unacked_segments: valid_direct_receiver_limits().max_unacked_segments,
        min_free_space_reserve_bytes: valid_direct_receiver_limits().min_free_space_reserve_bytes,
        ack_deadline: Duration::from_millis(200),
        max_direct_session_duration: Duration::from_millis(500),
    }
}

#[tokio::test]
async fn ack_deadline_expires_without_cumulative_ack_for_inflight_segment() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let bytes = default_segment_bytes();
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let offer = segment_offer_for_sequence(0, &bytes);
    let transport = MockDirectWssTransport::accepting_pending_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&offer_json(&offer)),
    ]);
    let config = session_config_with_limits(store, checkpoint, short_limits());
    let stop = tokio::time::timeout(Duration::from_secs(2), async {
        run_session(config, &transport).await
    })
    .await
    .expect("session should stop before outer timeout");
    assert_eq!(stop, DirectLocalWssStopReason::AckDeadlineExceeded);
}

#[tokio::test]
async fn max_direct_session_duration_closes_cleanly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let transport =
        MockDirectWssTransport::accepting_pending_script(vec![text_control(&accept_json(&accept))]);
    let config = session_config_with_limits(store, checkpoint, short_limits());
    let stop = tokio::time::timeout(Duration::from_secs(2), async {
        run_session(config, &transport).await
    })
    .await
    .expect("session should stop before outer timeout");
    assert_eq!(stop, DirectLocalWssStopReason::MaxSessionDurationExceeded);
}

#[tokio::test]
async fn max_direct_session_duration_bounds_connect_handshake() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let transport = MockDirectWssTransport::pending_connect();
    let config = session_config_with_limits(store, checkpoint, short_limits());
    let stop = tokio::time::timeout(Duration::from_secs(2), async {
        run_session(config, &transport).await
    })
    .await
    .expect("session should bound connect before outer timeout");
    assert_eq!(stop, DirectLocalWssStopReason::MaxSessionDurationExceeded);
}

#[tokio::test]
async fn ack_deadline_covers_durability_before_ack_send() {
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
    let config = session_config_with_durability_delay(
        store,
        checkpoint,
        Duration::from_millis(60),
        valid_direct_receiver_limits(),
    );
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::AckDeadlineExceeded);
    assert!(transport.cumulative_acks_sent().is_empty());
}
