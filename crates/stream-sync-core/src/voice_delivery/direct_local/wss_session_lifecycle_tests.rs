//! sessionReject/sessionRevoke, graceful close, RFC6455 ping/pong — no app-level heartbeat.

use super::test_support::SESSION_ID;
use super::wss_test_support::{
    accept_json, binary_segment, default_segment_bytes, offer_json, open_test_store,
    parsed_binding, run_session, segment_offer_for_sequence, session_accept_for_binding,
    text_control, ws_close_event, ws_ping_event, MockDirectWssTransport,
};
use crate::voice_delivery::direct_local::wss::session::{
    DirectLocalWssDarkSession, DirectLocalWssSessionError, DirectLocalWssStopReason,
};
use crate::voice_delivery::direct_local::DirectCumulativeCheckpoint;
use crate::voice_delivery::fs::DestRoot;

#[tokio::test]
async fn session_reject_closes_without_disk_write() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let transport = MockDirectWssTransport::accepting_script(vec![text_control(
        r#"{"type":"sessionReject","protocolVersion":1,"code":"bindingMismatch"}"#,
    )]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::Rejected);
    assert!(transport.was_closed());
    assert!(transport.cumulative_acks_sent().is_empty());
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(&open_test_store(&root),).is_err());
}

#[tokio::test]
async fn session_revoke_closes_before_segment_persist() {
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
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"leaderFenced","deliveryGeneration":1,"leaderFenceGeneration":99}"#,
        ),
        text_control(&offer_json(&offer)),
        binary_segment(&bytes),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::Revoked);
    assert!(transport.cumulative_acks_sent().is_empty());
}

#[tokio::test]
async fn graceful_server_close_stops_without_panic() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        ws_close_event(),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let stop = run_session(config, &transport).await;
    assert_eq!(stop, DirectLocalWssStopReason::GracefulClose);
}

#[tokio::test]
async fn ws_ping_receives_pong_without_app_level_heartbeat() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        ws_ping_event(),
        text_control(
            r#"{"type":"sessionRevoke","protocolVersion":1,"code":"generationAdvanced","deliveryGeneration":2,"leaderFenceGeneration":2}"#,
        ),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let _ = run_session(config, &transport).await;
    assert_eq!(transport.drain_sent_pongs(), vec![vec![1, 2, 3]]);
    assert!(
        transport.drain_sent_text().iter().all(
            |t| !t.contains("\"type\":\"heartbeat\"") && !t.contains("\"type\":\"serverPing\"")
        ),
        "no app-level heartbeat control messages"
    );
    assert!(transport.drain_sent_binary().is_empty());
}

#[tokio::test]
async fn unexpected_second_session_accept_fails_closed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = open_test_store(&root);
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    let binding = parsed_binding();
    let accept = session_accept_for_binding(&binding, 0);
    let transport = MockDirectWssTransport::accepting_script(vec![
        text_control(&accept_json(&accept)),
        text_control(&accept_json(&accept)),
    ]);
    let config = super::wss_test_support::session_config(store, checkpoint);
    let mut session = DirectLocalWssDarkSession::try_new(config).expect("session");
    assert!(matches!(
        session.run_until_stop(&transport).await,
        Err(DirectLocalWssSessionError::Protocol)
    ));
    assert!(transport.was_closed());
}
