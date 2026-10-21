//! Segment offer + single binary frame state machine (no interleaving, pre-disk validation).

use super::test_support::valid_binding_json;
use super::test_support::{DEVICE_ID, DISCORD_INSTANCE_ID, GUILD_ID, SESSION_ID};
use super::wss_test_support::{default_segment_bytes, segment_offer_for_sequence};
use crate::voice_delivery::direct_local::wss::segment_fsm::{
    DirectWssSegmentFsm, DirectWssSegmentFsmError, DirectWssSegmentFsmState,
};
use crate::voice_delivery::direct_local::DirectSessionBinding;

fn fsm_for_binding() -> DirectWssSegmentFsm {
    let binding = DirectSessionBinding::from_json(&valid_binding_json()).expect("binding");
    DirectWssSegmentFsm::new(binding)
}

#[test]
fn rejects_binary_before_segment_offer() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let err = fsm.on_binary_frame(&bytes).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::UnexpectedBinary);
    assert_eq!(fsm.state(), DirectWssSegmentFsmState::Idle);
}

#[test]
fn rejects_text_interleaved_between_offer_and_binary() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    fsm.on_segment_offer(&offer).expect("offer");
    let err = fsm
        .on_control_interleaved(DirectWssSegmentFsm::ping_as_interleaved_control())
        .unwrap_err();
    assert_eq!(
        err,
        DirectWssSegmentFsmError::InterleavedControlWhileAwaitingBinary
    );
}

#[test]
fn rejects_duplicate_segment_offer_without_completing_prior() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    fsm.on_segment_offer(&offer).expect("first offer");
    let err = fsm.on_segment_offer(&offer).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::DuplicateInFlightOffer);
}

#[test]
fn rejects_binary_length_mismatch_before_persist() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    fsm.on_segment_offer(&offer).expect("offer");
    let short = bytes[..bytes.len() / 2].to_vec();
    let err = fsm.on_binary_frame(&short).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::BinaryLengthMismatch);
    assert!(!fsm.ready_for_durability());
}

#[test]
fn rejects_delivery_generation_mismatch_before_persist() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    let mut bad = offer.clone();
    bad.delivery_generation = 99;
    let err = fsm.on_segment_offer(&bad).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::BindingFieldMismatch);
}

#[test]
fn rejects_discord_instance_mismatch_before_persist() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    let mut bad = offer.clone();
    bad.discord_instance_id = "other-instance".to_string();
    let err = fsm.on_segment_offer(&bad).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::BindingFieldMismatch);
}

#[test]
fn rejects_leader_fence_mismatch_before_persist() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    let mut bad = offer.clone();
    bad.leader_fence_generation = 42;
    let err = fsm.on_segment_offer(&bad).unwrap_err();
    assert_eq!(err, DirectWssSegmentFsmError::BindingFieldMismatch);
}

#[test]
fn rejects_session_guild_device_mismatch_before_persist() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    let mut bad = offer.clone();
    bad.session_id = "00000000-0000-0000-0000-000000000001".to_string();
    assert_eq!(
        fsm.on_segment_offer(&bad).unwrap_err(),
        DirectWssSegmentFsmError::BindingFieldMismatch
    );
    bad = offer.clone();
    bad.guild_id = "999999999999999999".to_string();
    assert_eq!(
        fsm.on_segment_offer(&bad).unwrap_err(),
        DirectWssSegmentFsmError::BindingFieldMismatch
    );
    bad = offer.clone();
    bad.device_id = "other-device".to_string();
    assert_eq!(
        fsm.on_segment_offer(&bad).unwrap_err(),
        DirectWssSegmentFsmError::BindingFieldMismatch
    );
    assert_eq!(offer.session_id, SESSION_ID);
    assert_eq!(offer.guild_id, GUILD_ID);
    assert_eq!(offer.device_id, DEVICE_ID);
    assert_eq!(offer.discord_instance_id, DISCORD_INSTANCE_ID);
}

#[test]
fn accepts_offer_then_exact_binary_and_yields_identity() {
    let mut fsm = fsm_for_binding();
    let bytes = default_segment_bytes();
    let offer = segment_offer_for_sequence(0, &bytes);
    fsm.on_segment_offer(&offer).expect("offer");
    let ready = fsm.on_binary_frame(&bytes).expect("binary");
    assert!(fsm.ready_for_durability());
    assert_eq!(ready.segment_sequence, 0);
    assert_eq!(fsm.state(), DirectWssSegmentFsmState::Idle);
}
