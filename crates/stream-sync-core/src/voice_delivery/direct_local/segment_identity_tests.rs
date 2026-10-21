//! `LogicalSegmentIdentity` binds session, generation, sequence, length, and digest.

use super::test_support::{
    minimal_synj_segment_bytes, sha256_lower_hex, JS_MAX_SAFE_INTEGER, SESSION_ID,
};
use crate::voice_delivery::direct_local::{LogicalSegmentIdentity, LogicalSegmentIdentityError};

#[test]
fn binds_session_generation_sequence_length_and_lowercase_sha256() {
    let bytes = minimal_synj_segment_bytes();
    let identity = LogicalSegmentIdentity::try_new(
        SESSION_ID,
        1,
        0,
        bytes.len() as u64,
        sha256_lower_hex(&bytes),
    )
    .expect("identity");
    assert_eq!(identity.session_id, SESSION_ID);
    assert_eq!(identity.delivery_generation, 1);
    assert_eq!(identity.segment_sequence, 0);
    assert_eq!(identity.byte_length, bytes.len() as u64);
    assert!(identity
        .sha256_hex
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    assert_eq!(identity.sha256_hex.len(), 64);
}

#[test]
fn rejects_uppercase_sha256() {
    let bytes = minimal_synj_segment_bytes();
    let upper = sha256_lower_hex(&bytes).to_uppercase();
    assert_eq!(
        LogicalSegmentIdentity::try_new(SESSION_ID, 1, 0, bytes.len() as u64, upper),
        Err(LogicalSegmentIdentityError::MalformedSha256)
    );
}

#[test]
fn rejects_wrong_digest_length() {
    assert_eq!(
        LogicalSegmentIdentity::try_new(SESSION_ID, 1, 0, 10, "abc"),
        Err(LogicalSegmentIdentityError::MalformedSha256)
    );
}

#[test]
fn rejects_zero_delivery_generation() {
    let bytes = minimal_synj_segment_bytes();
    assert_eq!(
        LogicalSegmentIdentity::try_new(
            SESSION_ID,
            0,
            0,
            bytes.len() as u64,
            sha256_lower_hex(&bytes),
        ),
        Err(LogicalSegmentIdentityError::StaleGeneration)
    );
}

#[test]
fn rejects_generation_below_binding() {
    let bytes = minimal_synj_segment_bytes();
    assert_eq!(
        LogicalSegmentIdentity::validate_against_binding_generation(
            &LogicalSegmentIdentity::try_new(
                SESSION_ID,
                1,
                0,
                bytes.len() as u64,
                sha256_lower_hex(&bytes),
            )
            .expect("identity"),
            2,
        ),
        Err(LogicalSegmentIdentityError::GenerationMismatch {
            identity_generation: 1,
            binding_generation: 2,
        })
    );
}

#[test]
fn rejects_segment_sequence_above_js_safe_integer() {
    let bytes = minimal_synj_segment_bytes();
    assert_eq!(
        LogicalSegmentIdentity::try_new(
            SESSION_ID,
            1,
            JS_MAX_SAFE_INTEGER + 1,
            bytes.len() as u64,
            sha256_lower_hex(&bytes),
        ),
        Err(LogicalSegmentIdentityError::SegmentSequenceNotJsSafe)
    );
}

#[test]
fn rejects_byte_length_above_js_safe_integer() {
    let bytes = minimal_synj_segment_bytes();
    assert_eq!(
        LogicalSegmentIdentity::try_new(
            SESSION_ID,
            1,
            0,
            JS_MAX_SAFE_INTEGER + 1,
            sha256_lower_hex(&bytes),
        ),
        Err(LogicalSegmentIdentityError::ByteLengthNotJsSafe)
    );
}

#[test]
fn rejects_generation_above_binding() {
    let bytes = minimal_synj_segment_bytes();
    assert_eq!(
        LogicalSegmentIdentity::validate_against_binding_generation(
            &LogicalSegmentIdentity::try_new(
                SESSION_ID,
                3,
                0,
                bytes.len() as u64,
                sha256_lower_hex(&bytes),
            )
            .expect("identity"),
            2,
        ),
        Err(LogicalSegmentIdentityError::GenerationMismatch {
            identity_generation: 3,
            binding_generation: 2,
        })
    );
}
