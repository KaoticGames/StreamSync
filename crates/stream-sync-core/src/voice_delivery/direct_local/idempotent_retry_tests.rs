//! Identical segment retries are idempotent; conflicting bytes or identity fail closed.

use super::idempotent_retry::{DirectIdempotentSegmentRetry, DirectSegmentRetryError};
use super::test_support::{minimal_synj_segment_bytes, sha256_lower_hex, SESSION_ID};
use crate::voice_delivery::direct_local::DirectGenerationStore;
use crate::voice_delivery::fs::DestRoot;
use std::fs;

#[test]
fn identical_segment_retry_succeeds_after_length_hash_verification() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
    let bytes = minimal_synj_segment_bytes();
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let retry = DirectIdempotentSegmentRetry::new(&store);
    retry.persist_idempotent(&identity, &bytes).expect("first");
    retry
        .persist_idempotent(&identity, &bytes)
        .expect("idempotent retry");
    let path = tmp.path().join(store.segment_relative_path(&identity));
    assert_eq!(fs::read(path).expect("read"), bytes);
}

#[test]
fn conflicting_bytes_with_same_identity_fail_without_replacing_durable_segment() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
    let bytes = minimal_synj_segment_bytes();
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let retry = DirectIdempotentSegmentRetry::new(&store);
    retry.persist_idempotent(&identity, &bytes).expect("first");
    let tampered = b"not-synj-bytes";
    let err = retry
        .persist_idempotent(&identity, tampered)
        .expect_err("hash mismatch");
    assert_eq!(err, DirectSegmentRetryError::ConflictingPayload);
    let path = tmp.path().join(store.segment_relative_path(&identity));
    assert_eq!(fs::read(path).expect("durable unchanged"), bytes);
}

#[test]
fn conflicting_identity_for_same_sequence_fails_without_replace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
    let bytes = minimal_synj_segment_bytes();
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let retry = DirectIdempotentSegmentRetry::new(&store);
    retry.persist_idempotent(&identity, &bytes).expect("first");
    let wrong_hash = "0".repeat(64);
    let conflicting = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &wrong_hash)
        .expect("conflicting identity");
    let err = retry
        .persist_idempotent(&conflicting, &bytes)
        .expect_err("identity conflict");
    assert_eq!(err, DirectSegmentRetryError::ConflictingIdentity);
}
