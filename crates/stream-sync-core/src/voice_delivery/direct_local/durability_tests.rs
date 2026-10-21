//! Cumulative ACK eligibility follows full durability pipeline (injection at each boundary).

use super::test_support::{minimal_synj_segment_bytes, sha256_lower_hex, SESSION_ID};
use crate::voice_delivery::direct_local::durability_pipeline::DirectDurabilityPipelineTestHooks;
use crate::voice_delivery::direct_local::{
    DirectCumulativeCheckpoint, DirectDurabilityBoundary, DirectDurabilityReport,
    DirectGenerationStore, DirectSegmentDurabilityFailureKind, DirectSegmentDurabilityPipeline,
    DirectSegmentPersistOutcome,
};
use crate::voice_delivery::fs::{DestRoot, NamespaceDurability};
use std::fs;

fn open_store_and_checkpoint(
    root: &DestRoot,
) -> (DirectGenerationStore, DirectCumulativeCheckpoint, Vec<u8>) {
    let store = DirectGenerationStore::open_for_session(root, SESSION_ID, 1).expect("store");
    let bytes = minimal_synj_segment_bytes();
    let checkpoint = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    (store, checkpoint, bytes)
}

#[test]
fn cumulative_ack_eligible_only_after_full_durability_pipeline() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, checkpoint, bytes) = open_store_and_checkpoint(&root);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let mut pipeline = DirectSegmentDurabilityPipeline::new(&store);
    let outcome = pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
        .expect("persist");
    assert_eq!(
        outcome,
        DirectSegmentPersistOutcome {
            cumulative_ack_eligible: true,
            highest_contiguous_sequence: 0,
        }
    );
    let report = pipeline.last_durability_report().expect("report");
    assert!(report.segment_write_completed);
    assert!(report.segment_file_sync_completed);
    assert!(report.length_and_hash_verified);
    assert!(report.checkpoint_temp_write_completed);
    assert!(report.checkpoint_temp_sync_completed);
    assert!(report.checkpoint_atomic_replace_completed);
    assert!(matches!(
        report.namespace_durability,
        NamespaceDurability::Proven | NamespaceDurability::Unavailable
    ));

    let durable = DirectCumulativeCheckpoint::read_durable_from_store(&store)
        .expect("on-disk cumulative checkpoint");
    assert_eq!(durable.session_id, SESSION_ID);
    assert_eq!(durable.delivery_generation, 1);
    assert_eq!(durable.highest_contiguous_sequence, 0);
    assert_eq!(durable.last_segment_byte_length, bytes.len() as u64);
    assert_eq!(durable.last_segment_sha256_hex, sha256_lower_hex(&bytes));
    assert_eq!(durable.namespace_durability, report.namespace_durability);
}

#[test]
fn failure_before_segment_file_sync_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeSegmentFileSync);
}

#[test]
fn failure_after_segment_write_before_sync_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::AfterSegmentPayloadWrite);
}

#[test]
fn failure_before_length_hash_verify_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeLengthAndHashVerify);
}

#[test]
fn failure_before_checkpoint_temp_write_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeCheckpointTempWrite);
}

#[test]
fn failure_before_checkpoint_temp_sync_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeCheckpointTempSync);
}

#[test]
fn failure_before_checkpoint_atomic_replace_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeCheckpointAtomicReplace);
}

#[test]
fn failure_before_directory_namespace_report_prevents_ack() {
    inject_and_expect_no_ack(DirectDurabilityBoundary::BeforeDirectoryNamespaceReport);
}

#[test]
fn failure_before_checkpoint_replace_leaves_prior_checkpoint_bytes_unchanged() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let store = DirectGenerationStore::open_for_session(&root, SESSION_ID, 1).expect("store");
    let seq0 = minimal_synj_segment_bytes();
    let id0 = store
        .logical_identity_for_incoming(0, seq0.len() as u64, &sha256_lower_hex(&seq0))
        .expect("identity");
    let mut pipeline = DirectSegmentDurabilityPipeline::new(&store);
    let initial = DirectCumulativeCheckpoint::initial_for_session(SESSION_ID, 1);
    pipeline
        .persist_for_cumulative_ack(&id0, &seq0, &initial)
        .expect("seed seq0");
    let checkpoint_path = tmp.path().join(store.cumulative_checkpoint_relative_path());
    let prior_bytes = fs::read(&checkpoint_path).expect("read seeded checkpoint");

    let seq1 = minimal_synj_segment_bytes();
    let id1 = store
        .logical_identity_for_incoming(1, seq1.len() as u64, &sha256_lower_hex(&seq1))
        .expect("identity seq1");
    let prior_checkpoint =
        DirectCumulativeCheckpoint::read_durable_from_store(&store).expect("prior checkpoint");
    let mut injected = DirectSegmentDurabilityPipeline::new(&store)
        .with_injected_failure_at(DirectDurabilityBoundary::BeforeCheckpointAtomicReplace);
    let err = injected
        .persist_for_cumulative_ack(&id1, &seq1, &prior_checkpoint)
        .expect_err("injected failure before replace");
    assert!(!err.cumulative_ack_eligible);
    assert_eq!(
        fs::read(&checkpoint_path).expect("checkpoint after failed replace"),
        prior_bytes
    );
    let still = DirectCumulativeCheckpoint::read_durable_from_store(&store).expect("read back");
    assert_eq!(still.highest_contiguous_sequence, 0);
    assert_ne!(still.highest_contiguous_sequence, 1);
}

#[test]
fn identical_retry_after_synced_segment_before_checkpoint_completes_checkpoint() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, initial, bytes) = open_store_and_checkpoint(&root);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");

    let mut interrupted = DirectSegmentDurabilityPipeline::new(&store)
        .with_injected_failure_at(DirectDurabilityBoundary::BeforeCheckpointTempWrite);
    assert!(interrupted
        .persist_for_cumulative_ack(&identity, &bytes, &initial)
        .is_err());

    let mut resumed = DirectSegmentDurabilityPipeline::new(&store);
    let outcome = resumed
        .persist_for_cumulative_ack(&identity, &bytes, &initial)
        .expect("identical durable segment resumes checkpoint");
    assert!(outcome.cumulative_ack_eligible);
    let durable = DirectCumulativeCheckpoint::read_durable_from_store(&store)
        .expect("durable resumed checkpoint");
    assert_eq!(durable.highest_contiguous_sequence, 0);
    assert_eq!(durable.last_segment_sha256_hex, sha256_lower_hex(&bytes));
}

#[test]
fn stale_supplied_checkpoint_cannot_advance_past_durable_authority() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, initial, bytes) = open_store_and_checkpoint(&root);
    let id0 = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity 0");
    DirectSegmentDurabilityPipeline::new(&store)
        .persist_for_cumulative_ack(&id0, &bytes, &initial)
        .expect("seed durable checkpoint");

    let stale_id0 = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("stale identity");
    let err = DirectSegmentDurabilityPipeline::new(&store)
        .persist_for_cumulative_ack(&stale_id0, &bytes, &initial)
        .expect_err("stale caller checkpoint must not override durable checkpoint");
    assert_eq!(
        err.kind,
        DirectSegmentDurabilityFailureKind::CheckpointAuthorityMismatch
    );
    assert!(!err.cumulative_ack_eligible);
    assert!(!err.partial_report.segment_write_completed);
    assert_eq!(
        DirectCumulativeCheckpoint::read_durable_from_store(&store)
            .expect("durable checkpoint")
            .highest_contiguous_sequence,
        0
    );
}

#[test]
fn rejects_invalid_synj_before_checkpoint_and_ack() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, checkpoint, _) = open_store_and_checkpoint(&root);
    let bad_bytes = b"not-a-valid-synj-segment";
    let identity = store
        .logical_identity_for_incoming(0, bad_bytes.len() as u64, &sha256_lower_hex(bad_bytes))
        .expect("identity");
    let mut pipeline = DirectSegmentDurabilityPipeline::new(&store);
    let err = pipeline
        .persist_for_cumulative_ack(&identity, bad_bytes, &checkpoint)
        .expect_err("invalid synj");
    assert_eq!(
        err.kind,
        DirectSegmentDurabilityFailureKind::DurableSegmentVerification
    );
    assert!(!err.cumulative_ack_eligible);
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(&store).is_err());
}

#[test]
fn verifier_reads_durable_on_disk_bytes_not_caller_buffer() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, checkpoint, bytes) = open_store_and_checkpoint(&root);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let seg_rel = store.segment_relative_path(&identity);
    let corrupt_path = tmp.path().join(seg_rel);
    let mut pipeline = DirectSegmentDurabilityPipeline::new(&store).with_test_hooks(
        DirectDurabilityPipelineTestHooks {
            after_segment_file_sync: Some(Box::new(move |store, segment_name| {
                let path = corrupt_path.clone();
                let _ = store;
                let _ = segment_name;
                fs::write(&path, b"corrupted-on-disk").expect("corrupt durable bytes");
            })),
        },
    );
    let err = pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
        .expect_err("durable verify fails");
    assert_eq!(
        err.kind,
        DirectSegmentDurabilityFailureKind::DurableSegmentVerification
    );
    assert!(!err.cumulative_ack_eligible);
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(&store).is_err());
}

#[test]
fn conflicting_existing_segment_fails_without_advancing_checkpoint() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, checkpoint, bytes) = open_store_and_checkpoint(&root);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let seg_path = tmp.path().join(store.segment_relative_path(&identity));
    fs::write(&seg_path, b"wrong-bytes-on-disk").expect("seed conflicting segment");

    let mut pipeline = DirectSegmentDurabilityPipeline::new(&store);
    let err = pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
        .expect_err("existing segment must match identity");
    assert_eq!(
        err.kind,
        DirectSegmentDurabilityFailureKind::ExistingSegmentConflict
    );
    assert!(!err.cumulative_ack_eligible);
    assert!(!err.partial_report.segment_write_completed);
    assert!(err.partial_report.segment_file_sync_completed);
    assert!(!err.partial_report.length_and_hash_verified);
    assert!(DirectCumulativeCheckpoint::read_durable_from_store(&store).is_err());
}

fn inject_and_expect_no_ack(boundary: DirectDurabilityBoundary) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = DestRoot::open(tmp.path()).expect("root");
    let (store, checkpoint, bytes) = open_store_and_checkpoint(&root);
    let identity = store
        .logical_identity_for_incoming(0, bytes.len() as u64, &sha256_lower_hex(&bytes))
        .expect("identity");
    let mut pipeline =
        DirectSegmentDurabilityPipeline::new(&store).with_injected_failure_at(boundary);
    let outcome = pipeline
        .persist_for_cumulative_ack(&identity, &bytes, &checkpoint)
        .expect_err("injected failure");
    assert_eq!(
        outcome.kind,
        DirectSegmentDurabilityFailureKind::InjectedTestFailure
    );
    assert!(!outcome.cumulative_ack_eligible);
    let report: DirectDurabilityReport = outcome.partial_report;
    assert!(!report.cumulative_ack_eligible);
}
