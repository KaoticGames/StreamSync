//! Task 2.3 lifecycle tracer: publication committed locally before receipt POST fails must
//! re-enter idempotently on the next run and post exactly one matching receipt.
//!
//! Requires `local-opus-native` so integration tests can decode the seeded journal segment.

#![cfg(feature = "local-opus-native")]

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use stream_sync_core::test_support::{
    install_post_publication_receipt_gate, install_storage_preflight_injection,
    resolve_durable_publication_authority, PostPublicationReceiptBoundary, StoragePreflightFailure,
};
use stream_sync_core::voice_delivery::local_finalization::{
    assert_local_finalization_receipt_matches_manifest, compute_local_finalization_manifest_digest,
    derive_voice_v2_opaque_stem_id, encode_journal_file, resolve_canonical_session_paths,
    LeaseGrant, LeaseProof, LocalFinalizationControls, LocalFinalizationManifest,
    LocalFinalizationObjectRef, LocalFinalizationOutputPolicy, LocalFinalizationReceipt,
    LocalFinalizationSegment, LocalFinalizationUser, LocalJournalStore, LocalOrchestratorError,
    JOURNAL_FORMAT_VERSION_V2, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::{
    sha256_hex_reader, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, LocalFinalizationProgressEvent, LocalPendingItem, StemRangeMeta,
    VoiceV2ClientError, DEFAULT_STREAM_CHUNK, RECOVERY_REPORT_RELATIVE_PATH,
};

const SESSION_ID: &str = "550e8400-e29b-41d4-a716-4466554400bb";
const GUILD_ID: &str = "123456789012345678";
const DEVICE_ID: &str = "lifecycle-receipt-idempotency-device";
const TARGET_SAMPLES: u64 = 4_800;

fn valid_segment_body() -> Vec<u8> {
    encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[])
}

fn sample_manifest() -> LocalFinalizationManifest {
    let user = LocalFinalizationUser {
        discord_user_id: "123456789012345678".into(),
        path_nick: "player".into(),
        opaque_stem_id: derive_voice_v2_opaque_stem_id(
            SESSION_ID,
            GUILD_ID,
            "123456789012345678",
            "player",
        ),
    };
    let body = valid_segment_body();
    let sha = sha256_hex_reader(std::io::Cursor::new(&body), DEFAULT_STREAM_CHUNK).expect("sha");
    let segments = vec![LocalFinalizationSegment {
        segment_seq: 0,
        key: format!(
            "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/{:08}.synj",
            0
        ),
        byte_length: body.len() as u64,
        sha256: sha,
    }];
    LocalFinalizationManifest {
        version: 1,
        finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
        session_id: SESSION_ID.into(),
        guild_id: GUILD_ID.into(),
        channel_id: "234567890123456789".into(),
        session_start_wall_ms: 1,
        session_start_monotonic_ns: "100".into(),
        sealed_stop_wall_ms: 2,
        sealed_stop_monotonic_ns: "200".into(),
        target_sample_count_48k: TARGET_SAMPLES,
        journal_sha256: "a".repeat(64),
        recording_manifest: LocalFinalizationObjectRef {
            key: format!(
                "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/manifest/recording.manifest.json"
            ),
            sha256: "b".repeat(64),
            byte_length: 4096,
        },
        sealed_index: LocalFinalizationObjectRef {
            key: format!("voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/index.sealed.json"),
            sha256: "c".repeat(64),
            byte_length: 512,
        },
        segments,
        users: vec![user],
        output: LocalFinalizationOutputPolicy {
            wav_format: LOCAL_FINALIZATION_WAV_FORMAT.into(),
            recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
        },
    }
}

#[derive(Clone, Debug)]
struct RecordedReceiptAttempt {
    receipt: LocalFinalizationReceipt,
    proof: LeaseProof,
}

#[derive(Clone)]
struct LeaseAuthority {
    revoked_generations: Arc<Mutex<Vec<i64>>>,
}

impl LeaseAuthority {
    fn new() -> Self {
        Self {
            revoked_generations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn revoke_generation(&self, generation: i64) {
        self.revoked_generations.lock().unwrap().push(generation);
    }

    fn is_revoked(&self, proof: &LeaseProof) -> bool {
        self.revoked_generations
            .lock()
            .unwrap()
            .contains(&proof.generation)
    }
}

#[derive(Clone)]
struct ReceiptAfterPublicationClient {
    manifest: LocalFinalizationManifest,
    segment_body: Vec<u8>,
    digest: String,
    next_generation: Arc<AtomicI64>,
    fail_receipt_once: Arc<AtomicBool>,
    lease_authority: Option<LeaseAuthority>,
    releases: Arc<Mutex<Vec<LeaseProof>>>,
    claim_generations: Arc<Mutex<Vec<i64>>>,
    receipt_attempts: Arc<Mutex<Vec<RecordedReceiptAttempt>>>,
    receipt_acknowledgements: Arc<Mutex<Vec<LocalFinalizationReceipt>>>,
    renew_proofs: Arc<Mutex<Vec<LeaseProof>>>,
}

impl ReceiptAfterPublicationClient {
    fn new(manifest: LocalFinalizationManifest, segment_body: Vec<u8>) -> Self {
        Self::new_with_options(manifest, segment_body, true, None)
    }

    fn new_with_lease_authority(
        manifest: LocalFinalizationManifest,
        segment_body: Vec<u8>,
    ) -> Self {
        Self::new_with_options(manifest, segment_body, false, Some(LeaseAuthority::new()))
    }

    fn new_with_options(
        manifest: LocalFinalizationManifest,
        segment_body: Vec<u8>,
        fail_receipt_once: bool,
        lease_authority: Option<LeaseAuthority>,
    ) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            segment_body,
            digest,
            next_generation: Arc::new(AtomicI64::new(1)),
            fail_receipt_once: Arc::new(AtomicBool::new(fail_receipt_once)),
            lease_authority,
            releases: Arc::new(Mutex::new(Vec::new())),
            claim_generations: Arc::new(Mutex::new(Vec::new())),
            receipt_attempts: Arc::new(Mutex::new(Vec::new())),
            receipt_acknowledgements: Arc::new(Mutex::new(Vec::new())),
            renew_proofs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn lease_authority(&self) -> Option<&LeaseAuthority> {
        self.lease_authority.as_ref()
    }

    /// Shares lease generation counter with the priming client; isolates replay POST attempts.
    fn clone_with_lease_authority_for_retry(&self) -> Self {
        Self {
            manifest: self.manifest.clone(),
            segment_body: self.segment_body.clone(),
            digest: self.digest.clone(),
            next_generation: Arc::clone(&self.next_generation),
            fail_receipt_once: Arc::new(AtomicBool::new(false)),
            lease_authority: Some(LeaseAuthority::new()),
            releases: Arc::new(Mutex::new(Vec::new())),
            claim_generations: Arc::new(Mutex::new(Vec::new())),
            receipt_attempts: Arc::new(Mutex::new(Vec::new())),
            receipt_acknowledgements: Arc::new(Mutex::new(Vec::new())),
            renew_proofs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn renew_proofs(&self) -> Vec<LeaseProof> {
        self.renew_proofs.lock().unwrap().clone()
    }

    fn releases(&self) -> Vec<LeaseProof> {
        self.releases.lock().unwrap().clone()
    }

    fn claim_generations(&self) -> Vec<i64> {
        self.claim_generations.lock().unwrap().clone()
    }

    fn receipt_attempts(&self) -> Vec<RecordedReceiptAttempt> {
        self.receipt_attempts.lock().unwrap().clone()
    }

    fn receipt_acknowledgements(&self) -> Vec<LocalFinalizationReceipt> {
        self.receipt_acknowledgements.lock().unwrap().clone()
    }
}

impl LocalFinalizationClient for ReceiptAfterPublicationClient {
    fn fetch_pending(&self, _limit: u32) -> Result<Vec<LocalPendingItem>, VoiceV2ClientError> {
        Ok(vec![])
    }

    fn claim_delivery(
        &self,
        session_id: &str,
        _device_id: &str,
    ) -> Result<LeaseGrant, VoiceV2ClientError> {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        self.claim_generations.lock().unwrap().push(generation);
        let token = format!("lease-token-gen-{generation}");
        Ok(LeaseGrant {
            session_id: session_id.into(),
            manifest_digest: self.digest.clone(),
            generation,
            token,
            expires_at_ms: i64::MAX,
        })
    }

    fn renew_lease(
        &self,
        _session_id: &str,
        _device_id: &str,
        proof: &LeaseProof,
    ) -> Result<Option<i64>, VoiceV2ClientError> {
        self.renew_proofs.lock().unwrap().push(proof.clone());
        if self
            .lease_authority
            .as_ref()
            .is_some_and(|auth| auth.is_revoked(proof))
        {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        Ok(None)
    }

    fn fetch_manifest(
        &self,
        _session_id: &str,
        _proof: &LeaseProof,
    ) -> Result<LocalFinalizationManifest, VoiceV2ClientError> {
        Ok(self.manifest.clone())
    }

    fn fetch_segment_range(
        &self,
        _session_id: &str,
        segment_seq: u32,
        offset: u64,
        length: u64,
        _proof: &LeaseProof,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        let body = &self.segment_body;
        assert_eq!(segment_seq, 0);
        let start = offset as usize;
        let end = (offset + length).min(body.len() as u64) as usize;
        let slice = &body[start..end];
        writer
            .write_all(slice)
            .map_err(|e| VoiceV2ClientError::Parse(format!("mock segment write failed: {e}")))?;
        Ok(StemRangeMeta {
            status: 206,
            content_range: format!("bytes {}-{}/{}", start, end - 1, body.len()),
            content_length: slice.len() as u64,
            etag: "\"mock\"".into(),
            total_length: body.len() as u64,
        })
    }

    fn post_receipt(
        &self,
        _session_id: &str,
        receipt: &LocalFinalizationReceipt,
        proof: &LeaseProof,
    ) -> Result<(), VoiceV2ClientError> {
        self.receipt_attempts
            .lock()
            .unwrap()
            .push(RecordedReceiptAttempt {
                receipt: receipt.clone(),
                proof: proof.clone(),
            });
        if self
            .lease_authority
            .as_ref()
            .is_some_and(|auth| auth.is_revoked(proof))
        {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        if self.fail_receipt_once.swap(false, Ordering::SeqCst) {
            return Err(VoiceV2ClientError::Network {
                message: "simulated receipt endpoint unavailable after publication".into(),
                retry_after: None,
            });
        }
        self.receipt_acknowledgements
            .lock()
            .unwrap()
            .push(receipt.clone());
        Ok(())
    }

    fn release_delivery(
        &self,
        _session_id: &str,
        _device_id: &str,
        proof: &LeaseProof,
    ) -> Result<(), VoiceV2ClientError> {
        self.releases.lock().unwrap().push(proof.clone());
        Ok(())
    }
}

fn proofs_equal(a: &LeaseProof, b: &LeaseProof) -> bool {
    a.generation == b.generation && a.token == b.token
}

fn publication_complete_count(events: &[LocalFinalizationProgressEvent]) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                LocalFinalizationProgressEvent::PublicationComplete { .. }
            )
        })
        .count()
}

fn processing_started_count(events: &[LocalFinalizationProgressEvent]) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, LocalFinalizationProgressEvent::ProcessingStarted { .. }))
        .count()
}

fn receipt_acknowledged_count(events: &[LocalFinalizationProgressEvent]) -> usize {
    events
        .iter()
        .filter(|e| {
            matches!(
                e,
                LocalFinalizationProgressEvent::ReceiptAcknowledged { .. }
            )
        })
        .count()
}

fn orchestrator_reports_stale_lease(err: &LocalOrchestratorError) -> bool {
    match err {
        LocalOrchestratorError::Client(VoiceV2ClientError::IdentityConflict) => true,
        LocalOrchestratorError::Other(msg) => {
            msg.contains("lease renewal failed") || msg.contains("lease")
        }
        _ => false,
    }
}

fn receipt_intent_path(store: &LocalJournalStore) -> std::path::PathBuf {
    store.session_root().join("receipt-intent.json")
}

fn receipt_ack_path(store: &LocalJournalStore) -> std::path::PathBuf {
    store.session_root().join("receipt-ack.json")
}

#[test]
fn publication_before_receipt_failure_retries_with_single_matching_receipt() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = ReceiptAfterPublicationClient::new(manifest.clone(), segment_body);
    let controls = LocalFinalizationControls::default();
    let orch = LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls);
    let _store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let first = orch
        .process_pending(&client, &pending)
        .expect_err("receipt failure after publication must abort the first run");
    assert!(
        matches!(first, LocalOrchestratorError::Client(_)),
        "expected client error after receipt failure, got {first:?}"
    );
    assert_ne!(orch.phase(), LocalFinalizationPhase::Complete);

    let published_paths =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published ledger");
    assert!(
        published_paths.final_session_dir.is_dir(),
        "publication must be durably committed before receipt failure is surfaced"
    );

    let attempts_after_first = client.receipt_attempts();
    assert_eq!(
        attempts_after_first.len(),
        1,
        "first run must record exactly one receipt POST attempt before failure"
    );
    let expected_gen1 = LeaseProof {
        generation: 1,
        token: "lease-token-gen-1".into(),
    };
    assert!(
        proofs_equal(&attempts_after_first[0].proof, &expected_gen1),
        "generation-1 lease must carry the first receipt attempt"
    );
    assert_local_finalization_receipt_matches_manifest(&attempts_after_first[0].receipt, &manifest)
        .expect("first receipt attempt must satisfy production receipt/manifest contract");
    assert!(
        client.receipt_acknowledgements().is_empty(),
        "failed receipt POST must not count as an acknowledged receipt"
    );
    let canonical_receipt = attempts_after_first[0].receipt.clone();

    let events_after_first = orch.drain_events();
    assert_eq!(
        publication_complete_count(&events_after_first),
        1,
        "first run must reach publication before receipt failure; got {events_after_first:?}"
    );
    assert!(
        !events_after_first.iter().any(|e| matches!(
            e,
            LocalFinalizationProgressEvent::ReceiptAcknowledged { .. }
        )),
        "receipt must not be acknowledged when POST fails"
    );

    let releases_after_first = client.releases();
    assert_eq!(
        releases_after_first.len(),
        1,
        "receipt failure must release generation-1 lease exactly once; got {releases_after_first:?}"
    );
    assert!(proofs_equal(&releases_after_first[0], &expected_gen1));
    assert_eq!(
        client.claim_generations(),
        vec![1],
        "first run must claim generation-1 only"
    );
    let gen1_fence_renews = client
        .renew_proofs()
        .iter()
        .filter(|p| proofs_equal(p, &expected_gen1))
        .count();
    assert!(
        gen1_fence_renews >= 2,
        "fresh publish path must synchronously fence generation-1 lease before intent and before POST; renew_proofs={:?}",
        client.renew_proofs()
    );

    orch.process_pending(&client, &pending)
        .expect("second run must finish after idempotent publication recovery and receipt POST");
    assert_eq!(orch.phase(), LocalFinalizationPhase::Complete);

    let attempts_after_second = client.receipt_attempts();
    assert_eq!(
        attempts_after_second.len(),
        2,
        "retry must post exactly two receipt attempts across both runs"
    );
    let expected_gen2 = LeaseProof {
        generation: 2,
        token: "lease-token-gen-2".into(),
    };
    assert!(
        proofs_equal(&attempts_after_second[1].proof, &expected_gen2),
        "second receipt attempt must use the generation-2 lease proof"
    );
    assert_eq!(
        attempts_after_second[1].receipt, canonical_receipt,
        "retry receipt body must exactly match the first failed attempt"
    );
    let acknowledgements_after_second = client.receipt_acknowledgements();
    assert_eq!(
        acknowledgements_after_second.len(),
        1,
        "retry must acknowledge exactly one receipt total"
    );
    assert_eq!(
        acknowledgements_after_second[0], canonical_receipt,
        "acknowledged receipt must match the durable first-attempt body"
    );
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "retry must claim a new lease generation"
    );
    let gen2_fence_renews = client
        .renew_proofs()
        .iter()
        .filter(|p| proofs_equal(p, &expected_gen2))
        .count();
    assert!(
        gen2_fence_renews >= 1,
        "receipt-only replay must synchronously fence generation-2 lease before POST; renew_proofs={:?}",
        client.renew_proofs()
    );

    let events_after_second = orch.drain_events();
    assert_eq!(
        publication_complete_count(&events_after_second),
        0,
        "receipt-only retry must not re-emit PublicationComplete; got {events_after_second:?}"
    );
    assert!(
        events_after_second.iter().any(|e| matches!(
            e,
            LocalFinalizationProgressEvent::ReceiptAcknowledged { .. }
        )),
        "second run must acknowledge receipt; got {events_after_second:?}"
    );

    orch.process_pending(&client, &pending)
        .expect("third run with receipt already posted should remain idempotent");
    assert_eq!(
        client.receipt_attempts().len(),
        2,
        "must not post duplicate receipts on further idempotent re-entry"
    );
    assert_eq!(
        client.receipt_acknowledgements().len(),
        1,
        "must not acknowledge additional receipts on idempotent re-entry"
    );

    let paths_after_retry =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published paths");
    assert_eq!(
        paths_after_retry.final_session_dir, published_paths.final_session_dir,
        "idempotent retry must not create a second published session directory"
    );
}

/// Regression: receipt-only recovery must not run full download preflight before durable receipt replay.
#[test]
fn receipt_only_retry_must_complete_without_full_storage_preflight() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = ReceiptAfterPublicationClient::new(manifest.clone(), segment_body);
    let controls = LocalFinalizationControls::default();
    let orch = LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls);
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let first = orch
        .process_pending(&client, &pending)
        .expect_err("receipt failure after publication must abort the first run");
    assert!(
        matches!(first, LocalOrchestratorError::Client(_)),
        "expected client error after receipt failure, got {first:?}"
    );

    let published_paths =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published ledger");
    assert!(
        published_paths.final_session_dir.is_dir(),
        "canonical publication must exist before injecting preflight failure"
    );
    let receipt_state = store
        .load_receipt_durable_state()
        .expect("load receipt durable state");
    assert!(
        receipt_state.intent.is_some(),
        "durable receipt intent must exist after first run"
    );
    assert!(
        receipt_state.ack.is_none(),
        "receipt must remain unacknowledged after failed POST"
    );
    assert_eq!(client.receipt_attempts().len(), 1);
    let canonical_receipt = client.receipt_attempts()[0].receipt.clone();
    let _ = orch.drain_events();

    let _preflight_inject =
        install_storage_preflight_injection(StoragePreflightFailure::InsufficientSpace {
            required_bytes: 512 * 1024 * 1024,
            available_bytes: 0,
        });

    orch.process_pending(&client, &pending).expect(
        "receipt-only retry must post the persisted receipt without full storage preflight",
    );
    assert_eq!(orch.phase(), LocalFinalizationPhase::Complete);

    let attempts_after_second = client.receipt_attempts();
    assert_eq!(
        attempts_after_second.len(),
        2,
        "retry must post exactly one additional receipt under generation 2"
    );
    let expected_gen2 = LeaseProof {
        generation: 2,
        token: "lease-token-gen-2".into(),
    };
    assert!(proofs_equal(
        &attempts_after_second[1].proof,
        &expected_gen2
    ));
    assert_eq!(attempts_after_second[1].receipt, canonical_receipt);
    assert_eq!(client.receipt_acknowledgements().len(), 1);

    let events_after_second = orch.drain_events();
    assert_eq!(
        publication_complete_count(&events_after_second),
        0,
        "receipt-only retry must not re-emit PublicationComplete; got {events_after_second:?}"
    );
    assert_eq!(
        processing_started_count(&events_after_second),
        0,
        "receipt-only retry must not start processing; got {events_after_second:?}"
    );
    assert!(
        events_after_second.iter().any(|e| matches!(
            e,
            LocalFinalizationProgressEvent::ReceiptAcknowledged { .. }
        )),
        "second run must acknowledge receipt; got {events_after_second:?}"
    );
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "retry must claim generation 2 only after generation 1"
    );
}

fn alternate_valid_sha256_hex(base: &str) -> String {
    assert_eq!(base.len(), 64, "digest must be 64 hex chars");
    let mut alt = base.to_string();
    let last = alt.pop().expect("non-empty digest");
    let flipped = if last == 'a' { 'b' } else { 'a' };
    alt.push(flipped);
    alt
}

fn tamper_durable_receipt_intent_stem_sha256(
    store: &LocalJournalStore,
    relative_path: &str,
    sha256: &str,
) {
    let path = store.session_root().join("receipt-intent.json");
    let raw = std::fs::read_to_string(&path).expect("receipt intent must exist");
    let mut value: serde_json::Value =
        serde_json::from_str(&raw).expect("receipt intent must be JSON");
    let stems = value
        .get_mut("stems")
        .and_then(|v| v.as_array_mut())
        .expect("receipt intent must have stems array");
    assert_eq!(
        stems.len(),
        1,
        "fixture manifest must produce exactly one receipt stem"
    );
    let stem = stems[0]
        .as_object_mut()
        .expect("receipt stem must be object");
    assert_eq!(
        stem.get("relativePath").and_then(|v| v.as_str()),
        Some(relative_path),
        "receipt stem relativePath must match the canonical published WAV"
    );
    stem.insert(
        "sha256".into(),
        serde_json::Value::String(sha256.to_string()),
    );
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&value).expect("serialize tampered intent"),
    )
    .expect("write tampered receipt intent");
}

fn flip_one_wav_byte_in_place(wav_path: &std::path::Path) -> (u64, String) {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(wav_path)
        .expect("open canonical published WAV for tamper");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .expect("read canonical published WAV");
    let original_len = bytes.len() as u64;
    assert!(!bytes.is_empty(), "published WAV must be non-empty");
    bytes[0] ^= 0x01;
    file.seek(SeekFrom::Start(0))
        .expect("rewind WAV for durable in-place rewrite");
    file.write_all(&bytes)
        .expect("rewrite tampered WAV in place");
    file.flush().expect("flush tampered WAV");
    file.sync_all()
        .expect("sync tampered WAV before receipt-only recovery");

    let on_disk = std::fs::read(wav_path).expect("read tampered WAV after sync");
    assert_eq!(
        on_disk.len(),
        bytes.len(),
        "tampered WAV must preserve exact byte length on disk"
    );
    let sha = sha256_hex_reader(std::io::Cursor::new(&on_disk), DEFAULT_STREAM_CHUNK)
        .expect("hash on-disk tampered WAV bytes");
    (original_len, sha)
}

fn tamper_durable_receipt_intent_output_digest(
    store: &LocalJournalStore,
    output_manifest_digest: &str,
) {
    let path = store.session_root().join("receipt-intent.json");
    let raw = std::fs::read_to_string(&path).expect("receipt intent must exist");
    let mut value: serde_json::Value =
        serde_json::from_str(&raw).expect("receipt intent must be JSON");
    value
        .as_object_mut()
        .expect("receipt intent must be object")
        .insert(
            "outputManifestDigest".into(),
            serde_json::Value::String(output_manifest_digest.to_string()),
        );
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&value).expect("serialize tampered intent"),
    )
    .expect("write tampered receipt intent");
}

#[test]
fn tampered_receipt_intent_output_digest_fails_closed_without_repost_or_republication() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = ReceiptAfterPublicationClient::new(manifest.clone(), segment_body);
    let controls = LocalFinalizationControls::default();
    let orch = LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls);
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let first = orch
        .process_pending(&client, &pending)
        .expect_err("receipt failure after publication must abort the first run");
    assert!(
        matches!(first, LocalOrchestratorError::Client(_)),
        "expected client error after receipt failure, got {first:?}"
    );

    let published_paths =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published ledger");
    assert!(
        published_paths.final_session_dir.is_dir(),
        "publication must be durably committed before tampering receipt intent"
    );
    assert_eq!(
        client.receipt_attempts().len(),
        1,
        "first run must record exactly one receipt POST attempt"
    );
    assert!(
        client.receipt_acknowledgements().is_empty(),
        "failed receipt POST must not count as an acknowledged receipt"
    );
    let _ = orch.drain_events();

    let canonical_digest = client.receipt_attempts()[0]
        .receipt
        .output_manifest_digest
        .clone();
    let tampered_digest = alternate_valid_sha256_hex(&canonical_digest);
    assert_ne!(tampered_digest, canonical_digest);
    tamper_durable_receipt_intent_output_digest(&store, &tampered_digest);

    let tampered_intent = store
        .load_receipt_durable_state()
        .expect("load tampered intent")
        .intent
        .expect("tampered receipt intent must remain present");
    assert_eq!(
        tampered_intent.output_manifest_digest, tampered_digest,
        "only outputManifestDigest may change in durable receipt intent"
    );
    assert_local_finalization_receipt_matches_manifest(&tampered_intent, &manifest).expect(
        "syntactically valid digest-only tamper must still satisfy delivery manifest contract",
    );

    let second = orch
        .process_pending(&client, &pending)
        .expect_err("tampered output manifest digest must fail closed before another POST");
    assert!(
        matches!(second, LocalOrchestratorError::StoreConflict),
        "expected terminal store conflict for digest/publication mismatch, got {second:?}"
    );
    assert_ne!(orch.phase(), LocalFinalizationPhase::Complete);

    assert_eq!(
        client.receipt_attempts().len(),
        1,
        "digest mismatch must not trigger another receipt POST"
    );
    assert!(
        client.receipt_acknowledgements().is_empty(),
        "tampered intent must not be acknowledged locally"
    );
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "re-entry must claim a fresh lease generation before failing closed"
    );

    let events_after_tamper = orch.drain_events();
    assert_eq!(
        publication_complete_count(&events_after_tamper),
        0,
        "tampered receipt recovery must not re-publish; got {events_after_tamper:?}"
    );

    let paths_after_tamper =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published paths");
    assert_eq!(
        paths_after_tamper.final_session_dir, published_paths.final_session_dir,
        "tampered receipt recovery must not create a second published session directory"
    );
}

/// Regression: receipt-only recovery must verify published artifacts against trusted ledger/api-binding
/// finalized-manifest authority, not self-referential hashes in the durable receipt intent.
#[test]
fn receipt_only_recovery_must_not_trust_receipt_stem_hashes_over_ledger_authority() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = ReceiptAfterPublicationClient::new(manifest.clone(), segment_body);
    let controls = LocalFinalizationControls::default();
    let orch = LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls);
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let first = orch
        .process_pending(&client, &pending)
        .expect_err("receipt failure after publication must abort the first run");
    assert!(
        matches!(first, LocalOrchestratorError::Client(_)),
        "expected client error after receipt failure, got {first:?}"
    );

    let published_paths =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published ledger");
    assert!(
        published_paths.final_session_dir.is_dir(),
        "publication must be durably committed before tampering receipt intent"
    );
    let receipt_state = store
        .load_receipt_durable_state()
        .expect("load receipt durable state");
    assert!(
        receipt_state.intent.is_some(),
        "durable receipt intent must exist after first run"
    );
    assert!(
        receipt_state.ack.is_none(),
        "receipt must remain unacknowledged after failed POST"
    );
    assert_eq!(
        client.receipt_attempts().len(),
        1,
        "first run must record exactly one receipt POST attempt"
    );
    assert!(
        client.receipt_acknowledgements().is_empty(),
        "failed receipt POST must not count as an acknowledged receipt"
    );
    let canonical_receipt = client.receipt_attempts()[0].receipt.clone();
    assert_eq!(
        canonical_receipt.stems.len(),
        1,
        "fixture must publish exactly one canonical WAV stem"
    );
    let stem_meta = &canonical_receipt.stems[0];
    let original_stem_sha = stem_meta.sha256.clone();
    let original_output_digest = canonical_receipt.output_manifest_digest.clone();
    let _ = orch.drain_events();

    let wav_path = published_paths
        .final_session_dir
        .join(&stem_meta.relative_path);
    assert!(
        wav_path.is_file(),
        "canonical published WAV must exist at receipt stem relativePath"
    );
    let (tampered_len, tampered_sha) = flip_one_wav_byte_in_place(&wav_path);
    assert_eq!(
        tampered_len, stem_meta.byte_length,
        "tampered WAV must preserve exact byte length"
    );
    assert_ne!(
        tampered_sha, original_stem_sha,
        "tampered WAV hash must differ from generation-1 receipt stem hash"
    );

    tamper_durable_receipt_intent_stem_sha256(&store, &stem_meta.relative_path, &tampered_sha);

    let tampered_intent = store
        .load_receipt_durable_state()
        .expect("load tampered intent")
        .intent
        .expect("tampered receipt intent must remain present");
    assert_eq!(
        tampered_intent.output_manifest_digest, original_output_digest,
        "outputManifestDigest must remain the trusted generation-1 ledger-bound digest"
    );
    assert_eq!(
        tampered_intent.stems[0].sha256, tampered_sha,
        "durable receipt stem sha256 must match tampered WAV bytes"
    );
    assert_local_finalization_receipt_matches_manifest(&tampered_intent, &manifest)
        .expect("mutually tampered receipt must still satisfy delivery manifest syntax contract");

    let on_disk_sha = sha256_hex_reader(
        std::fs::File::open(&wav_path).expect("open tampered WAV"),
        DEFAULT_STREAM_CHUNK,
    )
    .expect("hash on-disk tampered WAV");
    assert_eq!(on_disk_sha, tampered_sha);
    assert_ne!(on_disk_sha, original_stem_sha);

    let authority = resolve_durable_publication_authority(tmp.path(), SESSION_ID)
        .expect("trusted ledger/api-binding publication authority must resolve");
    assert_eq!(
        authority.output_manifest_digest, original_output_digest,
        "ledger authority must still bind the original finalized manifest digest"
    );
    assert!(
        LocalJournalStore::verify_canonical_publication_matches_receipt(
            tmp.path(),
            &canonical_receipt
        )
        .is_err(),
        "generation-1 receipt stem hashes must no longer match tampered on-disk artifacts"
    );
    assert!(
        LocalJournalStore::verify_canonical_publication_matches_receipt(
            tmp.path(),
            &tampered_intent
        )
        .is_err(),
        "receipt stem metadata must match ledger-bound authority, not only on-disk bytes"
    );

    let second = orch
        .process_pending(&client, &pending)
        .expect_err(
            "receipt-only recovery must fail closed when on-disk artifacts disagree with ledger authority",
        );
    assert!(
        matches!(second, LocalOrchestratorError::StoreConflict),
        "expected terminal store conflict for artifact/ledger mismatch, got {second:?}"
    );
    assert_ne!(orch.phase(), LocalFinalizationPhase::Complete);

    assert_eq!(
        client.receipt_attempts().len(),
        1,
        "artifact tamper must not trigger another receipt POST"
    );
    assert!(
        client.receipt_acknowledgements().is_empty(),
        "tampered publication must not be acknowledged"
    );
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "re-entry must claim generation 2 before failing closed"
    );

    let events_after_tamper = orch.drain_events();
    assert_eq!(
        publication_complete_count(&events_after_tamper),
        0,
        "tampered receipt recovery must not re-publish; got {events_after_tamper:?}"
    );
    assert_eq!(
        processing_started_count(&events_after_tamper),
        0,
        "tampered receipt recovery must not start processing; got {events_after_tamper:?}"
    );

    let paths_after_tamper =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published paths");
    assert_eq!(
        paths_after_tamper.final_session_dir, published_paths.final_session_dir,
        "tampered receipt recovery must not create a second published session directory"
    );
}

/// Regression: `publish_and_receipt` must synchronously fence lease authority after durable
/// publication and before receipt intent or POST. Exercises the fresh post-publication intent path
/// (first `process_pending` run); receipt-only replay uses the same fence at
/// `BeforeReceiptReplayPost` (see `stale_lease_must_fence_before_receipt_replay_post`).
#[test]
fn stale_lease_must_fence_after_publication_before_receipt_side_effects() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client =
        ReceiptAfterPublicationClient::new_with_lease_authority(manifest.clone(), segment_body);
    let controls = LocalFinalizationControls::default();
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        tmp.path(),
        DEVICE_ID,
        controls,
    ));
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let (_gate, arrived_rx, resume_tx) = install_post_publication_receipt_gate(
        tmp.path(),
        SESSION_ID,
        PostPublicationReceiptBoundary::AfterPublicationBeforeReceiptIntent,
    );

    let orch_worker = Arc::clone(&orch);
    let client_worker = client.clone();
    let worker = thread::Builder::new()
        .name("lifecycle-receipt-lease-fence".into())
        .spawn(move || orch_worker.process_pending(&client_worker, &pending))
        .expect("spawn process_pending worker");

    arrived_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("worker must block at post-publication receipt gate");

    let published_paths =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published ledger");
    assert!(
        published_paths.final_session_dir.is_dir(),
        "canonical publication must be durably committed before the receipt boundary gate"
    );
    let events_at_gate = orch.progress_events_snapshot();
    assert_eq!(
        publication_complete_count(&events_at_gate),
        1,
        "PublicationComplete must be emitted before the receipt boundary; got {events_at_gate:?}"
    );
    assert!(
        !receipt_intent_path(&store).exists(),
        "receipt-intent.json must not exist before lease fence at publication boundary"
    );
    assert!(
        !receipt_ack_path(&store).exists(),
        "receipt-ack.json must not exist before lease fence at publication boundary"
    );
    assert_eq!(
        client.receipt_attempts().len(),
        0,
        "no receipt POST attempt may occur before lease fence at publication boundary"
    );

    client
        .lease_authority()
        .expect("lease authority fixture")
        .revoke_generation(1);
    resume_tx.send(()).expect("release post-publication gate");

    let run_result = worker.join().expect("join process_pending worker");
    let err =
        run_result.expect_err("revoked generation-1 lease must fail closed at receipt boundary");
    assert!(
        orchestrator_reports_stale_lease(&err),
        "expected terminal stale-lease / lease-renewal error after boundary, got {err:?}"
    );

    let paths_after =
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).expect("published paths");
    assert_eq!(
        paths_after.final_session_dir, published_paths.final_session_dir,
        "canonical publication must be retained after stale-lease fence failure"
    );

    let receipt_state = store
        .load_receipt_durable_state()
        .expect("load receipt durable state");
    assert!(
        receipt_state.intent.is_none(),
        "stale generation-1 lease must not author durable receipt intent before POST; got intent={:?}",
        receipt_state.intent
    );
    assert!(
        receipt_state.ack.is_none(),
        "stale lease must not persist receipt ack"
    );
    assert_eq!(
        client.receipt_attempts().len(),
        0,
        "stale generation-1 lease must not attempt receipt POST with revoked proof; attempts={:?}",
        client.receipt_attempts()
    );

    let events_after = orch.drain_events();
    assert_eq!(
        receipt_acknowledged_count(&events_at_gate) + receipt_acknowledged_count(&events_after),
        0,
        "stale lease must not emit ReceiptAcknowledged"
    );

    let expected_gen1 = LeaseProof {
        generation: 1,
        token: "lease-token-gen-1".into(),
    };
    let releases = client.releases();
    assert_eq!(
        releases.len(),
        1,
        "generation-1 lease must be released exactly once after boundary failure; got {releases:?}"
    );
    assert!(proofs_equal(&releases[0], &expected_gen1));
    assert_eq!(client.claim_generations(), vec![1]);
}

/// Regression: receipt-only replay must fence lease authority immediately before POST, using the
/// same synchronous primitive as the fresh post-publication path.
#[test]
fn stale_lease_must_fence_before_receipt_replay_post() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let segment_body = valid_segment_body();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = ReceiptAfterPublicationClient::new(manifest.clone(), segment_body.clone());
    let controls = LocalFinalizationControls::default();
    let orch =
        LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls.clone());
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest.clone(),
        sealed_at_ms: 1,
    };

    let first = orch
        .process_pending(&client, &pending)
        .expect_err("prime durable receipt intent via failed POST");
    assert!(matches!(first, LocalOrchestratorError::Client(_)));
    assert!(
        store
            .load_receipt_durable_state()
            .expect("receipt state")
            .intent
            .is_some(),
        "first run must persist receipt intent before replay test"
    );
    assert_eq!(client.receipt_attempts().len(), 1);
    let _ = orch.drain_events();

    let client = client.clone_with_lease_authority_for_retry();
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        tmp.path(),
        DEVICE_ID,
        controls.clone(),
    ));
    let (_gate, arrived_rx, resume_tx) = install_post_publication_receipt_gate(
        tmp.path(),
        SESSION_ID,
        PostPublicationReceiptBoundary::BeforeReceiptReplayPost,
    );

    let orch_worker = Arc::clone(&orch);
    let client_worker = client.clone();
    let worker = thread::Builder::new()
        .name("lifecycle-receipt-replay-fence".into())
        .spawn(move || orch_worker.process_pending(&client_worker, &pending))
        .expect("spawn replay worker");

    arrived_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("replay worker must block before receipt POST");

    assert_eq!(
        client.receipt_attempts().len(),
        0,
        "replay must not POST before lease fence at replay boundary"
    );

    client
        .lease_authority()
        .expect("lease authority fixture")
        .revoke_generation(2);
    resume_tx.send(()).expect("release replay gate");

    let run_result = worker.join().expect("join replay worker");
    let err =
        run_result.expect_err("revoked generation-2 lease must fail closed before replay POST");
    assert!(
        orchestrator_reports_stale_lease(&err),
        "expected stale-lease fence before replay POST, got {err:?}"
    );
    assert_eq!(
        client.receipt_attempts().len(),
        0,
        "stale lease must not replay receipt POST under revoked generation-2; attempts={:?}",
        client.receipt_attempts()
    );
    assert!(
        store
            .load_receipt_durable_state()
            .expect("receipt state")
            .ack
            .is_none(),
        "stale replay must not acknowledge receipt locally"
    );
    let events_after = orch.drain_events();
    assert_eq!(
        receipt_acknowledged_count(&events_after),
        0,
        "stale replay must not emit ReceiptAcknowledged; got {events_after:?}"
    );
}
