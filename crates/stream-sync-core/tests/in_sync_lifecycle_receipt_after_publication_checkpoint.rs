//! Task 2.3 lifecycle tracer: publication committed locally before receipt POST fails must
//! re-enter idempotently on the next run and post exactly one matching receipt.
//!
//! Requires `local-opus-native` so integration tests can decode the seeded journal segment.

#![cfg(feature = "local-opus-native")]

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

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

#[derive(Clone)]
struct RecordedReceiptAttempt {
    receipt: LocalFinalizationReceipt,
    proof: LeaseProof,
}

#[derive(Clone)]
struct ReceiptAfterPublicationClient {
    manifest: LocalFinalizationManifest,
    segment_body: Vec<u8>,
    digest: String,
    next_generation: Arc<AtomicI64>,
    fail_receipt_once: Arc<AtomicBool>,
    releases: Arc<Mutex<Vec<LeaseProof>>>,
    claim_generations: Arc<Mutex<Vec<i64>>>,
    receipt_attempts: Arc<Mutex<Vec<RecordedReceiptAttempt>>>,
    receipt_acknowledgements: Arc<Mutex<Vec<LocalFinalizationReceipt>>>,
}

impl ReceiptAfterPublicationClient {
    fn new(manifest: LocalFinalizationManifest, segment_body: Vec<u8>) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            segment_body,
            digest,
            next_generation: Arc::new(AtomicI64::new(1)),
            fail_receipt_once: Arc::new(AtomicBool::new(true)),
            releases: Arc::new(Mutex::new(Vec::new())),
            claim_generations: Arc::new(Mutex::new(Vec::new())),
            receipt_attempts: Arc::new(Mutex::new(Vec::new())),
            receipt_acknowledgements: Arc::new(Mutex::new(Vec::new())),
        }
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
        _proof: &LeaseProof,
    ) -> Result<Option<i64>, VoiceV2ClientError> {
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

fn alternate_valid_sha256_hex(base: &str) -> String {
    assert_eq!(base.len(), 64, "digest must be 64 hex chars");
    let mut alt = base.to_string();
    let last = alt.pop().expect("non-empty digest");
    let flipped = if last == 'a' { 'b' } else { 'a' };
    alt.push(flipped);
    alt
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
