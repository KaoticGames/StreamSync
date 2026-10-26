//! Task 2.3 lifecycle tracer: abrupt failure mid-segment download must leave a durable partial
//! prefix, release the active lease without publication/receipt, and resume from on-disk length
//! on the next run without re-fetching the verified prefix.

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use stream_sync_core::voice_delivery::local_finalization::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id,
    resolve_canonical_session_paths, LeaseGrant, LeaseProof, LocalFinalizationControls,
    LocalFinalizationManifest, LocalFinalizationObjectRef, LocalFinalizationOutputPolicy,
    LocalFinalizationReceipt, LocalFinalizationSegment, LocalFinalizationUser, LocalJournalStore,
    LocalOrchestratorError, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::{
    sha256_hex_reader, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, LocalFinalizationProgressEvent, LocalPendingItem, StemRangeMeta,
    VoiceV2ClientError, DEFAULT_STREAM_CHUNK, RECOVERY_REPORT_RELATIVE_PATH,
};

const SESSION_ID: &str = "550e8400-e29b-41d4-a716-4466554400aa";
const GUILD_ID: &str = "123456789012345678";
const DEVICE_ID: &str = "lifecycle-download-resume-device";
const SEGMENT_BYTE_LENGTH: usize = 96;
const PARTIAL_PREFIX_BYTES: usize = 37;

fn segment_body(seq: u32) -> Vec<u8> {
    (0..SEGMENT_BYTE_LENGTH)
        .map(|i| ((seq as usize * 17 + i * 3) & 0xff) as u8)
        .collect()
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
    let body = segment_body(0);
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
        target_sample_count_48k: 48_000,
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

type RangeRequest = (u32, u64, u64);

#[derive(Clone)]
struct AbruptMidDownloadClient {
    manifest: LocalFinalizationManifest,
    segment_bodies: Vec<Vec<u8>>,
    digest: String,
    next_generation: Arc<AtomicI64>,
    fail_first_chunk: Arc<AtomicBool>,
    releases: Arc<Mutex<Vec<LeaseProof>>>,
    claim_generations: Arc<Mutex<Vec<i64>>>,
    range_requests: Arc<Mutex<Vec<RangeRequest>>>,
    fetch_proofs: Arc<Mutex<Vec<LeaseProof>>>,
    receipt_posts: Arc<AtomicI64>,
}

impl AbruptMidDownloadClient {
    fn new(manifest: LocalFinalizationManifest, segment_bodies: Vec<Vec<u8>>) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            segment_bodies,
            digest,
            next_generation: Arc::new(AtomicI64::new(1)),
            fail_first_chunk: Arc::new(AtomicBool::new(true)),
            releases: Arc::new(Mutex::new(Vec::new())),
            claim_generations: Arc::new(Mutex::new(Vec::new())),
            range_requests: Arc::new(Mutex::new(Vec::new())),
            fetch_proofs: Arc::new(Mutex::new(Vec::new())),
            receipt_posts: Arc::new(AtomicI64::new(0)),
        }
    }

    fn releases(&self) -> Vec<LeaseProof> {
        self.releases.lock().unwrap().clone()
    }

    fn claim_generations(&self) -> Vec<i64> {
        self.claim_generations.lock().unwrap().clone()
    }

    fn range_requests(&self) -> Vec<RangeRequest> {
        self.range_requests.lock().unwrap().clone()
    }

    fn fetch_proofs(&self) -> Vec<LeaseProof> {
        self.fetch_proofs.lock().unwrap().clone()
    }

    fn receipt_posts(&self) -> i64 {
        self.receipt_posts.load(Ordering::SeqCst)
    }
}

impl LocalFinalizationClient for AbruptMidDownloadClient {
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
        proof: &LeaseProof,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        self.fetch_proofs.lock().unwrap().push(proof.clone());
        self.range_requests
            .lock()
            .unwrap()
            .push((segment_seq, offset, length));
        let body = &self.segment_bodies[segment_seq as usize];
        if segment_seq == 0 && offset == 0 && self.fail_first_chunk.swap(false, Ordering::SeqCst) {
            writer
                .write_all(&body[..PARTIAL_PREFIX_BYTES])
                .map_err(|e| {
                    VoiceV2ClientError::Parse(format!("mock partial write failed: {e}"))
                })?;
            return Err(VoiceV2ClientError::Network {
                message: "simulated abrupt disconnect mid-chunk".into(),
                retry_after: None,
            });
        }
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
        _receipt: &LocalFinalizationReceipt,
        _proof: &LeaseProof,
    ) -> Result<(), VoiceV2ClientError> {
        self.receipt_posts.fetch_add(1, Ordering::SeqCst);
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

fn progress_has_publication_or_receipt(events: &[LocalFinalizationProgressEvent]) -> bool {
    events.iter().any(|e| {
        matches!(
            e,
            LocalFinalizationProgressEvent::PublicationComplete { .. }
                | LocalFinalizationProgressEvent::ReceiptAcknowledged { .. }
        )
    })
}

#[test]
fn abrupt_mid_segment_download_resumes_from_durable_prefix_without_redownload() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest();
    let seg0_len = manifest.segments[0].byte_length;
    let seg0_sha = manifest.segments[0].sha256.clone();
    let body = segment_body(0);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = AbruptMidDownloadClient::new(manifest.clone(), vec![body]);
    let controls = LocalFinalizationControls::default();
    let orch = LocalFinalizationOrchestrator::open_with_controls(tmp.path(), DEVICE_ID, controls);
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let interrupted = orch
        .process_pending(&client, &pending)
        .expect_err("mid-chunk network failure must abort the first run");
    assert!(
        matches!(interrupted, LocalOrchestratorError::Client(_)),
        "expected client/network error, got {interrupted:?}"
    );
    assert_ne!(orch.phase(), LocalFinalizationPhase::Complete);

    let expected_gen1 = LeaseProof {
        generation: 1,
        token: "lease-token-gen-1".into(),
    };
    let releases_after_interrupt = client.releases();
    assert_eq!(
        releases_after_interrupt.len(),
        1,
        "interruption must release exactly the generation-1 lease once; got {releases_after_interrupt:?}"
    );
    assert!(
        proofs_equal(&releases_after_interrupt[0], &expected_gen1),
        "release must use generation-1 proof; got {:?}",
        releases_after_interrupt[0]
    );
    assert_eq!(
        client.receipt_posts(),
        0,
        "interruption must not post a receipt"
    );
    assert!(
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).is_err(),
        "interruption must not commit publication ledger or final session directory"
    );
    let events_after_interrupt = orch.drain_events();
    assert!(
        !progress_has_publication_or_receipt(&events_after_interrupt),
        "interruption must not emit publication or receipt progress; got {events_after_interrupt:?}"
    );

    let durable_prefix = store
        .segment_bytes_on_disk(0)
        .expect("durable segment length after interruption");
    assert_eq!(
        durable_prefix, PARTIAL_PREFIX_BYTES as u64,
        "interruption must leave exactly the written partial prefix on disk"
    );
    let checkpoint_after_interrupt = store.load_checkpoint().expect("checkpoint");
    if let Some(seg0_cp) = checkpoint_after_interrupt
        .segments
        .iter()
        .find(|s| s.segment_seq == 0)
    {
        assert!(
            !seg0_cp.complete,
            "interrupted segment must not be marked complete (false complete)"
        );
        assert!(
            seg0_cp.sha256.is_none(),
            "incomplete segment must not record a final hash"
        );
        assert!(
            seg0_cp.bytes_complete <= durable_prefix,
            "checkpoint bytes_complete must not run ahead of durable prefix after interruption"
        );
    }
    assert!(
        !store
            .verify_segment_complete(0, seg0_len, &seg0_sha)
            .expect("verify"),
        "durable prefix must not verify as the full segment"
    );
    assert_eq!(
        store
            .reconcile_segment_resume_bytes(0, seg0_len)
            .expect("reconcile"),
        PARTIAL_PREFIX_BYTES as u64,
        "resume offset must follow durable file length, not stale checkpoint alone"
    );

    let resume_result = orch.process_pending(&client, &pending);
    assert!(
        resume_result.is_err(),
        "non-synj segment bytes should fail media processing after download resume succeeds"
    );
    assert!(
        store
            .verify_segment_complete(0, seg0_len, &seg0_sha)
            .expect("verify after resume"),
        "resume run must finish downloading and hashing segment 0"
    );

    let range_requests = client.range_requests();
    assert_eq!(
        range_requests,
        vec![
            (0, 0, seg0_len),
            (
                0,
                PARTIAL_PREFIX_BYTES as u64,
                seg0_len - PARTIAL_PREFIX_BYTES as u64,
            ),
        ],
        "resume must request only the remaining byte range after durable prefix"
    );

    let fetch_proofs = client.fetch_proofs();
    assert_eq!(
        fetch_proofs.len(),
        2,
        "expected one interrupted fetch and one resume fetch; got {fetch_proofs:?}"
    );
    assert!(
        proofs_equal(&fetch_proofs[0], &expected_gen1),
        "interrupted fetch must use generation-1 proof"
    );
    let expected_gen2 = LeaseProof {
        generation: 2,
        token: "lease-token-gen-2".into(),
    };
    assert!(
        proofs_equal(&fetch_proofs[1], &expected_gen2),
        "resume fetch must use generation-2 proof, not stale generation-1; got {:?}",
        fetch_proofs[1]
    );

    let releases_after_resume = client.releases();
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "resume must claim a new lease generation"
    );
    let gen1_release_count = releases_after_resume
        .iter()
        .filter(|p| p.generation == 1)
        .count();
    let gen2_release_count = releases_after_resume
        .iter()
        .filter(|p| p.generation == 2)
        .count();
    assert_eq!(
        gen1_release_count, 1,
        "generation-1 release must not duplicate"
    );
    assert_eq!(
        gen2_release_count, 1,
        "processing failure must release generation-2 lease exactly once"
    );
    assert!(
        releases_after_resume
            .iter()
            .any(|p| proofs_equal(p, &expected_gen2)),
        "generation-2 cleanup must use the active resume lease proof"
    );
    assert_eq!(
        client.receipt_posts(),
        0,
        "media failure must not post receipt"
    );

    let checkpoint_after_resume = store.load_checkpoint().expect("checkpoint after resume");
    let seg0_final = checkpoint_after_resume
        .segments
        .iter()
        .find(|s| s.segment_seq == 0)
        .expect("segment 0 checkpoint");
    assert!(
        seg0_final.complete,
        "verified download must durably mark segment complete before later processing fails"
    );
    assert_eq!(seg0_final.sha256.as_deref(), Some(seg0_sha.as_str()));
    assert_eq!(
        seg0_final.bytes_complete, seg0_len,
        "checkpoint bytes_complete must match fully downloaded durable file"
    );
    assert_eq!(
        store.segment_bytes_on_disk(0).expect("durable len"),
        seg0_len,
        "durable file length must match manifest after resume download"
    );
    assert!(
        store
            .verify_segment_complete(0, seg0_len, &seg0_sha)
            .expect("final verify"),
        "checkpoint and on-disk segment must stay coherent after media processing failure"
    );
    assert_ne!(
        orch.phase(),
        LocalFinalizationPhase::Complete,
        "media failure must not report overall Complete"
    );
}
