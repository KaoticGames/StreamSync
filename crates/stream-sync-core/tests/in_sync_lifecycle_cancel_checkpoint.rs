//! Task 2.3 lifecycle tracer: cancel requested during segment-1 fetch must be observed at the
//! next cooperative boundary (after that fetch returns), release the active lease, avoid
//! publication/receipt, and leave a checkpoint coherent with durable bytes without reporting Complete.

use std::io::Write;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

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
const DEVICE_ID: &str = "lifecycle-test-device";
const WAIT_BUDGET: Duration = Duration::from_secs(5);

fn sample_manifest(segment_count: u32) -> LocalFinalizationManifest {
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
    let segments: Vec<LocalFinalizationSegment> = (0..segment_count)
        .map(|seq| {
            let body = vec![seq as u8];
            let sha =
                sha256_hex_reader(std::io::Cursor::new(&body), DEFAULT_STREAM_CHUNK).expect("sha");
            LocalFinalizationSegment {
                segment_seq: seq,
                key: format!(
                    "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/{:08}.synj",
                    seq
                ),
                byte_length: body.len() as u64,
                sha256: sha,
            }
        })
        .collect();
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

#[derive(Clone)]
struct SegmentBoundaryCancelClient {
    manifest: LocalFinalizationManifest,
    digest: String,
    next_generation: Arc<AtomicI64>,
    active_proof: Arc<Mutex<Option<LeaseProof>>>,
    releases: Arc<Mutex<Vec<LeaseProof>>>,
    segment_fetches: Arc<Mutex<Vec<u32>>>,
    receipt_posts: Arc<AtomicI64>,
    seg1_download_started: Arc<(Mutex<bool>, Condvar)>,
}

impl SegmentBoundaryCancelClient {
    fn new(manifest: LocalFinalizationManifest) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            digest,
            next_generation: Arc::new(AtomicI64::new(1)),
            active_proof: Arc::new(Mutex::new(None)),
            releases: Arc::new(Mutex::new(Vec::new())),
            segment_fetches: Arc::new(Mutex::new(Vec::new())),
            receipt_posts: Arc::new(AtomicI64::new(0)),
            seg1_download_started: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn releases(&self) -> Vec<LeaseProof> {
        self.releases.lock().unwrap().clone()
    }

    fn segment_fetches(&self) -> Vec<u32> {
        self.segment_fetches.lock().unwrap().clone()
    }

    fn receipt_posts(&self) -> i64 {
        self.receipt_posts.load(Ordering::SeqCst)
    }

    fn wait_for_seg1_download_start(&self, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let (lock, cv) = &*self.seg1_download_started;
        let mut started = lock.lock().unwrap();
        while !*started && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (guard, timeout) = cv
                .wait_timeout(started, remaining)
                .expect("seg1 download condvar");
            started = guard;
            if timeout.timed_out() && Instant::now() >= deadline {
                break;
            }
        }
        *started
    }
}

impl LocalFinalizationClient for SegmentBoundaryCancelClient {
    fn fetch_pending(&self, _limit: u32) -> Result<Vec<LocalPendingItem>, VoiceV2ClientError> {
        Ok(vec![])
    }

    fn claim_delivery(
        &self,
        session_id: &str,
        _device_id: &str,
    ) -> Result<LeaseGrant, VoiceV2ClientError> {
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let token = format!("lease-token-gen-{generation}");
        let proof = LeaseProof {
            generation,
            token: token.clone(),
        };
        *self.active_proof.lock().unwrap() = Some(proof);
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
        _offset: u64,
        length: u64,
        _proof: &LeaseProof,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        self.segment_fetches.lock().unwrap().push(segment_seq);
        if segment_seq == 1 {
            // Signal test thread only; fetch still runs synchronously to completion unless
            // production observes cancel at the next cooperative check after this returns.
            let (lock, cv) = &*self.seg1_download_started;
            {
                let mut started = lock.lock().unwrap();
                *started = true;
            }
            cv.notify_all();
        }
        let byte = segment_seq as u8;
        writer
            .write_all(&[byte])
            .map_err(|e| VoiceV2ClientError::Parse(format!("mock segment write failed: {e}")))?;
        Ok(StemRangeMeta {
            status: 206,
            content_range: format!("bytes 0-{}/1", length.min(1)),
            content_length: length.min(1),
            etag: "\"mock\"".into(),
            total_length: length.min(1),
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

fn wait_for_segment_checkpoint(
    store: &LocalJournalStore,
    segment_seq: u32,
    complete: bool,
    budget: Duration,
) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if let Ok(cp) = store.load_checkpoint() {
            if let Some(entry) = cp.segments.iter().find(|s| s.segment_seq == segment_seq) {
                if entry.complete == complete {
                    return true;
                }
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    false
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

fn assert_segment_checkpoint_coherent_with_durable(
    store: &LocalJournalStore,
    segment_seq: u32,
    bytes_complete: u64,
    complete: bool,
    checkpoint_sha256: Option<&str>,
    expected_byte_length: u64,
    expected_sha256: &str,
) {
    assert!(
        bytes_complete <= expected_byte_length,
        "segment {segment_seq} bytes_complete must stay within manifest bounds"
    );
    let durable_len = store
        .segment_bytes_on_disk(segment_seq)
        .expect("durable segment length");
    assert_eq!(
        bytes_complete, durable_len,
        "segment {segment_seq} checkpoint bytes_complete must match durable file length"
    );
    if complete {
        assert_eq!(
            bytes_complete, expected_byte_length,
            "segment {segment_seq} marked complete must record full manifest length"
        );
        assert_eq!(
            checkpoint_sha256,
            Some(expected_sha256),
            "segment {segment_seq} marked complete must record manifest hash"
        );
        assert!(
            store
                .verify_segment_complete(segment_seq, expected_byte_length, expected_sha256)
                .expect("verify_segment_complete"),
            "segment {segment_seq} durable file must verify when checkpoint marks complete"
        );
    } else {
        assert!(
            checkpoint_sha256.is_none(),
            "segment {segment_seq} must not claim a final hash while incomplete"
        );
        assert!(
            !store
                .verify_segment_complete(segment_seq, expected_byte_length, expected_sha256)
                .expect("verify_segment_complete"),
            "segment {segment_seq} must not verify as fully complete while checkpoint is incomplete"
        );
        let resume = store
            .reconcile_segment_resume_bytes(segment_seq, expected_byte_length)
            .expect("reconcile resume bytes");
        assert_eq!(
            resume, durable_len,
            "segment {segment_seq} durable prefix must remain resumable"
        );
        assert!(
            durable_len < expected_byte_length,
            "segment {segment_seq} must not leave a full durable file without complete checkpoint (false complete)"
        );
    }
}

#[test]
fn cancel_during_segment_one_fetch_aborts_without_publication_or_receipt() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest(2);
    let manifest_for_validate = manifest.clone();
    let seg1_expected_len = manifest.segments[1].byte_length;
    let seg1_expected_sha = manifest.segments[1].sha256.clone();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = SegmentBoundaryCancelClient::new(manifest);
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

    let pending_first = pending.clone();
    let orch_run = orch.clone();
    let client_run = client.clone();
    let worker = thread::spawn(move || orch_run.process_pending(&client_run, &pending_first));

    assert!(
        wait_for_segment_checkpoint(&store, 0, true, WAIT_BUDGET),
        "segment 0 must finish downloading before cancel targets the next segment"
    );
    assert!(
        client.wait_for_seg1_download_start(WAIT_BUDGET),
        "cancel must be requested during segment 1 fetch (before cooperative cancel is observed)"
    );
    orch.cancellation().cancel();

    let cancelled = worker
        .join()
        .expect("process_pending thread")
        .expect_err("cancel must abort the run before completion");
    assert!(
        matches!(cancelled, LocalOrchestratorError::Cancelled),
        "expected Cancelled, got {cancelled:?}"
    );
    assert_eq!(
        orch.phase(),
        LocalFinalizationPhase::Cancelled,
        "orchestrator must surface Cancelled phase after cooperative cancel"
    );
    assert_ne!(
        orch.phase(),
        LocalFinalizationPhase::Complete,
        "cancel must not report Complete"
    );

    let expected_proof = LeaseProof {
        generation: 1,
        token: "lease-token-gen-1".into(),
    };
    let releases = client.releases();
    assert_eq!(
        releases.len(),
        1,
        "cancel must release exactly the active lease once; got {releases:?}"
    );
    assert!(
        proofs_equal(&releases[0], &expected_proof),
        "release must use the active lease proof (generation/token), got {:?} expected {:?}",
        releases[0],
        expected_proof
    );
    assert_eq!(client.receipt_posts(), 0, "cancel must not post a receipt");

    let events = orch.drain_events();
    assert!(
        !progress_has_publication_or_receipt(&events),
        "cancel must not emit publication or receipt progress; got {events:?}"
    );
    assert!(
        resolve_canonical_session_paths(tmp.path(), SESSION_ID).is_err(),
        "cancel must not commit a published output ledger or final session directory"
    );

    let checkpoint = store.load_checkpoint().expect("checkpoint on disk");
    LocalJournalStore::validate_checkpoint_against_manifest(&checkpoint, &manifest_for_validate)
        .expect("checkpoint must stay within manifest bounds");
    let seg0 = checkpoint
        .segments
        .iter()
        .find(|s| s.segment_seq == 0)
        .expect("segment 0 checkpoint entry");
    assert!(seg0.complete, "segment 0 must remain durably complete");
    assert_eq!(seg0.bytes_complete, 1);
    let seg1 = checkpoint
        .segments
        .iter()
        .find(|s| s.segment_seq == 1)
        .expect("segment 1 checkpoint entry");
    assert_segment_checkpoint_coherent_with_durable(
        &store,
        1,
        seg1.bytes_complete,
        seg1.complete,
        seg1.sha256.as_deref(),
        seg1_expected_len,
        &seg1_expected_sha,
    );
    assert!(
        client.segment_fetches().contains(&0),
        "segment 0 must have been downloaded before cancel"
    );
    assert!(
        client.segment_fetches().contains(&1),
        "segment 1 fetch must have started before cancel was observed at the cooperative boundary"
    );
}
