//! Task 2.3 lifecycle tracer: pause at a segment boundary must release the active lease
//! and persist a resumable download checkpoint for a later run.

use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use stream_sync_core::voice_delivery::local_finalization::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id, LeaseGrant,
    LeaseProof, LocalFinalizationControls, LocalFinalizationManifest, LocalFinalizationObjectRef,
    LocalFinalizationOutputPolicy, LocalFinalizationReceipt, LocalFinalizationSegment,
    LocalFinalizationUser, LocalJournalStore, LocalOrchestratorError,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION, LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::{
    sha256_hex_reader, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, LocalPendingItem, StemRangeMeta, VoiceV2ClientError,
    DEFAULT_STREAM_CHUNK, RECOVERY_REPORT_RELATIVE_PATH,
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

/// Blocks segment-0 `fetch_segment_range` after the segment byte is written but before
/// returning `StemRangeMeta`, until the test thread calls `resume`.
#[derive(Clone)]
struct SegmentZeroFetchBarrier {
    entered: Arc<(Mutex<bool>, Condvar)>,
    resume: Arc<(Mutex<bool>, Condvar)>,
}

impl SegmentZeroFetchBarrier {
    fn new() -> Self {
        Self {
            entered: Arc::new((Mutex::new(false), Condvar::new())),
            resume: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn wait_entered(&self, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        let (lock, cv) = &*self.entered;
        let mut entered = lock.lock().unwrap();
        while !*entered && Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let (guard, timeout) = cv
                .wait_timeout(entered, remaining)
                .expect("segment 0 fetch barrier entered");
            entered = guard;
            if timeout.timed_out() && Instant::now() >= deadline {
                break;
            }
        }
        *entered
    }

    fn resume(&self) {
        let (lock, cv) = &*self.resume;
        {
            let mut resumed = lock.lock().unwrap();
            *resumed = true;
        }
        cv.notify_all();
    }

    fn wait_on_fetch_path(&self) {
        let (lock, cv) = &*self.entered;
        {
            let mut entered = lock.lock().unwrap();
            *entered = true;
        }
        cv.notify_all();
        let (rlock, rcv) = &*self.resume;
        let mut resumed = rlock.lock().unwrap();
        while !*resumed {
            resumed = rcv.wait(resumed).expect("segment 0 fetch barrier resume");
        }
    }
}

/// Releases the segment-0 fetch barrier on drop so a panicking test cannot wedge the worker.
struct SegmentZeroFetchBarrierReleaseGuard {
    barrier: SegmentZeroFetchBarrier,
    armed: bool,
}

impl SegmentZeroFetchBarrierReleaseGuard {
    fn arm(barrier: SegmentZeroFetchBarrier) -> Self {
        Self {
            barrier,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for SegmentZeroFetchBarrierReleaseGuard {
    fn drop(&mut self) {
        if self.armed {
            self.barrier.resume();
        }
    }
}

#[derive(Clone)]
struct SegmentBoundaryPauseClient {
    manifest: LocalFinalizationManifest,
    digest: String,
    seg0_fetch_barrier: SegmentZeroFetchBarrier,
    next_generation: Arc<AtomicI64>,
    active_proof: Arc<Mutex<Option<LeaseProof>>>,
    releases: Arc<Mutex<Vec<LeaseProof>>>,
    claim_generations: Arc<Mutex<Vec<i64>>>,
    segment_fetches: Arc<Mutex<Vec<u32>>>,
    receipt_posts: Arc<AtomicI64>,
    release_attempts: Arc<AtomicU32>,
    release_error: Arc<Mutex<Option<VoiceV2ClientError>>>,
}

impl SegmentBoundaryPauseClient {
    fn new(manifest: LocalFinalizationManifest) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            digest,
            seg0_fetch_barrier: SegmentZeroFetchBarrier::new(),
            next_generation: Arc::new(AtomicI64::new(1)),
            active_proof: Arc::new(Mutex::new(None)),
            releases: Arc::new(Mutex::new(Vec::new())),
            claim_generations: Arc::new(Mutex::new(Vec::new())),
            segment_fetches: Arc::new(Mutex::new(Vec::new())),
            receipt_posts: Arc::new(AtomicI64::new(0)),
            release_attempts: Arc::new(AtomicU32::new(0)),
            release_error: Arc::new(Mutex::new(None)),
        }
    }

    fn wait_for_seg0_fetch_barrier(&self, budget: Duration) -> bool {
        self.seg0_fetch_barrier.wait_entered(budget)
    }

    fn seg0_fetch_barrier_release_guard(&self) -> SegmentZeroFetchBarrierReleaseGuard {
        SegmentZeroFetchBarrierReleaseGuard::arm(self.seg0_fetch_barrier.clone())
    }

    fn fail_release_with(&self, err: VoiceV2ClientError) {
        *self.release_error.lock().unwrap() = Some(err);
    }

    fn release_attempts(&self) -> u32 {
        self.release_attempts.load(Ordering::SeqCst)
    }

    fn releases(&self) -> Vec<LeaseProof> {
        self.releases.lock().unwrap().clone()
    }

    fn claim_generations(&self) -> Vec<i64> {
        self.claim_generations.lock().unwrap().clone()
    }

    fn segment_fetches(&self) -> Vec<u32> {
        self.segment_fetches.lock().unwrap().clone()
    }

    fn receipt_posts(&self) -> i64 {
        self.receipt_posts.load(Ordering::SeqCst)
    }
}

impl LocalFinalizationClient for SegmentBoundaryPauseClient {
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
        let byte = segment_seq as u8;
        writer
            .write_all(&[byte])
            .map_err(|e| VoiceV2ClientError::Parse(format!("mock segment write failed: {e}")))?;
        self.segment_fetches.lock().unwrap().push(segment_seq);

        if segment_seq == 0 {
            self.seg0_fetch_barrier.wait_on_fetch_path();
        }

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
        self.release_attempts.fetch_add(1, Ordering::SeqCst);
        self.releases.lock().unwrap().push(proof.clone());
        if let Some(err) = self.release_error.lock().unwrap().clone() {
            return Err(err);
        }
        Ok(())
    }
}

fn orchestrator_error_is_bounded_non_secret(err: &LocalOrchestratorError, secret: &str) -> bool {
    let display = err.to_string();
    !display.contains(secret) && display.len() <= 256
}

fn proofs_equal(a: &LeaseProof, b: &LeaseProof) -> bool {
    a.generation == b.generation && a.token == b.token
}

fn arm_pause_at_seg0_boundary(
    orch: &Arc<LocalFinalizationOrchestrator>,
    client: &SegmentBoundaryPauseClient,
) {
    assert!(
        client.wait_for_seg0_fetch_barrier(WAIT_BUDGET),
        "segment 0 fetch must write its byte and reach the test barrier before pause is armed"
    );
    let guard = client.seg0_fetch_barrier_release_guard();
    orch.set_paused(true);
    client.seg0_fetch_barrier.resume();
    guard.disarm();
}

#[test]
fn pause_at_segment_boundary_does_not_report_ordinary_paused_when_release_fails() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest(2);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let controls = LocalFinalizationControls::default();
    let client = SegmentBoundaryPauseClient::new(manifest);
    client.fail_release_with(VoiceV2ClientError::DeliveryNotFound);
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        tmp.path(),
        DEVICE_ID,
        controls.clone(),
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

    arm_pause_at_seg0_boundary(&orch, &client);

    let result = worker.join().expect("process_pending thread");
    assert_eq!(
        client.release_attempts(),
        1,
        "pause must attempt lease release exactly once even when the server rejects release"
    );
    assert!(
        !matches!(result, Err(LocalOrchestratorError::Paused)),
        "ordinary Paused must mean release succeeded; must not mask release failure (got {result:?})"
    );
    let err = result.expect_err("release failure must surface as a bounded orchestrator error");
    assert!(matches!(
        &err,
        LocalOrchestratorError::PauseLeaseReleaseFailed
    ));
    assert!(
        orchestrator_error_is_bounded_non_secret(&err, "lease-token-gen-1"),
        "release failure must stay bounded and non-secret; got {err:?}"
    );
    assert_eq!(
        client.receipt_posts(),
        0,
        "failed pause release must not post a receipt"
    );

    let checkpoint = store.load_checkpoint().expect("checkpoint on disk");
    let seg0 = checkpoint
        .segments
        .iter()
        .find(|s| s.segment_seq == 0)
        .expect("segment 0 checkpoint entry");
    assert!(
        seg0.complete,
        "verified segment 0 checkpoint must remain intact"
    );
    assert_eq!(seg0.bytes_complete, 1);
    let seg1 = checkpoint
        .segments
        .iter()
        .find(|s| s.segment_seq == 1)
        .expect("segment 1 checkpoint entry");
    assert!(
        !seg1.complete,
        "segment 1 must not be marked complete after boundary pause"
    );
    assert_eq!(seg1.bytes_complete, 0);
}

#[test]
fn pause_at_segment_boundary_releases_lease_and_leaves_resumable_checkpoint() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manifest = sample_manifest(2);
    let seg1_sha256 = manifest.segments[1].sha256.clone();
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let controls = LocalFinalizationControls::default();
    let client = SegmentBoundaryPauseClient::new(manifest);
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        tmp.path(),
        DEVICE_ID,
        controls.clone(),
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

    arm_pause_at_seg0_boundary(&orch, &client);

    let paused = worker
        .join()
        .expect("process_pending thread")
        .expect_err("first run must pause instead of finishing");
    assert!(
        matches!(paused, LocalOrchestratorError::Paused),
        "expected Paused, got {paused:?}"
    );
    assert_eq!(orch.phase(), LocalFinalizationPhase::Paused);

    let expected_proof = LeaseProof {
        generation: 1,
        token: "lease-token-gen-1".into(),
    };
    let releases = client.releases();
    assert_eq!(
        releases.len(),
        1,
        "pause at segment boundary must release exactly the active lease once; got {releases:?}"
    );
    assert!(
        proofs_equal(&releases[0], &expected_proof),
        "release must use the active lease proof (generation/token), got {:?} expected {:?}",
        releases[0],
        expected_proof
    );
    assert_eq!(client.receipt_posts(), 0, "pause must not post a receipt");

    let checkpoint = store.load_checkpoint().expect("checkpoint on disk");
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
    assert!(
        !seg1.complete,
        "segment 1 must not be marked complete after boundary pause"
    );
    assert_eq!(seg1.bytes_complete, 0);
    assert_eq!(
        client.segment_fetches(),
        vec![0],
        "pause must be observed before any segment 1 fetch on the first run"
    );

    orch.set_paused(false);
    let resume_result = orch.process_pending(&client, &pending);
    assert!(
        resume_result.is_err(),
        "junk segment bytes should fail processing after download resume"
    );
    assert_eq!(
        client.segment_fetches(),
        vec![0, 1],
        "resume must skip verified segment 0 and fetch only segment 1"
    );
    assert!(
        store
            .verify_segment_complete(1, 1, &seg1_sha256)
            .expect("verify seg1"),
        "resume run must finish downloading segment 1 from checkpoint"
    );
    let releases_after_resume = client.releases();
    let gen1_release_count = releases_after_resume
        .iter()
        .filter(|p| p.generation == 1)
        .count();
    let gen2_release_count = releases_after_resume
        .iter()
        .filter(|p| p.generation == 2)
        .count();
    assert_eq!(
        gen1_release_count,
        1,
        "generation-1 lease must appear in release history exactly once (pause), never duplicated; got {releases_after_resume:?}"
    );
    assert_eq!(
        gen2_release_count,
        1,
        "processing failure must release the generation-2 lease exactly once; got {releases_after_resume:?}"
    );
    assert_eq!(
        releases_after_resume.len(),
        2,
        "release history must be one generation-1 pause release and one generation-2 cleanup release; got {releases_after_resume:?}"
    );
    let expected_gen2_proof = LeaseProof {
        generation: 2,
        token: "lease-token-gen-2".into(),
    };
    assert!(
        releases_after_resume
            .iter()
            .any(|p| proofs_equal(p, &expected_gen2_proof)),
        "generation-2 cleanup must use the active resume lease proof; got {releases_after_resume:?}"
    );
    assert_eq!(
        client.claim_generations(),
        vec![1, 2],
        "resume must claim a new generation-2 lease after checkpoint resume"
    );
}
