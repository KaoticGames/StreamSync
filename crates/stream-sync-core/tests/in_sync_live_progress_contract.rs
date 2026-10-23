//! RED: live In-Sync progress must be visible during process_pending and retained with a finite cap.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use serde_json::Value;
use stream_sync_core::voice_delivery::local_finalization::{
    compute_local_finalization_manifest_digest, derive_voice_v2_opaque_stem_id, LeaseGrant,
    LeaseProof, LocalFinalizationManifest, LocalFinalizationObjectRef,
    LocalFinalizationOutputPolicy, LocalFinalizationReceipt, LocalFinalizationSegment,
    LocalFinalizationUser, LocalJournalStore, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::{
    sha256_hex_reader, LocalFinalizationClient, LocalFinalizationOrchestrator,
    LocalFinalizationPhase, LocalFinalizationProgressEvent, LocalPendingItem, StemRangeMeta,
    VoiceV2ClientError, DEFAULT_STREAM_CHUNK, RECOVERY_REPORT_RELATIVE_PATH,
};
use stream_sync_core::{
    fs_secret_store, paths_for_root, AppState, OverlayConfig, OverlayServer, CONTROL_TOKEN_HEADER,
};
use tower::ServiceExt;

const SESSION_ID: &str = "550e8400-e29b-41d4-a716-4466554400aa";
const GUILD_ID: &str = "123456789012345678";
const IN_SYNC_PROGRESS_EVENT_RETENTION_CAP: usize = 128;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf()
}

async fn test_router_and_state() -> (axum::Router, Arc<AppState>, tempfile::TempDir) {
    let userdata = tempfile::tempdir().unwrap();
    let config = OverlayConfig {
        port: 4040,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.path().to_path_buf()),
        secret_store: None,
    };
    let (router, state, _services) = OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app");
    {
        let mut cfg = state.discord_voice_config.write().await;
        cfg.recording_parent = Some(userdata.path().to_path_buf().to_string_lossy().to_string());
        cfg.device_id = "test-device".into();
        cfg.host_token = Some("sdk_test_token_placeholder".into());
    }
    (router, state, userdata)
}

async fn post_in_sync_pause(router: &axum::Router, state: &AppState, paused: bool) {
    let origin = format!("http://127.0.0.1:{}", state.port);
    let body = serde_json::json!({ "paused": paused });
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/in-sync/pause")
                .header(header::ORIGIN, origin)
                .header(CONTROL_TOKEN_HEADER, state.control_token())
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request"),
        )
        .await
        .expect("pause response");
    assert_eq!(response.status(), StatusCode::OK);
}

async fn fetch_in_sync_status(router: &axum::Router, state: &AppState) -> Value {
    let origin = format!("http://127.0.0.1:{}", state.port);
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/api/in-sync/status")
                .header(header::ORIGIN, origin)
                .header(CONTROL_TOKEN_HEADER, state.control_token())
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("status response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    serde_json::from_slice(&body).expect("status json")
}

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
struct BlockingDownloadClient {
    manifest: LocalFinalizationManifest,
    digest: String,
    download_started: Arc<(Mutex<bool>, Condvar)>,
    release_download: Arc<AtomicBool>,
}

impl BlockingDownloadClient {
    fn new(manifest: LocalFinalizationManifest) -> Self {
        let digest = compute_local_finalization_manifest_digest(&manifest);
        Self {
            manifest,
            digest,
            download_started: Arc::new((Mutex::new(false), Condvar::new())),
            release_download: Arc::new(AtomicBool::new(false)),
        }
    }

    fn wait_until_download_blocked(&self) {
        let (lock, cv) = &*self.download_started;
        let mut started = lock.lock().unwrap();
        while !*started {
            started = cv.wait(started).unwrap();
        }
    }

    fn release_download(&self) {
        self.release_download.store(true, Ordering::SeqCst);
        let (_, cv) = &*self.download_started;
        cv.notify_all();
    }
}

impl LocalFinalizationClient for BlockingDownloadClient {
    fn fetch_pending(&self, _limit: u32) -> Result<Vec<LocalPendingItem>, VoiceV2ClientError> {
        Ok(vec![])
    }

    fn claim_delivery(
        &self,
        session_id: &str,
        _device_id: &str,
    ) -> Result<LeaseGrant, VoiceV2ClientError> {
        Ok(LeaseGrant {
            session_id: session_id.into(),
            manifest_digest: self.digest.clone(),
            generation: 1,
            token: "lease-token-1234567890".into(),
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
        {
            let (lock, cv) = &*self.download_started;
            let mut started = lock.lock().unwrap();
            *started = true;
            cv.notify_all();
        }
        while !self.release_download.load(Ordering::SeqCst) {
            thread::yield_now();
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
        Ok(())
    }

    fn release_delivery(
        &self,
        _session_id: &str,
        _device_id: &str,
        _proof: &LeaseProof,
    ) -> Result<(), VoiceV2ClientError> {
        Ok(())
    }
}

#[tokio::test]
async fn in_sync_status_prefers_terminal_runtime_phase_over_stale_active_orchestrator() {
    let (router, state, _userdata) = test_router_and_state().await;
    let parent = state
        .discord_voice_config
        .read()
        .await
        .recording_parent
        .clone()
        .expect("recording parent");
    let manifest = sample_manifest(1);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = BlockingDownloadClient::new(manifest);
    let orch = Arc::new(LocalFinalizationOrchestrator::open(
        std::path::Path::new(&parent),
        "test-device",
    ));
    {
        let mut worker = state.in_sync_worker.write().await;
        worker.active = Some(orch.clone());
    }
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let orch_run = orch.clone();
    let client_run = client.clone();
    let worker = thread::spawn(move || orch_run.process_pending(&client_run, &pending));

    client.wait_until_download_blocked();
    {
        let mut runtime = state.in_sync_runtime.write().await;
        runtime.phase = Some(LocalFinalizationPhase::Quarantined);
        runtime.active_session_id = Some(SESSION_ID.into());
    }
    let status = fetch_in_sync_status(&router, &state).await;
    assert_eq!(
        status.get("phase").and_then(|v| v.as_str()),
        Some("quarantined"),
        "terminal runtime phase must win over stale active orchestrator phase; got {status}"
    );

    client.release_download();
    let _ = worker.join().expect("process_pending thread");
}

#[tokio::test]
async fn in_sync_status_includes_live_orchestrator_progress_before_process_pending_returns() {
    let (router, state, _userdata) = test_router_and_state().await;
    let parent = state
        .discord_voice_config
        .read()
        .await
        .recording_parent
        .clone()
        .expect("recording parent");
    let manifest = sample_manifest(1);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = BlockingDownloadClient::new(manifest);
    let orch = Arc::new(LocalFinalizationOrchestrator::open(
        std::path::Path::new(&parent),
        "test-device",
    ));
    {
        let mut worker = state.in_sync_worker.write().await;
        worker.active = Some(orch.clone());
    }
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let orch_run = orch.clone();
    let client_run = client.clone();
    let worker = thread::spawn(move || orch_run.process_pending(&client_run, &pending));

    client.wait_until_download_blocked();
    {
        let mut runtime = state.in_sync_runtime.write().await;
        runtime.phase = Some(LocalFinalizationPhase::Claiming);
        runtime.active_session_id = Some(SESSION_ID.into());
    }
    let status = fetch_in_sync_status(&router, &state).await;
    assert_eq!(
        status.get("phase").and_then(|v| v.as_str()),
        Some("downloading"),
        "GET /api/in-sync/status must report live orchestrator phase while download is in-flight; got {status}"
    );

    let events = status
        .get("progressEvents")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let saw_journal_ready = events.iter().any(|e| {
        e.get("type").and_then(|t| t.as_str()) == Some("journal_ready")
            && e.get("sessionId").and_then(|s| s.as_str()) == Some(SESSION_ID)
    });
    assert!(
        saw_journal_ready,
        "GET /api/in-sync/status must surface in-flight orchestrator progress before \
         process_pending completes; got progressEvents={events:?}"
    );

    client.release_download();
    let _ = worker.join().expect("process_pending thread");
}

struct ReleaseDownloadOnDrop(BlockingDownloadClient);

impl Drop for ReleaseDownloadOnDrop {
    fn drop(&mut self) {
        self.0.release_download();
    }
}

#[tokio::test]
async fn in_sync_status_reports_paused_while_download_blocked_before_process_pending_returns() {
    let (router, state, _userdata) = test_router_and_state().await;
    let parent = state
        .discord_voice_config
        .read()
        .await
        .recording_parent
        .clone()
        .expect("recording parent");
    let manifest = sample_manifest(1);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let client = BlockingDownloadClient::new(manifest);
    let controls = state.in_sync_worker.read().await.controls.clone();
    let orch = Arc::new(LocalFinalizationOrchestrator::open_with_controls(
        std::path::Path::new(&parent),
        "test-device",
        controls,
    ));
    {
        let mut worker = state.in_sync_worker.write().await;
        worker.active = Some(orch.clone());
    }
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let orch_run = orch.clone();
    let client_run = client.clone();
    let worker = thread::spawn(move || orch_run.process_pending(&client_run, &pending));

    {
        let _release_guard = ReleaseDownloadOnDrop(client.clone());
        client.wait_until_download_blocked();
        post_in_sync_pause(&router, &state, true).await;
        let status = fetch_in_sync_status(&router, &state).await;
        assert_eq!(
            status.get("phase").and_then(|v| v.as_str()),
            Some("paused"),
            "GET /api/in-sync/status must surface in-flight pause before process_pending returns; got {status}"
        );
    }

    let _ = worker.join().expect("process_pending thread");
}

#[test]
fn orchestrator_retains_at_most_latest_progress_events() {
    let tmp = tempfile::tempdir().unwrap();
    // >128 verify events before Processing; durable segments skip slow range downloads.
    let manifest = sample_manifest(130);
    let digest = compute_local_finalization_manifest_digest(&manifest);
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    for seg in &manifest.segments {
        let body = vec![seg.segment_seq as u8];
        let path = store.segment_path(seg.segment_seq);
        fs::create_dir_all(path.parent().expect("segment parent")).expect("segment dir");
        fs::write(&path, &body).expect("segment bytes");
    }
    store.persist_manifest(&manifest).expect("seed manifest");
    let client = BlockingDownloadClient::new(manifest);
    let orch = LocalFinalizationOrchestrator::open(tmp.path(), "test-device");
    let pending = LocalPendingItem {
        session_id: SESSION_ID.into(),
        manifest_digest: digest,
        sealed_at_ms: 1,
    };

    let orch_arc = Arc::new(orch);
    let orch_watch = Arc::clone(&orch_arc);
    let watcher = thread::spawn(move || {
        while orch_watch.phase() != LocalFinalizationPhase::Processing {
            thread::yield_now();
        }
        orch_watch.cancellation().cancel();
    });

    client.release_download();
    let _ = orch_arc.process_pending(&client, &pending);
    watcher.join().expect("cancel watcher");

    let drained = orch_arc.drain_events();
    assert!(
        drained.len() <= IN_SYNC_PROGRESS_EVENT_RETENTION_CAP,
        "orchestrator progress vector must be bounded (cap {}), retained {}",
        IN_SYNC_PROGRESS_EVENT_RETENTION_CAP,
        drained.len()
    );
}

#[test]
fn supervisor_style_progress_sink_extend_must_be_bounded() {
    let tmp = tempfile::tempdir().unwrap();
    let paths = paths_for_root(tmp.path(), false).expect("paths");
    let state = AppState::new(paths, repo_root(), 0, false, fs_secret_store(tmp.path()))
        .expect("AppState::new");
    let session_id = SESSION_ID;
    let batch: Vec<LocalFinalizationProgressEvent> = (0..300)
        .map(|i| LocalFinalizationProgressEvent::TimelineProgress {
            session_id: session_id.into(),
            processed_segments: i as u32 + 1,
            total_segments: 300,
        })
        .collect();
    {
        let mut sink = state.in_sync_progress_events.blocking_write();
        sink.extend(batch);
    }
    let retained = state.in_sync_progress_events.blocking_read().len();
    assert!(
        retained <= IN_SYNC_PROGRESS_EVENT_RETENTION_CAP,
        "supervisor/API progress sink must retain at most {} events, held {}",
        IN_SYNC_PROGRESS_EVENT_RETENTION_CAP,
        retained
    );
}
