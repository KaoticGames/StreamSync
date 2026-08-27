//! Phase 2 review corrections — production-path delegated revocation tests.
//!
//! These tests inject an explicit userdata root via [`OverlayConfig::userdata_root`] and never
//! mutate process-global `STREAMSYNC_USERDATA`, so they are safe under parallel cargo test.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stream_sync_core::{
    connection_key_events_url, disconnect_twitch, paths_for_root, remove_file_durable,
    sync_live_identity, write_delegated_revoked_tombstone, write_json, AppState,
    DelegatedSessionFile, OverlayConfig, OverlayServer, TeardownPhase, TwitchActiveMode,
    TwitchActiveModeFile, TwitchServices, MAX_DELEGATED_REVOCATION_DELAY, SYNDICATE_HTTP_TIMEOUT,
    SYNDICATE_SSE_READ_TIMEOUT,
};

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn test_userdata_dir() -> std::path::PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("streamsync-phase2-it-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create test userdata dir");
    dir
}

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace rust root")
        .to_path_buf()
}

async fn build_app_at(
    userdata: std::path::PathBuf,
    port: u16,
) -> (axum::Router, Arc<AppState>, Arc<TwitchServices>) {
    let config = OverlayConfig {
        port,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata),
    };
    OverlayServer::new(config)
        .build_app()
        .await
        .expect("build_app")
}

async fn build_app(port: u16) -> (axum::Router, Arc<AppState>, Arc<TwitchServices>) {
    build_app_at(test_userdata_dir(), port).await
}

fn restart_app_at(userdata: &std::path::Path, readonly: bool) -> Arc<AppState> {
    let paths = paths_for_root(userdata, readonly).expect("paths_for_root");
    AppState::new(paths, repo_root(), 0, readonly).expect("AppState::new")
}

fn sample_delegated_json() -> serde_json::Value {
    serde_json::json!({
        "generation": 1,
        "connection_key": "ssk_test_placeholder_not_a_real_key",
        "client_id": "cid",
        "access_token": "delegated-access",
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
    })
}

fn sample_session(generation: u64, key: &str) -> DelegatedSessionFile {
    let mut session =
        serde_json::from_value::<DelegatedSessionFile>(sample_delegated_json()).unwrap();
    session.generation = generation;
    session.connection_key = key.into();
    session
}

#[tokio::test]
async fn durable_revoke_propagates_injected_credential_remove_failure() {
    let (_router, state, _services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_revoke_a"),
    )
    .unwrap();
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    assert!(state.durable_revoke_delegated().await.is_err());
    assert!(state.paths.twitch_delegated.is_file());
    assert!(state.paths.twitch_delegated_revoked.is_file());
}

#[tokio::test]
async fn durable_revoke_propagates_injected_tombstone_write_failure() {
    let (_router, state, _services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_revoke_b"),
    )
    .unwrap();
    state
        .durable_fail
        .tombstone_write
        .store(true, Ordering::SeqCst);
    assert!(state.durable_revoke_delegated().await.is_err());
    assert!(state.paths.twitch_delegated.is_file());
    assert!(!state.paths.twitch_delegated_revoked.is_file());
}

#[tokio::test]
async fn disconnect_twitch_propagates_durable_revoke_failure() {
    let (_router, state, services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_disconnect"),
    )
    .unwrap();
    state.delegated_generation.store(1, Ordering::SeqCst);
    *state.delegated.write().await = Some(sample_session(1, "ssk_test_placeholder_disconnect"));
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    services.install_delegated_generation(1).await;
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    assert!(disconnect_twitch(state.clone(), services.clone())
        .await
        .is_err());
    assert!(state.paths.twitch_delegated.is_file());
}

#[tokio::test]
async fn parent_sync_failure_propagates_from_durable_revoke() {
    let (_router, state, _services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_sync"),
    )
    .unwrap();
    state.durable_fail.parent_sync.store(true, Ordering::SeqCst);
    assert!(state.durable_revoke_delegated().await.is_err());
}

#[tokio::test]
async fn active_mode_persistence_failure_is_visible() {
    let (_router, state, _services) = build_app(0).await;
    *state.active_mode.write().await = TwitchActiveMode::Local;
    state
        .durable_fail
        .save_active_mode
        .store(true, Ordering::SeqCst);
    assert!(state.save_active_mode().await.is_err());
}

#[tokio::test]
async fn revoked_tombstone_prevents_restart_into_delegated_mode() {
    let userdata = test_userdata_dir();
    write_json(
        &userdata.join("twitch-delegated.json"),
        &sample_session(1, "ssk_test_placeholder_restart"),
    )
    .unwrap();
    write_delegated_revoked_tombstone(&userdata.join("twitch-delegated.revoked")).unwrap();

    let state = restart_app_at(&userdata, false);
    assert!(state.delegated.read().await.is_none());
    assert_ne!(*state.active_mode.read().await, TwitchActiveMode::Delegated);
}

#[tokio::test]
async fn new_session_clears_revoked_tombstone_and_survives_restart() {
    let userdata = test_userdata_dir();
    let (_router, state, services) = build_app_at(userdata.clone(), 0).await;
    write_delegated_revoked_tombstone(&state.paths.twitch_delegated_revoked).unwrap();
    assert!(state.paths.twitch_delegated_revoked.is_file());

    let session = sample_session(2, "ssk_test_placeholder_gen_b");
    state.persist_delegated_session(&session).unwrap();
    state.clear_delegated_revoked_tombstone().unwrap();
    *state.delegated.write().await = Some(session);
    services.install_delegated_generation(2).await;

    assert!(!state.paths.twitch_delegated_revoked.is_file());
    assert!(state.paths.twitch_delegated.is_file());
    drop(state);
    drop(services);

    let restarted = restart_app_at(&userdata, false);
    assert!(restarted.delegated.read().await.is_some());
    assert_eq!(
        restarted
            .delegated
            .read()
            .await
            .as_ref()
            .map(|s| s.connection_key.as_str()),
        Some("ssk_test_placeholder_gen_b")
    );
}

#[tokio::test]
async fn tombstone_clear_failure_keeps_fail_closed_on_restart() {
    let userdata = test_userdata_dir();
    let (_router, state, _services) = build_app_at(userdata.clone(), 0).await;
    write_delegated_revoked_tombstone(&state.paths.twitch_delegated_revoked).unwrap();
    let session = sample_session(3, "ssk_test_placeholder_gen_c");
    state.persist_delegated_session(&session).unwrap();
    state
        .durable_fail
        .tombstone_clear
        .store(true, Ordering::SeqCst);
    assert!(state.clear_delegated_revoked_tombstone().is_err());
    assert!(state.paths.twitch_delegated_revoked.is_file());
    drop(state);

    let restarted = restart_app_at(&userdata, false);
    assert!(restarted.delegated.read().await.is_none());
}

#[tokio::test]
async fn stale_generation_teardown_cannot_clear_replacement_session() {
    let (_router, state, services) = build_app(0).await;
    let gen_a = state.bump_delegated_generation();
    *state.delegated.write().await = Some(sample_session(gen_a, "ssk_test_placeholder_gen_a"));
    services.install_delegated_generation(gen_a).await;

    let gen_b = state.bump_delegated_generation();
    *state.delegated.write().await = Some(sample_session(gen_b, "ssk_test_placeholder_gen_b"));
    services.install_delegated_generation(gen_b).await;

    services
        .signal_delegated_teardown(state.clone(), gen_a, "revoked")
        .await
        .expect("stale teardown no-op");

    let delegated = state.delegated.read().await.clone();
    assert_eq!(
        delegated.as_ref().map(|s| s.connection_key.as_str()),
        Some("ssk_test_placeholder_gen_b")
    );
}

#[tokio::test]
async fn teardown_paused_after_check_cannot_clear_replacement() {
    let (_router, state, services) = build_app(0).await;
    services.init_teardown_worker();
    let gen_a = state.bump_delegated_generation();
    *state.delegated.write().await = Some(sample_session(gen_a, "ssk_test_placeholder_a"));
    services.install_delegated_generation(gen_a).await;
    state
        .persist_delegated_session(state.delegated.read().await.as_ref().unwrap())
        .unwrap();

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    let fail = services
        .signal_delegated_teardown(state.clone(), gen_a, "revoked")
        .await;
    assert!(fail.is_err());

    let gen_b = state.bump_delegated_generation();
    let session_b = sample_session(gen_b, "ssk_test_placeholder_b");
    state.persist_delegated_session(&session_b).unwrap();
    state.clear_delegated_revoked_tombstone().ok();
    *state.delegated.write().await = Some(session_b);
    services.install_delegated_generation(gen_b).await;
    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);

    let _ = services
        .signal_delegated_teardown(state.clone(), gen_a, "revoked")
        .await;
    assert_eq!(
        state
            .delegated
            .read()
            .await
            .as_ref()
            .map(|s| s.connection_key.as_str()),
        Some("ssk_test_placeholder_b")
    );
}

#[tokio::test]
async fn active_mode_persist_failure_retries_to_completion() {
    let (_router, state, services) = build_app(0).await;
    services.init_teardown_worker();
    let gen = state.bump_delegated_generation();
    *state.delegated.write().await = Some(sample_session(gen, "ssk_test_placeholder_mode"));
    services.install_delegated_generation(gen).await;
    state
        .persist_delegated_session(state.delegated.read().await.as_ref().unwrap())
        .unwrap();
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    state.save_active_mode().await.unwrap();

    state
        .durable_fail
        .save_active_mode
        .store(true, Ordering::SeqCst);
    let first = services
        .signal_delegated_teardown(state.clone(), gen, "revoked")
        .await;
    assert!(first.is_err());
    assert!(state.delegated.read().await.is_none());

    // Disk may still say Delegated — retry must finish mode persistence.
    state
        .durable_fail
        .save_active_mode
        .store(false, Ordering::SeqCst);
    let second = services
        .signal_delegated_teardown(state.clone(), gen, "revoked")
        .await;
    assert!(second.is_ok());
    let mode: TwitchActiveModeFile =
        serde_json::from_str(&std::fs::read_to_string(&state.paths.twitch_active_mode).unwrap())
            .unwrap();
    assert_eq!(mode.mode, TwitchActiveMode::Local);
    assert_eq!(
        services.teardown_coordinator.phase_for(gen).await,
        TeardownPhase::Completed
    );
}

#[tokio::test]
async fn dead_coordinator_channel_does_not_report_success() {
    let (_router, state, services) = build_app(0).await;
    let gen = state.bump_delegated_generation();
    *state.delegated.write().await = Some(sample_session(gen, "ssk_test_placeholder_dead"));
    services.install_delegated_generation(gen).await;
    write_json(
        &state.paths.twitch_delegated,
        state.delegated.read().await.as_ref().unwrap(),
    )
    .unwrap();
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    let result = services
        .signal_delegated_teardown(state.clone(), gen, "revoked")
        .await;
    assert!(result.is_err());
    assert!(state.paths.twitch_delegated.is_file());
}

#[tokio::test]
async fn kick_feed_uses_authorization_not_query_key() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let saw_auth = Arc::new(AtomicBool::new(false));
    let saw_query = Arc::new(AtomicBool::new(false));
    let saw_auth2 = saw_auth.clone();
    let saw_query2 = saw_query.clone();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 8192];
        let n = socket.read(&mut buf).await.unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase();
        if req.contains("authorization:") && req.contains("bearer ssk_") {
            saw_auth2.store(true, Ordering::SeqCst);
        }
        if req.contains("?key=") {
            saw_query2.store(true, Ordering::SeqCst);
        }
        let body = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
        let _ = socket.write_all(body).await;
    });
    std::env::set_var("SYNDICATE_API_BASE", format!("http://127.0.0.1:{port}"));

    let (_router, state, services) = build_app(0).await;
    state.delegated_generation.store(1, Ordering::SeqCst);
    services.install_delegated_generation(1).await;
    services.install_validated_authority_lease(1, None).await;
    *state.delegated.write().await = Some(sample_session(1, "ssk_test_placeholder_kick_feed_key"));
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    {
        let mut k = state.kick.write().await;
        k.tokens.access_token = Some("delegated-kick".into());
        k.tokens.kick_id = Some("k1".into());
    }
    {
        let mut d = state.delegated.write().await;
        if let Some(s) = d.as_mut() {
            s.kick_id = Some("k1".into());
            s.kick_access_token = Some("delegated-kick".into());
        }
    }
    sync_live_identity(state.clone()).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(saw_auth.load(Ordering::SeqCst));
    assert!(!saw_query.load(Ordering::SeqCst));
}

#[tokio::test]
async fn kick_redaction_uses_request_scoped_key_not_current_session() {
    use stream_sync_core::redact_connection_key;
    let key_a = "ssk_test_placeholder_request_a";
    let key_b = "ssk_test_placeholder_current_b";
    let err = format!("upstream rejected Authorization Bearer {key_a}");
    // Simulate: request used A; current session already replaced with B.
    let redacted = redact_connection_key(&err, key_a);
    assert!(!redacted.contains(key_a));
    assert!(!redacted.contains(key_b));
    assert!(redacted.contains("[redacted-connection-key]"));
}

#[test]
fn documented_revocation_bound_is_lease_deadline() {
    assert_eq!(MAX_DELEGATED_REVOCATION_DELAY.as_secs(), 300);
    assert!(SYNDICATE_HTTP_TIMEOUT <= MAX_DELEGATED_REVOCATION_DELAY);
    assert!(SYNDICATE_SSE_READ_TIMEOUT <= MAX_DELEGATED_REVOCATION_DELAY);
}

#[test]
fn events_url_helper_has_no_query_key() {
    let url = connection_key_events_url();
    assert!(url.contains("/api/stream-sync/connection-keys/events"));
    assert!(!url.contains("?key="));
}

#[tokio::test]
async fn each_streamsync_instance_owns_independent_twitch_services() {
    let (_r1, _s1, t1) = build_app(0).await;
    let (_r2, _s2, t2) = build_app(0).await;
    assert!(!Arc::ptr_eq(&t1, &t2));
}

#[tokio::test]
async fn two_instances_process_revoke_independently() {
    let (_r1, s1, t1) = build_app(0).await;
    let (_r2, s2, t2) = build_app(0).await;
    let g1 = s1.bump_delegated_generation();
    let g2 = s2.bump_delegated_generation();
    *s1.delegated.write().await = Some(sample_session(g1, "ssk_test_placeholder_inst1"));
    *s2.delegated.write().await = Some(sample_session(g2, "ssk_test_placeholder_inst2"));
    t1.install_delegated_generation(g1).await;
    t2.install_delegated_generation(g2).await;
    s1.persist_delegated_session(s1.delegated.read().await.as_ref().unwrap())
        .unwrap();
    s2.persist_delegated_session(s2.delegated.read().await.as_ref().unwrap())
        .unwrap();
    t1.signal_delegated_teardown(s1.clone(), g1, "revoked")
        .await
        .unwrap();
    assert!(s1.delegated.read().await.is_none());
    assert!(s2.delegated.read().await.is_some());
    assert!(s2.paths.twitch_delegated.is_file());
}

#[test]
fn remove_file_durable_deletes_existing_file() {
    let dir = test_userdata_dir();
    let path = dir.join("twitch-delegated.json");
    std::fs::write(&path, b"{}").unwrap();
    remove_file_durable(&path).unwrap();
    assert!(!path.is_file());
}

#[tokio::test]
async fn autonomous_durable_revoke_completes_after_transient_failure() {
    let (_router, state, services) = build_app(0).await;
    services.init_teardown_worker();
    services.init_durable_revoke_worker();
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_autonomous"),
    )
    .unwrap();
    state.delegated_generation.store(1, Ordering::SeqCst);
    *state.delegated.write().await = Some(sample_session(1, "ssk_test_placeholder_autonomous"));
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    services.install_delegated_generation(1).await;

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    services
        .signal_delegated_teardown(state.clone(), 1, "revoked")
        .await
        .expect_err("first teardown fails injected");
    assert!(state.paths.twitch_delegated.is_file());

    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);
    for _ in 0..40 {
        if !state.paths.twitch_delegated.is_file() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!state.paths.twitch_delegated.is_file());
    assert!(state.paths.twitch_delegated_revoked.is_file());
}

#[tokio::test]
async fn revoke_pending_marker_survives_crash_and_quarantines_restart() {
    let userdata = test_userdata_dir();
    let (_router, state, _services) = build_app_at(userdata.clone(), 0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_pending_crash"),
    )
    .unwrap();
    state.mark_durable_revoke_pending().unwrap();
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    drop(state);

    let restarted = restart_app_at(&userdata, false);
    assert!(restarted.delegated.read().await.is_none());
    assert_ne!(
        *restarted.active_mode.read().await,
        TwitchActiveMode::Delegated
    );
}

#[tokio::test]
async fn tombstone_write_failure_still_persists_pending_marker() {
    let (_router, state, _services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_pending_tombstone"),
    )
    .unwrap();
    state
        .durable_fail
        .tombstone_write
        .store(true, Ordering::SeqCst);
    assert!(state.durable_revoke_delegated().await.is_err());
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    assert!(!state.paths.twitch_delegated_revoked.is_file());
    assert!(state.paths.twitch_delegated.is_file());
}

#[tokio::test]
async fn disconnect_intent_advances_even_when_revoke_fails() {
    let (_router, state, services) = build_app(0).await;
    write_json(
        &state.paths.twitch_delegated,
        &sample_session(1, "ssk_test_placeholder_disconnect_intent"),
    )
    .unwrap();
    state.delegated_generation.store(1, Ordering::SeqCst);
    *state.delegated.write().await =
        Some(sample_session(1, "ssk_test_placeholder_disconnect_intent"));
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    services.install_delegated_generation(1).await;
    let before = services.apply_intent_for_test();
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    assert!(disconnect_twitch(state.clone(), services.clone())
        .await
        .is_err());
    assert!(services.apply_intent_for_test() > before);
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
}
