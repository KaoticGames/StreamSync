//! Bounded two-slot journal: concurrency, rollback, readonly, revoke, and Kick coherence.

#![allow(clippy::too_many_arguments, clippy::await_holding_lock)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stream_sync_core::{
    all_delegated_bundle_slot_keys, delegated_bundle_slot_key, fs_secret_store,
    legacy_revision_bundle_key, paths_for_root, read_bound_delegated_bundle,
    read_bound_delegated_bundle_with_provenance, validate_delegated_session_coherence, write_json,
    AppState, BoundBundleProvenance, DelegatedSecretBundle, DelegatedSessionFile, OverlayConfig,
    OverlayServer, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_CONNECTION_KEY,
    TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
};

static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

/// Authority gates are process-global; serialize gate tests to avoid cross-test races.
static AUTHORITY_GATE_TEST_LOCK: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

fn lock_authority_gate_tests() -> std::sync::MutexGuard<'static, ()> {
    AUTHORITY_GATE_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn test_userdata_dir() -> std::path::PathBuf {
    let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "streamsync-delegated-tx-{}-{n}",
        std::process::id()
    ));
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

fn restart_app_at(userdata: &std::path::Path, readonly: bool) -> Arc<AppState> {
    let paths = paths_for_root(userdata, readonly).expect("paths_for_root");
    AppState::new(paths, repo_root(), 0, readonly, fs_secret_store(userdata))
        .expect("AppState::new")
}

fn sample_metadata_only_json(
    generation: u64,
    secret_revision: u64,
    bundle_slot: u8,
) -> serde_json::Value {
    serde_json::json!({
        "generation": generation,
        "secret_revision": secret_revision,
        "bundle_slot": bundle_slot,
        "client_id": "cid",
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": "2099-01-01T00:00:00Z"
    })
}

fn sample_inline_legacy_json(connection_key: &str, access_token: &str) -> serde_json::Value {
    serde_json::json!({
        "generation": 1,
        "connection_key": connection_key,
        "client_id": "cid",
        "access_token": access_token,
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": "2099-01-01T00:00:00Z"
    })
}

fn sample_session(access_token: &str) -> DelegatedSessionFile {
    DelegatedSessionFile {
        generation: 1,
        connection_key: "ssk_test_placeholder_conn".into(),
        client_id: "cid".into(),
        access_token: access_token.into(),
        channel_login: "takeover_chan".into(),
        channel_twitch_id: "999".into(),
        twitch_expires_at: "2099-01-01T00:00:00Z".into(),
        ..Default::default()
    }
}

fn write_slot_bundle(
    store: &dyn stream_sync_core::SecretStore,
    slot: u8,
    transaction_id: u64,
    generation: u64,
    connection_key: &str,
    access_token: &str,
    kick_access: Option<&str>,
    kick_refresh: Option<&str>,
) {
    let bundle = DelegatedSecretBundle {
        transaction_id,
        generation,
        slot,
        connection_key: connection_key.into(),
        access_token: access_token.into(),
        kick_access_token: kick_access.map(|s| s.to_string()),
        kick_refresh_token: kick_refresh.map(|s| s.to_string()),
    };
    store
        .set(
            &delegated_bundle_slot_key(slot),
            serde_json::to_vec(&bundle).unwrap().as_slice(),
        )
        .unwrap();
}

fn secret_store_snapshot(store: &dyn stream_sync_core::SecretStore) -> Vec<bool> {
    let mut keys = all_delegated_bundle_slot_keys().to_vec();
    keys.push(TWITCH_DELEGATED_CONNECTION_KEY.to_string());
    keys.push(TWITCH_DELEGATED_ACCESS_TOKEN_KEY.to_string());
    keys.push(TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY.to_string());
    keys.push(TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY.to_string());
    keys.iter()
        .map(|k| store.get(k).unwrap().is_some())
        .collect()
}

fn metadata_bytes(path: &std::path::Path) -> Vec<u8> {
    std::fs::read(path).unwrap()
}

async fn assert_metadata_bundle_coherent(userdata: &std::path::Path, state: &AppState) {
    let session = state
        .delegated
        .read()
        .await
        .clone()
        .expect("delegated session");
    let store = fs_secret_store(userdata);
    let bundle = read_bound_delegated_bundle(store.as_ref(), &session)
        .expect("read bound bundle")
        .expect("bundle must exist");
    assert_eq!(bundle.access_token, session.access_token);
    assert_eq!(bundle.connection_key, session.connection_key);
    assert_eq!(bundle.transaction_id, session.secret_revision);
    assert_eq!(bundle.slot, session.bundle_slot);
}

#[tokio::test]
async fn concurrent_persist_loads_metadata_matched_secret_pair() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_initial"))
        .unwrap();

    let state_a = state.clone();
    let state_b = state.clone();
    let s_a = sample_session("ssk_test_placeholder_concurrent_a");
    let s_b = sample_session("ssk_test_placeholder_concurrent_b");
    let t_a = std::thread::spawn(move || state_a.persist_delegated_session(&s_a));
    let t_b = std::thread::spawn(move || state_b.persist_delegated_session(&s_b));
    let _ = t_a.join().unwrap();
    let _ = t_b.join().unwrap();

    let restarted = restart_app_at(&userdata, false);
    assert_metadata_bundle_coherent(&userdata, &restarted).await;
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn superseded_apply_rollback_restores_metadata_and_bound_bundle() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, services) = OverlayServer::new(config).build_app().await.unwrap();

    let gen1 = sample_session("ssk_test_placeholder_gen1_at");
    state.persist_delegated_session(&gen1).unwrap();
    let disk_gen1: DelegatedSessionFile = {
        let raw = std::fs::read_to_string(&state.paths.twitch_delegated).unwrap();
        let meta: serde_json::Value = serde_json::from_str(&raw).unwrap();
        DelegatedSessionFile {
            generation: meta["generation"].as_u64().unwrap_or(1),
            secret_revision: meta["secret_revision"].as_u64().unwrap_or(1),
            bundle_slot: meta["bundle_slot"].as_u64().unwrap_or(0) as u8,
            client_id: meta["client_id"].as_str().unwrap_or("").into(),
            channel_login: meta["channel_login"].as_str().unwrap_or("").into(),
            channel_twitch_id: meta["channel_twitch_id"].as_str().unwrap_or("").into(),
            twitch_expires_at: meta["twitch_expires_at"].as_str().unwrap_or("").into(),
            connection_key: gen1.connection_key.clone(),
            access_token: gen1.access_token.clone(),
            ..Default::default()
        }
    };
    let gen1_meta = metadata_bytes(&state.paths.twitch_delegated);
    let store = fs_secret_store(&userdata);
    let gen1_slots = [
        store.get(&delegated_bundle_slot_key(0)).unwrap(),
        store.get(&delegated_bundle_slot_key(1)).unwrap(),
    ];

    let mut snapshot =
        stream_sync_core::test_support::DurableApplySnapshot::capture(&state).expect("capture");
    let live = stream_sync_core::test_support::LiveApplySnapshot::capture(&state, &services).await;

    let mut stale = gen1.clone();
    stale.access_token = "ssk_test_placeholder_stale_at".into();
    stale.generation = 2;
    let stale_identity = state.persist_delegated_session(&stale).unwrap();
    snapshot.note_persist_completed(stale_identity);

    let err = anyhow::anyhow!("superseded by newer identity action");
    stream_sync_core::test_support::rollback_superseded_apply(
        &snapshot, live, &state, &services, err,
    )
    .await
    .expect_err("rollback returns original error");

    let rolled_meta = std::fs::read(&state.paths.twitch_delegated).unwrap();
    assert_eq!(rolled_meta, gen1_meta, "metadata must roll back to gen1");
    for slot in 0..2 {
        assert_eq!(
            store.get(&delegated_bundle_slot_key(slot)).unwrap(),
            gen1_slots[slot as usize],
            "bundle slot {slot} must roll back"
        );
    }
    let abandoned = read_bound_delegated_bundle(store.as_ref(), &disk_gen1)
        .unwrap()
        .expect("gen1 bundle");
    assert_eq!(abandoned.access_token, "ssk_test_placeholder_gen1_at");
    assert!(store
        .get(&delegated_bundle_slot_key(abandoned.slot))
        .unwrap()
        .is_some());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn readonly_startup_both_inline_legacy_no_store_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(
        &paths.twitch_delegated,
        &sample_inline_legacy_json("ssk_test_placeholder_conn", "ssk_test_placeholder_at"),
    )
    .unwrap();
    let store = fs_secret_store(&userdata);
    let before_meta = paths.twitch_delegated.exists();
    let before_store = secret_store_snapshot(store.as_ref());

    let state = restart_app_at(&userdata, true);
    let session = state
        .delegated
        .read()
        .await
        .clone()
        .expect("hydrate inline");
    assert_eq!(session.connection_key, "ssk_test_placeholder_conn");
    assert_eq!(session.access_token, "ssk_test_placeholder_at");

    assert_eq!(before_meta, paths.twitch_delegated.exists());
    let raw = std::fs::read_to_string(&paths.twitch_delegated).unwrap();
    assert!(raw.contains("ssk_test_placeholder_conn"));
    assert_eq!(secret_store_snapshot(store.as_ref()), before_store);
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn readonly_startup_unversioned_legacy_store_pair_no_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 0, 0)).unwrap();
    let store = fs_secret_store(&userdata);
    store
        .set(
            TWITCH_DELEGATED_CONNECTION_KEY,
            b"ssk_test_placeholder_legacy_conn".as_slice(),
        )
        .unwrap();
    store
        .set(
            TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
            b"ssk_test_placeholder_legacy_at".as_slice(),
        )
        .unwrap();
    let before_meta = std::fs::read(&paths.twitch_delegated).unwrap();
    let before_store = secret_store_snapshot(store.as_ref());

    let state = restart_app_at(&userdata, true);
    let session = state.delegated.read().await.clone().expect("hydrate pair");
    assert_eq!(session.connection_key, "ssk_test_placeholder_legacy_conn");
    assert_eq!(session.access_token, "ssk_test_placeholder_legacy_at");
    assert_eq!(std::fs::read(&paths.twitch_delegated).unwrap(), before_meta);
    assert_eq!(secret_store_snapshot(store.as_ref()), before_store);
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn orphan_bundle_revoke_clears_all_slots_before_pending_clears() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_revoke"))
        .unwrap();
    let store = fs_secret_store(&userdata);
    write_slot_bundle(
        store.as_ref(),
        1,
        99,
        1,
        "ssk_test_placeholder_orphan_conn",
        "ssk_test_placeholder_orphan_at",
        None,
        None,
    );

    state.durable_revoke_delegated().await.unwrap();
    assert!(!state.delegated_secret_store_authority_remain().unwrap());
    assert!(!state.delegated_authority_artifacts_remain().unwrap());
    for key in all_delegated_bundle_slot_keys() {
        assert!(
            store.get(&key).unwrap().is_none(),
            "slot {key} must be gone"
        );
    }
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn corrupt_metadata_revoke_fail_closed_clears_enumerable_slots() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    std::fs::write(&state.paths.twitch_delegated, b"{not valid json").unwrap();
    let store = fs_secret_store(&userdata);
    write_slot_bundle(
        store.as_ref(),
        0,
        1,
        1,
        "ssk_test_placeholder_corrupt_conn",
        "ssk_test_placeholder_corrupt_at",
        None,
        None,
    );

    let err = state.durable_revoke_delegated().await.unwrap_err();
    assert!(
        err.to_string().contains("unparseable"),
        "must fail closed: {err:#}"
    );
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    assert!(!state.delegated_secret_store_authority_remain().unwrap());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn u64_max_revision_fails_before_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(
        &paths.twitch_delegated,
        &sample_metadata_only_json(1, u64::MAX, 0),
    )
    .unwrap();
    write_slot_bundle(
        fs_secret_store(&userdata).as_ref(),
        0,
        u64::MAX,
        1,
        "ssk_test_placeholder_max_conn",
        "ssk_test_placeholder_max_at",
        None,
        None,
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let before_meta = std::fs::read(&paths.twitch_delegated).unwrap();
    let store = fs_secret_store(&userdata);
    let before_slots = secret_store_snapshot(store.as_ref());

    let mut refreshed = sample_session("ssk_test_placeholder_overflow");
    refreshed.secret_revision = u64::MAX;
    refreshed.bundle_slot = 0;
    let err = state.persist_delegated_session(&refreshed).unwrap_err();
    assert!(
        err.to_string().contains("exhausted"),
        "must reject overflow: {err:#}"
    );
    assert_eq!(std::fs::read(&paths.twitch_delegated).unwrap(), before_meta);
    assert_eq!(secret_store_snapshot(store.as_ref()), before_slots);
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn existing_slot_collision_with_different_bundle_rejects_without_overwrite() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1, 0)).unwrap();
    let store = fs_secret_store(&userdata);
    write_slot_bundle(
        store.as_ref(),
        1,
        1,
        1,
        "ssk_test_placeholder_bound_conn",
        "ssk_test_placeholder_bound_at",
        None,
        None,
    );
    write_slot_bundle(
        store.as_ref(),
        1,
        42,
        9,
        "ssk_test_placeholder_collision_conn",
        "ssk_test_placeholder_collision_at",
        None,
        None,
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let mut next = sample_session("ssk_test_placeholder_next_at");
    next.bundle_slot = 0;
    let err = state.persist_delegated_session(&next).unwrap_err();
    assert!(
        err.to_string().contains("occupied"),
        "must reject collision: {err:#}"
    );
    let bundle: DelegatedSecretBundle = serde_json::from_slice(
        &store
            .get(&delegated_bundle_slot_key(1))
            .unwrap()
            .expect("collision bundle remains"),
    )
    .unwrap();
    assert_eq!(bundle.transaction_id, 42);
    let _ = std::fs::remove_dir_all(&userdata);
}

#[test]
fn kick_refresh_without_access_rejects() {
    let session = DelegatedSessionFile {
        generation: 1,
        kick_id: Some("kid".into()),
        kick_refresh_token: Some("ssk_test_placeholder_kick_rt".into()),
        ..Default::default()
    };
    let err = validate_delegated_session_coherence(&session).unwrap_err();
    assert!(err.to_string().contains("refresh"));
}

#[test]
fn metadata_kick_id_without_bundle_access_rejects() {
    let session = DelegatedSessionFile {
        generation: 1,
        kick_id: Some("kid".into()),
        kick_login: Some("kick_chan".into()),
        ..Default::default()
    };
    let err = validate_delegated_session_coherence(&session).unwrap_err();
    assert!(err.to_string().contains("Kick identity"));
}

#[tokio::test]
async fn valid_twitch_only_delegated_session_remains_valid() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1, 0)).unwrap();
    write_slot_bundle(
        fs_secret_store(&userdata).as_ref(),
        0,
        1,
        1,
        "ssk_test_placeholder_conn",
        "ssk_test_placeholder_at",
        None,
        None,
    );

    let state = restart_app_at(&userdata, false);
    let session = state.delegated.read().await.clone().expect("twitch-only");
    assert_eq!(session.access_token, "ssk_test_placeholder_at");
    assert!(session.kick_access_token.is_none());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn valid_coherent_twitch_kick_delegated_session_remains_valid() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    let meta = serde_json::json!({
        "generation": 1,
        "secret_revision": 1,
        "bundle_slot": 0,
        "client_id": "cid",
        "channel_login": "takeover_chan",
        "channel_twitch_id": "999",
        "twitch_expires_at": "2099-01-01T00:00:00Z",
        "kick_id": "kid",
        "kick_login": "kick_chan"
    });
    write_json(&paths.twitch_delegated, &meta).unwrap();
    write_slot_bundle(
        fs_secret_store(&userdata).as_ref(),
        0,
        1,
        1,
        "ssk_test_placeholder_conn",
        "ssk_test_placeholder_at",
        Some("ssk_test_placeholder_kick_at"),
        Some("ssk_test_placeholder_kick_rt"),
    );

    let state = restart_app_at(&userdata, false);
    let session = state.delegated.read().await.clone().expect("twitch+kick");
    assert_eq!(
        session.kick_access_token.as_deref(),
        Some("ssk_test_placeholder_kick_at")
    );
    assert_eq!(session.kick_id.as_deref(), Some("kid"));
    let _ = std::fs::remove_dir_all(&userdata);
}

fn write_legacy_revision_bundle(
    store: &dyn stream_sync_core::SecretStore,
    revision: u64,
    _generation: u64,
    connection_key: &str,
    access_token: &str,
) {
    let bundle = serde_json::json!({
        "revision": revision,
        "connection_key": connection_key,
        "access_token": access_token,
    });
    store
        .set(
            &legacy_revision_bundle_key(revision),
            serde_json::to_vec(&bundle).unwrap().as_slice(),
        )
        .unwrap();
}

#[tokio::test]
async fn concurrent_rollback_refuses_when_newer_persist_wins() {
    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_gen1_rb"))
        .unwrap();
    let gen1_meta = metadata_bytes(&state.paths.twitch_delegated);

    let snapshot =
        stream_sync_core::test_support::DurableApplySnapshot::capture(&state).expect("capture");

    let (_gate, arrived_rx, resume_tx) =
        stream_sync_core::test_support::install_delegated_authority_gate(
            stream_sync_core::test_support::DelegatedAuthorityBoundary::RollbackBeforeRestore,
        );
    let state_rb = state.clone();
    let snapshot_rb = snapshot.clone();
    let rollback_task = std::thread::spawn(move || snapshot_rb.rollback(&state_rb));

    arrived_rx.recv().expect("rollback gate arrived");
    let mut winner = sample_session("ssk_test_placeholder_winner_rb");
    winner.generation = 2;
    state
        .persist_delegated_session(&winner)
        .expect("winner persist");
    resume_tx.send(()).expect("release rollback");

    let err = rollback_task.join().expect("rollback join").unwrap_err();
    assert!(
        err.to_string().contains("stale rollback refused"),
        "rollback must refuse when newer persist won: {err:#}"
    );
    assert_ne!(
        std::fs::read(&state.paths.twitch_delegated).unwrap(),
        gen1_meta,
        "winner metadata must remain"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn concurrent_rollback_refuses_when_revoke_wins() {
    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_gen1_revoke_rb"))
        .unwrap();

    let mut snapshot =
        stream_sync_core::test_support::DurableApplySnapshot::capture(&state).expect("capture");
    let mut stale = sample_session("ssk_test_placeholder_stale_revoke_rb");
    stale.generation = 2;
    let stale_identity = state.persist_delegated_session(&stale).unwrap();
    snapshot.note_persist_completed(stale_identity);

    let (_gate, arrived_rx, resume_tx) =
        stream_sync_core::test_support::install_delegated_authority_gate(
            stream_sync_core::test_support::DelegatedAuthorityBoundary::RollbackBeforeRestore,
        );
    let state_rb = state.clone();
    let snapshot_rb = snapshot.clone();
    let rollback_task = std::thread::spawn(move || snapshot_rb.rollback(&state_rb));
    arrived_rx.recv().expect("rollback gate arrived");
    state
        .durable_revoke_delegated()
        .await
        .expect("revoke winner");
    resume_tx.send(()).expect("release rollback");

    let err = rollback_task.join().expect("rollback join").unwrap_err();
    assert!(
        err.to_string().contains("stale rollback refused"),
        "rollback must refuse when revoke won: {err:#}"
    );
    assert!(state.paths.twitch_delegated_revoked.is_file());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn replacement_persist_clears_revoke_markers_atomically() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_pre_revoke"))
        .unwrap();
    state.durable_revoke_delegated().await.unwrap();
    assert!(state.paths.twitch_delegated_revoked.is_file());

    state
        .persist_delegated_replacement_session(&sample_session("ssk_test_placeholder_replacement"))
        .expect("replacement persist");
    assert!(
        !state.paths.twitch_delegated_revoked.is_file(),
        "replacement must clear tombstone in the same authority transaction"
    );
    assert!(
        !state.paths.twitch_delegated_revoke_pending.is_file(),
        "replacement must clear pending in the same authority transaction"
    );
    let restarted = restart_app_at(&userdata, false);
    assert_metadata_bundle_coherent(&userdata, &restarted).await;
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn concurrent_apply_vs_revoke_marker_race_keeps_revoke_signal() {
    use std::sync::mpsc;

    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_pre_revoke_race"))
        .unwrap();
    state.durable_revoke_delegated().await.unwrap();
    assert!(state.paths.twitch_delegated_revoked.is_file());

    let (_gate, arrived_rx, resume_tx) =
        stream_sync_core::test_support::install_delegated_authority_gate(
            stream_sync_core::test_support::DelegatedAuthorityBoundary::PersistBeforeMarkerClear,
        );
    let state_apply = state.clone();
    let replacement = sample_session("ssk_test_placeholder_replacement_race");
    let apply_task =
        std::thread::spawn(move || state_apply.persist_delegated_replacement_session(&replacement));
    arrived_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("apply must reach marker-clear gate");

    let (revoke_started_tx, revoke_started_rx) = mpsc::channel();
    let (revoke_done_tx, revoke_done_rx) = mpsc::channel();
    let state_revoke = state.clone();
    let revoke_thread = std::thread::spawn(move || {
        let _ = revoke_started_tx.send(());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("revoke runtime");
        rt.block_on(async {
            let _ = state_revoke.durable_revoke_delegated().await;
        });
        let _ = revoke_done_tx.send(());
    });
    revoke_started_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("revoke thread must reach durable revoke entry");
    assert!(
        revoke_done_rx.try_recv().is_err(),
        "revoke must block on authority lock while apply holds marker-clear transaction"
    );

    resume_tx.send(()).expect("release apply");
    apply_task
        .join()
        .expect("apply join")
        .expect("replacement apply");

    revoke_done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("concurrent revoke must complete after replacement releases authority lock");
    let _ = revoke_thread.join();

    assert!(
        state.delegated.read().await.is_none(),
        "terminal state must be revoked with no in-memory delegated session"
    );
    assert!(
        !state.paths.twitch_delegated.is_file(),
        "successful durable revoke must remove delegated metadata"
    );
    assert!(
        state.paths.twitch_delegated_revoked.is_file(),
        "revoke signal must survive replacement marker-clear race"
    );
    assert!(!state.paths.twitch_delegated_revoke_pending.is_file());
    assert!(!state.delegated_secret_store_authority_remain().unwrap());
    assert!(!state.delegated_authority_artifacts_remain().unwrap());

    let restarted = restart_app_at(&userdata, false);
    assert!(restarted.delegated.read().await.is_none());
    assert!(!restarted.paths.twitch_delegated.is_file());
    assert!(restarted.paths.twitch_delegated_revoked.is_file());
    assert!(!restarted.delegated_secret_store_authority_remain().unwrap());

    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn legacy_revision_bundle_migrates_on_writable_startup() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1, 0)).unwrap();
    let store = fs_secret_store(&userdata);
    write_legacy_revision_bundle(
        store.as_ref(),
        1,
        1,
        "ssk_test_placeholder_legacy_conn",
        "ssk_test_placeholder_legacy_at",
    );

    let before = store
        .get(&legacy_revision_bundle_key(1))
        .unwrap()
        .expect("legacy key before migrate");
    assert!(!before.is_empty());

    let state = restart_app_at(&userdata, false);
    let session = state.delegated.read().await.clone().expect("hydrated");
    assert_eq!(session.access_token, "ssk_test_placeholder_legacy_at");
    let (_, provenance) = read_bound_delegated_bundle_with_provenance(store.as_ref(), &session)
        .unwrap()
        .expect("bound bundle");
    assert_eq!(provenance, BoundBundleProvenance::Slot(session.bundle_slot));
    assert!(
        store.get(&legacy_revision_bundle_key(1)).unwrap().is_none(),
        "legacy revision key must be deleted after migration"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn legacy_revision_bundle_revoke_deletes_proven_key() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1, 0)).unwrap();
    let store = fs_secret_store(&userdata);
    write_legacy_revision_bundle(
        store.as_ref(),
        1,
        1,
        "ssk_test_placeholder_revoke_legacy_conn",
        "ssk_test_placeholder_revoke_legacy_at",
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state.durable_revoke_delegated().await.unwrap();
    assert!(
        store.get(&legacy_revision_bundle_key(1)).unwrap().is_none(),
        "revoke must delete proven legacy revision bundle"
    );
    assert!(!state.delegated_secret_store_authority_remain().unwrap());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn corrupt_metadata_revoke_retains_pending_with_unknown_legacy_provenance() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_before_corrupt"))
        .unwrap();
    std::fs::write(&state.paths.twitch_delegated, b"{not valid json").unwrap();
    let store = fs_secret_store(&userdata);
    write_legacy_revision_bundle(
        store.as_ref(),
        1,
        1,
        "ssk_test_placeholder_fail_legacy_conn",
        "ssk_test_placeholder_fail_legacy_at",
    );
    let err = state.durable_revoke_delegated().await.unwrap_err();
    assert!(
        err.to_string().contains("unparseable"),
        "must fail closed on corrupt metadata: {err:#}"
    );
    assert!(
        store.get(&legacy_revision_bundle_key(1)).unwrap().is_some(),
        "legacy bundle must remain when metadata provenance is unknown"
    );
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[test]
fn same_transaction_id_non_identical_secrets_rejects_without_mutation() {
    let store = stream_sync_core::memory_secret_store();
    let bundle_a = DelegatedSecretBundle {
        transaction_id: 1,
        generation: 1,
        slot: 0,
        connection_key: "ssk_test_placeholder_conn_a".into(),
        access_token: "ssk_test_placeholder_at_a".into(),
        kick_access_token: None,
        kick_refresh_token: None,
    };
    stream_sync_core::write_delegated_bundle_create(store.as_ref(), &bundle_a).unwrap();
    let bundle_b = DelegatedSecretBundle {
        transaction_id: 1,
        generation: 1,
        slot: 0,
        connection_key: "ssk_test_placeholder_conn_b".into(),
        access_token: "ssk_test_placeholder_at_b".into(),
        kick_access_token: None,
        kick_refresh_token: None,
    };
    let err =
        stream_sync_core::write_delegated_bundle_create(store.as_ref(), &bundle_b).unwrap_err();
    assert!(
        err.to_string().contains("collision"),
        "must reject same-id non-identical write: {err:#}"
    );
    let kept: DelegatedSecretBundle = serde_json::from_slice(
        &store
            .get(&delegated_bundle_slot_key(0))
            .unwrap()
            .expect("slot unchanged"),
    )
    .unwrap();
    assert_eq!(kept.access_token, "ssk_test_placeholder_at_a");
}

#[tokio::test]
async fn rollback_refuses_after_failed_revoke_preserves_marker_epoch() {
    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let identity = state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_failed_revoke_rb"))
        .unwrap();
    let mut snapshot =
        stream_sync_core::test_support::DurableApplySnapshot::capture(&state).expect("capture");
    snapshot.note_persist_completed(identity);

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    let revoke_err = state.durable_revoke_delegated().await.unwrap_err();
    assert!(
        revoke_err.to_string().contains("credential_remove"),
        "revoke must fail after markers: {revoke_err:#}"
    );
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    assert!(state.paths.twitch_delegated_revoked.is_file());
    assert!(state.paths.twitch_delegated.is_file());

    let rb_err = snapshot.rollback(&state).unwrap_err();
    assert!(
        rb_err.to_string().contains("stale rollback refused"),
        "rollback must refuse when revoke markers advanced: {rb_err:#}"
    );
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    assert!(state.paths.twitch_delegated_revoked.is_file());

    let restarted = restart_app_at(&userdata, false);
    assert!(
        restarted.paths.twitch_delegated_revoke_pending.is_file()
            || restarted.paths.twitch_delegated_revoked.is_file(),
        "restart must quarantine failed revoke"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn scheduled_revoke_pending_survives_replacement_marker_clear_race() {
    use std::sync::mpsc;

    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, services) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_sched_revoke_race"))
        .unwrap();
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());

    let (_gate, arrived_rx, resume_tx) =
        stream_sync_core::test_support::install_delegated_authority_gate(
            stream_sync_core::test_support::DelegatedAuthorityBoundary::PersistBeforeMarkerClear,
        );
    let state_apply = state.clone();
    let replacement = sample_session("ssk_test_placeholder_sched_revoke_replacement");
    let apply_task =
        std::thread::spawn(move || state_apply.persist_delegated_replacement_session(&replacement));
    arrived_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("replacement must reach marker-clear gate");

    let (done_tx, done_rx) = mpsc::channel();
    let svc = services.clone();
    let state_sched = state.clone();
    let schedule_thread = std::thread::spawn(move || {
        svc.schedule_durable_revoke_for_test(state_sched, 1, "scheduled_race");
        let _ = done_tx.send(());
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        done_rx.try_recv().is_err(),
        "scheduled revoke must block on authority lock while replacement holds marker-clear transaction"
    );

    resume_tx.send(()).expect("release replacement");
    let _apply_result = apply_task.join().expect("apply join");
    done_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("scheduled revoke must publish pending after replacement releases lock");
    let _ = schedule_thread.join();
    assert!(
        state.paths.twitch_delegated_revoke_pending.is_file(),
        "scheduled revoke pending must survive replacement marker-clear race"
    );

    let restarted = restart_app_at(&userdata, false);
    assert!(
        restarted.paths.twitch_delegated_revoke_pending.is_file()
            || restarted.paths.twitch_delegated_revoked.is_file(),
        "restart remains fail-closed while revoke signal present"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn invalid_slot_metadata_revoke_deletes_legacy_revision_bundle() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 2, 9)).unwrap();
    let store = fs_secret_store(&userdata);
    write_legacy_revision_bundle(
        store.as_ref(),
        2,
        1,
        "ssk_test_placeholder_invalid_slot_conn",
        "ssk_test_placeholder_invalid_slot_at",
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state.durable_revoke_delegated().await.unwrap();
    assert!(
        store.get(&legacy_revision_bundle_key(2)).unwrap().is_none(),
        "revoke must delete legacy revision bundle when secret_revision is recoverable"
    );
    assert!(!state.delegated_secret_store_authority_remain().unwrap());
    assert!(!state.paths.twitch_delegated_revoke_pending.is_file());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn invalid_slot_metadata_revoke_fail_closed_on_legacy_delete() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 3, 9)).unwrap();
    let store = fs_secret_store(&userdata);
    write_legacy_revision_bundle(
        store.as_ref(),
        3,
        1,
        "ssk_test_placeholder_invalid_slot_del_conn",
        "ssk_test_placeholder_invalid_slot_del_at",
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .durable_fail
        .legacy_revision_remove
        .store(true, Ordering::SeqCst);
    let err = state.durable_revoke_delegated().await.unwrap_err();
    assert!(
        err.to_string().contains("legacy_revision_remove"),
        "must fail closed on legacy delete injection: {err:#}"
    );
    assert!(
        store.get(&legacy_revision_bundle_key(3)).unwrap().is_some(),
        "legacy bundle must remain when delete fails"
    );
    assert!(state.paths.twitch_delegated_revoke_pending.is_file());
    let _ = std::fs::remove_dir_all(&userdata);
}

#[test]
fn authority_remain_uses_known_revision_without_metadata_file() {
    let store = stream_sync_core::memory_secret_store();
    write_legacy_revision_bundle(
        store.as_ref(),
        7,
        1,
        "ssk_test_placeholder_known_rev_conn",
        "ssk_test_placeholder_known_rev_at",
    );
    assert!(
        stream_sync_core::delegated_secret_store_authority_remain(store.as_ref(), Some(7)).unwrap(),
        "known revision must detect legacy authority without metadata"
    );
    assert!(
        !stream_sync_core::delegated_secret_store_authority_remain(store.as_ref(), None).unwrap(),
        "revoke must pass proven revision — metadata-only lookup is insufficient after deletion"
    );
}

#[tokio::test]
async fn invalid_committed_slot_rejects_before_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    write_json(&paths.twitch_delegated, &sample_metadata_only_json(1, 1, 9)).unwrap();
    write_slot_bundle(
        fs_secret_store(&userdata).as_ref(),
        0,
        1,
        1,
        "ssk_test_placeholder_bound_conn",
        "ssk_test_placeholder_bound_at",
        None,
        None,
    );

    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let before_meta = std::fs::read(&paths.twitch_delegated).unwrap();
    let err = state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_invalid_slot"))
        .unwrap_err();
    assert!(
        err.to_string().contains("out of range"),
        "must reject invalid committed slot before mutation: {err:#}"
    );
    assert_eq!(std::fs::read(&paths.twitch_delegated).unwrap(), before_meta);
    let _ = std::fs::remove_dir_all(&userdata);
}

fn read_pending_marker_epoch(pending_path: &std::path::Path) -> u64 {
    let raw = std::fs::read(pending_path).expect("pending marker bytes");
    let value: serde_json::Value = serde_json::from_slice(&raw).expect("pending marker json");
    value
        .get("marker_epoch")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

#[tokio::test]
async fn rollback_refuses_when_revoke_marker_epoch_reused_after_replacement() {
    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    let identity = state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_epoch_reuse_rb"))
        .unwrap();
    let mut snapshot =
        stream_sync_core::test_support::DurableApplySnapshot::capture(&state).expect("capture");
    snapshot.note_persist_completed(identity);

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    assert_eq!(
        read_pending_marker_epoch(&state.paths.twitch_delegated_revoke_pending),
        1
    );

    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);
    state
        .persist_delegated_replacement_session(&sample_session(
            "ssk_test_placeholder_epoch_reuse_replacement",
        ))
        .unwrap();
    assert!(!state.paths.twitch_delegated_revoke_pending.is_file());

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    let second_epoch = read_pending_marker_epoch(&state.paths.twitch_delegated_revoke_pending);
    assert!(
        second_epoch > 1,
        "second revoke cycle must allocate a fresh epoch, got {second_epoch}"
    );

    let rb_err = snapshot.rollback(&state).unwrap_err();
    assert!(
        rb_err.to_string().contains("stale rollback refused"),
        "stale rollback must refuse when revoke epoch advanced: {rb_err:#}"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn two_revoke_cycles_separated_by_replacement_use_increasing_epochs() {
    let _gate_lock = lock_authority_gate_tests();
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_two_revoke_cycles"))
        .unwrap();

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    let first_epoch = read_pending_marker_epoch(&state.paths.twitch_delegated_revoke_pending);
    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);
    state
        .persist_delegated_replacement_session(&sample_session(
            "ssk_test_placeholder_two_revoke_cycles_repl",
        ))
        .unwrap();

    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    let second_epoch = read_pending_marker_epoch(&state.paths.twitch_delegated_revoke_pending);
    assert!(
        second_epoch > first_epoch,
        "epochs must strictly increase across cycles ({first_epoch} -> {second_epoch})"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn revoke_marker_high_water_survives_restart_and_marker_absence() {
    let userdata = test_userdata_dir();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_hw_survives"))
        .unwrap();
    state
        .durable_fail
        .credential_remove
        .store(true, Ordering::SeqCst);
    state.durable_revoke_delegated().await.unwrap_err();
    state
        .durable_fail
        .credential_remove
        .store(false, Ordering::SeqCst);
    state
        .persist_delegated_replacement_session(&sample_session(
            "ssk_test_placeholder_hw_survives_repl",
        ))
        .unwrap();
    assert!(!state.paths.twitch_delegated_revoke_pending.is_file());
    let hw_path = state.paths.twitch_delegated_revoke_marker_hw.clone();
    assert!(
        hw_path.is_file(),
        "high-water must remain after marker clear"
    );
    let hw_before = stream_sync_core::read_delegated_revoke_marker_high_water(&hw_path).unwrap();
    assert!(hw_before >= 1);

    let restarted = restart_app_at(&userdata, false);
    assert!(!restarted.paths.twitch_delegated_revoke_pending.is_file());
    let hw_after = stream_sync_core::read_delegated_revoke_marker_high_water(
        &restarted.paths.twitch_delegated_revoke_marker_hw,
    )
    .unwrap();
    assert_eq!(hw_before, hw_after);
    let _ = std::fs::remove_dir_all(&userdata);
}

#[tokio::test]
async fn revoke_marker_high_water_max_rejects_new_intent_without_mutation() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    let config = OverlayConfig {
        port: 0,
        repo_root: repo_root(),
        readonly: false,
        userdata_root: Some(userdata.clone()),
        secret_store: None,
    };
    let (_router, state, _svc) = OverlayServer::new(config).build_app().await.unwrap();
    state
        .persist_delegated_session(&sample_session("ssk_test_placeholder_hw_max"))
        .unwrap();
    stream_sync_core::write_delegated_revoke_marker_high_water(
        &paths.twitch_delegated_revoke_marker_hw,
        u64::MAX,
    )
    .unwrap();
    let hw_before = std::fs::read(&paths.twitch_delegated_revoke_marker_hw).unwrap();
    let pending_before = std::fs::read(&paths.twitch_delegated_revoke_pending).ok();

    let err = state.mark_durable_revoke_pending().unwrap_err();
    assert!(
        err.to_string().contains("epoch exhausted"),
        "must reject allocation at u64::MAX: {err:#}"
    );
    assert_eq!(
        std::fs::read(&paths.twitch_delegated_revoke_marker_hw).unwrap(),
        hw_before
    );
    assert_eq!(
        std::fs::read(&paths.twitch_delegated_revoke_pending).ok(),
        pending_before
    );
    let _ = std::fs::remove_dir_all(&userdata);
}

#[test]
fn corrupt_revoke_marker_high_water_fails_closed_on_startup() {
    let userdata = test_userdata_dir();
    let paths = paths_for_root(&userdata, false).unwrap();
    std::fs::write(&paths.twitch_delegated_revoke_marker_hw, b"not-json").unwrap();
    let startup = AppState::new(paths, repo_root(), 0, false, fs_secret_store(&userdata));
    assert!(
        startup.is_err(),
        "corrupt revoke marker high-water must fail closed"
    );
    let _ = std::fs::remove_dir_all(&userdata);
}
