//! RED: same-session manifest identity must be exact immutable manifest equality, not session_id alone.

use std::fs;
use stream_sync_core::voice_delivery::local_finalization::{
    derive_voice_v2_opaque_stem_id, LocalFinalizationManifest, LocalFinalizationObjectRef,
    LocalFinalizationOutputPolicy, LocalFinalizationSegment, LocalFinalizationUser,
    LocalJournalStore, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::RECOVERY_REPORT_RELATIVE_PATH;

const SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440099";
const GUILD_ID: &str = "123456789012345678";

fn sample_manifest(journal_sha256: &str, target_samples: u64) -> LocalFinalizationManifest {
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
        target_sample_count_48k: target_samples,
        journal_sha256: journal_sha256.into(),
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
        segments: vec![LocalFinalizationSegment {
            segment_seq: 0,
            key: format!(
                "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/{:08}.synj",
                0u32
            ),
            byte_length: 64,
            sha256: "d".repeat(64),
        }],
        users: vec![user],
        output: LocalFinalizationOutputPolicy {
            wav_format: LOCAL_FINALIZATION_WAV_FORMAT.into(),
            recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
        },
    }
}

fn manifest_on_disk(store: &LocalJournalStore) -> std::path::PathBuf {
    store.session_root().join("manifest.json")
}

fn assert_journal_store_conflict(err: impl std::fmt::Display) {
    assert!(
        err.to_string().to_lowercase().contains("conflict"),
        "expected journal store conflict, got: {err}"
    );
}

#[test]
fn persist_manifest_idempotent_for_identical_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest).expect("first persist");
    let bytes_before = fs::read(manifest_on_disk(&store)).expect("read manifest");
    store
        .persist_manifest(&manifest)
        .expect("identical manifest must be accepted idempotently");
    let bytes_after = fs::read(manifest_on_disk(&store)).expect("read manifest after");
    assert_eq!(
        bytes_before, bytes_after,
        "idempotent persist must not rewrite bytes"
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_immutable_journal_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original manifest bytes");

    let manifest_b = sample_manifest(&"e".repeat(64), 48_000);
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(manifest_a.journal_sha256, manifest_b.journal_sha256);

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("conflicting manifest must not be accepted");
    assert_journal_store_conflict(err);
    let on_disk = fs::read(manifest_on_disk(&store)).expect("read manifest after conflict");
    assert_eq!(
        on_disk, original,
        "conflicting persist must leave original manifest bytes unchanged"
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_target_sample_count() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let manifest_b = sample_manifest(&"a".repeat(64), 96_000);
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(
        manifest_a.target_sample_count_48k,
        manifest_b.target_sample_count_48k
    );

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("target sample count change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_segment_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let mut manifest_b = sample_manifest(&"a".repeat(64), 48_000);
    manifest_b.segments[0].sha256 = "f".repeat(64);
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(manifest_a.segments[0].sha256, manifest_b.segments[0].sha256);

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("segment digest change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_segment_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let mut manifest_b = sample_manifest(&"a".repeat(64), 48_000);
    manifest_b.segments[0].key = format!(
        "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/{:08}.synj",
        1u32
    );
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(manifest_a.segments[0].key, manifest_b.segments[0].key);

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("segment key change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_segment_byte_length() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let mut manifest_b = sample_manifest(&"a".repeat(64), 48_000);
    manifest_b.segments[0].byte_length = 128;
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(
        manifest_a.segments[0].byte_length,
        manifest_b.segments[0].byte_length
    );

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("segment byte length change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_user_mapping() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let mut manifest_b = sample_manifest(&"a".repeat(64), 48_000);
    manifest_b.users[0].path_nick = "other-player".into();
    manifest_b.users[0].opaque_stem_id =
        derive_voice_v2_opaque_stem_id(SESSION_ID, GUILD_ID, "123456789012345678", "other-player");
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(manifest_a.users, manifest_b.users);

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("user mapping change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}

#[test]
fn persist_manifest_rejects_same_session_different_finalizer_algorithm_version() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let manifest_a = sample_manifest(&"a".repeat(64), 48_000);
    store.persist_manifest(&manifest_a).expect("persist A");
    let original = fs::read(manifest_on_disk(&store)).expect("read original");

    let mut manifest_b = sample_manifest(&"a".repeat(64), 48_000);
    manifest_b.finalizer_algorithm_version = "other-finalizer-v99".into();
    assert_eq!(manifest_a.session_id, manifest_b.session_id);
    assert_ne!(
        manifest_a.finalizer_algorithm_version,
        manifest_b.finalizer_algorithm_version
    );

    let err = store
        .persist_manifest(&manifest_b)
        .expect_err("finalizer algorithm version change must conflict");
    assert_journal_store_conflict(err);
    assert_eq!(
        fs::read(manifest_on_disk(&store)).expect("read after"),
        original
    );
}
