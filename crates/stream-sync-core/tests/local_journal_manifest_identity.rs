//! RED: same-session manifest identity must be exact immutable manifest equality, not session_id alone.

use std::fs;

use serde_json::Value;
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

fn seed_manifest_on_disk(
    store: &LocalJournalStore,
    manifest: &LocalFinalizationManifest,
) -> Vec<u8> {
    store.persist_manifest(manifest).expect("seed manifest");
    fs::read(manifest_on_disk(store)).expect("read seeded manifest")
}

fn mutate_manifest_json_on_disk(store: &LocalJournalStore, mutator: impl FnOnce(&mut Value)) {
    let path = manifest_on_disk(store);
    let raw = fs::read_to_string(&path).expect("read manifest for mutation");
    let mut value: Value = serde_json::from_str(&raw).expect("manifest must be readable json");
    mutator(&mut value);
    let rewritten = serde_json::to_string_pretty(&value).expect("rewrite manifest json");
    fs::write(&path, rewritten).expect("write mutated manifest");
}

fn persist_authority_after_on_disk_mutation(
    store: &LocalJournalStore,
    authority: &LocalFinalizationManifest,
    mutator: impl FnOnce(&mut Value),
) {
    mutate_manifest_json_on_disk(store, mutator);
    let mutated_bytes = fs::read(manifest_on_disk(store)).expect("read mutated manifest");
    let err = store
        .persist_manifest(authority)
        .expect_err("mutated on-disk manifest must not be overwritten by authority persist");
    assert_journal_store_conflict(err);
    let on_disk = fs::read(manifest_on_disk(store)).expect("read manifest after conflict");
    assert_eq!(
        on_disk, mutated_bytes,
        "conflicting persist must leave mutated on-disk manifest bytes unchanged"
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
fn persist_manifest_rejects_on_disk_mutated_journal_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["journalSha256"] = Value::String("e".repeat(64));
    });
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_target_sample_count() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["targetSampleCount48k"] = Value::from(96_000_i64);
    });
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_segment_sha256() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["segments"][0]["sha256"] = Value::String("f".repeat(64));
    });
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_segment_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["segments"][0]["key"] = Value::String(format!(
            "voice-delivery-v2/{GUILD_ID}/{SESSION_ID}/journal/{:08}.synj",
            1u32
        ));
    });
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_segment_byte_length() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["segments"][0]["byteLength"] = Value::from(128_i64);
    });
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_user_mapping() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["users"][0]["pathNick"] = Value::String("other-player".into());
        v["users"][0]["opaqueStemId"] = Value::String(derive_voice_v2_opaque_stem_id(
            SESSION_ID,
            GUILD_ID,
            "123456789012345678",
            "other-player",
        ));
    });
}

#[test]
fn persist_manifest_rejects_existing_manifest_with_invalid_utf8() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    let invalid_utf8 = vec![0xff, 0xfe, 0xfd];
    fs::write(manifest_on_disk(&store), &invalid_utf8).expect("write invalid utf-8 manifest");
    let err = store
        .persist_manifest(&authority)
        .expect_err("invalid utf-8 on-disk manifest must not be overwritten by authority persist");
    assert_journal_store_conflict(err);
    let on_disk = fs::read(manifest_on_disk(&store)).expect("read manifest after conflict");
    assert_eq!(
        on_disk, invalid_utf8,
        "conflicting persist must leave invalid utf-8 manifest bytes unchanged"
    );
}

#[test]
fn persist_manifest_rejects_syntactically_malformed_existing_manifest_json() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    let malformed = b"{not valid json".to_vec();
    fs::write(manifest_on_disk(&store), &malformed).expect("write malformed manifest");
    let err = store
        .persist_manifest(&authority)
        .expect_err("malformed on-disk manifest must not be overwritten by authority persist");
    assert_journal_store_conflict(err);
    let on_disk = fs::read(manifest_on_disk(&store)).expect("read manifest after conflict");
    assert_eq!(
        on_disk, malformed,
        "conflicting persist must leave malformed on-disk manifest bytes unchanged"
    );
}

#[test]
fn persist_manifest_rejects_on_disk_mutated_finalizer_algorithm_version() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalJournalStore::open(tmp.path(), SESSION_ID).expect("open store");
    let authority = sample_manifest(&"a".repeat(64), 48_000);
    seed_manifest_on_disk(&store, &authority);
    persist_authority_after_on_disk_mutation(&store, &authority, |v| {
        v["finalizerAlgorithmVersion"] = Value::String("other-finalizer-v99".into());
    });
}
