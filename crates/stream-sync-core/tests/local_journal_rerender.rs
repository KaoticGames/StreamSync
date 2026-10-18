use sha2::{Digest, Sha256};
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use stream_sync_core::voice_delivery::local_finalization::{
    analyze_local_journal_session, derive_voice_v2_opaque_stem_id, encode_journal_file,
    encode_journal_record, run_offline_journal_rerender, session_forensics_certified_clean,
    JournalRecord, LocalFinalizationManifest, LocalFinalizationObjectRef,
    LocalFinalizationOutputPolicy, LocalFinalizationSegment, LocalFinalizationUser,
    OfflineJournalRerenderError, OfflineJournalRerenderNetworkSeal, OfflineJournalRerenderOptions,
    JOURNAL_FORMAT_VERSION_V2, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT, OFFLINE_RECOVERY_COMPLETE_MARKER,
    OFFLINE_RECOVERY_MANIFEST_FILENAME,
};
use stream_sync_core::voice_delivery::{
    hex_digest, sha256_hex_reader, DEFAULT_STREAM_CHUNK, RECOVERY_REPORT_RELATIVE_PATH,
};

fn test_user(session_id: &str, discord: &str, nick: &str) -> LocalFinalizationUser {
    let guild_id = "123456789012345678";
    LocalFinalizationUser {
        discord_user_id: discord.into(),
        path_nick: nick.into(),
        opaque_stem_id: derive_voice_v2_opaque_stem_id(session_id, guild_id, discord, nick),
    }
}

fn sample_record(user: &str, epoch: u32, seq: u16, capture: u64) -> JournalRecord {
    JournalRecord {
        discord_user_id: user.into(),
        ssrc_epoch: epoch,
        sequence: seq,
        rtp_timestamp: capture as u32,
        arrived_at_ns: u64::from(seq) * 1_000_000,
        session_capture_offset_samples: Some(capture),
        opus_payload: vec![0x08, 0x01],
    }
}

fn manifest_with_segments(
    session_id: &str,
    users: Vec<LocalFinalizationUser>,
    segment_bodies: Vec<(u32, Vec<u8>)>,
) -> LocalFinalizationManifest {
    let segments: Vec<LocalFinalizationSegment> = segment_bodies
        .iter()
        .map(|(seq, body)| {
            let sha = sha256_hex_reader(Cursor::new(body), DEFAULT_STREAM_CHUNK).unwrap();
            LocalFinalizationSegment {
                segment_seq: *seq,
                key: format!("seg-{seq}"),
                byte_length: body.len() as u64,
                sha256: sha,
            }
        })
        .collect();
    let guild_id = "123456789012345678";
    LocalFinalizationManifest {
        version: 1,
        finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
        session_id: session_id.into(),
        guild_id: guild_id.into(),
        channel_id: "234567890123456789".into(),
        session_start_wall_ms: 0,
        session_start_monotonic_ns: "0".into(),
        sealed_stop_wall_ms: 0,
        sealed_stop_monotonic_ns: "0".into(),
        target_sample_count_48k: 960,
        journal_sha256: "a".repeat(64),
        recording_manifest: LocalFinalizationObjectRef {
            key: format!(
                "voice-delivery-v2/{guild_id}/{session_id}/manifest/recording.manifest.json"
            ),
            sha256: "b".repeat(64),
            byte_length: 4096,
        },
        sealed_index: LocalFinalizationObjectRef {
            key: format!("voice-delivery-v2/{guild_id}/{session_id}/journal/index.sealed.json"),
            sha256: "c".repeat(64),
            byte_length: 512,
        },
        segments: segments
            .into_iter()
            .map(|seg| LocalFinalizationSegment {
                key: format!(
                    "voice-delivery-v2/{guild_id}/{session_id}/journal/{:08}.synj",
                    seg.segment_seq
                ),
                segment_seq: seg.segment_seq,
                byte_length: seg.byte_length,
                sha256: seg.sha256,
            })
            .collect(),
        users,
        output: LocalFinalizationOutputPolicy {
            wav_format: LOCAL_FINALIZATION_WAV_FORMAT.into(),
            recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
        },
    }
}

fn write_session(
    session_root: &Path,
    manifest: &LocalFinalizationManifest,
    segment_bodies: &[Vec<u8>],
) {
    fs::create_dir_all(session_root.join("journal/segments")).unwrap();
    for (seg, body) in manifest.segments.iter().zip(segment_bodies) {
        let path = session_root
            .join("journal/segments")
            .join(format!("{:08}.synj", seg.segment_seq));
        fs::write(path, body).unwrap();
    }
    let json = serde_json::to_string_pretty(manifest).unwrap();
    fs::write(session_root.join("manifest.json"), json).unwrap();
}

fn certified_clean_session(tmp: &Path) -> (PathBuf, LocalFinalizationManifest) {
    let session_id = "550e8400-e29b-41d4-a716-446655440005";
    let session_root = tmp.join(session_id);
    let user = "123456789012345678";
    let body = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[]);
    let users = vec![test_user(session_id, user, "wrap_epoch")];
    let manifest = manifest_with_segments(session_id, users, vec![(0, body.clone())]);
    write_session(&session_root, &manifest, &[body]);
    let report = analyze_local_journal_session(&session_root).unwrap();
    assert!(session_forensics_certified_clean(&report));
    (session_root, manifest)
}

fn run_options() -> OfflineJournalRerenderOptions {
    OfflineJournalRerenderOptions {
        skip_native_opus_check: true,
        test_fail_after_staging: false,
    }
}

#[test]
fn refuses_existing_output_before_staging() {
    let tmp = tempfile::tempdir().unwrap();
    let (session_root, _) = certified_clean_session(tmp.path());
    let out_parent = tmp.path().join("dest");
    fs::create_dir_all(&out_parent).unwrap();
    let final_dir = out_parent.join("recovered-final");
    fs::create_dir_all(&final_dir).unwrap();
    let err = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &final_dir,
        run_options(),
    )
    .unwrap_err();
    assert!(matches!(
        err,
        OfflineJournalRerenderError::OutputAlreadyExists
    ));
    assert!(
        out_parent.read_dir().unwrap().all(|e| !e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".streamsync-stage-")),
        "must not create staging when final exists"
    );
}

#[test]
fn refuses_uncertified_source_before_staging() {
    let tmp = tempfile::tempdir().unwrap();
    let session_id = "550e8400-e29b-41d4-a716-446655440013";
    let session_root = tmp.path().join(session_id);
    let user = "123456789012345678";
    let records = vec![sample_record(user, 1, 0, 0), sample_record(user, 1, 5, 0)];
    let encoded: Vec<Vec<u8>> = records
        .iter()
        .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
        .collect();
    let body = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded);
    let users = vec![test_user(session_id, user, "pathological")];
    let manifest = manifest_with_segments(session_id, users, vec![(0, body.clone())]);
    write_session(&session_root, &manifest, &[body]);
    let out = tmp.path().join("dest").join("recovered");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    let err = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &out,
        run_options(),
    )
    .unwrap_err();
    assert!(
        matches!(err, OfflineJournalRerenderError::ForensicsNotCertified),
        "unexpected err: {err:?}"
    );
    assert!(!out.exists());
}

#[test]
fn success_publishes_recovery_manifest_and_completion_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let (session_root, manifest) = certified_clean_session(tmp.path());
    let manifest_before = fs::read(session_root.join("manifest.json")).unwrap();
    let out = tmp.path().join("dest").join("recovered-success");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    let outcome = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &out,
        run_options(),
    )
    .unwrap();
    assert!(outcome.final_dir.is_dir());
    assert!(outcome.recovery_manifest_path.is_file());
    assert!(outcome.completion_marker_path.is_file());
    assert!(outcome.final_dir.join("wrap_epoch.wav").is_file());
    assert!(outcome
        .final_dir
        .join(RECOVERY_REPORT_RELATIVE_PATH)
        .is_file());
    let marker = fs::read_to_string(&outcome.completion_marker_path).unwrap();
    assert!(marker.contains(&manifest.session_id));
    let recovery_json = fs::read_to_string(&outcome.recovery_manifest_path).unwrap();
    assert!(recovery_json.contains("\"offlineRecovery\": true"));
    assert!(recovery_json.contains("\"cloudReceiptPosted\": false"));
    assert_eq!(
        fs::read(session_root.join("manifest.json")).unwrap(),
        manifest_before
    );
}

#[test]
fn failure_preserves_source_and_leaves_staging_without_final() {
    let tmp = tempfile::tempdir().unwrap();
    let (session_root, _) = certified_clean_session(tmp.path());
    let manifest_before = fs::read(session_root.join("manifest.json")).unwrap();
    let out = tmp.path().join("dest").join("recovered-fail");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    let opts = OfflineJournalRerenderOptions {
        skip_native_opus_check: true,
        test_fail_after_staging: true,
    };
    assert!(run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &out,
        opts,
    )
    .is_err());
    assert!(!out.exists());
    assert_eq!(
        fs::read(session_root.join("manifest.json")).unwrap(),
        manifest_before
    );
    let parent = out.parent().unwrap();
    let staging_left = parent.read_dir().unwrap().filter_map(|e| e.ok()).any(|e| {
        e.file_name()
            .to_string_lossy()
            .starts_with(".streamsync-stage-")
    });
    assert!(staging_left);
}

#[test]
fn metadata_binds_source_manifest_digest() {
    let tmp = tempfile::tempdir().unwrap();
    let (session_root, _) = certified_clean_session(tmp.path());
    let digest = hex_digest(&Sha256::digest(
        &fs::read(session_root.join("manifest.json")).unwrap(),
    ));
    let out = tmp.path().join("dest").join("recovered-meta");
    fs::create_dir_all(out.parent().unwrap()).unwrap();
    let outcome = run_offline_journal_rerender(
        OfflineJournalRerenderNetworkSeal,
        &session_root,
        &out,
        run_options(),
    )
    .unwrap();
    let json = fs::read_to_string(outcome.recovery_manifest_path).unwrap();
    assert!(json.contains(&digest));
    let marker = fs::read_to_string(outcome.completion_marker_path).unwrap();
    assert!(marker.contains(OFFLINE_RECOVERY_MANIFEST_FILENAME));
    let recovery_manifest_sha = hex_digest(&Sha256::digest(json.as_bytes()));
    assert!(marker.contains(&recovery_manifest_sha));
}

#[test]
fn offline_rerender_source_has_no_network_client_symbols() {
    let src = include_str!("../src/voice_delivery/local_finalization/offline_journal_rerender.rs");
    let production = src.split("#[cfg(test)]").next().unwrap_or(src);
    assert!(!production.contains("reqwest"));
    assert!(!production.contains("VoiceV2Client"));
    assert!(!production.contains("HttpLocalFinalizationClient"));
}
