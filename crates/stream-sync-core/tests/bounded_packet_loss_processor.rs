use std::path::PathBuf;
use stream_sync_core::voice_delivery::local_finalization::{
    encode_journal_file, encode_journal_record, process_verified_segments, JournalRecord,
    LocalFinalizationManifest, LocalFinalizationObjectRef, LocalFinalizationOutputPolicy,
    LocalFinalizationSegment, LocalFinalizationUser, LocalProcessorError,
    JOURNAL_FORMAT_VERSION_V2, LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    LOCAL_FINALIZATION_WAV_FORMAT,
};
use stream_sync_core::voice_delivery::RECOVERY_REPORT_RELATIVE_PATH;

fn manifest(user_id: &str, nick: &str, target: u64) -> LocalFinalizationManifest {
    LocalFinalizationManifest {
        version: 1,
        finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
        session_id: "sess-policy".into(),
        guild_id: "123456789012345678".into(),
        channel_id: "234567890123456789".into(),
        session_start_wall_ms: 0,
        session_start_monotonic_ns: "0".into(),
        sealed_stop_wall_ms: 0,
        sealed_stop_monotonic_ns: "0".into(),
        target_sample_count_48k: target,
        journal_sha256: "a".repeat(64),
        recording_manifest: LocalFinalizationObjectRef {
            key: "k".into(),
            sha256: "b".repeat(64),
            byte_length: 0,
        },
        sealed_index: LocalFinalizationObjectRef {
            key: "k".into(),
            sha256: "c".repeat(64),
            byte_length: 0,
        },
        segments: vec![LocalFinalizationSegment {
            segment_seq: 0,
            key: "seg".into(),
            byte_length: 0,
            sha256: "d".repeat(64),
        }],
        users: vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: nick.into(),
            opaque_stem_id: "f".repeat(32),
        }],
        output: LocalFinalizationOutputPolicy {
            wav_format: LOCAL_FINALIZATION_WAV_FORMAT.into(),
            recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.into(),
        },
    }
}

fn payload() -> Vec<u8> {
    vec![0x08, 0x01]
}

fn overlap_pair(user: &str, base_seq: u16) -> Vec<JournalRecord> {
    let capture = u64::from(base_seq) * 960;
    let first = JournalRecord {
        discord_user_id: user.into(),
        ssrc_epoch: 1,
        sequence: base_seq,
        rtp_timestamp: capture as u32,
        arrived_at_ns: u64::from(base_seq),
        session_capture_offset_samples: Some(capture),
        opus_payload: payload(),
    };
    let second = JournalRecord {
        discord_user_id: user.into(),
        ssrc_epoch: 1,
        sequence: base_seq + 1,
        rtp_timestamp: (capture as u32) + 480,
        arrived_at_ns: u64::from(base_seq) + 1,
        session_capture_offset_samples: Some(capture),
        opus_payload: payload(),
    };
    vec![first, second]
}

fn loss_marker(user: &str, seq: u16, capture: u64) -> JournalRecord {
    JournalRecord {
        discord_user_id: user.into(),
        ssrc_epoch: 1,
        sequence: seq,
        rtp_timestamp: capture as u32,
        arrived_at_ns: u64::from(seq) * 1_000,
        session_capture_offset_samples: Some(capture),
        opus_payload: vec![],
    }
}

fn foreign_pad(count: usize) -> Vec<JournalRecord> {
    (0..count)
        .map(|i| {
            let seq = i as u16;
            JournalRecord {
                discord_user_id: "999999999999999999".into(),
                ssrc_epoch: 1,
                sequence: seq,
                rtp_timestamp: u32::from(seq) * 960,
                arrived_at_ns: u64::from(seq),
                session_capture_offset_samples: Some(u64::from(seq) * 960),
                opus_payload: payload(),
            }
        })
        .collect()
}

fn run_journal(user: &str, records: &[JournalRecord]) -> Result<(), LocalProcessorError> {
    let dir = tempfile::tempdir().unwrap();
    let encoded: Vec<_> = records
        .iter()
        .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
        .collect();
    let seg = dir.path().join("seg.synj");
    std::fs::write(
        &seg,
        encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
    )
    .unwrap();
    let m = manifest(user, "u", 10_000_000);
    process_verified_segments(&m, &[seg], &dir.path().join("staging")).map(|_| ())
}

#[test]
fn isolated_overlap_among_ten_thousand_records_allowed() {
    let user = "123456789012345678";
    let mut records = Vec::new();
    for seq in 0..10_000u16 {
        let capture = u64::from(seq) * 960;
        records.push(JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: seq,
            rtp_timestamp: capture as u32,
            arrived_at_ns: u64::from(seq),
            session_capture_offset_samples: Some(capture),
            opus_payload: payload(),
        });
    }
    records.extend(overlap_pair(user, 20_000));
    assert!(run_journal(user, &records).is_ok());
}

#[test]
fn ratio_violation_refused() {
    let user = "123456789012345678";
    let mut records = Vec::new();
    for seq in 0..5000u16 {
        let capture = u64::from(seq) * 960;
        records.push(JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: seq,
            rtp_timestamp: capture as u32,
            arrived_at_ns: u64::from(seq),
            session_capture_offset_samples: Some(capture),
            opus_payload: payload(),
        });
    }
    records.extend(overlap_pair(user, 10_000));
    records.extend(overlap_pair(user, 10_002));
    let err = run_journal(user, &records).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("0.01%") || msg.contains("Ratio"),
        "expected ratio refusal: {}",
        msg
    );
}

#[test]
fn thirty_third_overlap_refused() {
    let user = "123456789012345678";
    let mut records = Vec::new();
    for i in 0..200u16 {
        let capture = u64::from(i) * 960;
        records.push(JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: i,
            rtp_timestamp: capture as u32,
            arrived_at_ns: u64::from(i),
            session_capture_offset_samples: Some(capture),
            opus_payload: payload(),
        });
    }
    for block in 0..33u16 {
        let base = 1_000 + block * 4;
        records.extend(overlap_pair(user, base));
    }
    let err = run_journal(user, &records).unwrap_err();
    assert!(
        err.to_string().contains("maximum 32") || err.to_string().contains("exceeds session"),
        "expected max skipped: {}",
        err
    );
}

#[test]
fn consecutive_loss_markers_same_stream_refused() {
    let user = "123456789012345678";
    let records = vec![
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(0),
            opus_payload: payload(),
        },
        loss_marker(user, 1, 960),
        loss_marker(user, 2, 1920),
    ];
    let err = run_journal(user, &records).unwrap_err();
    assert!(
        err.to_string().contains("consecutive"),
        "expected consecutive refusal: {}",
        err
    );
}

#[test]
fn decode_framing_and_overlap_share_one_budget() {
    let user = "123456789012345678";
    let mut records = foreign_pad(10_000);
    for seq in 0..20_000u16 {
        let capture = u64::from(seq) * 960;
        records.push(JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: seq,
            rtp_timestamp: capture as u32,
            arrived_at_ns: u64::from(seq),
            session_capture_offset_samples: Some(capture),
            opus_payload: payload(),
        });
    }
    records.push(loss_marker(user, 20_000, 20_000 * 960));
    records.extend(overlap_pair(user, 20_010));
    let out_dir = tempfile::tempdir().unwrap();
    let encoded: Vec<_> = records
        .iter()
        .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
        .collect();
    let seg: PathBuf = out_dir.path().join("seg.synj");
    std::fs::write(
        &seg,
        encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
    )
    .unwrap();
    let out = process_verified_segments(
        &manifest(user, "u", 10_000_000),
        &[seg],
        &out_dir.path().join("staging"),
    )
    .unwrap();
    assert_eq!(out.packet_loss_policy.skipped_count, 2);
    assert_eq!(
        out.packet_loss_policy
            .source_counts
            .framing_or_lost_placement,
        1
    );
    assert_eq!(out.packet_loss_policy.source_counts.placement_overlap, 1);
}
