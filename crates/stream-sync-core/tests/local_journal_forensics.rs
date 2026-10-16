use std::fs;
use std::path::Path;
use stream_sync_core::voice_delivery::local_finalization::{
    analyze_local_journal_session, build_minimal_manifest_json, encode_journal_file,
    encode_journal_record, forensics_report_to_json, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
};

fn write_session(root: &Path, session_id: &str, users: &[(&str, &str)], records: &[JournalRecord]) {
    fs::create_dir_all(root.join("journal/segments")).unwrap();
    let manifest = build_minimal_manifest_json(session_id, users, 48_000 * 60);
    fs::write(root.join("manifest.json"), manifest).unwrap();
    let encoded: Vec<Vec<u8>> = records
        .iter()
        .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
        .collect();
    let file = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded);
    fs::write(root.join("journal/segments/00000000.synj"), file).unwrap();
}

fn sample_record(
    user: &str,
    epoch: u32,
    seq: u16,
    capture: u64,
    payload_len: usize,
) -> JournalRecord {
    JournalRecord {
        discord_user_id: user.into(),
        ssrc_epoch: epoch,
        sequence: seq,
        rtp_timestamp: (capture as u32) * 960,
        arrived_at_ns: u64::from(seq) * 1_000_000,
        session_capture_offset_samples: Some(capture),
        opus_payload: if payload_len == 0 {
            Vec::new()
        } else {
            vec![0xFE; payload_len]
        },
    }
}

#[test]
fn two_epochs_trigger_local_epoch_frontier_stall() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![
        sample_record(user, 1, 0, 0, 120),
        sample_record(user, 1, 1, 960, 120),
        sample_record(user, 2, 0, 1920, 120),
        sample_record(user, 2, 1, 2880, 120),
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440000",
        &[("123456789012345678", "player")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "player")
        .unwrap();
    assert!(u.local_epoch_frontier_stall);
    assert!(u
        .diagnostics
        .iter()
        .any(|d| d == "LOCAL_EPOCH_FRONTIER_STALL"));
    assert_eq!(u.distinct_ssrc_epochs, 2);
    assert_eq!(u.accumulator.discarded_duplicate, 0);
}

#[test]
fn in_window_reorder_measured_as_duplicate_drops() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    // Arrival order 5, 3, 4 — seq 3 should be discarded stale after 5 drains.
    let records = vec![
        sample_record(user, 1, 5, 4800, 120),
        sample_record(user, 1, 3, 2880, 120),
        sample_record(user, 1, 4, 3840, 120),
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440001",
        &[("123456789012345678", "reorder")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "reorder")
        .unwrap();
    assert!(u.arrival_reorder_events >= 1);
    assert!(u.accumulator.discarded_duplicate >= 1);
    assert!(!u.local_epoch_frontier_stall);
}

#[test]
fn empty_marker_counted_separate_from_algorithm_drops() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![
        sample_record(user, 1, 0, 0, 120),
        sample_record(user, 1, 1, 960, 0),
        sample_record(user, 1, 2, 1920, 120),
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440002",
        &[("123456789012345678", "markers")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "markers")
        .unwrap();
    assert_eq!(u.empty_payload_markers, 1);
    assert_eq!(u.accumulator.lost_placement, 1);
    assert_eq!(u.accumulator.discarded_duplicate, 0);
}

#[test]
fn json_output_has_no_payload_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![sample_record(user, 1, 0, 0, 64)];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440003",
        &[("123456789012345678", "clean")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let json = forensics_report_to_json(&report).unwrap();
    assert!(!json.contains("opusPayload"));
    assert!(!json.contains("opus_payload"));
    assert!(!json.contains("FE")); // fixture payload byte pattern
    assert!(json.contains("wavRelativePath"));
    assert!(json.contains("clean.wav"));
}

#[test]
fn single_clean_epoch_does_not_flag_stall() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![
        sample_record(user, 1, 0, 0, 120),
        sample_record(user, 1, 1, 960, 120),
        sample_record(user, 1, 2, 1920, 120),
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440004",
        &[("123456789012345678", "single")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "single")
        .unwrap();
    assert!(!u.local_epoch_frontier_stall);
    assert_eq!(u.distinct_ssrc_epochs, 1);
    assert_eq!(u.accumulator.decode, 3);
}
