use std::fs;
use std::path::Path;
use stream_sync_core::voice_delivery::local_finalization::{
    analyze_local_journal_session, analyze_local_journal_session_with_options,
    build_minimal_manifest_json, encode_journal_file, encode_journal_record,
    forensics_report_to_json, local_journal_forensics_exit_code, offline_journal_forensics_options,
    render_forensics_table, session_forensics_certified_clean, JournalRecord,
    JOURNAL_FORMAT_VERSION_V2,
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
        rtp_timestamp: capture as u32,
        arrived_at_ns: u64::from(seq) * 1_000_000,
        session_capture_offset_samples: Some(capture),
        opus_payload: if payload_len == 0 {
            Vec::new()
        } else {
            vec![0x08, 0x01]
        },
    }
}

#[test]
fn two_epochs_do_not_stall_after_accumulator_parity() {
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
    assert!(!u.local_epoch_frontier_stall);
    assert_eq!(u.distinct_ssrc_epochs, 2);
    assert_eq!(u.accumulator.decode, 4);
    assert_eq!(u.accumulator.discarded_out_of_window, 0);
}

#[test]
fn production_shaped_multi_wrap_epoch_fixture_no_mass_discard() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let mut records = Vec::new();
    let mut seq = 65530u16;
    let mut capture = 0u64;
    for _ in 0..8 {
        records.push(sample_record(user, 1, seq, capture, 120));
        capture += 960;
        seq = seq.wrapping_add(1);
    }
    records.push(sample_record(user, 2, 0, capture, 120));
    records.push(sample_record(user, 2, 1, capture + 960, 120));
    records.push(sample_record(user, 2, 2, capture + 1920, 120));
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440005",
        &[("123456789012345678", "wrap_epoch")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "wrap_epoch")
        .unwrap();
    assert!(!u.local_epoch_frontier_stall);
    let total_nonempty = u.nonempty_payload_records;
    let emitted = u.accumulator.decode + u.accumulator.lost_placement;
    assert_eq!(emitted, total_nonempty);
    let drops = u.accumulator.discarded_duplicate
        + u.accumulator.discarded_conflict
        + u.accumulator.discarded_out_of_window;
    assert_eq!(drops, 0);
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
fn pathological_overlap_fixture_not_certified_clean() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 1,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        },
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: 5,
            rtp_timestamp: 480,
            arrived_at_ns: 5,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        },
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440099",
        &[("123456789012345678", "pathological")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "pathological")
        .unwrap();
    assert!(!u.forensics_certified_clean);
    assert!(u.accumulator.accumulator_errors > 0);
    assert!(u
        .diagnostics
        .iter()
        .any(|d| d.contains("ACCUMULATOR_FLUSH_ERROR")));
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

fn assert_session_not_certified(
    report: &stream_sync_core::voice_delivery::local_finalization::LocalJournalForensicsReport,
) {
    assert!(!report.session_forensics_certified_clean);
    let table = render_forensics_table(report);
    assert!(
        table.contains("NOT CLEAN"),
        "human table must show NOT CLEAN: {}",
        table
    );
    assert_ne!(local_journal_forensics_exit_code(report), 0);
    let json = forensics_report_to_json(report).unwrap();
    assert!(json.contains("\"sessionForensicsCertifiedClean\": false"));
}

#[test]
fn production_shaped_fixture_session_certified_clean() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let mut records = Vec::new();
    let mut seq = 65530u16;
    let mut capture = 0u64;
    for _ in 0..8 {
        records.push(sample_record(user, 1, seq, capture, 120));
        capture += 960;
        seq = seq.wrapping_add(1);
    }
    records.push(sample_record(user, 2, 0, capture, 120));
    records.push(sample_record(user, 2, 1, capture + 960, 120));
    let session_id = "550e8400-e29b-41d4-a716-446655440005";
    let session_root = dir.path().join(session_id);
    write_session(
        &session_root,
        session_id,
        &[("123456789012345678", "wrap_epoch")],
        &records,
    );
    let report = analyze_local_journal_session(&session_root).unwrap();
    assert!(report.session_forensics_certified_clean);
    assert_eq!(local_journal_forensics_exit_code(&report), 0);
}

#[test]
fn missing_segment_session_not_certified() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("journal/segments")).unwrap();
    let manifest = build_minimal_manifest_json(
        "550e8400-e29b-41d4-a716-446655440010",
        &[("123456789012345678", "player")],
        48_000,
    );
    fs::write(dir.path().join("manifest.json"), manifest).unwrap();
    let report = analyze_local_journal_session(dir.path()).unwrap();
    assert_session_not_certified(&report);
}

#[test]
fn fatal_header_session_not_certified() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("journal/segments")).unwrap();
    let manifest = build_minimal_manifest_json(
        "550e8400-e29b-41d4-a716-446655440011",
        &[("123456789012345678", "player")],
        48_000,
    );
    fs::write(dir.path().join("manifest.json"), manifest).unwrap();
    fs::write(
        dir.path().join("journal/segments/00000000.synj"),
        b"BADJUNK!",
    )
    .unwrap();
    let report = analyze_local_journal_session(dir.path()).unwrap();
    assert_session_not_certified(&report);
}

#[test]
fn recoverable_framing_session_not_certified() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![sample_record(user, 1, 0, 0, 120)];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440012",
        &[("123456789012345678", "player")],
        &records,
    );
    let seg_path = dir.path().join("journal/segments/00000000.synj");
    let mut bytes = fs::read(&seg_path).unwrap();
    if bytes.len() > 20 {
        bytes[20] ^= 0xff;
    }
    fs::write(seg_path, bytes).unwrap();
    let report = analyze_local_journal_session(dir.path()).unwrap();
    assert_session_not_certified(&report);
}

fn production_shaped_40ms_overlap_records(user: &str, base_seq: u16) -> Vec<JournalRecord> {
    let prev_start = u64::from(base_seq) * 960;
    let rtp_first = u32::from(base_seq) * 960;
    let payload = vec![0x08, 0x01];
    vec![
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: base_seq,
            rtp_timestamp: rtp_first,
            arrived_at_ns: 1,
            session_capture_offset_samples: Some(prev_start),
            opus_payload: payload.clone(),
        },
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: base_seq + 1,
            rtp_timestamp: rtp_first + 480,
            arrived_at_ns: 2,
            session_capture_offset_samples: Some(prev_start),
            opus_payload: payload,
        },
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: base_seq + 2,
            rtp_timestamp: rtp_first + 960,
            arrived_at_ns: 3,
            session_capture_offset_samples: Some(prev_start + 960),
            opus_payload: vec![0x08, 0x01],
        },
    ]
}

#[test]
fn production_shaped_overlap_strict_forensics_not_certified() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let mut records = Vec::new();
    for seq in 0..10_000u16 {
        let capture = u64::from(seq) * 960;
        records.push(sample_record(user, 1, seq, capture, 120));
    }
    records.extend(production_shaped_40ms_overlap_records(user, 10_000));
    let session_id = "550e8400-e29b-41d4-a716-446655440020";
    let session_root = dir.path().join(session_id);
    write_session(
        &session_root,
        session_id,
        &[("123456789012345678", "overlap_strict")],
        &records,
    );
    let report = analyze_local_journal_session(&session_root).unwrap();
    assert_session_not_certified(&report);
    assert_eq!(report.accumulator_placement_policy, "strict");
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "overlap_strict")
        .unwrap();
    assert!(u.accumulator.accumulator_errors > 0);
    let json = forensics_report_to_json(&report).unwrap();
    assert!(json.contains("\"accumulatorPlacementPolicy\": \"strict\""));
}

#[test]
fn production_shaped_overlap_tolerant_forensics_clean_within_policy() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let mut records = Vec::new();
    for seq in 0..10_000u16 {
        let capture = u64::from(seq) * 960;
        records.push(sample_record(user, 1, seq, capture, 120));
    }
    records.extend(production_shaped_40ms_overlap_records(user, 10_000));
    let session_id = "550e8400-e29b-41d4-a716-446655440021";
    let session_root = dir.path().join(session_id);
    write_session(
        &session_root,
        session_id,
        &[("123456789012345678", "overlap_tolerant")],
        &records,
    );
    let report = analyze_local_journal_session_with_options(
        &session_root,
        offline_journal_forensics_options(),
    )
    .unwrap();
    assert!(session_forensics_certified_clean(&report));
    assert_eq!(report.accumulator_placement_policy, "overlapTolerant");
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "overlap_tolerant")
        .unwrap();
    assert_eq!(u.accumulator.discarded_overlap, 1);
    assert_eq!(u.accumulator.accumulator_errors, 0);
    let preflight = report.packet_loss_preflight.as_ref().unwrap();
    assert_eq!(preflight.source_counts.placement_overlap, 1);
    assert!(preflight.within_policy);
    let table = render_forensics_table(&report);
    assert!(table.contains("overlapTolerant"));
    assert!(table.contains("overlap=1"));
}

#[test]
fn accumulator_error_session_not_certified() {
    let dir = tempfile::tempdir().unwrap();
    let user = "123456789012345678";
    let records = vec![
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 1,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        },
        JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: 5,
            rtp_timestamp: 480,
            arrived_at_ns: 5,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        },
    ];
    write_session(
        dir.path(),
        "550e8400-e29b-41d4-a716-446655440013",
        &[("123456789012345678", "pathological")],
        &records,
    );
    let report = analyze_local_journal_session(dir.path()).unwrap();
    assert_session_not_certified(&report);
    let u = report
        .users
        .iter()
        .find(|u| u.path_nick == "pathological")
        .unwrap();
    assert!(u.accumulator.accumulator_errors > 0);
}
