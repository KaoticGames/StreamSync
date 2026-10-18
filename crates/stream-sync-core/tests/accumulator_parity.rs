//! Vertical TDD regression tests for journal v2 accumulator semantic parity (TypeScript reference).

use serde::Deserialize;
use std::fs;
use std::path::Path;
use stream_sync_core::voice_delivery::local_finalization::{
    AccumulatorError, AccumulatorPlacementPolicy, JournalRecord, PacketDisposition,
    UserPacketAccumulator,
};

const USER: &str = "123456789012345678";
const FRAME: u64 = 960;
const TARGET: u64 = 100_000_000;
const NS_PER_FRAME: u64 = 20_000_000;

fn payload_toc_20ms() -> Vec<u8> {
    vec![0x08, 0x01]
}

fn payload_framing_skip() -> Vec<u8> {
    vec![0x0b, 0x00]
}

fn rec(
    epoch: u32,
    seq: u16,
    rtp: u32,
    capture: u64,
    arrived_ns: u64,
    payload: Vec<u8>,
) -> JournalRecord {
    JournalRecord {
        discord_user_id: USER.into(),
        ssrc_epoch: epoch,
        sequence: seq,
        rtp_timestamp: rtp,
        arrived_at_ns: arrived_ns,
        session_capture_offset_samples: Some(capture),
        opus_payload: payload,
    }
}

#[derive(Clone, Debug)]
struct EmittedRow {
    sequence: u16,
    disposition: PacketDisposition,
    extended_sequence: i64,
    timeline_sample_start: Option<u64>,
}

fn ingest_all(acc: &mut UserPacketAccumulator, records: &[JournalRecord]) -> Vec<EmittedRow> {
    let mut out = Vec::new();
    for r in records {
        let ready = acc.ingest_timeline(r.clone()).expect("ingest");
        for p in ready {
            out.push(EmittedRow {
                sequence: p.record.sequence,
                disposition: p.disposition,
                extended_sequence: p.extended_sequence,
                timeline_sample_start: p.timeline_sample_start,
            });
        }
    }
    for p in acc.flush_remaining().expect("flush") {
        out.push(EmittedRow {
            sequence: p.record.sequence,
            disposition: p.disposition,
            extended_sequence: p.extended_sequence,
            timeline_sample_start: p.timeline_sample_start,
        });
    }
    out
}

fn assert_emitted_sequences(got: &[EmittedRow], want_seqs: &[u16]) {
    let decode_seqs: Vec<u16> = got
        .iter()
        .filter(|r| {
            matches!(
                r.disposition,
                PacketDisposition::Decode | PacketDisposition::LostPlacement
            )
        })
        .map(|r| r.sequence)
        .collect();
    assert_eq!(decode_seqs, want_seqs, "emitted sequences: {:?}", got);
}

#[test]
fn parity_seq_wrap_65534_65535_0_1() {
    let records = vec![
        rec(0, 65534, 65534 * 960, 65534 * FRAME, 1, payload_toc_20ms()),
        rec(0, 65535, 65535 * 960, 65535 * FRAME, 2, payload_toc_20ms()),
        rec(0, 0, 65536 * 960, 65536 * FRAME, 3, payload_toc_20ms()),
        rec(
            0,
            1,
            65536 * 960 + 960,
            65536 * FRAME + FRAME,
            4,
            payload_toc_20ms(),
        ),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[65534, 65535, 0, 1]);
    assert_eq!(
        got[2].extended_sequence, 65536,
        "seq 0 wraps to extended 65536"
    );
}

#[test]
fn parity_reorder_100_102_101() {
    let records = vec![
        rec(0, 100, 100 * 960, 100 * FRAME, 100, payload_toc_20ms()),
        rec(0, 102, 102 * 960, 102 * FRAME, 102, payload_toc_20ms()),
        rec(0, 101, 101 * 960, 101 * FRAME, 101, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[100, 101, 102]);
}

#[test]
fn parity_forward_gap_gt_reorder_then_continue() {
    let mut records = vec![rec(0, 0, 0, 0, 0, payload_toc_20ms())];
    for i in 40..60u16 {
        records.push(rec(
            0,
            i,
            u32::from(i) * 960,
            u64::from(i) * FRAME,
            u64::from(i),
            payload_toc_20ms(),
        ));
    }
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    let decodes = got
        .iter()
        .filter(|r| r.disposition == PacketDisposition::Decode)
        .count();
    assert_eq!(decodes, records.len());
    assert!(
        !got.iter()
            .any(|r| r.disposition == PacketDisposition::DiscardedOutOfWindow),
        "continuing run after gap: {:?}",
        got
    );
}

#[test]
fn parity_explicit_ssrc_epoch_restart() {
    let records = vec![
        rec(0, 0, 0, 0, 1, payload_toc_20ms()),
        rec(0, 1, 960, FRAME, 2, payload_toc_20ms()),
        rec(1, 0, 1920, 2 * FRAME, 3, payload_toc_20ms()),
        rec(1, 1, 2880, 3 * FRAME, 4, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[0, 1, 0, 1]);
    assert_eq!(acc.effective_epoch(), 1);
}

#[test]
fn parity_same_ssrc_implicit_generation_restart() {
    let prior_end = 28931 * FRAME;
    let records = vec![
        rec(
            0,
            28930,
            (28930 * 960) as u32,
            prior_end - 2 * FRAME,
            100 * NS_PER_FRAME,
            payload_toc_20ms(),
        ),
        rec(
            0,
            28931,
            (28931 * 960) as u32,
            prior_end - FRAME,
            101 * NS_PER_FRAME,
            payload_toc_20ms(),
        ),
        rec(
            0,
            120,
            5000,
            prior_end + FRAME,
            102 * NS_PER_FRAME,
            payload_toc_20ms(),
        ),
        rec(
            0,
            121,
            5000 + 960,
            prior_end + 2 * FRAME,
            103 * NS_PER_FRAME,
            payload_toc_20ms(),
        ),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[28930, 28931, 120, 121]);
    assert_eq!(acc.inferred_generation_count(), 1);
    assert_eq!(acc.effective_epoch(), 1);
}

#[test]
fn parity_rtp_u32_wrap_forward() {
    let prev_rtp = 0xffff_ffffu32 - 480;
    let next_rtp = 480u32;
    let records = vec![
        rec(
            0,
            10,
            prev_rtp,
            10 * FRAME,
            NS_PER_FRAME,
            payload_toc_20ms(),
        ),
        rec(
            0,
            11,
            next_rtp,
            11 * FRAME,
            2 * NS_PER_FRAME,
            payload_toc_20ms(),
        ),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[10, 11]);
    let t1 = got[0].timeline_sample_start.expect("t0");
    let t2 = got[1].timeline_sample_start.expect("t1");
    assert!(t2 > t1);
    assert!(
        t2 - t1 <= FRAME * 64,
        "u32 wrap should not create multi-minute placement gap: {}",
        t2 - t1
    );
}

#[test]
fn parity_exact_duplicate_and_conflict() {
    let base = rec(0, 5, 5 * 960, 5 * FRAME, 5, payload_toc_20ms());
    let dup = base.clone();
    let conflict = rec(0, 5, 5 * 960, 5 * FRAME, 5, vec![0x08, 0x02]);
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let r1 = acc.ingest_timeline(base).expect("first");
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].disposition, PacketDisposition::Decode);
    let r2 = acc.ingest_timeline(dup).expect("dup");
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].disposition, PacketDisposition::DiscardedDuplicate);
    let err = acc.ingest_timeline(conflict).unwrap_err();
    assert!(err.is_conflict());
}

#[test]
fn parity_loss_marker_among_valid_packets() {
    let records = vec![
        rec(0, 0, 0, 0, 1, payload_toc_20ms()),
        rec(0, 1, 960, FRAME, 2, payload_framing_skip()),
        rec(0, 2, 1920, 2 * FRAME, 3, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[0, 1, 2]);
    assert_eq!(got[1].disposition, PacketDisposition::LostPlacement);
}

#[test]
fn parity_overlap_placement_refused() {
    let records = vec![
        rec(0, 1, 0, 0, 1, payload_toc_20ms()),
        rec(0, 2, 480, 0, 2, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    acc.ingest_timeline(records[0].clone()).expect("first");
    let err = acc.ingest_timeline(records[1].clone()).unwrap_err();
    assert!(err.is_overlap());
}

#[test]
fn tolerant_overlap_emits_discarded_overlap_without_advancing_timeline() {
    let records = vec![
        rec(0, 1, 0, 0, 1, payload_toc_20ms()),
        rec(0, 2, 480, 0, 2, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new_with_placement_policy(
        USER,
        TARGET,
        AccumulatorPlacementPolicy::PlacementOverlapTolerant,
    );
    let first = acc.ingest_timeline(records[0].clone()).expect("first");
    assert_eq!(first[0].disposition, PacketDisposition::Decode);
    let second = acc.ingest_timeline(records[1].clone()).expect("overlap");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].disposition, PacketDisposition::DiscardedOverlap);
    assert_eq!(second[0].timeline_sample_start, Some(480));
    assert_eq!(second[0].span_samples, Some(FRAME));
    let third = rec(0, 3, 960, 0, 3, payload_toc_20ms());
    let after = acc.ingest_timeline(third).expect("continues");
    assert!(after
        .iter()
        .any(|p| p.disposition == PacketDisposition::Decode && p.record.sequence == 3));
}

#[test]
fn tolerant_overlap_updates_last_emitted_before_next_discontinuity() {
    let prev_start = 10_000u64 * FRAME;
    let base_seq = 100u16;
    let rtp_first = u32::try_from(prev_start).expect("rtp fits u32");
    let first = rec(0, base_seq, rtp_first, prev_start, 1, payload_toc_20ms());
    let overlap = rec(
        0,
        base_seq + 1,
        rtp_first + 480,
        prev_start + 480,
        2,
        payload_toc_20ms(),
    );
    // Same RTP step as production overlap recovery (+960): discontinuity consults
    // last-emitted capture/sequence after the overlap packet was dropped.
    let next = rec(
        0,
        base_seq + 2,
        rtp_first + 960,
        prev_start + FRAME,
        NS_PER_FRAME * 3,
        payload_toc_20ms(),
    );
    let mut acc = UserPacketAccumulator::new_with_placement_policy(
        USER,
        TARGET,
        AccumulatorPlacementPolicy::PlacementOverlapTolerant,
    );
    let r1 = acc.ingest_timeline(first).expect("first");
    assert_eq!(r1[0].disposition, PacketDisposition::Decode);
    assert_eq!(r1[0].timeline_sample_start, Some(prev_start));
    let r2 = acc.ingest_timeline(overlap).expect("overlap");
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].disposition, PacketDisposition::DiscardedOverlap);
    let r3 = acc.ingest_timeline(next).expect("after overlap identity");
    let decoded = r3
        .iter()
        .find(|p| p.record.sequence == base_seq + 2)
        .expect("next packet emitted");
    assert_eq!(decoded.disposition, PacketDisposition::Decode);
    assert_eq!(decoded.timeline_sample_start, Some(prev_start + FRAME));
    assert!(
        !r3.iter()
            .any(|p| p.disposition == PacketDisposition::DiscardedOverlap),
        "overlap drop must not cascade silence via a second overlap discard"
    );
}

#[derive(Deserialize)]
struct FixtureFile {
    scenario: String,
    records: Vec<FixtureRecord>,
    #[serde(default)]
    want_emitted_sequences: Vec<u16>,
}

#[derive(Deserialize)]
struct FixtureRecord {
    ssrc_epoch: u32,
    sequence: u16,
    rtp_timestamp: u32,
    capture: u64,
    arrived_at_ns: u64,
    payload_kind: String,
}

fn load_fixture(name: &str) -> FixtureFile {
    let path = Path::new("tests/fixtures/accumulator_parity").join(name);
    let raw = fs::read_to_string(path).unwrap();
    serde_json::from_str(&raw).unwrap()
}

fn payload_for_kind(kind: &str) -> Vec<u8> {
    match kind {
        "toc20ms" => payload_toc_20ms(),
        "framingSkip" => payload_framing_skip(),
        other => panic!("unknown payload kind {}", other),
    }
}

#[test]
fn parity_implicit_boundary_flush_emits_buffered_before_new_generation() {
    let prior_end = 100 * FRAME;
    let records = vec![
        rec(0, 0, 0, 0, 1, payload_toc_20ms()),
        rec(0, 100, 100 * 960, prior_end, 100, payload_toc_20ms()),
        rec(
            0,
            50,
            40_000_000,
            prior_end + FRAME,
            2_020_000_000,
            payload_toc_20ms(),
        ),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[0, 100, 50]);
    assert_eq!(got[0].extended_sequence, 0);
    assert_eq!(got[1].extended_sequence, 100);
    assert_eq!(got[2].extended_sequence, 50);
}

#[test]
fn parity_reverse_late_65535_after_seq0_extended_negative_one() {
    let records = vec![
        rec(0, 0, 0, 0, 1, payload_toc_20ms()),
        rec(0, 65535, 960, 65535 * FRAME, 2, payload_toc_20ms()),
    ];
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &[0, 65535]);
    assert_eq!(got[1].extended_sequence, -1);
}

#[test]
fn parity_drops_decode_past_target_sample_count() {
    let small_target = 1000u64;
    let records = vec![rec(0, 0, 0, 900, 1, payload_toc_20ms())];
    let mut acc = UserPacketAccumulator::new(USER, small_target);
    let got = ingest_all(&mut acc, &records);
    assert!(
        got.iter()
            .all(|r| r.disposition != PacketDisposition::Decode),
        "out-of-target packet must not surface as Decode: {:?}",
        got
    );
}

#[test]
fn flush_remaining_returns_overlap_error_fail_closed() {
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    acc.ingest_timeline(rec(0, 0, 0, 0, 1, payload_toc_20ms()))
        .expect("first");
    acc.ingest_timeline(rec(0, 5, 480, 0, 5, payload_toc_20ms()))
        .expect("buffer seq 5");
    let flush_err = acc.flush_remaining().unwrap_err();
    assert!(flush_err.is_overlap());
}

#[test]
fn parity_eight_packet_wrap_near_u16_rollover() {
    let mut records = Vec::new();
    let mut seq = 65530u16;
    let mut capture = 0u64;
    for i in 0..8u64 {
        records.push(rec(
            1,
            seq,
            (capture as u32) * 960,
            capture,
            i + 1,
            payload_toc_20ms(),
        ));
        capture += FRAME;
        seq = seq.wrapping_add(1);
    }
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    let decodes = got
        .iter()
        .filter(|r| r.disposition == PacketDisposition::Decode)
        .count();
    assert_eq!(decodes, 8, "got {:?}", got);
}

#[test]
fn parity_wrap_epoch_transition_emits_epoch_two() {
    let mut records = Vec::new();
    let mut seq = 65530u16;
    let mut capture = 0u64;
    for i in 0..8u64 {
        records.push(rec(
            1,
            seq,
            (capture as u32) * 960,
            capture,
            i + 1,
            payload_toc_20ms(),
        ));
        capture += FRAME;
        seq = seq.wrapping_add(1);
    }
    records.push(rec(
        2,
        0,
        (capture as u32) * 960,
        capture,
        9,
        payload_toc_20ms(),
    ));
    records.push(rec(
        2,
        1,
        ((capture + FRAME) as u32) * 960,
        capture + FRAME,
        10,
        payload_toc_20ms(),
    ));
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    let decodes = got
        .iter()
        .filter(|r| r.disposition == PacketDisposition::Decode)
        .count();
    assert_eq!(decodes, 10, "got {:?}", got);
}

#[test]
fn sample_start_plus_frame_span_overflow_pathological() {
    let target = u64::MAX;
    let start = u64::MAX - 400;
    let mut acc = UserPacketAccumulator::new(USER, target);
    let err = acc
        .ingest_timeline(rec(0, 0, 0, start, 1, payload_toc_20ms()))
        .unwrap_err();
    assert!(
        matches!(err, AccumulatorError::Pathological(_)),
        "expected pathological overflow, got {:?}",
        err
    );
}

#[test]
fn prev_timeline_plus_rtp_delta_overflow_pathological() {
    let target = u64::MAX;
    let anchor = u64::MAX - 5000;
    let mut acc = UserPacketAccumulator::new(USER, target);
    acc.ingest_timeline(rec(0, 0, 0, anchor, 1, payload_toc_20ms()))
        .expect("anchor packet");
    let err = acc
        .ingest_timeline(rec(0, 1, 6000, anchor, 2, payload_toc_20ms()))
        .unwrap_err();
    assert!(
        matches!(err, AccumulatorError::Pathological(_)),
        "expected pathological timeline overflow, got {:?}",
        err
    );
}

#[test]
fn parity_json_fixture_seq_wrap() {
    let fx = load_fixture("seq_wrap.json");
    let records: Vec<JournalRecord> = fx
        .records
        .iter()
        .map(|r| {
            rec(
                r.ssrc_epoch,
                r.sequence,
                r.rtp_timestamp,
                r.capture,
                r.arrived_at_ns,
                payload_for_kind(&r.payload_kind),
            )
        })
        .collect();
    let mut acc = UserPacketAccumulator::new(USER, TARGET);
    let got = ingest_all(&mut acc, &records);
    assert_emitted_sequences(&got, &fx.want_emitted_sequences);
}
