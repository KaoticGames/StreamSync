//! Read-only local journal forensics (framing + timeline metadata only; no Opus decode).

use super::accumulator::{lost_marker_span_samples, PacketDisposition, UserPacketAccumulator};
use super::manifest::{derive_voice_v2_opaque_stem_id, parse_local_finalization_manifest};
use super::synj::{decode_journal_record, read_file_header, JournalFramingError, JournalRecord};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

const SAMPLE_RATE_HZ: u64 = 48_000;
const OPUS_FRAME_SAMPLES: u64 = 960;

#[derive(Debug, Error)]
pub enum JournalForensicsError {
    #[error("forensics io: {0}")]
    Io(#[from] std::io::Error),
    #[error("forensics: {0}")]
    Other(String),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FramingIssueRef {
    pub segment_seq: u32,
    pub record_index_in_segment: u32,
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentForensics {
    pub segment_seq: u32,
    pub path: String,
    pub present: bool,
    pub format_version: Option<u32>,
    pub records_decoded: u32,
    pub fatal_framing: Vec<FramingIssueRef>,
    pub recoverable_framing: Vec<FramingIssueRef>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EpochTransitionRef {
    pub global_record_index: u64,
    pub segment_seq: u32,
    pub record_index_in_segment: u32,
    pub from_ssrc_epoch: u32,
    pub to_ssrc_epoch: u32,
    pub sequence: u16,
    pub rtp_timestamp: u32,
    pub session_capture_offset_samples: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureGapBucketCounts {
    pub under_20ms: u32,
    pub ms_20_to_100: u32,
    pub ms_100_to_1000: u32,
    pub over_1000ms: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccumulatorDispositionCounts {
    pub decode: u64,
    pub lost_placement: u64,
    pub discarded_duplicate: u64,
    pub discarded_conflict: u64,
    pub discarded_out_of_window: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserJournalForensics {
    pub discord_user_id: String,
    pub path_nick: String,
    pub opaque_stem_id: String,
    pub wav_relative_path: String,
    pub total_records: u64,
    pub nonempty_payload_records: u64,
    pub empty_payload_markers: u64,
    pub distinct_ssrc_epochs: u32,
    pub epoch_transitions: Vec<EpochTransitionRef>,
    pub capture_offset_monotonic_violations: u32,
    pub capture_gap_buckets: CaptureGapBucketCounts,
    pub max_capture_gap_samples: u64,
    pub arrival_reorder_events: u32,
    pub arrival_reorder_max_depth: u16,
    pub sequence_gap_events: u32,
    pub accumulator: AccumulatorDispositionCounts,
    pub first_emitted_capture_sample: Option<u64>,
    pub last_emitted_capture_end_sample: Option<u64>,
    pub predicted_trailing_silence_samples: u64,
    pub local_epoch_frontier_stall: bool,
    pub journal_capture_max_after_first_epoch_transition: Option<u64>,
    pub emitted_capture_max_after_first_epoch_transition: Option<u64>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SilenceCauseEntry {
    pub cause: String,
    pub weight: u64,
    pub affected_path_nicks: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalJournalForensicsReport {
    pub session_root: String,
    pub session_id: String,
    pub guild_id: String,
    pub channel_id: String,
    pub target_sample_count_48k: u64,
    pub manifest_valid: bool,
    pub manifest_warnings: Vec<String>,
    pub segments: Vec<SegmentForensics>,
    pub users: Vec<UserJournalForensics>,
    pub session_framing_fatal_count: u32,
    pub session_framing_recoverable_count: u32,
    pub silence_cause_ranking: Vec<SilenceCauseEntry>,
}

#[derive(Debug, Clone)]
struct RecordRef {
    global_index: u64,
    segment_seq: u32,
    record_index_in_segment: u32,
    meta: JournalRecordMeta,
}

#[derive(Debug, Clone)]
struct JournalRecordMeta {
    discord_user_id: String,
    ssrc_epoch: u32,
    sequence: u16,
    rtp_timestamp: u32,
    arrived_at_ns: u64,
    session_capture_offset_samples: Option<u64>,
    payload_len: u16,
}

impl From<&JournalRecord> for JournalRecordMeta {
    fn from(r: &JournalRecord) -> Self {
        Self {
            discord_user_id: r.discord_user_id.clone(),
            ssrc_epoch: r.ssrc_epoch,
            sequence: r.sequence,
            rtp_timestamp: r.rtp_timestamp,
            arrived_at_ns: r.arrived_at_ns,
            session_capture_offset_samples: r.session_capture_offset_samples,
            payload_len: r.opus_payload.len().min(u16::MAX as usize) as u16,
        }
    }
}

fn journal_record_from_meta(meta: &JournalRecordMeta) -> JournalRecord {
    let payload_len = meta.payload_len as usize;
    JournalRecord {
        discord_user_id: meta.discord_user_id.clone(),
        ssrc_epoch: meta.ssrc_epoch,
        sequence: meta.sequence,
        rtp_timestamp: meta.rtp_timestamp,
        arrived_at_ns: meta.arrived_at_ns,
        session_capture_offset_samples: meta.session_capture_offset_samples,
        opus_payload: if payload_len == 0 {
            Vec::new()
        } else {
            vec![0xAB; payload_len]
        },
    }
}

pub fn analyze_local_journal_session(
    session_root: &Path,
) -> Result<LocalJournalForensicsReport, JournalForensicsError> {
    let manifest_path = session_root.join("manifest.json");
    let raw = fs::read_to_string(&manifest_path)?;
    let value: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| JournalForensicsError::Other(e.to_string()))?;
    let manifest = parse_local_finalization_manifest(&value)
        .map_err(|e| JournalForensicsError::Other(e.to_string()))?;

    let mut manifest_warnings = Vec::new();
    if session_root
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|name| name != manifest.session_id)
    {
        manifest_warnings.push(format!(
            "session root directory name does not match manifest sessionId ({})",
            manifest.session_id
        ));
    }

    let mut segments_out = Vec::new();
    let mut all_records: Vec<RecordRef> = Vec::new();
    let mut global_index = 0u64;
    let mut session_fatal = 0u32;
    let mut session_recoverable = 0u32;

    let sorted_segments: Vec<_> = manifest.segments.iter().collect();
    for seg in sorted_segments {
        let rel = format!("journal/segments/{:08}.synj", seg.segment_seq);
        let path = session_root.join(&rel);
        let present = path.is_file();
        let mut seg_row = SegmentForensics {
            segment_seq: seg.segment_seq,
            path: rel.clone(),
            present,
            format_version: None,
            records_decoded: 0,
            fatal_framing: Vec::new(),
            recoverable_framing: Vec::new(),
        };
        if !present {
            manifest_warnings.push(format!("missing segment file: {}", rel));
            segments_out.push(seg_row);
            continue;
        }
        let data = fs::read(&path)?;
        let format_version = match read_file_header(&data) {
            Ok(v) => v,
            Err(JournalFramingError::Fatal(detail)) => {
                session_fatal += 1;
                seg_row.fatal_framing.push(FramingIssueRef {
                    segment_seq: seg.segment_seq,
                    record_index_in_segment: 0,
                    kind: "fatal".into(),
                    detail,
                });
                segments_out.push(seg_row);
                continue;
            }
            Err(JournalFramingError::Recoverable(detail)) => {
                session_recoverable += 1;
                seg_row.recoverable_framing.push(FramingIssueRef {
                    segment_seq: seg.segment_seq,
                    record_index_in_segment: 0,
                    kind: "recoverable".into(),
                    detail,
                });
                segments_out.push(seg_row);
                continue;
            }
        };
        seg_row.format_version = Some(format_version);
        let mut offset = 8usize;
        let mut record_index_in_segment = 0u32;
        while offset < data.len() {
            match decode_journal_record(&data, offset, format_version) {
                Ok((record, next)) => {
                    let meta = JournalRecordMeta::from(&record);
                    all_records.push(RecordRef {
                        global_index,
                        segment_seq: seg.segment_seq,
                        record_index_in_segment,
                        meta,
                    });
                    global_index += 1;
                    record_index_in_segment += 1;
                    seg_row.records_decoded += 1;
                    offset = next;
                }
                Err(JournalFramingError::Fatal(detail)) => {
                    session_fatal += 1;
                    seg_row.fatal_framing.push(FramingIssueRef {
                        segment_seq: seg.segment_seq,
                        record_index_in_segment,
                        kind: "fatal".into(),
                        detail,
                    });
                    break;
                }
                Err(JournalFramingError::Recoverable(detail)) => {
                    session_recoverable += 1;
                    seg_row.recoverable_framing.push(FramingIssueRef {
                        segment_seq: seg.segment_seq,
                        record_index_in_segment,
                        kind: "recoverable".into(),
                        detail,
                    });
                    break;
                }
            }
        }
        segments_out.push(seg_row);
    }

    let user_map: BTreeMap<String, (String, String)> = manifest
        .users
        .iter()
        .map(|u| {
            (
                u.discord_user_id.clone(),
                (u.path_nick.clone(), u.opaque_stem_id.clone()),
            )
        })
        .collect();

    let mut per_user_records: BTreeMap<String, Vec<&RecordRef>> = BTreeMap::new();
    for rec in &all_records {
        per_user_records
            .entry(rec.meta.discord_user_id.clone())
            .or_default()
            .push(rec);
    }

    let target = manifest.target_sample_count_48k;
    let mut users_out = Vec::new();
    for user in &manifest.users {
        let rows = per_user_records
            .get(&user.discord_user_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        users_out.push(analyze_user(
            user.discord_user_id.clone(),
            user.path_nick.clone(),
            user.opaque_stem_id.clone(),
            rows,
            target,
        ));
    }

    for (discord_user_id, rows) in &per_user_records {
        if !user_map.contains_key(discord_user_id) {
            let (path_nick, opaque) = (format!("unknown-{}", discord_user_id), "unknown".into());
            users_out.push(analyze_user(
                discord_user_id.clone(),
                path_nick,
                opaque,
                rows,
                target,
            ));
            manifest_warnings.push(format!(
                "journal records for discordUserId not listed in manifest: {}",
                discord_user_id
            ));
        }
    }
    users_out.sort_by(|a, b| a.path_nick.cmp(&b.path_nick));

    let silence_cause_ranking = rank_silence_causes(&users_out);

    Ok(LocalJournalForensicsReport {
        session_root: session_root.display().to_string(),
        session_id: manifest.session_id.clone(),
        guild_id: manifest.guild_id.clone(),
        channel_id: manifest.channel_id.clone(),
        target_sample_count_48k: target,
        manifest_valid: true,
        manifest_warnings,
        segments: segments_out,
        users: users_out,
        session_framing_fatal_count: session_fatal,
        session_framing_recoverable_count: session_recoverable,
        silence_cause_ranking,
    })
}

fn analyze_user(
    discord_user_id: String,
    path_nick: String,
    opaque_stem_id: String,
    rows: &[&RecordRef],
    target_samples: u64,
) -> UserJournalForensics {
    let mut total_records = 0u64;
    let mut nonempty = 0u64;
    let mut empty_markers = 0u64;
    let mut epochs = BTreeMap::new();
    let mut epoch_transitions = Vec::new();
    let mut last_epoch: Option<u32> = None;
    let mut capture_violations = 0u32;
    let mut gap_buckets = CaptureGapBucketCounts {
        under_20ms: 0,
        ms_20_to_100: 0,
        ms_100_to_1000: 0,
        over_1000ms: 0,
    };
    let mut max_gap = 0u64;
    let mut last_capture: Option<u64> = None;
    let mut reorder_events = 0u32;
    let mut reorder_max_depth = 0u16;
    let mut last_seq_by_epoch: BTreeMap<u32, u16> = BTreeMap::new();
    let mut seq_gap_events = 0u32;

    for rec in rows {
        total_records += 1;
        if rec.meta.payload_len == 0 {
            empty_markers += 1;
        } else {
            nonempty += 1;
        }
        epochs.insert(rec.meta.ssrc_epoch, ());
        if let Some(prev) = last_epoch {
            if rec.meta.ssrc_epoch != prev {
                epoch_transitions.push(EpochTransitionRef {
                    global_record_index: rec.global_index,
                    segment_seq: rec.segment_seq,
                    record_index_in_segment: rec.record_index_in_segment,
                    from_ssrc_epoch: prev,
                    to_ssrc_epoch: rec.meta.ssrc_epoch,
                    sequence: rec.meta.sequence,
                    rtp_timestamp: rec.meta.rtp_timestamp,
                    session_capture_offset_samples: rec.meta.session_capture_offset_samples,
                });
            }
        }
        last_epoch = Some(rec.meta.ssrc_epoch);

        if let Some(cap) = rec.meta.session_capture_offset_samples {
            if let Some(prev_cap) = last_capture {
                if cap < prev_cap {
                    capture_violations += 1;
                } else if cap > prev_cap {
                    let gap = cap - prev_cap;
                    if gap > max_gap {
                        max_gap = gap;
                    }
                    bucket_capture_gap(&mut gap_buckets, gap);
                }
            }
            last_capture = Some(cap);
        }

        if let Some(prev_seq) = last_seq_by_epoch.get(&rec.meta.ssrc_epoch) {
            let delta = rec.meta.sequence.wrapping_sub(*prev_seq);
            if delta > 1 && delta < 0x8000 {
                seq_gap_events += 1;
            }
            if rec.meta.sequence < *prev_seq && prev_seq.wrapping_sub(rec.meta.sequence) < 0x8000 {
                reorder_events += 1;
                let depth = prev_seq.wrapping_sub(rec.meta.sequence);
                if depth > reorder_max_depth {
                    reorder_max_depth = depth;
                }
            }
        }
        last_seq_by_epoch.insert(rec.meta.ssrc_epoch, rec.meta.sequence);
    }

    let mut acc = UserPacketAccumulator::default();
    let mut counts = AccumulatorDispositionCounts {
        decode: 0,
        lost_placement: 0,
        discarded_duplicate: 0,
        discarded_conflict: 0,
        discarded_out_of_window: 0,
    };
    let mut first_emitted: Option<u64> = None;
    let mut last_emitted_end: Option<u64> = None;
    let first_transition_index = epoch_transitions.first().map(|t| t.global_record_index);
    let first_epoch = epochs.keys().next().copied();
    let mut emitted_capture_max_after_transition: Option<u64> = None;
    let mut emitted_in_later_epochs = 0u64;

    for rec in rows {
        let jr = journal_record_from_meta(&rec.meta);
        let ready = acc.ingest(jr);
        for pkt in ready {
            track_emitted(
                &pkt,
                &mut acc,
                &mut counts,
                &mut first_emitted,
                &mut last_emitted_end,
                first_transition_index,
                first_epoch,
                &mut emitted_capture_max_after_transition,
                &mut emitted_in_later_epochs,
            );
        }
    }
    for pkt in acc.flush_remaining() {
        track_emitted(
            &pkt,
            &mut acc,
            &mut counts,
            &mut first_emitted,
            &mut last_emitted_end,
            first_transition_index,
            first_epoch,
            &mut emitted_capture_max_after_transition,
            &mut emitted_in_later_epochs,
        );
    }

    let mut journal_capture_max_after_transition: Option<u64> = None;
    let mut later_epoch_nonempty = 0u64;
    if let Some(tidx) = first_transition_index {
        for rec in rows {
            if rec.global_index >= tidx {
                if let Some(cap) = rec.meta.session_capture_offset_samples {
                    journal_capture_max_after_transition =
                        Some(journal_capture_max_after_transition.map_or(cap, |m| m.max(cap)));
                }
                if Some(rec.meta.ssrc_epoch) != first_epoch && rec.meta.payload_len > 0 {
                    later_epoch_nonempty += 1;
                }
            }
        }
    }

    let predicted_trailing = match last_emitted_end {
        Some(end) if end < target_samples => target_samples - end,
        _ => 0,
    };

    let mut local_epoch_frontier_stall = false;
    let mut diagnostics = Vec::new();
    if !epoch_transitions.is_empty() && later_epoch_nonempty > 0 && emitted_in_later_epochs == 0 {
        local_epoch_frontier_stall = true;
        diagnostics.push("LOCAL_EPOCH_FRONTIER_STALL".into());
    }

    let wav_relative_path = format!("{path_nick}.wav");
    UserJournalForensics {
        discord_user_id,
        path_nick,
        opaque_stem_id,
        wav_relative_path,
        total_records,
        nonempty_payload_records: nonempty,
        empty_payload_markers: empty_markers,
        distinct_ssrc_epochs: epochs.len() as u32,
        epoch_transitions,
        capture_offset_monotonic_violations: capture_violations,
        capture_gap_buckets: gap_buckets,
        max_capture_gap_samples: max_gap,
        arrival_reorder_events: reorder_events,
        arrival_reorder_max_depth: reorder_max_depth,
        sequence_gap_events: seq_gap_events,
        accumulator: counts,
        first_emitted_capture_sample: first_emitted,
        last_emitted_capture_end_sample: last_emitted_end,
        predicted_trailing_silence_samples: predicted_trailing,
        local_epoch_frontier_stall,
        journal_capture_max_after_first_epoch_transition: journal_capture_max_after_transition,
        emitted_capture_max_after_first_epoch_transition: emitted_capture_max_after_transition,
        diagnostics,
    }
}

fn track_emitted(
    pkt: &super::accumulator::ReadyPacket,
    acc: &mut UserPacketAccumulator,
    counts: &mut AccumulatorDispositionCounts,
    first_emitted: &mut Option<u64>,
    last_emitted_end: &mut Option<u64>,
    first_transition_index: Option<u64>,
    first_epoch: Option<u32>,
    emitted_capture_max_after_transition: &mut Option<u64>,
    emitted_in_later_epochs: &mut u64,
) {
    let placement = acc.placement_samples(&pkt.record);
    let end = placement + span_for_disposition(pkt.disposition);
    if first_emitted.is_none() {
        *first_emitted = Some(placement);
    }
    *last_emitted_end = Some(last_emitted_end.map_or(end, |e| e.max(end)));
    bump_disposition(counts, pkt.disposition);

    if first_transition_index.is_some() && Some(pkt.record.ssrc_epoch) != first_epoch {
        if matches!(
            pkt.disposition,
            PacketDisposition::Decode | PacketDisposition::LostPlacement
        ) {
            *emitted_in_later_epochs += 1;
            *emitted_capture_max_after_transition =
                Some(emitted_capture_max_after_transition.map_or(end, |m| m.max(end)));
        }
    }
}

fn span_for_disposition(d: PacketDisposition) -> u64 {
    match d {
        PacketDisposition::Decode => OPUS_FRAME_SAMPLES,
        PacketDisposition::LostPlacement => lost_marker_span_samples(),
        PacketDisposition::DiscardedDuplicate
        | PacketDisposition::DiscardedConflict
        | PacketDisposition::DiscardedOutOfWindow => 0,
    }
}

fn bump_disposition(counts: &mut AccumulatorDispositionCounts, d: PacketDisposition) {
    match d {
        PacketDisposition::Decode => counts.decode += 1,
        PacketDisposition::LostPlacement => counts.lost_placement += 1,
        PacketDisposition::DiscardedDuplicate => counts.discarded_duplicate += 1,
        PacketDisposition::DiscardedConflict => counts.discarded_conflict += 1,
        PacketDisposition::DiscardedOutOfWindow => counts.discarded_out_of_window += 1,
    }
}

fn bucket_capture_gap(buckets: &mut CaptureGapBucketCounts, gap_samples: u64) {
    let ms = gap_samples * 1000 / SAMPLE_RATE_HZ;
    if ms < 20 {
        buckets.under_20ms += 1;
    } else if ms < 100 {
        buckets.ms_20_to_100 += 1;
    } else if ms < 1000 {
        buckets.ms_100_to_1000 += 1;
    } else {
        buckets.over_1000ms += 1;
    }
}

fn rank_silence_causes(users: &[UserJournalForensics]) -> Vec<SilenceCauseEntry> {
    let mut causes: BTreeMap<String, (u64, Vec<String>)> = BTreeMap::new();
    for u in users {
        if u.local_epoch_frontier_stall {
            let e = causes
                .entry("local_epoch_frontier_stall".into())
                .or_default();
            e.0 += u.predicted_trailing_silence_samples.max(1);
            e.1.push(u.path_nick.clone());
        }
        let algo_drops = u.accumulator.discarded_duplicate
            + u.accumulator.discarded_conflict
            + u.accumulator.discarded_out_of_window;
        if algo_drops > 0 {
            let e = causes
                .entry("local_reorder_and_window_drops".into())
                .or_default();
            e.0 += algo_drops * OPUS_FRAME_SAMPLES;
            e.1.push(u.path_nick.clone());
        }
        if u.empty_payload_markers > 0 {
            let e = causes
                .entry("journal_empty_payload_markers".into())
                .or_default();
            e.0 += u.empty_payload_markers * lost_marker_span_samples();
            e.1.push(u.path_nick.clone());
        }
        let gap_weight = u.capture_gap_buckets.over_1000ms as u64 * OPUS_FRAME_SAMPLES
            + u.max_capture_gap_samples;
        if gap_weight > OPUS_FRAME_SAMPLES {
            let e = causes.entry("capture_timeline_gaps".into()).or_default();
            e.0 += gap_weight;
            e.1.push(u.path_nick.clone());
        }
        if u.predicted_trailing_silence_samples > OPUS_FRAME_SAMPLES
            && !u.local_epoch_frontier_stall
        {
            let e = causes.entry("predicted_tail_padding".into()).or_default();
            e.0 += u.predicted_trailing_silence_samples;
            e.1.push(u.path_nick.clone());
        }
    }
    let mut ranking: Vec<SilenceCauseEntry> = causes
        .into_iter()
        .map(|(cause, (weight, nicks))| {
            let mut affected_path_nicks = nicks;
            affected_path_nicks.sort();
            affected_path_nicks.dedup();
            SilenceCauseEntry {
                cause,
                weight,
                affected_path_nicks,
            }
        })
        .collect();
    ranking.sort_by(|a, b| b.weight.cmp(&a.weight).then_with(|| a.cause.cmp(&b.cause)));
    ranking
}

pub fn render_forensics_table(report: &LocalJournalForensicsReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Session {} ({})\n",
        report.session_id, report.session_root
    ));
    out.push_str(&format!(
        "Target samples: {} | Segments: {} | Framing fatal/recoverable: {}/{}\n\n",
        report.target_sample_count_48k,
        report.segments.len(),
        report.session_framing_fatal_count,
        report.session_framing_recoverable_count
    ));
    out.push_str(
        "pathNick          records  epochs  stall  acc(decode/lost/dup/conf/oow)  trailSilence\n",
    );
    for u in &report.users {
        out.push_str(&format!(
            "{:<16}  {:7}  {:6}  {:5}  {:>6}/{:>4}/{:>3}/{:>4}/{:>3}  {:>12}\n",
            u.path_nick,
            u.total_records,
            u.distinct_ssrc_epochs,
            if u.local_epoch_frontier_stall {
                "YES"
            } else {
                "no"
            },
            u.accumulator.decode,
            u.accumulator.lost_placement,
            u.accumulator.discarded_duplicate,
            u.accumulator.discarded_conflict,
            u.accumulator.discarded_out_of_window,
            u.predicted_trailing_silence_samples,
        ));
    }
    if !report.silence_cause_ranking.is_empty() {
        out.push_str("\nSilence cause ranking (weight):\n");
        for entry in &report.silence_cause_ranking {
            out.push_str(&format!(
                "  {} — {} ({:?})\n",
                entry.cause, entry.weight, entry.affected_path_nicks
            ));
        }
    }
    out
}

/// Serialize report to stable JSON (sorted keys via serde on structs + BTreeMap usage in analysis).
pub fn forensics_report_to_json(
    report: &LocalJournalForensicsReport,
) -> Result<String, JournalForensicsError> {
    serde_json::to_string_pretty(report).map_err(|e| JournalForensicsError::Other(e.to_string()))
}

/// Minimal manifest JSON for forensics unit/integration fixtures.
pub fn build_minimal_manifest_json(
    session_id: &str,
    users: &[(&str, &str)],
    target_samples: u64,
) -> String {
    let guild_id = "123456789012345678";
    let users_json = users
        .iter()
        .map(|(discord, nick)| {
            let opaque = derive_voice_v2_opaque_stem_id(session_id, guild_id, discord, nick);
            format!(
                r#"{{"discordUserId":"{discord}","pathNick":"{nick}","opaqueStemId":"{opaque}"}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"{{
  "version": 1,
  "finalizerAlgorithmVersion": "syndicate-local-opus-finalizer-v1",
  "sessionId": "{session_id}",
  "guildId": "123456789012345678",
  "channelId": "234567890123456789",
  "sessionStartWallMs": 1700000000000,
  "sessionStartMonotonicNs": "1000000000",
  "sealedStopWallMs": 1700000360000,
  "sealedStopMonotonicNs": "4600000000000",
  "targetSampleCount48k": {target_samples},
  "journalSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "recordingManifest": {{
    "key": "voice-delivery-v2/123456789012345678/{session_id}/manifest/recording.manifest.json",
    "byteLength": 4096,
    "sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }},
  "sealedIndex": {{
    "key": "voice-delivery-v2/123456789012345678/{session_id}/journal/index.sealed.json",
    "byteLength": 512,
    "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }},
  "segments": [
    {{
      "segmentSeq": 0,
      "key": "voice-delivery-v2/123456789012345678/{session_id}/journal/00000000.synj",
      "byteLength": 1048576,
      "sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    }}
  ],
  "users": [{users_json}],
  "output": {{
    "wavFormat": "pcm_s16le_stereo_48000",
    "recoveryReportRelativePath": "Syndicate Audio Recovery Report.txt"
  }}
}}"#,
    )
}
