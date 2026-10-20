//! Single-pass journal demux + Opus decode into aligned stereo WAV stems.
//!
//! Each [`UserWriter`] streams PCM directly to its staging WAV (header written at open time)
//! with a 1 MiB [`std::io::BufWriter`] and incremental SHA-256 over the exact on-disk bytes.
//! Memory is bounded by per-writer buffers and the packet reorder window, not session duration.

use super::accumulator::{
    AccumulatorPlacementPolicy, PacketDisposition, ReadyPacket, UserPacketAccumulator,
};
use super::cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
use super::manifest::LocalFinalizationManifest;
use super::opus_decode::{OpusDecodeError, StereoOpusDecoder};
use super::opus_packet_duration::opus_packet_duration_samples;
use super::packet_loss_policy::{
    BoundedPacketLossError, BoundedPacketLossTracker, LocalPacketLossPolicySummary,
    PacketLossSource,
};
use super::report::{
    DiscardReason, SessionRecoveryReportBuilder, StemRecoveryReportBuilder, StemSilenceReason,
};
use super::synj::{decode_journal_record, read_file_header, JournalFramingError, JournalRecord};
use crate::voice_delivery::bounds::STEREO_PCM_FRAME_BYTES;
use crate::voice_delivery::finalized_manifest::expected_wav_file_bytes;
use crate::voice_delivery::fs::durable_payload::{ensure_parent, write_durable_payload_at_path};
use crate::voice_delivery::hash::LiveSha256;
use crate::voice_delivery::wav::minimal_wav_header;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// Buffered file I/O for stem PCM; large enough to amortize syscall cost on Windows.
pub const STEM_FILE_BUF_CAPACITY: usize = 1024 * 1024;

/// Reusable zero block for timeline / tail silence (frame-aligned stereo s16).
pub const SILENCE_WRITE_CHUNK_BYTES: usize = 256 * 1024;

/// Emit [`LocalProcessorProgress`] to the orchestrator at most once per this many journal segments.
pub const PROCESSOR_PROGRESS_EVERY_SEGMENTS: u32 = 4;

/// Check cancel/pause every this many journal records within a segment.
const RECORD_INTERRUPT_CHECK_INTERVAL: u64 = 8192;

#[derive(Debug, Error)]
pub enum LocalProcessorError {
    #[error("processor fatal: {0}")]
    Fatal(String),
    #[error("processor io: {0}")]
    Io(#[from] std::io::Error),
    #[error("cancelled")]
    Cancelled,
    #[error("paused")]
    Paused,
}

#[derive(Debug)]
pub struct LocalProcessOutput {
    pub stem_paths: HashMap<String, PathBuf>,
    pub stem_sha256: HashMap<String, String>,
    pub report_path: Option<PathBuf>,
    pub report_sha256: Option<String>,
    pub report_bytes: Option<u64>,
    pub discarded_packet_count: u64,
    pub silence_range_count: u64,
    pub packet_loss_policy: LocalPacketLossPolicySummary,
}

#[derive(Clone, Default)]
pub struct LocalProcessorOptions {
    pub cancel: Option<LocalFinalizationCancel>,
    pub paused: Option<Arc<AtomicBool>>,
    pub progress: Option<Arc<dyn Fn(LocalProcessorProgress) + Send + Sync>>,
}

#[derive(Debug, Clone, Copy)]
pub struct LocalProcessorProgress {
    pub processed_segments: u32,
    pub total_segments: u32,
}

impl LocalProcessorOptions {
    fn check_interrupt(&self) -> Result<(), LocalProcessorError> {
        if let Some(cancel) = &self.cancel {
            cancel.check().map_err(|_| LocalProcessorError::Cancelled)?;
        }
        if self
            .paused
            .as_ref()
            .is_some_and(|p| p.load(Ordering::SeqCst))
        {
            return Err(LocalProcessorError::Paused);
        }
        Ok(())
    }

    fn maybe_emit_progress(&self, processed_segments: u32, total_segments: u32) {
        if processed_segments == 0 {
            return;
        }
        let emit = processed_segments == total_segments
            || processed_segments.is_multiple_of(PROCESSOR_PROGRESS_EVERY_SEGMENTS);
        if emit {
            if let Some(cb) = &self.progress {
                cb(LocalProcessorProgress {
                    processed_segments,
                    total_segments,
                });
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyPacketResult {
    Applied,
    DecodeFailed { recovery_span_samples: u64 },
}

struct UserWriter {
    path_nick: String,
    path: PathBuf,
    target_samples: u64,
    expected_file_bytes: u64,
    file: BufWriter<File>,
    sha: LiveSha256,
    silence_chunk: Vec<u8>,
    pcm_byte_scratch: Vec<u8>,
    decoder: StereoOpusDecoder,
    next_write_sample: u64,
    pcm_bytes_written: u64,
    report: StemRecoveryReportBuilder,
}

impl UserWriter {
    fn new(
        path_nick: String,
        discord_user_id: String,
        out_dir: &Path,
        target_samples: u64,
    ) -> Result<Self, LocalProcessorError> {
        let path = out_dir.join(format!("{}.wav", path_nick));
        let header = minimal_wav_header(target_samples * STEREO_PCM_FRAME_BYTES)
            .map_err(|e| LocalProcessorError::Fatal(e.to_string()))?;
        let expected_file_bytes = expected_wav_file_bytes(target_samples);
        let file = File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let mut buf = BufWriter::with_capacity(STEM_FILE_BUF_CAPACITY, file);
        buf.write_all(&header)?;
        let mut sha = LiveSha256::new();
        sha.update(&header);
        let decoder = StereoOpusDecoder::new()
            .map_err(|_| LocalProcessorError::Fatal("opus decoder init failed".into()))?;
        let silence_len = SILENCE_WRITE_CHUNK_BYTES.div_ceil(STEREO_PCM_FRAME_BYTES as usize)
            * STEREO_PCM_FRAME_BYTES as usize;
        let report = StemRecoveryReportBuilder::new(path_nick.clone(), discord_user_id);
        Ok(Self {
            path_nick,
            path,
            target_samples,
            expected_file_bytes,
            file: buf,
            sha,
            silence_chunk: vec![0u8; silence_len],
            pcm_byte_scratch: Vec::new(),
            decoder,
            next_write_sample: 0,
            pcm_bytes_written: 0,
            report,
        })
    }

    fn write_zero_pcm_bytes(&mut self, bytes: u64) -> Result<(), LocalProcessorError> {
        if bytes == 0 {
            return Ok(());
        }
        if !bytes.is_multiple_of(STEREO_PCM_FRAME_BYTES) {
            return Err(LocalProcessorError::Fatal(
                "silence write not frame aligned".into(),
            ));
        }
        let mut remaining = bytes;
        while remaining > 0 {
            let chunk = remaining.min(self.silence_chunk.len() as u64) as usize;
            self.file.write_all(&self.silence_chunk[..chunk])?;
            self.sha.update(&self.silence_chunk[..chunk]);
            self.pcm_bytes_written += chunk as u64;
            remaining -= chunk as u64;
        }
        Ok(())
    }

    fn pad_silence_to(
        &mut self,
        sample: u64,
        reason: StemSilenceReason,
        bad_packet_count: u64,
    ) -> Result<(), LocalProcessorError> {
        if sample <= self.next_write_sample {
            return Ok(());
        }
        let start = self.next_write_sample;
        let end = sample.min(self.target_samples);
        if end > start {
            self.report
                .record_silence_span(start, end, reason, bad_packet_count);
            let frames = end - start;
            self.write_zero_pcm_bytes(frames * STEREO_PCM_FRAME_BYTES)?;
            self.next_write_sample = end;
        }
        Ok(())
    }

    fn apply_packet(
        &mut self,
        record: &JournalRecord,
        disposition: PacketDisposition,
        placement: u64,
        span_samples: Option<u64>,
    ) -> Result<ApplyPacketResult, LocalProcessorError> {
        match disposition {
            PacketDisposition::DiscardedDuplicate => {
                self.report.record_discarded(DiscardReason::Duplicate);
                Ok(ApplyPacketResult::Applied)
            }
            PacketDisposition::DiscardedConflict => {
                self.report.record_discarded(DiscardReason::Conflict);
                Ok(ApplyPacketResult::Applied)
            }
            PacketDisposition::DiscardedOutOfWindow => {
                self.report.record_discarded(DiscardReason::OutOfWindow);
                Ok(ApplyPacketResult::Applied)
            }
            PacketDisposition::DiscardedOverlap => {
                self.report.record_discarded(DiscardReason::Overlap);
                Ok(ApplyPacketResult::Applied)
            }
            PacketDisposition::LostPlacement => {
                self.pad_silence_to(placement, StemSilenceReason::TimelineGap, 0)?;
                if let Some(span) = span_samples {
                    let end = placement
                        .checked_add(span)
                        .ok_or_else(|| {
                            LocalProcessorError::Fatal("journal placement span overflow".into())
                        })?
                        .min(self.target_samples);
                    self.pad_silence_to(end, StemSilenceReason::JournalLossMarker, 1)?;
                } else {
                    let end = placement
                        .checked_add(1)
                        .ok_or_else(|| {
                            LocalProcessorError::Fatal("journal placement span overflow".into())
                        })?
                        .min(self.target_samples);
                    self.pad_silence_to(end, StemSilenceReason::JournalLossMarker, 1)?;
                }
                Ok(ApplyPacketResult::Applied)
            }
            PacketDisposition::Decode => {
                if record.opus_payload.is_empty() {
                    self.report.record_discarded(DiscardReason::Other);
                    return Ok(ApplyPacketResult::Applied);
                }
                if placement >= self.target_samples {
                    self.report.record_discarded(DiscardReason::Other);
                    return Ok(ApplyPacketResult::Applied);
                }
                self.pad_silence_to(placement, StemSilenceReason::TimelineGap, 0)?;
                let mut pcm_buf = [0i16; 5760 * 2];
                match self.decoder.decode_packet(record, &mut pcm_buf) {
                    Ok(samples) => {
                        let frame_samples = samples as u64;
                        let start = self.next_write_sample;
                        let end = (start + frame_samples).min(self.target_samples);
                        let write_samples = end - start;
                        let i16_len = (write_samples * 2) as usize;
                        append_i16_pcm_le(&mut self.pcm_byte_scratch, &pcm_buf[..i16_len]);
                        let pcm_bytes = self.pcm_byte_scratch.as_slice();
                        self.file.write_all(pcm_bytes)?;
                        self.sha.update(pcm_bytes);
                        self.pcm_bytes_written += pcm_bytes.len() as u64;
                        self.next_write_sample = end;
                        Ok(ApplyPacketResult::Applied)
                    }
                    Err(OpusDecodeError::Unavailable) => Err(LocalProcessorError::Fatal(
                        "opus decoder unavailable".into(),
                    )),
                    Err(OpusDecodeError::Failed) => {
                        self.report.record_discarded(DiscardReason::Other);
                        let recovery_span = opus_packet_duration_samples(&record.opus_payload)
                            .map_err(|e| {
                                LocalProcessorError::Fatal(format!(
                                    "decode failure without TOC duration: {e}"
                                ))
                            })?;
                        if recovery_span > 0 {
                            let end = placement
                                .checked_add(recovery_span)
                                .ok_or_else(|| {
                                    LocalProcessorError::Fatal(
                                        "journal placement recovery span overflow".into(),
                                    )
                                })?
                                .min(self.target_samples);
                            self.pad_silence_to(end, StemSilenceReason::OpusDecodeFailed, 1)?;
                        }
                        Ok(ApplyPacketResult::DecodeFailed {
                            recovery_span_samples: recovery_span,
                        })
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn apply_packet_for_test(
        &mut self,
        record: &JournalRecord,
        disposition: PacketDisposition,
        placement: u64,
        span_samples: Option<u64>,
    ) -> Result<ApplyPacketResult, LocalProcessorError> {
        self.apply_packet(record, disposition, placement, span_samples)
    }

    fn finalize(
        mut self,
    ) -> Result<(PathBuf, String, StemRecoveryReportBuilder), LocalProcessorError> {
        self.pad_silence_to(self.target_samples, StemSilenceReason::TailPadding, 0)?;
        self.file.flush()?;
        self.file
            .get_mut()
            .sync_all()
            .map_err(LocalProcessorError::Io)?;
        let len = std::fs::metadata(&self.path)?.len();
        if len != self.expected_file_bytes {
            return Err(LocalProcessorError::Fatal(format!(
                "stem {} byte length {} != {}",
                self.path_nick, len, self.expected_file_bytes
            )));
        }
        let expected_pcm = self.target_samples * STEREO_PCM_FRAME_BYTES;
        if self.pcm_bytes_written != expected_pcm || self.next_write_sample != self.target_samples {
            return Err(LocalProcessorError::Fatal(format!(
                "stem {} pcm sample cursor {} bytes {} != target samples {} pcm bytes {}",
                self.path_nick,
                self.next_write_sample,
                self.pcm_bytes_written,
                self.target_samples,
                expected_pcm
            )));
        }
        let sha = self.sha.finish_hex();
        Ok((self.path, sha, self.report))
    }
}

fn append_i16_pcm_le(dst: &mut Vec<u8>, samples: &[i16]) {
    dst.clear();
    dst.reserve(samples.len() * 2);
    for sample in samples {
        dst.extend_from_slice(&sample.to_le_bytes());
    }
}

struct UserAccumulator {
    path_nick: String,
    writer: UserWriter,
    packets: UserPacketAccumulator,
}

impl UserAccumulator {
    fn ingest(
        &mut self,
        record: JournalRecord,
        tracker: &mut BoundedPacketLossTracker,
    ) -> Result<(), LocalProcessorError> {
        let ready = self
            .packets
            .ingest_timeline(record)
            .map_err(|e| LocalProcessorError::Fatal(format!("accumulator: {e:?}")))?;
        for pkt in ready {
            self.apply_ready_packet(&pkt, tracker)?;
        }
        Ok(())
    }

    fn apply_ready_packet(
        &mut self,
        pkt: &ReadyPacket,
        tracker: &mut BoundedPacketLossTracker,
    ) -> Result<(), LocalProcessorError> {
        use super::accumulator::PacketDisposition;
        match pkt.disposition {
            PacketDisposition::DiscardedDuplicate | PacketDisposition::DiscardedConflict => {
                self.writer
                    .apply_packet(&pkt.record, pkt.disposition, 0, None)?;
            }
            PacketDisposition::DiscardedOutOfWindow => {
                self.writer
                    .apply_packet(&pkt.record, pkt.disposition, 0, None)?;
                register_media_loss(tracker, &self.path_nick, pkt, PacketLossSource::OutOfWindow)?;
            }
            PacketDisposition::DiscardedOverlap => {
                self.writer
                    .apply_packet(&pkt.record, pkt.disposition, 0, None)?;
                register_media_loss(
                    tracker,
                    &self.path_nick,
                    pkt,
                    PacketLossSource::PlacementOverlap,
                )?;
            }
            PacketDisposition::Decode | PacketDisposition::LostPlacement => {
                let placement = pkt.timeline_sample_start.ok_or_else(|| {
                    LocalProcessorError::Fatal(format!(
                        "ready packet {:?} missing authoritative timeline",
                        pkt.disposition
                    ))
                })?;
                let outcome = self.writer.apply_packet(
                    &pkt.record,
                    pkt.disposition,
                    placement,
                    pkt.span_samples,
                )?;
                if pkt.disposition == PacketDisposition::LostPlacement {
                    register_media_loss(
                        tracker,
                        &self.path_nick,
                        pkt,
                        PacketLossSource::FramingOrLostPlacement,
                    )?;
                } else if matches!(outcome, ApplyPacketResult::DecodeFailed { .. }) {
                    register_media_loss(
                        tracker,
                        &self.path_nick,
                        pkt,
                        PacketLossSource::DecodeFailure,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn flush_ready(
        &mut self,
        tracker: &mut BoundedPacketLossTracker,
    ) -> Result<(), LocalProcessorError> {
        let remaining = self
            .packets
            .flush_remaining()
            .map_err(|e| LocalProcessorError::Fatal(format!("accumulator: {e:?}")))?;
        for pkt in remaining {
            self.apply_ready_packet(&pkt, tracker)?;
        }
        Ok(())
    }
}

fn register_media_loss(
    tracker: &mut BoundedPacketLossTracker,
    path_nick: &str,
    pkt: &ReadyPacket,
    source: PacketLossSource,
) -> Result<(), LocalProcessorError> {
    tracker
        .register_media_loss(
            &pkt.record.discord_user_id,
            pkt.effective_epoch,
            pkt.record.sequence,
            pkt.extended_sequence,
            source,
        )
        .map_err(|e| LocalProcessorError::Fatal(format_packet_loss_diagnostic(path_nick, pkt, e)))
}

fn format_packet_loss_diagnostic(
    path_nick: &str,
    pkt: &ReadyPacket,
    err: BoundedPacketLossError,
) -> String {
    let sample_start = pkt
        .timeline_sample_start
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none".into());
    let span = pkt
        .span_samples
        .map(|s| s.to_string())
        .unwrap_or_else(|| "none".into());
    format!(
        "pathNick={path_nick} user={} effectiveEpoch={} sequence={} extendedSequence={} sampleStart={} spanSamples={} rtp={} capture={} policy: {err}",
        pkt.record.discord_user_id,
        pkt.effective_epoch,
        pkt.record.sequence,
        pkt.extended_sequence,
        sample_start,
        span,
        pkt.record.rtp_timestamp,
        pkt.record
            .session_capture_offset_samples
            .map(|c| c.to_string())
            .unwrap_or_else(|| "none".into()),
    )
}

#[cfg(test)]
pub fn process_verified_segments(
    manifest: &LocalFinalizationManifest,
    segment_paths: &[PathBuf],
    staging_dir: &Path,
) -> Result<LocalProcessOutput, LocalProcessorError> {
    process_verified_segments_with_options(
        manifest,
        segment_paths,
        staging_dir,
        LocalProcessorOptions::default(),
    )
}

pub fn process_verified_segments_with_options(
    manifest: &LocalFinalizationManifest,
    segment_paths: &[PathBuf],
    staging_dir: &Path,
    options: LocalProcessorOptions,
) -> Result<LocalProcessOutput, LocalProcessorError> {
    std::fs::create_dir_all(staging_dir)?;
    options.check_interrupt()?;
    let target = manifest.target_sample_count_48k;
    let expected_bytes = expected_wav_file_bytes(target);
    let mut users: HashMap<String, UserAccumulator> = HashMap::new();
    let mut packet_loss_tracker = BoundedPacketLossTracker::new();
    for user in &manifest.users {
        packet_loss_tracker.register_user(&user.discord_user_id, &user.path_nick);
        let writer = UserWriter::new(
            user.path_nick.clone(),
            user.discord_user_id.clone(),
            staging_dir,
            target,
        )?;
        users.insert(
            user.discord_user_id.clone(),
            UserAccumulator {
                path_nick: user.path_nick.clone(),
                writer,
                packets: UserPacketAccumulator::new_with_placement_policy(
                    &user.discord_user_id,
                    target,
                    AccumulatorPlacementPolicy::PlacementOverlapTolerant,
                ),
            },
        );
    }
    let mut session_report = SessionRecoveryReportBuilder::default();
    let total_segments = segment_paths.len() as u32;

    for (seg_idx, seg_path) in segment_paths.iter().enumerate() {
        options.check_interrupt()?;
        let mut file = File::open(seg_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        let format_version = match read_file_header(&data) {
            Ok(v) => v,
            Err(JournalFramingError::Fatal(e)) => return Err(LocalProcessorError::Fatal(e)),
            Err(JournalFramingError::Recoverable(e)) => {
                return Err(LocalProcessorError::Fatal(e));
            }
        };
        let mut offset = 8usize;
        let mut records_in_segment = 0u64;
        while offset < data.len() {
            if records_in_segment > 0
                && records_in_segment.is_multiple_of(RECORD_INTERRUPT_CHECK_INTERVAL)
            {
                options.check_interrupt()?;
            }
            match decode_journal_record(&data, offset, format_version) {
                Ok((record, next)) => {
                    offset = next;
                    records_in_segment += 1;
                    if let Some(acc) = users.get_mut(&record.discord_user_id) {
                        packet_loss_tracker.note_known_user_journal_record();
                        acc.ingest(record, &mut packet_loss_tracker)?;
                    } else {
                        session_report.record_session_discarded_other();
                    }
                }
                Err(JournalFramingError::Fatal(e)) => return Err(LocalProcessorError::Fatal(e)),
                Err(JournalFramingError::Recoverable(_)) => {
                    session_report.record_session_discarded_other();
                    break;
                }
            }
        }
        options.maybe_emit_progress(seg_idx as u32 + 1, total_segments);
    }

    let mut stem_paths = HashMap::new();
    let mut stem_sha256 = HashMap::new();
    for user in &manifest.users {
        let mut acc = users
            .remove(&user.discord_user_id)
            .ok_or_else(|| LocalProcessorError::Fatal("missing user accumulator".into()))?;
        acc.flush_ready(&mut packet_loss_tracker)?;
        let (path, sha, report) = acc.writer.finalize()?;
        let len = std::fs::metadata(&path)?.len();
        if len != expected_bytes {
            return Err(LocalProcessorError::Fatal(format!(
                "stem {} byte length {} != {}",
                user.path_nick, len, expected_bytes
            )));
        }
        session_report.push_stem(report);
        stem_paths.insert(user.discord_user_id.clone(), path);
        stem_sha256.insert(user.discord_user_id.clone(), sha);
    }

    let packet_loss_policy = packet_loss_tracker
        .finish()
        .map_err(|e| LocalProcessorError::Fatal(e.to_string()))?;
    session_report.set_packet_loss_policy(packet_loss_policy.clone());

    let report_text = session_report.render(
        &manifest.session_id,
        &manifest.guild_id,
        &manifest.channel_id,
        target,
    );
    let report_path = staging_dir.join("Syndicate Audio Recovery Report.txt");
    ensure_parent(&report_path).map_err(LocalProcessorError::Io)?;
    let report_write = write_durable_payload_at_path(&report_path, report_text.as_bytes())
        .map_err(LocalProcessorError::Io)?;

    Ok(LocalProcessOutput {
        stem_paths,
        stem_sha256,
        report_path: Some(report_path),
        report_sha256: Some(report_write.sha256),
        report_bytes: Some(report_write.byte_length),
        discarded_packet_count: session_report.discarded_packet_count(),
        silence_range_count: session_report.silence_range_count(),
        packet_loss_policy,
    })
}

impl From<LocalFinalizationCancelled> for LocalProcessorError {
    fn from(_: LocalFinalizationCancelled) -> LocalProcessorError {
        LocalProcessorError::Cancelled
    }
}

#[cfg(test)]
pub(crate) fn write_zero_pcm_bytes_for_test<W: Write>(
    writer: &mut W,
    bytes: u64,
    silence_chunk: &[u8],
) -> std::io::Result<()> {
    if bytes == 0 {
        return Ok(());
    }
    let mut remaining = bytes;
    while remaining > 0 {
        let chunk = remaining.min(silence_chunk.len() as u64) as usize;
        writer.write_all(&silence_chunk[..chunk])?;
        remaining -= chunk as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::voice_delivery::hash::sha256_hex_reader;
    use crate::voice_delivery::hash::DEFAULT_STREAM_CHUNK;
    use crate::voice_delivery::local_finalization::manifest::{
        LocalFinalizationManifest, LocalFinalizationObjectRef, LocalFinalizationOutputPolicy,
        LocalFinalizationSegment, LocalFinalizationUser,
        LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
    };
    use crate::voice_delivery::local_finalization::synj::{
        encode_journal_file, encode_journal_record, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
    };
    use crate::voice_delivery::wav::validate_canonical_pcm_wav_header;
    use std::fs::File;
    use std::io::Write;

    fn test_manifest(
        target_samples: u64,
        users: Vec<LocalFinalizationUser>,
    ) -> LocalFinalizationManifest {
        LocalFinalizationManifest {
            version: 1,
            finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.into(),
            session_id: "sess-processor-test".into(),
            guild_id: "123456789012345678".into(),
            channel_id: "123456789012345678".into(),
            session_start_wall_ms: 0,
            session_start_monotonic_ns: "0".into(),
            sealed_stop_wall_ms: 0,
            sealed_stop_monotonic_ns: "0".into(),
            target_sample_count_48k: target_samples,
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
            users,
            output: LocalFinalizationOutputPolicy {
                wav_format: crate::voice_delivery::local_finalization::manifest::LOCAL_FINALIZATION_WAV_FORMAT.into(),
                recovery_report_relative_path: crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH.into(),
            },
        }
    }

    fn empty_journal_segment() -> Vec<u8> {
        encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[])
    }

    fn foreign_pad_records(count: usize) -> Vec<JournalRecord> {
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
                    opus_payload: vec![0x08, 0x01],
                }
            })
            .collect()
    }

    fn pad_timeline_records(user_id: &str, count: usize, start_seq: u16) -> Vec<JournalRecord> {
        (0..count)
            .map(|i| {
                let seq = start_seq + i as u16;
                let capture = u64::from(seq) * 960;
                JournalRecord {
                    discord_user_id: user_id.into(),
                    ssrc_epoch: 1,
                    sequence: seq,
                    rtp_timestamp: capture as u32,
                    arrived_at_ns: u64::from(seq) * 20_000_000,
                    session_capture_offset_samples: Some(capture),
                    opus_payload: vec![0x08, 0x01],
                }
            })
            .collect()
    }

    #[test]
    fn silence_stem_is_valid_wav_matching_incremental_hash() {
        let dir = tempfile::tempdir().unwrap();
        let target = 4800u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: "123456789012345678".into(),
            path_nick: "alice".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(&seg_path, empty_journal_segment()).unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let wav_path = out.stem_paths["123456789012345678"].clone();
        let len = std::fs::metadata(&wav_path).unwrap().len();
        assert_eq!(len, expected_wav_file_bytes(target));
        let header = std::fs::read(&wav_path).unwrap();
        validate_canonical_pcm_wav_header(&header[..44], Some(len)).unwrap();
        let reread =
            sha256_hex_reader(File::open(&wav_path).unwrap(), DEFAULT_STREAM_CHUNK).unwrap();
        assert_eq!(reread, out.stem_sha256["123456789012345678"]);
    }

    #[test]
    fn large_silence_uses_bounded_write_calls() {
        struct CountWriter {
            writes: u64,
        }
        impl Write for CountWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.writes += 1;
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let chunk = vec![0u8; SILENCE_WRITE_CHUNK_BYTES];
        let pcm_bytes = 2_000_000u64 * STEREO_PCM_FRAME_BYTES;
        let mut counter = CountWriter { writes: 0 };
        write_zero_pcm_bytes_for_test(&mut counter, pcm_bytes, &chunk).unwrap();
        let expected = pcm_bytes.div_ceil(SILENCE_WRITE_CHUNK_BYTES as u64);
        assert_eq!(counter.writes, expected);
        assert!(counter.writes < 100);
    }

    #[test]
    fn user_writer_does_not_hold_target_pcm_vec() {
        let dir = tempfile::tempdir().unwrap();
        let writer = UserWriter::new("x".into(), "1".into(), dir.path(), 48_000).unwrap();
        assert_eq!(writer.silence_chunk.len(), SILENCE_WRITE_CHUNK_BYTES);
        assert!(writer.pcm_byte_scratch.capacity() < 16 * 1024);
        // A duration-sized buffer would be ~192 KiB for this target alone (48k * 4), not a few KiB scratch.
        assert!(writer.pcm_bytes_written == 0);
    }

    #[test]
    fn seven_writers_single_journal_pass() {
        let dir = tempfile::tempdir().unwrap();
        let target = 960u64;
        let users: Vec<LocalFinalizationUser> = (0..7)
            .map(|i| LocalFinalizationUser {
                discord_user_id: format!("{:018}", i + 1),
                path_nick: format!("u{}", i),
                opaque_stem_id: format!("{:032x}", i),
            })
            .collect();
        let manifest = test_manifest(target, users);
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(&seg_path, empty_journal_segment()).unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        assert_eq!(out.stem_paths.len(), 7);
        for user in &manifest.users {
            let path = &out.stem_paths[&user.discord_user_id];
            assert_eq!(
                std::fs::metadata(path).unwrap().len(),
                expected_wav_file_bytes(target)
            );
        }
    }

    #[test]
    fn processor_honours_cancel_between_segments() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = test_manifest(
            960,
            vec![LocalFinalizationUser {
                discord_user_id: "123456789012345678".into(),
                path_nick: "a".into(),
                opaque_stem_id: "a".repeat(32),
            }],
        );
        let seg = dir.path().join("0.synj");
        std::fs::write(&seg, empty_journal_segment()).unwrap();
        let seg2 = dir.path().join("1.synj");
        std::fs::write(&seg2, empty_journal_segment()).unwrap();
        let cancel = LocalFinalizationCancel::new();
        cancel.cancel();
        assert!(matches!(
            process_verified_segments_with_options(
                &manifest,
                &[seg, seg2],
                &dir.path().join("staging"),
                LocalProcessorOptions {
                    cancel: Some(cancel),
                    paused: None,
                    progress: None,
                },
            ),
            Err(LocalProcessorError::Cancelled)
        ));
    }

    #[test]
    fn processor_emits_bounded_progress() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = test_manifest(
            960,
            vec![LocalFinalizationUser {
                discord_user_id: "123456789012345678".into(),
                path_nick: "a".into(),
                opaque_stem_id: "a".repeat(32),
            }],
        );
        let paths: Vec<PathBuf> = (0..10)
            .map(|i| {
                let p = dir.path().join(format!("{i}.synj"));
                std::fs::write(&p, empty_journal_segment()).unwrap();
                p
            })
            .collect();
        let progress = Arc::new(std::sync::Mutex::new(Vec::new()));
        let progress_cb = {
            let progress = Arc::clone(&progress);
            Arc::new(move |p: LocalProcessorProgress| {
                progress.lock().unwrap().push(p);
            })
        };
        process_verified_segments_with_options(
            &manifest,
            &paths,
            &dir.path().join("staging"),
            LocalProcessorOptions {
                cancel: None,
                paused: None,
                progress: Some(progress_cb),
            },
        )
        .unwrap();
        let events = progress.lock().unwrap();
        assert!(events.len() <= 4);
        assert_eq!(events.last().unwrap().processed_segments, 10);
    }

    fn loss_marker_record(user_id: &str, sequence: u16, placement: u64) -> JournalRecord {
        JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence,
            rtp_timestamp: u32::from(sequence) * 960,
            arrived_at_ns: u64::from(sequence) * 20_000_000,
            session_capture_offset_samples: Some(placement),
            opus_payload: vec![],
        }
    }

    #[test]
    fn lost_placement_pads_timeline_gap_then_marker_span() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 20_000_000u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "alice".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let mut records = foreign_pad_records(10_000);
        records.extend(pad_timeline_records(user_id, 10_000, 0));
        records.push(loss_marker_record(user_id, 20_000, 20_000 * 960));
        let encoded: Vec<_> = records
            .iter()
            .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
            .collect();
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(report_text.contains("badPacketCount=1"));
        assert!(report_text.contains("badPacketCount=1"));
        assert!(report_text.contains("journal_loss_marker"));
    }

    #[test]
    fn adjacent_loss_markers_in_journal_count_one_bad_packet_each() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 20_000_000u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "alice".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let mut records = foreign_pad_records(20_000);
        records.extend(pad_timeline_records(user_id, 20_000, 0));
        records.push(loss_marker_record(user_id, 30_000, 30_000 * 960));
        records.push(loss_marker_record(user_id, 30_002, 30_002 * 960));
        let encoded: Vec<_> = records
            .iter()
            .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
            .collect();
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        assert_eq!(out.packet_loss_policy.skipped_count, 2);
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(
            report_text.contains("framingOrPlacement=2") || report_text.contains("skippedCount=2")
        );
    }

    #[test]
    fn recovery_report_counts_discarded_duplicate_without_timeline_placement() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 4800u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "dup".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let base = JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        };
        let dup = base.clone();
        let r0 = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &base);
        let r1 = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &dup);
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[r0, r1]),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(
            report_text.contains("discarded duplicate=1"),
            "report must count duplicate discard: {}",
            report_text
        );
    }

    #[test]
    fn recovery_report_counts_out_of_window_discard() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 200_000u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "oow".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let mk = |seq: u16, capture: u64| JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence: seq,
            rtp_timestamp: u32::from(seq) * 960,
            arrived_at_ns: u64::from(seq) * 20_000_000,
            session_capture_offset_samples: Some(capture),
            opus_payload: vec![0x08, 0x01],
        };
        let mut records = foreign_pad_records(10_000);
        records.extend(pad_timeline_records(user_id, 10_000, 0));
        records.extend([
            mk(20_000, 20_000 * 960),
            mk(20_100, 20_100 * 960),
            mk(20_050, 20_050 * 960),
        ]);
        let encoded: Vec<_> = records
            .iter()
            .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
            .collect();
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(
            report_text.contains("out_of_window=1"),
            "report must count out-of-window discard: {}",
            report_text
        );
    }

    #[test]
    fn lost_placement_at_u64_max_placement_returns_fatal_not_wrap() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = UserWriter::new("x".into(), "1".into(), dir.path(), 1_000_000).unwrap();
        let record = JournalRecord {
            discord_user_id: "1".into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(u64::MAX),
            opus_payload: vec![],
        };
        let err = writer
            .apply_packet_for_test(&record, PacketDisposition::LostPlacement, u64::MAX, None)
            .unwrap_err();
        assert!(
            matches!(err, LocalProcessorError::Fatal(_)),
            "expected fatal on placement+1 overflow, got {:?}",
            err
        );
    }

    #[test]
    fn decode_failed_recovery_span_at_u64_max_returns_fatal_not_wrap() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = UserWriter::new("x".into(), "1".into(), dir.path(), 1_000_000).unwrap();
        let record = JournalRecord {
            discord_user_id: "1".into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0xff, 0xff],
        };
        let err = writer
            .apply_packet_for_test(&record, PacketDisposition::Decode, 100, Some(u64::MAX - 50))
            .unwrap_err();
        assert!(
            matches!(err, LocalProcessorError::Fatal(_)),
            "expected fatal on placement+recovery_span overflow, got {:?}",
            err
        );
    }

    /// Same geometry as production (1920-sample / 40ms overlap via +480 RTP delta) with
    /// sequential journal sequence after timeline padding.
    fn production_shaped_overlap_records(user_id: &str, base_seq: u16) -> Vec<JournalRecord> {
        let payload = vec![0x08, 0x01];
        let prev_start = u64::from(base_seq) * 960;
        let rtp_first = u32::from(base_seq) * 960;
        let first = JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence: base_seq,
            rtp_timestamp: rtp_first,
            arrived_at_ns: 1,
            session_capture_offset_samples: Some(prev_start),
            opus_payload: payload.clone(),
        };
        let second = JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence: base_seq + 1,
            rtp_timestamp: rtp_first + 480,
            arrived_at_ns: 2,
            session_capture_offset_samples: Some(prev_start),
            opus_payload: payload,
        };
        vec![
            first,
            second,
            JournalRecord {
                discord_user_id: user_id.into(),
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
    fn production_shaped_overlap_drops_without_silence_incident() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 500_000_000u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "prod_overlap".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let mut records = foreign_pad_records(10_000);
        records.extend(pad_timeline_records(user_id, 10_000, 0));
        records.extend(production_shaped_overlap_records(user_id, 10_000));
        let encoded: Vec<_> = records
            .iter()
            .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
            .collect();
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(
            report_text.contains("overlap=1"),
            "expected overlap discard count: {}",
            report_text
        );
        assert!(
            report_text.contains("placementOverlap=1"),
            "policy summary: {}",
            report_text
        );
        assert!(
            !report_text.contains("samples=399340157"),
            "overlap must not create recovery incident: {}",
            report_text
        );
        assert_eq!(out.packet_loss_policy.skipped_count, 1);
        assert_eq!(out.packet_loss_policy.source_counts.placement_overlap, 1);
    }

    #[test]
    fn exact_duplicate_not_counted_in_packet_loss_budget() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 4800u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "dup".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let base = JournalRecord {
            discord_user_id: user_id.into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0x08, 0x01],
        };
        let r0 = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &base);
        let r1 = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &base);
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[r0, r1]),
        )
        .unwrap();
        let out =
            process_verified_segments(&manifest, &[seg_path], &dir.path().join("staging")).unwrap();
        assert_eq!(out.packet_loss_policy.skipped_count, 0);
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
            opus_payload: vec![0x08, 0x01],
        };
        let second = JournalRecord {
            discord_user_id: user.into(),
            ssrc_epoch: 1,
            sequence: base_seq + 1,
            rtp_timestamp: (capture as u32) + 480,
            arrived_at_ns: u64::from(base_seq) + 1,
            session_capture_offset_samples: Some(capture),
            opus_payload: vec![0x08, 0x01],
        };
        vec![first, second]
    }

    fn run_journal_policy(
        user: &str,
        records: &[JournalRecord],
    ) -> Result<LocalProcessOutput, LocalProcessorError> {
        let dir = tempfile::tempdir().unwrap();
        let encoded: Vec<_> = records
            .iter()
            .map(|r| encode_journal_record(JOURNAL_FORMAT_VERSION_V2, r))
            .collect();
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &encoded),
        )
        .unwrap();
        let users = vec![LocalFinalizationUser {
            discord_user_id: user.into(),
            path_nick: "u".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(10_000_000, users);
        process_verified_segments(&manifest, &[seg_path], &dir.path().join("staging"))
    }

    #[test]
    fn isolated_overlap_among_ten_thousand_records_allowed() {
        let user = "123456789012345678";
        let mut records = pad_timeline_records(user, 10_000, 0);
        records.extend(overlap_pair(user, 20_000));
        assert!(run_journal_policy(user, &records).is_ok());
    }

    #[test]
    fn ratio_violation_refused() {
        let user = "123456789012345678";
        let mut records = pad_timeline_records(user, 5000, 0);
        records.extend(overlap_pair(user, 10_000));
        records.extend(overlap_pair(user, 10_002));
        let err = run_journal_policy(user, &records).unwrap_err();
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
        let mut records = pad_timeline_records(user, 200, 0);
        for block in 0..33u16 {
            let base = 1_000 + block * 4;
            records.extend(overlap_pair(user, base));
        }
        let err = run_journal_policy(user, &records).unwrap_err();
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
                opus_payload: vec![0x08, 0x01],
            },
            loss_marker_record(user, 1, 960),
            loss_marker_record(user, 2, 1920),
        ];
        let err = run_journal_policy(user, &records).unwrap_err();
        assert!(
            err.to_string().contains("consecutive"),
            "expected consecutive refusal: {}",
            err
        );
    }

    #[test]
    fn decode_framing_and_overlap_share_one_budget() {
        let user = "123456789012345678";
        let mut records = foreign_pad_records(10_000);
        records.extend(pad_timeline_records(user, 20_000, 0));
        records.push(loss_marker_record(user, 20_000, 20_000 * 960));
        records.extend(overlap_pair(user, 20_010));
        let out = run_journal_policy(user, &records).unwrap();
        assert_eq!(out.packet_loss_policy.skipped_count, 2);
        assert_eq!(
            out.packet_loss_policy
                .source_counts
                .framing_or_lost_placement,
            1
        );
        assert_eq!(out.packet_loss_policy.source_counts.placement_overlap, 1);
    }

    #[test]
    fn adjacent_out_of_window_extended_identity_refuses_consecutive() {
        let user = "123456789012345678";
        let mut records = foreign_pad_records(10_000);
        records.extend(pad_timeline_records(user, 10_000, 0));
        records.extend([
            JournalRecord {
                discord_user_id: user.into(),
                ssrc_epoch: 1,
                sequence: 50,
                rtp_timestamp: 50u32 * 960,
                arrived_at_ns: 1,
                session_capture_offset_samples: Some(50u64 * 960),
                opus_payload: vec![0x08, 0x01],
            },
            JournalRecord {
                discord_user_id: user.into(),
                ssrc_epoch: 1,
                sequence: 51,
                rtp_timestamp: 51u32 * 960,
                arrived_at_ns: 2,
                session_capture_offset_samples: Some(51u64 * 960),
                opus_payload: vec![0x08, 0x01],
            },
        ]);
        let err = run_journal_policy(user, &records).unwrap_err();
        assert!(
            err.to_string().contains("consecutive"),
            "adjacent OOW must refuse consecutive policy: {}",
            err
        );
    }

    #[test]
    #[cfg(feature = "local-opus-native")]
    fn encoded_packet_round_trip_through_journal() {
        let record = JournalRecord {
            discord_user_id: "123456789012345678".into(),
            ssrc_epoch: 1,
            sequence: 0,
            rtp_timestamp: 0,
            arrived_at_ns: 0,
            session_capture_offset_samples: Some(0),
            opus_payload: vec![0xFC, 0xFF, 0xFE],
        };
        let bytes = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &record);
        let file = encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[bytes]);
        let (decoded, _) = decode_journal_record(&file, 8, JOURNAL_FORMAT_VERSION_V2).unwrap();
        assert_eq!(decoded.discord_user_id, record.discord_user_id);
        assert_eq!(decoded.sequence, record.sequence);
    }
}
