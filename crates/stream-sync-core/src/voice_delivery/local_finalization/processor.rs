//! Single-pass journal demux + Opus decode into aligned stereo WAV stems.
//!
//! Each [`UserWriter`] streams PCM directly to its staging WAV (header written at open time)
//! with a 1 MiB [`std::io::BufWriter`] and incremental SHA-256 over the exact on-disk bytes.
//! Memory is bounded by per-writer buffers and the packet reorder window, not session duration.

use super::accumulator::{PacketDisposition, UserPacketAccumulator};
use super::cancellation::{LocalFinalizationCancel, LocalFinalizationCancelled};
use super::manifest::LocalFinalizationManifest;
use super::opus_decode::StereoOpusDecoder;
use super::report::{
    DiscardReason, SessionRecoveryReportBuilder, StemRecoveryReportBuilder, StemSilenceReason,
};
use super::synj::{decode_journal_record, read_file_header, JournalFramingError, JournalRecord};
use crate::voice_delivery::bounds::STEREO_PCM_FRAME_BYTES;
use crate::voice_delivery::finalized_manifest::expected_wav_file_bytes;
use crate::voice_delivery::fs::durable_payload::{ensure_parent, write_durable_payload_at_path};
use crate::voice_delivery::hash::{hex_digest, LiveSha256};
use crate::voice_delivery::wav::minimal_wav_header;
use sha2::Digest;
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

pub struct LocalProcessOutput {
    pub stem_paths: HashMap<String, PathBuf>,
    pub stem_sha256: HashMap<String, String>,
    pub report_path: Option<PathBuf>,
    pub report_sha256: Option<String>,
    pub report_bytes: Option<u64>,
    pub discarded_packet_count: u64,
    pub silence_range_count: u64,
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
            || processed_segments % PROCESSOR_PROGRESS_EVERY_SEGMENTS == 0;
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
    ) -> Result<(), LocalProcessorError> {
        match disposition {
            PacketDisposition::DiscardedDuplicate => {
                self.report.record_discarded(DiscardReason::Duplicate);
            }
            PacketDisposition::DiscardedConflict => {
                self.report.record_discarded(DiscardReason::Conflict);
            }
            PacketDisposition::DiscardedOutOfWindow => {
                self.report.record_discarded(DiscardReason::OutOfWindow);
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
            }
            PacketDisposition::Decode => {
                if record.opus_payload.is_empty() {
                    self.report.record_discarded(DiscardReason::Other);
                    return Ok(());
                }
                if placement >= self.target_samples {
                    self.report.record_discarded(DiscardReason::Other);
                    return Ok(());
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
                    }
                    Err(_) => {
                        self.report.record_discarded(DiscardReason::Other);
                        let recovery_span = span_samples.unwrap_or(0);
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
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn apply_packet_for_test(
        &mut self,
        record: &JournalRecord,
        disposition: PacketDisposition,
        placement: u64,
        span_samples: Option<u64>,
    ) -> Result<(), LocalProcessorError> {
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
    writer: UserWriter,
    packets: UserPacketAccumulator,
}

impl UserAccumulator {
    fn ingest(&mut self, record: JournalRecord) -> Result<(), LocalProcessorError> {
        let ready = self
            .packets
            .ingest_timeline(record)
            .map_err(|e| LocalProcessorError::Fatal(format!("accumulator: {e:?}")))?;
        for pkt in ready {
            self.apply_ready_packet(&pkt)?;
        }
        Ok(())
    }

    fn apply_ready_packet(
        &mut self,
        pkt: &super::accumulator::ReadyPacket,
    ) -> Result<(), LocalProcessorError> {
        use super::accumulator::PacketDisposition;
        match pkt.disposition {
            PacketDisposition::DiscardedDuplicate
            | PacketDisposition::DiscardedConflict
            | PacketDisposition::DiscardedOutOfWindow => {
                self.writer
                    .apply_packet(&pkt.record, pkt.disposition, 0, None)?;
            }
            PacketDisposition::Decode | PacketDisposition::LostPlacement => {
                let placement = pkt.timeline_sample_start.ok_or_else(|| {
                    LocalProcessorError::Fatal(format!(
                        "ready packet {:?} missing authoritative timeline",
                        pkt.disposition
                    ))
                })?;
                self.writer.apply_packet(
                    &pkt.record,
                    pkt.disposition,
                    placement,
                    pkt.span_samples,
                )?;
            }
        }
        Ok(())
    }

    fn flush_ready(&mut self) -> Result<(), LocalProcessorError> {
        let remaining = self
            .packets
            .flush_remaining()
            .map_err(|e| LocalProcessorError::Fatal(format!("accumulator: {e:?}")))?;
        for pkt in remaining {
            self.apply_ready_packet(&pkt)?;
        }
        Ok(())
    }
}

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
    for user in &manifest.users {
        let writer = UserWriter::new(
            user.path_nick.clone(),
            user.discord_user_id.clone(),
            staging_dir,
            target,
        )?;
        users.insert(
            user.discord_user_id.clone(),
            UserAccumulator {
                writer,
                packets: UserPacketAccumulator::new(&user.discord_user_id, target),
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
            if records_in_segment > 0 && records_in_segment % RECORD_INTERRUPT_CHECK_INTERVAL == 0 {
                options.check_interrupt()?;
            }
            match decode_journal_record(&data, offset, format_version) {
                Ok((record, next)) => {
                    offset = next;
                    records_in_segment += 1;
                    if let Some(acc) = users.get_mut(&record.discord_user_id) {
                        acc.ingest(record)?;
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
        acc.flush_ready()?;
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

    let report_text = session_report.render(
        &manifest.session_id,
        &manifest.guild_id,
        &manifest.channel_id,
        target,
    );
    let report_bytes = report_text.len() as u64;
    let report_path = staging_dir.join("Syndicate Audio Recovery Report.txt");
    let report_sha = hex_digest(&sha2::Sha256::digest(report_text.as_bytes()));
    ensure_parent(&report_path).map_err(LocalProcessorError::Io)?;
    write_durable_payload_at_path(&report_path, report_text.as_bytes())
        .map_err(LocalProcessorError::Io)?;

    Ok(LocalProcessOutput {
        stem_paths,
        stem_sha256,
        report_path: Some(report_path),
        report_sha256: Some(report_sha),
        report_bytes: Some(report_bytes),
        discarded_packet_count: session_report.discarded_packet_count(),
        silence_range_count: session_report.silence_range_count(),
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
        let target = 5760u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "alice".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let record = encode_journal_record(
            JOURNAL_FORMAT_VERSION_V2,
            &loss_marker_record(user_id, 0, 4800),
        );
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[record]),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(report_text.contains("badPacketCount=1"));
        assert!(report_text.contains("samples=4800-4801"));
        assert!(report_text.contains("tail_padding"));
    }

    #[test]
    fn adjacent_loss_markers_in_journal_count_one_bad_packet_each() {
        let dir = tempfile::tempdir().unwrap();
        let user_id = "123456789012345678";
        let target = 3840u64;
        let users = vec![LocalFinalizationUser {
            discord_user_id: user_id.into(),
            path_nick: "alice".into(),
            opaque_stem_id: "a".repeat(32),
        }];
        let manifest = test_manifest(target, users);
        let r0 = encode_journal_record(
            JOURNAL_FORMAT_VERSION_V2,
            &loss_marker_record(user_id, 0, 0),
        );
        let r1 = encode_journal_record(
            JOURNAL_FORMAT_VERSION_V2,
            &loss_marker_record(user_id, 1, 960),
        );
        let seg_path = dir.path().join("seg.synj");
        std::fs::write(
            &seg_path,
            encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[r0, r1]),
        )
        .unwrap();
        let staging = dir.path().join("staging");
        let out = process_verified_segments(&manifest, &[seg_path], &staging).unwrap();
        assert_eq!(out.silence_range_count, 2);
        let report_text = std::fs::read_to_string(out.report_path.unwrap()).unwrap();
        assert!(report_text.contains("badPacketCount=2"));
        assert!(report_text.contains("samples=0-1"));
        assert!(report_text.contains("samples=960-961"));
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
        let records = [mk(0, 0), mk(100, 100 * 960), mk(50, 50 * 960)];
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
