//! Single-pass journal demux + Opus decode into aligned stereo WAV stems.

use super::manifest::LocalFinalizationManifest;
use super::opus_decode::StereoOpusDecoder;
use super::report::{RecoveryReportBuilder, SilenceInterval};
use super::synj::{decode_journal_record, read_file_header, JournalFramingError, JournalRecord};
use crate::voice_delivery::finalized_manifest::expected_wav_file_bytes;
use crate::voice_delivery::hash::{hex_digest, sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::wav::minimal_wav_header;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const OPUS_FRAME_SAMPLES: u64 = 960;
const REORDER_WINDOW: u16 = 32;

#[derive(Debug, Error)]
pub enum LocalProcessorError {
    #[error("processor fatal: {0}")]
    Fatal(String),
    #[error("processor io: {0}")]
    Io(#[from] std::io::Error),
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

struct UserWriter {
    path_nick: String,
    path: PathBuf,
    target_samples: u64,
    pcm: Vec<i16>,
    decoder: StereoOpusDecoder,
    next_write_sample: u64,
    report: RecoveryReportBuilder,
}

impl UserWriter {
    fn new(
        path_nick: String,
        out_dir: &Path,
        target_samples: u64,
    ) -> Result<Self, LocalProcessorError> {
        let path = out_dir.join(format!("{}.wav", path_nick));
        let decoder = StereoOpusDecoder::new()
            .map_err(|_| LocalProcessorError::Fatal("opus decoder init failed".into()))?;
        Ok(Self {
            path_nick,
            path,
            target_samples,
            pcm: vec![0i16; (target_samples * 2) as usize],
            decoder,
            next_write_sample: 0,
            report: RecoveryReportBuilder::default(),
        })
    }

    fn pad_silence_to(&mut self, sample: u64, reason: &str) {
        if sample <= self.next_write_sample {
            return;
        }
        let start = self.next_write_sample;
        let end = sample.min(self.target_samples);
        if end > start {
            self.report.record_silence_interval(SilenceInterval {
                start_sample: start,
                end_sample: end,
                packet_count: 1,
                reason_code: reason.to_string(),
            });
        }
        self.next_write_sample = end;
    }

    fn decode_packet(&mut self, record: &JournalRecord) {
        if record.opus_payload.is_empty() {
            self.report.record_discarded_packet();
            return;
        }
        let capture = record
            .session_capture_offset_samples
            .unwrap_or(self.next_write_sample);
        if capture >= self.target_samples {
            self.report.record_discarded_packet();
            return;
        }
        self.pad_silence_to(capture, "timeline_gap");
        let mut pcm_buf = [0i16; 960 * 2];
        match self.decoder.decode_packet(record, &mut pcm_buf) {
            Ok(samples) => {
                let frame_samples = samples as u64;
                let start = self.next_write_sample;
                let end = (start + frame_samples).min(self.target_samples);
                let write_samples = end - start;
                let base = (start * 2) as usize;
                let len = (write_samples * 2) as usize;
                for i in 0..len {
                    if base + i < self.pcm.len() {
                        self.pcm[base + i] = pcm_buf[i];
                    }
                }
                self.next_write_sample = end;
            }
            Err(_) => {
                self.report.record_discarded_packet();
                let end = (capture + OPUS_FRAME_SAMPLES).min(self.target_samples);
                self.pad_silence_to(end, "opus_decode_failed");
            }
        }
    }

    fn finalize(mut self) -> Result<(PathBuf, String, RecoveryReportBuilder), LocalProcessorError> {
        self.pad_silence_to(self.target_samples, "tail_padding");
        let header = minimal_wav_header(self.target_samples)
            .map_err(|e| LocalProcessorError::Fatal(e.to_string()))?;
        let mut file = File::create(&self.path)?;
        file.write_all(&header)?;
        for sample in &self.pcm {
            file.write_all(&sample.to_le_bytes())?;
        }
        file.sync_all()?;
        file.seek(SeekFrom::Start(0))?;
        let sha = sha256_hex_reader(file, DEFAULT_STREAM_CHUNK)
            .map_err(|e| LocalProcessorError::Fatal(e.to_string()))?;
        Ok((self.path, sha, self.report))
    }
}

struct BufferedPacket {
    record: JournalRecord,
    extended_seq: u32,
}

struct UserAccumulator {
    writer: UserWriter,
    buffer: Vec<BufferedPacket>,
    frontier_seq: Option<u32>,
    epoch: u32,
}

impl UserAccumulator {
    fn flush_ready(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.buffer.sort_by_key(|p| p.extended_seq);
        while let Some(idx) = self.buffer.iter().position(|p| {
            self.frontier_seq
                .map(|f| p.extended_seq <= f + REORDER_WINDOW as u32)
                .unwrap_or(true)
        }) {
            let pkt = self.buffer.remove(idx);
            if let Some(f) = self.frontier_seq {
                if pkt.extended_seq <= f {
                    continue;
                }
            }
            self.frontier_seq = Some(pkt.extended_seq);
            self.writer.decode_packet(&pkt.record);
        }
    }

    fn ingest(&mut self, record: JournalRecord) {
        let extended_seq = (record.ssrc_epoch << 16) | u32::from(record.sequence);
        if self.epoch != 0 && record.ssrc_epoch != self.epoch {
            self.epoch = record.ssrc_epoch;
        } else if self.epoch == 0 {
            self.epoch = record.ssrc_epoch;
        }
        self.buffer.push(BufferedPacket {
            record,
            extended_seq,
        });
        self.flush_ready();
    }
}

pub fn process_verified_segments(
    manifest: &LocalFinalizationManifest,
    segment_paths: &[PathBuf],
    staging_dir: &Path,
) -> Result<LocalProcessOutput, LocalProcessorError> {
    std::fs::create_dir_all(staging_dir)?;
    let target = manifest.target_sample_count_48k;
    let expected_bytes = expected_wav_file_bytes(target);
    let mut users: HashMap<String, UserAccumulator> = HashMap::new();
    for user in &manifest.users {
        let writer = UserWriter::new(user.path_nick.clone(), staging_dir, target)?;
        users.insert(
            user.discord_user_id.clone(),
            UserAccumulator {
                writer,
                buffer: Vec::new(),
                frontier_seq: None,
                epoch: 0,
            },
        );
    }
    let mut global_report = RecoveryReportBuilder::default();

    for seg_path in segment_paths {
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
        while offset < data.len() {
            match decode_journal_record(&data, offset, format_version) {
                Ok((record, next)) => {
                    offset = next;
                    if let Some(acc) = users.get_mut(&record.discord_user_id) {
                        acc.ingest(record);
                    } else {
                        global_report.record_discarded_packet();
                    }
                }
                Err(JournalFramingError::Fatal(e)) => return Err(LocalProcessorError::Fatal(e)),
                Err(JournalFramingError::Recoverable(_)) => {
                    global_report.record_discarded_packet();
                    break;
                }
            }
        }
    }

    let mut stem_paths = HashMap::new();
    let mut stem_sha256 = HashMap::new();
    for user in &manifest.users {
        let mut acc = users
            .remove(&user.discord_user_id)
            .ok_or_else(|| LocalProcessorError::Fatal("missing user accumulator".into()))?;
        acc.flush_ready();
        let (path, sha, report) = acc.writer.finalize()?;
        let len = std::fs::metadata(&path)?.len();
        if len != expected_bytes {
            return Err(LocalProcessorError::Fatal(format!(
                "stem {} byte length {} != {}",
                user.path_nick, len, expected_bytes
            )));
        }
        global_report.discarded_packet_count += report.discarded_packet_count;
        global_report.discarded_packet_count += report.discarded_packet_count;
        for interval in report.intervals_snapshot() {
            global_report.record_silence_interval(interval);
        }
        stem_paths.insert(user.discord_user_id.clone(), path);
        stem_sha256.insert(user.discord_user_id.clone(), sha);
    }

    let report_text = global_report.render(
        &manifest.session_id,
        &manifest.guild_id,
        &manifest.channel_id,
        target,
    );
    let report_bytes = report_text.len() as u64;
    let report_path = staging_dir.join("Syndicate Audio Recovery Report.txt");
    let report_sha = hex_digest(&Sha256::digest(report_text.as_bytes()));
    std::fs::write(&report_path, report_text)?;

    Ok(LocalProcessOutput {
        stem_paths,
        stem_sha256,
        report_path: Some(report_path),
        report_sha256: Some(report_sha),
        report_bytes: Some(report_bytes),
        discarded_packet_count: global_report.discarded_packet_count,
        silence_range_count: global_report.silence_range_count(),
    })
}
