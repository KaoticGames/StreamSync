//! Phase 0C spike: large-file resumable ingest + durable session-directory publication.
//! Isolated from live `discord_voice` ingest; reusable primitives for v2 delivery.

#![allow(dead_code)]

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Stereo s16 @ 48 kHz — authoritative session bounds from the delivery plan.
pub const SAMPLE_RATE_HZ: u64 = 48_000;
pub const CHANNELS_STEREO: u16 = 2;
pub const BYTES_PER_SAMPLE: u16 = 2;

pub const SESSION_4_5H_SAMPLE_FRAMES: u64 = 777_600_000;
pub const SESSION_4_5H_DATA_BYTES: u64 = 3_110_400_000;

pub const SESSION_6H_SAMPLE_FRAMES: u64 = 1_036_800_000;
pub const SESSION_6H_DATA_BYTES: u64 = 4_147_200_000;

/// Bytes per stereo s16 PCM frame (block alignment for this spike).
pub const STEREO_PCM_FRAME_BYTES: u64 = (CHANNELS_STEREO as u64) * (BYTES_PER_SAMPLE as u64);

/// Classic RIFF/WAV maximum PCM `data` bytes: `riff_size = data + 36` must fit `u32` and `data` is frame-aligned.
pub const RIFF_PCM_OVERHEAD_BYTES: u64 = 36;
pub const RIFF_MAX_CHUNK_BYTES: u64 =
    ((u32::MAX as u64) - RIFF_PCM_OVERHEAD_BYTES) / STEREO_PCM_FRAME_BYTES * STEREO_PCM_FRAME_BYTES;

pub const DEFAULT_STREAM_CHUNK: usize = 256 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BoundsError {
    #[error("sample frame count overflow")]
    SampleFramesOverflow,
    #[error("data byte count overflow")]
    DataBytesOverflow,
    #[error("session exceeds RIFF/WAV size limit ({RIFF_MAX_CHUNK_BYTES} data bytes)")]
    ExceedsRiffLimit,
}

/// Compute PCM data bytes for a session length without signed 32-bit narrowing.
pub fn wav_data_bytes_for_frames(
    sample_frames: u64,
    channels: u16,
    bytes_per_sample: u16,
) -> Result<u64, BoundsError> {
    let channels_u64 = channels as u64;
    let bps = bytes_per_sample as u64;
    let frames = sample_frames;
    let product = frames
        .checked_mul(channels_u64)
        .and_then(|v| v.checked_mul(bps))
        .ok_or(BoundsError::DataBytesOverflow)?;
    if product > RIFF_MAX_CHUNK_BYTES {
        return Err(BoundsError::ExceedsRiffLimit);
    }
    Ok(product)
}

pub fn validate_plan_session_bounds(sample_frames: u64) -> Result<u64, BoundsError> {
    wav_data_bytes_for_frames(sample_frames, CHANNELS_STEREO, BYTES_PER_SAMPLE)
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartialWriteError {
    #[error("existing partial length {existing} exceeds expected {expected}")]
    OversizedExisting { existing: u64, expected: u64 },
    #[error("offset {offset} is beyond file capacity {capacity}")]
    OutOfRange { offset: u64, capacity: u64 },
    #[error("range length overflow")]
    LengthOverflow,
    #[error("non-contiguous write at {offset}, expected {expected}")]
    NonContiguous { offset: u64, expected: u64 },
    #[error("overlapping range disagrees with existing bytes")]
    OverlapMismatch,
    #[error("resume binding mismatch")]
    BindingMismatch,
    #[error("checkpoint corrupt or inconsistent")]
    CheckpointInvalid,
    #[error("sparse or uncheckpointed tail in partial file")]
    UncheckpointedTail,
    #[error("prefix digest mismatch at checkpoint")]
    PrefixDigestMismatch,
    #[error("invalid stream chunk size")]
    InvalidChunkSize,
    #[error("io error: {0}")]
    Io(String),
}

impl From<std::io::Error> for PartialWriteError {
    fn from(e: std::io::Error) -> Self {
        PartialWriteError::Io(e.to_string())
    }
}

/// Strong binding for resumable partial content (ingest supplies expected identity).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialResumeBinding {
    pub binding: String,
}

impl PartialResumeBinding {
    pub fn new(binding: impl Into<String>) -> Self {
        Self {
            binding: binding.into(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PartialCheckpoint {
    expected_len: u64,
    durable_contiguous_len: u64,
    binding: String,
    prefix_sha256: String,
}

fn partial_checkpoint_path(partial: &Path) -> PathBuf {
    let mut name = partial.as_os_str().to_os_string();
    name.push(".checkpoint");
    PathBuf::from(name)
}

fn reject_zero_chunk(chunk_size: usize) -> Result<(), PartialWriteError> {
    if chunk_size == 0 {
        return Err(PartialWriteError::InvalidChunkSize);
    }
    Ok(())
}

fn sha256_prefix_file(
    path: &Path,
    len: u64,
    chunk_size: usize,
) -> Result<String, PartialWriteError> {
    reject_zero_chunk(chunk_size)?;
    let mut file = File::open(path).map_err(PartialWriteError::from)?;
    let mut hasher = Sha256::new();
    let mut remaining = len;
    let mut buf = vec![0u8; chunk_size];
    while remaining > 0 {
        let take = remaining.min(chunk_size as u64) as usize;
        file.read_exact(&mut buf[..take])
            .map_err(PartialWriteError::from)?;
        hasher.update(&buf[..take]);
        remaining -= take as u64;
    }
    Ok(hex_digest(&hasher.finalize()))
}

fn write_partial_checkpoint(
    partial: &Path,
    checkpoint: &PartialCheckpoint,
) -> Result<(), PartialWriteError> {
    let path = partial_checkpoint_path(partial);
    let data = serde_json::to_vec(checkpoint).map_err(|e| PartialWriteError::Io(e.to_string()))?;
    write_bytes_atomic_plain(&path, &data).map_err(|e| PartialWriteError::Io(e.to_string()))
}

fn read_partial_checkpoint(partial: &Path) -> Result<Option<PartialCheckpoint>, PartialWriteError> {
    let path = partial_checkpoint_path(partial);
    if !path.is_file() {
        return Ok(None);
    }
    let data = fs::read(&path).map_err(PartialWriteError::from)?;
    let cp: PartialCheckpoint =
        serde_json::from_slice(&data).map_err(|_| PartialWriteError::CheckpointInvalid)?;
    Ok(Some(cp))
}

/// Resumable ranged writer for a `.partial` artifact (u64 offsets end-to-end).
pub struct PartialFileWriter {
    path: PathBuf,
    expected_len: u64,
    durable_contiguous_len: u64,
    in_memory_contiguous_len: u64,
    binding: PartialResumeBinding,
    file: File,
}

impl PartialFileWriter {
    pub fn open(
        path: impl AsRef<Path>,
        expected_len: u64,
        binding: PartialResumeBinding,
    ) -> Result<Self, PartialWriteError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(PartialWriteError::from)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(PartialWriteError::from)?;

        let durable_contiguous_len = if let Some(cp) = read_partial_checkpoint(&path)? {
            if cp.expected_len != expected_len || cp.binding != binding.binding {
                return Err(PartialWriteError::BindingMismatch);
            }
            let physical = path.metadata().map_err(PartialWriteError::from)?.len();
            if physical > expected_len {
                return Err(PartialWriteError::OversizedExisting {
                    existing: physical,
                    expected: expected_len,
                });
            }
            if physical > cp.durable_contiguous_len {
                file.set_len(cp.durable_contiguous_len)
                    .map_err(PartialWriteError::from)?;
            } else if physical < cp.durable_contiguous_len {
                return Err(PartialWriteError::UncheckpointedTail);
            }
            let digest =
                sha256_prefix_file(&path, cp.durable_contiguous_len, DEFAULT_STREAM_CHUNK)?;
            if digest != cp.prefix_sha256 {
                return Err(PartialWriteError::PrefixDigestMismatch);
            }
            cp.durable_contiguous_len
        } else if path.is_file() {
            let physical = path.metadata().map_err(PartialWriteError::from)?.len();
            if physical > 0 {
                return Err(PartialWriteError::CheckpointInvalid);
            }
            0
        } else {
            0
        };

        Ok(Self {
            path,
            expected_len,
            durable_contiguous_len,
            in_memory_contiguous_len: durable_contiguous_len,
            binding,
            file,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn expected_len(&self) -> u64 {
        self.expected_len
    }

    pub fn contiguous_len(&self) -> u64 {
        self.in_memory_contiguous_len
    }

    pub fn durable_contiguous_len(&self) -> u64 {
        self.durable_contiguous_len
    }

    pub fn write_range(&mut self, offset: u64, data: &[u8]) -> Result<(), PartialWriteError> {
        let len = data.len();
        let end = offset
            .checked_add(len as u64)
            .ok_or(PartialWriteError::LengthOverflow)?;
        if end > self.expected_len {
            return Err(PartialWriteError::OutOfRange {
                offset,
                capacity: self.expected_len,
            });
        }

        if offset == self.in_memory_contiguous_len {
            self.file
                .seek(SeekFrom::Start(offset))
                .map_err(PartialWriteError::from)?;
            self.file.write_all(data).map_err(PartialWriteError::from)?;
            self.in_memory_contiguous_len = end;
            return Ok(());
        }

        if offset < self.in_memory_contiguous_len {
            let overlap_end = end.min(self.in_memory_contiguous_len);
            if overlap_end > offset {
                let overlap_len = (overlap_end - offset) as usize;
                self.file
                    .seek(SeekFrom::Start(offset))
                    .map_err(PartialWriteError::from)?;
                let mut existing = vec![0u8; overlap_len];
                self.file
                    .read_exact(&mut existing)
                    .map_err(PartialWriteError::from)?;
                if existing != data[..overlap_len] {
                    return Err(PartialWriteError::OverlapMismatch);
                }
            }
            if end <= self.in_memory_contiguous_len {
                return Ok(());
            }
            let append_offset = self.in_memory_contiguous_len;
            self.file
                .seek(SeekFrom::Start(append_offset))
                .map_err(PartialWriteError::from)?;
            let skip = (append_offset - offset) as usize;
            self.file
                .write_all(&data[skip..])
                .map_err(PartialWriteError::from)?;
            self.in_memory_contiguous_len = end;
            return Ok(());
        }

        Err(PartialWriteError::NonContiguous {
            offset,
            expected: self.in_memory_contiguous_len,
        })
    }

    pub fn fsync(&mut self) -> Result<(), PartialWriteError> {
        self.file.sync_all().map_err(PartialWriteError::from)?;
        if self.in_memory_contiguous_len < self.durable_contiguous_len {
            return Err(PartialWriteError::CheckpointInvalid);
        }
        let prefix_sha256 = sha256_prefix_file(
            &self.path,
            self.in_memory_contiguous_len,
            DEFAULT_STREAM_CHUNK,
        )?;
        let cp = PartialCheckpoint {
            expected_len: self.expected_len,
            durable_contiguous_len: self.in_memory_contiguous_len,
            binding: self.binding.binding.clone(),
            prefix_sha256,
        };
        write_partial_checkpoint(&self.path, &cp)?;
        self.durable_contiguous_len = self.in_memory_contiguous_len;
        Ok(())
    }
}

pub fn sha256_hex_file(path: &Path, chunk_size: usize) -> Result<String> {
    reject_zero_chunk(chunk_size).map_err(|e| anyhow!(e))?;
    let mut file = File::open(path).context("open for hash")?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; chunk_size];
    loop {
        let n = file.read(&mut buf).context("read for hash")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_digest(&hasher.finalize()))
}

pub fn sha256_hex_reader<R: Read>(mut reader: R, chunk_size: usize) -> Result<String> {
    reject_zero_chunk(chunk_size).map_err(|e| anyhow!(e))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; chunk_size];
    loop {
        let n = reader.read(&mut buf).context("read for hash")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_digest(&hasher.finalize()))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Deterministic synthetic byte stream (no full materialization in RAM).
pub struct SyntheticByteSource {
    total_len: u64,
    pos: u64,
}

impl SyntheticByteSource {
    pub fn new(total_len: u64) -> Self {
        Self { total_len, pos: 0 }
    }

    pub fn byte_at(offset: u64) -> u8 {
        ((offset.wrapping_mul(0x9E37_79B9)) >> 24) as u8
    }
}

impl Read for SyntheticByteSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.total_len {
            return Ok(0);
        }
        let mut written = 0usize;
        for slot in buf.iter_mut() {
            if self.pos >= self.total_len {
                break;
            }
            *slot = Self::byte_at(self.pos);
            self.pos += 1;
            written += 1;
        }
        Ok(written)
    }
}

pub fn streaming_sha256_synthetic(total_len: u64, chunk_size: usize) -> Result<String> {
    reject_zero_chunk(chunk_size).map_err(|e| anyhow!(e))?;
    sha256_hex_reader(SyntheticByteSource::new(total_len), chunk_size)
}

/// Minimal WAV header for a fixed data chunk size (classic RIFF, not RF64).
pub fn minimal_wav_header(data_bytes: u64) -> Result<Vec<u8>> {
    if data_bytes % STEREO_PCM_FRAME_BYTES != 0 {
        return Err(anyhow!(
            "WAV data size must be {STEREO_PCM_FRAME_BYTES}-byte frame aligned"
        ));
    }
    if data_bytes > RIFF_MAX_CHUNK_BYTES {
        return Err(anyhow!(
            "WAV data size exceeds classic RIFF limit ({RIFF_MAX_CHUNK_BYTES} bytes)"
        ));
    }
    let data_u32 = u32::try_from(data_bytes).map_err(|_| anyhow!("WAV data size exceeds u32"))?;
    let riff_size = 36u32
        .checked_add(data_u32)
        .ok_or_else(|| anyhow!("RIFF size field overflow"))?;
    let mut h = Vec::with_capacity(44);
    h.extend_from_slice(b"RIFF");
    h.extend_from_slice(&riff_size.to_le_bytes());
    h.extend_from_slice(b"WAVEfmt ");
    h.extend_from_slice(&16u32.to_le_bytes());
    h.extend_from_slice(&1u16.to_le_bytes()); // PCM
    h.extend_from_slice(&CHANNELS_STEREO.to_le_bytes());
    h.extend_from_slice(&(SAMPLE_RATE_HZ as u32).to_le_bytes());
    let byte_rate = (SAMPLE_RATE_HZ * CHANNELS_STEREO as u64 * BYTES_PER_SAMPLE as u64) as u32;
    h.extend_from_slice(&byte_rate.to_le_bytes());
    let block_align = (CHANNELS_STEREO * BYTES_PER_SAMPLE) as u16;
    h.extend_from_slice(&block_align.to_le_bytes());
    h.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    h.extend_from_slice(b"data");
    h.extend_from_slice(&data_u32.to_le_bytes());
    Ok(h)
}

pub fn parse_wav_data_chunk_len(header: &[u8]) -> Result<u64> {
    validate_canonical_pcm_wav_header(header, None)
}

/// Validate the canonical 44-byte stereo PCM WAV header produced by `minimal_wav_header`.
/// When `file_len` is provided, RIFF size must equal `file_len - 8` and file must cover header + data (+ optional pad).
pub fn validate_canonical_pcm_wav_header(header: &[u8], file_len: Option<u64>) -> Result<u64> {
    if header.len() < 44 {
        return Err(anyhow!("WAV header too short"));
    }
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        return Err(anyhow!("not a RIFF WAVE file"));
    }
    let riff_size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
    if &header[12..16] != b"fmt " {
        return Err(anyhow!("missing fmt chunk"));
    }
    let fmt_len = u32::from_le_bytes(header[16..20].try_into().unwrap());
    if fmt_len != 16 {
        return Err(anyhow!("unexpected fmt chunk length"));
    }
    let audio_format = u16::from_le_bytes(header[20..22].try_into().unwrap());
    if audio_format != 1 {
        return Err(anyhow!("expected PCM format"));
    }
    let channels = u16::from_le_bytes(header[22..24].try_into().unwrap());
    if channels != CHANNELS_STEREO {
        return Err(anyhow!("expected stereo channels"));
    }
    let sample_rate = u32::from_le_bytes(header[24..28].try_into().unwrap()) as u64;
    if sample_rate != SAMPLE_RATE_HZ {
        return Err(anyhow!("expected 48 kHz sample rate"));
    }
    let byte_rate = u32::from_le_bytes(header[28..32].try_into().unwrap()) as u64;
    let expected_byte_rate = SAMPLE_RATE_HZ * CHANNELS_STEREO as u64 * BYTES_PER_SAMPLE as u64;
    if byte_rate != expected_byte_rate {
        return Err(anyhow!("unexpected byte rate"));
    }
    let block_align = u16::from_le_bytes(header[32..34].try_into().unwrap());
    if block_align as u64 != STEREO_PCM_FRAME_BYTES {
        return Err(anyhow!("unexpected block alignment"));
    }
    let bits_per_sample = u16::from_le_bytes(header[34..36].try_into().unwrap());
    if bits_per_sample != 16 {
        return Err(anyhow!("expected 16-bit samples"));
    }
    if &header[36..40] != b"data" {
        return Err(anyhow!("missing data chunk"));
    }
    let data_len = u32::from_le_bytes(header[40..44].try_into().unwrap()) as u64;
    if data_len % STEREO_PCM_FRAME_BYTES != 0 {
        return Err(anyhow!("data chunk length not frame aligned"));
    }
    if riff_size != 36 + data_len {
        return Err(anyhow!("RIFF size inconsistent with data chunk"));
    }
    if let Some(len) = file_len {
        if len < 8 {
            return Err(anyhow!("file too short for RIFF"));
        }
        if riff_size != len - 8 {
            return Err(anyhow!("RIFF size inconsistent with file length"));
        }
        let padded_data = data_len + (data_len % 2);
        if len != 44 + padded_data {
            return Err(anyhow!(
                "file length inconsistent with data chunk and RIFF padding"
            ));
        }
    }
    Ok(data_len)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExpectedStem {
    pub file_name: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipState {
    Prepared,
    Published,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryOwnershipRecord {
    pub delivery_id: String,
    pub manifest_hash: String,
    pub staging_session_dir: PathBuf,
    pub final_session_dir: PathBuf,
    pub stems: Vec<ExpectedStem>,
    pub state: OwnershipState,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LedgerError {
    #[error("destination exists without matching ownership record")]
    UnrelatedDestination,
    #[error("manifest hash mismatch")]
    ManifestMismatch,
    #[error("ownership ledger record mismatch")]
    LedgerRecordMismatch,
    #[error("invalid stem manifest")]
    InvalidStemManifest { reason: String },
    #[error("unexpected session directory entry: {name}")]
    UnexpectedSessionEntry { name: String },
    #[error("publication lock held by another owner")]
    PublicationLocked,
    #[error("published ledger state inconsistent with filesystem")]
    PublishedStateInvalid,
    #[error("stem verification failed for {file_name}")]
    StemMismatch { file_name: String },
    #[error("incomplete staging session")]
    StagingIncomplete,
    #[error("io: {0}")]
    Io(String),
}

impl From<std::io::Error> for LedgerError {
    fn from(e: std::io::Error) -> Self {
        LedgerError::Io(e.to_string())
    }
}

pub fn ownership_ledger_path(parent: &Path, delivery_id: &str) -> PathBuf {
    parent
        .join(".syndicate-staging")
        .join(delivery_id)
        .join("ownership.json")
}

pub fn publication_lock_path(parent: &Path, delivery_id: &str) -> PathBuf {
    parent
        .join(".syndicate-staging")
        .join(delivery_id)
        .join("publication.lock")
}

fn normalize_session_path(path: &Path) -> Result<PathBuf, LedgerError> {
    path.canonicalize()
        .or_else(|_| {
            let parent = path
                .parent()
                .ok_or_else(|| LedgerError::Io("path has no parent".into()))?;
            durable_create_dir_all(parent).map_err(|e| LedgerError::Io(e.to_string()))?;
            path.canonicalize().map_err(LedgerError::from)
        })
        .map_err(LedgerError::from)
}

fn validate_stem_file_name(file_name: &str) -> Result<(), LedgerError> {
    if file_name.is_empty() {
        return Err(LedgerError::InvalidStemManifest {
            reason: "empty file name".into(),
        });
    }
    if file_name.contains('/') || file_name.contains('\\') {
        return Err(LedgerError::InvalidStemManifest {
            reason: "path separators not allowed".into(),
        });
    }
    if Path::new(file_name).components().count() != 1 {
        return Err(LedgerError::InvalidStemManifest {
            reason: "non-normal file name".into(),
        });
    }
    if file_name == "." || file_name == ".." {
        return Err(LedgerError::InvalidStemManifest {
            reason: "reserved file name".into(),
        });
    }
    Ok(())
}

pub fn validate_stem_manifest(stems: &[ExpectedStem]) -> Result<(), LedgerError> {
    if stems.is_empty() {
        return Err(LedgerError::InvalidStemManifest {
            reason: "empty stem list".into(),
        });
    }
    let mut seen = HashSet::new();
    for stem in stems {
        validate_stem_file_name(&stem.file_name)?;
        if !seen.insert(stem.file_name.clone()) {
            return Err(LedgerError::InvalidStemManifest {
                reason: format!("duplicate stem {}", stem.file_name),
            });
        }
    }
    Ok(())
}

fn records_compatible(existing: &DeliveryOwnershipRecord, next: &DeliveryOwnershipRecord) -> bool {
    existing.delivery_id == next.delivery_id
        && existing.manifest_hash == next.manifest_hash
        && existing.staging_session_dir == next.staging_session_dir
        && existing.final_session_dir == next.final_session_dir
        && existing.stems == next.stems
}

fn assert_ledger_transition_allowed(
    path: &Path,
    next: &DeliveryOwnershipRecord,
) -> Result<(), LedgerError> {
    if !path.is_file() {
        return Ok(());
    }
    let existing = read_ownership_ledger(path).map_err(|e| LedgerError::Io(e.to_string()))?;
    if !records_compatible(&existing, next) {
        return Err(LedgerError::LedgerRecordMismatch);
    }
    match (existing.state, next.state) {
        (OwnershipState::Prepared, OwnershipState::Prepared)
        | (OwnershipState::Prepared, OwnershipState::Published)
        | (OwnershipState::Published, OwnershipState::Published) => Ok(()),
        (OwnershipState::Published, OwnershipState::Prepared) => {
            Err(LedgerError::LedgerRecordMismatch)
        }
    }
}

struct PublicationLockGuard {
    _file: File,
}

fn acquire_publication_lock(lock_path: &Path) -> Result<PublicationLockGuard, LedgerError> {
    if let Some(parent) = lock_path.parent() {
        durable_create_dir_all(parent).map_err(|e| LedgerError::Io(e.to_string()))?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .map_err(LedgerError::from)?;
    file.try_lock_exclusive().map_err(|e| {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            LedgerError::PublicationLocked
        } else {
            LedgerError::Io(e.to_string())
        }
    })?;
    Ok(PublicationLockGuard { _file: file })
}

fn write_ownership_ledger(
    path: &Path,
    record: &DeliveryOwnershipRecord,
    fault: Option<&mut LedgerFaultInjector>,
) -> Result<()> {
    validate_stem_manifest(&record.stems)?;
    assert_ledger_transition_allowed(path, record)?;
    durable_create_dir_all(
        path.parent()
            .ok_or_else(|| anyhow!("ledger path has no parent"))?,
    )?;
    let data = serde_json::to_vec_pretty(record)?;
    write_bytes_atomic(path, &data, fault)
}

fn read_ownership_ledger(path: &Path) -> Result<DeliveryOwnershipRecord> {
    let data = fs::read(path).context("read ownership ledger")?;
    let record: DeliveryOwnershipRecord = serde_json::from_slice(&data)?;
    Ok(record)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerAtomicFault {
    AfterTempFsync,
    AfterReplace,
    AfterParentFsync,
}

pub struct LedgerFaultInjector {
    trip: Option<LedgerAtomicFault>,
}

impl LedgerFaultInjector {
    pub fn trip_once(point: LedgerAtomicFault) -> Self {
        Self { trip: Some(point) }
    }

    pub fn check(&mut self, point: LedgerAtomicFault) -> Result<(), SimulatedCrash> {
        if self.trip == Some(point) {
            self.trip = None;
            Err(SimulatedCrash)
        } else {
            Ok(())
        }
    }
}

fn write_bytes_atomic_plain(target: &Path, data: &[u8]) -> Result<()> {
    let file_name = target
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("ledger");
    let tmp = target.with_file_name(format!(
        "{file_name}.tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    replace_file_same_volume(&tmp, target)?;
    sync_parent_dir(target)?;
    Ok(())
}

fn write_bytes_atomic(
    target: &Path,
    data: &[u8],
    mut fault: Option<&mut LedgerFaultInjector>,
) -> Result<()> {
    let file_name = target
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("ledger");
    let tmp = target.with_file_name(format!(
        "{file_name}.tmp-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    {
        let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    if let Some(ref mut f) = fault {
        f.check(LedgerAtomicFault::AfterTempFsync)
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    replace_file_same_volume(&tmp, target)?;
    if let Some(ref mut f) = fault {
        f.check(LedgerAtomicFault::AfterReplace)
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    sync_parent_dir(target)?;
    if let Some(ref mut f) = fault {
        f.check(LedgerAtomicFault::AfterParentFsync)
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    Ok(())
}

/// Replace `target` with `tmp` on the same volume without unlinking `target` first.
fn replace_file_same_volume(tmp: &Path, target: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        fs::rename(tmp, target)?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        windows_replace_file_write_through(tmp, target)?;
        return Ok(());
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (tmp, target);
        Err(anyhow!("unsupported platform for atomic file replace"))
    }
}

#[cfg(windows)]
fn windows_replace_file_write_through(tmp: &Path, target: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let src_w: Vec<u16> = tmp.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    let flags = MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING;
    let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), flags) };
    if ok == 0 {
        let err = unsafe { GetLastError() };
        return Err(anyhow!("MoveFileExW file replace failed: {}", err));
    }
    Ok(())
}

pub fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        #[cfg(unix)]
        {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        #[cfg(windows)]
        {
            sync_path_windows(parent)?;
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = parent;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn sync_path_windows(path: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{GetLastError, HANDLE};
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FlushFileBuffers, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_BACKUP_SEMANTICS,
        OPEN_EXISTING,
    };

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_WRITE,
            windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ
                | windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS,
            0 as HANDLE,
        )
    };
    if handle == windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE {
        let err = unsafe { GetLastError() };
        return Err(anyhow!("CreateFileW for directory sync failed: {}", err));
    }
    let flushed = unsafe { FlushFileBuffers(handle) };
    if flushed == 0 {
        let err = unsafe { GetLastError() };
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(handle);
        }
        return Err(anyhow!("FlushFileBuffers failed: {}", err));
    }
    unsafe {
        windows_sys::Win32::Foundation::CloseHandle(handle);
    }
    Ok(())
}

/// Create `path` and every missing ancestor, fsyncing each new directory and its parent (Unix).
pub fn durable_create_dir_all(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    if path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            durable_create_dir_all(parent)?;
        }
    }
    fs::create_dir(path)?;
    sync_parent_dir(path)?;
    Ok(())
}

pub fn same_volume(a: &Path, b: &Path) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let ma = fs::metadata(a)?;
        let mb = fs::metadata(b)?;
        Ok(ma.dev() == mb.dev())
    }
    #[cfg(windows)]
    {
        Ok(windows_volume_serial_for_path(a)? == windows_volume_serial_for_path(b)?)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (a, b);
        Ok(true)
    }
}

#[cfg(windows)]
fn windows_volume_serial_for_path(path: &Path) -> Result<u32> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationW;

    let mut probe = path.to_path_buf();
    while !probe.exists() {
        if !probe.pop() {
            break;
        }
    }
    let mut wide: Vec<u16> = probe.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut serial = 0u32;
    let ok = unsafe {
        GetVolumeInformationW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            0,
            &mut serial,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    if ok == 0 {
        let err = unsafe { GetLastError() };
        return Err(anyhow!("GetVolumeInformationW failed: {}", err));
    }
    Ok(serial)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationFault {
    AfterPreparedPersist,
    AfterStagingStemFsync,
    AfterStagingDirFsync,
    AfterDirectoryRename,
    AfterRenameSourceParentFsync,
    AfterRenameDestParentFsync,
}

#[derive(Debug, Error)]
#[error("simulated crash at fault point")]
pub struct SimulatedCrash;

pub struct FaultInjector {
    trip: Option<PublicationFault>,
}

impl FaultInjector {
    pub fn trip_once(point: PublicationFault) -> Self {
        Self { trip: Some(point) }
    }

    pub fn check(&mut self, point: PublicationFault) -> Result<(), SimulatedCrash> {
        if self.trip == Some(point) {
            self.trip = None;
            Err(SimulatedCrash)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublishError {
    #[error("cross-volume publish not allowed")]
    CrossVolume,
    #[error("ledger error: {0}")]
    Ledger(#[from] LedgerError),
    #[error("simulated crash")]
    SimulatedCrash,
    #[error("io: {0}")]
    Io(String),
}

impl From<std::io::Error> for PublishError {
    fn from(e: std::io::Error) -> Self {
        PublishError::Io(e.to_string())
    }
}

impl From<SimulatedCrash> for PublishError {
    fn from(_: SimulatedCrash) -> PublishError {
        PublishError::SimulatedCrash
    }
}

pub fn verify_stem_file(path: &Path, expected: &ExpectedStem) -> Result<(), LedgerError> {
    if path.is_symlink() {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    let meta = fs::metadata(path)?;
    if meta.len() != expected.byte_count {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    let actual =
        sha256_hex_file(path, DEFAULT_STREAM_CHUNK).map_err(|e| LedgerError::Io(e.to_string()))?;
    if actual != expected.sha256 {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    if path.extension().map(|e| e != "wav").unwrap_or(true) {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    if expected.byte_count < 44 {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    let mut header = [0u8; 44];
    let mut f = File::open(path)?;
    f.read_exact(&mut header)
        .map_err(|e| LedgerError::Io(e.to_string()))?;
    let data_len = validate_canonical_pcm_wav_header(&header, Some(meta.len()))
        .map_err(|e| LedgerError::Io(e.to_string()))?;
    let expected_data = expected.byte_count.saturating_sub(44);
    if data_len != expected_data {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    Ok(())
}

fn session_entry_path(session_dir: &Path, file_name: &str) -> Result<PathBuf, LedgerError> {
    validate_stem_file_name(file_name)?;
    let base = session_dir
        .canonicalize()
        .map_err(|e| LedgerError::Io(e.to_string()))?;
    let candidate = base.join(file_name);
    if candidate
        .parent()
        .map(|p| p != base.as_path())
        .unwrap_or(true)
    {
        return Err(LedgerError::InvalidStemManifest {
            reason: "stem path escapes session root".into(),
        });
    }
    Ok(candidate)
}

pub fn verify_all_stems(session_dir: &Path, stems: &[ExpectedStem]) -> Result<(), LedgerError> {
    validate_stem_manifest(stems)?;
    let mut expected_names: HashSet<String> = HashSet::new();
    for stem in stems {
        expected_names.insert(stem.file_name.clone());
        let path = session_entry_path(session_dir, &stem.file_name)?;
        if !path.is_file() {
            return Err(LedgerError::StemMismatch {
                file_name: stem.file_name.clone(),
            });
        }
        verify_stem_file(&path, stem)?;
    }
    let entries = fs::read_dir(session_dir).map_err(LedgerError::from)?;
    for entry in entries {
        let entry = entry.map_err(LedgerError::from)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !expected_names.contains(&name) {
            return Err(LedgerError::UnexpectedSessionEntry { name });
        }
    }
    Ok(())
}

fn staging_complete(staging_dir: &Path, stems: &[ExpectedStem]) -> Result<(), LedgerError> {
    verify_all_stems(staging_dir, stems)
}

fn fsync_file_path(path: &Path) -> Result<()> {
    let f = File::open(path)?;
    f.sync_all()?;
    Ok(())
}

fn fsync_staging_for_publication(
    record: &DeliveryOwnershipRecord,
    mut fault: Option<&mut FaultInjector>,
) -> Result<(), PublishError> {
    for stem in &record.stems {
        let path = record.staging_session_dir.join(&stem.file_name);
        fsync_file_path(&path).map_err(|e| PublishError::Io(e.to_string()))?;
    }
    sync_parent_dir(&record.staging_session_dir).map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(parent) = record.staging_session_dir.parent() {
        sync_parent_dir(parent).map_err(|e| PublishError::Io(e.to_string()))?;
    }
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterStagingStemFsync)?;
    }
    sync_parent_dir(&record.staging_session_dir).map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterStagingDirFsync)?;
    }
    Ok(())
}

fn rename_no_replace(src: &Path, dst: &Path) -> Result<(), PublishError> {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let src_c = CString::new(src.as_os_str().as_bytes())
            .map_err(|_| PublishError::Io("invalid src path".into()))?;
        let dst_c = CString::new(dst.as_os_str().as_bytes())
            .map_err(|_| PublishError::Io("invalid dst path".into()))?;
        let rc = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                src_c.as_ptr(),
                libc::AT_FDCWD,
                dst_c.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if rc != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::AlreadyExists {
                return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
            }
            return Err(PublishError::Io(err.to_string()));
        }
        return Ok(());
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        #[cfg(target_os = "macos")]
        {
            const RENAME_EXCL: u32 = 0x0000_0004;
            let src_c = CString::new(src.as_os_str().as_bytes())
                .map_err(|_| PublishError::Io("invalid src path".into()))?;
            let dst_c = CString::new(dst.as_os_str().as_bytes())
                .map_err(|_| PublishError::Io("invalid dst path".into()))?;
            let rc = unsafe { libc::renamex_np(src_c.as_ptr(), dst_c.as_ptr(), RENAME_EXCL) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
                }
                return Err(PublishError::Io(err.to_string()));
            }
            return Ok(());
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (src, dst);
            return Err(PublishError::Io(
                "atomic no-replace directory publish unsupported on this Unix".into(),
            ));
        }
    }

    #[cfg(windows)]
    {
        windows_publish_directory(src, dst)?;
        return Ok(());
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (src, dst);
        Err(PublishError::Io(
            "unsupported platform for directory publish".into(),
        ))
    }
}

/// Same-volume directory publication primitive (Unix: rename no-replace; Windows: `MoveFileExW` write-through).
/// Never replaces or deletes an existing destination; v2 recovery handles republish via the ledger.
pub fn publish_directory_same_volume(src: &Path, dst: &Path) -> Result<(), PublishError> {
    let src_parent = src
        .parent()
        .ok_or_else(|| PublishError::Io("src has no parent".into()))?;
    let dst_parent = dst
        .parent()
        .ok_or_else(|| PublishError::Io("dst has no parent".into()))?;
    durable_create_dir_all(dst_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    if !same_volume(src_parent, dst_parent).map_err(|e| PublishError::Io(e.to_string()))? {
        return Err(PublishError::CrossVolume);
    }

    rename_no_replace(src, dst)?;

    sync_parent_dir(src_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    sync_parent_dir(dst_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    Ok(())
}

pub fn publish_directory_same_volume_with_fault(
    src: &Path,
    dst: &Path,
    mut fault: Option<&mut FaultInjector>,
) -> Result<(), PublishError> {
    let src_parent = src
        .parent()
        .ok_or_else(|| PublishError::Io("src has no parent".into()))?;
    let dst_parent = dst
        .parent()
        .ok_or_else(|| PublishError::Io("dst has no parent".into()))?;
    durable_create_dir_all(dst_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    if !same_volume(src_parent, dst_parent).map_err(|e| PublishError::Io(e.to_string()))? {
        return Err(PublishError::CrossVolume);
    }

    rename_no_replace(src, dst)?;

    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterDirectoryRename)?;
    }
    sync_parent_dir(src_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterRenameSourceParentFsync)?;
    }
    sync_parent_dir(dst_parent).map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterRenameDestParentFsync)?;
    }
    Ok(())
}

#[cfg(windows)]
fn windows_publish_directory(src: &Path, dst: &Path) -> Result<(), PublishError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

    let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
    let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), MOVEFILE_WRITE_THROUGH) };
    if ok == 0 {
        let err = unsafe { GetLastError() };
        const ERROR_ALREADY_EXISTS: u32 = 183;
        if err == ERROR_ALREADY_EXISTS {
            return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
        }
        return Err(PublishError::Io(format!("MoveFileExW failed: {}", err)));
    }
    Ok(())
}

/// Persist `prepared`, publish the staged session directory, verify, then mark `published`.
pub fn publish_session_directory(
    ledger_path: &Path,
    record: &DeliveryOwnershipRecord,
    mut fault: Option<&mut FaultInjector>,
) -> Result<DeliveryOwnershipRecord, PublishError> {
    let lock_path = ledger_path
        .parent()
        .map(|p| p.join("publication.lock"))
        .ok_or_else(|| PublishError::Io("ledger path has no parent".into()))?;
    let _lock = acquire_publication_lock(&lock_path).map_err(PublishError::from)?;
    publish_session_directory_locked(ledger_path, record, fault)
}

fn publish_session_directory_locked(
    ledger_path: &Path,
    record: &DeliveryOwnershipRecord,
    mut fault: Option<&mut FaultInjector>,
) -> Result<DeliveryOwnershipRecord, PublishError> {
    if record.state != OwnershipState::Prepared {
        return Err(PublishError::Ledger(LedgerError::StagingIncomplete));
    }
    staging_complete(&record.staging_session_dir, &record.stems)?;

    let prepared = DeliveryOwnershipRecord {
        state: OwnershipState::Prepared,
        ..record.clone()
    };
    write_ownership_ledger(ledger_path, &prepared, None)
        .map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterPreparedPersist)?;
    }

    fsync_staging_for_publication(&prepared, fault.as_deref_mut())?;

    publish_directory_same_volume_with_fault(
        &record.staging_session_dir,
        &record.final_session_dir,
        fault.as_deref_mut(),
    )?;

    verify_all_stems(&record.final_session_dir, &record.stems)?;

    let published = DeliveryOwnershipRecord {
        state: OwnershipState::Published,
        ..prepared
    };
    write_ownership_ledger(ledger_path, &published, None)
        .map_err(|e| PublishError::Io(e.to_string()))?;
    Ok(published)
}

/// Startup / retry recovery for prepared→published transitions.
pub fn recover_publication(ledger_path: &Path) -> Result<DeliveryOwnershipRecord, PublishError> {
    let lock_path = ledger_path
        .parent()
        .map(|p| p.join("publication.lock"))
        .ok_or_else(|| PublishError::Io("ledger path has no parent".into()))?;
    let _lock = acquire_publication_lock(&lock_path).map_err(PublishError::from)?;

    let record = read_ownership_ledger(ledger_path).map_err(|e| PublishError::Io(e.to_string()))?;
    validate_stem_manifest(&record.stems).map_err(PublishError::from)?;

    match record.state {
        OwnershipState::Published => {
            if !record.final_session_dir.is_dir() {
                return Err(PublishError::Ledger(LedgerError::PublishedStateInvalid));
            }
            if record.staging_session_dir.exists() {
                return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
            }
            verify_all_stems(&record.final_session_dir, &record.stems)?;
            Ok(record)
        }
        OwnershipState::Prepared => {
            let staging = record.staging_session_dir.is_dir();
            let final_exists = record.final_session_dir.is_dir();

            if final_exists {
                if staging {
                    return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
                }
                verify_all_stems(&record.final_session_dir, &record.stems)?;
                let published = DeliveryOwnershipRecord {
                    state: OwnershipState::Published,
                    ..record
                };
                write_ownership_ledger(ledger_path, &published, None)
                    .map_err(|e| PublishError::Io(e.to_string()))?;
                return Ok(published);
            }

            if staging {
                return publish_session_directory_locked(ledger_path, &record, None);
            }

            Err(PublishError::Ledger(LedgerError::StagingIncomplete))
        }
    }
}

fn ledger_matches_destination_request(
    record: &DeliveryOwnershipRecord,
    delivery_id: &str,
    manifest_hash: &str,
    final_session_dir: &Path,
) -> Result<(), LedgerError> {
    if record.delivery_id != delivery_id || record.manifest_hash != manifest_hash {
        return Err(LedgerError::UnrelatedDestination);
    }
    let normalized_final = normalize_session_path(final_session_dir)?;
    let normalized_record = normalize_session_path(&record.final_session_dir)?;
    if normalized_final != normalized_record {
        return Err(LedgerError::UnrelatedDestination);
    }
    Ok(())
}

/// Refuse to publish when an unrelated final directory already exists.
pub fn assert_destination_available(
    final_session_dir: &Path,
    ledger_path: &Path,
    delivery_id: &str,
    manifest_hash: &str,
) -> Result<(), LedgerError> {
    if !final_session_dir.exists() {
        return Ok(());
    }
    if !ledger_path.is_file() {
        return Err(LedgerError::UnrelatedDestination);
    }
    let record = read_ownership_ledger(ledger_path).map_err(|e| LedgerError::Io(e.to_string()))?;
    ledger_matches_destination_request(&record, delivery_id, manifest_hash, final_session_dir)?;
    if record.state != OwnershipState::Published {
        return Err(LedgerError::PublishedStateInvalid);
    }
    if !record.final_session_dir.is_dir() {
        return Err(LedgerError::PublishedStateInvalid);
    }
    verify_all_stems(&record.final_session_dir, &record.stems)?;
    Ok(())
}

/// Idempotent API commit replay marker (spike stand-in for per-file commit state).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitReplayState {
    pub delivery_id: String,
    pub manifest_hash: String,
    pub committed_file_ids: Vec<String>,
}

pub fn replay_file_commit(
    state: &mut CommitReplayState,
    delivery_id: &str,
    manifest_hash: &str,
    file_id: &str,
) -> Result<bool, LedgerError> {
    if state.delivery_id != delivery_id || state.manifest_hash != manifest_hash {
        return Err(LedgerError::ManifestMismatch);
    }
    if state.committed_file_ids.iter().any(|id| id == file_id) {
        return Ok(false);
    }
    state.committed_file_ids.push(file_id.to_string());
    Ok(true)
}

/// Materialize a synthetic WAV-shaped stem on disk by streaming (bounded RAM).
pub fn write_synthetic_wav_stem(
    path: &Path,
    data_bytes: u64,
    chunk_size: usize,
    binding: PartialResumeBinding,
) -> Result<ExpectedStem> {
    reject_zero_chunk(chunk_size).map_err(|e| anyhow!(e))?;
    let header = minimal_wav_header(data_bytes)?;
    let total = data_bytes + header.len() as u64;
    if let Some(parent) = path.parent() {
        durable_create_dir_all(parent)?;
    }
    let mut writer = PartialFileWriter::open(path, total, binding)?;
    writer.write_range(0, &header)?;
    let mut remaining = data_bytes;
    let mut offset = header.len() as u64;
    let mut chunk_buf = vec![0u8; chunk_size];
    while remaining > 0 {
        let take = remaining.min(chunk_size as u64) as usize;
        for i in 0..take {
            chunk_buf[i] = SyntheticByteSource::byte_at(offset + i as u64);
        }
        writer.write_range(offset, &chunk_buf[..take])?;
        offset += take as u64;
        remaining -= take as u64;
    }
    writer.fsync()?;
    drop(writer);
    let _ = fs::remove_file(partial_checkpoint_path(path));
    let sha256 = sha256_hex_file(path, chunk_size)?;
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("stem.wav")
        .to_string();
    Ok(ExpectedStem {
        file_name,
        byte_count: total,
        sha256,
    })
}

/// Extend a sparse partial file past `offset` without filling intermediate bytes (holes read as 0).
pub fn sparse_extend_file(path: &Path, new_len: u64) -> Result<()> {
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;
    f.seek(SeekFrom::Start(new_len.saturating_sub(1)))?;
    f.write_all(&[0])?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ss-delivery-spike-{}-{}", label, nanos))
    }

    fn test_binding(label: &str) -> PartialResumeBinding {
        PartialResumeBinding::new(format!("test-binding-{label}"))
    }

    fn minimal_stem(dir: &Path, name: &str, data_bytes: u64) -> ExpectedStem {
        write_synthetic_wav_stem(&dir.join(name), data_bytes, 4096, test_binding(name)).unwrap()
    }

    #[test]
    fn plan_duration_bounds_match_unsigned_expectations() {
        assert_eq!(
            validate_plan_session_bounds(SESSION_4_5H_SAMPLE_FRAMES).unwrap(),
            SESSION_4_5H_DATA_BYTES
        );
        assert_eq!(
            validate_plan_session_bounds(SESSION_6H_SAMPLE_FRAMES).unwrap(),
            SESSION_6H_DATA_BYTES
        );
        assert!(SESSION_6H_DATA_BYTES < RIFF_MAX_CHUNK_BYTES);
        let over_frames = (RIFF_MAX_CHUNK_BYTES / 4) + 1;
        assert_eq!(
            validate_plan_session_bounds(over_frames),
            Err(BoundsError::ExceedsRiffLimit)
        );
    }

    #[test]
    fn riff_max_data_bytes_exact_limit_and_reject_one_over() {
        assert_eq!(RIFF_MAX_CHUNK_BYTES, 4_294_967_256);
        assert_eq!(
            RIFF_MAX_CHUNK_BYTES,
            ((u32::MAX as u64) - RIFF_PCM_OVERHEAD_BYTES) / STEREO_PCM_FRAME_BYTES
                * STEREO_PCM_FRAME_BYTES
        );
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES).is_ok());
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES + 4).is_err());
        assert!(minimal_wav_header(RIFF_MAX_CHUNK_BYTES + 1).is_err());
        let max_aligned_frames = RIFF_MAX_CHUNK_BYTES / 4;
        assert_eq!(
            wav_data_bytes_for_frames(max_aligned_frames, CHANNELS_STEREO, BYTES_PER_SAMPLE)
                .unwrap(),
            max_aligned_frames * 4
        );
        assert_eq!(
            wav_data_bytes_for_frames(max_aligned_frames + 1, CHANNELS_STEREO, BYTES_PER_SAMPLE),
            Err(BoundsError::ExceedsRiffLimit)
        );
    }

    #[test]
    fn partial_writer_resumes_contiguous_ranges() {
        let dir = unique_temp_dir("partial");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("Alice.wav.partial");
        let expected = 1_000u64;
        let binding = test_binding("partial-resume");
        let mut w = PartialFileWriter::open(&path, expected, binding.clone()).unwrap();
        w.write_range(0, &[1, 2, 3]).unwrap();
        w.write_range(3, &[4, 5]).unwrap();
        w.fsync().unwrap();
        assert_eq!(w.contiguous_len(), 5);
        let mut w2 = PartialFileWriter::open(&path, expected, binding).unwrap();
        assert_eq!(w2.contiguous_len(), 5);
        w2.write_range(5, &[6]).unwrap();
        w2.fsync().unwrap();
        assert_eq!(fs::read(&path).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_oversized_existing_file() {
        let dir = unique_temp_dir("oversized-partial");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.partial");
        let binding = test_binding("oversized");
        let mut w = PartialFileWriter::open(&path, 100, binding.clone()).unwrap();
        w.write_range(0, &[0; 100]).unwrap();
        w.fsync().unwrap();
        sparse_extend_file(&path, 500).unwrap();
        assert!(matches!(
            PartialFileWriter::open(&path, 100, binding),
            Err(PartialWriteError::OversizedExisting {
                existing: 500,
                expected: 100,
            })
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_holes() {
        let dir = unique_temp_dir("hole");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.partial");
        let mut w = PartialFileWriter::open(&path, 100, test_binding("hole")).unwrap();
        w.write_range(0, &[1]).unwrap();
        let err = w.write_range(2, &[2]).unwrap_err();
        assert_eq!(
            err,
            PartialWriteError::NonContiguous {
                offset: 2,
                expected: 1,
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn streaming_hash_matches_materialized_file() {
        let dir = unique_temp_dir("hash");
        fs::create_dir_all(&dir).unwrap();
        let data_bytes = 512 * 1024;
        let stem = write_synthetic_wav_stem(
            &dir.join("a.wav"),
            data_bytes,
            64 * 1024,
            test_binding("stream-hash"),
        )
        .unwrap();
        let reread = sha256_hex_file(&dir.join("a.wav"), DEFAULT_STREAM_CHUNK).unwrap();
        assert_eq!(reread, stem.sha256);
        let header_len = minimal_wav_header(data_bytes).unwrap().len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(&minimal_wav_header(data_bytes).unwrap());
        let mut offset = header_len;
        let mut remaining = data_bytes;
        let mut buf = vec![0u8; 32 * 1024];
        while remaining > 0 {
            let take = remaining.min(32 * 1024) as usize;
            for i in 0..take {
                buf[i] = SyntheticByteSource::byte_at(offset + i as u64);
            }
            hasher.update(&buf[..take]);
            offset += take as u64;
            remaining -= take as u64;
        }
        assert_eq!(hex_digest(&hasher.finalize()), stem.sha256);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn checksum_mismatch_blocks_publish() {
        let dir = unique_temp_dir("bad-hash");
        fs::create_dir_all(&dir).unwrap();
        let staging = dir.join("staging");
        fs::create_dir_all(&staging).unwrap();
        let stem =
            write_synthetic_wav_stem(&staging.join("a.wav"), 1024, 4096, test_binding("bad-hash"))
                .unwrap();
        let file_name = stem.file_name.clone();
        let bad = ExpectedStem {
            sha256: "00".repeat(32),
            ..stem
        };
        let err = verify_stem_file(&staging.join("a.wav"), &bad).unwrap_err();
        assert_eq!(err, LedgerError::StemMismatch { file_name });
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepared_survives_crash_before_rename_and_recovers() {
        let parent = unique_temp_dir("crash-before");
        fs::create_dir_all(&parent).unwrap();
        let delivery_id = "delivery-1";
        let staging = parent
            .join(".syndicate-staging")
            .join(delivery_id)
            .join("session");
        fs::create_dir_all(&staging).unwrap();
        let stem = write_synthetic_wav_stem(
            &staging.join("Alice.wav"),
            50_000,
            8192,
            test_binding("alice"),
        )
        .unwrap();
        let final_dir = parent.join("guild").join("channel-01012025000000");
        let ledger = ownership_ledger_path(&parent, delivery_id);
        let record = DeliveryOwnershipRecord {
            delivery_id: delivery_id.to_string(),
            manifest_hash: "abc123".to_string(),
            staging_session_dir: staging.clone(),
            final_session_dir: final_dir.clone(),
            stems: vec![stem],
            state: OwnershipState::Prepared,
        };

        let mut fault = FaultInjector::trip_once(PublicationFault::AfterPreparedPersist);
        let err = publish_session_directory(&ledger, &record, Some(&mut fault)).unwrap_err();
        assert_eq!(err, PublishError::SimulatedCrash);
        assert!(ledger.is_file());
        assert!(staging.is_dir());
        assert!(!final_dir.exists());

        let recovered = recover_publication(&ledger).unwrap();
        assert_eq!(recovered.state, OwnershipState::Published);
        assert!(final_dir.is_dir());
        assert!(!staging.exists());
        verify_all_stems(&final_dir, &recovered.stems).unwrap();
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn crash_after_rename_advances_ledger_when_hashes_match() {
        let parent = unique_temp_dir("crash-after");
        fs::create_dir_all(&parent).unwrap();
        let delivery_id = "delivery-2";
        let staging = parent
            .join(".syndicate-staging")
            .join(delivery_id)
            .join("session");
        fs::create_dir_all(&staging).unwrap();
        let stem =
            write_synthetic_wav_stem(&staging.join("Bob.wav"), 80_000, 8192, test_binding("bob"))
                .unwrap();
        let final_dir = parent.join("guild").join("channel-02022025000000");
        let ledger = ownership_ledger_path(&parent, delivery_id);
        let record = DeliveryOwnershipRecord {
            delivery_id: delivery_id.to_string(),
            manifest_hash: "def456".to_string(),
            staging_session_dir: staging.clone(),
            final_session_dir: final_dir.clone(),
            stems: vec![stem.clone()],
            state: OwnershipState::Prepared,
        };
        write_ownership_ledger(&ledger, &record, None).unwrap();

        publish_directory_same_volume(&staging, &final_dir).unwrap();
        assert!(final_dir.is_dir());
        assert!(!staging.exists());

        let recovered = recover_publication(&ledger).unwrap();
        assert_eq!(recovered.state, OwnershipState::Published);
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn publish_never_replaces_existing_destination_bytes() {
        let parent = unique_temp_dir("no-replace");
        fs::create_dir_all(&parent).unwrap();
        let src = parent.join("staging-session");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("stem.wav"), b"staging-bytes").unwrap();
        let dst = parent.join("final-session");
        fs::create_dir_all(&dst).unwrap();
        let intruder_path = dst.join("intruder.wav");
        let before = b"keep-this-exactly";
        fs::write(&intruder_path, before).unwrap();

        let err = publish_directory_same_volume(&src, &dst).unwrap_err();
        assert_eq!(err, PublishError::Ledger(LedgerError::UnrelatedDestination));
        assert!(src.is_dir());
        assert!(dst.is_dir());
        assert_eq!(fs::read(&intruder_path).unwrap(), before);
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn ledger_atomic_replace_survives_fault_at_each_step() {
        let dir = unique_temp_dir("ledger-fault");
        fs::create_dir_all(&dir).unwrap();
        let ledger = dir.join("ownership.json");
        let stem = minimal_stem(&dir, "x.wav", 64);
        let prepared = DeliveryOwnershipRecord {
            delivery_id: "d".into(),
            manifest_hash: "mh".into(),
            staging_session_dir: dir.join("staging"),
            final_session_dir: dir.join("final"),
            stems: vec![stem],
            state: OwnershipState::Prepared,
        };
        let published = DeliveryOwnershipRecord {
            state: OwnershipState::Published,
            ..prepared.clone()
        };
        write_ownership_ledger(&ledger, &prepared, None).unwrap();
        let published_bytes = serde_json::to_vec_pretty(&published).unwrap();

        for point in [
            LedgerAtomicFault::AfterTempFsync,
            LedgerAtomicFault::AfterReplace,
            LedgerAtomicFault::AfterParentFsync,
        ] {
            write_bytes_atomic(
                &ledger,
                &serde_json::to_vec_pretty(&prepared).unwrap(),
                None,
            )
            .unwrap();
            let mut inj = LedgerFaultInjector::trip_once(point);
            let err = write_bytes_atomic(&ledger, &published_bytes, Some(&mut inj)).unwrap_err();
            assert!(err.to_string().contains("simulated crash"));
            let record: DeliveryOwnershipRecord =
                serde_json::from_slice(&fs::read(&ledger).unwrap()).unwrap();
            match point {
                LedgerAtomicFault::AfterTempFsync => {
                    assert_eq!(record.state, OwnershipState::Prepared);
                }
                LedgerAtomicFault::AfterReplace | LedgerAtomicFault::AfterParentFsync => {
                    assert_eq!(record.state, OwnershipState::Published);
                }
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn wav_header_read_uses_fixed_44_bytes_without_usize_file_length() {
        let dir = unique_temp_dir("large-header");
        fs::create_dir_all(&dir).unwrap();
        let data_bytes = 1_024u64;
        let header = minimal_wav_header(data_bytes).unwrap();
        let path = dir.join("sparse.wav");
        let total_len = u64::from(u32::MAX) + 4096;
        sparse_extend_file(&path, total_len).unwrap();
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all(&header).unwrap();
        f.sync_all().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), total_len);
        let mut header_buf = [0u8; 44];
        let mut rf = File::open(&path).unwrap();
        rf.read_exact(&mut header_buf).unwrap();
        assert_eq!(parse_wav_data_chunk_len(&header_buf).unwrap(), data_bytes);
        assert!(validate_canonical_pcm_wav_header(&header_buf, Some(total_len)).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn unrelated_destination_fails_closed() {
        let parent = unique_temp_dir("collision");
        fs::create_dir_all(&parent).unwrap();
        let final_dir = parent.join("guild").join("channel-existing");
        fs::create_dir_all(&final_dir).unwrap();
        fs::write(final_dir.join("intruder.wav"), b"not ours").unwrap();
        let ledger = ownership_ledger_path(&parent, "other");
        let err = assert_destination_available(&final_dir, &ledger, "d1", "mh1").unwrap_err();
        assert_eq!(err, LedgerError::UnrelatedDestination);
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn published_replay_commits_are_idempotent() {
        let mut state = CommitReplayState {
            delivery_id: "d".into(),
            manifest_hash: "mh".into(),
            committed_file_ids: vec![],
        };
        assert!(replay_file_commit(&mut state, "d", "mh", "f1").unwrap());
        assert!(!replay_file_commit(&mut state, "d", "mh", "f1").unwrap());
        assert_eq!(state.committed_file_ids.len(), 1);
        let err = replay_file_commit(&mut state, "d", "other", "f2").unwrap_err();
        assert_eq!(err, LedgerError::ManifestMismatch);
    }

    #[test]
    fn u64_offset_write_near_three_gib_boundary_without_full_allocation() {
        let dir = unique_temp_dir("sparse-offset");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("big.partial");
        let logical_len = SESSION_4_5H_DATA_BYTES + 44;
        let near_end: u64 = logical_len - 128;
        assert!(logical_len > 3_000_000_000);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        f.write_all(&minimal_wav_header(SESSION_4_5H_DATA_BYTES).unwrap())
            .unwrap();
        f.seek(SeekFrom::Start(near_end)).unwrap();
        f.write_all(&[7u8; 64]).unwrap();
        f.sync_all().unwrap();
        let meta = fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), near_end + 64);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore = "manual stress: streams logical >3 GiB through SHA-256 (slow)"]
    fn manual_stress_sparse_three_gib_synthetic_hash() {
        let dir = unique_temp_dir("stress-hash");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("stress.wav");
        sparse_extend_file(&path, SESSION_4_5H_DATA_BYTES + 44).unwrap();
        let header = minimal_wav_header(SESSION_4_5H_DATA_BYTES).unwrap();
        let mut w =
            PartialFileWriter::open(&path, SESSION_4_5H_DATA_BYTES + 44, test_binding("stress"))
                .unwrap();
        w.write_range(0, &header).unwrap();
        w.fsync().unwrap();
        let h = sha256_hex_file(&path, DEFAULT_STREAM_CHUNK).unwrap();
        assert_eq!(h.len(), 64);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn session_directory_visible_only_after_all_stems_verify() {
        let parent = unique_temp_dir("all-stems");
        fs::create_dir_all(&parent).unwrap();
        let delivery_id = "delivery-3";
        let staging = parent
            .join(".syndicate-staging")
            .join(delivery_id)
            .join("session");
        fs::create_dir_all(&staging).unwrap();
        let a = write_synthetic_wav_stem(&staging.join("A.wav"), 10_000, 4096, test_binding("A"))
            .unwrap();
        let b = write_synthetic_wav_stem(&staging.join("B.wav"), 20_000, 4096, test_binding("B"))
            .unwrap();
        let final_dir = parent.join("guild").join("channel-03032025000000");
        let ledger = ownership_ledger_path(&parent, delivery_id);
        let record = DeliveryOwnershipRecord {
            delivery_id: delivery_id.to_string(),
            manifest_hash: "ghi789".to_string(),
            staging_session_dir: staging,
            final_session_dir: final_dir.clone(),
            stems: vec![a, b],
            state: OwnershipState::Prepared,
        };
        let published = publish_session_directory(&ledger, &record, None).unwrap();
        assert_eq!(published.state, OwnershipState::Published);
        assert_eq!(published.stems.len(), 2);
        for stem in &published.stems {
            verify_stem_file(&final_dir.join(&stem.file_name), stem).unwrap();
        }
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn chunk_size_zero_rejected_by_hash_and_synthetic_helpers() {
        let dir = unique_temp_dir("chunk-zero");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bin");
        fs::write(&path, b"abc").unwrap();
        assert_eq!(
            sha256_hex_file(&path, 0).unwrap_err().to_string(),
            PartialWriteError::InvalidChunkSize.to_string()
        );
        assert_eq!(
            sha256_hex_reader(File::open(&path).unwrap(), 0)
                .unwrap_err()
                .to_string(),
            PartialWriteError::InvalidChunkSize.to_string()
        );
        assert_eq!(
            streaming_sha256_synthetic(10, 0).unwrap_err().to_string(),
            PartialWriteError::InvalidChunkSize.to_string()
        );
        assert_eq!(
            write_synthetic_wav_stem(&dir.join("z.wav"), 64, 0, test_binding("z"))
                .unwrap_err()
                .to_string(),
            PartialWriteError::InvalidChunkSize.to_string()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_valid_resume_requires_checkpoint_not_length() {
        let dir = unique_temp_dir("checkpoint-resume");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.partial");
        let binding = test_binding("resume");
        let mut w = PartialFileWriter::open(&path, 20, binding.clone()).unwrap();
        w.write_range(0, &[1, 2, 3, 4, 5]).unwrap();
        w.fsync().unwrap();
        w.write_range(5, &[6, 7, 8, 9, 10]).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 10);
        let mut w2 = PartialFileWriter::open(&path, 20, binding).unwrap();
        assert_eq!(w2.contiguous_len(), 5);
        assert_eq!(fs::metadata(&path).unwrap().len(), 5);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_corrupt_checkpoint_prefix() {
        let dir = unique_temp_dir("corrupt-prefix");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.partial");
        let binding = test_binding("corrupt");
        let mut w = PartialFileWriter::open(&path, 20, binding.clone()).unwrap();
        w.write_range(0, &[1, 2, 3, 4, 5]).unwrap();
        w.fsync().unwrap();
        fs::write(&path, &[9; 20]).unwrap();
        assert!(matches!(
            PartialFileWriter::open(&path, 20, binding),
            Err(PartialWriteError::PrefixDigestMismatch)
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_foreign_binding_and_sparse_tail() {
        let dir = unique_temp_dir("foreign-sparse");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.partial");
        let binding = test_binding("foreign");
        let mut w = PartialFileWriter::open(&path, 16, binding.clone()).unwrap();
        w.write_range(0, &[1, 2, 3, 4]).unwrap();
        w.fsync().unwrap();
        w.write_range(4, &[5, 6, 7, 8]).unwrap();
        assert!(matches!(
            PartialFileWriter::open(&path, 16, PartialResumeBinding::new("other")),
            Err(PartialWriteError::BindingMismatch)
        ));
        let mut reopened = PartialFileWriter::open(&path, 16, binding).unwrap();
        assert_eq!(reopened.contiguous_len(), 4);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_bytes_without_checkpoint() {
        let dir = unique_temp_dir("no-checkpoint");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.partial");
        fs::write(&path, &[1, 2, 3]).unwrap();
        assert!(matches!(
            PartialFileWriter::open(&path, 10, test_binding("none")),
            Err(PartialWriteError::CheckpointInvalid)
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn canonical_wav_header_mutations_fail_verification() {
        let data_bytes = 128u64;
        let header = minimal_wav_header(data_bytes).unwrap();
        let total = 44 + data_bytes;
        let dir = unique_temp_dir("wav-mut");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.wav");
        let mut valid = header.clone();
        valid.extend_from_slice(&vec![0u8; data_bytes as usize]);
        fs::write(&path, &valid).unwrap();
        let sha256 = sha256_hex_file(&path, DEFAULT_STREAM_CHUNK).unwrap();
        let stem = ExpectedStem {
            file_name: "m.wav".into(),
            byte_count: total,
            sha256,
        };
        verify_stem_file(&path, &stem).unwrap();
        let cases: Vec<(usize, u8)> = vec![
            (0, b'X'),
            (8, b'X'),
            (12, b'X'),
            (20, 2),
            (22, 1),
            (24, 1),
            (28, 1),
            (32, 1),
            (34, 8),
            (36, b'X'),
            (40, 1),
        ];
        for (idx, value) in cases {
            let mut h = header.clone();
            h[idx] = value;
            fs::write(
                &path,
                [&h[..], &vec![0u8; data_bytes as usize][..]].concat(),
            )
            .unwrap();
            assert!(
                verify_stem_file(&path, &stem).is_err(),
                "mutation at byte {idx} should fail"
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn stem_manifest_rejects_empty_duplicate_and_traversal() {
        assert!(matches!(
            validate_stem_manifest(&[]),
            Err(LedgerError::InvalidStemManifest { .. })
        ));
        let dup = vec![
            ExpectedStem {
                file_name: "a.wav".into(),
                byte_count: 44,
                sha256: "00".repeat(32),
            },
            ExpectedStem {
                file_name: "a.wav".into(),
                byte_count: 44,
                sha256: "11".repeat(32),
            },
        ];
        assert!(validate_stem_manifest(&dup).is_err());
        assert!(validate_stem_file_name("../x.wav").is_err());
        assert!(validate_stem_file_name("sub/x.wav").is_err());
    }

    #[test]
    fn verify_all_stems_rejects_unexpected_directory_entry() {
        let dir = unique_temp_dir("unexpected-file");
        fs::create_dir_all(&dir).unwrap();
        let stem = minimal_stem(&dir, "only.wav", 64);
        fs::write(dir.join("extra.txt"), b"x").unwrap();
        let err = verify_all_stems(&dir, &[stem]).unwrap_err();
        assert_eq!(
            err,
            LedgerError::UnexpectedSessionEntry {
                name: "extra.txt".into()
            }
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn verify_all_stems_rejects_symlink_escape() {
        let dir = unique_temp_dir("symlink");
        fs::create_dir_all(&dir).unwrap();
        let outside = dir.join("outside.wav");
        let _ = minimal_stem(&dir, "outside.wav", 64);
        let stem = ExpectedStem {
            file_name: "link.wav".into(),
            byte_count: 44,
            sha256: "00".repeat(32),
        };
        std::os::unix::fs::symlink(&outside, dir.join("link.wav")).unwrap();
        assert!(verify_all_stems(&dir, &[stem]).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_publish_exactly_one_wins() {
        let parent = unique_temp_dir("race-publish");
        fs::create_dir_all(&parent).unwrap();
        let dst = parent.join("final-session");
        fs::create_dir_all(&parent.join("guild")).unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let barrier1 = Arc::clone(&barrier);
        let barrier2 = Arc::clone(&barrier);
        let dst_path = dst.clone();
        let dst_check = dst.clone();
        let parent_a = parent.clone();
        let parent_b = parent.clone();
        let t1 = thread::spawn(move || {
            let src = parent_a.join("staging-a");
            fs::create_dir_all(&src).unwrap();
            fs::write(src.join("a.wav"), b"a").unwrap();
            barrier1.wait();
            publish_directory_same_volume(&src, &dst_path)
        });
        let t2 = thread::spawn(move || {
            let src = parent_b.join("staging-b");
            fs::create_dir_all(&src).unwrap();
            fs::write(src.join("b.wav"), b"b").unwrap();
            barrier2.wait();
            publish_directory_same_volume(&src, &dst)
        });
        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();
        let oks = usize::from(r1.is_ok()) + usize::from(r2.is_ok());
        assert_eq!(oks, 1);
        assert!(dst_check.is_dir());
        let has_a = dst_check.join("a.wav").is_file();
        let has_b = dst_check.join("b.wav").is_file();
        assert_ne!(has_a, has_b);
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn publication_lock_denies_conflicting_owner() {
        let dir = unique_temp_dir("pub-lock");
        fs::create_dir_all(&dir).unwrap();
        let lock = dir.join("publication.lock");
        let guard = acquire_publication_lock(&lock).unwrap();
        assert!(matches!(
            acquire_publication_lock(&lock),
            Err(LedgerError::PublicationLocked)
        ));
        drop(guard);
        let _ = acquire_publication_lock(&lock).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn recover_published_requires_final_stems() {
        let parent = unique_temp_dir("published-recover");
        fs::create_dir_all(&parent).unwrap();
        let delivery_id = "delivery-published";
        let final_dir = parent.join("guild").join("channel-final");
        fs::create_dir_all(&final_dir).unwrap();
        let stem = minimal_stem(&final_dir, "Only.wav", 128);
        let ledger = ownership_ledger_path(&parent, delivery_id);
        let record = DeliveryOwnershipRecord {
            delivery_id: delivery_id.into(),
            manifest_hash: "mh".into(),
            staging_session_dir: parent.join("missing-staging"),
            final_session_dir: final_dir.clone(),
            stems: vec![stem.clone()],
            state: OwnershipState::Published,
        };
        write_ownership_ledger(&ledger, &record, None).unwrap();
        recover_publication(&ledger).unwrap();
        fs::remove_file(final_dir.join("Only.wav")).unwrap();
        assert!(matches!(
            recover_publication(&ledger).unwrap_err(),
            PublishError::Ledger(LedgerError::StemMismatch { .. })
        ));
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn assert_destination_requires_published_state_and_matching_bytes() {
        let parent = unique_temp_dir("assert-dest");
        fs::create_dir_all(&parent).unwrap();
        let delivery_id = "delivery-assert";
        let final_dir = parent.join("guild").join("channel-assert");
        fs::create_dir_all(&final_dir).unwrap();
        let stem = minimal_stem(&final_dir, "Track.wav", 256);
        let ledger = ownership_ledger_path(&parent, delivery_id);
        let prepared = DeliveryOwnershipRecord {
            delivery_id: delivery_id.into(),
            manifest_hash: "mh".into(),
            staging_session_dir: parent.join("staging"),
            final_session_dir: final_dir.clone(),
            stems: vec![stem.clone()],
            state: OwnershipState::Prepared,
        };
        write_ownership_ledger(&ledger, &prepared, None).unwrap();
        assert_eq!(
            assert_destination_available(&final_dir, &ledger, delivery_id, "mh").unwrap_err(),
            LedgerError::PublishedStateInvalid
        );
        let published = DeliveryOwnershipRecord {
            state: OwnershipState::Published,
            ..prepared
        };
        write_ownership_ledger(&ledger, &published, None).unwrap();
        assert_destination_available(&final_dir, &ledger, delivery_id, "mh").unwrap();
        fs::write(final_dir.join("Track.wav"), vec![0; 256]).unwrap();
        assert!(assert_destination_available(&final_dir, &ledger, delivery_id, "mh").is_err());
        let _ = fs::remove_dir_all(&parent);
    }

    #[test]
    fn publication_durability_fault_points_are_ordered() {
        let points = [
            PublicationFault::AfterPreparedPersist,
            PublicationFault::AfterStagingStemFsync,
            PublicationFault::AfterStagingDirFsync,
            PublicationFault::AfterDirectoryRename,
            PublicationFault::AfterRenameSourceParentFsync,
            PublicationFault::AfterRenameDestParentFsync,
        ];
        for (idx, point) in points.into_iter().enumerate() {
            let parent = unique_temp_dir(&format!("durability-order-{idx}"));
            fs::create_dir_all(&parent).unwrap();
            let delivery_id = "delivery-dur";
            let staging = parent
                .join(".syndicate-staging")
                .join(delivery_id)
                .join("session");
            fs::create_dir_all(&staging).unwrap();
            let stem =
                write_synthetic_wav_stem(&staging.join("Dur.wav"), 4096, 4096, test_binding("dur"))
                    .unwrap();
            let final_dir = parent.join("guild").join("channel-dur");
            let ledger = ownership_ledger_path(&parent, delivery_id);
            let record = DeliveryOwnershipRecord {
                delivery_id: delivery_id.into(),
                manifest_hash: "mh".into(),
                staging_session_dir: staging,
                final_session_dir: final_dir,
                stems: vec![stem],
                state: OwnershipState::Prepared,
            };
            let mut fault = FaultInjector::trip_once(point);
            let err = publish_session_directory(&ledger, &record, Some(&mut fault)).unwrap_err();
            assert_eq!(err, PublishError::SimulatedCrash, "point {point:?}");
            let _ = fs::remove_dir_all(&parent);
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_publish_directory_smoke() {
        let parent = unique_temp_dir("win-publish");
        fs::create_dir_all(&parent).unwrap();
        let src = parent.join("src-session");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("x.wav"), b"RIFF").unwrap();
        let dst = parent.join("dst-session");
        publish_directory_same_volume(&src, &dst).unwrap();
        assert!(dst.join("x.wav").is_file());
        assert!(!src.exists());
        let _ = fs::remove_dir_all(&parent);
    }
}
