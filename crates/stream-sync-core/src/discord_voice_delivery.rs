//! Phase 0C spike: large-file resumable ingest + durable session-directory publication.
//! Isolated from live `discord_voice` ingest; reusable primitives for v2 delivery.

#![allow(dead_code)]

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
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

/// Classic RIFF/WAV `data` chunk maximum (unsigned 32-bit chunk size field).
pub const RIFF_MAX_CHUNK_BYTES: u64 = 0xFFFF_FFFF;

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
    #[error("offset {offset} is beyond file capacity {capacity}")]
    OutOfRange { offset: u64, capacity: u64 },
    #[error("range length overflow")]
    LengthOverflow,
    #[error("non-contiguous write at {offset}, expected {expected}")]
    NonContiguous { offset: u64, expected: u64 },
    #[error("overlapping range disagrees with existing bytes")]
    OverlapMismatch,
    #[error("io error: {0}")]
    Io(String),
}

impl From<std::io::Error> for PartialWriteError {
    fn from(e: std::io::Error) -> Self {
        PartialWriteError::Io(e.to_string())
    }
}

/// Resumable ranged writer for a `.partial` artifact (u64 offsets end-to-end).
pub struct PartialFileWriter {
    path: PathBuf,
    expected_len: u64,
    contiguous_len: u64,
    file: File,
}

impl PartialFileWriter {
    pub fn open(path: impl AsRef<Path>, expected_len: u64) -> Result<Self, PartialWriteError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(PartialWriteError::from)?;
        }
        let contiguous_len = if path.is_file() {
            path.metadata().map_err(PartialWriteError::from)?.len()
        } else {
            0
        };
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(PartialWriteError::from)?;
        Ok(Self {
            path,
            expected_len,
            contiguous_len,
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
        self.contiguous_len
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

        if offset == self.contiguous_len {
            self.file
                .seek(SeekFrom::Start(offset))
                .map_err(PartialWriteError::from)?;
            self.file.write_all(data).map_err(PartialWriteError::from)?;
            self.contiguous_len = end;
            return Ok(());
        }

        if offset < self.contiguous_len {
            let overlap_end = end.min(self.contiguous_len);
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
            if end <= self.contiguous_len {
                return Ok(());
            }
            let append_offset = self.contiguous_len;
            self.file
                .seek(SeekFrom::Start(append_offset))
                .map_err(PartialWriteError::from)?;
            let skip = (append_offset - offset) as usize;
            self.file
                .write_all(&data[skip..])
                .map_err(PartialWriteError::from)?;
            self.contiguous_len = end;
            return Ok(());
        }

        Err(PartialWriteError::NonContiguous {
            offset,
            expected: self.contiguous_len,
        })
    }

    pub fn fsync(&self) -> Result<(), PartialWriteError> {
        self.file.sync_all().map_err(PartialWriteError::from)
    }
}

pub fn sha256_hex_file(path: &Path, chunk_size: usize) -> Result<String> {
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
    sha256_hex_reader(SyntheticByteSource::new(total_len), chunk_size)
}

/// Minimal WAV header for a fixed data chunk size (classic RIFF, not RF64).
pub fn minimal_wav_header(data_bytes: u64) -> Result<Vec<u8>> {
    if data_bytes > u32::MAX as u64 {
        return Err(anyhow!("WAV data size exceeds u32 RIFF chunk field"));
    }
    let data_u32 = data_bytes as u32;
    let riff_size = 36u32 + data_u32;
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
    if header.len() < 44 {
        return Err(anyhow!("WAV header too short"));
    }
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        return Err(anyhow!("not a RIFF WAVE file"));
    }
    if &header[36..40] != b"data" {
        return Err(anyhow!("missing data chunk"));
    }
    let data_len = u32::from_le_bytes(header[40..44].try_into().unwrap()) as u64;
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

fn write_ownership_ledger(path: &Path, record: &DeliveryOwnershipRecord) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let data = serde_json::to_vec_pretty(record)?;
    write_bytes_atomic(path, &data)
}

fn read_ownership_ledger(path: &Path) -> Result<DeliveryOwnershipRecord> {
    let data = fs::read(path).context("read ownership ledger")?;
    let record: DeliveryOwnershipRecord = serde_json::from_slice(&data)?;
    Ok(record)
}

fn write_bytes_atomic(target: &Path, data: &[u8]) -> Result<()> {
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
    if target.exists() {
        fs::remove_file(target)?;
    }
    fs::rename(&tmp, target)?;
    sync_parent_dir(target)?;
    Ok(())
}

pub fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        #[cfg(unix)]
        {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        #[cfg(not(unix))]
        {
            let _ = parent;
        }
    }
    Ok(())
}

pub fn same_volume(a: &Path, b: &Path) -> Result<bool> {
    let ma = fs::metadata(a)?;
    let mb = fs::metadata(b)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(ma.dev() == mb.dev())
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        Ok(ma.volume_serial_number() == mb.volume_serial_number())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (ma, mb);
        Ok(true)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationFault {
    AfterPreparedPersist,
    AfterDirectoryRename,
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
    let header_len = 44usize.min(expected.byte_count as usize);
    let mut header = vec![0u8; header_len];
    let mut f = File::open(path)?;
    f.read_exact(&mut header)?;
    let data_len = parse_wav_data_chunk_len(&header).map_err(|e| LedgerError::Io(e.to_string()))?;
    let expected_data = expected.byte_count.saturating_sub(44);
    if data_len != expected_data {
        return Err(LedgerError::StemMismatch {
            file_name: expected.file_name.clone(),
        });
    }
    Ok(())
}

pub fn verify_all_stems(session_dir: &Path, stems: &[ExpectedStem]) -> Result<(), LedgerError> {
    for stem in stems {
        let path = session_dir.join(&stem.file_name);
        if !path.is_file() {
            return Err(LedgerError::StemMismatch {
                file_name: stem.file_name.clone(),
            });
        }
        verify_stem_file(&path, stem)?;
    }
    Ok(())
}

fn staging_complete(staging_dir: &Path, stems: &[ExpectedStem]) -> Result<(), LedgerError> {
    for stem in stems {
        let path = staging_dir.join(&stem.file_name);
        if !path.is_file() {
            return Err(LedgerError::StagingIncomplete);
        }
        verify_stem_file(&path, stem)?;
    }
    Ok(())
}

/// Same-volume directory publication primitive (Unix: rename; Windows: `MoveFileExW` write-through).
pub fn publish_directory_same_volume(
    src: &Path,
    dst: &Path,
    replace_owned: bool,
) -> Result<(), PublishError> {
    let src_parent = src
        .parent()
        .ok_or_else(|| PublishError::Io("src has no parent".into()))?;
    let dst_parent = dst
        .parent()
        .ok_or_else(|| PublishError::Io("dst has no parent".into()))?;
    fs::create_dir_all(dst_parent)?;
    if !same_volume(src_parent, dst_parent).map_err(|e| PublishError::Io(e.to_string()))? {
        return Err(PublishError::CrossVolume);
    }

    if dst.exists() {
        if !replace_owned {
            return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
        }
    }

    #[cfg(unix)]
    {
        if dst.exists() {
            fs::remove_dir_all(dst)?;
        }
        fs::rename(src, dst)?;
        sync_parent_dir(dst).map_err(|e| PublishError::Io(e.to_string()))?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        windows_publish_directory(src, dst, replace_owned)?;
        return Ok(());
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = replace_owned;
        Err(PublishError::Io(
            "unsupported platform for directory publish".into(),
        ))
    }
}

#[cfg(windows)]
fn windows_publish_directory(
    src: &Path,
    dst: &Path,
    replace_owned: bool,
) -> Result<(), PublishError> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::GetLastError;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
    let flags = if replace_owned {
        MOVEFILE_WRITE_THROUGH | MOVEFILE_REPLACE_EXISTING
    } else {
        MOVEFILE_WRITE_THROUGH
    };
    let ok = MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), flags);
    if ok == 0 {
        let err = GetLastError();
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
    if record.state != OwnershipState::Prepared {
        return Err(PublishError::Ledger(LedgerError::StagingIncomplete));
    }
    staging_complete(&record.staging_session_dir, &record.stems)?;

    let prepared = DeliveryOwnershipRecord {
        state: OwnershipState::Prepared,
        ..record.clone()
    };
    write_ownership_ledger(ledger_path, &prepared).map_err(|e| PublishError::Io(e.to_string()))?;
    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterPreparedPersist)?;
    }

    let replace = false;
    publish_directory_same_volume(
        &record.staging_session_dir,
        &record.final_session_dir,
        replace,
    )?;

    if let Some(ref mut f) = fault {
        f.check(PublicationFault::AfterDirectoryRename)?;
    }

    verify_all_stems(&record.final_session_dir, &record.stems)?;

    let published = DeliveryOwnershipRecord {
        state: OwnershipState::Published,
        ..prepared
    };
    write_ownership_ledger(ledger_path, &published).map_err(|e| PublishError::Io(e.to_string()))?;
    Ok(published)
}

/// Startup / retry recovery for prepared→published transitions.
pub fn recover_publication(ledger_path: &Path) -> Result<DeliveryOwnershipRecord, PublishError> {
    let record = read_ownership_ledger(ledger_path).map_err(|e| PublishError::Io(e.to_string()))?;

    match record.state {
        OwnershipState::Published => return Ok(record),
        OwnershipState::Prepared => {
            let staging = record.staging_session_dir.is_dir();
            let final_exists = record.final_session_dir.is_dir();

            if final_exists {
                if staging {
                    // Ambiguous — refuse to clobber; operator must reconcile.
                    return Err(PublishError::Ledger(LedgerError::UnrelatedDestination));
                }
                verify_all_stems(&record.final_session_dir, &record.stems)?;
                let published = DeliveryOwnershipRecord {
                    state: OwnershipState::Published,
                    ..record
                };
                write_ownership_ledger(ledger_path, &published)
                    .map_err(|e| PublishError::Io(e.to_string()))?;
                return Ok(published);
            }

            if staging {
                return publish_session_directory(ledger_path, &record, None);
            }

            Err(PublishError::Ledger(LedgerError::StagingIncomplete))
        }
    }
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
    if record.delivery_id != delivery_id || record.manifest_hash != manifest_hash {
        return Err(LedgerError::UnrelatedDestination);
    }
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
) -> Result<ExpectedStem> {
    let header = minimal_wav_header(data_bytes)?;
    let total = data_bytes + header.len() as u64;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer = PartialFileWriter::open(path, total)?;
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("ss-delivery-spike-{}-{}", label, nanos))
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
    fn partial_writer_resumes_contiguous_ranges() {
        let dir = unique_temp_dir("partial");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("Alice.wav.partial");
        let expected = 1_000u64;
        let mut w = PartialFileWriter::open(&path, expected).unwrap();
        w.write_range(0, &[1, 2, 3]).unwrap();
        w.write_range(3, &[4, 5]).unwrap();
        assert_eq!(w.contiguous_len(), 5);
        let mut w2 = PartialFileWriter::open(&path, expected).unwrap();
        assert_eq!(w2.contiguous_len(), 5);
        w2.write_range(5, &[6]).unwrap();
        assert_eq!(fs::read(&path).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn partial_writer_rejects_holes() {
        let dir = unique_temp_dir("hole");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.partial");
        let mut w = PartialFileWriter::open(&path, 100).unwrap();
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
        let stem = write_synthetic_wav_stem(&dir.join("a.wav"), data_bytes, 64 * 1024).unwrap();
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
        let stem = write_synthetic_wav_stem(&staging.join("a.wav"), 1024, 4096).unwrap();
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
        let stem = write_synthetic_wav_stem(&staging.join("Alice.wav"), 50_000, 8192).unwrap();
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
        let stem = write_synthetic_wav_stem(&staging.join("Bob.wav"), 80_000, 8192).unwrap();
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
        write_ownership_ledger(&ledger, &record).unwrap();

        publish_directory_same_volume(&staging, &final_dir, false).unwrap();
        assert!(final_dir.is_dir());
        assert!(!staging.exists());

        let recovered = recover_publication(&ledger).unwrap();
        assert_eq!(recovered.state, OwnershipState::Published);
        let _ = fs::remove_dir_all(&parent);
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
        let mut w = PartialFileWriter::open(&path, SESSION_4_5H_DATA_BYTES + 44).unwrap();
        w.write_range(0, &header).unwrap();
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
        let a = write_synthetic_wav_stem(&staging.join("A.wav"), 10_000, 4096).unwrap();
        let b = write_synthetic_wav_stem(&staging.join("B.wav"), 20_000, 4096).unwrap();
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

    #[cfg(windows)]
    #[test]
    fn windows_publish_directory_smoke() {
        let parent = unique_temp_dir("win-publish");
        fs::create_dir_all(&parent).unwrap();
        let src = parent.join("src-session");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("x.wav"), b"RIFF").unwrap();
        let dst = parent.join("dst-session");
        publish_directory_same_volume(&src, &dst, false).unwrap();
        assert!(dst.join("x.wav").is_file());
        assert!(!src.exists());
        let _ = fs::remove_dir_all(&parent);
    }
}
