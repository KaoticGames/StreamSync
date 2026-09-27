//! Streaming SHA-256 without whole-file usize narrowing.

use sha2::{Digest, Sha256};
use std::io::Read;
use thiserror::Error;

pub const DEFAULT_STREAM_CHUNK: usize = 256 * 1024;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HashError {
    #[error("invalid stream chunk size")]
    InvalidChunkSize,
    #[error("length overflow")]
    LengthOverflow,
    #[error("io error: {0}")]
    Io(String),
}

impl From<std::io::Error> for HashError {
    fn from(e: std::io::Error) -> Self {
        HashError::Io(e.to_string())
    }
}

pub fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn reject_zero_chunk(chunk_size: usize) -> Result<(), HashError> {
    if chunk_size == 0 {
        return Err(HashError::InvalidChunkSize);
    }
    Ok(())
}

pub fn sha256_hex_reader<R: Read>(mut reader: R, chunk_size: usize) -> Result<String, HashError> {
    reject_zero_chunk(chunk_size)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; chunk_size];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_digest(&hasher.finalize()))
}

/// Incremental hasher for live ingest; `prefix_digest_hex` clones state in O(1).
pub struct LiveSha256 {
    hasher: Sha256,
}

impl LiveSha256 {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn prefix_digest_hex(&self) -> String {
        hex_digest(&self.hasher.clone().finalize())
    }
}

impl Default for LiveSha256 {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic test fixture byte (matches historical `SyntheticByteSource` stream).
#[cfg(test)]
pub(crate) fn synthetic_byte_at(offset: u64) -> u8 {
    ((offset.wrapping_mul(0x9E37_79B9)) >> 24) as u8
}

#[cfg(test)]
mod streaming_hash_matches_materialized_file {
    use super::*;
    use crate::voice_delivery::hash::hex_digest;
    use crate::voice_delivery::wav::minimal_wav_header;
    use sha2::{Digest, Sha256};
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;

    fn write_synthetic_wav(path: &Path, data_bytes: u64, chunk_size: usize) -> String {
        let header = minimal_wav_header(data_bytes).unwrap();
        let mut f = File::create(path).unwrap();
        f.write_all(&header).unwrap();
        let header_len = header.len() as u64;
        let mut offset = header_len;
        let mut remaining = data_bytes;
        let mut buf = vec![0u8; chunk_size];
        while remaining > 0 {
            let take = remaining.min(chunk_size as u64) as usize;
            for (i, slot) in buf[..take].iter_mut().enumerate() {
                *slot = synthetic_byte_at(offset + i as u64);
            }
            f.write_all(&buf[..take]).unwrap();
            offset += take as u64;
            remaining -= take as u64;
        }
        f.sync_all().unwrap();
        sha256_hex_reader(File::open(path).unwrap(), chunk_size).unwrap()
    }

    #[test]
    fn streaming_hash_matches_materialized_file() {
        let dir = tempfile::tempdir().unwrap();
        let data_bytes = 512 * 1024;
        let path = dir.path().join("a.wav");
        let digest = write_synthetic_wav(&path, data_bytes, 64 * 1024);
        let reread = sha256_hex_reader(File::open(&path).unwrap(), DEFAULT_STREAM_CHUNK).unwrap();
        assert_eq!(reread, digest);
        let header_len = minimal_wav_header(data_bytes).unwrap().len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(minimal_wav_header(data_bytes).unwrap());
        let mut offset = header_len;
        let mut remaining = data_bytes;
        let mut buf = vec![0u8; 32 * 1024];
        while remaining > 0 {
            let take = remaining.min(32 * 1024) as usize;
            for (i, slot) in buf[..take].iter_mut().enumerate() {
                *slot = synthetic_byte_at(offset + i as u64);
            }
            hasher.update(&buf[..take]);
            offset += take as u64;
            remaining -= take as u64;
        }
        assert_eq!(hex_digest(&hasher.finalize()), digest);
    }
}
