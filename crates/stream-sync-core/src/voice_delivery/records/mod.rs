//! Immutable generation files (ledger + checkpoint).

pub(crate) mod checkpoint_generation;
pub(crate) mod ledger_generation;

pub(crate) const GEN_PREFIX: &str = "gen-";
pub(crate) const GEN_SUFFIX: &str = ".json";
pub(crate) const MAX_GENERATION: u64 = 999_999_999_999_999_999;

#[derive(Debug, Error)]
pub enum GenerationIoError {
    #[error("generation number exhausted")]
    Exhausted,
    #[error("generation file already exists")]
    AlreadyExists,
    #[error("invalid generation filename")]
    InvalidFilename,
    #[error("symlink or reparse generation entry")]
    SymlinkGeneration,
    #[error(transparent)]
    Fs(#[from] crate::voice_delivery::fs::FsError),
    #[error("io: {0}")]
    Io(String),
}

use thiserror::Error;

/// Parse `gen-<u64>.json` generation numbers from a directory entry name.
pub fn parse_generation_filename(name: &str) -> Option<u64> {
    if !name.starts_with(GEN_PREFIX) || !name.ends_with(GEN_SUFFIX) {
        return None;
    }
    let mid = name
        .strip_prefix(GEN_PREFIX)
        .and_then(|s| s.strip_suffix(GEN_SUFFIX))?;
    if mid.is_empty() || mid.len() > 20 {
        return None;
    }
    if !mid.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    mid.parse().ok()
}

pub fn generation_filename(generation: u64) -> Result<String, GenerationIoError> {
    if generation > MAX_GENERATION {
        return Err(GenerationIoError::Exhausted);
    }
    Ok(format!("{GEN_PREFIX}{generation}{GEN_SUFFIX}"))
}

pub fn next_generation_after_max(max_observed: u64) -> Result<u64, GenerationIoError> {
    let next = max_observed
        .checked_add(1)
        .ok_or(GenerationIoError::Exhausted)?;
    if next > MAX_GENERATION {
        return Err(GenerationIoError::Exhausted);
    }
    Ok(next)
}

pub(crate) fn record_digest_hex(payload_without_digest: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    crate::voice_delivery::hash::hex_digest(&Sha256::digest(payload_without_digest))
}
