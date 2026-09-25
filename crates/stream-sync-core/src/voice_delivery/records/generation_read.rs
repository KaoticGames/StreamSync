//! Shared immutable generation directory scan, allocation, and highest-valid selection.

use super::{
    generation_filename, next_generation_after_max, parse_generation_filename, GenerationIoError,
    GEN_PREFIX,
};
use crate::voice_delivery::fs::{DirHandle, FsError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GenerationScanError {
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("record checksum mismatch")]
    ChecksumMismatch,
    #[error(transparent)]
    Io(#[from] GenerationIoError),
    #[error("parse: {0}")]
    Parse(String),
}

/// Classify whether a parse failure allows scanning other generations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseFailureKind {
    MalformedContent,
    ForeignIdentity,
    FilenameGenerationMismatch,
    ChecksumMismatch,
}

pub trait GenerationRecord: Clone {
    fn generation(&self) -> u64;
}

/// Max numeric generation from **canonical** filenames only (malformed file bodies still count).
pub fn scan_max_canonical_generation_number(dir: &DirHandle) -> Result<u64, GenerationScanError> {
    let mut max = 0u64;
    for name in dir
        .list_child_names()
        .map_err(|e| GenerationScanError::Parse(e.to_string()))?
    {
        if let Some(n) = parse_generation_filename(&name) {
            max = max.max(n);
        }
    }
    Ok(max)
}

pub fn allocate_next_generation(dir: &DirHandle) -> Result<u64, GenerationScanError> {
    let max = scan_max_canonical_generation_number(dir)?;
    next_generation_after_max(max).map_err(GenerationScanError::Io)
}

fn map_read_fs_error(_name: &str, err: FsError) -> Result<(), GenerationScanError> {
    match err {
        FsError::SymlinkOrReparseComponent(_) | FsError::SymlinkOrReparseRoot => Err(
            GenerationScanError::Io(GenerationIoError::SymlinkGeneration),
        ),
        FsError::Io(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => Err(GenerationScanError::Parse(other.to_string())),
    }
}

/// Read one generation file; symlink/reparse and unexpected I/O fail closed.
pub fn read_generation_bytes(
    dir: &DirHandle,
    name: &str,
) -> Result<Option<Vec<u8>>, GenerationScanError> {
    if parse_generation_filename(name).is_none() {
        return Ok(None);
    }
    match dir.read_file_all(name) {
        Ok(data) => Ok(Some(data)),
        Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => {
            map_read_fs_error(name, e)?;
            Ok(None)
        }
    }
}

/// Select highest structurally valid record; malformed higher generations are skipped.
pub fn select_highest_valid<T, F, P>(
    dir: &DirHandle,
    mut identity_matches: F,
    mut parse_record: P,
) -> Result<Option<T>, GenerationScanError>
where
    T: GenerationRecord,
    F: FnMut(&T) -> Result<(), ParseFailureKind>,
    P: FnMut(&str, &[u8]) -> Result<T, ParseFailureKind>,
{
    let mut candidates: Vec<(u64, String, T)> = Vec::new();
    for name in dir
        .list_child_names()
        .map_err(|e| GenerationScanError::Parse(e.to_string()))?
    {
        let file_gen = match parse_generation_filename(&name) {
            Some(n) => n,
            None => continue,
        };
        let Some(data) = read_generation_bytes(dir, &name)? else {
            continue;
        };
        match parse_record(&name, &data) {
            Ok(rec) => {
                if rec.generation() != file_gen {
                    continue;
                }
                match identity_matches(&rec) {
                    Ok(()) => candidates.push((file_gen, name, rec)),
                    Err(ParseFailureKind::ForeignIdentity) => {
                        return Err(GenerationScanError::IdentityMismatch);
                    }
                    Err(_) => {}
                }
            }
            Err(ParseFailureKind::ForeignIdentity) => {
                return Err(GenerationScanError::IdentityMismatch);
            }
            Err(_) => {}
        }
    }
    candidates.sort_by_key(|(g, _, _)| *g);
    Ok(candidates.pop().map(|(_, _, rec)| rec))
}

pub fn write_generation_create_new(
    dir: &DirHandle,
    generation: u64,
    bytes: &[u8],
) -> Result<NamespaceDurabilityReceipt, GenerationScanError> {
    use crate::voice_delivery::fs::durability::{sync_dir_exact, sync_file};
    let name = generation_filename(generation).map_err(GenerationScanError::Io)?;
    let mut file = dir.create_new_file(&name).map_err(|e| match e {
        FsError::AlreadyExists => GenerationScanError::Io(GenerationIoError::AlreadyExists),
        other => GenerationScanError::Parse(other.to_string()),
    })?;
    file.write_all_at(0, bytes)
        .map_err(|e| GenerationScanError::Parse(e.to_string()))?;
    sync_file(&file).map_err(|e| GenerationScanError::Parse(e.to_string()))?;
    let durability = sync_dir_exact(dir).map_err(|e| GenerationScanError::Parse(e.to_string()))?;
    Ok(NamespaceDurabilityReceipt { durability })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceDurabilityReceipt {
    pub durability: crate::voice_delivery::fs::durability::NamespaceDurability,
}

/// Policy: ignore unrelated names; reject ambiguous gen-prefix garbage for allocation elsewhere.
pub fn is_unrelated_generation_name(name: &str) -> bool {
    name.starts_with(GEN_PREFIX) && parse_generation_filename(name).is_none()
}
