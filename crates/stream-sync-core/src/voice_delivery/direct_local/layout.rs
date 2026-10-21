//! Validated relative paths under `DestRoot` for direct-local segments and checkpoints.

use crate::voice_delivery::fs::{validate_single_component, FsError};
use std::path::PathBuf;
use uuid::Uuid;

pub(crate) const DIRECT_ROOT_DIR: &str = ".streamsync-direct";
pub(crate) const SEGMENT_FILENAME_PREFIX: &str = "seg-";
pub(crate) const SEGMENT_FILENAME_SUFFIX: &str = ".synj";
pub(crate) const CUMULATIVE_CHECKPOINT_FILENAME: &str = "cumulative-checkpoint.json";

pub(crate) fn validate_session_id_component(session_id: &str) -> Result<(), FsError> {
    let trimmed = session_id.trim();
    if trimmed.is_empty() || trimmed != session_id {
        return Err(FsError::InvalidComponent("sessionId".into()));
    }
    Uuid::parse_str(session_id).map_err(|_| FsError::InvalidComponent("sessionId".into()))?;
    validate_single_component(session_id)?;
    Ok(())
}

pub(crate) fn generation_dir_name(delivery_generation: u64) -> Result<String, FsError> {
    if delivery_generation == 0 {
        return Err(FsError::InvalidComponent("deliveryGeneration".into()));
    }
    let name = format!("g{delivery_generation}");
    validate_single_component(&name)?;
    Ok(name)
}

pub(crate) fn segment_filename(segment_sequence: u64) -> Result<String, FsError> {
    let name = format!("{SEGMENT_FILENAME_PREFIX}{segment_sequence:016}{SEGMENT_FILENAME_SUFFIX}");
    validate_single_component(&name)?;
    Ok(name)
}

pub(crate) fn generation_relative_components(
    session_id: &str,
    delivery_generation: u64,
) -> Result<Vec<String>, FsError> {
    validate_session_id_component(session_id)?;
    let gen = generation_dir_name(delivery_generation)?;
    validate_single_component(DIRECT_ROOT_DIR)?;
    Ok(vec![
        DIRECT_ROOT_DIR.to_string(),
        session_id.to_string(),
        gen,
    ])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectSegmentFileName {
    relative: PathBuf,
}

impl DirectSegmentFileName {
    pub fn for_identity(session_id: &str, delivery_generation: u64, segment_sequence: u64) -> Self {
        let mut relative = PathBuf::new();
        for comp in generation_relative_components(session_id, delivery_generation)
            .expect("identity path components")
        {
            relative.push(comp);
        }
        relative.push(segment_filename(segment_sequence).expect("segment filename"));
        Self { relative }
    }

    pub fn relative_path(&self) -> &PathBuf {
        &self.relative
    }
}

pub(crate) fn segment_relative_path(
    session_id: &str,
    delivery_generation: u64,
    segment_sequence: u64,
) -> Result<PathBuf, FsError> {
    let mut path = PathBuf::new();
    for comp in generation_relative_components(session_id, delivery_generation)? {
        path.push(comp);
    }
    path.push(segment_filename(segment_sequence)?);
    Ok(path)
}

pub(crate) fn cumulative_checkpoint_relative_path(
    session_id: &str,
    delivery_generation: u64,
) -> Result<PathBuf, FsError> {
    let mut path = PathBuf::new();
    for comp in generation_relative_components(session_id, delivery_generation)? {
        path.push(comp);
    }
    path.push(CUMULATIVE_CHECKPOINT_FILENAME);
    Ok(path)
}
