//! Durable cumulative checkpoint for direct-local generation scope.

use super::layout::CUMULATIVE_CHECKPOINT_FILENAME;
use crate::voice_delivery::fs::durability::NamespaceDurability;
use crate::voice_delivery::fs::{rename_replace_same_parent, DirHandle, FsError};
use crate::voice_delivery::records::record_digest_hex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
pub const NO_SEGMENTS_YET: u64 = u64::MAX;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectCumulativeCheckpoint {
    pub schema_version: u32,
    pub session_id: String,
    pub delivery_generation: u64,
    pub highest_contiguous_sequence: u64,
    pub last_segment_byte_length: u64,
    pub last_segment_sha256_hex: String,
    pub namespace_durability: NamespaceDurability,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum DirectCheckpointError {
    #[error("checkpoint checksum mismatch")]
    ChecksumMismatch,
    #[error("checkpoint identity mismatch")]
    IdentityMismatch,
    #[error("checkpoint not found")]
    NotFound,
    #[error(transparent)]
    Fs(#[from] FsError),
}

impl DirectCumulativeCheckpoint {
    pub fn initial_for_session(session_id: &str, delivery_generation: u64) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            session_id: session_id.to_string(),
            delivery_generation,
            highest_contiguous_sequence: NO_SEGMENTS_YET,
            last_segment_byte_length: 0,
            last_segment_sha256_hex: String::new(),
            namespace_durability: NamespaceDurability::Unavailable,
            record_digest: String::new(),
        }
    }

    pub fn read_durable_from_store(
        store: &super::generation_store::DirectGenerationStore,
    ) -> Result<Self, DirectCheckpointError> {
        let bytes = store
            .generation_dir()
            .read_file_all(CUMULATIVE_CHECKPOINT_FILENAME)
            .map_err(|e| match e {
                FsError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
                    DirectCheckpointError::NotFound
                }
                other => DirectCheckpointError::Fs(other),
            })?;
        let parsed = parse_checkpoint_bytes(&bytes)?;
        if parsed.session_id != store.session_id()
            || parsed.delivery_generation != store.delivery_generation()
        {
            return Err(DirectCheckpointError::IdentityMismatch);
        }
        Ok(parsed)
    }

    pub(crate) fn next_expected_segment_sequence(&self) -> u64 {
        if self.highest_contiguous_sequence == NO_SEGMENTS_YET {
            0
        } else {
            self.highest_contiguous_sequence + 1
        }
    }

    pub(crate) fn reconcile_supplied_with_durable_authority(
        store: &super::generation_store::DirectGenerationStore,
        supplied: &Self,
    ) -> Result<Self, DirectCheckpointError> {
        match Self::read_durable_from_store(store) {
            Ok(durable) => {
                if durable != *supplied {
                    return Err(DirectCheckpointError::IdentityMismatch);
                }
                Ok(durable)
            }
            Err(DirectCheckpointError::NotFound) => {
                if !is_structurally_valid_initial_for_store(store, supplied) {
                    return Err(DirectCheckpointError::IdentityMismatch);
                }
                Ok(supplied.clone())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) fn advance_after_segment(
        &self,
        segment_sequence: u64,
        byte_length: u64,
        sha256_hex: &str,
        namespace_durability: NamespaceDurability,
    ) -> Self {
        Self {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            session_id: self.session_id.clone(),
            delivery_generation: self.delivery_generation,
            highest_contiguous_sequence: segment_sequence,
            last_segment_byte_length: byte_length,
            last_segment_sha256_hex: sha256_hex.to_string(),
            namespace_durability,
            record_digest: String::new(),
        }
    }
}

fn is_structurally_valid_initial_for_store(
    store: &super::generation_store::DirectGenerationStore,
    supplied: &DirectCumulativeCheckpoint,
) -> bool {
    let expected = DirectCumulativeCheckpoint::initial_for_session(
        store.session_id(),
        store.delivery_generation(),
    );
    supplied == &expected
}

pub(crate) fn parse_checkpoint_bytes(
    bytes: &[u8],
) -> Result<DirectCumulativeCheckpoint, DirectCheckpointError> {
    let record: DirectCumulativeCheckpoint =
        serde_json::from_slice(bytes).map_err(|_| DirectCheckpointError::ChecksumMismatch)?;
    if record.schema_version != CHECKPOINT_SCHEMA_VERSION {
        return Err(DirectCheckpointError::ChecksumMismatch);
    }
    let mut tmp = record.clone();
    let expected = tmp.record_digest.clone();
    tmp.record_digest = String::new();
    let payload = serde_json::to_vec(&tmp).map_err(|_| DirectCheckpointError::ChecksumMismatch)?;
    if record_digest_hex(&payload) != expected {
        return Err(DirectCheckpointError::ChecksumMismatch);
    }
    Ok(record)
}

pub(crate) fn serialize_checkpoint(
    record: &DirectCumulativeCheckpoint,
) -> Result<Vec<u8>, FsError> {
    let mut tmp = record.clone();
    tmp.record_digest = String::new();
    let payload = serde_json::to_vec(&tmp)
        .map_err(|_| FsError::InvalidComponent("checkpoint json".into()))?;
    let digest = record_digest_hex(&payload);
    tmp.record_digest = digest;
    serde_json::to_vec(&tmp).map_err(|_| FsError::InvalidComponent("checkpoint json".into()))
}

pub(crate) fn write_checkpoint_temp(
    generation_dir: &DirHandle,
    record: &DirectCumulativeCheckpoint,
) -> Result<(String, crate::voice_delivery::fs::VoiceFile), FsError> {
    let bytes = serialize_checkpoint(record)?;
    let temp_name = format!(".cp-{}.tmp", uuid::Uuid::new_v4().simple());
    let mut temp_file = generation_dir.create_new_file(&temp_name)?;
    temp_file.write_all_at(0, &bytes)?;
    Ok((temp_name, temp_file))
}

pub(crate) fn atomic_replace_checkpoint_from_temp(
    generation_dir: &DirHandle,
    temp_name: &str,
) -> Result<(), FsError> {
    rename_replace_same_parent(generation_dir, temp_name, CUMULATIVE_CHECKPOINT_FILENAME)
}
