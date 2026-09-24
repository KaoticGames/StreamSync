//! Immutable checkpoint generation records.

use super::{
    generation_filename, next_generation_after_max, parse_generation_filename, record_digest_hex,
    GenerationIoError,
};
use crate::voice_delivery::fs::durability::{sync_dir_exact, sync_file};
use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError};
use crate::voice_delivery::identity::{DeliveryImmutableIdentity, StemArtifactId};
use crate::voice_delivery::session::DeliverySessionGuard;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointGeneration {
    pub schema_version: u32,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub stem_portable_name: String,
    pub expected_total_bytes: u64,
    pub expected_full_sha256: String,
    pub generation: u64,
    pub durable_contiguous_len: u64,
    pub prefix_digest: String,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum CheckpointGenerationError {
    #[error("structurally invalid checkpoint record")]
    Invalid,
    #[error("record checksum mismatch")]
    ChecksumMismatch,
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error(transparent)]
    Io(#[from] GenerationIoError),
    #[error("parse: {0}")]
    Parse(String),
}

pub struct CheckpointStore {
    pub(crate) dir: DirHandle,
}

impl CheckpointStore {
    pub fn open_at_root(
        root: &DestRoot,
        identity: &DeliveryImmutableIdentity,
    ) -> Result<Self, FsError> {
        let comps = crate::voice_delivery::ids::checkpoint_dir_relative_components(
            &identity.opaque_delivery_id,
        );
        let mut current = root.handle().clone_handle()?;
        for comp in comps.iter() {
            current = current.create_or_open_child_dir(comp)?;
        }
        Ok(Self { dir: current })
    }

    pub fn read_highest_valid(
        &self,
        artifact: &StemArtifactId,
    ) -> Result<Option<CheckpointGeneration>, CheckpointGenerationError> {
        let mut best: Option<CheckpointGeneration> = None;
        for name in self
            .dir
            .list_child_names()
            .map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?
        {
            if parse_generation_filename(&name).is_none() {
                continue;
            }
            let data = match self.dir.read_file_all(&name) {
                Ok(d) => d,
                Err(_) => continue,
            };
            match parse_checkpoint_bytes(&data) {
                Ok(rec) => {
                    if !artifact_matches(&rec, artifact) {
                        return Err(CheckpointGenerationError::IdentityMismatch);
                    }
                    if best
                        .as_ref()
                        .map(|b| rec.generation > b.generation)
                        .unwrap_or(true)
                    {
                        best = Some(rec);
                    }
                }
                Err(CheckpointGenerationError::ChecksumMismatch) => {
                    return Err(CheckpointGenerationError::ChecksumMismatch);
                }
                Err(_) => {}
            }
        }
        Ok(best)
    }

    pub fn commit(
        &self,
        _guard: &DeliverySessionGuard,
        artifact: &StemArtifactId,
        expected_total_bytes: u64,
        expected_full_sha256: &str,
        durable_contiguous_len: u64,
        prefix_digest: &str,
    ) -> Result<CheckpointGeneration, CheckpointGenerationError> {
        let max_observed = scan_max_generation_number(&self.dir)?;
        let generation = next_generation_after_max(max_observed)?;
        let record = CheckpointGeneration {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            delivery_uuid: artifact.delivery_uuid.clone(),
            manifest_digest: artifact.manifest_digest.clone(),
            stem_portable_name: artifact.stem_portable_name.clone(),
            expected_total_bytes,
            expected_full_sha256: expected_full_sha256.to_string(),
            generation,
            durable_contiguous_len,
            prefix_digest: prefix_digest.to_string(),
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&record)?;
        let name = generation_filename(generation)?;
        write_generation_create_new(&self.dir, &name, &bytes)?;
        parse_checkpoint_bytes(&bytes)
    }
}

fn artifact_matches(rec: &CheckpointGeneration, artifact: &StemArtifactId) -> bool {
    rec.delivery_uuid == artifact.delivery_uuid
        && rec.manifest_digest == artifact.manifest_digest
        && rec.stem_portable_name == artifact.stem_portable_name
}

fn scan_max_generation_number(dir: &DirHandle) -> Result<u64, CheckpointGenerationError> {
    let mut max = 0u64;
    for name in dir
        .list_child_names()
        .map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?
    {
        if let Some(n) = parse_generation_filename(&name) {
            max = max.max(n);
        } else if name.starts_with(super::GEN_PREFIX) {
            max = max.saturating_add(1);
        }
    }
    Ok(max)
}

fn serialize_with_digest(
    record: &CheckpointGeneration,
) -> Result<Vec<u8>, CheckpointGenerationError> {
    let mut tmp = record.clone();
    tmp.record_digest = String::new();
    let payload =
        serde_json::to_vec(&tmp).map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?;
    let digest = record_digest_hex(&payload);
    tmp.record_digest = digest;
    serde_json::to_vec(&tmp).map_err(|e| CheckpointGenerationError::Parse(e.to_string()))
}

pub fn parse_checkpoint_bytes(
    data: &[u8],
) -> Result<CheckpointGeneration, CheckpointGenerationError> {
    let record: CheckpointGeneration =
        serde_json::from_slice(data).map_err(|_| CheckpointGenerationError::Invalid)?;
    validate_checkpoint_record(&record)?;
    Ok(record)
}

pub fn validate_checkpoint_record(
    record: &CheckpointGeneration,
) -> Result<(), CheckpointGenerationError> {
    if record.schema_version != CHECKPOINT_SCHEMA_VERSION {
        return Err(CheckpointGenerationError::Invalid);
    }
    if record.record_digest.len() != 64 || record.prefix_digest.len() != 64 {
        return Err(CheckpointGenerationError::Invalid);
    }
    if record.expected_full_sha256.len() != 64 {
        return Err(CheckpointGenerationError::Invalid);
    }
    let mut tmp = record.clone();
    let expected = tmp.record_digest.clone();
    tmp.record_digest = String::new();
    let payload = serde_json::to_vec(&tmp).map_err(|_| CheckpointGenerationError::Invalid)?;
    let digest = record_digest_hex(&payload);
    if digest != expected {
        return Err(CheckpointGenerationError::ChecksumMismatch);
    }
    Ok(())
}

fn write_generation_create_new(
    dir: &DirHandle,
    name: &str,
    bytes: &[u8],
) -> Result<(), CheckpointGenerationError> {
    let mut file = dir.create_new_file(name).map_err(|e| match e {
        FsError::AlreadyExists => CheckpointGenerationError::Io(GenerationIoError::AlreadyExists),
        other => CheckpointGenerationError::Parse(other.to_string()),
    })?;
    file.write_all_at(0, bytes)
        .map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?;
    sync_file(&file).map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?;
    let _ = sync_dir_exact(dir);
    Ok(())
}
