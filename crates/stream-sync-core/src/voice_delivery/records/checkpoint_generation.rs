//! Immutable checkpoint generation records (per stem artifact directory).

use super::generation_read::{
    allocate_next_generation, select_highest_valid, write_generation_create_new, GenerationRecord,
    GenerationScanError, ParseFailureKind,
};
use super::{record_digest_hex, GenerationIoError};
use crate::voice_delivery::identity::StemArtifactId;
use crate::voice_delivery::manifest::ValidatedManifest;
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

impl GenerationRecord for CheckpointGeneration {
    fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Error)]
pub enum CheckpointGenerationError {
    #[error("structurally invalid checkpoint record")]
    Invalid,
    #[error("record checksum mismatch")]
    ChecksumMismatch,
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("checkpoint bounds violation")]
    Bounds,
    #[error(transparent)]
    Io(#[from] GenerationIoError),
    #[error("parse: {0}")]
    Parse(String),
}

pub struct CheckpointStore {
    pub(crate) dir: crate::voice_delivery::fs::DirHandle,
    artifact: StemArtifactId,
}

impl CheckpointStore {
    pub fn open_for_guard(
        guard: &DeliverySessionGuard,
        artifact: StemArtifactId,
    ) -> Result<Self, CheckpointGenerationError> {
        guard
            .assert_same_delivery_uuid(&artifact.delivery_uuid)
            .map_err(|_| CheckpointGenerationError::IdentityMismatch)?;
        let dir = guard
            .checkpoint_dir_for(&artifact.checkpoint_dir_key())
            .map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?;
        Ok(Self { dir, artifact })
    }

    pub fn read_highest_valid(
        &self,
    ) -> Result<Option<CheckpointGeneration>, CheckpointGenerationError> {
        let artifact = &self.artifact;
        select_highest_valid::<CheckpointGeneration, _, _>(
            &self.dir,
            |rec: &CheckpointGeneration| {
                if rec.delivery_uuid != artifact.delivery_uuid
                    || rec.manifest_digest != artifact.manifest_digest
                    || rec.stem_portable_name != artifact.stem_portable_name
                {
                    if artifact.delivery_uuid != rec.delivery_uuid
                        || artifact.manifest_digest != rec.manifest_digest
                    {
                        Err(ParseFailureKind::ForeignIdentity)
                    } else {
                        Err(ParseFailureKind::MalformedContent)
                    }
                } else {
                    Ok(())
                }
            },
            |name, data| parse_checkpoint_file(name, data),
        )
        .map_err(map_scan_err)
    }

    pub fn commit(
        &self,
        guard: &DeliverySessionGuard,
        manifest: &ValidatedManifest,
        durable_contiguous_len: u64,
        prefix_digest: &str,
    ) -> Result<CheckpointGeneration, CheckpointGenerationError> {
        guard
            .assert_same_delivery_uuid(&self.artifact.delivery_uuid)
            .map_err(|_| CheckpointGenerationError::IdentityMismatch)?;
        let stem = self
            .artifact
            .manifest_entry(manifest)
            .map_err(|_| CheckpointGenerationError::IdentityMismatch)?;
        validate_checkpoint_fields(
            durable_contiguous_len,
            prefix_digest,
            stem.byte_count,
            &stem.sha256,
        )?;
        let generation = allocate_next_generation(&self.dir).map_err(map_scan_err)?;
        let record = CheckpointGeneration {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            delivery_uuid: self.artifact.delivery_uuid.clone(),
            manifest_digest: self.artifact.manifest_digest.clone(),
            stem_portable_name: self.artifact.stem_portable_name.clone(),
            expected_total_bytes: stem.byte_count,
            expected_full_sha256: stem.sha256.clone(),
            generation,
            durable_contiguous_len,
            prefix_digest: prefix_digest.to_string(),
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&record)?;
        write_generation_create_new(&self.dir, generation, &bytes).map_err(map_scan_err)?;
        parse_checkpoint_bytes(&bytes)
    }
}

fn validate_checkpoint_fields(
    durable_contiguous_len: u64,
    prefix_digest: &str,
    expected_total_bytes: u64,
    expected_full_sha256: &str,
) -> Result<(), CheckpointGenerationError> {
    if durable_contiguous_len > expected_total_bytes {
        return Err(CheckpointGenerationError::Bounds);
    }
    if prefix_digest.len() != 64
        || !prefix_digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(CheckpointGenerationError::Invalid);
    }
    if expected_full_sha256.len() != 64
        || !expected_full_sha256
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return Err(CheckpointGenerationError::Invalid);
    }
    Ok(())
}

fn map_scan_err(err: GenerationScanError) -> CheckpointGenerationError {
    match err {
        GenerationScanError::IdentityMismatch => CheckpointGenerationError::IdentityMismatch,
        GenerationScanError::ChecksumMismatch => CheckpointGenerationError::ChecksumMismatch,
        GenerationScanError::Io(e) => CheckpointGenerationError::Io(e),
        GenerationScanError::Parse(s) => CheckpointGenerationError::Parse(s),
    }
}

fn parse_checkpoint_file(
    name: &str,
    data: &[u8],
) -> Result<CheckpointGeneration, ParseFailureKind> {
    let file_gen =
        super::parse_generation_filename(name).ok_or(ParseFailureKind::MalformedContent)?;
    let record = parse_checkpoint_bytes(data).map_err(|e| match e {
        CheckpointGenerationError::ChecksumMismatch => ParseFailureKind::ChecksumMismatch,
        CheckpointGenerationError::IdentityMismatch => ParseFailureKind::ForeignIdentity,
        _ => ParseFailureKind::MalformedContent,
    })?;
    if record.generation != file_gen {
        return Err(ParseFailureKind::FilenameGenerationMismatch);
    }
    Ok(record)
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
    if record.record_digest.len() != 64
        || record.prefix_digest.len() != 64
        || record.expected_full_sha256.len() != 64
    {
        return Err(CheckpointGenerationError::Invalid);
    }
    for field in [
        &record.record_digest,
        &record.prefix_digest,
        &record.expected_full_sha256,
    ] {
        if !field
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(CheckpointGenerationError::Invalid);
        }
    }
    if record.durable_contiguous_len > record.expected_total_bytes {
        return Err(CheckpointGenerationError::Bounds);
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
