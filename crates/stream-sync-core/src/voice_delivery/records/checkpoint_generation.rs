//! Immutable checkpoint generation records (per stem artifact directory).

use super::generation_read::{
    allocate_next_generation, select_highest_valid, write_generation_create_new, GenerationRecord,
    GenerationScanError, ParseFailureKind,
};
use super::{record_digest_hex, GenerationIoError};
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
    bound_identity: DeliveryImmutableIdentity,
    expected_total_bytes: u64,
    expected_full_sha256: String,
}

impl CheckpointStore {
    pub fn open_for_guard(
        guard: &DeliverySessionGuard,
        artifact: StemArtifactId,
    ) -> Result<Self, CheckpointGenerationError> {
        if !artifact.matches_identity(guard.identity()) {
            return Err(CheckpointGenerationError::IdentityMismatch);
        }
        let stem = artifact
            .manifest_entry(guard.manifest())
            .map_err(|_| CheckpointGenerationError::IdentityMismatch)?;
        let expected_total_bytes = stem.byte_count;
        let expected_full_sha256 = stem.sha256.clone();
        let dir = guard
            .checkpoint_dir_for(&artifact.checkpoint_dir_key())
            .map_err(|e| CheckpointGenerationError::Parse(e.to_string()))?;
        Ok(Self {
            dir,
            artifact,
            bound_identity: guard.identity().clone(),
            expected_total_bytes,
            expected_full_sha256,
        })
    }

    pub fn read_highest_valid(
        &self,
    ) -> Result<Option<CheckpointGeneration>, CheckpointGenerationError> {
        let artifact = &self.artifact;
        let expected_total_bytes = self.expected_total_bytes;
        let expected_full_sha256 = &self.expected_full_sha256;
        select_highest_valid::<CheckpointGeneration, _, _>(
            &self.dir,
            |rec: &CheckpointGeneration| {
                if rec.delivery_uuid != artifact.delivery_uuid()
                    || rec.manifest_digest != artifact.manifest_digest()
                    || rec.stem_portable_name != artifact.stem_portable_name()
                {
                    if artifact.delivery_uuid() != rec.delivery_uuid
                        || artifact.manifest_digest() != rec.manifest_digest
                    {
                        Err(ParseFailureKind::ForeignIdentity)
                    } else {
                        Err(ParseFailureKind::MalformedContent)
                    }
                } else if rec.expected_total_bytes != expected_total_bytes
                    || rec.expected_full_sha256 != *expected_full_sha256
                {
                    Err(ParseFailureKind::MalformedContent)
                } else {
                    Ok(())
                }
            },
            parse_checkpoint_file,
        )
        .map_err(map_scan_err)
    }

    pub fn commit(
        &self,
        durable_contiguous_len: u64,
        prefix_digest: &str,
    ) -> Result<CheckpointGeneration, CheckpointGenerationError> {
        validate_checkpoint_fields(
            durable_contiguous_len,
            prefix_digest,
            self.expected_total_bytes,
            &self.expected_full_sha256,
        )?;
        let generation = allocate_next_generation(&self.dir).map_err(map_scan_err)?;
        let record = CheckpointGeneration {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            delivery_uuid: self.bound_identity.delivery_uuid.clone(),
            manifest_digest: self.bound_identity.manifest_digest.clone(),
            stem_portable_name: self.artifact.stem_portable_name().to_string(),
            expected_total_bytes: self.expected_total_bytes,
            expected_full_sha256: self.expected_full_sha256.clone(),
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

#[cfg(test)]
mod checkpoint_adversarial {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs;

    fn guard_one_stem() -> (tempfile::TempDir, DeliverySessionGuard) {
        let tmp = tempfile::tempdir().unwrap();
        let manifest = ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 500,
            sha256: "d".repeat(64),
        }])
        .unwrap();
        let guard = DeliverySessionGuard::begin(
            DestRoot::open(tmp.path()).unwrap(),
            "cp-adv",
            manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec![],
            "final",
            true,
        )
        .unwrap();
        (tmp, guard)
    }

    #[test]
    fn higher_manifest_inconsistent_checkpoint_falls_back() {
        let (tmp, guard) = guard_one_stem();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
                .unwrap();
        let cp_key = artifact.checkpoint_dir_key();
        let store = CheckpointStore::open_for_guard(&guard, artifact).unwrap();
        store.commit(32, &"a".repeat(64)).unwrap();
        let cp_dir = tmp
            .path()
            .join(".streamsync-control/checkpoints")
            .join(guard.identity().opaque_delivery_id.clone())
            .join(cp_key);
        let mut bad = CheckpointGeneration {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            delivery_uuid: guard.identity().delivery_uuid.clone(),
            manifest_digest: guard.identity().manifest_digest.clone(),
            stem_portable_name: "a.wav".into(),
            expected_total_bytes: 999,
            expected_full_sha256: "f".repeat(64),
            generation: 9,
            durable_contiguous_len: 32,
            prefix_digest: "e".repeat(64),
            record_digest: String::new(),
        };
        let bytes = {
            let mut tmp_rec = bad.clone();
            tmp_rec.record_digest = String::new();
            let payload = serde_json::to_vec(&tmp_rec).unwrap();
            let digest = record_digest_hex(&payload);
            bad.record_digest = digest;
            serde_json::to_vec(&bad).unwrap()
        };
        fs::write(cp_dir.join("gen-9.json"), bytes).unwrap();
        let highest = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(highest.generation, 1);
        assert_eq!(highest.expected_total_bytes, 500);
    }

    #[test]
    fn foreign_uuid_checkpoint_fails_closed() {
        let (tmp, guard) = guard_one_stem();
        let artifact =
            StemArtifactId::from_manifest_stem(guard.identity(), guard.manifest(), "a.wav")
                .unwrap();
        let cp_key = artifact.checkpoint_dir_key();
        let store = CheckpointStore::open_for_guard(&guard, artifact).unwrap();
        let cp_dir = tmp
            .path()
            .join(".streamsync-control/checkpoints")
            .join(guard.identity().opaque_delivery_id.clone())
            .join(cp_key);
        let foreign = CheckpointGeneration {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            delivery_uuid: "other-uuid".into(),
            manifest_digest: guard.identity().manifest_digest.clone(),
            stem_portable_name: "a.wav".into(),
            expected_total_bytes: 500,
            expected_full_sha256: "d".repeat(64),
            generation: 3,
            durable_contiguous_len: 0,
            prefix_digest: "a".repeat(64),
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&foreign).unwrap();
        fs::write(cp_dir.join("gen-3.json"), bytes).unwrap();
        assert!(matches!(
            store.read_highest_valid(),
            Err(CheckpointGenerationError::IdentityMismatch)
        ));
    }
}
