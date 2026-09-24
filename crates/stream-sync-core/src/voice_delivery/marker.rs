//! Ownership marker (`.streamsync-delivery.json`) and seal / PublishIntent pipeline.

use crate::voice_delivery::fs::durability::{sync_dir_exact, sync_file, NamespaceDurability};
use crate::voice_delivery::fs::file::open_existing_file_at;
use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError, VoiceFile};
use crate::voice_delivery::hash::{sha256_hex_reader, DEFAULT_STREAM_CHUNK};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
use crate::voice_delivery::records::ledger_generation::{LedgerGeneration, LedgerStore};
use crate::voice_delivery::records::record_digest_hex;
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::LedgerState;
use crate::voice_delivery::wav::validate_canonical_pcm_wav_header;
use serde::{Deserialize, Serialize};
use std::io::Seek;
use thiserror::Error;

pub const MARKER_FILENAME: &str = ".streamsync-delivery.json";
pub const MARKER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StemMarkerEntry {
    pub file_name: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryMarker {
    pub schema_version: u32,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub staging_token: String,
    pub final_parent_relative: Vec<String>,
    pub final_session_name: String,
    pub stems: Vec<StemMarkerEntry>,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum MarkerError {
    #[error("marker checksum mismatch")]
    ChecksumMismatch,
    #[error("marker identity mismatch")]
    IdentityMismatch,
    #[error("unexpected staging entry: {0}")]
    UnexpectedEntry(String),
    #[error("missing stem: {0}")]
    MissingStem(String),
    #[error("stem verification failed: {0}")]
    StemVerification(String),
    #[error("marker already exists")]
    AlreadyExists,
    #[error(transparent)]
    Ledger(#[from] crate::voice_delivery::records::ledger_generation::LedgerGenerationError),
    #[error(transparent)]
    Fs(#[from] FsError),
    #[error("parse: {0}")]
    Parse(String),
}

/// Records publication-side effects for ordering tests (rename must not run before intent).
pub trait PublicationOps {
    fn rename_stage_to_final(&mut self) -> Result<(), FsError>;
}

#[derive(Default)]
pub struct OperationRecorder {
    pub rename_calls: u32,
    pub intent_written: bool,
}

impl PublicationOps for OperationRecorder {
    fn rename_stage_to_final(&mut self) -> Result<(), FsError> {
        self.rename_calls += 1;
        Ok(())
    }
}

impl DeliveryMarker {
    pub fn from_manifest(
        identity: &DeliveryImmutableIdentity,
        manifest: &ValidatedManifest,
    ) -> Self {
        Self {
            schema_version: MARKER_SCHEMA_VERSION,
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            staging_token: identity.staging_token.clone(),
            final_parent_relative: identity.final_parent_relative.clone(),
            final_session_name: identity.final_session_name.clone(),
            stems: manifest
                .stems
                .iter()
                .map(|s| StemMarkerEntry {
                    file_name: s.file_name.clone(),
                    byte_count: s.byte_count,
                    sha256: s.sha256.clone(),
                })
                .collect(),
            record_digest: String::new(),
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>, MarkerError> {
        let mut tmp = self.clone();
        tmp.record_digest = String::new();
        let payload = serde_json::to_vec(&tmp).map_err(|e| MarkerError::Parse(e.to_string()))?;
        let digest = record_digest_hex(&payload);
        tmp.record_digest = digest;
        serde_json::to_vec(&tmp).map_err(|e| MarkerError::Parse(e.to_string()))
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, MarkerError> {
        let marker: Self =
            serde_json::from_slice(bytes).map_err(|_| MarkerError::Parse("json".into()))?;
        let mut tmp = marker.clone();
        let expected = tmp.record_digest.clone();
        tmp.record_digest = String::new();
        let payload = serde_json::to_vec(&tmp).map_err(|_| MarkerError::Parse("json".into()))?;
        if record_digest_hex(&payload) != expected {
            return Err(MarkerError::ChecksumMismatch);
        }
        Ok(marker)
    }
}

pub fn write_marker_create_new(
    staging_dir: &DirHandle,
    marker: &DeliveryMarker,
) -> Result<(), MarkerError> {
    let bytes = marker.serialize()?;
    let mut file = staging_dir
        .create_new_file(MARKER_FILENAME)
        .map_err(|e| match e {
            FsError::AlreadyExists => MarkerError::AlreadyExists,
            other => MarkerError::Fs(other),
        })?;
    file.write_all_at(0, &bytes)?;
    sync_file(&file)?;
    sync_dir_exact(staging_dir)?;
    Ok(())
}

pub fn verify_stem_from_handle(
    file: &VoiceFile,
    expected: &StemManifestEntry,
) -> Result<(), MarkerError> {
    let len = file.len()?;
    if len != expected.byte_count {
        return Err(MarkerError::StemVerification(format!(
            "length {} != {}",
            len, expected.byte_count
        )));
    }
    let mut header = [0u8; 44];
    file.read_exact_at(0, &mut header)?;
    validate_canonical_pcm_wav_header(&header, Some(len))
        .map_err(|e| MarkerError::StemVerification(e.to_string()))?;
    let mut reader = file.std_file();
    reader
        .seek(std::io::SeekFrom::Start(0))
        .map_err(|e| MarkerError::StemVerification(e.to_string()))?;
    let digest = sha256_hex_reader(reader, DEFAULT_STREAM_CHUNK)
        .map_err(|e| MarkerError::StemVerification(e.to_string()))?;
    if digest != expected.sha256 {
        return Err(MarkerError::StemVerification("sha256 mismatch".into()));
    }
    Ok(())
}

pub fn verify_exact_staging_membership(
    staging_dir: &DirHandle,
    manifest: &ValidatedManifest,
) -> Result<(), MarkerError> {
    let expected_wavs: std::collections::HashSet<String> =
        manifest.stems.iter().map(|s| s.file_name.clone()).collect();
    let mut seen = std::collections::HashSet::new();
    for name in staging_dir.list_child_names()? {
        if name == MARKER_FILENAME {
            continue;
        }
        if name.ends_with(".partial") {
            return Err(MarkerError::UnexpectedEntry(name));
        }
        if !name.ends_with(".wav") {
            return Err(MarkerError::UnexpectedEntry(name));
        }
        if !expected_wavs.contains(&name) {
            return Err(MarkerError::UnexpectedEntry(name));
        }
        if !seen.insert(name.clone()) {
            return Err(MarkerError::UnexpectedEntry(format!("duplicate {name}")));
        }
        let stem = manifest
            .stems
            .iter()
            .find(|s| s.file_name == name)
            .expect("contains");
        let file = open_existing_file_at(staging_dir, &name)?;
        verify_stem_from_handle(&file, stem)?;
    }
    for stem in &manifest.stems {
        if !seen.contains(&stem.file_name) {
            return Err(MarkerError::MissingStem(stem.file_name.clone()));
        }
    }
    Ok(())
}

/// Seal staging (marker + membership), durable Sealed ledger, then PublishIntent — **no rename**.
pub fn seal_and_write_publish_intent(
    guard: &DeliverySessionGuard,
    _root: &DestRoot,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
    staging_dir: &DirHandle,
    ledger_store: &LedgerStore,
    prior_ledger: Option<&LedgerGeneration>,
) -> Result<(LedgerGeneration, LedgerGeneration, NamespaceDurability), MarkerError> {
    verify_exact_staging_membership(staging_dir, manifest)?;
    let marker = DeliveryMarker::from_manifest(identity, manifest);
    write_marker_create_new(staging_dir, &marker)?;
    let staging_durability = sync_dir_exact(staging_dir)?;
    let sealed = ledger_store.commit(guard, identity, LedgerState::Sealed, prior_ledger)?;
    let intent = ledger_store.commit(guard, identity, LedgerState::PublishIntent, Some(&sealed))?;
    Ok((sealed, intent, staging_durability))
}

/// Publication rename is blocked until a durable PublishIntent generation exists (slice 10+ wires real rename).
pub fn execute_publication_rename(
    ledger_store: &LedgerStore,
    identity: &DeliveryImmutableIdentity,
    ops: &mut dyn PublicationOps,
) -> Result<(), MarkerError> {
    let intent = ledger_store
        .read_highest_valid(identity)?
        .ok_or_else(|| MarkerError::Parse("no ledger".into()))?;
    if intent.state != LedgerState::PublishIntent {
        return Err(MarkerError::Parse("publish intent not durable".into()));
    }
    ops.rename_stage_to_final()?;
    Ok(())
}

#[cfg(test)]
mod publish_intent_before_rename {
    use super::*;
    use crate::voice_delivery::hash::SyntheticByteSource;
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::wav::minimal_wav_header;
    fn write_minimal_stem(staging: &DirHandle, name: &str, data_bytes: u64) -> StemManifestEntry {
        let header = minimal_wav_header(data_bytes).unwrap();
        let mut body = Vec::new();
        let mut pos = 0u64;
        while pos < data_bytes {
            body.push(SyntheticByteSource::byte_at(pos));
            pos += 1;
        }
        let mut file = staging.create_new_file(name).unwrap();
        file.write_all_at(0, &header).unwrap();
        file.write_all_at(header.len() as u64, &body).unwrap();
        file.std_file().sync_all().unwrap();
        let sha = crate::voice_delivery::hash::sha256_hex_reader(
            open_existing_file_at(staging, name).unwrap().std_file(),
            crate::voice_delivery::hash::DEFAULT_STREAM_CHUNK,
        )
        .unwrap();
        StemManifestEntry {
            file_name: name.to_string(),
            byte_count: header.len() as u64 + data_bytes,
            sha256: sha,
        }
    }

    #[test]
    fn publish_intent_before_rename() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef";
        root.create_child_dir(stage).unwrap();
        let staging = root.open_child_dir(stage).unwrap();
        let data_bytes = 4u64;
        let stem = write_minimal_stem(&staging, "a.wav", data_bytes);
        let stems = vec![stem];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let identity = DeliveryImmutableIdentity::new(
            "delivery-intent",
            &manifest,
            stage,
            vec!["guild".into()],
            "published-name",
        )
        .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-intent", true).unwrap();
        let guard = DeliverySessionGuard::new(lock);
        let ledger = LedgerStore::open_at_root(&root, &identity).unwrap();
        let receiving = ledger
            .commit(&guard, &identity, LedgerState::Receiving, None)
            .unwrap();
        let mut recorder = OperationRecorder::default();
        assert!(execute_publication_rename(&ledger, &identity, &mut recorder).is_err());
        assert_eq!(recorder.rename_calls, 0);
        let (_sealed, intent, _dur) = seal_and_write_publish_intent(
            &guard,
            &root,
            &identity,
            &manifest,
            &staging,
            &ledger,
            Some(&receiving),
        )
        .unwrap();
        assert_eq!(intent.state, LedgerState::PublishIntent);
        let highest = ledger.read_highest_valid(&identity).unwrap().unwrap();
        assert_eq!(highest.state, LedgerState::PublishIntent);
        execute_publication_rename(&ledger, &identity, &mut recorder).unwrap();
        assert_eq!(recorder.rename_calls, 1);
    }

    #[test]
    fn seal_path_never_renames() {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stage = ".streamsync-stage-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        root.create_child_dir(stage).unwrap();
        let staging = root.open_child_dir(stage).unwrap();
        let data_bytes = 4u64;
        let stem = write_minimal_stem(&staging, "a.wav", data_bytes);
        let stems = vec![stem];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let identity =
            DeliveryImmutableIdentity::new("delivery-no-rename", &manifest, stage, vec![], "final")
                .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-no-rename", true).unwrap();
        let guard = DeliverySessionGuard::new(lock);
        let ledger = LedgerStore::open_at_root(&root, &identity).unwrap();
        let receiving = ledger
            .commit(&guard, &identity, LedgerState::Receiving, None)
            .unwrap();
        seal_and_write_publish_intent(
            &guard,
            &root,
            &identity,
            &manifest,
            &staging,
            &ledger,
            Some(&receiving),
        )
        .unwrap();
        assert!(root.open_child_dir(stage).is_ok());
    }
}
