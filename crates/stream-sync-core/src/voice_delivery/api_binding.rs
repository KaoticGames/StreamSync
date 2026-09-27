//! Durable accepted API manifest metadata bound to delivery identity (control dir).

use crate::voice_delivery::finalized_manifest::{
    compute_finalized_manifest_digest, parse_syndicate_finalized_manifest,
    SyndicateFinalizedManifest,
};
use crate::voice_delivery::fs::{DirHandle, FsError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::manifest::ValidatedManifest;
use crate::voice_delivery::records::record_digest_hex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const API_BINDING_FILENAME: &str = "api-binding.json";
pub const API_BINDING_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiBindingRecord {
    pub schema_version: u32,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub stem_set_digest: String,
    pub sealed_at: String,
    pub manifest: SyndicateFinalizedManifest,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum ApiBindingError {
    #[error("api binding checksum mismatch")]
    ChecksumMismatch,
    #[error("api binding identity mismatch")]
    IdentityMismatch,
    #[error("api binding already exists")]
    AlreadyExists,
    #[error("parse: {0}")]
    Parse(String),
    #[error(transparent)]
    Fs(#[from] FsError),
}

impl ApiBindingRecord {
    pub fn from_accepted(
        identity: &DeliveryImmutableIdentity,
        manifest: &ValidatedManifest,
        syndicate: &SyndicateFinalizedManifest,
        sealed_at: &str,
    ) -> Result<Self, ApiBindingError> {
        let computed = compute_finalized_manifest_digest(syndicate);
        if computed != identity.manifest_digest {
            return Err(ApiBindingError::Parse("manifest digest mismatch".into()));
        }
        if manifest.digest() != identity.stem_set_digest {
            return Err(ApiBindingError::Parse("stem digest mismatch".into()));
        }
        Ok(Self {
            schema_version: API_BINDING_SCHEMA_VERSION,
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            stem_set_digest: identity.stem_set_digest.clone(),
            sealed_at: sealed_at.to_string(),
            manifest: syndicate.clone(),
            record_digest: String::new(),
        })
    }

    fn serialize(&self) -> Result<Vec<u8>, ApiBindingError> {
        let mut tmp = self.clone();
        tmp.record_digest = String::new();
        let payload =
            serde_json::to_vec(&tmp).map_err(|e| ApiBindingError::Parse(e.to_string()))?;
        let digest = record_digest_hex(&payload);
        tmp.record_digest = digest;
        serde_json::to_vec(&tmp).map_err(|e| ApiBindingError::Parse(e.to_string()))
    }

    pub fn parse(
        bytes: &[u8],
        identity: &DeliveryImmutableIdentity,
    ) -> Result<Self, ApiBindingError> {
        let record: Self =
            serde_json::from_slice(bytes).map_err(|_| ApiBindingError::Parse("json".into()))?;
        if record.schema_version != API_BINDING_SCHEMA_VERSION {
            return Err(ApiBindingError::Parse("schema".into()));
        }
        let mut tmp = record.clone();
        let expected = tmp.record_digest.clone();
        tmp.record_digest = String::new();
        let payload =
            serde_json::to_vec(&tmp).map_err(|_| ApiBindingError::Parse("json".into()))?;
        if record_digest_hex(&payload) != expected {
            return Err(ApiBindingError::ChecksumMismatch);
        }
        if record.delivery_uuid != identity.delivery_uuid
            || record.manifest_digest != identity.manifest_digest
            || record.stem_set_digest != identity.stem_set_digest
        {
            return Err(ApiBindingError::IdentityMismatch);
        }
        let computed = compute_finalized_manifest_digest(&record.manifest);
        if computed != record.manifest_digest {
            return Err(ApiBindingError::Parse("embedded manifest digest".into()));
        }
        Ok(record)
    }
}

pub fn write_api_binding_create_new(
    ledger_dir: &DirHandle,
    record: &ApiBindingRecord,
) -> Result<(), ApiBindingError> {
    if ledger_dir.read_file_all(API_BINDING_FILENAME).is_ok() {
        return Err(ApiBindingError::AlreadyExists);
    }
    let bytes = record.serialize()?;
    let mut file = ledger_dir
        .create_new_file(API_BINDING_FILENAME)
        .map_err(|e| match e {
            FsError::AlreadyExists => ApiBindingError::AlreadyExists,
            other => ApiBindingError::Fs(other),
        })?;
    file.write_all_at(0, &bytes).map_err(ApiBindingError::Fs)?;
    crate::voice_delivery::fs::durability::sync_file(&file).map_err(ApiBindingError::Fs)?;
    Ok(())
}

pub fn read_api_binding(
    ledger_dir: &DirHandle,
    identity: &DeliveryImmutableIdentity,
) -> Result<ApiBindingRecord, ApiBindingError> {
    let bytes = ledger_dir
        .read_file_all(API_BINDING_FILENAME)
        .map_err(ApiBindingError::Fs)?;
    ApiBindingRecord::parse(&bytes, identity)
}

pub fn persist_api_binding_if_absent(
    ledger_dir: &DirHandle,
    identity: &DeliveryImmutableIdentity,
    manifest: &ValidatedManifest,
    syndicate: &SyndicateFinalizedManifest,
    sealed_at: &str,
) -> Result<(), ApiBindingError> {
    match read_api_binding(ledger_dir, identity) {
        Ok(existing) => {
            if existing.sealed_at != sealed_at {
                return Err(ApiBindingError::Parse("sealedAt drift".into()));
            }
            Ok(())
        }
        Err(ApiBindingError::Fs(FsError::Io(e))) if e.kind() == std::io::ErrorKind::NotFound => {
            let record = ApiBindingRecord::from_accepted(identity, manifest, syndicate, sealed_at)?;
            write_api_binding_create_new(ledger_dir, &record)
        }
        Err(e) => Err(e),
    }
}
