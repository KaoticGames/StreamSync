//! Immutable ledger generation records under `.streamsync-control/ledgers/`.

use super::{
    generation_filename, next_generation_after_max, parse_generation_filename, record_digest_hex,
    GenerationIoError, GEN_PREFIX,
};
use crate::voice_delivery::fs::durability::{sync_dir_exact, sync_file};
use crate::voice_delivery::fs::{DestRoot, DirHandle, FsError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::session::DeliverySessionGuard;
use crate::voice_delivery::state::{validate_ledger_transition, LedgerState};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const LEDGER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerGeneration {
    pub schema_version: u32,
    pub state: LedgerState,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub staging_token: String,
    pub final_parent_relative: Vec<String>,
    pub final_session_name: String,
    pub generation: u64,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum LedgerGenerationError {
    #[error("structurally invalid ledger record")]
    Invalid,
    #[error("record checksum mismatch")]
    ChecksumMismatch,
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("illegal state transition")]
    Transition,
    #[error(transparent)]
    Io(#[from] GenerationIoError),
    #[error("parse: {0}")]
    Parse(String),
}

pub struct LedgerStore {
    pub(crate) dir: DirHandle,
}

impl LedgerStore {
    pub fn open_at_root(
        root: &DestRoot,
        identity: &DeliveryImmutableIdentity,
    ) -> Result<Self, FsError> {
        let comps = crate::voice_delivery::ids::ledger_dir_relative_components(
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
        identity: &DeliveryImmutableIdentity,
    ) -> Result<Option<LedgerGeneration>, LedgerGenerationError> {
        let mut best: Option<LedgerGeneration> = None;
        for name in self
            .dir
            .list_child_names()
            .map_err(|e| LedgerGenerationError::Parse(e.to_string()))?
        {
            if parse_generation_filename(&name).is_none() {
                continue;
            }
            let data = match self.dir.read_file_all(&name) {
                Ok(d) => d,
                Err(_) => continue,
            };
            match parse_ledger_bytes(&data) {
                Ok(rec) => {
                    if !identity.matches_record_fields(
                        &rec.delivery_uuid,
                        &rec.manifest_digest,
                        &rec.staging_token,
                    ) {
                        return Err(LedgerGenerationError::IdentityMismatch);
                    }
                    if best
                        .as_ref()
                        .map(|b| rec.generation > b.generation)
                        .unwrap_or(true)
                    {
                        best = Some(rec);
                    }
                }
                Err(LedgerGenerationError::ChecksumMismatch) => {
                    return Err(LedgerGenerationError::ChecksumMismatch);
                }
                Err(_) => {}
            }
        }
        Ok(best)
    }

    pub fn commit(
        &self,
        _guard: &DeliverySessionGuard,
        identity: &DeliveryImmutableIdentity,
        state: LedgerState,
        prior: Option<&LedgerGeneration>,
    ) -> Result<LedgerGeneration, LedgerGenerationError> {
        let from_state = prior.map(|p| p.state);
        validate_ledger_transition(from_state, state)
            .map_err(|_| LedgerGenerationError::Transition)?;
        let max_observed = scan_max_generation_number(&self.dir)?;
        let generation = next_generation_after_max(max_observed)?;
        let record = LedgerGeneration {
            schema_version: LEDGER_SCHEMA_VERSION,
            state,
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            staging_token: identity.staging_token.clone(),
            final_parent_relative: identity.final_parent_relative.clone(),
            final_session_name: identity.final_session_name.clone(),
            generation,
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&record)?;
        let name = generation_filename(generation)?;
        write_generation_create_new(&self.dir, &name, &bytes)?;
        let written = parse_ledger_bytes(&bytes)?;
        Ok(written)
    }
}

fn scan_max_generation_number(dir: &DirHandle) -> Result<u64, LedgerGenerationError> {
    let mut max = 0u64;
    for name in dir
        .list_child_names()
        .map_err(|e| LedgerGenerationError::Parse(e.to_string()))?
    {
        if let Some(n) = parse_generation_filename(&name) {
            max = max.max(n);
        } else if name.starts_with(GEN_PREFIX) {
            max = max.saturating_add(1);
        }
    }
    Ok(max)
}

fn serialize_with_digest(record: &LedgerGeneration) -> Result<Vec<u8>, LedgerGenerationError> {
    let mut tmp = record.clone();
    tmp.record_digest = String::new();
    let payload =
        serde_json::to_vec(&tmp).map_err(|e| LedgerGenerationError::Parse(e.to_string()))?;
    let digest = record_digest_hex(&payload);
    tmp.record_digest = digest;
    serde_json::to_vec(&tmp).map_err(|e| LedgerGenerationError::Parse(e.to_string()))
}

pub fn parse_ledger_bytes(data: &[u8]) -> Result<LedgerGeneration, LedgerGenerationError> {
    let record: LedgerGeneration =
        serde_json::from_slice(data).map_err(|_| LedgerGenerationError::Invalid)?;
    validate_ledger_record(&record)?;
    Ok(record)
}

pub fn validate_ledger_record(record: &LedgerGeneration) -> Result<(), LedgerGenerationError> {
    if record.schema_version != LEDGER_SCHEMA_VERSION {
        return Err(LedgerGenerationError::Invalid);
    }
    if record.record_digest.len() != 64 {
        return Err(LedgerGenerationError::Invalid);
    }
    let mut tmp = record.clone();
    let expected = tmp.record_digest.clone();
    tmp.record_digest = String::new();
    let payload = serde_json::to_vec(&tmp).map_err(|_| LedgerGenerationError::Invalid)?;
    let digest = record_digest_hex(&payload);
    if digest != expected {
        return Err(LedgerGenerationError::ChecksumMismatch);
    }
    Ok(())
}

fn write_generation_create_new(
    dir: &DirHandle,
    name: &str,
    bytes: &[u8],
) -> Result<(), LedgerGenerationError> {
    let mut file = dir.create_new_file(name).map_err(|e| match e {
        FsError::AlreadyExists => LedgerGenerationError::Io(GenerationIoError::AlreadyExists),
        other => LedgerGenerationError::Parse(other.to_string()),
    })?;
    file.write_all_at(0, bytes)
        .map_err(|e| LedgerGenerationError::Parse(e.to_string()))?;
    sync_file(&file).map_err(|e| LedgerGenerationError::Parse(e.to_string()))?;
    let _ = sync_dir_exact(dir);
    Ok(())
}

#[cfg(test)]
mod ledger_generation_wins_valid {
    use super::*;
    use crate::voice_delivery::ids::delivery_opaque_dir_id;
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs;

    fn fixture() -> (
        tempfile::TempDir,
        DestRoot,
        DeliveryImmutableIdentity,
        DeliverySessionGuard,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let root = DestRoot::open(tmp.path()).unwrap();
        let stems = vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 100,
            sha256: "a".repeat(64),
        }];
        let manifest = ValidatedManifest::validate(stems).unwrap();
        let identity = DeliveryImmutableIdentity::new(
            "delivery-test-uuid",
            &manifest,
            ".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef",
            vec!["guild".into()],
            "session-final",
        )
        .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-test-uuid", true).unwrap();
        let guard = DeliverySessionGuard::new(lock);
        (tmp, root, identity, guard)
    }

    #[test]
    fn ledger_generation_wins_valid() {
        let (tmp, root, identity, guard) = fixture();
        let store = LedgerStore::open_at_root(&root, &identity).unwrap();
        let g1 = store
            .commit(&guard, &identity, LedgerState::Receiving, None)
            .unwrap();
        assert_eq!(g1.generation, 1);
        let g2 = store
            .commit(&guard, &identity, LedgerState::Receiving, Some(&g1))
            .unwrap();
        assert_eq!(g2.generation, 2);
        let ledger_dir = tmp.path().join(".streamsync-control/ledgers");
        let opaque = delivery_opaque_dir_id("delivery-test-uuid");
        let gen_dir = ledger_dir.join(opaque);
        fs::write(gen_dir.join("gen-999.json"), b"{not json").unwrap();
        let highest = store.read_highest_valid(&identity).unwrap().unwrap();
        assert_eq!(highest.generation, 2);
    }

    #[test]
    fn checksum_mismatch_rejected() {
        let (_tmp, root, identity, _guard) = fixture();
        let store = LedgerStore::open_at_root(&root, &identity).unwrap();
        let gen_dir_path = _tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(identity.opaque_delivery_id.clone());
        let record = LedgerGeneration {
            schema_version: LEDGER_SCHEMA_VERSION,
            state: LedgerState::Receiving,
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            staging_token: identity.staging_token.clone(),
            final_parent_relative: identity.final_parent_relative.clone(),
            final_session_name: identity.final_session_name.clone(),
            generation: 5,
            record_digest: "f".repeat(64),
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        fs::write(gen_dir_path.join("gen-5.json"), bytes).unwrap();
        assert!(matches!(
            store.read_highest_valid(&identity),
            Err(LedgerGenerationError::ChecksumMismatch)
        ));
    }

    #[test]
    fn identity_swap_fails_closed() {
        let (_tmp, root, identity, _guard) = fixture();
        let store = LedgerStore::open_at_root(&root, &identity).unwrap();
        let gen_dir_path = _tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(identity.opaque_delivery_id.clone());
        let other = LedgerGeneration {
            schema_version: LEDGER_SCHEMA_VERSION,
            state: LedgerState::Receiving,
            delivery_uuid: "other".into(),
            manifest_digest: identity.manifest_digest.clone(),
            staging_token: identity.staging_token.clone(),
            final_parent_relative: identity.final_parent_relative.clone(),
            final_session_name: identity.final_session_name.clone(),
            generation: 3,
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&other).unwrap();
        fs::write(gen_dir_path.join("gen-3.json"), bytes).unwrap();
        assert!(matches!(
            store.read_highest_valid(&identity),
            Err(LedgerGenerationError::IdentityMismatch)
        ));
    }

    #[test]
    fn cannot_replace_existing_generation() {
        let (_tmp, root, identity, guard) = fixture();
        let store = LedgerStore::open_at_root(&root, &identity).unwrap();
        let _ = store
            .commit(&guard, &identity, LedgerState::Receiving, None)
            .unwrap();
        let gen_dir = store.dir.clone_handle().unwrap();
        let err = write_generation_create_new(&gen_dir, "gen-1.json", b"{}");
        assert!(matches!(
            err,
            Err(LedgerGenerationError::Io(GenerationIoError::AlreadyExists))
        ));
    }
}
