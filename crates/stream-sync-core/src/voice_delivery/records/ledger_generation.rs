//! Immutable ledger generation records under `.streamsync-control/ledgers/`.

use super::generation_read::{
    allocate_next_generation, select_highest_valid, write_generation_create_new, GenerationRecord,
    GenerationScanError, ParseFailureKind,
};
use super::{record_digest_hex, GenerationIoError};
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

impl GenerationRecord for LedgerGeneration {
    fn generation(&self) -> u64 {
        self.generation
    }
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
    pub(crate) dir: crate::voice_delivery::fs::DirHandle,
    identity: DeliveryImmutableIdentity,
}

impl LedgerStore {
    pub(crate) fn open_for_guard(
        guard: &DeliverySessionGuard,
    ) -> Result<Self, LedgerGenerationError> {
        let dir = guard
            .ledger_dir()
            .map_err(|e| LedgerGenerationError::Parse(e.to_string()))?;
        Ok(Self {
            dir,
            identity: guard.identity().clone(),
        })
    }

    pub fn read_highest_valid(&self) -> Result<Option<LedgerGeneration>, LedgerGenerationError> {
        let identity = &self.identity;
        select_highest_valid::<LedgerGeneration, _, _>(
            &self.dir,
            |rec: &LedgerGeneration| {
                if identity.foreign_identity_in_record(
                    &rec.delivery_uuid,
                    &rec.manifest_digest,
                    &rec.staging_token,
                    &rec.final_parent_relative,
                    &rec.final_session_name,
                ) {
                    Err(ParseFailureKind::ForeignIdentity)
                } else if !identity.matches_ledger_record(rec) {
                    Err(ParseFailureKind::MalformedContent)
                } else {
                    Ok(())
                }
            },
            |name, data| parse_ledger_file(name, data),
        )
        .map_err(map_scan_err)
    }

    pub fn commit(
        &self,
        guard: &DeliverySessionGuard,
        state: LedgerState,
    ) -> Result<LedgerGeneration, LedgerGenerationError> {
        guard
            .assert_same_identity(&self.identity)
            .map_err(|_| LedgerGenerationError::IdentityMismatch)?;
        let prior = self.read_highest_valid()?;
        let from_state = prior.as_ref().map(|p| p.state);
        validate_ledger_transition(from_state, state)
            .map_err(|_| LedgerGenerationError::Transition)?;
        let generation = allocate_next_generation(&self.dir).map_err(map_scan_err)?;
        let record = LedgerGeneration {
            schema_version: LEDGER_SCHEMA_VERSION,
            state,
            delivery_uuid: self.identity.delivery_uuid.clone(),
            manifest_digest: self.identity.manifest_digest.clone(),
            staging_token: self.identity.staging_token.clone(),
            final_parent_relative: self.identity.final_parent_relative.clone(),
            final_session_name: self.identity.final_session_name.clone(),
            generation,
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&record)?;
        write_generation_create_new(&self.dir, generation, &bytes).map_err(map_scan_err)?;
        parse_ledger_bytes(&bytes)
    }
}

fn map_scan_err(err: GenerationScanError) -> LedgerGenerationError {
    match err {
        GenerationScanError::IdentityMismatch => LedgerGenerationError::IdentityMismatch,
        GenerationScanError::ChecksumMismatch => LedgerGenerationError::ChecksumMismatch,
        GenerationScanError::Io(e) => LedgerGenerationError::Io(e),
        GenerationScanError::Parse(s) => LedgerGenerationError::Parse(s),
    }
}

fn parse_ledger_file(name: &str, data: &[u8]) -> Result<LedgerGeneration, ParseFailureKind> {
    let file_gen =
        super::parse_generation_filename(name).ok_or(ParseFailureKind::MalformedContent)?;
    let record = parse_ledger_bytes(data).map_err(|e| match e {
        LedgerGenerationError::ChecksumMismatch => ParseFailureKind::ChecksumMismatch,
        LedgerGenerationError::IdentityMismatch => ParseFailureKind::ForeignIdentity,
        _ => ParseFailureKind::MalformedContent,
    })?;
    if record.generation != file_gen {
        return Err(ParseFailureKind::FilenameGenerationMismatch);
    }
    Ok(record)
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
    if record.record_digest.len() != 64
        || !record
            .record_digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
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

#[cfg(test)]
mod ledger_generation_wins_valid {
    use super::*;
    use crate::voice_delivery::fs::DestRoot;
    use crate::voice_delivery::ids::delivery_opaque_dir_id;
    use crate::voice_delivery::lock::acquire_delivery_domain_lock;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::session::DeliverySessionGuard;
    use std::fs;

    fn fixture() -> (
        tempfile::TempDir,
        DeliverySessionGuard,
        DeliveryImmutableIdentity,
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
        root.create_child_dir(".streamsync-stage-deadbeefdeadbeefdeadbeefdeadbeef")
            .unwrap();
        let lock = acquire_delivery_domain_lock(&root, "delivery-test-uuid", true).unwrap();
        let guard = DeliverySessionGuard::open(root, identity.clone(), manifest, lock).unwrap();
        (tmp, guard, identity)
    }

    #[test]
    fn ledger_generation_wins_valid() {
        let (tmp, guard, _identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        let g1 = store.commit(&guard, LedgerState::Receiving).unwrap();
        assert_eq!(g1.generation, 1);
        let g2 = store.commit(&guard, LedgerState::Receiving).unwrap();
        assert_eq!(g2.generation, 2);
        let ledger_dir = tmp.path().join(".streamsync-control/ledgers");
        let opaque = delivery_opaque_dir_id("delivery-test-uuid");
        let gen_dir = ledger_dir.join(opaque);
        fs::write(gen_dir.join("gen-999.json"), b"{not json").unwrap();
        let highest = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(highest.generation, 2);
    }

    #[test]
    fn checksum_mismatch_higher_falls_back() {
        let (tmp, guard, _identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        let _ = store.commit(&guard, LedgerState::Receiving).unwrap();
        let gen_dir_path = tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(_identity.opaque_delivery_id.clone());
        let record = LedgerGeneration {
            schema_version: LEDGER_SCHEMA_VERSION,
            state: LedgerState::Receiving,
            delivery_uuid: _identity.delivery_uuid.clone(),
            manifest_digest: _identity.manifest_digest.clone(),
            staging_token: _identity.staging_token.clone(),
            final_parent_relative: _identity.final_parent_relative.clone(),
            final_session_name: _identity.final_session_name.clone(),
            generation: 5,
            record_digest: "f".repeat(64),
        };
        let bytes = serde_json::to_vec(&record).unwrap();
        fs::write(gen_dir_path.join("gen-5.json"), bytes).unwrap();
        let highest = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(highest.generation, 1);
    }

    #[test]
    fn identity_swap_fails_closed() {
        let (tmp, guard, identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        let gen_dir_path = tmp
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
            store.read_highest_valid(),
            Err(LedgerGenerationError::IdentityMismatch)
        ));
    }

    #[test]
    fn cannot_replace_existing_generation() {
        let (_tmp, guard, _identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        let _ = store.commit(&guard, LedgerState::Receiving).unwrap();
        let gen_dir = store.dir.clone_handle().unwrap();
        let err = gen_dir.create_new_file("gen-1.json");
        assert!(matches!(
            err,
            Err(crate::voice_delivery::fs::FsError::AlreadyExists)
        ));
    }

    #[test]
    fn skip_transition_publish_intent_from_receiving_rejected() {
        let (_tmp, guard, _identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        store.commit(&guard, LedgerState::Receiving).unwrap();
        let err = store.commit(&guard, LedgerState::PublishIntent);
        assert!(matches!(err, Err(LedgerGenerationError::Transition)));
    }

    #[test]
    fn filename_record_generation_mismatch_skipped() {
        let (tmp, guard, identity) = fixture();
        let store = LedgerStore::open_for_guard(&guard).unwrap();
        store.commit(&guard, LedgerState::Receiving).unwrap();
        let gen_dir_path = tmp
            .path()
            .join(".streamsync-control/ledgers")
            .join(identity.opaque_delivery_id.clone());
        let mut record = store.read_highest_valid().unwrap().unwrap();
        record.generation = 99;
        let bytes = serialize_with_digest(&record).unwrap();
        fs::write(gen_dir_path.join("gen-2.json"), bytes).unwrap();
        let highest = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(highest.generation, 1);
    }
}
