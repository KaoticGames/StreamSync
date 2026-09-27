//! Immutable receipt generation records under `.streamsync-control/receipts/`.

use super::generation_read::{
    allocate_next_generation, classify_highest_valid, write_generation_create_new,
    GenerationRecord, GenerationScanError, HighestValidOutcome, ParseFailureKind,
};
use super::{record_digest_hex, GenerationIoError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::session::DeliverySessionGuard;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const RECEIPT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptPhase {
    Pending,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptGeneration {
    pub schema_version: u32,
    pub phase: ReceiptPhase,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub stem_set_digest: String,
    pub local_receipt_id: String,
    pub generation: u64,
    pub record_digest: String,
}

impl GenerationRecord for ReceiptGeneration {
    fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Error)]
pub enum ReceiptGenerationError {
    #[error("structurally invalid receipt record")]
    #[allow(dead_code)]
    Invalid,
    #[error("record checksum mismatch")]
    ChecksumMismatch,
    #[error("identity mismatch")]
    IdentityMismatch,
    #[error("receipt history corrupt")]
    CorruptHistory,
    #[error("illegal receipt transition")]
    Transition,
    #[error("receipt id drift")]
    ReceiptIdDrift,
    #[error(transparent)]
    Io(#[from] GenerationIoError),
    #[error("parse: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiptHistoryState {
    Absent,
    Valid(ReceiptGeneration),
    CorruptHistory,
    IdentityConflict,
}

pub struct ReceiptStore<'guard> {
    guard: &'guard DeliverySessionGuard,
    pub(crate) dir: crate::voice_delivery::fs::DirHandle,
    identity: DeliveryImmutableIdentity,
}

impl<'guard> ReceiptStore<'guard> {
    pub fn open_for_guard(
        guard: &'guard DeliverySessionGuard,
    ) -> Result<Self, ReceiptGenerationError> {
        let dir = guard
            .receipt_dir()
            .map_err(|e| ReceiptGenerationError::Parse(e.to_string()))?;
        Ok(Self {
            identity: guard.identity().clone(),
            dir,
            guard,
        })
    }

    /// Classify receipt generation history (absent, valid, corrupt, or identity conflict).
    pub fn scan_history(&self) -> Result<ReceiptHistoryState, ReceiptGenerationError> {
        let identity = &self.identity;
        match classify_highest_valid::<ReceiptGeneration, _, _>(
            &self.dir,
            |rec| receipt_identity_matches(identity, rec),
            parse_receipt_file,
        )
        .map_err(map_scan_err)?
        {
            HighestValidOutcome::Absent => Ok(ReceiptHistoryState::Absent),
            HighestValidOutcome::Valid(rec) => Ok(ReceiptHistoryState::Valid(rec)),
            HighestValidOutcome::CorruptHistory => Ok(ReceiptHistoryState::CorruptHistory),
            HighestValidOutcome::IdentityConflict => Ok(ReceiptHistoryState::IdentityConflict),
        }
    }

    pub fn read_highest_valid(&self) -> Result<Option<ReceiptGeneration>, ReceiptGenerationError> {
        match self.scan_history()? {
            ReceiptHistoryState::Absent => Ok(None),
            ReceiptHistoryState::Valid(rec) => Ok(Some(rec)),
            ReceiptHistoryState::CorruptHistory => Err(ReceiptGenerationError::CorruptHistory),
            ReceiptHistoryState::IdentityConflict => Err(ReceiptGenerationError::IdentityMismatch),
        }
    }

    pub fn commit_pending(
        &self,
        local_receipt_id: &str,
    ) -> Result<ReceiptGeneration, ReceiptGenerationError> {
        self.guard
            .assert_same_identity(&self.identity)
            .map_err(|_| ReceiptGenerationError::IdentityMismatch)?;
        let prior = self.read_highest_valid()?;
        if let Some(p) = &prior {
            if p.phase == ReceiptPhase::Acked {
                return Err(ReceiptGenerationError::Transition);
            }
            if p.local_receipt_id != local_receipt_id {
                return Err(ReceiptGenerationError::ReceiptIdDrift);
            }
        }
        self.write_record(ReceiptPhase::Pending, local_receipt_id)
    }

    pub fn commit_acked(
        &self,
        local_receipt_id: &str,
    ) -> Result<ReceiptGeneration, ReceiptGenerationError> {
        self.guard
            .assert_same_identity(&self.identity)
            .map_err(|_| ReceiptGenerationError::IdentityMismatch)?;
        let prior = self.read_highest_valid()?;
        match prior.as_ref().map(|p| p.phase) {
            None => return Err(ReceiptGenerationError::Transition),
            Some(ReceiptPhase::Acked) => {
                if prior
                    .as_ref()
                    .is_some_and(|p| p.local_receipt_id == local_receipt_id)
                {
                    return Ok(prior.unwrap());
                }
                return Err(ReceiptGenerationError::ReceiptIdDrift);
            }
            Some(ReceiptPhase::Pending) => {
                if prior
                    .as_ref()
                    .is_none_or(|p| p.local_receipt_id != local_receipt_id)
                {
                    return Err(ReceiptGenerationError::ReceiptIdDrift);
                }
            }
        }
        self.write_record(ReceiptPhase::Acked, local_receipt_id)
    }

    fn write_record(
        &self,
        phase: ReceiptPhase,
        local_receipt_id: &str,
    ) -> Result<ReceiptGeneration, ReceiptGenerationError> {
        let generation = allocate_next_generation(&self.dir).map_err(map_scan_err)?;
        let record = ReceiptGeneration {
            schema_version: RECEIPT_SCHEMA_VERSION,
            phase,
            delivery_uuid: self.identity.delivery_uuid.clone(),
            manifest_digest: self.identity.manifest_digest.clone(),
            stem_set_digest: self.identity.stem_set_digest.clone(),
            local_receipt_id: local_receipt_id.to_string(),
            generation,
            record_digest: String::new(),
        };
        let bytes = serialize_with_digest(&record)?;
        write_generation_create_new(&self.dir, generation, &bytes).map_err(map_scan_err)?;
        parse_receipt_bytes(&bytes)
    }
}

fn map_scan_err(err: GenerationScanError) -> ReceiptGenerationError {
    match err {
        GenerationScanError::IdentityMismatch => ReceiptGenerationError::IdentityMismatch,
        GenerationScanError::ChecksumMismatch => ReceiptGenerationError::ChecksumMismatch,
        GenerationScanError::Io(e) => ReceiptGenerationError::Io(e),
        GenerationScanError::Parse(s) => ReceiptGenerationError::Parse(s),
    }
}

fn receipt_identity_matches(
    identity: &DeliveryImmutableIdentity,
    rec: &ReceiptGeneration,
) -> Result<(), ParseFailureKind> {
    if identity.foreign_identity_in_receipt_record(rec) {
        Err(ParseFailureKind::ForeignIdentity)
    } else {
        Ok(())
    }
}

fn parse_receipt_file(name: &str, data: &[u8]) -> Result<ReceiptGeneration, ParseFailureKind> {
    let file_gen =
        super::parse_generation_filename(name).ok_or(ParseFailureKind::MalformedContent)?;
    let record = parse_receipt_bytes(data).map_err(|e| match e {
        ReceiptGenerationError::ChecksumMismatch => ParseFailureKind::ChecksumMismatch,
        ReceiptGenerationError::IdentityMismatch => ParseFailureKind::ForeignIdentity,
        _ => ParseFailureKind::MalformedContent,
    })?;
    if record.generation != file_gen {
        return Err(ParseFailureKind::FilenameGenerationMismatch);
    }
    Ok(record)
}

pub fn parse_receipt_bytes(bytes: &[u8]) -> Result<ReceiptGeneration, ReceiptGenerationError> {
    let record: ReceiptGeneration =
        serde_json::from_slice(bytes).map_err(|_| ReceiptGenerationError::Parse("json".into()))?;
    if record.schema_version != RECEIPT_SCHEMA_VERSION {
        return Err(ReceiptGenerationError::Parse("schema".into()));
    }
    let mut tmp = record.clone();
    let expected = tmp.record_digest.clone();
    tmp.record_digest = String::new();
    let payload =
        serde_json::to_vec(&tmp).map_err(|_| ReceiptGenerationError::Parse("json".into()))?;
    if record_digest_hex(&payload) != expected {
        return Err(ReceiptGenerationError::ChecksumMismatch);
    }
    Ok(record)
}

fn serialize_with_digest(record: &ReceiptGeneration) -> Result<Vec<u8>, ReceiptGenerationError> {
    let mut tmp = record.clone();
    tmp.record_digest = String::new();
    let payload =
        serde_json::to_vec(&tmp).map_err(|e| ReceiptGenerationError::Parse(e.to_string()))?;
    tmp.record_digest = record_digest_hex(&payload);
    serde_json::to_vec(&tmp).map_err(|e| ReceiptGenerationError::Parse(e.to_string()))
}

#[cfg(test)]
mod receipt_generation_tests {
    use super::*;
    use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
    use crate::voice_delivery::records::generation_read::scan_max_canonical_generation_number;
    use crate::voice_delivery::records::ledger_generation::LedgerStore;
    use crate::voice_delivery::state::LedgerState;

    fn stem_manifest() -> ValidatedManifest {
        ValidatedManifest::validate(vec![StemManifestEntry {
            file_name: "a.wav".into(),
            byte_count: 44,
            sha256: "a".repeat(64),
        }])
        .unwrap()
    }

    fn stage_token(suffix: &str) -> String {
        format!(".streamsync-stage-{suffix}")
    }

    fn open_fixture(
        delivery_uuid: &str,
    ) -> (
        tempfile::TempDir,
        DeliverySessionGuard,
        DeliveryImmutableIdentity,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let root = crate::voice_delivery::fs::DestRoot::open(tmp.path()).unwrap();
        let manifest = stem_manifest();
        let identity = DeliveryImmutableIdentity::new_bound(
            delivery_uuid,
            manifest.digest(),
            &manifest,
            stage_token("01234567890123456789012345678901"),
            vec![],
            "final",
        )
        .unwrap();
        let guard = DeliverySessionGuard::begin(root, identity.clone(), manifest, true).unwrap();
        (tmp, guard, identity)
    }

    fn signed_receipt_bytes(
        identity: &DeliveryImmutableIdentity,
        phase: ReceiptPhase,
        generation: u64,
        local_receipt_id: &str,
        overrides: ReceiptOverrides<'_>,
    ) -> Vec<u8> {
        let record = ReceiptGeneration {
            schema_version: RECEIPT_SCHEMA_VERSION,
            phase,
            delivery_uuid: overrides
                .delivery_uuid
                .unwrap_or(&identity.delivery_uuid)
                .to_string(),
            manifest_digest: overrides
                .manifest_digest
                .unwrap_or(&identity.manifest_digest)
                .to_string(),
            stem_set_digest: overrides
                .stem_set_digest
                .unwrap_or(&identity.stem_set_digest)
                .to_string(),
            local_receipt_id: local_receipt_id.to_string(),
            generation,
            record_digest: String::new(),
        };
        serialize_with_digest(&record).unwrap()
    }

    struct ReceiptOverrides<'a> {
        delivery_uuid: Option<&'a str>,
        manifest_digest: Option<&'a str>,
        stem_set_digest: Option<&'a str>,
    }

    fn plant_generation(dir: &crate::voice_delivery::fs::DirHandle, gen: u64, bytes: &[u8]) {
        let name = crate::voice_delivery::records::generation_filename(gen).unwrap();
        dir.create_new_file(&name)
            .unwrap()
            .write_all_at(0, bytes)
            .unwrap();
    }

    #[test]
    fn empty_receipt_dir_scan_is_absent() {
        let (_tmp, guard, _identity) = open_fixture("receipt-absent-empty");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        assert_eq!(store.scan_history().unwrap(), ReceiptHistoryState::Absent);
        assert!(store.read_highest_valid().unwrap().is_none());
    }

    #[test]
    fn all_malformed_generations_are_corrupt_history() {
        let (_tmp, guard, _identity) = open_fixture("receipt-all-malformed");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        plant_generation(&dir, 1, b"{not-json");
        plant_generation(&dir, 2, b"also bad");
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::CorruptHistory
        );
        assert!(matches!(
            store.read_highest_valid(),
            Err(ReceiptGenerationError::CorruptHistory)
        ));
    }

    #[test]
    fn checksum_corrupt_generation_is_corrupt_history() {
        let (_tmp, guard, identity) = open_fixture("receipt-checksum-corrupt");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let bytes = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            3,
            "rid",
            ReceiptOverrides {
                delivery_uuid: None,
                manifest_digest: None,
                stem_set_digest: None,
            },
        );
        let mut corrupt = bytes;
        corrupt.extend_from_slice(b"tamper");
        plant_generation(&dir, 3, &corrupt);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::CorruptHistory
        );
    }

    #[test]
    fn stem_digest_mismatch_is_identity_conflict() {
        let (_tmp, guard, identity) = open_fixture("receipt-stem-mismatch");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let wrong_stem = "b".repeat(64);
        let bytes = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            1,
            "rid",
            ReceiptOverrides {
                delivery_uuid: None,
                manifest_digest: None,
                stem_set_digest: Some(&wrong_stem),
            },
        );
        plant_generation(&dir, 1, &bytes);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::IdentityConflict
        );
    }

    #[test]
    fn api_manifest_digest_mismatch_is_identity_conflict() {
        let (_tmp, guard, identity) = open_fixture("receipt-api-mismatch");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let wrong_api = "c".repeat(64);
        let bytes = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            1,
            "rid",
            ReceiptOverrides {
                delivery_uuid: None,
                manifest_digest: Some(&wrong_api),
                stem_set_digest: None,
            },
        );
        plant_generation(&dir, 1, &bytes);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::IdentityConflict
        );
    }

    #[test]
    fn valid_foreign_delivery_is_identity_conflict() {
        let (_tmp, guard, identity) = open_fixture("receipt-foreign-delivery");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let bytes = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            1,
            "rid",
            ReceiptOverrides {
                delivery_uuid: Some("other-delivery"),
                manifest_digest: None,
                stem_set_digest: None,
            },
        );
        plant_generation(&dir, 1, &bytes);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::IdentityConflict
        );
    }

    #[test]
    fn valid_exact_plus_valid_foreign_is_identity_conflict() {
        let (_tmp, guard, identity) = open_fixture("receipt-exact-and-foreign");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let exact = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            1,
            "rid",
            ReceiptOverrides {
                delivery_uuid: None,
                manifest_digest: None,
                stem_set_digest: None,
            },
        );
        plant_generation(&dir, 1, &exact);
        let foreign = signed_receipt_bytes(
            &identity,
            ReceiptPhase::Pending,
            2,
            "rid",
            ReceiptOverrides {
                delivery_uuid: Some("foreign"),
                manifest_digest: None,
                stem_set_digest: None,
            },
        );
        plant_generation(&dir, 2, &foreign);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::IdentityConflict
        );
    }

    #[test]
    fn acked_with_corrupt_higher_generation_stays_acked() {
        let (_tmp, guard, identity) = open_fixture("receipt-acked-corrupt-high");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let rid = format!("{}:1", identity.delivery_uuid);
        store.commit_pending(&rid).unwrap();
        store.commit_acked(&rid).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        plant_generation(&dir, 50, b"{ corrupt after ack");
        let acked = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(acked.phase, ReceiptPhase::Acked);
        assert_eq!(
            store.scan_history().unwrap(),
            ReceiptHistoryState::Valid(acked)
        );
    }

    #[test]
    fn generation_allocates_beyond_malformed_observed_max() {
        let (_tmp, guard, identity) = open_fixture("receipt-alloc-past-malformed");
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let rid = format!("{}:1", identity.delivery_uuid);
        store.commit_pending(&rid).unwrap();
        plant_generation(&dir, 40, b"{bad");
        let acked = store.commit_acked(&rid).unwrap();
        assert_eq!(acked.generation, 41);
        assert_eq!(scan_max_canonical_generation_number(&dir).unwrap(), 41);
    }

    #[test]
    fn pending_before_post_then_acked_monotonic_generations() {
        let tmp = tempfile::tempdir().unwrap();
        let root = crate::voice_delivery::fs::DestRoot::open(tmp.path()).unwrap();
        let manifest = stem_manifest();
        let identity = DeliveryImmutableIdentity::new_bound(
            "delivery-1",
            manifest.digest(),
            &manifest,
            stage_token("01234567890123456789012345678901"),
            vec![],
            "final",
        )
        .unwrap();
        let guard = DeliverySessionGuard::begin(root, identity.clone(), manifest, true).unwrap();
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let rid = format!("{}:1", identity.delivery_uuid);
        let pending = store.commit_pending(&rid).unwrap();
        assert_eq!(pending.phase, ReceiptPhase::Pending);
        assert_eq!(pending.generation, 1);
        let acked = store.commit_acked(&rid).unwrap();
        assert_eq!(acked.phase, ReceiptPhase::Acked);
        assert_eq!(acked.generation, 2);
        assert_eq!(
            store.read_highest_valid().unwrap().unwrap().phase,
            ReceiptPhase::Acked
        );
    }

    #[test]
    fn malformed_higher_generation_does_not_erase_lower_valid() {
        let tmp = tempfile::tempdir().unwrap();
        let root = crate::voice_delivery::fs::DestRoot::open(tmp.path()).unwrap();
        let manifest = stem_manifest();
        let identity = DeliveryImmutableIdentity::new_bound(
            "delivery-malformed-high",
            manifest.digest(),
            &manifest,
            stage_token("01234567890123456789012345678901"),
            vec![],
            "final",
        )
        .unwrap();
        let guard = DeliverySessionGuard::begin(root, identity.clone(), manifest, true).unwrap();
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let rid = format!("{}:9", identity.delivery_uuid);
        store.commit_pending(&rid).unwrap();
        let dir = store.dir.clone_handle().unwrap();
        let garbage_name = crate::voice_delivery::records::generation_filename(99).unwrap();
        dir.create_new_file(&garbage_name)
            .unwrap()
            .write_all_at(0, b"{not-json")
            .unwrap();
        let highest = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(highest.generation, 1);
        assert_eq!(highest.phase, ReceiptPhase::Pending);
        assert_eq!(scan_max_canonical_generation_number(&dir).unwrap(), 99);
    }

    #[test]
    fn crash_after_api_ack_before_local_acked_retries_idempotently() {
        let tmp = tempfile::tempdir().unwrap();
        let root = crate::voice_delivery::fs::DestRoot::open(tmp.path()).unwrap();
        let manifest = stem_manifest();
        let identity = DeliveryImmutableIdentity::new_bound(
            "delivery-crash-ack",
            manifest.digest(),
            &manifest,
            stage_token("01234567890123456789012345678901"),
            vec![],
            "final",
        )
        .unwrap();
        let guard = DeliverySessionGuard::begin(root, identity.clone(), manifest, true).unwrap();
        let ledger = LedgerStore::open_for_guard(&guard).unwrap();
        ledger.commit(&guard, LedgerState::Receiving).unwrap();
        let store = ReceiptStore::open_for_guard(&guard).unwrap();
        let rid = format!("{}:1", identity.delivery_uuid);
        store.commit_pending(&rid).unwrap();
        // Simulate API success without local ack generation.
        store.commit_acked(&rid).unwrap();
        let again = store.read_highest_valid().unwrap().unwrap();
        assert_eq!(again.phase, ReceiptPhase::Acked);
        assert!(store.commit_acked(&rid).is_ok());
    }
}
