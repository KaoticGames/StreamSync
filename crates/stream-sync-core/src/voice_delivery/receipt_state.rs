//! Durable local receipt lifecycle (pending before POST, acked only after API success).

use crate::voice_delivery::fs::{DirHandle, FsError};
use crate::voice_delivery::identity::DeliveryImmutableIdentity;
use crate::voice_delivery::records::record_digest_hex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const RECEIPT_STATE_FILENAME: &str = "receipt-state.json";
pub const RECEIPT_STATE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptPhase {
    Pending,
    Acked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptStateRecord {
    pub schema_version: u32,
    pub phase: ReceiptPhase,
    pub delivery_uuid: String,
    pub manifest_digest: String,
    pub local_receipt_id: String,
    pub record_digest: String,
}

#[derive(Debug, Error)]
pub enum ReceiptStateError {
    #[error("receipt state checksum mismatch")]
    ChecksumMismatch,
    #[error("receipt state identity mismatch")]
    IdentityMismatch,
    #[error("parse: {0}")]
    Parse(String),
    #[error(transparent)]
    Fs(#[from] FsError),
}

impl ReceiptStateRecord {
    pub fn pending(
        identity: &DeliveryImmutableIdentity,
        local_receipt_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: RECEIPT_STATE_SCHEMA_VERSION,
            phase: ReceiptPhase::Pending,
            delivery_uuid: identity.delivery_uuid.clone(),
            manifest_digest: identity.manifest_digest.clone(),
            local_receipt_id: local_receipt_id.into(),
            record_digest: String::new(),
        }
    }

    fn serialize(&self) -> Result<Vec<u8>, ReceiptStateError> {
        let mut tmp = self.clone();
        tmp.record_digest = String::new();
        let payload =
            serde_json::to_vec(&tmp).map_err(|e| ReceiptStateError::Parse(e.to_string()))?;
        tmp.record_digest = record_digest_hex(&payload);
        serde_json::to_vec(&tmp).map_err(|e| ReceiptStateError::Parse(e.to_string()))
    }

    pub fn parse(
        bytes: &[u8],
        identity: &DeliveryImmutableIdentity,
    ) -> Result<Self, ReceiptStateError> {
        let record: Self =
            serde_json::from_slice(bytes).map_err(|_| ReceiptStateError::Parse("json".into()))?;
        if record.schema_version != RECEIPT_STATE_SCHEMA_VERSION {
            return Err(ReceiptStateError::Parse("schema".into()));
        }
        let mut tmp = record.clone();
        let expected = tmp.record_digest.clone();
        tmp.record_digest = String::new();
        let payload =
            serde_json::to_vec(&tmp).map_err(|_| ReceiptStateError::Parse("json".into()))?;
        if record_digest_hex(&payload) != expected {
            return Err(ReceiptStateError::ChecksumMismatch);
        }
        if record.delivery_uuid != identity.delivery_uuid
            || record.manifest_digest != identity.manifest_digest
        {
            return Err(ReceiptStateError::IdentityMismatch);
        }
        Ok(record)
    }
}

pub fn read_receipt_state(
    ledger_dir: &DirHandle,
    identity: &DeliveryImmutableIdentity,
) -> Result<Option<ReceiptStateRecord>, ReceiptStateError> {
    match ledger_dir.read_file_all(RECEIPT_STATE_FILENAME) {
        Ok(bytes) => Ok(Some(ReceiptStateRecord::parse(&bytes, identity)?)),
        Err(FsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn write_receipt_state_replace(
    ledger_dir: &DirHandle,
    record: &ReceiptStateRecord,
) -> Result<(), ReceiptStateError> {
    let bytes = record.serialize()?;
    let mut file = ledger_dir
        .open_or_create_file(RECEIPT_STATE_FILENAME)
        .map_err(ReceiptStateError::Fs)?;
    file.write_all_at(0, &bytes)
        .map_err(ReceiptStateError::Fs)?;
    file.set_len(bytes.len() as u64)
        .map_err(ReceiptStateError::Fs)?;
    crate::voice_delivery::fs::durability::sync_file(&file).map_err(ReceiptStateError::Fs)?;
    Ok(())
}
