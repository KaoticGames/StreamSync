//! Complete SYNJ segment validation before cumulative checkpoint commit.

use crate::voice_delivery::local_finalization::synj::{
    decode_journal_record, read_file_header, JournalFramingError, FILE_HEADER_BYTES,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectSynjValidationError {
    #[error("invalid synj segment")]
    InvalidSynj,
}

pub(crate) fn validate_complete_synj_segment(
    bytes: &[u8],
) -> Result<(), DirectSynjValidationError> {
    let version = match read_file_header(bytes) {
        Ok(v) => v,
        Err(JournalFramingError::Fatal(_) | JournalFramingError::Recoverable(_)) => {
            return Err(DirectSynjValidationError::InvalidSynj);
        }
    };
    let mut offset = FILE_HEADER_BYTES;
    if offset == bytes.len() {
        return Err(DirectSynjValidationError::InvalidSynj);
    }
    while offset < bytes.len() {
        match decode_journal_record(bytes, offset, version) {
            Ok((_, next)) => offset = next,
            Err(JournalFramingError::Fatal(_) | JournalFramingError::Recoverable(_)) => {
                return Err(DirectSynjValidationError::InvalidSynj);
            }
        }
    }
    Ok(())
}
