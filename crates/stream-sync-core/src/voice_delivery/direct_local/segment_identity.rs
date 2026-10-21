//! `LogicalSegmentIdentity` binds session, generation, sequence, length, and digest.

use super::limits::JS_MAX_SAFE_INTEGER;
use crate::voice_delivery::finalized_manifest::validate_sha256_hex;
use crate::voice_delivery::finalized_manifest::FinalizedManifestError;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSegmentIdentity {
    pub session_id: String,
    pub delivery_generation: u64,
    pub segment_sequence: u64,
    pub byte_length: u64,
    pub sha256_hex: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LogicalSegmentIdentityError {
    #[error("malformed sha256")]
    MalformedSha256,
    #[error("stale delivery generation")]
    StaleGeneration,
    #[error(
        "delivery generation mismatch: identity={identity_generation} binding={binding_generation}"
    )]
    GenerationMismatch {
        identity_generation: u64,
        binding_generation: u64,
    },
    #[error("segment sequence exceeds js safe integer")]
    SegmentSequenceNotJsSafe,
    #[error("byte length exceeds js safe integer")]
    ByteLengthNotJsSafe,
}

impl LogicalSegmentIdentity {
    pub fn try_new(
        session_id: &str,
        delivery_generation: u64,
        segment_sequence: u64,
        byte_length: u64,
        sha256_hex: impl AsRef<str>,
    ) -> Result<Self, LogicalSegmentIdentityError> {
        if delivery_generation == 0 {
            return Err(LogicalSegmentIdentityError::StaleGeneration);
        }
        if segment_sequence > JS_MAX_SAFE_INTEGER {
            return Err(LogicalSegmentIdentityError::SegmentSequenceNotJsSafe);
        }
        if byte_length > JS_MAX_SAFE_INTEGER {
            return Err(LogicalSegmentIdentityError::ByteLengthNotJsSafe);
        }
        let sha256_hex = match validate_sha256_hex(sha256_hex.as_ref(), "sha256") {
            Ok(s) => s,
            Err(FinalizedManifestError::Invalid(_)) => {
                return Err(LogicalSegmentIdentityError::MalformedSha256);
            }
        };
        Ok(Self {
            session_id: session_id.to_string(),
            delivery_generation,
            segment_sequence,
            byte_length,
            sha256_hex,
        })
    }

    pub fn validate_against_binding_generation(
        identity: &Self,
        binding_delivery_generation: u64,
    ) -> Result<(), LogicalSegmentIdentityError> {
        if identity.delivery_generation != binding_delivery_generation {
            return Err(LogicalSegmentIdentityError::GenerationMismatch {
                identity_generation: identity.delivery_generation,
                binding_generation: binding_delivery_generation,
            });
        }
        Ok(())
    }
}
