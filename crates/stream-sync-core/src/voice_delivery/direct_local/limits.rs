//! `DirectReceiverLimits` validation (nonzero bounds + preflight overflow).

use std::time::Duration;
use thiserror::Error;

pub const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectReceiverLimits {
    pub max_segment_bytes: u64,
    pub max_in_memory_queued_bytes: u64,
    pub max_unacked_bytes: u64,
    pub max_unacked_segments: u64,
    pub min_free_space_reserve_bytes: u64,
    pub ack_deadline: Duration,
    pub max_direct_session_duration: Duration,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectReceiverLimitsError {
    #[error("zero bound: {field}")]
    ZeroBound { field: &'static str },
    #[error("inconsistent bound: {field}")]
    InconsistentBound { field: &'static str },
    #[error("preflight arithmetic overflow")]
    PreflightOverflow,
}

pub struct DirectReceiverPreflight;

impl DirectReceiverLimits {
    pub fn validate(limits: &DirectReceiverLimits) -> Result<(), DirectReceiverLimitsError> {
        if limits.max_segment_bytes == 0 {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "max_segment_bytes",
            });
        }
        if limits.max_in_memory_queued_bytes == 0 {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "max_in_memory_queued_bytes",
            });
        }
        if limits.max_unacked_bytes == 0 {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "max_unacked_bytes",
            });
        }
        if limits.max_unacked_segments == 0 {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "max_unacked_segments",
            });
        }
        if limits.min_free_space_reserve_bytes == 0 {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "min_free_space_reserve_bytes",
            });
        }
        if limits.ack_deadline.is_zero() {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "ack_deadline",
            });
        }
        if limits.max_direct_session_duration.is_zero() {
            return Err(DirectReceiverLimitsError::ZeroBound {
                field: "max_direct_session_duration",
            });
        }
        if limits.max_segment_bytes > limits.max_in_memory_queued_bytes {
            return Err(DirectReceiverLimitsError::InconsistentBound {
                field: "max_segment_bytes",
            });
        }
        if limits.max_segment_bytes > limits.max_unacked_bytes {
            return Err(DirectReceiverLimitsError::InconsistentBound {
                field: "max_segment_bytes",
            });
        }
        let max_unacked_payload = limits
            .max_unacked_segments
            .checked_mul(limits.max_segment_bytes)
            .ok_or(DirectReceiverLimitsError::InconsistentBound {
                field: "max_unacked_segments",
            })?;
        if max_unacked_payload > limits.max_unacked_bytes {
            return Err(DirectReceiverLimitsError::InconsistentBound {
                field: "max_unacked_bytes",
            });
        }
        Ok(())
    }
}

impl DirectReceiverPreflight {
    pub fn for_session_open(
        limits: &DirectReceiverLimits,
        _session_id: &str,
        _delivery_generation: u64,
        segment_byte_length: u64,
        _segment_sha256_hex: &str,
    ) -> Result<(), DirectReceiverLimitsError> {
        if segment_byte_length > limits.max_segment_bytes {
            return Err(DirectReceiverLimitsError::PreflightOverflow);
        }
        let _ = limits
            .max_unacked_segments
            .checked_mul(limits.max_segment_bytes)
            .ok_or(DirectReceiverLimitsError::PreflightOverflow)?;
        let _ = limits
            .max_unacked_segments
            .checked_mul(segment_byte_length)
            .ok_or(DirectReceiverLimitsError::PreflightOverflow)?;
        Ok(())
    }
}
