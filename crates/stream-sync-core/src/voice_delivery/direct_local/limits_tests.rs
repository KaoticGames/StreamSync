//! `DirectReceiverLimits` validation (nonzero bounds + preflight overflow).

use super::test_support::{minimal_synj_segment_bytes, sha256_lower_hex, SESSION_ID};
use crate::voice_delivery::direct_local::{
    DirectReceiverLimits, DirectReceiverLimitsError, DirectReceiverPreflight,
};
use std::time::Duration;

fn valid_limits() -> DirectReceiverLimits {
    DirectReceiverLimits {
        max_segment_bytes: 8 * 1024 * 1024,
        max_in_memory_queued_bytes: 64 * 1024 * 1024,
        max_unacked_bytes: 128 * 1024 * 1024,
        max_unacked_segments: 16,
        min_free_space_reserve_bytes: 512 * 1024 * 1024,
        ack_deadline: Duration::from_secs(30),
        max_direct_session_duration: Duration::from_secs(6 * 3600),
    }
}

#[test]
fn rejects_zero_max_segment_bytes() {
    let limits = DirectReceiverLimits {
        max_segment_bytes: 0,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "max_segment_bytes",
        })
    );
}

#[test]
fn rejects_zero_max_in_memory_queued_bytes() {
    let limits = DirectReceiverLimits {
        max_in_memory_queued_bytes: 0,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "max_in_memory_queued_bytes",
        })
    );
}

#[test]
fn rejects_zero_max_unacked_bytes() {
    let limits = DirectReceiverLimits {
        max_unacked_bytes: 0,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "max_unacked_bytes",
        })
    );
}

#[test]
fn rejects_zero_max_unacked_segments() {
    let limits = DirectReceiverLimits {
        max_unacked_segments: 0,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "max_unacked_segments",
        })
    );
}

#[test]
fn rejects_zero_min_free_space_reserve_bytes() {
    let limits = DirectReceiverLimits {
        min_free_space_reserve_bytes: 0,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "min_free_space_reserve_bytes",
        })
    );
}

#[test]
fn rejects_zero_ack_deadline() {
    let limits = DirectReceiverLimits {
        ack_deadline: Duration::ZERO,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "ack_deadline",
        })
    );
}

#[test]
fn rejects_zero_max_direct_session_duration() {
    let limits = DirectReceiverLimits {
        max_direct_session_duration: Duration::ZERO,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::ZeroBound {
            field: "max_direct_session_duration",
        })
    );
}

#[test]
fn rejects_max_segment_bytes_above_in_memory_queued() {
    let limits = DirectReceiverLimits {
        max_segment_bytes: 64 * 1024 * 1024 + 1,
        max_in_memory_queued_bytes: 64 * 1024 * 1024,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::InconsistentBound {
            field: "max_segment_bytes",
        })
    );
}

#[test]
fn rejects_max_segment_bytes_above_max_unacked_bytes() {
    let limits = DirectReceiverLimits {
        max_segment_bytes: 128 * 1024 * 1024 + 1,
        max_unacked_bytes: 128 * 1024 * 1024,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::InconsistentBound {
            field: "max_segment_bytes",
        })
    );
}

#[test]
fn rejects_unacked_segment_count_times_max_segment_overflow() {
    let limits = DirectReceiverLimits {
        max_unacked_segments: u64::MAX,
        max_segment_bytes: 2,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::InconsistentBound {
            field: "max_unacked_segments",
        })
    );
}

#[test]
fn rejects_unacked_segment_payload_exceeding_max_unacked_bytes() {
    let limits = DirectReceiverLimits {
        max_unacked_segments: 256,
        max_segment_bytes: 8 * 1024 * 1024,
        max_unacked_bytes: 128 * 1024 * 1024 - 1,
        ..valid_limits()
    };
    assert_eq!(
        DirectReceiverLimits::validate(&limits),
        Err(DirectReceiverLimitsError::InconsistentBound {
            field: "max_unacked_bytes",
        })
    );
}

#[test]
fn preflight_rejects_segment_byte_length_above_max() {
    let limits = valid_limits();
    DirectReceiverLimits::validate(&limits).expect("structural bounds ok");
    let err = DirectReceiverPreflight::for_session_open(
        &limits,
        SESSION_ID,
        1,
        limits.max_segment_bytes + 1,
        &sha256_lower_hex(&minimal_synj_segment_bytes()),
    )
    .expect_err("segment too large");
    assert_eq!(err, DirectReceiverLimitsError::PreflightOverflow);
}
