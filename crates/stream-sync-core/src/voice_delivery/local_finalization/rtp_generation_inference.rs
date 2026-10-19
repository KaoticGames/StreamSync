//! RTP timestamp discontinuity / implicit generation inference (ChatBot `rtpGenerationInference.ts`).

pub const REORDER_WINDOW_PACKETS: u16 = 32;
pub const OPUS_FRAME_SAMPLES: u64 = 960;
const RTP_CLOCK_HZ: u64 = 48_000;
const UINT32_MOD: u64 = 1u64 << 32;
pub const MAX_RTP_GAP_SAMPLES: u64 = OPUS_FRAME_SAMPLES * 30_000;
pub const CAPTURE_ARRIVAL_TOLERANCE_SAMPLES: u64 = OPUS_FRAME_SAMPLES * 8;
pub const RTP_CAPTURE_GAP_TOLERANCE_SAMPLES: u64 = OPUS_FRAME_SAMPLES * 4;

#[derive(Debug, Clone)]
pub struct RtpPlacementContext {
    pub prior_raw_sequence: u16,
    pub prior_extended_sequence: i64,
    pub prior_raw_rtp_timestamp: u32,
    pub prior_capture_offset_samples: u64,
    pub prior_arrived_at_ns: u64,
    pub prior_timeline_sample_end: u64,
}

#[derive(Debug, Clone)]
pub struct RtpCandidatePacket {
    pub raw_sequence: u16,
    pub extended_sequence: i64,
    pub rtp_timestamp: u32,
    pub capture_offset_samples: u64,
    pub arrived_at_ns: u64,
}

#[derive(Debug, Clone)]
pub struct InferredRtpGenerationBoundary {
    pub _journal_ssrc_epoch: u32,
    pub _implicit_generation_ordinal: u32,
    pub _prior_effective_epoch: u32,
    pub _new_effective_epoch: u32,
    pub _sequence: u16,
    pub _capture_offset_samples: u64,
}

#[derive(Debug)]
pub enum RtpDiscontinuityDecision {
    Gap { rtp_delta_samples: u64 },
    ImplicitGeneration,
    Refuse { reason: String },
}

pub enum RtpDiscontinuityAction {
    Normal { sample_start: u64 },
    Gap { sample_start: u64 },
    ImplicitBoundary,
    Refuse { reason: String },
}

pub fn rtp_forward_delta(prev_raw: u32, raw_timestamp: u32) -> u64 {
    let prev = u64::from(prev_raw);
    let raw = u64::from(raw_timestamp);
    if raw >= prev {
        raw - prev
    } else {
        raw + UINT32_MOD - prev
    }
}

fn seq16(n: u16) -> u16 {
    n
}

pub fn arrival_ns_to_capture_samples(delta_ns: u64) -> u64 {
    if delta_ns == 0 {
        return 0;
    }
    let product = u128::from(delta_ns) * u128::from(RTP_CLOCK_HZ);
    (product / 1_000_000_000) as u64
}

pub fn capture_and_arrival_coherent(capture_delta_samples: u64, arrival_delta_ns: u64) -> bool {
    if capture_delta_samples == 0 || arrival_delta_ns == 0 {
        return false;
    }
    let from_arrival = arrival_ns_to_capture_samples(arrival_delta_ns);
    if from_arrival == 0 && capture_delta_samples >= OPUS_FRAME_SAMPLES {
        return false;
    }
    let slip = capture_delta_samples.abs_diff(from_arrival);
    slip <= CAPTURE_ARRIVAL_TOLERANCE_SAMPLES
}

pub fn rtp_delta_matches_capture_gap(rtp_delta_samples: u64, capture_delta_samples: u64) -> bool {
    if capture_delta_samples == 0 {
        return false;
    }
    let slip = rtp_delta_samples.abs_diff(capture_delta_samples);
    slip <= RTP_CAPTURE_GAP_TOLERANCE_SAMPLES
}

pub fn is_likely_rtp_timestamp_wrap_forward(
    prior_raw_rtp: u32,
    current_raw_rtp: u32,
    rtp_delta_samples: u64,
) -> bool {
    let prev = u64::from(prior_raw_rtp);
    let raw = u64::from(current_raw_rtp);
    if raw >= prev {
        return false;
    }
    if rtp_delta_samples > MAX_RTP_GAP_SAMPLES {
        return false;
    }
    let backward = prev - raw;
    backward > 0xffff_0000 && rtp_delta_samples < OPUS_FRAME_SAMPLES * 64
}

pub fn has_sequence_discontinuity_evidence(
    prior_raw_sequence: u16,
    current_raw_sequence: u16,
    prior_extended_sequence: i64,
    current_extended_sequence: i64,
) -> bool {
    let prior_raw = seq16(prior_raw_sequence);
    let raw = seq16(current_raw_sequence);
    if raw == prior_raw {
        return false;
    }
    let ahead = raw.wrapping_sub(prior_raw);
    if ahead != 0 && ahead < 0x8000 && ahead > REORDER_WINDOW_PACKETS {
        return true;
    }
    let behind = prior_raw.wrapping_sub(raw);
    if behind != 0 && behind < 0x8000 && behind > REORDER_WINDOW_PACKETS {
        return true;
    }
    if ahead != 0 && ahead < 0x8000 && ahead <= REORDER_WINDOW_PACKETS {
        return false;
    }
    let window = i64::from(REORDER_WINDOW_PACKETS);
    let extended_delta = match current_extended_sequence.checked_sub(prior_extended_sequence) {
        Some(d) => d,
        None => return true,
    };
    if extended_delta > window {
        return true;
    }
    if extended_delta < -window {
        return true;
    }
    false
}

pub fn decide_rtp_timestamp_discontinuity(
    ctx: &RtpPlacementContext,
    pkt: &RtpCandidatePacket,
) -> RtpDiscontinuityDecision {
    let rtp_delta = rtp_forward_delta(ctx.prior_raw_rtp_timestamp, pkt.rtp_timestamp);
    let capture_delta = match pkt
        .capture_offset_samples
        .checked_sub(ctx.prior_capture_offset_samples)
    {
        Some(d) => d,
        None => {
            return RtpDiscontinuityDecision::Refuse {
                reason: "capture offset regressed across discontinuity".into(),
            };
        }
    };
    let arrival_delta_ns = pkt.arrived_at_ns.saturating_sub(ctx.prior_arrived_at_ns);

    if is_likely_rtp_timestamp_wrap_forward(
        ctx.prior_raw_rtp_timestamp,
        pkt.rtp_timestamp,
        rtp_delta,
    ) {
        return RtpDiscontinuityDecision::Gap {
            rtp_delta_samples: rtp_delta,
        };
    }

    if rtp_delta > MAX_RTP_GAP_SAMPLES || rtp_delta == 0 {
        let use_capture_clocks =
            ctx.prior_capture_offset_samples > 0 || pkt.capture_offset_samples > 0;
        let clocks_ok = if use_capture_clocks {
            capture_delta > 0
                && arrival_delta_ns > 0
                && capture_and_arrival_coherent(capture_delta, arrival_delta_ns)
        } else {
            arrival_delta_ns > 0
        };

        if rtp_delta > MAX_RTP_GAP_SAMPLES
            && clocks_ok
            && rtp_delta_matches_capture_gap(rtp_delta, capture_delta)
        {
            return RtpDiscontinuityDecision::Gap {
                rtp_delta_samples: rtp_delta,
            };
        }

        let seq_evidence = has_sequence_discontinuity_evidence(
            ctx.prior_raw_sequence,
            pkt.raw_sequence,
            ctx.prior_extended_sequence,
            pkt.extended_sequence,
        );

        if !seq_evidence && rtp_delta == 0 {
            return RtpDiscontinuityDecision::Refuse {
                reason: "rtp timestamp unchanged without sequence discontinuity corroboration"
                    .into(),
            };
        }

        if !seq_evidence && rtp_delta > MAX_RTP_GAP_SAMPLES {
            return RtpDiscontinuityDecision::Refuse {
                reason: "rtp timestamp jump without sequence discontinuity corroboration".into(),
            };
        }

        if !clocks_ok {
            return RtpDiscontinuityDecision::Refuse {
                reason: "incoherent capture offset and arrival time across discontinuity".into(),
            };
        }

        let anchor = pkt
            .capture_offset_samples
            .max(ctx.prior_timeline_sample_end);
        if anchor < ctx.prior_timeline_sample_end {
            return RtpDiscontinuityDecision::Refuse {
                reason: "inferred generation anchor would overlap prior timeline".into(),
            };
        }

        if rtp_delta_matches_capture_gap(rtp_delta, capture_delta) {
            return RtpDiscontinuityDecision::Gap {
                rtp_delta_samples: rtp_delta,
            };
        }

        return RtpDiscontinuityDecision::ImplicitGeneration;
    }

    RtpDiscontinuityDecision::Gap {
        rtp_delta_samples: rtp_delta,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_regression_refused_before_unsigned_wrap() {
        let ctx = RtpPlacementContext {
            prior_raw_sequence: 10,
            prior_extended_sequence: 10,
            prior_raw_rtp_timestamp: 0,
            prior_capture_offset_samples: 10_000,
            prior_arrived_at_ns: 1_000_000,
            prior_timeline_sample_end: 10_000,
        };
        let pkt = RtpCandidatePacket {
            raw_sequence: 11,
            extended_sequence: 11,
            rtp_timestamp: 0,
            capture_offset_samples: 5_000,
            arrived_at_ns: 2_000_000,
        };
        let decision = decide_rtp_timestamp_discontinuity(&ctx, &pkt);
        assert!(
            matches!(decision, RtpDiscontinuityDecision::Refuse { .. }),
            "expected refuse on capture regression, got {:?}",
            decision
        );
    }

    #[test]
    fn extended_sequence_ahead_gap_is_discontinuity_evidence() {
        assert!(has_sequence_discontinuity_evidence(0, 40, 0, 40));
    }

    #[test]
    fn extended_sequence_behind_gap_is_discontinuity_evidence() {
        assert!(has_sequence_discontinuity_evidence(100, 50, 100, 50));
    }

    #[test]
    fn extended_sequence_negative_within_window_is_not_extended_discontinuity() {
        assert!(!has_sequence_discontinuity_evidence(0, 65535, 0, -1));
    }

    #[test]
    fn huge_capture_delta_not_falsely_coherent_via_i64_wrap() {
        assert!(!capture_and_arrival_coherent(u64::MAX, 20_000_000));
        let huge = i64::MAX as u64 + 50_000;
        assert!(!capture_and_arrival_coherent(huge, 20_000_000));
    }

    #[test]
    fn huge_rtp_capture_gap_not_falsely_matched_via_i64_wrap() {
        assert!(!rtp_delta_matches_capture_gap(u64::MAX, 960));
        let huge = i64::MAX as u64 + 960;
        assert!(!rtp_delta_matches_capture_gap(huge, 960));
    }
}
