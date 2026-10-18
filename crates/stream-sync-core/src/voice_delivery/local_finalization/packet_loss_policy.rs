//! Bounded audited local packet-loss policy (production offline / one-pass processor).

use std::collections::HashMap;

pub const LOCAL_PACKET_LOSS_POLICY_VERSION: &str = "bounded-audited-v1";
pub const LOCAL_PACKET_LOSS_MAX_SKIPPED: u64 = 32;
const RATIO_SCALE: u128 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketLossSource {
    FramingOrLostPlacement,
    DecodeFailure,
    PlacementOverlap,
    OutOfWindow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundedPacketLossError {
    MaxSkippedExceeded {
        skipped: u64,
        max: u64,
    },
    RatioExceeded {
        skipped: u64,
        total_records: u64,
    },
    ConsecutiveLoss {
        path_nick: String,
        discord_user_id: String,
        effective_epoch: u32,
        sequence: u16,
        extended_sequence: i64,
        source: PacketLossSource,
    },
}

impl std::fmt::Display for BoundedPacketLossError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxSkippedExceeded { skipped, max } => {
                write!(
                    f,
                    "packet-loss policy: skipped count {skipped} exceeds session maximum {max}"
                )
            }
            Self::RatioExceeded {
                skipped,
                total_records,
            } => {
                write!(
                    f,
                    "packet-loss policy: skipped count {skipped} exceeds 0.01% of {total_records} journal records"
                )
            }
            Self::ConsecutiveLoss {
                path_nick,
                discord_user_id,
                effective_epoch,
                sequence,
                extended_sequence,
                source,
            } => {
                write!(
                    f,
                    "packet-loss policy: consecutive loss on stream pathNick={path_nick} user={discord_user_id} effectiveEpoch={effective_epoch} seq={sequence} extendedSeq={extended_sequence} source={source:?}"
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PacketLossSourceCounts {
    pub framing_or_lost_placement: u64,
    pub decode_failure: u64,
    pub placement_overlap: u64,
    pub out_of_window: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPacketLossPolicySummary {
    pub version: String,
    pub total_journal_records: u64,
    pub max_skipped: u64,
    pub skipped_count: u64,
    pub source_counts: PacketLossSourceCounts,
    pub consecutive_loss_detected: bool,
    pub within_policy: bool,
}

#[derive(Default)]
struct StreamLossCursor {
    last_loss_extended: Option<i64>,
}

#[derive(Default)]
pub struct BoundedPacketLossTracker {
    path_nick_by_user: HashMap<String, String>,
    total_journal_records: u64,
    skipped_count: u64,
    source_counts: PacketLossSourceCounts,
    stream_cursors: HashMap<(String, u32), StreamLossCursor>,
    consecutive_loss_detected: bool,
    finished: bool,
}

impl BoundedPacketLossTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_user(&mut self, discord_user_id: &str, path_nick: &str) {
        self.path_nick_by_user
            .insert(discord_user_id.to_string(), path_nick.to_string());
    }

    pub fn note_known_user_journal_record(&mut self) {
        self.total_journal_records += 1;
    }

    pub fn register_media_loss(
        &mut self,
        discord_user_id: &str,
        effective_epoch: u32,
        sequence: u16,
        extended_sequence: i64,
        source: PacketLossSource,
    ) -> Result<(), BoundedPacketLossError> {
        if self.finished {
            return Ok(());
        }
        let path_nick = self
            .path_nick_by_user
            .get(discord_user_id)
            .cloned()
            .unwrap_or_else(|| "?".into());

        let stream_key = (discord_user_id.to_string(), effective_epoch);
        let cursor = self.stream_cursors.entry(stream_key).or_default();
        if let Some(prev) = cursor.last_loss_extended {
            if prev.checked_add(1) == Some(extended_sequence) {
                self.consecutive_loss_detected = true;
                return Err(BoundedPacketLossError::ConsecutiveLoss {
                    path_nick,
                    discord_user_id: discord_user_id.to_string(),
                    effective_epoch,
                    sequence,
                    extended_sequence,
                    source,
                });
            }
        }
        cursor.last_loss_extended = Some(extended_sequence);

        if self.skipped_count >= LOCAL_PACKET_LOSS_MAX_SKIPPED {
            return Err(BoundedPacketLossError::MaxSkippedExceeded {
                skipped: self.skipped_count + 1,
                max: LOCAL_PACKET_LOSS_MAX_SKIPPED,
            });
        }

        self.skipped_count += 1;
        match source {
            PacketLossSource::FramingOrLostPlacement => {
                self.source_counts.framing_or_lost_placement += 1;
            }
            PacketLossSource::DecodeFailure => self.source_counts.decode_failure += 1,
            PacketLossSource::PlacementOverlap => self.source_counts.placement_overlap += 1,
            PacketLossSource::OutOfWindow => self.source_counts.out_of_window += 1,
        }
        Ok(())
    }

    pub fn finish(&mut self) -> Result<LocalPacketLossPolicySummary, BoundedPacketLossError> {
        self.finished = true;
        let within_ratio = ratio_within_policy(self.skipped_count, self.total_journal_records);
        if !within_ratio {
            return Err(BoundedPacketLossError::RatioExceeded {
                skipped: self.skipped_count,
                total_records: self.total_journal_records,
            });
        }
        Ok(self.summary(true))
    }

    pub fn summary(&self, within_policy: bool) -> LocalPacketLossPolicySummary {
        LocalPacketLossPolicySummary {
            version: LOCAL_PACKET_LOSS_POLICY_VERSION.into(),
            total_journal_records: self.total_journal_records,
            max_skipped: LOCAL_PACKET_LOSS_MAX_SKIPPED,
            skipped_count: self.skipped_count,
            source_counts: self.source_counts,
            consecutive_loss_detected: self.consecutive_loss_detected,
            within_policy,
        }
    }

    pub fn skipped_count(&self) -> u64 {
        self.skipped_count
    }

    pub fn total_journal_records(&self) -> u64 {
        self.total_journal_records
    }
}

pub fn ratio_within_policy(skipped: u64, total_records: u64) -> bool {
    let lhs = u128::from(skipped) * RATIO_SCALE;
    let rhs = u128::from(total_records);
    lhs <= rhs
}

#[cfg(test)]
mod tests {
    use super::*;

    const USER: &str = "123456789012345678";

    fn tracker_with_user() -> BoundedPacketLossTracker {
        let mut t = BoundedPacketLossTracker::new();
        t.register_user(USER, "alice");
        t
    }

    fn register(
        t: &mut BoundedPacketLossTracker,
        epoch: u32,
        seq: u16,
        ext: i64,
        source: PacketLossSource,
    ) {
        t.register_media_loss(USER, epoch, seq, ext, source)
            .expect("register");
    }

    #[test]
    fn ratio_allows_one_in_ten_thousand() {
        let mut t = tracker_with_user();
        for _ in 0..10_000 {
            t.note_known_user_journal_record();
        }
        register(&mut t, 0, 1, 1, PacketLossSource::PlacementOverlap);
        assert!(t.finish().unwrap().within_policy);
    }

    #[test]
    fn ratio_violation_refused_at_finish() {
        let mut t = tracker_with_user();
        for _ in 0..9_999 {
            t.note_known_user_journal_record();
        }
        register(&mut t, 0, 1, 1, PacketLossSource::DecodeFailure);
        register(&mut t, 0, 3, 3, PacketLossSource::FramingOrLostPlacement);
        let err = t.finish().unwrap_err();
        assert!(matches!(err, BoundedPacketLossError::RatioExceeded { .. }));
    }

    #[test]
    fn thirty_second_allowed_thirty_third_refused() {
        let mut t = tracker_with_user();
        for i in 0..100_000u64 {
            t.note_known_user_journal_record();
            if i >= 32 {
                break;
            }
        }
        for ext in 1..=32 {
            register(
                &mut t,
                0,
                ext as u16,
                ext as i64 * 2,
                PacketLossSource::PlacementOverlap,
            );
        }
        let err = t
            .register_media_loss(USER, 0, 99, 99, PacketLossSource::PlacementOverlap)
            .unwrap_err();
        assert!(matches!(
            err,
            BoundedPacketLossError::MaxSkippedExceeded { max: 32, .. }
        ));
    }

    #[test]
    fn consecutive_loss_i64_max_prior_does_not_wrap_to_adjacent() {
        let mut t = tracker_with_user();
        t.note_known_user_journal_record();
        t.register_media_loss(USER, 0, 1, i64::MAX, PacketLossSource::DecodeFailure)
            .expect("first loss at MAX");
        t.register_media_loss(USER, 0, 2, i64::MIN, PacketLossSource::PlacementOverlap)
            .expect("MIN must not look consecutive to MAX");
        assert_eq!(t.skipped_count(), 2);
        assert!(!t.summary(true).consecutive_loss_detected);
    }

    #[test]
    fn consecutive_same_stream_refused() {
        let mut t = tracker_with_user();
        t.note_known_user_journal_record();
        register(&mut t, 0, 10, 10, PacketLossSource::DecodeFailure);
        let err = t
            .register_media_loss(USER, 0, 11, 11, PacketLossSource::PlacementOverlap)
            .unwrap_err();
        assert!(matches!(
            err,
            BoundedPacketLossError::ConsecutiveLoss { .. }
        ));
    }

    #[test]
    fn nonadjacent_and_different_users_allowed_within_budget() {
        let mut t = tracker_with_user();
        t.register_user("234567890123456789", "bob");
        for _ in 0..30_000 {
            t.note_known_user_journal_record();
        }
        register(&mut t, 0, 1, 1, PacketLossSource::OutOfWindow);
        register(&mut t, 0, 5, 5, PacketLossSource::FramingOrLostPlacement);
        t.register_media_loss(
            "234567890123456789",
            0,
            2,
            2,
            PacketLossSource::DecodeFailure,
        )
        .expect("other user");
        assert_eq!(t.skipped_count(), 3);
        assert!(t.finish().unwrap().within_policy);
    }

    #[test]
    fn source_counts_tracked() {
        let mut t = tracker_with_user();
        t.note_known_user_journal_record();
        register(&mut t, 0, 1, 1, PacketLossSource::PlacementOverlap);
        register(&mut t, 0, 5, 5, PacketLossSource::DecodeFailure);
        let summary = t.summary(true);
        assert_eq!(summary.source_counts.placement_overlap, 1);
        assert_eq!(summary.source_counts.decode_failure, 1);
    }
}
