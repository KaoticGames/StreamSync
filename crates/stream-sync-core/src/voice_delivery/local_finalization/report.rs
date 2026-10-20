//! Deterministic `Syndicate Audio Recovery Report.txt` generation (format v2).

use std::fmt::Write as _;

pub const REPORT_FORMAT_VERSION: &str = "v2";

const SAMPLE_RATE: u64 = 48_000;

/// Internal silence reason while finalizing a stem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StemSilenceReason {
    TimelineGap,
    TailPadding,
    JournalLossMarker,
    OpusDecodeFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscardReason {
    Duplicate,
    Conflict,
    OutOfWindow,
    Overlap,
    Other,
}

#[derive(Debug, Clone)]
struct RecoveryIncident {
    start_sample: u64,
    end_sample: u64,
    bad_packet_count: u64,
    reasons: Vec<IncidentReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IncidentReason {
    JournalLossMarker,
    OpusDecodeFailed,
}

/// Per-stem recovery audit (bounded: normal gaps aggregated only).
#[derive(Debug, Default, Clone)]
pub struct StemRecoveryReportBuilder {
    pub path_nick: String,
    pub discord_user_id: String,
    normal_timeline_silence_samples: u64,
    incidents: Vec<RecoveryIncident>,
    discarded_duplicate: u64,
    discarded_conflict: u64,
    discarded_out_of_window: u64,
    discarded_overlap: u64,
    discarded_other: u64,
}

impl StemRecoveryReportBuilder {
    pub fn new(path_nick: String, discord_user_id: String) -> Self {
        Self {
            path_nick,
            discord_user_id,
            ..Self::default()
        }
    }

    pub fn record_discarded(&mut self, reason: DiscardReason) {
        match reason {
            DiscardReason::Duplicate => self.discarded_duplicate += 1,
            DiscardReason::Conflict => self.discarded_conflict += 1,
            DiscardReason::OutOfWindow => self.discarded_out_of_window += 1,
            DiscardReason::Overlap => self.discarded_overlap += 1,
            DiscardReason::Other => self.discarded_other += 1,
        }
    }

    pub fn record_silence_span(
        &mut self,
        start_sample: u64,
        end_sample: u64,
        reason: StemSilenceReason,
        bad_packet_count: u64,
    ) {
        if end_sample <= start_sample {
            return;
        }
        let span = end_sample - start_sample;
        match reason {
            StemSilenceReason::TimelineGap | StemSilenceReason::TailPadding => {
                self.normal_timeline_silence_samples += span;
            }
            StemSilenceReason::JournalLossMarker => {
                if bad_packet_count == 0 {
                    if let Some(last) = self.incidents.last_mut() {
                        if last.end_sample == start_sample {
                            last.end_sample = end_sample;
                        }
                    }
                    return;
                }
                self.push_incident(
                    start_sample,
                    end_sample,
                    bad_packet_count,
                    IncidentReason::JournalLossMarker,
                );
            }
            StemSilenceReason::OpusDecodeFailed => {
                if bad_packet_count == 0 {
                    if let Some(last) = self.incidents.last_mut() {
                        if last.end_sample == start_sample {
                            last.end_sample = end_sample;
                        }
                    }
                    return;
                }
                self.push_incident(
                    start_sample,
                    end_sample,
                    bad_packet_count,
                    IncidentReason::OpusDecodeFailed,
                );
            }
        }
    }

    fn push_incident(
        &mut self,
        start_sample: u64,
        end_sample: u64,
        bad_packet_count: u64,
        reason: IncidentReason,
    ) {
        if let Some(last) = self.incidents.last_mut() {
            if last.end_sample == start_sample {
                last.end_sample = end_sample;
                last.bad_packet_count += bad_packet_count;
                if !last.reasons.contains(&reason) {
                    last.reasons.push(reason);
                    last.reasons.sort();
                }
                return;
            }
        }
        self.incidents.push(RecoveryIncident {
            start_sample,
            end_sample,
            bad_packet_count,
            reasons: vec![reason],
        });
    }

    pub fn recovery_incident_count(&self) -> u64 {
        self.incidents.len() as u64
    }

    pub fn bad_packet_count(&self) -> u64 {
        self.incidents.iter().map(|i| i.bad_packet_count).sum()
    }

    pub fn inserted_recovery_silence_samples(&self) -> u64 {
        self.incidents
            .iter()
            .map(|i| i.end_sample - i.start_sample)
            .sum()
    }

    pub fn normal_timeline_silence_samples(&self) -> u64 {
        self.normal_timeline_silence_samples
    }

    pub fn discarded_duplicate(&self) -> u64 {
        self.discarded_duplicate
    }

    pub fn discarded_conflict(&self) -> u64 {
        self.discarded_conflict
    }

    pub fn discarded_out_of_window(&self) -> u64 {
        self.discarded_out_of_window
    }

    pub fn discarded_other(&self) -> u64 {
        self.discarded_other
    }

    pub fn discarded_overlap(&self) -> u64 {
        self.discarded_overlap
    }

    fn render_stem_section(&self, out: &mut String) {
        let file_name = format!("{}.wav", self.path_nick);
        let normal_ms = samples_to_ms_f64(self.normal_timeline_silence_samples);
        let recovery_ms = samples_to_ms_f64(self.inserted_recovery_silence_samples());
        let _ = writeln!(
            out,
            "[stem file=\"{}\" userId={}]",
            file_name, self.discord_user_id
        );
        let _ = writeln!(
            out,
            "  stemSummary recoveryIncidentCount={} badPacketCount={} insertedRecoverySilenceMs={:.3} insertedRecoverySilenceSamples={} normalTimelineSilenceMs={:.3} normalTimelineSilenceSamples={}",
            self.recovery_incident_count(),
            self.bad_packet_count(),
            recovery_ms,
            self.inserted_recovery_silence_samples(),
            normal_ms,
            self.normal_timeline_silence_samples
        );
        let _ = writeln!(
            out,
            "  discarded duplicate={} conflict={} out_of_window={} overlap={} other={}",
            self.discarded_duplicate,
            self.discarded_conflict,
            self.discarded_out_of_window,
            self.discarded_overlap,
            self.discarded_other
        );
        let mut incidents = self.incidents.clone();
        incidents.sort_by_key(|a| a.start_sample);
        for incident in &incidents {
            let dur_samples = incident.end_sample - incident.start_sample;
            let start = format_hms_ms(incident.start_sample);
            let end = format_hms_ms(incident.end_sample);
            let dur_ms = samples_to_ms_f64(dur_samples);
            let reasons = incident_reason_codes(&incident.reasons);
            let category = incident_category_label(&incident.reasons);
            let _ = writeln!(
                out,
                "  INCIDENT {} - {} ({:.3} ms) samples={}-{} badPackets={} reasons={} category={}",
                start,
                end,
                dur_ms,
                incident.start_sample,
                incident.end_sample,
                incident.bad_packet_count,
                reasons,
                category
            );
        }
        let _ = writeln!(out);
    }
}

#[derive(Debug, Default)]
pub struct SessionRecoveryReportBuilder {
    stems: Vec<StemRecoveryReportBuilder>,
    session_discarded_other: u64,
    packet_loss_policy: Option<super::packet_loss_policy::LocalPacketLossPolicySummary>,
}

impl SessionRecoveryReportBuilder {
    pub fn record_session_discarded_other(&mut self) {
        self.session_discarded_other += 1;
    }

    pub fn push_stem(&mut self, stem: StemRecoveryReportBuilder) {
        self.stems.push(stem);
    }

    pub fn set_packet_loss_policy(
        &mut self,
        summary: super::packet_loss_policy::LocalPacketLossPolicySummary,
    ) {
        self.packet_loss_policy = Some(summary);
    }

    pub fn discarded_packet_count(&self) -> u64 {
        let stem = self.stems.iter().fold(0u64, |acc, s| {
            acc + s.discarded_duplicate()
                + s.discarded_conflict()
                + s.discarded_out_of_window()
                + s.discarded_overlap()
                + s.discarded_other()
        });
        stem + self.session_discarded_other
    }

    pub fn silence_range_count(&self) -> u64 {
        self.stems.iter().map(|s| s.recovery_incident_count()).sum()
    }

    pub fn render(
        &self,
        session_id: &str,
        guild_id: &str,
        channel_id: &str,
        target_samples: u64,
    ) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Syndicate Audio Recovery Report (format {})",
            REPORT_FORMAT_VERSION
        );
        let _ = writeln!(out, "Session: {}", session_id);
        let _ = writeln!(out, "Guild: {}", guild_id);
        let _ = writeln!(out, "Channel: {}", channel_id);
        let _ = writeln!(out, "Target samples (48 kHz): {}", target_samples);
        let _ = writeln!(out);
        let _ = writeln!(out, "Legend (recovery incidents only):");
        let _ = writeln!(
            out,
            "  journal_loss_marker — journal recorded an unavailable packet; deterministic silence at the authoritative timeline position (local journal has no cloud loss-reason code)."
        );
        let _ = writeln!(
            out,
            "  opus_decode_failed — payload was present but the Opus decoder rejected it; recovery silence inserted."
        );
        let _ = writeln!(
            out,
            "  discarded duplicate/conflict/out_of_window/overlap — packet dropped to preserve a deterministic per-stem timeline (counts per stem)."
        );
        let _ = writeln!(
            out,
            "  overlap — overlapping Opus placement was dropped because the timeline already had PCM for that interval; no recovery silence was inserted."
        );
        let _ = writeln!(
            out,
            "Normal timeline silence (timeline_gap, tail_padding) is expected alignment or no-audio (e.g. Discord DTX); it is not evidence of packet corruption."
        );
        let _ = writeln!(
            out,
            "Limitation: this report is derived from the local Opus journal only; exact upstream framing/loss audit is not available when the journal only carries an empty payload marker."
        );
        let _ = writeln!(out);

        let mut stems = self.stems.clone();
        stems.sort_by(|a, b| {
            a.path_nick
                .cmp(&b.path_nick)
                .then_with(|| a.discord_user_id.cmp(&b.discord_user_id))
        });

        let total_incidents = stems
            .iter()
            .map(|s| s.recovery_incident_count())
            .sum::<u64>();
        let total_bad = stems.iter().map(|s| s.bad_packet_count()).sum::<u64>();
        let total_recovery_samples = stems
            .iter()
            .map(|s| s.inserted_recovery_silence_samples())
            .sum::<u64>();
        let total_normal_samples = stems
            .iter()
            .map(|s| s.normal_timeline_silence_samples())
            .sum::<u64>();
        let affected = stems
            .iter()
            .filter(|s| s.recovery_incident_count() > 0)
            .count();

        let _ = writeln!(
            out,
            "Session summary recoveryIncidentCount={} badPacketCount={} insertedRecoverySilenceMs={:.3} insertedRecoverySilenceSamples={} normalTimelineSilenceMs={:.3} normalTimelineSilenceSamples={} discardedPackets={} affectedStemCount={} sessionDiscardedOther={}",
            total_incidents,
            total_bad,
            samples_to_ms_f64(total_recovery_samples),
            total_recovery_samples,
            samples_to_ms_f64(total_normal_samples),
            total_normal_samples,
            self.discarded_packet_count(),
            affected,
            self.session_discarded_other
        );
        let _ = writeln!(out);

        if let Some(policy) = &self.packet_loss_policy {
            let _ = writeln!(out, "Packet-loss policy (local bounded audited):");
            let _ = writeln!(
                out,
                "  version={} totalJournalRecords={} maxSkipped={} skippedCount={} framingOrPlacement={} decodeFailure={} placementOverlap={} outOfWindow={} consecutiveLoss={} withinPolicy={}",
                policy.version,
                policy.total_journal_records,
                policy.max_skipped,
                policy.skipped_count,
                policy.source_counts.framing_or_lost_placement,
                policy.source_counts.decode_failure,
                policy.source_counts.placement_overlap,
                policy.source_counts.out_of_window,
                policy.consecutive_loss_detected,
                policy.within_policy
            );
            let _ = writeln!(out);
        }

        for stem in &stems {
            stem.render_stem_section(&mut out);
        }
        out
    }
}

fn incident_reason_codes(reasons: &[IncidentReason]) -> String {
    reasons
        .iter()
        .map(|r| match r {
            IncidentReason::JournalLossMarker => "journal_loss_marker",
            IncidentReason::OpusDecodeFailed => "opus_decode_failed",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn incident_category_label(reasons: &[IncidentReason]) -> String {
    if reasons.len() == 1 {
        return match reasons[0] {
            IncidentReason::JournalLossMarker => {
                "Journal packet unavailable (recovery silence)".into()
            }
            IncidentReason::OpusDecodeFailed => {
                "Decoder rejected payload (recovery silence)".into()
            }
        };
    }
    "Combined recovery silence".into()
}

fn samples_to_ms_f64(samples: u64) -> f64 {
    (samples as f64) * 1000.0 / SAMPLE_RATE as f64
}

fn format_hms_ms(sample: u64) -> String {
    let total_ns = sample * 1_000_000_000 / SAMPLE_RATE;
    let sub_ms_ns = total_ns % 1_000_000;
    let total_ms = total_ns / 1_000_000;
    let ms = total_ms % 1000;
    let total_s = total_ms / 1000;
    let s = total_s % 60;
    let m = (total_s / 60) % 60;
    let h = total_s / 3600;
    if sub_ms_ns > 0 && ms == 0 && total_ms < 1000 {
        format!(
            "{:02}:{:02}:{:02}.{:03}+{:03}us",
            h,
            m,
            s,
            ms,
            sub_ms_ns / 1000
        )
    } else {
        format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_stems_distinct_sections_and_ordering() {
        let mut session = SessionRecoveryReportBuilder::default();
        let mut a = StemRecoveryReportBuilder::new("bob".into(), "222".into());
        a.record_silence_span(0, 480, StemSilenceReason::JournalLossMarker, 1);
        let mut b = StemRecoveryReportBuilder::new("alice".into(), "111".into());
        b.record_silence_span(0, 960, StemSilenceReason::OpusDecodeFailed, 1);
        session.push_stem(b);
        session.push_stem(a);
        let text = session.render("sess", "g", "c", 48_000);
        assert!(text.contains("[stem file=\"alice.wav\" userId=111]"));
        assert!(text.contains("[stem file=\"bob.wav\" userId=222]"));
        let alice_pos = text.find("alice.wav").unwrap();
        let bob_pos = text.find("bob.wav").unwrap();
        assert!(alice_pos < bob_pos);
    }

    #[test]
    fn timeline_gaps_aggregated_not_listed_as_incidents() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 4800, StemSilenceReason::TimelineGap, 0);
        stem.record_silence_span(4800, 9600, StemSilenceReason::TailPadding, 0);
        assert_eq!(stem.recovery_incident_count(), 0);
        assert_eq!(stem.normal_timeline_silence_samples(), 9600);
        let text = {
            let mut session = SessionRecoveryReportBuilder::default();
            session.push_stem(stem);
            session.render("s", "g", "c", 9600)
        };
        assert!(!text.contains("INCIDENT"));
        assert!(text.contains("normalTimelineSilenceSamples=9600"));
        assert!(text.contains("not evidence of packet corruption"));
    }

    #[test]
    fn loss_marker_and_decode_failure_wording() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "9".into());
        stem.record_silence_span(0, 960, StemSilenceReason::JournalLossMarker, 1);
        stem.record_silence_span(2000, 2960, StemSilenceReason::OpusDecodeFailed, 1);
        let text = {
            let mut session = SessionRecoveryReportBuilder::default();
            session.push_stem(stem);
            session.render("s", "g", "c", 48_000)
        };
        assert!(text.contains("journal_loss_marker"));
        assert!(text.contains("Journal packet unavailable"));
        assert!(text.contains("opus_decode_failed"));
        assert!(text.contains("Decoder rejected payload"));
    }

    #[test]
    fn sub_ms_duration_formatting() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 1, StemSilenceReason::JournalLossMarker, 1);
        let text = {
            let mut session = SessionRecoveryReportBuilder::default();
            session.push_stem(stem);
            session.render("s", "g", "c", 48_000)
        };
        assert!(text.contains("(0.021 ms)"));
        assert!(text.contains("samples=0-1"));
    }

    #[test]
    fn coalesces_adjacent_journal_loss_marker_spans() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 480, StemSilenceReason::JournalLossMarker, 1);
        stem.record_silence_span(480, 960, StemSilenceReason::JournalLossMarker, 1);
        assert_eq!(stem.recovery_incident_count(), 1);
        assert_eq!(stem.bad_packet_count(), 2);
        assert_eq!(stem.inserted_recovery_silence_samples(), 960);
    }

    #[test]
    fn session_summary_counts() {
        let mut session = SessionRecoveryReportBuilder::default();
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_discarded(DiscardReason::Duplicate);
        stem.record_silence_span(0, 960, StemSilenceReason::JournalLossMarker, 1);
        session.push_stem(stem);
        session.record_session_discarded_other();
        assert_eq!(session.silence_range_count(), 1);
        assert_eq!(session.discarded_packet_count(), 2);
        let text = session.render("s", "g", "c", 48_000);
        assert!(text.contains("recoveryIncidentCount=1"));
        assert!(text.contains("discardedPackets=2"));
        assert!(text.contains("sessionDiscardedOther=1"));
    }

    #[test]
    fn normal_gap_memory_bounded_by_aggregate_only() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        for i in 0..10_000u64 {
            let start = i * 960;
            stem.record_silence_span(start, start + 480, StemSilenceReason::TimelineGap, 0);
        }
        assert_eq!(stem.recovery_incident_count(), 0);
        assert_eq!(stem.normal_timeline_silence_samples(), 10_000 * 480);
    }

    #[test]
    fn long_gap_before_loss_marker_attributes_silence_to_normal_aggregate() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 4800, StemSilenceReason::TimelineGap, 0);
        stem.record_silence_span(4800, 5760, StemSilenceReason::JournalLossMarker, 1);
        assert_eq!(stem.recovery_incident_count(), 1);
        assert_eq!(stem.bad_packet_count(), 1);
        assert_eq!(stem.inserted_recovery_silence_samples(), 960);
        assert_eq!(stem.normal_timeline_silence_samples(), 4800);
        let text = {
            let mut session = SessionRecoveryReportBuilder::default();
            session.push_stem(stem);
            session.render("s", "g", "c", 9600)
        };
        assert!(text.contains("normalTimelineSilenceSamples=4800"));
        assert!(text.contains("badPacketCount=1"));
        assert!(text.contains("(20.000 ms)"));
        assert!(text.contains("samples=4800-5760"));
    }

    #[test]
    fn separate_loss_markers_with_timeline_gap_between() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 960, StemSilenceReason::JournalLossMarker, 1);
        stem.record_silence_span(960, 5760, StemSilenceReason::TimelineGap, 0);
        stem.record_silence_span(5760, 6720, StemSilenceReason::JournalLossMarker, 1);
        assert_eq!(stem.recovery_incident_count(), 2);
        assert_eq!(stem.bad_packet_count(), 2);
        assert_eq!(stem.inserted_recovery_silence_samples(), 1920);
        assert_eq!(stem.normal_timeline_silence_samples(), 4800);
    }

    #[test]
    fn zero_bad_count_continuation_does_not_inflate_packet_count() {
        let mut stem = StemRecoveryReportBuilder::new("u".into(), "1".into());
        stem.record_silence_span(0, 960, StemSilenceReason::JournalLossMarker, 1);
        stem.record_silence_span(960, 1920, StemSilenceReason::JournalLossMarker, 0);
        assert_eq!(stem.recovery_incident_count(), 1);
        assert_eq!(stem.bad_packet_count(), 1);
        assert_eq!(stem.inserted_recovery_silence_samples(), 1920);
    }
}
