//! Deterministic `Syndicate Audio Recovery Report.txt` generation.

use std::fmt::Write as _;

#[derive(Debug, Clone)]
pub struct SilenceInterval {
    pub start_sample: u64,
    pub end_sample: u64,
    pub packet_count: u64,
    pub reason_code: String,
}

#[derive(Debug, Default)]
pub struct RecoveryReportBuilder {
    pub discarded_packet_count: u64,
    intervals: Vec<SilenceInterval>,
}

impl RecoveryReportBuilder {
    pub fn record_discarded_packet(&mut self) {
        self.discarded_packet_count += 1;
    }

    pub fn record_silence_interval(&mut self, interval: SilenceInterval) {
        self.intervals.push(interval);
    }

    pub fn silence_range_count(&self) -> u64 {
        self.intervals.len() as u64
    }

    pub fn intervals_snapshot(&self) -> Vec<SilenceInterval> {
        self.intervals.clone()
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
            "Syndicate Audio Recovery Report\nSession: {}\nGuild: {}\nChannel: {}\nTarget samples (48 kHz): {}\nDiscarded packets: {}\nSilence ranges: {}\n",
            session_id,
            guild_id,
            channel_id,
            target_samples,
            self.discarded_packet_count,
            self.intervals.len()
        );
        let mut intervals = self.intervals.clone();
        intervals.sort_by(|a, b| a.start_sample.cmp(&b.start_sample));
        for interval in &intervals {
            let start = format_hms_ms(interval.start_sample);
            let end = format_hms_ms(interval.end_sample);
            let dur_ms = ((interval.end_sample - interval.start_sample) * 1000) / 48_000;
            let _ = writeln!(
                out,
                "SILENCE {} - {} ({} ms) packets={} reason={}",
                start, end, dur_ms, interval.packet_count, interval.reason_code
            );
        }
        out
    }
}

fn format_hms_ms(sample: u64) -> String {
    let total_ms = (sample * 1000) / 48_000;
    let ms = total_ms % 1000;
    let total_s = total_ms / 1000;
    let s = total_s % 60;
    let m = (total_s / 60) % 60;
    let h = total_s / 3600;
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}
