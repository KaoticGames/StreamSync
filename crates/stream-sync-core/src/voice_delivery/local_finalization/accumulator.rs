//! Per-user journal packet accumulator (ChatBot `opusJournal/accumulator` semantics).

use super::synj::JournalRecord;

pub const REORDER_WINDOW: u16 = 32;
pub const RTP_TIMESTAMP_MOD: u64 = 1u64 << 32;
const OPUS_FRAME_SAMPLES: u64 = 960;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketDisposition {
    Decode,
    LostPlacement,
    DiscardedDuplicate,
    DiscardedConflict,
    DiscardedOutOfWindow,
}

#[derive(Debug, Clone)]
pub struct ReadyPacket {
    pub record: JournalRecord,
    pub disposition: PacketDisposition,
}

struct BufferedPacket {
    record: JournalRecord,
    extended_seq: u64,
}

#[derive(Default)]
pub struct UserPacketAccumulator {
    ssrc_epoch: u32,
    epoch_generation: u32,
    last_sequence: Option<u16>,
    extended_base: u64,
    frontier_extended: Option<u64>,
    buffer: Vec<BufferedPacket>,
    rtp_anchor: Option<u32>,
    capture_anchor_samples: u64,
    arrival_anchor_ns: u64,
}

impl UserPacketAccumulator {
    pub fn ingest(&mut self, record: JournalRecord) -> Vec<ReadyPacket> {
        let extended = self.map_extended_sequence(&record);
        if self.is_duplicate_or_stale(extended) {
            return vec![ReadyPacket {
                record,
                disposition: PacketDisposition::DiscardedDuplicate,
            }];
        }
        if self.buffer.iter().any(|p| {
            p.extended_seq == extended
                && (p.record.opus_payload != record.opus_payload
                    || p.record.rtp_timestamp != record.rtp_timestamp)
        }) {
            return vec![ReadyPacket {
                record,
                disposition: PacketDisposition::DiscardedConflict,
            }];
        }
        self.buffer.push(BufferedPacket {
            record,
            extended_seq: extended,
        });
        self.drain_ready()
    }

    pub fn flush_remaining(&mut self) -> Vec<ReadyPacket> {
        self.buffer.sort_by_key(|p| p.extended_seq);
        let mut out = Vec::new();
        while let Some(pkt) = self.buffer.first() {
            if let Some(frontier) = self.frontier_extended {
                if pkt.extended_seq > frontier + REORDER_WINDOW as u64 {
                    break;
                }
            }
            let pkt = self.buffer.remove(0);
            out.push(self.emit_ready(pkt));
        }
        out
    }

    pub fn placement_samples(&mut self, record: &JournalRecord) -> u64 {
        if let Some(capture) = record.session_capture_offset_samples {
            return capture;
        }
        if record.opus_payload.is_empty() {
            return self.capture_anchor_samples;
        }
        if self.rtp_anchor.is_none() {
            self.rtp_anchor = Some(record.rtp_timestamp);
            self.arrival_anchor_ns = record.arrived_at_ns;
            self.capture_anchor_samples = 0;
        }
        let anchor = self.rtp_anchor.unwrap();
        let delta = rtp_delta_samples(anchor, record.rtp_timestamp);
        self.capture_anchor_samples + delta
    }

    fn map_extended_sequence(&mut self, record: &JournalRecord) -> u64 {
        if self.ssrc_epoch == 0 && self.last_sequence.is_none() {
            self.ssrc_epoch = record.ssrc_epoch;
        } else if record.ssrc_epoch != self.ssrc_epoch {
            self.ssrc_epoch = record.ssrc_epoch;
            self.epoch_generation = self.epoch_generation.saturating_add(1);
            self.last_sequence = None;
        }
        let seq = record.sequence;
        if let Some(last) = self.last_sequence {
            let delta = seq.wrapping_sub(last) as i32;
            if delta < -i16::MAX as i32 / 2 {
                self.extended_base += 1u64 << 16;
            } else if delta > i16::MAX as i32 / 2 {
                self.extended_base = self.extended_base.saturating_sub(1u64 << 16);
            }
        }
        self.last_sequence = Some(seq);
        self.extended_base | ((self.epoch_generation as u64) << 48) | u64::from(seq)
    }

    fn is_duplicate_or_stale(&self, extended: u64) -> bool {
        if let Some(f) = self.frontier_extended {
            if extended <= f {
                return true;
            }
            if extended > f + REORDER_WINDOW as u64 {
                return false;
            }
        }
        false
    }

    fn drain_ready(&mut self) -> Vec<ReadyPacket> {
        self.buffer.sort_by_key(|p| p.extended_seq);
        let mut out = Vec::new();
        while let Some(idx) = self.buffer.iter().position(|p| {
            self.frontier_extended
                .map(|f| p.extended_seq <= f + REORDER_WINDOW as u64)
                .unwrap_or(true)
        }) {
            let pkt = self.buffer.remove(idx);
            if let Some(f) = self.frontier_extended {
                if pkt.extended_seq <= f {
                    out.push(ReadyPacket {
                        record: pkt.record,
                        disposition: PacketDisposition::DiscardedDuplicate,
                    });
                    continue;
                }
            }
            self.frontier_extended = Some(pkt.extended_seq);
            out.push(self.emit_ready(pkt));
        }
        out
    }

    fn emit_ready(&self, pkt: BufferedPacket) -> ReadyPacket {
        let disposition = if pkt.record.opus_payload.is_empty() {
            PacketDisposition::LostPlacement
        } else {
            PacketDisposition::Decode
        };
        ReadyPacket {
            record: pkt.record,
            disposition,
        }
    }
}

fn rtp_delta_samples(anchor: u32, ts: u32) -> u64 {
    let delta_ts = if ts >= anchor {
        u64::from(ts - anchor)
    } else {
        u64::from(ts) + RTP_TIMESTAMP_MOD - u64::from(anchor)
    };
    delta_ts
}

pub fn lost_marker_span_samples() -> u64 {
    OPUS_FRAME_SAMPLES
}
