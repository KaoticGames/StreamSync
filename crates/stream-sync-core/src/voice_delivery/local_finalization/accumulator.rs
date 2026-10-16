//! Per-user journal packet accumulator (parity with ChatBot `userJournalPacketAccumulator.ts`).

use super::opus_journal_timeline::validate_opus_packet_for_journal_timeline;
use super::rtp_generation_inference::{
    decide_rtp_timestamp_discontinuity, discontinuity_decision_to_action, rtp_forward_delta,
    InferredRtpGenerationBoundary, RtpCandidatePacket, RtpDiscontinuityAction,
    RtpDiscontinuityDecision, RtpPlacementContext, MAX_RTP_GAP_SAMPLES, OPUS_FRAME_SAMPLES,
    REORDER_WINDOW_PACKETS,
};
use super::synj::JournalRecord;

pub const REORDER_WINDOW: u16 = REORDER_WINDOW_PACKETS;
const REORDER_BUFFER_EXTRA_PACKETS: u16 = 4;
const MAX_EPOCH_REORDER_BUFFER_PACKETS: usize = 36;
const DEDUPE_RETENTION_PACKETS: u64 = 32;
const MAX_RETAINED_INFERRED_GENERATION_BOUNDARIES: usize = 64;
const UINT16_MOD: u32 = 0x1_0000;
const HALF_RANGE: u32 = UINT16_MOD / 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    pub extended_sequence: i64,
    pub effective_epoch: u32,
    pub timeline_sample_start: Option<u64>,
    /// Known Opus span in 48 kHz samples when framing validates; loss markers may be `None`.
    pub span_samples: Option<u64>,
}

#[derive(Debug)]
pub enum AccumulatorError {
    Conflict,
    Overlap(String),
    Pathological(String),
    SequenceAmbiguity(String),
    ReorderBufferExceeded,
    FramingSkipPlacement(String),
}

impl AccumulatorError {
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict)
    }
    pub fn is_overlap(&self) -> bool {
        matches!(self, Self::Overlap(_))
    }
}

struct MutablePacket {
    sequence: u16,
    rtp_timestamp: u32,
    capture_offset_samples: u64,
    arrived_at_ns: u64,
    opus_payload: Vec<u8>,
}

struct BufferedPacket {
    record: JournalRecord,
    extended_sequence: i64,
    framing_skip: bool,
}

struct EmittedDedupeSnapshot {
    rtp_timestamp: u32,
    capture_offset_samples: u64,
    arrived_at_ns: u64,
    opus_payload: Vec<u8>,
}

pub struct UserPacketAccumulator {
    user_id: String,
    target_sample_count: u64,
    placement_overlap_tolerant: bool,
    frontier_extended: Option<i64>,
    buffer: std::collections::BTreeMap<i64, BufferedPacket>,
    dedupe_by_extended: std::collections::BTreeMap<i64, MutablePacket>,
    emitted_by_extended: std::collections::BTreeMap<i64, EmittedDedupeSnapshot>,
    next_emit_extended: Option<i64>,
    prev_raw_timestamp: Option<u32>,
    prev_sample_start: u64,
    prev_end: u64,
    epoch_anchor: u64,
    prior_epoch_end: u64,
    prior_epoch_last_capture: u64,
    max_emitted_sample_end: u64,
    journal_ssrc_epoch: u32,
    effective_epoch: u32,
    is_first_effective_epoch: bool,
    implicit_generation_ordinal: u32,
    total_inferred_boundary_count: u32,
    inferred_boundaries_truncated: bool,
    last_emitted_raw_sequence: u16,
    last_emitted_extended_sequence: i64,
    last_emitted_capture_offset: u64,
    last_emitted_arrived_at_ns: u64,
    inferred_generation_boundaries: Vec<InferredRtpGenerationBoundary>,
    timeline_active: bool,
    active_journal_epoch: Option<u32>,
    epoch_anchor_capture: u64,
}

impl Default for UserPacketAccumulator {
    fn default() -> Self {
        Self::new("", u64::MAX)
    }
}

impl UserPacketAccumulator {
    pub fn new(user_id: &str, target_sample_count: u64) -> Self {
        Self {
            user_id: user_id.into(),
            target_sample_count,
            placement_overlap_tolerant: false,
            frontier_extended: None,
            buffer: std::collections::BTreeMap::new(),
            dedupe_by_extended: std::collections::BTreeMap::new(),
            emitted_by_extended: std::collections::BTreeMap::new(),
            next_emit_extended: None,
            prev_raw_timestamp: None,
            prev_sample_start: 0,
            prev_end: 0,
            epoch_anchor: 0,
            prior_epoch_end: 0,
            prior_epoch_last_capture: 0,
            max_emitted_sample_end: 0,
            journal_ssrc_epoch: 0,
            effective_epoch: 0,
            is_first_effective_epoch: true,
            implicit_generation_ordinal: 0,
            total_inferred_boundary_count: 0,
            inferred_boundaries_truncated: false,
            last_emitted_raw_sequence: 0,
            last_emitted_extended_sequence: 0,
            last_emitted_capture_offset: 0,
            last_emitted_arrived_at_ns: 0,
            inferred_generation_boundaries: Vec::new(),
            timeline_active: false,
            active_journal_epoch: None,
            epoch_anchor_capture: 0,
        }
    }

    pub fn effective_epoch(&self) -> u32 {
        self.effective_epoch
    }

    pub fn inferred_generation_count(&self) -> u32 {
        self.total_inferred_boundary_count
    }

    /// Journal records in file order with automatic explicit `ssrc_epoch` transitions.
    pub fn ingest_timeline(
        &mut self,
        record: JournalRecord,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        if !self.user_id.is_empty() && record.discord_user_id != self.user_id {
            return Ok(Vec::new());
        }
        let epoch = record.ssrc_epoch;
        let anchor = record.session_capture_offset_samples.unwrap_or(0);
        let mut out = Vec::new();
        if self.active_journal_epoch != Some(epoch) {
            if self.timeline_active {
                out.extend(self.end_epoch_sync(self.epoch_anchor_capture)?);
            }
            out.extend(self.begin_epoch_sync(epoch, anchor)?);
            self.timeline_active = true;
            self.active_journal_epoch = Some(epoch);
            self.epoch_anchor_capture = anchor;
        }
        out.extend(self.ingest_record_in_epoch(record, epoch)?);
        Ok(out)
    }

    pub fn flush_remaining(&mut self) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let mut out = Vec::new();
        if self.timeline_active {
            out.extend(self.end_epoch_sync(self.epoch_anchor_capture)?);
            self.timeline_active = false;
            self.active_journal_epoch = None;
        }
        out.extend(self.flush_buffer_hard_sync()?);
        Ok(out)
    }

    pub fn placement_samples(&self, record: &JournalRecord) -> u64 {
        if let Some(capture) = record.session_capture_offset_samples {
            return capture;
        }
        if record.opus_payload.is_empty() {
            return self.capture_anchor_samples();
        }
        if self.prev_raw_timestamp.is_none() {
            return self.epoch_anchor;
        }
        let anchor = self.prev_raw_timestamp.unwrap();
        self.prev_sample_start + rtp_forward_delta(anchor, record.rtp_timestamp)
    }

    fn capture_anchor_samples(&self) -> u64 {
        self.epoch_anchor
    }

    fn begin_epoch_sync(
        &mut self,
        journal_epoch: u32,
        anchor_capture_offset: u64,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let flushed = self.flush_buffer_hard_sync()?;
        self.reset_epoch_state(journal_epoch, anchor_capture_offset);
        Ok(flushed)
    }

    fn end_epoch_sync(
        &mut self,
        anchor_capture_offset: u64,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let flushed = self.flush_buffer_hard_sync()?;
        self.prior_epoch_last_capture = anchor_capture_offset;
        self.prior_epoch_end = self.prev_end;
        Ok(flushed)
    }

    fn reset_epoch_state(&mut self, journal_epoch: u32, anchor_capture_offset: u64) {
        self.journal_ssrc_epoch = journal_epoch;
        self.implicit_generation_ordinal = 0;
        if !self.is_first_effective_epoch {
            self.effective_epoch += 1;
        } else {
            self.is_first_effective_epoch = false;
        }
        self.frontier_extended = None;
        self.dedupe_by_extended.clear();
        self.emitted_by_extended.clear();
        self.next_emit_extended = None;
        self.prev_raw_timestamp = None;
        let mut epoch_anchor = anchor_capture_offset;
        if self.effective_epoch > 0 && epoch_anchor < self.prior_epoch_last_capture {
            epoch_anchor = self.prior_epoch_last_capture;
        }
        if self.effective_epoch > 0 && epoch_anchor < self.prior_epoch_end {
            epoch_anchor = self.prior_epoch_end;
        }
        self.epoch_anchor = epoch_anchor;
        self.prev_sample_start = epoch_anchor;
        self.prev_end = epoch_anchor;
    }

    fn ingest_record_in_epoch(
        &mut self,
        record: JournalRecord,
        journal_epoch: u32,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let mut out = Vec::new();
        if record.opus_payload.is_empty() {
            out.extend(self.ingest_framing_skip_record(record, journal_epoch)?);
            return Ok(out);
        }
        if validate_opus_packet_for_journal_timeline(&record.opus_payload).is_err() {
            out.extend(self.ingest_framing_skip_record(record, journal_epoch)?);
            return Ok(out);
        }
        let (prefix, core) = self.ingest_record_core(&record, journal_epoch)?;
        out.extend(prefix);
        match core {
            IngestCoreResult::Buffered => {
                out.extend(self.drain_emittable_sync()?);
            }
            IngestCoreResult::ExactReplay => {
                out.push(ReadyPacket {
                    record: record.clone(),
                    disposition: PacketDisposition::DiscardedDuplicate,
                    extended_sequence: 0,
                    effective_epoch: self.effective_epoch,
                    timeline_sample_start: None,
                    span_samples: None,
                });
            }
            IngestCoreResult::OutOfWindow => {
                out.push(ReadyPacket {
                    record,
                    disposition: PacketDisposition::DiscardedOutOfWindow,
                    extended_sequence: 0,
                    effective_epoch: self.effective_epoch,
                    timeline_sample_start: None,
                    span_samples: None,
                });
            }
        }
        Ok(out)
    }

    fn ingest_framing_skip_record(
        &mut self,
        record: JournalRecord,
        journal_epoch: u32,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let pkt = self.record_to_packet(&record)?;
        let mut extended = self.extend_one(&pkt)?;
        let mut boundary_flush = Vec::new();
        if extended.is_none() {
            let ctx = self.classify_implicit_boundary_from_sequence(&pkt)?;
            if ctx.is_some() {
                boundary_flush = self.apply_implicit_boundary_sync(&pkt, &ctx.unwrap())?;
                extended = self.extend_one(&pkt)?;
            }
        }
        let extended = extended.ok_or_else(|| {
            AccumulatorError::FramingSkipPlacement(format!(
                "cannot place framing skip seq {}",
                record.sequence
            ))
        })?;
        if !self.buffer_ingested_packet(journal_epoch, extended, &record, &pkt, true)? {
            return Ok(boundary_flush);
        }
        let drained = self.drain_emittable_sync()?;
        boundary_flush.extend(drained);
        Ok(boundary_flush)
    }

    fn ingest_record_core(
        &mut self,
        record: &JournalRecord,
        journal_epoch: u32,
    ) -> Result<(Vec<ReadyPacket>, IngestCoreResult), AccumulatorError> {
        let pkt = self.record_to_packet(record)?;
        let mut prefix = Vec::new();
        let mut extended = self.extend_one(&pkt)?;
        if extended.is_none() {
            if let Some(ctx) = self.classify_implicit_boundary_from_sequence(&pkt)? {
                prefix = self.apply_implicit_boundary_sync(&pkt, &ctx)?;
                extended = self.extend_one(&pkt)?;
            }
        }
        let extended = match extended {
            Some(e) => e,
            None => return Ok((prefix, IngestCoreResult::OutOfWindow)),
        };
        if self.is_exact_replay(extended, &pkt) {
            return Ok((prefix, IngestCoreResult::ExactReplay));
        }
        if self.buffer_ingested_packet(journal_epoch, extended, record, &pkt, false)? {
            Ok((prefix, IngestCoreResult::Buffered))
        } else {
            Ok((prefix, IngestCoreResult::ExactReplay))
        }
    }

    fn record_to_packet(&self, rec: &JournalRecord) -> Result<MutablePacket, AccumulatorError> {
        let capture = rec.session_capture_offset_samples.ok_or_else(|| {
            AccumulatorError::Pathological(
                "journal v2 record missing sessionCaptureOffsetSamples".into(),
            )
        })?;
        Ok(MutablePacket {
            sequence: rec.sequence,
            rtp_timestamp: rec.rtp_timestamp,
            capture_offset_samples: capture,
            arrived_at_ns: rec.arrived_at_ns,
            opus_payload: rec.opus_payload.clone(),
        })
    }

    fn buffer_ingested_packet(
        &mut self,
        _epoch: u32,
        extended: i64,
        record: &JournalRecord,
        pkt: &MutablePacket,
        framing_skip: bool,
    ) -> Result<bool, AccumulatorError> {
        if self.is_conflicting_replay(extended, pkt) {
            return Err(AccumulatorError::Conflict);
        }
        if self.is_exact_replay(extended, pkt) {
            return Ok(false);
        }
        if let Some(prev_dup) = self.dedupe_by_extended.get(&extended) {
            if !packets_equivalent(prev_dup, pkt) {
                return Err(AccumulatorError::Conflict);
            }
            return Ok(false);
        }
        if let Some(existing) = self.buffer.get(&extended) {
            let existing_pkt = MutablePacket {
                sequence: existing.record.sequence,
                rtp_timestamp: existing.record.rtp_timestamp,
                capture_offset_samples: existing.record.session_capture_offset_samples.unwrap_or(0),
                arrived_at_ns: existing.record.arrived_at_ns,
                opus_payload: existing.record.opus_payload.clone(),
            };
            if !packets_equivalent(&existing_pkt, pkt) {
                return Err(AccumulatorError::Conflict);
            }
            return Ok(false);
        }
        if self.buffer.len() >= MAX_EPOCH_REORDER_BUFFER_PACKETS {
            return Err(AccumulatorError::ReorderBufferExceeded);
        }
        self.buffer.insert(
            extended,
            BufferedPacket {
                record: record.clone(),
                extended_sequence: extended,
                framing_skip,
            },
        );
        self.dedupe_by_extended.insert(extended, pkt.clone());
        Ok(true)
    }

    fn placement_context(&self) -> Option<RtpPlacementContext> {
        if self.prev_raw_timestamp.is_none() {
            return None;
        }
        Some(RtpPlacementContext {
            prior_raw_sequence: self.last_emitted_raw_sequence,
            prior_extended_sequence: self.last_emitted_extended_sequence,
            prior_raw_rtp_timestamp: self.prev_raw_timestamp.unwrap(),
            prior_capture_offset_samples: self.last_emitted_capture_offset,
            prior_arrived_at_ns: self.last_emitted_arrived_at_ns,
            prior_timeline_sample_end: self.prev_end,
        })
    }

    fn classify_implicit_boundary_from_sequence(
        &self,
        pkt: &MutablePacket,
    ) -> Result<Option<RtpPlacementContext>, AccumulatorError> {
        if self.frontier_extended.is_none() {
            return Ok(None);
        }
        let raw = seq16(pkt.sequence);
        let frontier_raw = seq16_i64(self.frontier_extended.unwrap());
        let behind = (frontier_raw.wrapping_sub(raw)) & 0xffff;
        if behind == 0 || behind <= REORDER_WINDOW_PACKETS {
            return Ok(None);
        }
        let ctx = self.placement_context().ok_or_else(|| {
            AccumulatorError::Pathological("implicit boundary without placement context".into())
        })?;
        let decision = decide_rtp_timestamp_discontinuity(
            &ctx,
            &RtpCandidatePacket {
                raw_sequence: pkt.sequence,
                extended_sequence: i64::from(raw),
                rtp_timestamp: pkt.rtp_timestamp,
                capture_offset_samples: pkt.capture_offset_samples,
                arrived_at_ns: pkt.arrived_at_ns,
            },
        );
        if !matches!(decision, RtpDiscontinuityDecision::ImplicitGeneration) {
            return Ok(None);
        }
        Ok(Some(ctx))
    }

    fn classify_placement_action(&self, pkt: &BufferedPacket) -> RtpDiscontinuityAction {
        let ctx = self.placement_context().expect("placement ctx");
        let delta = rtp_forward_delta(ctx.prior_raw_rtp_timestamp, pkt.record.rtp_timestamp);
        if delta != 0 && delta <= MAX_RTP_GAP_SAMPLES {
            return RtpDiscontinuityAction::Normal {
                sample_start: self.prev_sample_start + delta,
            };
        }
        let capture = pkt.record.session_capture_offset_samples.unwrap_or(0);
        let decision = decide_rtp_timestamp_discontinuity(
            &ctx,
            &RtpCandidatePacket {
                raw_sequence: pkt.record.sequence,
                extended_sequence: pkt.extended_sequence,
                rtp_timestamp: pkt.record.rtp_timestamp,
                capture_offset_samples: capture,
                arrived_at_ns: pkt.record.arrived_at_ns,
            },
        );
        discontinuity_decision_to_action(decision, |rtp_delta| self.prev_sample_start + rtp_delta)
    }

    fn apply_implicit_boundary_sync(
        &mut self,
        pkt: &MutablePacket,
        ctx: &RtpPlacementContext,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let flushed = self.flush_buffer_hard_sync()?;
        self.commit_implicit_boundary_state(pkt, ctx);
        Ok(flushed)
    }

    fn commit_implicit_boundary_state(&mut self, pkt: &MutablePacket, ctx: &RtpPlacementContext) {
        let prior_effective = self.effective_epoch;
        self.effective_epoch += 1;
        self.implicit_generation_ordinal += 1;
        let anchor = pkt
            .capture_offset_samples
            .max(ctx.prior_timeline_sample_end);
        self.record_inferred_boundary(InferredRtpGenerationBoundary {
            journal_ssrc_epoch: self.journal_ssrc_epoch,
            implicit_generation_ordinal: self.implicit_generation_ordinal,
            prior_effective_epoch: prior_effective,
            new_effective_epoch: self.effective_epoch,
            sequence: pkt.sequence,
            capture_offset_samples: anchor,
        });
        self.frontier_extended = None;
        self.dedupe_by_extended.clear();
        self.emitted_by_extended.clear();
        self.next_emit_extended = None;
        self.prev_raw_timestamp = None;
        self.epoch_anchor = anchor;
        self.prev_sample_start = anchor;
        self.prev_end = anchor;
        self.prior_epoch_last_capture = pkt.capture_offset_samples;
        self.prior_epoch_end = ctx.prior_timeline_sample_end;
    }

    fn record_inferred_boundary(&mut self, entry: InferredRtpGenerationBoundary) {
        self.total_inferred_boundary_count += 1;
        if self.inferred_generation_boundaries.len() >= MAX_RETAINED_INFERRED_GENERATION_BOUNDARIES
        {
            self.inferred_generation_boundaries.remove(0);
            self.inferred_boundaries_truncated = true;
        }
        self.inferred_generation_boundaries.push(entry);
    }

    fn extend_one(&mut self, pkt: &MutablePacket) -> Result<Option<i64>, AccumulatorError> {
        let raw = seq16(pkt.sequence);
        if self.frontier_extended.is_none() {
            let ext = i64::from(raw);
            self.frontier_extended = Some(ext);
            self.next_emit_extended = Some(ext);
            return Ok(Some(ext));
        }
        let frontier = self.frontier_extended.unwrap();
        let frontier_raw = seq16_i64(frontier);
        let behind = (frontier_raw.wrapping_sub(raw)) & 0xffff;
        if behind == HALF_RANGE as u16 {
            return Err(AccumulatorError::SequenceAmbiguity(format!(
                "ambiguous half-range delta for seq {}",
                raw
            )));
        }
        if behind != 0 && behind <= REORDER_WINDOW_PACKETS && seq_before(raw, frontier_raw) {
            return Ok(Some(frontier - i64::from(behind)));
        }
        let ahead = (raw.wrapping_sub(frontier_raw)) & 0xffff;
        let behind_alt = (frontier_raw.wrapping_sub(raw)) & 0xffff;
        if ahead == HALF_RANGE as u16 || behind_alt == HALF_RANGE as u16 {
            return Err(AccumulatorError::SequenceAmbiguity(
                "sequence unwrap hits ambiguous half-range (32768)".into(),
            ));
        }
        let mut candidates = Vec::new();
        if ahead != 0 && ahead < HALF_RANGE as u16 {
            candidates.push(frontier + i64::from(ahead));
        }
        if behind_alt != 0 && behind_alt < HALF_RANGE as u16 {
            candidates.push(frontier - i64::from(behind_alt));
        }
        if candidates.is_empty() {
            if behind == 0 && ahead == 0 {
                return Ok(Some(frontier));
            }
            if behind != 0 && behind > REORDER_WINDOW_PACKETS {
                return Ok(None);
            }
            return Err(AccumulatorError::SequenceAmbiguity(format!(
                "ambiguous RTP sequence {} relative to frontier {}",
                raw, frontier_raw
            )));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if candidates.len() > 1 {
            let span = candidates.last().unwrap() - candidates.first().unwrap();
            if span >= HALF_RANGE as i64 {
                return Err(AccumulatorError::SequenceAmbiguity(
                    "sequence unwrap spans ambiguous half-range".into(),
                ));
            }
        }
        let ext = candidates[0];
        if ext < frontier - i64::from(REORDER_WINDOW_PACKETS) {
            return Ok(None);
        }
        if ext > frontier {
            self.frontier_extended = Some(ext);
        }
        Ok(Some(ext))
    }

    fn is_conflicting_replay(&self, extended: i64, pkt: &MutablePacket) -> bool {
        if self.next_emit_extended.is_none() || extended >= self.next_emit_extended.unwrap() {
            return false;
        }
        self.emitted_by_extended
            .get(&extended)
            .is_some_and(|snap| !packets_equivalent_snapshot(snap, pkt))
    }

    fn is_exact_replay(&self, extended: i64, pkt: &MutablePacket) -> bool {
        if self.next_emit_extended.is_none() || extended >= self.next_emit_extended.unwrap() {
            return false;
        }
        if let Some(snap) = self.emitted_by_extended.get(&extended) {
            return packets_equivalent_snapshot(snap, pkt);
        }
        if extended < 0 {
            return false;
        }
        true
    }

    fn evict_retained_before(&mut self, min_extended: i64) {
        if min_extended == i64::MIN {
            return;
        }
        self.dedupe_by_extended.retain(|k, _| *k >= min_extended);
        self.emitted_by_extended.retain(|k, _| *k >= min_extended);
    }

    fn note_irrevocably_emitted(&mut self, pkt: &BufferedPacket) {
        let extended = pkt.extended_sequence;
        self.dedupe_by_extended.remove(&extended);
        self.emitted_by_extended.insert(
            extended,
            EmittedDedupeSnapshot {
                rtp_timestamp: pkt.record.rtp_timestamp,
                capture_offset_samples: pkt.record.session_capture_offset_samples.unwrap_or(0),
                arrived_at_ns: pkt.record.arrived_at_ns,
                opus_payload: pkt.record.opus_payload.clone(),
            },
        );
        let min = extended - i64::try_from(DEDUPE_RETENTION_PACKETS).unwrap_or(i64::MAX);
        self.evict_retained_before(min);
    }

    fn drain_emittable_sync(&mut self) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let mut out = Vec::new();
        if self.frontier_extended.is_none() || self.next_emit_extended.is_none() {
            return Ok(out);
        }
        let stale_before = self.frontier_extended.unwrap() - i64::from(REORDER_WINDOW_PACKETS);
        while !self.buffer.is_empty() {
            let next = self.next_emit_extended.unwrap();
            if let Some(direct) = self.buffer.remove(&next) {
                out.extend(self.emit_buffered_sync(direct)?);
                self.next_emit_extended = Some(next + 1);
                continue;
            }
            let min_key = self.buffer.keys().next().copied().unwrap();
            if min_key <= stale_before {
                let pkt = self.buffer.remove(&min_key).unwrap();
                if self.next_emit_extended.is_none() || min_key < self.next_emit_extended.unwrap() {
                    self.next_emit_extended = Some(min_key + 1);
                }
                out.extend(self.emit_buffered_sync(pkt)?);
                continue;
            }
            break;
        }
        Ok(out)
    }

    fn flush_buffer_hard_sync(&mut self) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let mut out = Vec::new();
        if self.buffer.is_empty() {
            return Ok(out);
        }
        let keys: Vec<i64> = self.buffer.keys().copied().collect();
        for key in keys {
            let pkt = self.buffer.remove(&key).unwrap();
            out.extend(self.emit_buffered_sync(pkt)?);
        }
        self.next_emit_extended = None;
        Ok(out)
    }

    fn emit_buffered_sync(
        &mut self,
        pkt: BufferedPacket,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        if pkt.framing_skip {
            return Ok(vec![self.emit_framing_skip(pkt)]);
        }
        self.place_buffered_packet(pkt)
    }

    fn emit_framing_skip(&mut self, pkt: BufferedPacket) -> ReadyPacket {
        let sample_start = if self.prev_raw_timestamp.is_none() {
            self.epoch_anchor
        } else {
            let action = self.classify_placement_action(&pkt);
            match action {
                RtpDiscontinuityAction::ImplicitBoundary => {
                    let ctx = self.placement_context().unwrap();
                    let mp = MutablePacket {
                        sequence: pkt.record.sequence,
                        rtp_timestamp: pkt.record.rtp_timestamp,
                        capture_offset_samples: pkt
                            .record
                            .session_capture_offset_samples
                            .unwrap_or(0),
                        arrived_at_ns: pkt.record.arrived_at_ns,
                        opus_payload: pkt.record.opus_payload.clone(),
                    };
                    self.commit_implicit_boundary_state(&mp, &ctx);
                    self.epoch_anchor
                }
                RtpDiscontinuityAction::Gap { sample_start }
                | RtpDiscontinuityAction::Normal { sample_start } => sample_start,
                RtpDiscontinuityAction::Refuse { reason: _ } => pkt
                    .record
                    .session_capture_offset_samples
                    .unwrap_or(self.prev_end),
            }
        };
        self.last_emitted_raw_sequence = pkt.record.sequence;
        self.last_emitted_extended_sequence = pkt.extended_sequence;
        self.last_emitted_capture_offset = pkt.record.session_capture_offset_samples.unwrap_or(0);
        self.last_emitted_arrived_at_ns = pkt.record.arrived_at_ns;
        self.prev_raw_timestamp = Some(pkt.record.rtp_timestamp);
        self.note_irrevocably_emitted(&pkt);
        if self.next_emit_extended.is_none()
            || pkt.extended_sequence >= self.next_emit_extended.unwrap()
        {
            self.next_emit_extended = Some(pkt.extended_sequence + 1);
        }
        let span_samples = if pkt.record.opus_payload.is_empty() {
            None
        } else {
            validate_opus_packet_for_journal_timeline(&pkt.record.opus_payload).ok()
        };
        ReadyPacket {
            record: pkt.record,
            disposition: PacketDisposition::LostPlacement,
            extended_sequence: pkt.extended_sequence,
            effective_epoch: self.effective_epoch,
            timeline_sample_start: Some(sample_start),
            span_samples,
        }
    }

    fn place_buffered_packet(
        &mut self,
        pkt: BufferedPacket,
    ) -> Result<Vec<ReadyPacket>, AccumulatorError> {
        let frame_samples = validate_opus_packet_for_journal_timeline(&pkt.record.opus_payload)
            .map_err(|e| AccumulatorError::Pathological(format!("invalid opus framing: {e}")))?;
        if self.prev_raw_timestamp.is_none() {
            return Ok(self
                .finalize_placed_packet(pkt, frame_samples, self.epoch_anchor)?
                .into_iter()
                .collect());
        }
        let action = self.classify_placement_action(&pkt);
        match action {
            RtpDiscontinuityAction::ImplicitBoundary => {
                let ctx = self.placement_context().unwrap();
                let mp = MutablePacket {
                    sequence: pkt.record.sequence,
                    rtp_timestamp: pkt.record.rtp_timestamp,
                    capture_offset_samples: pkt.record.session_capture_offset_samples.unwrap_or(0),
                    arrived_at_ns: pkt.record.arrived_at_ns,
                    opus_payload: pkt.record.opus_payload.clone(),
                };
                let mut out = self.apply_implicit_boundary_sync(&mp, &ctx)?;
                out.extend(
                    self.finalize_placed_packet(pkt, frame_samples, self.epoch_anchor)?
                        .into_iter(),
                );
                Ok(out)
            }
            RtpDiscontinuityAction::Gap { sample_start }
            | RtpDiscontinuityAction::Normal { sample_start } => Ok(self
                .finalize_placed_packet(pkt, frame_samples, sample_start)?
                .into_iter()
                .collect()),
            RtpDiscontinuityAction::Refuse { reason } => {
                Err(AccumulatorError::Pathological(reason))
            }
        }
    }

    fn finalize_placed_packet(
        &mut self,
        pkt: BufferedPacket,
        frame_samples: u64,
        sample_start: u64,
    ) -> Result<Option<ReadyPacket>, AccumulatorError> {
        let sample_end = sample_start + frame_samples;
        if self.prev_raw_timestamp.is_some() && sample_start < self.prev_end {
            if self.placement_overlap_tolerant {
                self.note_irrevocably_emitted(&pkt);
                return Ok(None);
            }
            return Err(AccumulatorError::Overlap(format!(
                "overlapping opus placement seq {} sampleStart {} prevEnd {}",
                pkt.record.sequence, sample_start, self.prev_end
            )));
        }
        self.prev_raw_timestamp = Some(pkt.record.rtp_timestamp);
        self.prev_sample_start = sample_start;
        self.prev_end = sample_end;
        self.last_emitted_raw_sequence = pkt.record.sequence;
        self.last_emitted_extended_sequence = pkt.extended_sequence;
        self.last_emitted_capture_offset = pkt.record.session_capture_offset_samples.unwrap_or(0);
        self.last_emitted_arrived_at_ns = pkt.record.arrived_at_ns;
        if sample_end > self.max_emitted_sample_end {
            self.max_emitted_sample_end = sample_end;
        }
        if sample_end > self.target_sample_count {
            self.note_irrevocably_emitted(&pkt);
            return Ok(None);
        }
        self.note_irrevocably_emitted(&pkt);

        Ok(Some(ReadyPacket {
            record: pkt.record,
            disposition: PacketDisposition::Decode,
            extended_sequence: pkt.extended_sequence,
            effective_epoch: self.effective_epoch,
            timeline_sample_start: Some(sample_start),
            span_samples: Some(frame_samples),
        }))
    }
}

enum IngestCoreResult {
    Buffered,
    ExactReplay,
    OutOfWindow,
}

fn seq16(n: u16) -> u16 {
    n
}

fn seq16_i64(n: i64) -> u16 {
    n as u16
}

fn seq_before(a: u16, b: u16) -> bool {
    let diff = b.wrapping_sub(a);
    diff != 0 && diff < 0x8000
}

fn payloads_equal(a: &[u8], b: &[u8]) -> bool {
    a == b
}

fn packets_equivalent(a: &MutablePacket, b: &MutablePacket) -> bool {
    a.rtp_timestamp == b.rtp_timestamp
        && a.capture_offset_samples == b.capture_offset_samples
        && a.arrived_at_ns == b.arrived_at_ns
        && payloads_equal(&a.opus_payload, &b.opus_payload)
}

fn packets_equivalent_snapshot(snap: &EmittedDedupeSnapshot, pkt: &MutablePacket) -> bool {
    snap.rtp_timestamp == pkt.rtp_timestamp
        && snap.capture_offset_samples == pkt.capture_offset_samples
        && snap.arrived_at_ns == pkt.arrived_at_ns
        && payloads_equal(&snap.opus_payload, &pkt.opus_payload)
}

impl Clone for MutablePacket {
    fn clone(&self) -> Self {
        Self {
            sequence: self.sequence,
            rtp_timestamp: self.rtp_timestamp,
            capture_offset_samples: self.capture_offset_samples,
            arrived_at_ns: self.arrived_at_ns,
            opus_payload: self.opus_payload.clone(),
        }
    }
}

pub fn lost_marker_span_samples() -> u64 {
    OPUS_FRAME_SAMPLES
}
