//! One segment offer then one binary payload (no interleaving).

use super::protocol::DirectWssSegmentOffer;
use crate::voice_delivery::direct_local::DirectSessionBinding;
use crate::voice_delivery::hash::hex_digest;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectWssSegmentFsmState {
    Idle,
    AwaitingBinary,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectWssSegmentFsmError {
    #[error("unexpected binary")]
    UnexpectedBinary,
    #[error("interleaved control while awaiting binary")]
    InterleavedControlWhileAwaitingBinary,
    #[error("duplicate in-flight offer")]
    DuplicateInFlightOffer,
    #[error("binary length mismatch")]
    BinaryLengthMismatch,
    #[error("binary hash mismatch")]
    BinaryHashMismatch,
    #[error("binding field mismatch")]
    BindingFieldMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssSegmentReady {
    pub segment_sequence: u64,
    pub byte_length: u64,
    pub sha256_hex: String,
}

pub enum InterleavedControlMarker {
    Ping,
}

pub struct DirectWssSegmentFsm {
    binding: DirectSessionBinding,
    state: DirectWssSegmentFsmState,
    in_flight: Option<DirectWssSegmentOffer>,
    ready: Option<DirectWssSegmentReady>,
}

impl DirectWssSegmentFsm {
    pub fn new(binding: DirectSessionBinding) -> Self {
        Self {
            binding,
            state: DirectWssSegmentFsmState::Idle,
            in_flight: None,
            ready: None,
        }
    }

    pub fn state(&self) -> DirectWssSegmentFsmState {
        self.state
    }

    pub fn ready_for_durability(&self) -> bool {
        self.ready.is_some()
    }

    pub fn take_ready(&mut self) -> Option<DirectWssSegmentReady> {
        self.ready.take()
    }

    pub fn ping_as_interleaved_control() -> InterleavedControlMarker {
        InterleavedControlMarker::Ping
    }

    pub fn on_control_interleaved(
        &mut self,
        _control: InterleavedControlMarker,
    ) -> Result<(), DirectWssSegmentFsmError> {
        if self.state == DirectWssSegmentFsmState::AwaitingBinary {
            return Err(DirectWssSegmentFsmError::InterleavedControlWhileAwaitingBinary);
        }
        Ok(())
    }

    pub fn on_segment_offer(
        &mut self,
        offer: &DirectWssSegmentOffer,
    ) -> Result<(), DirectWssSegmentFsmError> {
        if self.state == DirectWssSegmentFsmState::AwaitingBinary {
            return Err(DirectWssSegmentFsmError::DuplicateInFlightOffer);
        }
        if offer.protocol_version != super::protocol::DIRECT_WSS_PROTOCOL_VERSION
            || offer.session_id != self.binding.session_id
            || offer.guild_id != self.binding.guild_id
            || offer.device_id != self.binding.device_id
            || offer.delivery_generation != self.binding.delivery_generation
            || offer.discord_instance_id != self.binding.discord_instance_id
            || offer.leader_fence_generation != self.binding.leader_fence_generation
        {
            return Err(DirectWssSegmentFsmError::BindingFieldMismatch);
        }
        self.in_flight = Some(offer.clone());
        self.ready = None;
        self.state = DirectWssSegmentFsmState::AwaitingBinary;
        Ok(())
    }

    pub fn on_binary_frame(
        &mut self,
        bytes: &[u8],
    ) -> Result<DirectWssSegmentReady, DirectWssSegmentFsmError> {
        let offer = match self.in_flight.as_ref() {
            Some(o) => o,
            None => return Err(DirectWssSegmentFsmError::UnexpectedBinary),
        };
        if bytes.len() as u64 != offer.byte_length {
            return Err(DirectWssSegmentFsmError::BinaryLengthMismatch);
        }
        let digest = hex_digest(&Sha256::digest(bytes));
        if digest != offer.sha256_hex {
            return Err(DirectWssSegmentFsmError::BinaryHashMismatch);
        }
        let ready = DirectWssSegmentReady {
            segment_sequence: offer.segment_sequence,
            byte_length: offer.byte_length,
            sha256_hex: offer.sha256_hex.clone(),
        };
        self.in_flight = None;
        self.state = DirectWssSegmentFsmState::Idle;
        self.ready = Some(ready.clone());
        Ok(ready)
    }
}
