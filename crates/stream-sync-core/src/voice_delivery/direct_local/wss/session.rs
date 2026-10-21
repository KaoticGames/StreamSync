//! Direct-local WSS dark session loop (library construct; not started by production wiring).

use super::auth::build_direct_wss_connect_request;
use super::protocol::{DirectWssControlMessage, DirectWssHello, DirectWssResumeCheckpoint};
use super::segment_fsm::DirectWssSegmentFsm;
use super::transport::{DirectWssTransport, DirectWssTransportEvent};
use crate::voice_delivery::direct_local::checkpoint::DirectCumulativeCheckpoint;
#[cfg(test)]
use crate::voice_delivery::direct_local::DirectDurabilityBoundary;
use crate::voice_delivery::direct_local::{
    DirectGenerationStore, DirectReceiverLimits, DirectSegmentDurabilityPipeline,
    DirectSessionBinding, DirectSessionBindingError,
};
use thiserror::Error;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectLocalWssStopReason {
    Rejected,
    Revoked,
    GracefulClose,
    AckDeadlineExceeded,
    MaxSessionDurationExceeded,
    TransportError,
    DurabilityError,
    AcceptedIdle,
}

#[derive(Debug, Error)]
pub enum DirectLocalWssSessionError {
    #[error("direct local wss session configuration invalid")]
    InvalidConfig,
    #[error("direct local wss transport error")]
    Transport,
    #[error("direct local wss protocol error")]
    Protocol,
}

pub struct DirectLocalWssRunConfig {
    pub binding: DirectSessionBinding,
    pub local_device_id: String,
    pub limits: DirectReceiverLimits,
    pub store: DirectGenerationStore,
    pub resume_checkpoint: DirectCumulativeCheckpoint,
    #[cfg(test)]
    pub test_injected_durability_failure: Option<DirectDurabilityBoundary>,
    #[cfg(test)]
    pub test_before_durability_delay: Option<std::time::Duration>,
}

pub struct DirectLocalWssDarkSession {
    binding: DirectSessionBinding,
    limits: DirectReceiverLimits,
    store: DirectGenerationStore,
    checkpoint: DirectCumulativeCheckpoint,
    fsm: DirectWssSegmentFsm,
    accepted: bool,
    next_expected_sequence: Option<u64>,
    #[cfg(test)]
    injected_durability_failure: Option<DirectDurabilityBoundary>,
    #[cfg(test)]
    before_durability_delay: Option<std::time::Duration>,
}

impl DirectLocalWssDarkSession {
    pub fn try_new(config: DirectLocalWssRunConfig) -> Result<Self, DirectLocalWssSessionError> {
        config
            .binding
            .assert_matches_local_device(&config.local_device_id)
            .map_err(|_| DirectLocalWssSessionError::InvalidConfig)?;
        DirectReceiverLimits::validate(&config.limits)
            .map_err(|_| DirectLocalWssSessionError::InvalidConfig)?;
        let checkpoint = DirectCumulativeCheckpoint::reconcile_supplied_with_durable_authority(
            &config.store,
            &config.resume_checkpoint,
        )
        .map_err(|_| DirectLocalWssSessionError::InvalidConfig)?;
        let binding = config.binding;
        Ok(Self {
            fsm: DirectWssSegmentFsm::new(binding.clone()),
            binding,
            limits: config.limits,
            store: config.store,
            checkpoint,
            accepted: false,
            next_expected_sequence: None,
            #[cfg(test)]
            injected_durability_failure: config.test_injected_durability_failure,
            #[cfg(test)]
            before_durability_delay: config.test_before_durability_delay,
        })
    }

    pub async fn run_until_stop<T: DirectWssTransport>(
        &mut self,
        transport: &T,
    ) -> Result<DirectLocalWssStopReason, DirectLocalWssSessionError> {
        let max_deadline = tokio::time::Instant::now() + self.limits.max_direct_session_duration;

        let connect = build_direct_wss_connect_request(&self.binding)
            .map_err(|_| DirectLocalWssSessionError::InvalidConfig)?;
        match tokio::time::timeout_at(max_deadline, transport.connect(connect.into_http_request()))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(DirectLocalWssSessionError::Transport),
            Err(_) => return Ok(DirectLocalWssStopReason::MaxSessionDurationExceeded),
        }

        let hello = DirectWssHello::from_binding_and_checkpoint(
            &self.binding,
            DirectWssResumeCheckpoint::from_durable_checkpoint(&self.checkpoint),
        );
        let hello_json = DirectWssControlMessage::Hello(hello)
            .to_json_text()
            .map_err(|_| DirectLocalWssSessionError::Protocol)?;
        match tokio::time::timeout_at(max_deadline, transport.send_text(hello_json)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(DirectLocalWssSessionError::Transport),
            Err(_) => return Ok(DirectLocalWssStopReason::MaxSessionDurationExceeded),
        }

        let mut ack_deadline: Option<tokio::time::Instant> = None;

        loop {
            tokio::select! {
                biased;
                _ = async {
                    let deadline = ack_deadline.expect("checked");
                    tokio::time::sleep_until(deadline).await
                }, if ack_deadline.is_some() => {
                    return Ok(DirectLocalWssStopReason::AckDeadlineExceeded);
                }
                _ = tokio::time::sleep_until(max_deadline) => {
                    return Ok(DirectLocalWssStopReason::MaxSessionDurationExceeded);
                }
                event = transport.next_event() => {
                    match event {
                        Ok(DirectWssTransportEvent::ConnectionEnded) => {
                            return Ok(DirectLocalWssStopReason::AcceptedIdle);
                        }
                        Ok(DirectWssTransportEvent::Message(msg)) => {
                            if let Some(stop) =
                                self.handle_ws_message(transport, msg, &mut ack_deadline).await?
                            {
                                return Ok(stop);
                            }
                        }
                        Err(_) => return Ok(DirectLocalWssStopReason::TransportError),
                    }
                }
            }
        }
    }

    async fn fail_protocol<T: DirectWssTransport>(
        transport: &T,
    ) -> Result<Option<DirectLocalWssStopReason>, DirectLocalWssSessionError> {
        let _ = transport.close(CloseCode::Protocol, "").await;
        Err(DirectLocalWssSessionError::Protocol)
    }

    async fn handle_ws_message<T: DirectWssTransport>(
        &mut self,
        transport: &T,
        msg: Message,
        ack_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<Option<DirectLocalWssStopReason>, DirectLocalWssSessionError> {
        match msg {
            Message::Text(text) => {
                let json = text.to_string();
                let control = match DirectWssControlMessage::parse_inbound_text(&json) {
                    Ok(c) => c,
                    Err(_) => return Self::fail_protocol(transport).await,
                };
                self.handle_control(transport, control, ack_deadline).await
            }
            Message::Binary(bytes) => {
                if !self.accepted {
                    return Self::fail_protocol(transport).await;
                }
                let stop = self
                    .handle_binary_segment(transport, &bytes, ack_deadline)
                    .await?;
                Ok(stop)
            }
            Message::Ping(payload) => {
                transport
                    .send_pong(payload)
                    .await
                    .map_err(|_| DirectLocalWssSessionError::Transport)?;
                Ok(None)
            }
            Message::Close(_) => {
                let _ = transport.close(CloseCode::Normal, "").await;
                Ok(Some(DirectLocalWssStopReason::GracefulClose))
            }
            Message::Pong(_) | Message::Frame(_) => Ok(None),
        }
    }

    async fn handle_control<T: DirectWssTransport>(
        &mut self,
        transport: &T,
        control: DirectWssControlMessage,
        ack_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<Option<DirectLocalWssStopReason>, DirectLocalWssSessionError> {
        if !self.accepted {
            match control {
                DirectWssControlMessage::SessionAccept(accept) => {
                    if !accept.matches_binding(&self.binding) {
                        return Self::fail_protocol(transport).await;
                    }
                    let expected = self.checkpoint.next_expected_segment_sequence();
                    if accept.next_expected_segment_sequence != expected {
                        return Self::fail_protocol(transport).await;
                    }
                    self.accepted = true;
                    self.next_expected_sequence = Some(accept.next_expected_segment_sequence);
                    return Ok(None);
                }
                DirectWssControlMessage::SessionReject(_) => {
                    let _ = transport.close(CloseCode::Normal, "").await;
                    return Ok(Some(DirectLocalWssStopReason::Rejected));
                }
                _ => return Self::fail_protocol(transport).await,
            }
        }

        match control {
            DirectWssControlMessage::SessionRevoke(_) => {
                let _ = transport.close(CloseCode::Normal, "").await;
                Ok(Some(DirectLocalWssStopReason::Revoked))
            }
            DirectWssControlMessage::SegmentOffer(offer) => {
                if self.fsm.state() == super::segment_fsm::DirectWssSegmentFsmState::AwaitingBinary
                {
                    return Self::fail_protocol(transport).await;
                }
                if self.fsm.on_segment_offer(&offer).is_err() {
                    return Self::fail_protocol(transport).await;
                }
                *ack_deadline = Some(tokio::time::Instant::now() + self.limits.ack_deadline);
                Ok(None)
            }
            _ => Self::fail_protocol(transport).await,
        }
    }

    async fn handle_binary_segment<T: DirectWssTransport>(
        &mut self,
        transport: &T,
        bytes: &[u8],
        ack_deadline: &mut Option<tokio::time::Instant>,
    ) -> Result<Option<DirectLocalWssStopReason>, DirectLocalWssSessionError> {
        let ready = match self.fsm.on_binary_frame(bytes) {
            Ok(r) => r,
            Err(_) => return Self::fail_protocol(transport).await,
        };
        let deadline = ack_deadline.ok_or(DirectLocalWssSessionError::Protocol)?;

        #[cfg(test)]
        if let Some(delay) = self.before_durability_delay {
            tokio::time::sleep(delay).await;
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(Some(DirectLocalWssStopReason::AckDeadlineExceeded));
        }

        let identity = match self.store.logical_identity_for_incoming(
            ready.segment_sequence,
            ready.byte_length,
            &ready.sha256_hex,
        ) {
            Ok(id) => id,
            Err(_) => return Self::fail_protocol(transport).await,
        };

        let mut pipeline =
            DirectSegmentDurabilityPipeline::try_new(&self.store, self.limits.clone())
                .map_err(|_| DirectLocalWssSessionError::InvalidConfig)?;
        #[cfg(test)]
        if let Some(boundary) = self.injected_durability_failure {
            pipeline = pipeline.with_injected_failure_at(boundary);
        }

        let prior = self.checkpoint.clone();
        match pipeline.persist_for_cumulative_ack(&identity, bytes, &prior) {
            Ok(outcome) if outcome.cumulative_ack_eligible => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok(Some(DirectLocalWssStopReason::AckDeadlineExceeded));
                }
                let durable = match DirectCumulativeCheckpoint::read_durable_from_store(&self.store)
                {
                    Ok(c) => c,
                    Err(_) => return Self::fail_protocol(transport).await,
                };
                self.checkpoint = durable.clone();
                self.next_expected_sequence = Some(durable.next_expected_segment_sequence());
                let ack = DirectWssControlMessage::cumulative_ack_from_checkpoint(&durable);
                let ack_json = ack
                    .to_json_text()
                    .map_err(|_| DirectLocalWssSessionError::Protocol)?;
                match tokio::time::timeout_at(deadline, transport.send_text(ack_json)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) => return Err(DirectLocalWssSessionError::Transport),
                    Err(_) => return Ok(Some(DirectLocalWssStopReason::AckDeadlineExceeded)),
                }
                *ack_deadline = None;
                self.fsm.take_ready();
                Ok(None)
            }
            Ok(_) => Ok(Some(DirectLocalWssStopReason::DurabilityError)),
            Err(_) => Ok(Some(DirectLocalWssStopReason::DurabilityError)),
        }
    }
}

impl From<DirectSessionBindingError> for DirectLocalWssSessionError {
    fn from(_: DirectSessionBindingError) -> DirectLocalWssSessionError {
        DirectLocalWssSessionError::InvalidConfig
    }
}
