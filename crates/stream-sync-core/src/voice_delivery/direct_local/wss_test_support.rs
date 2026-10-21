//! In-memory WSS transport and protocol fixtures for vertical-2 RED/GREEN tests.

use crate::voice_delivery::direct_local::test_support::{
    minimal_synj_segment_bytes, sha256_lower_hex, valid_direct_receiver_limits, CAPABILITY_TOKEN,
    DEVICE_ID, DISCORD_INSTANCE_ID, GUILD_ID, SESSION_ID,
};
use crate::voice_delivery::direct_local::wss::protocol::{
    DirectWssControlMessage, DirectWssHello, DirectWssResumeCheckpoint, DirectWssSegmentOffer,
    DirectWssSessionAccept, DIRECT_WSS_PROTOCOL_VERSION,
};
use crate::voice_delivery::direct_local::wss::session::{
    DirectLocalWssDarkSession, DirectLocalWssRunConfig,
};
use crate::voice_delivery::direct_local::wss::transport::{
    DirectWssTransport, DirectWssTransportError, DirectWssTransportEvent,
};
use crate::voice_delivery::direct_local::{
    DirectCumulativeCheckpoint, DirectDurabilityBoundary, DirectSessionBinding,
};
use crate::voice_delivery::direct_local::{DirectGenerationStore, DirectReceiverLimits};
use crate::voice_delivery::fs::DestRoot;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

pub fn parsed_binding() -> DirectSessionBinding {
    DirectSessionBinding::from_json(
        &crate::voice_delivery::direct_local::test_support::valid_binding_json(),
    )
    .expect("fixture binding")
}

pub fn hello_from_checkpoint(checkpoint: &DirectCumulativeCheckpoint) -> DirectWssHello {
    DirectWssHello::from_binding_and_checkpoint(
        &parsed_binding(),
        DirectWssResumeCheckpoint::from_durable_checkpoint(checkpoint),
    )
}

pub fn segment_offer_for_sequence(sequence: u64, bytes: &[u8]) -> DirectWssSegmentOffer {
    DirectWssSegmentOffer {
        protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
        session_id: SESSION_ID.to_string(),
        guild_id: GUILD_ID.to_string(),
        device_id: DEVICE_ID.to_string(),
        delivery_generation: 1,
        discord_instance_id: DISCORD_INSTANCE_ID.to_string(),
        leader_fence_generation: 1,
        segment_sequence: sequence,
        byte_length: bytes.len() as u64,
        sha256_hex: sha256_lower_hex(bytes),
    }
}

pub fn session_accept_for_binding(
    binding: &DirectSessionBinding,
    next_sequence: u64,
) -> DirectWssSessionAccept {
    DirectWssSessionAccept::echo_binding(binding, next_sequence)
}

pub fn open_test_store(root: &DestRoot) -> DirectGenerationStore {
    DirectGenerationStore::open_for_session(root, SESSION_ID, 1).expect("generation store")
}

pub fn session_config(
    store: DirectGenerationStore,
    checkpoint: DirectCumulativeCheckpoint,
) -> DirectLocalWssRunConfig {
    session_config_with_limits(store, checkpoint, valid_direct_receiver_limits())
}

pub fn session_config_with_limits(
    store: DirectGenerationStore,
    checkpoint: DirectCumulativeCheckpoint,
    limits: DirectReceiverLimits,
) -> DirectLocalWssRunConfig {
    DirectLocalWssRunConfig {
        binding: parsed_binding(),
        local_device_id: DEVICE_ID.to_string(),
        limits,
        store,
        resume_checkpoint: checkpoint,
        test_injected_durability_failure: None,
        test_before_durability_delay: None,
    }
}

pub fn session_config_with_failure(
    store: DirectGenerationStore,
    checkpoint: DirectCumulativeCheckpoint,
    boundary: DirectDurabilityBoundary,
) -> DirectLocalWssRunConfig {
    let mut config = session_config(store, checkpoint);
    config.test_injected_durability_failure = Some(boundary);
    config
}

pub fn session_config_with_durability_delay(
    store: DirectGenerationStore,
    checkpoint: DirectCumulativeCheckpoint,
    delay: std::time::Duration,
    mut limits: DirectReceiverLimits,
) -> DirectLocalWssRunConfig {
    limits.ack_deadline = std::time::Duration::from_millis(20);
    let mut config = session_config_with_limits(store, checkpoint, limits);
    config.test_before_durability_delay = Some(delay);
    config
}

/// Scripted peer: enqueue server events before `run_until_stop`.
#[derive(Default)]
pub struct MockDirectWssTransport {
    scripted: Arc<Mutex<VecDeque<DirectWssTransportEvent>>>,
    sent_text: Arc<Mutex<Vec<String>>>,
    sent_binary: Arc<Mutex<Vec<Vec<u8>>>>,
    sent_pongs: Arc<Mutex<Vec<Vec<u8>>>>,
    closed: Arc<Mutex<bool>>,
    pending_connect: bool,
    pending_when_empty: bool,
}

impl MockDirectWssTransport {
    pub fn accepting_script(script: Vec<DirectWssTransportEvent>) -> Self {
        Self {
            scripted: Arc::new(Mutex::new(script.into())),
            sent_text: Arc::new(Mutex::new(Vec::new())),
            sent_binary: Arc::new(Mutex::new(Vec::new())),
            sent_pongs: Arc::new(Mutex::new(Vec::new())),
            closed: Arc::new(Mutex::new(false)),
            pending_connect: false,
            pending_when_empty: false,
        }
    }

    pub fn accepting_pending_script(script: Vec<DirectWssTransportEvent>) -> Self {
        Self {
            pending_when_empty: true,
            ..Self::accepting_script(script)
        }
    }

    pub fn pending_connect() -> Self {
        Self {
            pending_connect: true,
            ..Self::accepting_script(Vec::new())
        }
    }

    pub fn push(&self, event: DirectWssTransportEvent) {
        self.scripted.lock().expect("lock").push_back(event);
    }

    pub fn drain_sent_text(&self) -> Vec<String> {
        self.sent_text.lock().expect("lock").clone()
    }

    pub fn drain_sent_binary(&self) -> Vec<Vec<u8>> {
        self.sent_binary.lock().expect("lock").clone()
    }

    pub fn was_closed(&self) -> bool {
        *self.closed.lock().expect("lock")
    }

    pub fn drain_sent_pongs(&self) -> Vec<Vec<u8>> {
        self.sent_pongs.lock().expect("lock").clone()
    }

    pub fn cumulative_acks_sent(&self) -> Vec<DirectWssControlMessage> {
        self.drain_sent_text()
            .into_iter()
            .filter_map(|t| DirectWssControlMessage::parse_inbound_text(&t).ok())
            .filter(|m| m.is_cumulative_ack())
            .collect()
    }
}

impl DirectWssTransport for MockDirectWssTransport {
    fn connect(
        &self,
        _request: http::Request<()>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let pending_connect = self.pending_connect;
        async move {
            if pending_connect {
                std::future::pending().await
            } else {
                Ok(())
            }
        }
    }

    fn send_text(
        &self,
        payload: String,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let sent = self.sent_text.clone();
        async move {
            sent.lock().expect("lock").push(payload);
            Ok(())
        }
    }

    fn send_pong(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let sent = self.sent_pongs.clone();
        async move {
            sent.lock().expect("lock").push(payload);
            Ok(())
        }
    }

    fn next_event(
        &self,
    ) -> impl std::future::Future<Output = Result<DirectWssTransportEvent, DirectWssTransportError>> + Send
    {
        let scripted = self.scripted.clone();
        let pending_when_empty = self.pending_when_empty;
        async move {
            let event = scripted.lock().expect("lock").pop_front();
            if let Some(ev) = event {
                Ok(ev)
            } else if pending_when_empty {
                std::future::pending().await
            } else {
                Ok(DirectWssTransportEvent::ConnectionEnded)
            }
        }
    }

    fn close(
        &self,
        _code: CloseCode,
        _reason: &str,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let closed = self.closed.clone();
        async move {
            *closed.lock().expect("lock") = true;
            Ok(())
        }
    }
}

pub fn ws_ping_event() -> DirectWssTransportEvent {
    DirectWssTransportEvent::Message(Message::Ping(vec![1, 2, 3]))
}

pub fn ws_close_event() -> DirectWssTransportEvent {
    DirectWssTransportEvent::Message(Message::Close(None))
}

pub fn text_control(json: &str) -> DirectWssTransportEvent {
    DirectWssTransportEvent::Message(Message::Text(json.to_string()))
}

pub fn binary_segment(bytes: &[u8]) -> DirectWssTransportEvent {
    DirectWssTransportEvent::Message(Message::Binary(bytes.to_vec()))
}

pub fn default_segment_bytes() -> Vec<u8> {
    minimal_synj_segment_bytes()
}

pub async fn run_session(
    config: DirectLocalWssRunConfig,
    transport: &MockDirectWssTransport,
) -> crate::voice_delivery::direct_local::wss::session::DirectLocalWssStopReason {
    let mut session = DirectLocalWssDarkSession::try_new(config).expect("session");
    session.run_until_stop(transport).await.expect("run")
}

pub fn offer_json(offer: &DirectWssSegmentOffer) -> String {
    offer.to_json_text().expect("offer json")
}

pub fn accept_json(accept: &DirectWssSessionAccept) -> String {
    accept.to_json_text().expect("accept json")
}

pub fn assert_no_capability_leak(text: &str) {
    assert!(
        !text.contains(CAPABILITY_TOKEN),
        "capability token must not appear in diagnostics"
    );
    assert!(
        !text.to_lowercase().contains("bearer"),
        "bearer scheme must not appear in redacted diagnostics"
    );
}
