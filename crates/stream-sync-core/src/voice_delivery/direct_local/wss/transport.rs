//! Transport seam for direct WSS (production uses tokio-tungstenite; tests use mocks).

use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

type DirectWssWebSocketStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectWssTransportError {
    ConnectFailed,
    SendFailed,
    ReadFailed,
    Closed,
}

impl std::fmt::Display for DirectWssTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectFailed => write!(f, "direct wss transport connect failed"),
            Self::SendFailed => write!(f, "direct wss transport send failed"),
            Self::ReadFailed => write!(f, "direct wss transport read failed"),
            Self::Closed => write!(f, "direct wss transport closed"),
        }
    }
}

impl std::error::Error for DirectWssTransportError {}

#[derive(Debug, Clone)]
pub enum DirectWssTransportEvent {
    Message(Message),
    ConnectionEnded,
}

pub trait DirectWssTransport {
    fn connect(
        &self,
        request: http::Request<()>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send;

    fn send_text(
        &self,
        payload: String,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send;

    fn send_pong(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send;

    fn next_event(
        &self,
    ) -> impl std::future::Future<Output = Result<DirectWssTransportEvent, DirectWssTransportError>> + Send;

    fn close(
        &self,
        code: CloseCode,
        reason: &str,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send;
}

/// Production WSS transport (library-only; not wired to app orchestration).
pub struct TokioTungsteniteDirectWssTransport {
    stream: Arc<Mutex<Option<DirectWssWebSocketStream>>>,
}

impl TokioTungsteniteDirectWssTransport {
    pub fn new() -> Self {
        Self {
            stream: Arc::new(Mutex::new(None)),
        }
    }
}

impl Default for TokioTungsteniteDirectWssTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectWssTransport for TokioTungsteniteDirectWssTransport {
    fn connect(
        &self,
        request: http::Request<()>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let stream_slot = self.stream.clone();
        async move {
            match connect_async(request).await {
                Ok((ws, _response)) => {
                    *stream_slot.lock().await = Some(ws);
                    Ok(())
                }
                Err(_) => Err(DirectWssTransportError::ConnectFailed),
            }
        }
    }

    fn send_text(
        &self,
        payload: String,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let stream_slot = self.stream.clone();
        async move {
            let mut guard = stream_slot.lock().await;
            let ws = guard.as_mut().ok_or(DirectWssTransportError::Closed)?;
            ws.send(Message::Text(payload))
                .await
                .map_err(|_| DirectWssTransportError::SendFailed)?;
            Ok(())
        }
    }

    fn send_pong(
        &self,
        payload: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let stream_slot = self.stream.clone();
        async move {
            let mut guard = stream_slot.lock().await;
            let ws = guard.as_mut().ok_or(DirectWssTransportError::Closed)?;
            ws.send(Message::Pong(payload))
                .await
                .map_err(|_| DirectWssTransportError::SendFailed)?;
            Ok(())
        }
    }

    fn next_event(
        &self,
    ) -> impl std::future::Future<Output = Result<DirectWssTransportEvent, DirectWssTransportError>> + Send
    {
        let stream_slot = self.stream.clone();
        async move {
            let mut guard = stream_slot.lock().await;
            let ws = guard.as_mut().ok_or(DirectWssTransportError::Closed)?;
            match ws.next().await {
                Some(Ok(msg)) => Ok(DirectWssTransportEvent::Message(msg)),
                Some(Err(_)) => Err(DirectWssTransportError::ReadFailed),
                None => Ok(DirectWssTransportEvent::ConnectionEnded),
            }
        }
    }

    fn close(
        &self,
        code: CloseCode,
        reason: &str,
    ) -> impl std::future::Future<Output = Result<(), DirectWssTransportError>> + Send {
        let stream_slot = self.stream.clone();
        let reason = reason.to_string();
        async move {
            let mut guard = stream_slot.lock().await;
            let ws = guard.as_mut().ok_or(DirectWssTransportError::Closed)?;
            ws.send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.into(),
            })))
            .await
            .map_err(|_| DirectWssTransportError::SendFailed)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod compile_tests {
    use super::*;

    #[test]
    fn tokio_tungstenite_transport_impls_trait() {
        fn assert_transport<T: DirectWssTransport>() {}
        assert_transport::<TokioTungsteniteDirectWssTransport>();
    }
}
