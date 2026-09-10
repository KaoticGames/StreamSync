//! WebSocket fan-out with explicit audience classes for private event isolation.

use crate::dock_capability::DockCredentialStore;
use axum::extract::ws::Message;
use futures_util::{future::BoxFuture, SinkExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

pub type WsSender = futures_util::stream::SplitSink<axum::extract::ws::WebSocket, Message>;
const FANOUT_SEND_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FeedAudience {
    PublicOverlay,
    ReadOnlyDock,
    PrivateControlDock,
}

impl FeedAudience {
    /// Public feed query may select overlay or read-only dock audiences only.
    pub fn parse_public_query(raw: Option<&str>) -> Self {
        match raw
            .unwrap_or("overlay")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "dock" | "readonly-dock" => Self::ReadOnlyDock,
            _ => Self::PublicOverlay,
        }
    }
}

#[derive(Clone)]
struct PrivateFeedAuth {
    token: String,
    platform: String,
}

trait FanoutSender: Send + Sync {
    fn send_text(&self, payload: String) -> BoxFuture<'_, Result<(), ()>>;
    fn send_close(&self) -> BoxFuture<'_, Result<(), ()>>;
    fn matches_ws_sender(&self, target: &Arc<RwLock<WsSender>>) -> bool;
}

struct WsFanoutSender {
    sender: Arc<RwLock<WsSender>>,
}

impl FanoutSender for WsFanoutSender {
    fn send_text(&self, payload: String) -> BoxFuture<'_, Result<(), ()>> {
        let sender = self.sender.clone();
        Box::pin(async move {
            let mut guard = sender.write().await;
            guard.send(Message::Text(payload)).await.map_err(|_| ())
        })
    }

    fn send_close(&self) -> BoxFuture<'_, Result<(), ()>> {
        let sender = self.sender.clone();
        Box::pin(async move {
            let mut guard = sender.write().await;
            guard.send(Message::Close(None)).await.map_err(|_| ())
        })
    }

    fn matches_ws_sender(&self, target: &Arc<RwLock<WsSender>>) -> bool {
        Arc::ptr_eq(&self.sender, target)
    }
}

struct FeedClient {
    sender: Arc<dyn FanoutSender>,
    audience: FeedAudience,
    private_auth: Option<PrivateFeedAuth>,
}

#[derive(Clone)]
struct AudienceRecipient {
    sender: Arc<dyn FanoutSender>,
    audience: FeedAudience,
    private_auth: Option<PrivateFeedAuth>,
}

#[derive(Clone, Default)]
pub struct FeedHub {
    inner: Arc<RwLock<HashMap<String, Vec<FeedClient>>>>,
}

impl FeedHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(
        &self,
        profile_id: String,
        audience: FeedAudience,
        sender: Arc<RwLock<WsSender>>,
    ) {
        let mut map = self.inner.write().await;
        map.entry(profile_id).or_default().push(FeedClient {
            sender: Arc::new(WsFanoutSender { sender }),
            audience,
            private_auth: None,
        });
    }

    pub async fn unregister(&self, profile_id: &str, target: &Arc<RwLock<WsSender>>) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            clients.retain(|c| !c.sender.matches_ws_sender(target));
            if clients.is_empty() {
                map.remove(profile_id);
            }
        }
    }

    pub async fn client_count(&self, profile_id: &str) -> usize {
        self.inner
            .read()
            .await
            .get(profile_id)
            .map(|c| c.len())
            .unwrap_or(0)
    }

    pub async fn broadcast_profile(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(
            profile_id,
            &[
                FeedAudience::PublicOverlay,
                FeedAudience::ReadOnlyDock,
                FeedAudience::PrivateControlDock,
            ],
            event,
            None,
        )
        .await;
    }

    pub async fn set_client_audience(
        &self,
        profile_id: &str,
        target: &Arc<RwLock<WsSender>>,
        audience: FeedAudience,
    ) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            for client in clients.iter_mut() {
                if client.sender.matches_ws_sender(target) {
                    client.audience = audience;
                    if audience != FeedAudience::PrivateControlDock {
                        client.private_auth = None;
                    }
                }
            }
        }
    }

    pub async fn set_client_private_auth(
        &self,
        profile_id: &str,
        target: &Arc<RwLock<WsSender>>,
        token: &str,
        platform: &str,
    ) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            for client in clients.iter_mut() {
                if client.sender.matches_ws_sender(target) {
                    client.audience = FeedAudience::PrivateControlDock;
                    client.private_auth = Some(PrivateFeedAuth {
                        token: token.to_string(),
                        platform: platform.to_string(),
                    });
                }
            }
        }
    }

    pub async fn clear_client_private_auth(
        &self,
        profile_id: &str,
        target: &Arc<RwLock<WsSender>>,
    ) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            for client in clients.iter_mut() {
                if client.sender.matches_ws_sender(target) {
                    client.audience = FeedAudience::PublicOverlay;
                    client.private_auth = None;
                }
            }
        }
    }

    pub async fn broadcast_public_overlay(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(profile_id, &[FeedAudience::PublicOverlay], event, None)
            .await;
    }

    pub async fn broadcast_readonly_dock(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(profile_id, &[FeedAudience::ReadOnlyDock], event, None)
            .await;
    }

    /// Private dock delivery revalidates each subscriber's credential under the shared store lock
    /// before every payload so cross-process revocation cannot leak private input.
    pub async fn broadcast_private_dock(
        &self,
        profile_id: &str,
        event: &serde_json::Value,
        store: &DockCredentialStore,
    ) {
        self.broadcast_to_audiences(
            profile_id,
            &[FeedAudience::PrivateControlDock],
            event,
            Some(store),
        )
        .await;
    }

    pub async fn broadcast_all(&self, event: &serde_json::Value) {
        self.broadcast_all_while(event, || true).await;
    }

    /// Broadcast with a per-send gate. Returns false if the gate rejected mid-fanout.
    pub async fn broadcast_all_while<F>(&self, event: &serde_json::Value, mut gate: F) -> bool
    where
        F: FnMut() -> bool,
    {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return true,
        };
        let recipients: Vec<Arc<dyn FanoutSender>> = {
            let map = self.inner.read().await;
            let mut snapshot = Vec::new();
            for clients in map.values() {
                for client in clients {
                    snapshot.push(client.sender.clone());
                }
            }
            snapshot
        };
        let mut gate_failed = false;
        let mut fanout_futures = Vec::new();
        for sender in recipients {
            if !gate() {
                gate_failed = true;
                break;
            }
            let payload_for_sender = payload.clone();
            fanout_futures.push(async move {
                let ok = FeedHub::send_text_with_timeout(sender.clone(), payload_for_sender).await;
                (sender, ok)
            });
        }
        let mut failed = Vec::new();
        for (sender, ok) in futures_util::future::join_all(fanout_futures).await {
            if !ok {
                Self::push_unique_sender(&mut failed, sender);
            }
        }
        if !failed.is_empty() {
            self.prune_failed_senders_global(&failed).await;
        }
        !gate_failed
    }

    async fn broadcast_to_audiences(
        &self,
        profile_id: &str,
        audiences: &[FeedAudience],
        event: &serde_json::Value,
        private_store: Option<&DockCredentialStore>,
    ) {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let recipients: Vec<AudienceRecipient> = {
            let map = self.inner.read().await;
            let mut snapshot = Vec::new();
            if let Some(clients) = map.get(profile_id) {
                for client in clients {
                    if !audiences.contains(&client.audience) {
                        continue;
                    }
                    snapshot.push(AudienceRecipient {
                        sender: client.sender.clone(),
                        audience: client.audience,
                        private_auth: client.private_auth.clone(),
                    });
                }
            }
            snapshot
        };

        let mut revoked_senders: Vec<Arc<dyn FanoutSender>> = Vec::new();
        let mut fanout_senders: Vec<Arc<dyn FanoutSender>> = Vec::new();
        for recipient in recipients {
            if recipient.audience == FeedAudience::PrivateControlDock {
                let Some(store) = private_store else {
                    continue;
                };
                let Some(auth) = recipient.private_auth.as_ref() else {
                    Self::push_unique_sender(&mut revoked_senders, recipient.sender.clone());
                    continue;
                };
                if !store.authorize_chat_send(&auth.token, &auth.platform, profile_id) {
                    Self::push_unique_sender(&mut revoked_senders, recipient.sender.clone());
                    continue;
                }
            }
            fanout_senders.push(recipient.sender);
        }

        let failed_fanout = Self::send_payload_concurrently(fanout_senders, &payload).await;
        if !failed_fanout.is_empty() {
            self.prune_profile_senders(profile_id, &failed_fanout).await;
        }

        if !revoked_senders.is_empty() {
            let failed_close =
                Self::close_senders_concurrently(revoked_senders.iter().cloned().collect()).await;
            self.clear_private_auth_for_senders(profile_id, &revoked_senders)
                .await;
            if !failed_close.is_empty() {
                self.prune_profile_senders(profile_id, &failed_close).await;
            }
        }
    }

    fn push_unique_sender(list: &mut Vec<Arc<dyn FanoutSender>>, sender: Arc<dyn FanoutSender>) {
        if !list.iter().any(|existing| Arc::ptr_eq(existing, &sender)) {
            list.push(sender);
        }
    }

    async fn send_text_with_timeout(sender: Arc<dyn FanoutSender>, payload: String) -> bool {
        match tokio::time::timeout(FANOUT_SEND_TIMEOUT, sender.send_text(payload)).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) | Err(_) => false,
        }
    }

    async fn close_with_timeout(sender: Arc<dyn FanoutSender>) -> bool {
        match tokio::time::timeout(FANOUT_SEND_TIMEOUT, sender.send_close()).await {
            Ok(Ok(())) => true,
            Ok(Err(_)) | Err(_) => false,
        }
    }

    async fn send_payload_concurrently(
        senders: Vec<Arc<dyn FanoutSender>>,
        payload: &str,
    ) -> Vec<Arc<dyn FanoutSender>> {
        let mut fanout_futures = Vec::new();
        for sender in senders {
            let payload_for_sender = payload.to_string();
            fanout_futures.push(async move {
                let ok = FeedHub::send_text_with_timeout(sender.clone(), payload_for_sender).await;
                (sender, ok)
            });
        }
        let mut failed = Vec::new();
        for (sender, ok) in futures_util::future::join_all(fanout_futures).await {
            if !ok {
                Self::push_unique_sender(&mut failed, sender);
            }
        }
        failed
    }

    async fn close_senders_concurrently(
        senders: Vec<Arc<dyn FanoutSender>>,
    ) -> Vec<Arc<dyn FanoutSender>> {
        let mut close_futures = Vec::new();
        for sender in senders {
            close_futures.push(async move {
                let ok = FeedHub::close_with_timeout(sender.clone()).await;
                (sender, ok)
            });
        }
        let mut failed = Vec::new();
        for (sender, ok) in futures_util::future::join_all(close_futures).await {
            if !ok {
                Self::push_unique_sender(&mut failed, sender);
            }
        }
        failed
    }

    async fn clear_private_auth_for_senders(
        &self,
        profile_id: &str,
        senders: &[Arc<dyn FanoutSender>],
    ) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            for client in clients.iter_mut() {
                if senders
                    .iter()
                    .any(|target| Arc::ptr_eq(&client.sender, target))
                {
                    client.audience = FeedAudience::PublicOverlay;
                    client.private_auth = None;
                }
            }
        }
    }

    async fn prune_profile_senders(&self, profile_id: &str, senders: &[Arc<dyn FanoutSender>]) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            clients.retain(|client| {
                !senders
                    .iter()
                    .any(|failed| Arc::ptr_eq(&client.sender, failed))
            });
            if clients.is_empty() {
                map.remove(profile_id);
            }
        }
    }

    async fn prune_failed_senders_global(&self, senders: &[Arc<dyn FanoutSender>]) {
        let mut map = self.inner.write().await;
        map.retain(|_, clients| {
            clients.retain(|client| {
                !senders
                    .iter()
                    .any(|failed| Arc::ptr_eq(&client.sender, failed))
            });
            !clients.is_empty()
        });
    }
}

pub fn make_dock_event(
    event_type: &str,
    detail: &str,
    label: Option<&str>,
    redemption: Option<serde_json::Value>,
) -> serde_json::Value {
    make_platform_dock_event("twitch", event_type, detail, label, redemption)
}

pub fn make_platform_dock_event(
    platform: &str,
    event_type: &str,
    detail: &str,
    label: Option<&str>,
    redemption: Option<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "dock-event",
        "id": uuid::Uuid::new_v4().to_string(),
        "ts": chrono::Utc::now().timestamp_millis(),
        "platform": platform,
        "eventType": event_type,
        "label": label.unwrap_or(event_type),
        "detail": detail,
        "redemption": redemption,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{mpsc, oneshot, Barrier};

    enum MockBehavior {
        Healthy {
            delivered: mpsc::UnboundedSender<String>,
        },
        Error,
        Hang {
            entered: Arc<Barrier>,
        },
    }

    struct MockSender {
        behavior: MockBehavior,
        close_events: Option<mpsc::UnboundedSender<()>>,
        send_attempts: AtomicUsize,
        close_attempts: AtomicUsize,
    }

    impl MockSender {
        fn healthy() -> (Arc<Self>, mpsc::UnboundedReceiver<String>) {
            let (tx, rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    behavior: MockBehavior::Healthy { delivered: tx },
                    close_events: None,
                    send_attempts: AtomicUsize::new(0),
                    close_attempts: AtomicUsize::new(0),
                }),
                rx,
            )
        }

        fn healthy_with_close() -> (
            Arc<Self>,
            mpsc::UnboundedReceiver<String>,
            mpsc::UnboundedReceiver<()>,
        ) {
            let (delivered_tx, delivered_rx) = mpsc::unbounded_channel();
            let (close_tx, close_rx) = mpsc::unbounded_channel();
            (
                Arc::new(Self {
                    behavior: MockBehavior::Healthy {
                        delivered: delivered_tx,
                    },
                    close_events: Some(close_tx),
                    send_attempts: AtomicUsize::new(0),
                    close_attempts: AtomicUsize::new(0),
                }),
                delivered_rx,
                close_rx,
            )
        }

        fn error() -> Arc<Self> {
            Arc::new(Self {
                behavior: MockBehavior::Error,
                close_events: None,
                send_attempts: AtomicUsize::new(0),
                close_attempts: AtomicUsize::new(0),
            })
        }

        fn hanging(entered: Arc<Barrier>) -> Arc<Self> {
            Arc::new(Self {
                behavior: MockBehavior::Hang { entered },
                close_events: None,
                send_attempts: AtomicUsize::new(0),
                close_attempts: AtomicUsize::new(0),
            })
        }

        fn send_attempts(&self) -> usize {
            self.send_attempts.load(Ordering::SeqCst)
        }

        fn close_attempts(&self) -> usize {
            self.close_attempts.load(Ordering::SeqCst)
        }
    }

    impl FanoutSender for MockSender {
        fn send_text(&self, payload: String) -> BoxFuture<'_, Result<(), ()>> {
            self.send_attempts.fetch_add(1, Ordering::SeqCst);
            match &self.behavior {
                MockBehavior::Healthy { delivered } => {
                    let delivered = delivered.clone();
                    Box::pin(async move { delivered.send(payload).map_err(|_| ()) })
                }
                MockBehavior::Error => Box::pin(async move { Err(()) }),
                MockBehavior::Hang { entered } => {
                    let entered = entered.clone();
                    Box::pin(async move {
                        entered.wait().await;
                        pending::<Result<(), ()>>().await
                    })
                }
            }
        }

        fn send_close(&self) -> BoxFuture<'_, Result<(), ()>> {
            self.close_attempts.fetch_add(1, Ordering::SeqCst);
            let close_events = self.close_events.clone();
            Box::pin(async move {
                if let Some(tx) = close_events {
                    let _ = tx.send(());
                }
                Ok(())
            })
        }

        fn matches_ws_sender(&self, _target: &Arc<RwLock<WsSender>>) -> bool {
            false
        }
    }

    async fn register_mock_client(
        hub: &FeedHub,
        profile_id: &str,
        audience: FeedAudience,
        sender: Arc<MockSender>,
        private_auth: Option<(&str, &str)>,
    ) {
        let mut map = hub.inner.write().await;
        map.entry(profile_id.to_string())
            .or_default()
            .push(FeedClient {
                sender: sender as Arc<dyn FanoutSender>,
                audience,
                private_auth: private_auth.map(|(token, platform)| PrivateFeedAuth {
                    token: token.to_string(),
                    platform: platform.to_string(),
                }),
            });
    }

    async fn unregister_mock_client(hub: &FeedHub, profile_id: &str, sender: &Arc<MockSender>) {
        let target: Arc<dyn FanoutSender> = sender.clone();
        let mut map = hub.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            clients.retain(|client| !Arc::ptr_eq(&client.sender, &target));
            if clients.is_empty() {
                map.remove(profile_id);
            }
        }
    }

    async fn recv_message_soon(rx: &mut mpsc::UnboundedReceiver<String>) -> String {
        match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
            Ok(Some(msg)) => msg,
            _ => panic!("expected message"),
        }
    }

    async fn recv_close_soon(rx: &mut mpsc::UnboundedReceiver<()>) {
        match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
            Ok(Some(())) => {}
            _ => panic!("expected close event"),
        }
    }

    async fn oneshot_done_soon(rx: oneshot::Receiver<()>) {
        match tokio::time::timeout(Duration::from_millis(250), rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => panic!("oneshot closed before completion"),
            Err(_) => panic!("oneshot did not complete"),
        }
    }

    #[tokio::test]
    async fn fanout_slow_client_does_not_block_healthy_delivery() {
        let hub = FeedHub::new();
        let profile = "chat-default";
        let entered = Arc::new(Barrier::new(2));
        let slow = MockSender::hanging(entered.clone());
        let (healthy, mut healthy_rx) = MockSender::healthy();
        register_mock_client(&hub, profile, FeedAudience::PublicOverlay, slow, None).await;
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PublicOverlay,
            healthy.clone(),
            None,
        )
        .await;

        let event = serde_json::json!({ "type": "fanout-test", "n": 1 });
        let hub_for_task = hub.clone();
        let broadcast = tokio::spawn(async move {
            hub_for_task.broadcast_profile(profile, &event).await;
        });

        entered.wait().await;
        let delivered = recv_message_soon(&mut healthy_rx).await;
        assert!(delivered.contains("\"fanout-test\""));

        tokio::time::timeout(Duration::from_millis(400), broadcast)
            .await
            .expect("broadcast should complete after timeout")
            .expect("broadcast task");
        assert_eq!(hub.client_count(profile).await, 1);
    }

    #[tokio::test]
    async fn fanout_send_in_flight_does_not_block_register_or_unregister() {
        let hub = FeedHub::new();
        let profile = "chat-default";
        let entered = Arc::new(Barrier::new(2));
        let blocked = MockSender::hanging(entered.clone());
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PublicOverlay,
            blocked.clone(),
            None,
        )
        .await;

        let event = serde_json::json!({ "type": "fanout-test", "n": 2 });
        let hub_for_task = hub.clone();
        let broadcast = tokio::spawn(async move {
            hub_for_task.broadcast_profile(profile, &event).await;
        });

        entered.wait().await;
        let (new_sender, _) = MockSender::healthy();
        let (register_done_tx, register_done_rx) = oneshot::channel();
        let hub_for_register = hub.clone();
        let new_sender_for_register = new_sender.clone();
        tokio::spawn(async move {
            register_mock_client(
                &hub_for_register,
                profile,
                FeedAudience::ReadOnlyDock,
                new_sender_for_register,
                None,
            )
            .await;
            let _ = register_done_tx.send(());
        });

        let (unregister_done_tx, unregister_done_rx) = oneshot::channel();
        let hub_for_unregister = hub.clone();
        let blocked_for_unregister = blocked.clone();
        tokio::spawn(async move {
            unregister_mock_client(&hub_for_unregister, profile, &blocked_for_unregister).await;
            let _ = unregister_done_tx.send(());
        });

        oneshot_done_soon(register_done_rx).await;
        oneshot_done_soon(unregister_done_rx).await;
        assert_eq!(hub.client_count(profile).await, 1);

        tokio::time::timeout(Duration::from_millis(400), broadcast)
            .await
            .expect("broadcast should complete after timeout")
            .expect("broadcast task");
    }

    #[tokio::test]
    async fn fanout_timeout_or_send_error_prunes_client() {
        let hub = FeedHub::new();
        let profile = "chat-default";
        let entered = Arc::new(Barrier::new(2));
        let slow = MockSender::hanging(entered.clone());
        let error = MockSender::error();
        let (healthy, mut healthy_rx) = MockSender::healthy();
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PublicOverlay,
            slow.clone(),
            None,
        )
        .await;
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PublicOverlay,
            error.clone(),
            None,
        )
        .await;
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PublicOverlay,
            healthy.clone(),
            None,
        )
        .await;

        let event1 = serde_json::json!({ "type": "fanout-test", "n": 3 });
        let hub_for_task = hub.clone();
        let first = tokio::spawn(async move {
            hub_for_task.broadcast_profile(profile, &event1).await;
        });
        entered.wait().await;
        let _ = recv_message_soon(&mut healthy_rx).await;
        tokio::time::timeout(Duration::from_millis(400), first)
            .await
            .expect("first broadcast should complete")
            .expect("first broadcast task");

        assert_eq!(slow.send_attempts(), 1);
        assert_eq!(error.send_attempts(), 1);
        assert_eq!(hub.client_count(profile).await, 1);

        let event2 = serde_json::json!({ "type": "fanout-test", "n": 4 });
        hub.broadcast_profile(profile, &event2).await;
        let _ = recv_message_soon(&mut healthy_rx).await;

        assert_eq!(slow.send_attempts(), 1);
        assert_eq!(error.send_attempts(), 1);
    }

    #[tokio::test]
    async fn broadcast_all_while_gate_failure_stops_work_but_client_timeout_does_not_trip_gate() {
        let hub_gate = FeedHub::new();
        let profile_gate = "gate";
        let (a, _) = MockSender::healthy();
        let (b, _) = MockSender::healthy();
        let (c, _) = MockSender::healthy();
        register_mock_client(
            &hub_gate,
            profile_gate,
            FeedAudience::PublicOverlay,
            a.clone(),
            None,
        )
        .await;
        register_mock_client(
            &hub_gate,
            profile_gate,
            FeedAudience::PublicOverlay,
            b.clone(),
            None,
        )
        .await;
        register_mock_client(
            &hub_gate,
            profile_gate,
            FeedAudience::PublicOverlay,
            c.clone(),
            None,
        )
        .await;

        let mut checks = 0usize;
        let gate_event = serde_json::json!({ "type": "fanout-test", "n": 5 });
        let gate_ok = hub_gate
            .broadcast_all_while(&gate_event, || {
                checks += 1;
                checks <= 1
            })
            .await;
        assert!(!gate_ok);
        let gate_attempts = a.send_attempts() + b.send_attempts() + c.send_attempts();
        assert_eq!(gate_attempts, 1);

        let hub_timeout = FeedHub::new();
        let profile_timeout = "timeout";
        let entered = Arc::new(Barrier::new(2));
        let slow = MockSender::hanging(entered.clone());
        let (healthy, mut healthy_rx) = MockSender::healthy();
        register_mock_client(
            &hub_timeout,
            profile_timeout,
            FeedAudience::PublicOverlay,
            slow.clone(),
            None,
        )
        .await;
        register_mock_client(
            &hub_timeout,
            profile_timeout,
            FeedAudience::ReadOnlyDock,
            healthy.clone(),
            None,
        )
        .await;

        let timeout_event = serde_json::json!({ "type": "fanout-test", "n": 6 });
        let hub_for_task = hub_timeout.clone();
        let send = tokio::spawn(async move {
            hub_for_task
                .broadcast_all_while(&timeout_event, || true)
                .await
        });
        entered.wait().await;
        let _ = recv_message_soon(&mut healthy_rx).await;
        let timeout_ok = tokio::time::timeout(Duration::from_millis(400), send)
            .await
            .expect("timeout-case broadcast should complete")
            .expect("timeout broadcast task");
        assert!(timeout_ok);
        assert_eq!(hub_timeout.client_count(profile_timeout).await, 1);
    }

    #[tokio::test]
    async fn private_dock_revalidation_semantics_preserved_with_prune() {
        let hub = FeedHub::new();
        let profile = "chat-default";
        let store = DockCredentialStore::empty_in_memory();
        let allowed = store
            .issue("twitch", profile)
            .expect("issue allowed credential");
        let revoked = store
            .issue("twitch", profile)
            .expect("issue revoked credential");
        let timeout = store
            .issue("twitch", profile)
            .expect("issue timeout credential");
        store
            .revoke(&revoked.token)
            .expect("revoke credential should succeed");

        let (allowed_sender, mut allowed_rx) = MockSender::healthy();
        let (revoked_sender, mut revoked_rx, mut revoked_close_rx) =
            MockSender::healthy_with_close();
        let entered = Arc::new(Barrier::new(2));
        let timeout_sender = MockSender::hanging(entered.clone());

        register_mock_client(
            &hub,
            profile,
            FeedAudience::PrivateControlDock,
            allowed_sender.clone(),
            Some((&allowed.token, "twitch")),
        )
        .await;
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PrivateControlDock,
            revoked_sender.clone(),
            Some((&revoked.token, "twitch")),
        )
        .await;
        register_mock_client(
            &hub,
            profile,
            FeedAudience::PrivateControlDock,
            timeout_sender.clone(),
            Some((&timeout.token, "twitch")),
        )
        .await;

        let first_event = serde_json::json!({ "type": "private-fanout", "n": 1 });
        let first = hub.broadcast_private_dock(profile, &first_event, &store);
        let observe = async {
            entered.wait().await;
            let delivered = recv_message_soon(&mut allowed_rx).await;
            assert!(delivered.contains("\"private-fanout\""));
            recv_close_soon(&mut revoked_close_rx).await;
            assert!(
                revoked_rx.try_recv().is_err(),
                "revoked client must not receive payload"
            );
        };
        tokio::time::timeout(Duration::from_millis(400), async {
            tokio::join!(first, observe);
        })
        .await
        .expect("first private broadcast should complete");

        assert_eq!(timeout_sender.send_attempts(), 1);
        assert_eq!(hub.client_count(profile).await, 2);
        assert_eq!(revoked_sender.close_attempts(), 1);

        {
            let map = hub.inner.read().await;
            let clients = map.get(profile).expect("profile still present");
            let revoked_target: Arc<dyn FanoutSender> = revoked_sender.clone();
            let revoked_client = clients
                .iter()
                .find(|client| Arc::ptr_eq(&client.sender, &revoked_target))
                .expect("revoked sender remains connected but downgraded");
            assert_eq!(revoked_client.audience, FeedAudience::PublicOverlay);
            assert!(revoked_client.private_auth.is_none());
        }

        let second_event = serde_json::json!({ "type": "private-fanout", "n": 2 });
        hub.broadcast_private_dock(profile, &second_event, &store)
            .await;
        let _ = recv_message_soon(&mut allowed_rx).await;

        assert_eq!(timeout_sender.send_attempts(), 1);
        assert_eq!(revoked_sender.close_attempts(), 1);
        assert!(
            revoked_rx.try_recv().is_err(),
            "revoked sender stays out of private audience"
        );
    }
}
