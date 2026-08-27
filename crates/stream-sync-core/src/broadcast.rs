//! WebSocket fan-out with explicit audience classes for private event isolation.

use crate::dock_capability::DockCredentialStore;
use axum::extract::ws::Message;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type WsSender = futures_util::stream::SplitSink<axum::extract::ws::WebSocket, Message>;

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

struct PrivateFeedAuth {
    token: String,
    platform: String,
}

struct FeedClient {
    sender: Arc<RwLock<WsSender>>,
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
            sender,
            audience,
            private_auth: None,
        });
    }

    pub async fn unregister(&self, profile_id: &str, target: &Arc<RwLock<WsSender>>) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            clients.retain(|c| !Arc::ptr_eq(&c.sender, target));
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
                if Arc::ptr_eq(&client.sender, target) {
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
                if Arc::ptr_eq(&client.sender, target) {
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
                if Arc::ptr_eq(&client.sender, target) {
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
        let map = self.inner.read().await;
        for clients in map.values() {
            for client in clients {
                if !gate() {
                    return false;
                }
                let acquired = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    client.sender.write(),
                )
                .await;
                let Ok(mut guard) = acquired else {
                    return false;
                };
                if !gate() {
                    return false;
                }
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    guard.send(Message::Text(payload.clone())),
                )
                .await;
            }
        }
        true
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
        let mut revoked_senders: Vec<Arc<RwLock<WsSender>>> = Vec::new();
        {
            let map = self.inner.read().await;
            if let Some(clients) = map.get(profile_id) {
                for client in clients {
                    if !audiences.contains(&client.audience) {
                        continue;
                    }
                    if client.audience == FeedAudience::PrivateControlDock {
                        let Some(store) = private_store else {
                            continue;
                        };
                        let Some(auth) = client.private_auth.as_ref() else {
                            revoked_senders.push(client.sender.clone());
                            continue;
                        };
                        if !store.authorize_chat_send(&auth.token, &auth.platform, profile_id) {
                            revoked_senders.push(client.sender.clone());
                            continue;
                        }
                    }
                    let mut guard = client.sender.write().await;
                    let _ = guard.send(Message::Text(payload.clone())).await;
                }
            }
        }
        for sender in revoked_senders {
            {
                let mut guard = sender.write().await;
                let _ = guard.send(Message::Close(None)).await;
            }
            self.clear_client_private_auth(profile_id, &sender).await;
        }
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
