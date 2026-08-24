//! WebSocket fan-out with explicit audience classes for private event isolation.

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

struct FeedClient {
    sender: Arc<RwLock<WsSender>>,
    audience: FeedAudience,
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
        map.entry(profile_id)
            .or_default()
            .push(FeedClient { sender, audience });
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
                }
            }
        }
    }

    pub async fn broadcast_public_overlay(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(profile_id, &[FeedAudience::PublicOverlay], event)
            .await;
    }

    pub async fn broadcast_readonly_dock(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(profile_id, &[FeedAudience::ReadOnlyDock], event)
            .await;
    }

    pub async fn broadcast_private_dock(&self, profile_id: &str, event: &serde_json::Value) {
        self.broadcast_to_audiences(profile_id, &[FeedAudience::PrivateControlDock], event)
            .await;
    }

    pub async fn broadcast_all(&self, event: &serde_json::Value) {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let map = self.inner.read().await;
        for clients in map.values() {
            for client in clients {
                let mut guard = client.sender.write().await;
                let _ = guard.send(Message::Text(payload.clone().into())).await;
            }
        }
    }

    async fn broadcast_to_audiences(
        &self,
        profile_id: &str,
        audiences: &[FeedAudience],
        event: &serde_json::Value,
    ) {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let map = self.inner.read().await;
        if let Some(clients) = map.get(profile_id) {
            for client in clients {
                if audiences.contains(&client.audience) {
                    let mut guard = client.sender.write().await;
                    let _ = guard.send(Message::Text(payload.clone().into())).await;
                }
            }
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
