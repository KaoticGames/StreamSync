//! WebSocket fan-out (port of broadcastEvent / broadcastEventToAll).

use axum::extract::ws::Message;
use futures_util::SinkExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type WsSender = futures_util::stream::SplitSink<axum::extract::ws::WebSocket, Message>;

#[derive(Clone, Default)]
pub struct FeedHub {
    inner: Arc<RwLock<HashMap<String, Vec<Arc<RwLock<WsSender>>>>>>,
}

impl FeedHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register(&self, profile_id: String, sender: Arc<RwLock<WsSender>>) {
        let mut map = self.inner.write().await;
        map.entry(profile_id).or_default().push(sender);
    }

    pub async fn unregister(&self, profile_id: &str, target: &Arc<RwLock<WsSender>>) {
        let mut map = self.inner.write().await;
        if let Some(clients) = map.get_mut(profile_id) {
            clients.retain(|c| !Arc::ptr_eq(c, target));
            if clients.is_empty() {
                map.remove(profile_id);
            }
        }
    }

    pub async fn broadcast_profile(&self, profile_id: &str, event: &serde_json::Value) {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let map = self.inner.read().await;
        if let Some(clients) = map.get(profile_id) {
            for client in clients {
                let mut guard = client.write().await;
                let _ = guard.send(Message::Text(payload.clone().into())).await;
            }
        }
    }

    pub async fn broadcast_all(&self, event: &serde_json::Value) {
        let payload = match serde_json::to_string(event) {
            Ok(s) => s,
            Err(_) => return,
        };
        let map = self.inner.read().await;
        for (_profile, clients) in map.iter() {
            for client in clients {
                let mut guard = client.write().await;
                let _ = guard.send(Message::Text(payload.clone().into())).await;
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
