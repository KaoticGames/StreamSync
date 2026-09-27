//! Finalized stem delivery worker (Syndicate voice v2). No legacy chunk endpoints.

use crate::app_state::{AppState, DiscordVoiceLastWrite};
use crate::voice_delivery::{
    DeliveryPhase, FinalizedVoiceDelivery, HttpVoiceV2Client, OrchestratorError, VoiceV2Client,
    VoiceV2ClientError,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const V2_POLL_LIMIT: u32 = 10;
pub const V2_DEFAULT_POLL_WAIT: Duration = Duration::from_secs(2);

/// Legacy chunk API paths — v2 must never reference these (static guard for review/tests).
pub async fn v2_poll_and_deliver_once(state: &AppState) -> Duration {
    let cfg = state.discord_voice_config.read().await.clone();
    let key = cfg
        .host_token
        .clone()
        .filter(|v| v.starts_with("sdk_"))
        .unwrap_or_default();
    if key.is_empty() {
        return V2_DEFAULT_POLL_WAIT;
    }
    let parent = cfg
        .recording_parent
        .clone()
        .filter(|v| !v.trim().is_empty());
    if parent.is_none() {
        set_last_error(
            state,
            "Set a recording folder to start Discord voice ingest.".into(),
        )
        .await;
        return V2_DEFAULT_POLL_WAIT;
    }
    let parent = parent.unwrap();
    let device_id = cfg.device_id.clone();
    let base = crate::syndicate_connection::api_base();
    let client = HttpVoiceV2Client::new(base, key);
    let orch = match FinalizedVoiceDelivery::open_recording_parent(std::path::Path::new(&parent)) {
        Ok(o) => o.with_device_id(device_id),
        Err(e) => {
            set_last_error(state, e.to_string()).await;
            return Duration::from_secs(2);
        }
    };
    let pending = match client.fetch_pending(V2_POLL_LIMIT) {
        Ok(list) => list,
        Err(VoiceV2ClientError::RetryAfter(d)) => return d.max(Duration::from_millis(200)),
        Err(e) => {
            set_last_error(state, e.to_string()).await;
            return Duration::from_secs(2);
        }
    };
    if pending.is_empty() {
        update_v2_runtime(state, &DeliveryPhase::Waiting, None).await;
        return V2_DEFAULT_POLL_WAIT;
    }
    for item in pending {
        let mut phase = DeliveryPhase::Waiting;
        match orch.deliver_pending(&client, &item, &mut phase) {
            Ok(()) => {
                clear_last_error(state).await;
                let published = format!(
                    "{}/syndicate-discord-voice/{}",
                    parent.trim_end_matches('/'),
                    item.manifest.guild_id
                );
                set_last_write(state, &published, 0).await;
                update_v2_runtime(state, &phase, Some(&published)).await;
            }
            Err(OrchestratorError::ClientRetryable(_)) => {
                return Duration::from_secs(2);
            }
            Err(e) => {
                warn!("voice v2 terminal delivery skipped: {e}");
                update_v2_runtime(state, &DeliveryPhase::Error(e.to_string()), None).await;
            }
        }
    }
    V2_DEFAULT_POLL_WAIT
}

async fn update_v2_runtime(state: &AppState, phase: &DeliveryPhase, published: Option<&str>) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.v2_phase = Some(format!("{phase:?}"));
    runtime.v2_last_published = published.map(|s| s.to_string());
}

async fn set_last_error(state: &AppState, msg: String) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_error = Some(msg);
}

async fn clear_last_error(state: &AppState) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_error = None;
}

async fn set_last_write(state: &AppState, path: &str, bytes: u64) {
    let mut runtime = state.discord_voice_runtime.write().await;
    runtime.last_write = Some(DiscordVoiceLastWrite {
        at: Some(chrono::Utc::now().to_rfc3339()),
        path: Some(path.to_string()),
        bytes: Some(bytes),
    });
}

#[cfg(test)]
mod legacy_endpoint_guard {
    #[test]
    fn v2_module_does_not_call_legacy_chunk_urls() {
        let src = include_str!("discord_voice_v2.rs");
        assert!(!src.contains("chunks/pending"));
        assert!(!src.contains("chunks/content"));
        assert!(!src.contains("chunks/ack"));
    }
}
