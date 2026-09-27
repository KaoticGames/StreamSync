//! Finalized stem delivery worker (Syndicate voice v2). No legacy chunk endpoints.

use crate::app_state::{AppState, DiscordVoiceLastWrite};
use crate::voice_delivery::{
    local_recovery::{sweep_local_recoveries, LocalRecoverySweepOutcome},
    DeliveryPhase, FinalizedVoiceDelivery, HttpVoiceV2Client, OrchestratorError, VoiceV2Client,
    VoiceV2ClientError,
};
use std::time::Duration;
use tracing::warn;

pub const V2_POLL_LIMIT: u32 = 10;
pub const V2_DEFAULT_POLL_WAIT: Duration = Duration::from_secs(2);
const V2_MIN_LOOP_WAIT: Duration = Duration::from_millis(200);

#[cfg(test)]
pub(crate) static V2_DELIVER_TICKS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub async fn v2_poll_and_deliver_once(state: &AppState) -> Duration {
    #[cfg(test)]
    V2_DELIVER_TICKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
            return V2_DEFAULT_POLL_WAIT;
        }
    };
    v2_poll_and_deliver_once_with_client(state, std::path::Path::new(&parent), &orch, &client).await
}

#[cfg(test)]
pub(crate) async fn v2_poll_and_deliver_once_with_mock(
    state: &AppState,
    client: &crate::voice_delivery::MockVoiceV2Client,
) -> Duration {
    V2_DELIVER_TICKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let cfg = state.discord_voice_config.read().await.clone();
    let parent = cfg
        .recording_parent
        .clone()
        .filter(|v| !v.trim().is_empty())
        .expect("recording parent");
    let orch = FinalizedVoiceDelivery::open_recording_parent(std::path::Path::new(&parent))
        .unwrap()
        .with_device_id(cfg.device_id);
    v2_poll_and_deliver_once_with_client(state, std::path::Path::new(&parent), &orch, client).await
}

async fn v2_poll_and_deliver_once_with_client<C: VoiceV2Client>(
    state: &AppState,
    parent: &std::path::Path,
    orch: &FinalizedVoiceDelivery,
    client: &C,
) -> Duration {
    let local = sweep_local_recoveries(parent, orch, client);
    let mut api_retry_after: Option<Duration> = None;
    let pending = match client.fetch_pending(V2_POLL_LIMIT) {
        Ok(list) => list,
        Err(VoiceV2ClientError::RetryAfter(d)) => {
            api_retry_after = Some(d);
            return combine_v2_worker_delay(local, api_retry_after);
        }
        Err(e) => {
            set_last_error(state, e.to_string()).await;
            api_retry_after = e.retry_after();
            return combine_v2_worker_delay(local, api_retry_after);
        }
    };
    if pending.is_empty() {
        update_v2_runtime(state, &DeliveryPhase::Waiting, None).await;
        return combine_v2_worker_delay(local, None);
    }
    for item in pending {
        let mut phase = DeliveryPhase::Waiting;
        match orch.deliver_pending(client, &item, &mut phase) {
            Ok(()) => {
                clear_last_error(state).await;
                let published = format!(
                    "{}/syndicate-discord-voice/{}",
                    parent.to_string_lossy().trim_end_matches('/'),
                    item.manifest.guild_id
                );
                set_last_write(state, &published, 0).await;
                update_v2_runtime(state, &phase, Some(&published)).await;
            }
            Err(OrchestratorError::ClientRetryable(e)) => {
                api_retry_after = e.retry_after().or(api_retry_after);
                return combine_v2_worker_delay(local, api_retry_after);
            }
            Err(e) => {
                warn!("voice v2 terminal delivery skipped: {e}");
                update_v2_runtime(state, &DeliveryPhase::Error(e.to_string()), None).await;
            }
        }
    }
    combine_v2_worker_delay(local, None)
}

pub(crate) fn combine_v2_worker_delay(
    local: LocalRecoverySweepOutcome,
    api_retry_after: Option<Duration>,
) -> Duration {
    let mut required = local.retry_after;
    if let Some(api) = api_retry_after {
        required = Some(required.map_or(api, |existing| existing.max(api)));
    }
    required
        .map(|wait| wait.max(V2_MIN_LOOP_WAIT))
        .unwrap_or_else(|| V2_DEFAULT_POLL_WAIT.max(V2_MIN_LOOP_WAIT))
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
mod v2_poll_tests {
    use super::*;
    use crate::voice_delivery::LocalRecoverySweepOutcome;

    #[test]
    fn combine_prefers_retry_after_over_default_poll_wait() {
        let local = LocalRecoverySweepOutcome {
            retry_after: Some(Duration::from_secs(17)),
            ..Default::default()
        };
        let wait = combine_v2_worker_delay(local, None);
        assert_eq!(wait, Duration::from_secs(17));
    }

    #[test]
    fn combine_uses_max_of_local_and_api_retry_after() {
        let local = LocalRecoverySweepOutcome {
            retry_after: Some(Duration::from_secs(9)),
            ..Default::default()
        };
        let wait = combine_v2_worker_delay(local, Some(Duration::from_secs(21)));
        assert_eq!(wait, Duration::from_secs(21));
    }
}
