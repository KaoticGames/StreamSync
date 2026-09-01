//! Generation- and personal-scoped Twitch platform worker ownership (IRC/EventSub).

use crate::app_state::AppState;
use crate::config_types::TwitchActiveMode;
use crate::delegated_lifecycle::DelegatedGeneration;
use std::sync::Arc;
use tracing::warn;

use super::{
    apply_post_commit_workers_may_run, recheck_apply_intent, stop_stale_apply_generation_workers,
    TwitchServices,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerOwner {
    Delegated { generation: DelegatedGeneration },
    Personal { local_gen: u64 },
}

impl WorkerOwner {
    pub(crate) fn delegated(generation: DelegatedGeneration) -> Self {
        Self::Delegated { generation }
    }

    pub(crate) fn personal(local_gen: u64) -> Self {
        Self::Personal { local_gen }
    }
}

pub(crate) async fn current_live_worker_owner(
    state: &AppState,
    services: &TwitchServices,
) -> Option<WorkerOwner> {
    match *state.active_mode.read().await {
        TwitchActiveMode::Local => {
            let local_gen = services.personal_token_generation_current();
            (local_gen > 0).then(|| WorkerOwner::personal(local_gen))
        }
        TwitchActiveMode::Delegated => {
            let generation = state.current_delegated_generation();
            (generation > 0).then(|| WorkerOwner::delegated(generation))
        }
    }
}

pub(crate) async fn platform_workers_may_start(
    state: &AppState,
    services: &TwitchServices,
    owner: WorkerOwner,
    apply_intent: Option<u64>,
) -> bool {
    if recheck_apply_intent(services, apply_intent).is_err() {
        return false;
    }
    match owner {
        WorkerOwner::Delegated { generation } => {
            apply_post_commit_workers_may_run(state, services, generation, apply_intent).await
        }
        WorkerOwner::Personal { local_gen } => {
            if !services.personal_token_generation_still_current(local_gen) {
                return false;
            }
            *state.active_mode.read().await == TwitchActiveMode::Local
        }
    }
}

pub(crate) async fn stop_platform_twitch_workers_for_owner(
    services: &TwitchServices,
    owner: WorkerOwner,
) {
    let stop_irc = services
        .irc_client
        .read()
        .await
        .as_ref()
        .is_some_and(|b| b.owner == owner);
    if stop_irc {
        *services.irc_client.write().await = None;
        if let Some(h) = services.irc_handle.write().await.take() {
            h.abort();
        }
    }
    let stop_eventsub = services
        .eventsub_owner
        .read()
        .await
        .is_some_and(|o| o == owner);
    if stop_eventsub {
        if let Some(h) = services.eventsub_handle.write().await.take() {
            h.abort();
        }
        *services.eventsub_owner.write().await = None;
    }
}

pub(crate) async fn stop_platform_twitch_workers_for_delegated_generation(
    services: &TwitchServices,
    generation: DelegatedGeneration,
) {
    stop_platform_twitch_workers_for_owner(services, WorkerOwner::delegated(generation)).await;
}

pub(crate) async fn start_platform_twitch_workers(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    owner: WorkerOwner,
    apply_intent: Option<u64>,
) {
    if !platform_workers_may_start(&state, &services, owner, apply_intent).await {
        return;
    }
    if let Err(e) = super::start_irc(state.clone(), services.clone(), owner).await {
        warn!("IRC start failed: {e}");
    }
    if !platform_workers_may_start(&state, &services, owner, apply_intent).await {
        if let WorkerOwner::Delegated { generation } = owner {
            stop_stale_apply_generation_workers(&services, generation).await;
        }
        return;
    }
    if let Err(e) = super::start_eventsub(state.clone(), services.clone(), owner).await {
        warn!("EventSub start failed: {e}");
    }
}

/// Stop handles owned by `previous`, then start `new_owner` when still live.
pub(crate) async fn transition_platform_twitch_workers(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    previous_owner: Option<WorkerOwner>,
    new_owner: WorkerOwner,
    apply_intent: Option<u64>,
) {
    if let Some(previous) = previous_owner {
        if previous != new_owner {
            stop_platform_twitch_workers_for_owner(&services, previous).await;
            if let WorkerOwner::Delegated { generation } = previous {
                stop_stale_apply_generation_workers(&services, generation).await;
            }
        }
    }
    if !platform_workers_may_start(&state, &services, new_owner, apply_intent).await {
        return;
    }
    start_platform_twitch_workers(state, services, new_owner, apply_intent).await;
}
