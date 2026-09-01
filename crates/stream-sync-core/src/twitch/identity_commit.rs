//! Identity commit helpers for delegated takeover (`apply_exchange_session`).
//!
//! **In-lock commit:** the last successful intent recheck while holding `lifecycle_lock`
//! is the commit point; superseded applies roll back durable and live snapshots.
//!
//! **Post-lock fence:** after the lock drops, workers start only if intent, generation,
//! session, and lease still allow; otherwise return `Ok(())` and leave the winner.

use crate::app_state::AppState;
use crate::config_types::{DelegatedSessionFile, TwitchActiveMode, TwitchTokenFile};
use crate::delegated_lifecycle::{AuthorityLease, DelegatedGeneration};
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;

use super::{pause_apply_durable_gate, start_eventsub, start_irc, TwitchServices};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ApplyDurableBoundary {
    BeforePersist,
    AfterPersist,
    AfterModeWrite,
    AfterTombstoneClear,
    BeforeLivePublish,
    BeforeLiveMemoryPublish,
    BeforeWorkerStart,
    BeforeTwitchStart,
}

/// Disk artifacts this apply may mutate; restored if a newer identity intent wins.
pub(crate) struct DurableApplySnapshot {
    delegated: Option<Vec<u8>>,
    active_mode: Option<Vec<u8>>,
    tombstone: bool,
    pending: bool,
}

/// In-memory identity published by apply; restored if intent goes stale after awaits.
pub(crate) struct LiveApplySnapshot {
    generation: DelegatedGeneration,
    coordinator_generation: DelegatedGeneration,
    delegated: Option<DelegatedSessionFile>,
    mode: TwitchActiveMode,
    tokens: TwitchTokenFile,
    lease: AuthorityLease,
}

impl LiveApplySnapshot {
    pub(crate) async fn capture(state: &AppState, services: &TwitchServices) -> Self {
        Self {
            generation: state.current_delegated_generation(),
            coordinator_generation: services.teardown_coordinator.active_generation(),
            delegated: state.delegated.read().await.clone(),
            mode: *state.active_mode.read().await,
            tokens: state.twitch.read().await.tokens.clone(),
            lease: services.authority_lease.lock().await.clone(),
        }
    }

    pub(crate) async fn restore(self, state: &AppState, services: &TwitchServices) {
        state
            .delegated_generation
            .store(self.generation, Ordering::SeqCst);
        *state.delegated.write().await = self.delegated;
        *state.active_mode.write().await = self.mode;
        state.twitch.write().await.tokens = self.tokens;
        *services.authority_lease.lock().await = self.lease;
        if let Err(err) = services
            .teardown_coordinator
            .restore_active_generation_for_rollback(self.coordinator_generation)
        {
            warn!("coordinator rollback after superseded apply failed: {err}");
        }
    }
}

pub(crate) fn recheck_apply_intent(
    services: &TwitchServices,
    apply_intent: Option<u64>,
) -> Result<()> {
    if let Some(intent) = apply_intent {
        services.ensure_apply_intent_current(intent)?;
    }
    Ok(())
}

fn clear_apply_replacement_artifacts(state: &AppState) {
    for path in [
        state.paths.twitch_delegated.with_extension("bak"),
        crate::storage::delegated_replace_pending_path(&state.paths.twitch_delegated),
        crate::storage::delegated_committing_path(&state.paths.twitch_delegated),
    ] {
        if let Err(err) = crate::storage::remove_file_durable(&path) {
            warn!(
                "superseded apply artifact cleanup failed for {}: {err:#}",
                path.display()
            );
        }
    }
}

impl DurableApplySnapshot {
    pub(crate) fn capture(state: &AppState) -> Result<Self> {
        Ok(Self {
            delegated: read_path_bytes_if_exists(&state.paths.twitch_delegated)?,
            active_mode: read_path_bytes_if_exists(&state.paths.twitch_active_mode)?,
            tombstone: state.paths.twitch_delegated_revoked.is_file(),
            pending: state.paths.twitch_delegated_revoke_pending.is_file(),
        })
    }

    fn rollback(&self, state: &AppState) -> Result<()> {
        let mut errors = Vec::new();
        if let Err(err) = restore_or_remove_path(
            &state.paths.twitch_delegated,
            self.delegated.as_deref(),
            true,
        ) {
            errors.push(format!("delegated credential rollback failed: {err:#}"));
        }
        if let Err(err) = restore_or_remove_path(
            &state.paths.twitch_active_mode,
            self.active_mode.as_deref(),
            false,
        ) {
            errors.push(format!("active mode rollback failed: {err:#}"));
        }
        if let Err(err) = restore_marker_file(
            &state.paths.twitch_delegated_revoked,
            self.tombstone,
            crate::storage::write_delegated_revoked_tombstone,
        ) {
            errors.push(format!("revoked tombstone rollback failed: {err:#}"));
        }
        if let Err(err) = restore_marker_file(
            &state.paths.twitch_delegated_revoke_pending,
            self.pending,
            crate::storage::write_delegated_revoke_pending,
        ) {
            errors.push(format!("revoke pending rollback failed: {err:#}"));
        }
        clear_apply_replacement_artifacts(state);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(errors.join("; ")))
        }
    }
}

pub(crate) async fn rollback_superseded_apply(
    durable_snapshot: &DurableApplySnapshot,
    live_snapshot: LiveApplySnapshot,
    state: &AppState,
    services: &TwitchServices,
    err: anyhow::Error,
) -> Result<()> {
    let durable_err = durable_snapshot.rollback(state).err();
    live_snapshot.restore(state, services).await;
    match durable_err {
        Some(rollback_err) => Err(anyhow!(
            "{err:#}; durable rollback also failed ({rollback_err:#})"
        )),
        None => Err(err),
    }
}

pub(crate) async fn apply_post_commit_workers_may_run(
    state: &AppState,
    services: &TwitchServices,
    generation: DelegatedGeneration,
    apply_intent: Option<u64>,
) -> bool {
    if recheck_apply_intent(services, apply_intent).is_err() {
        return false;
    }
    if state.current_delegated_generation() != generation {
        return false;
    }
    if !state.session_still_current(generation).await {
        return false;
    }
    if services.authority_lease_expired().await {
        return false;
    }
    let lease = services.read_authority_lease().await;
    lease.allows_platform_operations(generation)
}

pub(crate) async fn stop_stale_apply_generation_workers(
    services: &TwitchServices,
    generation: DelegatedGeneration,
) {
    {
        let mut guard = services.refresh_handle.write().await;
        if guard.as_ref().is_some_and(|t| t.generation == generation) {
            if let Some(task) = guard.take() {
                task.handle.abort();
            }
        }
    }
    {
        let mut guard = services.watch_handle.write().await;
        if guard.as_ref().is_some_and(|t| t.generation == generation) {
            if let Some(task) = guard.take() {
                task.handle.abort();
            }
        }
    }
}

pub(crate) async fn start_apply_exchange_twitch_clients(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    apply_intent: Option<u64>,
) {
    pause_apply_durable_gate(ApplyDurableBoundary::BeforeTwitchStart).await;
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        return;
    }
    if let Err(e) = start_irc(state.clone(), services.clone(), Some(generation)).await {
        warn!("IRC start failed: {e}");
    }
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        stop_stale_apply_generation_workers(&services, generation).await;
        return;
    }
    if let Err(e) = start_eventsub(state.clone(), services.clone(), Some(generation)).await {
        warn!("EventSub start failed: {e}");
    }
}

fn read_path_bytes_if_exists(path: &std::path::Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn restore_or_remove_path(
    path: &std::path::Path,
    bytes: Option<&[u8]>,
    authority_secret: bool,
) -> Result<()> {
    match bytes {
        Some(bytes) if authority_secret => {
            crate::storage::write_authority_bearing_secret(path, bytes)?;
            Ok(())
        }
        Some(bytes) => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, bytes)?;
            crate::storage::sync_parent_dir(path)?;
            Ok(())
        }
        None => crate::storage::remove_file_durable(path),
    }
}

fn restore_marker_file(
    path: &std::path::Path,
    should_exist: bool,
    write: impl FnOnce(&std::path::Path) -> Result<()>,
) -> Result<()> {
    if should_exist {
        write(path)
    } else {
        crate::storage::remove_file_durable(path)
    }
}
