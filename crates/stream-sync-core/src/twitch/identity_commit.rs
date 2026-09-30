//! Identity commit helpers for delegated takeover (`apply_exchange_session`) and
//! other identity entry points (`use_connection`, `remove_connection`, autostart).
//!
//! **In-lock commit:** the last successful intent recheck while holding `lifecycle_lock`
//! is the commit point; superseded applies roll back durable and live snapshots.
//!
//! **Post-lock fence:** after the lock drops, workers start only if intent, generation,
//! session, and lease still allow; otherwise return `Ok(())` and leave the winner.

use crate::app_state::{tokens_from_delegated_session, AppState};
use crate::config_types::{DelegatedSessionFile, TwitchActiveMode, TwitchTokenFile};
use crate::delegated_lifecycle::{AuthorityLease, DelegatedGeneration};
use crate::delegated_secrets::{
    assert_rollback_cas, capture_delegated_authority_epoch,
    capture_delegated_revoke_marker_snapshot, parse_committed_identity_from_metadata_bytes,
    restore_delegated_authority_epoch, with_delegated_authority_lock, DelegatedAuthorityEpoch,
    DelegatedCommittedIdentity,
};
use anyhow::{anyhow, Result};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;

use super::platform_workers::{start_platform_twitch_workers, transition_platform_twitch_workers};
use super::{
    clear_live_runtime_fields, pause_apply_durable_gate, start_delegated_refresh_loop,
    start_delegated_watch_loop, TwitchServices, WorkerOwner,
};

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
#[derive(Clone)]
pub struct DurableApplySnapshot {
    epoch: DelegatedAuthorityEpoch,
    expected_post_persist: Option<DelegatedCommittedIdentity>,
}

/// In-memory identity published by apply; restored if intent goes stale after awaits.
pub struct LiveApplySnapshot {
    generation: DelegatedGeneration,
    coordinator_generation: DelegatedGeneration,
    delegated: Option<DelegatedSessionFile>,
    mode: TwitchActiveMode,
    tokens: TwitchTokenFile,
    lease: AuthorityLease,
}

impl LiveApplySnapshot {
    pub async fn capture(state: &AppState, services: &TwitchServices) -> Self {
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
    pub fn capture(state: &AppState) -> Result<Self> {
        with_delegated_authority_lock(&state.paths.twitch_delegated, || {
            Ok(Self {
                epoch: capture_delegated_authority_epoch(
                    &state.paths.twitch_delegated,
                    &state.paths.twitch_active_mode,
                    &state.paths.twitch_delegated_revoked,
                    &state.paths.twitch_delegated_revoke_pending,
                    &state.paths.twitch_delegated_revoke_marker_hw,
                    state.secret_store().as_ref(),
                )?,
                expected_post_persist: None,
            })
        })
    }

    pub fn note_persist_completed(&mut self, identity: DelegatedCommittedIdentity) {
        self.expected_post_persist = Some(identity);
    }

    pub fn rollback(&self, state: &AppState) -> Result<()> {
        crate::delegated_secrets::authority_gates::pause_blocking(
            crate::delegated_secrets::authority_gates::DelegatedAuthorityBoundary::RollbackBeforeRestore,
        );
        let rollback_result = with_delegated_authority_lock(&state.paths.twitch_delegated, || {
            let current = if state.paths.twitch_delegated.is_file() {
                let raw = std::fs::read(&state.paths.twitch_delegated)?;
                parse_committed_identity_from_metadata_bytes(&raw)?
            } else {
                None
            };
            let current_markers = capture_delegated_revoke_marker_snapshot(
                &state.paths.twitch_delegated_revoked,
                &state.paths.twitch_delegated_revoke_pending,
                &state.paths.twitch_delegated_revoke_marker_hw,
            )?;
            assert_rollback_cas(
                current,
                self.expected_post_persist,
                &self.epoch,
                current_markers,
            )?;
            restore_delegated_authority_epoch(
                &state.paths.twitch_delegated,
                &state.paths.twitch_active_mode,
                &state.paths.twitch_delegated_revoked,
                &state.paths.twitch_delegated_revoke_pending,
                state.secret_store().as_ref(),
                &self.epoch,
            )
        });
        if let Err(err) = rollback_result {
            if err.to_string().contains("stale rollback refused") {
                return Err(err);
            }
            if let Err(marker_err) = crate::storage::write_identity_rollback_pending(
                &state.paths.twitch_tokens_rollback_pending,
            ) {
                return Err(anyhow!(
                    "delegated durable rollback failed ({err:#}); recovery marker also failed ({marker_err:#})"
                ));
            }
            return Err(err);
        }
        clear_apply_replacement_artifacts(state);
        Ok(())
    }
}

pub async fn rollback_superseded_apply(
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
    start_platform_twitch_workers(
        state,
        services,
        WorkerOwner::delegated(generation),
        apply_intent,
    )
    .await;
}

fn error_is_superseded_identity(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|e| e.to_string() == "superseded by newer identity action")
}

/// Disk snapshot for inactive personal-token removal.
pub(crate) struct PersonalTokensDurableSnapshot {
    personal_tokens: Option<Vec<u8>>,
}

impl PersonalTokensDurableSnapshot {
    pub(crate) fn capture(state: &AppState) -> Result<Self> {
        Ok(Self {
            personal_tokens: read_path_bytes_if_exists(&state.paths.twitch_tokens)?,
        })
    }

    pub(crate) fn rollback(&self, state: &AppState) -> Result<()> {
        restore_or_remove_path(
            &state.paths.twitch_tokens,
            self.personal_tokens.as_deref(),
            true,
        )
    }
}

/// In-memory personal live state cleared by personal disconnect mini-commit.
pub(crate) struct PersonalRemovalLiveSnapshot {
    personal_tokens: TwitchTokenFile,
    twitch_tokens: TwitchTokenFile,
}

impl PersonalRemovalLiveSnapshot {
    async fn capture(state: &AppState) -> Self {
        Self {
            personal_tokens: state.personal_tokens.read().await.clone(),
            twitch_tokens: state.twitch.read().await.tokens.clone(),
        }
    }

    async fn restore(self, state: &AppState) {
        *state.personal_tokens.write().await = self.personal_tokens;
        state.twitch.write().await.tokens = self.twitch_tokens;
    }
}

/// Outcome of a committed personal removal (disk + live personal gone).
pub(crate) struct PersonalRemovalCommitOutcome {
    pub previous_local_gen: u64,
}

/// Personal disconnect mini-commit: disk clear → intent current → live personal gone.
pub(crate) async fn commit_personal_removal(
    state: &AppState,
    services: &TwitchServices,
    intent: u64,
) -> Result<PersonalRemovalCommitOutcome> {
    let _lifecycle = services.lifecycle_lock.lock().await;
    recheck_apply_intent(services, Some(intent))?;

    let durable = PersonalTokensDurableSnapshot::capture(state)?;
    let live = PersonalRemovalLiveSnapshot::capture(state).await;
    let previous_local_gen = services.personal_token_generation_current();

    let commit = async {
        let previous = state.personal_tokens.read().await.clone();
        *state.personal_tokens.write().await = TwitchTokenFile::default();
        if let Err(e) = state.save_twitch_tokens().await {
            *state.personal_tokens.write().await = previous;
            return Err(e);
        }
        recheck_apply_intent(services, Some(intent))?;

        {
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile::default();
            clear_live_runtime_fields(&mut tw);
        }
        recheck_apply_intent(services, Some(intent))?;
        services.bump_personal_token_generation();
        Ok(PersonalRemovalCommitOutcome { previous_local_gen })
    }
    .await;

    match commit {
        Ok(outcome) => Ok(outcome),
        Err(e) if error_is_superseded_identity(&e) => {
            let _ = durable.rollback(state);
            live.restore(state).await;
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// After personal mini-commit, publish live identity none when delegated is not activated.
pub(crate) async fn finalize_live_identity_none_after_personal_removal(
    state: &AppState,
    services: &TwitchServices,
    intent: u64,
) -> Result<()> {
    let _lifecycle = services.lifecycle_lock.lock().await;
    services.ensure_apply_intent_current(intent)?;
    {
        let mut tw = state.twitch.write().await;
        tw.tokens = TwitchTokenFile::default();
        clear_live_runtime_fields(&mut tw);
    }
    *state.active_mode.write().await = TwitchActiveMode::Local;
    state.save_active_mode().await?;
    Ok(())
}

/// Publish a validated delegated session as the live identity under lifecycle lock.
pub(crate) async fn commit_delegated_activation(
    state: &AppState,
    services: &TwitchServices,
    intent: u64,
    validated: DelegatedSessionFile,
    saved: Option<&DelegatedSessionFile>,
) -> Result<DelegatedGeneration> {
    let _lifecycle = services.lifecycle_lock.lock().await;
    recheck_apply_intent(services, Some(intent))?;

    let mut durable_snapshot = DurableApplySnapshot::capture(state)?;
    let live_snapshot = LiveApplySnapshot::capture(state, services).await;

    let commit = async {
        if saved.is_none_or(|s| validated != *s) {
            pause_apply_durable_gate(ApplyDurableBoundary::BeforePersist).await;
            recheck_apply_intent(services, Some(intent))?;
            let identity = state.persist_delegated_session(&validated)?;
            durable_snapshot.note_persist_completed(identity);
            pause_apply_durable_gate(ApplyDurableBoundary::AfterPersist).await;
            recheck_apply_intent(services, Some(intent))?;
        }

        let previous_mode = *state.active_mode.read().await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        if let Err(e) = state.save_active_mode().await {
            *state.active_mode.write().await = previous_mode;
            return Err(e);
        }
        pause_apply_durable_gate(ApplyDurableBoundary::AfterModeWrite).await;
        recheck_apply_intent(services, Some(intent))?;

        services
            .renew_after_successful_remote_validation(
                validated.generation,
                validated.connection_expires_at.as_deref(),
            )
            .await
            .map_err(|e| anyhow!(e))?;
        recheck_apply_intent(services, Some(intent))?;

        pause_apply_durable_gate(ApplyDurableBoundary::BeforeLiveMemoryPublish).await;
        recheck_apply_intent(services, Some(intent))?;

        *state.delegated.write().await = Some(validated.clone());
        {
            let mut tw = state.twitch.write().await;
            clear_live_runtime_fields(&mut tw);
            tw.tokens = tokens_from_delegated_session(&validated);
        }
        recheck_apply_intent(services, Some(intent))?;
        Ok(validated.generation)
    }
    .await;

    match commit {
        Ok(generation) => Ok(generation),
        Err(e) if error_is_superseded_identity(&e) => {
            rollback_superseded_apply(&durable_snapshot, live_snapshot, state, services, e).await?;
            unreachable!()
        }
        Err(e) => Err(e),
    }
}

/// Post-lock going live for delegated activation (generation-fenced Twitch + refresh/watch).
pub(crate) async fn run_delegated_activation_post_commit(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    intent: u64,
) {
    let apply_intent = Some(intent);
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        return;
    }

    start_apply_exchange_twitch_clients(state.clone(), services.clone(), generation, apply_intent)
        .await;
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        return;
    }

    start_delegated_refresh_loop(state.clone(), services.clone(), generation).await;
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        return;
    }

    start_delegated_watch_loop(state.clone(), services.clone(), generation).await;
    if !apply_post_commit_workers_may_run(&state, &services, generation, apply_intent).await {
        return;
    }

    crate::kick::sync_live_identity_for_generation(state, generation, Some(&services)).await;
}

/// Intent-scoped personal IRC/EventSub start via PlatformWorkers.
pub(crate) async fn start_personal_twitch_clients(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    intent: u64,
    local_gen: u64,
    previous_mode: TwitchActiveMode,
    previous_delegated_generation: DelegatedGeneration,
) {
    let previous_owner =
        if previous_mode == TwitchActiveMode::Delegated && previous_delegated_generation > 0 {
            Some(WorkerOwner::delegated(previous_delegated_generation))
        } else {
            services
                .irc_client
                .read()
                .await
                .as_ref()
                .map(|b| b.owner)
                .filter(|owner| matches!(owner, WorkerOwner::Personal { .. }))
        };
    transition_platform_twitch_workers(
        state,
        services,
        previous_owner,
        WorkerOwner::personal(local_gen),
        Some(intent),
    )
    .await;
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
