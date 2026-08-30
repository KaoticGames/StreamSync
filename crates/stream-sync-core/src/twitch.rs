//! Twitch OAuth, Helix, IRC chat, EventSub (port of overlay-server/server.js Twitch stack).

use crate::app_state::{tokens_from_delegated_session, AppState};
use crate::broadcast::{make_dock_event, FeedHub};
use crate::config_types::{
    DelegatedSessionFile, TwitchActiveMode, TwitchActiveModeFile, TwitchTokenFile,
};
use crate::delegated_lifecycle::{
    append_sse_chunk, clear_finished_generation_task, connection_key_authorization,
    connection_key_events_url, generation_task_alive, install_generation_task, parse_sse_json_data,
    redact_connection_key, release_generation_slot_if_owned, stop_delegated_worker_handles,
    AuthorityLease, AuthorityLeasePhase, AuthorityLeaseSnapshot, DelegatedGeneration,
    DelegatedWorker, GenerationTask, SseBufferError, TeardownCoordinator,
    MAX_DELEGATED_REVOCATION_DELAY, SYNDICATE_HTTP_TIMEOUT, SYNDICATE_SSE_READ_TIMEOUT,
};
use crate::syndicate_connection::{self, SyndicateApiError};
use anyhow::{anyhow, Result};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Value};
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::sync::Mutex as RefreshGateMutex;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio_tungstenite::connect_async;
use tracing::{info, warn};
use twitch_irc::login::StaticLoginCredentials;
use twitch_irc::{ClientConfig, SecureTCPTransport, TwitchIRCClient};

type StreamSyncIrcClient = TwitchIRCClient<SecureTCPTransport, StaticLoginCredentials>;

#[derive(Clone)]
struct IrcClientBundle {
    client: StreamSyncIrcClient,
    provenance: PlatformCredentialProvenance,
}

/// Provenance of platform credentials captured at selection time (B5).
/// A Delegated capture must never be downgraded to an unfenced Local bypass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlatformCredentialProvenance {
    Local,
    Delegated { snap: AuthorityLeaseSnapshot },
}

/// Atomically selected platform credentials with provenance (B2).
#[derive(Debug, Clone)]
pub enum PlatformCredentialSelection {
    Local {
        client_id: String,
        access_token: String,
    },
    Delegated {
        snap: AuthorityLeaseSnapshot,
        client_id: String,
        access_token: String,
    },
}

impl PlatformCredentialSelection {
    pub fn provenance(&self) -> PlatformCredentialProvenance {
        match self {
            Self::Local { .. } => PlatformCredentialProvenance::Local,
            Self::Delegated { snap, .. } => PlatformCredentialProvenance::Delegated { snap: *snap },
        }
    }
}

static BADGE_TTL: Duration = Duration::from_secs(300);
static EMOTE_TTL: Duration = Duration::from_secs(300);
struct CacheEntry<T> {
    value: T,
    user_id: String,
    fetched_at: std::time::Instant,
}

pub struct TwitchServices {
    badge_cache: RwLock<Option<CacheEntry<Value>>>,
    emote_cache: RwLock<Option<CacheEntry<Vec<Value>>>>,
    irc_client: RwLock<Option<IrcClientBundle>>,
    irc_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    eventsub_handle: RwLock<Option<tokio::task::JoinHandle<()>>>,
    refresh_handle: RwLock<Option<GenerationTask>>,
    watch_handle: RwLock<Option<GenerationTask>>,
    pub teardown_coordinator: TeardownCoordinator,
    authority_lease: Mutex<AuthorityLease>,
    /// Monotonic apply-request fence; newer requests supersede older exchange completions.
    apply_intent: AtomicU64,
    /// Serializes apply / durable revoke / refresh-apply / mode transitions.
    lifecycle_lock: Mutex<()>,
    teardown_tx: OnceLock<mpsc::UnboundedSender<TeardownRequest>>,
    durable_revoke_tx: OnceLock<mpsc::UnboundedSender<DurableRevokeRequest>>,
}

struct TeardownRequest {
    generation: DelegatedGeneration,
    reason: String,
    state: Arc<AppState>,
    reply: oneshot::Sender<Result<(), String>>,
}

struct DurableRevokeRequest {
    generation: DelegatedGeneration,
    reason: String,
    state: Arc<AppState>,
}

impl TwitchServices {
    pub fn new() -> Self {
        Self {
            badge_cache: RwLock::new(None),
            emote_cache: RwLock::new(None),
            irc_client: RwLock::new(None),
            irc_handle: RwLock::new(None),
            eventsub_handle: RwLock::new(None),
            refresh_handle: RwLock::new(None),
            watch_handle: RwLock::new(None),
            teardown_coordinator: TeardownCoordinator::new(),
            authority_lease: Mutex::new(AuthorityLease::inactive()),
            apply_intent: AtomicU64::new(0),
            lifecycle_lock: Mutex::new(()),
            teardown_tx: OnceLock::new(),
            durable_revoke_tx: OnceLock::new(),
        }
    }

    /// Advance the monotonic identity-intent fence (newest user intent wins).
    pub fn bump_apply_intent(&self) -> u64 {
        self.apply_intent.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Current identity-intent sequence (tests / diagnostics).
    pub fn apply_intent_for_test(&self) -> u64 {
        self.apply_intent.load(Ordering::SeqCst)
    }

    /// Newest identity intent wins; stale operations must not commit after awaits.
    pub fn ensure_apply_intent_current(&self, intent: u64) -> Result<()> {
        if self.apply_intent.load(Ordering::SeqCst) != intent {
            return Err(anyhow!("superseded by newer identity action"));
        }
        Ok(())
    }

    pub async fn lock_lifecycle(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lifecycle_lock.lock().await
    }

    pub async fn read_authority_lease(&self) -> tokio::sync::MutexGuard<'_, AuthorityLease> {
        self.authority_lease.lock().await
    }

    /// Spawn the external teardown coordinator worker (must run outside watch/refresh tasks).
    pub fn init_teardown_worker(self: &Arc<Self>) {
        if self.teardown_tx.get().is_some() {
            return;
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _ = self.teardown_tx.set(tx);
        let services = self.clone();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let result = services
                    .teardown_coordinator
                    .run_or_join(req.generation, || async {
                        execute_delegated_teardown(
                            req.state.clone(),
                            services.clone(),
                            req.generation,
                            &req.reason,
                        )
                        .await
                    })
                    .await;
                let _ = req.reply.send(result);
            }
        });
    }

    /// Spawn the autonomous durable-revoke retry worker (independent of delegated refresh/watch).
    pub fn init_durable_revoke_worker(self: &Arc<Self>) {
        if self.durable_revoke_tx.get().is_some() {
            return;
        }
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _ = self.durable_revoke_tx.set(tx);
        let services = self.clone();
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                run_autonomous_durable_revoke(&services, req.state, req.generation, &req.reason)
                    .await;
            }
        });
    }

    fn schedule_durable_revoke(
        self: &Arc<Self>,
        state: Arc<AppState>,
        generation: DelegatedGeneration,
        reason: &str,
    ) {
        if let Err(e) = state.mark_durable_revoke_pending() {
            warn!("failed to persist durable-revoke pending marker: {e:#}");
        }
        self.init_durable_revoke_worker();
        let Some(tx) = self.durable_revoke_tx.get() else {
            return;
        };
        let _ = tx.send(DurableRevokeRequest {
            generation,
            reason: reason.to_string(),
            state,
        });
    }

    /// Test hook: schedule durable revoke recovery (including generation 0 marker cleanup).
    pub fn schedule_durable_revoke_for_test(
        self: &Arc<Self>,
        state: Arc<AppState>,
        generation: DelegatedGeneration,
        reason: &str,
    ) {
        self.schedule_durable_revoke(state, generation, reason);
    }

    pub async fn install_delegated_generation(&self, generation: DelegatedGeneration) {
        let _ = self
            .teardown_coordinator
            .install_generation_async(generation)
            .await;
    }

    pub async fn signal_delegated_teardown(
        self: &Arc<Self>,
        state: Arc<AppState>,
        generation: DelegatedGeneration,
        reason: &str,
    ) -> Result<(), String> {
        let result = if let Some(tx) = self.teardown_tx.get() {
            let (reply_tx, reply_rx) = oneshot::channel();
            if tx
                .send(TeardownRequest {
                    generation,
                    reason: reason.to_string(),
                    state: state.clone(),
                    reply: reply_tx,
                })
                .is_err()
            {
                self.teardown_coordinator
                    .run_or_join(generation, || {
                        let state = state.clone();
                        let services = self.clone();
                        let reason = reason.to_string();
                        async move {
                            execute_delegated_teardown(state, services, generation, &reason).await
                        }
                    })
                    .await
            } else {
                reply_rx
                    .await
                    .unwrap_or_else(|_| Err("teardown coordinator reply closed".into()))
            }
        } else {
            self.teardown_coordinator
                .run_or_join(generation, || {
                    let state = state.clone();
                    let services = self.clone();
                    let reason = reason.to_string();
                    async move {
                        execute_delegated_teardown(state, services, generation, &reason).await
                    }
                })
                .await
        };
        if result.is_err() && durable_revoke_still_needed(&state) {
            self.schedule_durable_revoke(state, generation, reason);
        }
        result
    }

    /// Renew lease only after successful remote validation for `generation`.
    async fn renew_after_successful_remote_validation(
        &self,
        generation: DelegatedGeneration,
        connection_expires_at: Option<&str>,
    ) -> Result<(), String> {
        let mut lease = self.authority_lease.lock().await;
        lease.renew_after_successful_remote_validation(generation, connection_expires_at)
    }

    /// Install a freshly validated lease for a new/replacement session.
    pub async fn install_validated_authority_lease(
        &self,
        generation: DelegatedGeneration,
        connection_expires_at: Option<&str>,
    ) {
        let mut lease = self.authority_lease.lock().await;
        lease.install_validated_generation(generation, connection_expires_at);
    }

    /// Cap lease by connection expiry without extending (local data never renews).
    async fn cap_authority_lease_by_connection_expiry(&self, connection_expires_at: Option<&str>) {
        let mut lease = self.authority_lease.lock().await;
        lease.cap_by_connection_expiry(connection_expires_at);
    }

    async fn install_pending_authority_lease(&self, generation: DelegatedGeneration) {
        let mut lease = self.authority_lease.lock().await;
        *lease = AuthorityLease::pending_remote_validation(generation);
    }

    async fn authority_lease_expired(&self) -> bool {
        self.authority_lease.lock().await.is_expired()
    }

    async fn authority_sleep_budget(&self, preferred: Duration) -> Duration {
        self.authority_lease.lock().await.sleep_budget(preferred)
    }

    async fn authority_request_timeout(&self, configured: Duration) -> Duration {
        self.authority_lease
            .lock()
            .await
            .request_timeout(configured)
    }

    async fn authority_lease_snapshot(&self) -> AuthorityLeaseSnapshot {
        self.authority_lease.lock().await.snapshot()
    }

    pub async fn authority_lease_snapshot_public(&self) -> AuthorityLeaseSnapshot {
        self.authority_lease_snapshot().await
    }

    /// Clear live delegated lease (identity left Delegated or revoked).
    pub async fn clear_authority_lease(&self) {
        let mut lease = self.authority_lease.lock().await;
        *lease = AuthorityLease::inactive();
    }

    /// Install a short revalidation-only lease for an inactive saved takeover session.
    pub async fn install_inactive_maintenance_lease(&self, generation: DelegatedGeneration) {
        self.install_pending_authority_lease(generation).await;
    }

    /// Capture credential provenance at selection time (before awaits that can race mode).
    pub async fn capture_platform_provenance(
        &self,
        state: &AppState,
    ) -> Result<PlatformCredentialProvenance> {
        Ok(self.select_platform_credentials(state).await?.provenance())
    }

    /// Atomically select provenance and credentials under the lifecycle fence (B2).
    pub async fn select_platform_credentials(
        &self,
        state: &AppState,
    ) -> Result<PlatformCredentialSelection> {
        let _lifecycle = self.lifecycle_lock.lock().await;
        self.select_platform_credentials_under_lock(state).await
    }

    async fn select_platform_credentials_under_lock(
        &self,
        state: &AppState,
    ) -> Result<PlatformCredentialSelection> {
        let mode = *state.active_mode.read().await;
        let delegated = state.delegated.read().await.clone();
        let tokens = state.twitch.read().await.tokens.clone();
        let access_token = tokens.access_token.unwrap_or_default();
        let client_id = if mode == TwitchActiveMode::Delegated {
            delegated
                .as_ref()
                .map(|d| d.client_id.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| state.client_id.clone())
        } else {
            state.client_id.clone()
        };
        if mode != TwitchActiveMode::Delegated || delegated.is_none() {
            return Ok(PlatformCredentialSelection::Local {
                client_id,
                access_token,
            });
        }
        let snap = self.authority_lease_snapshot().await;
        {
            let lease = self.authority_lease.lock().await;
            if !lease.allows_platform_operations(snap.generation)
                || lease.snapshot().generation != snap.generation
                || lease.snapshot().deadline != snap.deadline
            {
                return Err(anyhow!("Delegated authority expired or superseded"));
            }
        }
        if !state.session_still_current(snap.generation).await {
            return Err(anyhow!("Delegated authority expired or superseded"));
        }
        Ok(PlatformCredentialSelection::Delegated {
            snap,
            client_id,
            access_token,
        })
    }

    /// Atomically select IRC provenance, channel, and authenticated client (B1).
    pub async fn select_irc_send_bundle(
        &self,
        state: &AppState,
    ) -> Result<(PlatformCredentialProvenance, String, StreamSyncIrcClient)> {
        let _lifecycle = self.lifecycle_lock.lock().await;
        let provenance = self
            .select_platform_credentials_under_lock(state)
            .await?
            .provenance();
        let channel = state
            .twitch
            .read()
            .await
            .channel
            .clone()
            .ok_or_else(|| anyhow!("No Twitch channel joined"))?;
        let bundle = self
            .irc_client
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("Twitch IRC client is not ready"))?;
        if bundle.provenance != provenance {
            return Err(anyhow!(
                "IRC client identity does not match active credential provenance"
            ));
        }
        Ok((provenance, channel, bundle.client))
    }

    #[cfg(test)]
    pub(crate) async fn install_irc_bundle_for_test(
        &self,
        provenance: PlatformCredentialProvenance,
    ) {
        let (_incoming, client) = StreamSyncIrcClient::new(ClientConfig::new_simple(
            StaticLoginCredentials::new("test".into(), Some("token".into())),
        ));
        *self.irc_client.write().await = Some(IrcClientBundle { client, provenance });
    }

    /// True when the captured lease snapshot still authorizes platform fan-out/send.
    pub fn delegated_send_gate_still_valid(&self, snap: AuthorityLeaseSnapshot) -> bool {
        if snap.deadline <= std::time::Instant::now() {
            return false;
        }
        let Ok(lease) = self.authority_lease.try_lock() else {
            return false;
        };
        lease.allows_platform_operations(snap.generation)
            && lease.snapshot().generation == snap.generation
            && lease.snapshot().deadline == snap.deadline
    }

    /// True when Syndicate SSE/watch may continue for the captured snapshot.
    pub async fn syndicate_revalidation_still_valid(&self, snap: AuthorityLeaseSnapshot) -> bool {
        let lease = self.authority_lease.lock().await;
        lease.allows_syndicate_revalidation(snap.generation)
            && lease.snapshot().generation == snap.generation
            && lease.snapshot().deadline == snap.deadline
    }

    /// Race using provenance captured at credential selection — never downgrades Delegated→Local.
    pub async fn race_delegated_platform_with_provenance<T>(
        &self,
        state: &AppState,
        provenance: PlatformCredentialProvenance,
        network: impl std::future::Future<Output = T>,
    ) -> Result<T> {
        match provenance {
            PlatformCredentialProvenance::Local => Ok(network.await),
            PlatformCredentialProvenance::Delegated { snap } => {
                self.race_delegated_platform_with_snapshot(state, snap, network)
                    .await
            }
        }
    }

    /// Synchronous delegated-authority guard for privileged platform operations.
    /// Local mode is allowed (no-op). Does not validate a captured in-flight snapshot.
    pub async fn ensure_delegated_authority(&self, state: &AppState) -> Result<()> {
        if !state.is_delegated_mode().await {
            return Ok(());
        }
        let generation = state.current_delegated_generation();
        let lease = self.authority_lease.lock().await;
        if !lease.allows_platform_operations(generation) {
            return Err(anyhow!("Delegated authority expired or superseded"));
        }
        drop(lease);
        if !state.session_still_current(generation).await {
            return Err(anyhow!("Delegated session superseded"));
        }
        Ok(())
    }

    /// Validate a captured delegated lease snapshot after network completion.
    /// Never treats Local mode as success for a delegated in-flight request.
    pub async fn validate_delegated_snapshot(&self, snap: AuthorityLeaseSnapshot) -> Result<()> {
        if snap.generation == 0 {
            return Err(anyhow!("Delegated authority unavailable"));
        }
        if snap.deadline <= std::time::Instant::now() {
            return Err(anyhow!("Delegated authority expired during request"));
        }
        let lease = self.authority_lease.lock().await;
        if !lease.allows_platform_operations(snap.generation)
            || lease.phase() != AuthorityLeasePhase::Validated
            || lease.snapshot().generation != snap.generation
            || lease.snapshot().deadline != snap.deadline
        {
            return Err(anyhow!("Delegated authority superseded during request"));
        }
        Ok(())
    }

    /// Race a delegated platform future against the generation-bound absolute lease.
    /// Captures `(generation, deadline)` before dispatch and revalidates that snapshot after
    /// completion regardless of current active mode.
    pub async fn race_delegated_platform<T>(
        &self,
        state: &AppState,
        network: impl std::future::Future<Output = T>,
    ) -> Result<T> {
        if !state.is_delegated_mode().await {
            return Ok(network.await);
        }
        let snap = self.authority_lease_snapshot().await;
        if snap.generation == 0 || !state.session_still_current(snap.generation).await {
            return Err(anyhow!("Delegated authority expired or superseded"));
        }
        {
            let lease = self.authority_lease.lock().await;
            if !lease.allows_platform_operations(snap.generation)
                || lease.snapshot().generation != snap.generation
                || lease.snapshot().deadline != snap.deadline
            {
                return Err(anyhow!("Delegated authority expired or superseded"));
            }
        }
        let result = race_against_lease_deadline(self, snap.generation, network)
            .await
            .map_err(|()| anyhow!("Delegated authority expired during request"))?;
        self.validate_delegated_snapshot(snap).await?;
        Ok(result)
    }

    /// Race against a previously captured delegated lease snapshot (Kick feed, etc.).
    pub async fn race_delegated_platform_with_snapshot<T>(
        &self,
        _state: &AppState,
        snap: AuthorityLeaseSnapshot,
        network: impl std::future::Future<Output = T>,
    ) -> Result<T> {
        self.validate_delegated_snapshot(snap).await?;
        let result = race_against_lease_deadline(self, snap.generation, network)
            .await
            .map_err(|()| anyhow!("Delegated authority expired during request"))?;
        self.validate_delegated_snapshot(snap).await?;
        Ok(result)
    }

    #[cfg(test)]
    pub async fn set_authority_deadline_for_test(
        &self,
        generation: DelegatedGeneration,
        deadline: std::time::Instant,
    ) {
        let mut lease = self.authority_lease.lock().await;
        lease.install_validated_generation(generation, None);
        lease.set_deadline_for_test(deadline);
    }
}

impl Default for TwitchServices {
    fn default() -> Self {
        Self::new()
    }
}

pub fn token_expired(tokens: &TwitchTokenFile) -> bool {
    if tokens.access_token.is_none() {
        return true;
    }
    let Some(ts) = tokens.obtainment_timestamp else {
        return false;
    };
    let Some(exp) = tokens.expires_in else {
        return false;
    };
    let age = (chrono::Utc::now().timestamp_millis() - ts) / 1000;
    age >= exp - 10
}

pub async fn ensure_valid_token(state: &AppState) -> Result<()> {
    let tw = state.twitch.read().await;
    if tw.tokens.access_token.is_none() {
        return Err(anyhow!(
            "No Twitch accessToken. Please connect Twitch first."
        ));
    }
    if token_expired(&tw.tokens) {
        return Err(anyhow!("Twitch token expired. Please reconnect Twitch."));
    }
    Ok(())
}

/// Run a delegated platform HTTP future under a provenance captured with the credentials.
async fn delegated_platform_http_with_provenance<T>(
    state: &AppState,
    provenance: PlatformCredentialProvenance,
    network: impl std::future::Future<Output = T>,
) -> Result<T> {
    let Some(services) = state.twitch_services() else {
        return match provenance {
            PlatformCredentialProvenance::Local => Ok(network.await),
            PlatformCredentialProvenance::Delegated { .. } => {
                Err(anyhow!("Delegated authority unavailable"))
            }
        };
    };
    services
        .race_delegated_platform_with_provenance(state, provenance, network)
        .await
}

async fn select_helix_credentials(state: &AppState) -> Result<PlatformCredentialSelection> {
    let Some(services) = state.twitch_services() else {
        if state.is_delegated_mode().await {
            return Err(anyhow!("Delegated authority unavailable"));
        }
        let client_id = state.helix_client_id().await;
        let access_token = state
            .twitch
            .read()
            .await
            .tokens
            .access_token
            .clone()
            .unwrap_or_default();
        return Ok(PlatformCredentialSelection::Local {
            client_id,
            access_token,
        });
    };
    services.select_platform_credentials(state).await
}

pub async fn validate_token(access_token: &str) -> Result<Value> {
    let client = reqwest::Client::new();
    let res = client
        .get("https://id.twitch.tv/oauth2/validate")
        .header("Authorization", format!("OAuth {access_token}"))
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let text = res.text().await.unwrap_or_default();
        return Err(anyhow!("Twitch validate failed: {} {}", status, text));
    }
    Ok(res.json().await?)
}

pub async fn helix_get(state: &AppState, path: &str) -> Result<Value> {
    state.ensure_identity_coherent_for_platform()?;
    ensure_valid_token(state).await?;
    let selection = select_helix_credentials(state).await?;
    let (provenance, client_id, token) = match selection {
        PlatformCredentialSelection::Local {
            client_id,
            access_token,
        } => (PlatformCredentialProvenance::Local, client_id, access_token),
        PlatformCredentialSelection::Delegated {
            snap,
            client_id,
            access_token,
        } => (
            PlatformCredentialProvenance::Delegated { snap },
            client_id,
            access_token,
        ),
    };
    if client_id.is_empty() {
        return Err(anyhow!("TWITCH_CLIENT_ID not configured."));
    }
    let url = format!("https://api.twitch.tv/helix{path}");
    delegated_platform_http_with_provenance(state, provenance, async {
        let res = reqwest::Client::new()
            .get(&url)
            .header("Client-Id", &client_id)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("Helix GET {path} failed: {} {}", status, text));
        }
        Ok(res.json().await?)
    })
    .await?
}

pub async fn helix_patch(state: &AppState, path: &str, body: Value) -> Result<Value> {
    state.ensure_identity_coherent_for_platform()?;
    ensure_valid_token(state).await?;
    let selection = select_helix_credentials(state).await?;
    let (provenance, client_id, token) = match selection {
        PlatformCredentialSelection::Local {
            client_id,
            access_token,
        } => (PlatformCredentialProvenance::Local, client_id, access_token),
        PlatformCredentialSelection::Delegated {
            snap,
            client_id,
            access_token,
        } => (
            PlatformCredentialProvenance::Delegated { snap },
            client_id,
            access_token,
        ),
    };
    delegated_platform_http_with_provenance(state, provenance, async {
        let res = reqwest::Client::new()
            .patch(format!("https://api.twitch.tv/helix{path}"))
            .header("Client-Id", &client_id)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("Helix PATCH failed: {} {}", status, text));
        }
        Ok(res.json().await?)
    })
    .await?
}

pub async fn helix_post(state: &AppState, path: &str, body: Value) -> Result<Value> {
    state.ensure_identity_coherent_for_platform()?;
    ensure_valid_token(state).await?;
    let selection = select_helix_credentials(state).await?;
    let (provenance, client_id, token) = match selection {
        PlatformCredentialSelection::Local {
            client_id,
            access_token,
        } => (PlatformCredentialProvenance::Local, client_id, access_token),
        PlatformCredentialSelection::Delegated {
            snap,
            client_id,
            access_token,
        } => (
            PlatformCredentialProvenance::Delegated { snap },
            client_id,
            access_token,
        ),
    };
    delegated_platform_http_with_provenance(state, provenance, async {
        let res = reqwest::Client::new()
            .post(format!("https://api.twitch.tv/helix{path}"))
            .header("Client-Id", &client_id)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;
        if res.status() == 409 {
            return Ok(json!({ "ok": true, "already": true }));
        }
        let status = res.status();
        if !status.is_success() {
            let text = res.text().await.unwrap_or_default();
            return Err(anyhow!("Helix POST failed: {} {}", status, text));
        }
        Ok(res.json().await?)
    })
    .await?
}

pub async fn update_chat_settings(state: &AppState, partial: Value) -> Result<()> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Missing userId"))?;
    let path = format!("/chat/settings?broadcaster_id={user_id}&moderator_id={user_id}");
    let _ = helix_patch(state, &path, partial).await?;
    Ok(())
}

pub async fn get_merged_badges(state: &AppState, services: &TwitchServices) -> Result<Value> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Connect Twitch first"))?;
    {
        let cache = services.badge_cache.read().await;
        if let Some(c) = cache.as_ref() {
            if c.user_id == user_id && c.fetched_at.elapsed() < BADGE_TTL {
                return Ok(c.value.clone());
            }
        }
    }
    let global = helix_get(state, "/chat/badges/global").await?;
    let channel = helix_get(state, &format!("/chat/badges?broadcaster_id={user_id}")).await?;
    let merged = merge_badge_sets(&global, &channel);
    let mut cache = services.badge_cache.write().await;
    *cache = Some(CacheEntry {
        value: merged.clone(),
        user_id,
        fetched_at: std::time::Instant::now(),
    });
    Ok(merged)
}

fn merge_badge_sets(global: &Value, channel: &Value) -> Value {
    let mut badge_sets = serde_json::Map::new();
    for source in [global, channel] {
        if let Some(data) = source.get("data").and_then(|d| d.as_array()) {
            for set in data {
                let set_id = set.get("set_id").and_then(|s| s.as_str()).unwrap_or("");
                if set_id.is_empty() {
                    continue;
                }
                let entry = badge_sets
                    .entry(set_id.to_string())
                    .or_insert_with(|| json!({ "versions": {} }));
                if let Some(versions) = set.get("versions").and_then(|v| v.as_array()) {
                    for v in versions {
                        if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                            entry["versions"][id] = v.clone();
                        }
                    }
                }
            }
        }
    }
    json!({ "badge_sets": badge_sets })
}

pub async fn get_merged_emotes(state: &AppState, services: &TwitchServices) -> Result<Vec<Value>> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("Connect Twitch first"))?;
    {
        let cache = services.emote_cache.read().await;
        if let Some(c) = cache.as_ref() {
            if c.user_id == user_id && c.fetched_at.elapsed() < EMOTE_TTL {
                return Ok(c.value.clone());
            }
        }
    }
    let mut by_id: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    if let Ok(g) = helix_get(state, "/chat/emotes/global").await {
        add_emote_batch(&mut by_id, g.get("data"), Some("global"), None, &user_id);
    }
    if let Ok(c) = helix_get(state, &format!("/chat/emotes?broadcaster_id={user_id}")).await {
        add_emote_batch(
            &mut by_id,
            c.get("data"),
            Some("channel"),
            Some(&user_id),
            &user_id,
        );
    }
    let user_emotes = fetch_all_user_emotes(state, &user_id)
        .await
        .unwrap_or_default();
    // User emotes carry Helix `owner_id` for subscribed / followed channels.
    add_emote_batch(
        &mut by_id,
        Some(&Value::Array(user_emotes)),
        None,
        None,
        &user_id,
    );

    let mut list: Vec<Value> = by_id.into_values().collect();
    enrich_emote_owners(state, &mut list).await;

    let mut cache = services.emote_cache.write().await;
    *cache = Some(CacheEntry {
        value: list.clone(),
        user_id: user_id.clone(),
        fetched_at: std::time::Instant::now(),
    });
    Ok(list)
}

fn json_id_string(v: &Value) -> Option<String> {
    if let Some(s) = v.as_str() {
        let t = s.trim();
        if t.is_empty() || t == "0" {
            return None;
        }
        return Some(t.to_string());
    }
    if let Some(n) = v.as_u64() {
        if n == 0 {
            return None;
        }
        return Some(n.to_string());
    }
    if let Some(n) = v.as_i64() {
        if n <= 0 {
            return None;
        }
        return Some(n.to_string());
    }
    None
}

fn is_usable_owner_id(id: &str) -> bool {
    let t = id.trim();
    !t.is_empty() && t != "0"
}

fn add_emote_batch(
    by_id: &mut std::collections::HashMap<String, Value>,
    list: Option<&Value>,
    default_owner_type: Option<&str>,
    default_owner_id: Option<&str>,
    self_user_id: &str,
) {
    let Some(arr) = list.and_then(|v| v.as_array()) else {
        return;
    };
    for emote in arr {
        let Some(id) = emote.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        if by_id.contains_key(id) {
            continue;
        }

        let emote_type = emote.get("emote_type").and_then(|v| v.as_str());
        // Helix may return owner_id as "" or "0" for emotes without an owner — those
        // must not be queried via /users (they 400 the whole batch and wipe avatars).
        let owner_id = emote.get("owner_id").and_then(json_id_string).or_else(|| {
            default_owner_id
                .filter(|id| is_usable_owner_id(id))
                .map(|s| s.to_string())
        });

        let mut owner_type = default_owner_type.unwrap_or("unknown");
        if emote_type == Some("globals") {
            owner_type = "global";
        } else if matches!(
            emote_type,
            Some("subscriptions") | Some("bitstier") | Some("follower")
        ) {
            if owner_type == "unknown" {
                owner_type = "channel";
            }
        } else if owner_type == "unknown" && owner_id.is_some() {
            // Subscribed / followed channel emotes from /chat/emotes/user
            owner_type = "channel";
        }

        by_id.insert(
            id.to_string(),
            json!({
                "id": id,
                "name": emote.get("name"),
                "images": emote.get("images"),
                "emoteType": emote_type,
                "emoteSetId": emote.get("emote_set_id"),
                "ownerType": owner_type,
                "ownerId": owner_id,
                "ownerLogin": Value::Null,
                "ownerName": Value::Null,
                "ownerProfileImageUrl": Value::Null,
                "ownerIsSelf": owner_id.as_deref() == Some(self_user_id),
            }),
        );
    }
}

/// Resolve owner login / display name / avatar via Helix `/users` (chunked).
async fn enrich_emote_owners(state: &AppState, list: &mut [Value]) {
    let mut owner_ids: Vec<String> = list
        .iter()
        .filter_map(|e| e.get("ownerId").and_then(json_id_string))
        .collect();
    owner_ids.sort();
    owner_ids.dedup();
    if owner_ids.is_empty() {
        return;
    }

    let mut owners: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    for chunk in owner_ids.chunks(100) {
        // Build with reqwest so repeated `id` params are encoded correctly.
        match helix_get_users(state, chunk).await {
            Ok(res) => {
                if let Some(arr) = res.get("data").and_then(|d| d.as_array()) {
                    for u in arr {
                        if let Some(id) = u.get("id").and_then(json_id_string) {
                            owners.insert(id, u.clone());
                        }
                    }
                }
            }
            Err(e) => warn!("Helix /users for emote owners failed: {e}"),
        }
    }

    info!(
        "Emote owner enrichment: {} unique owners, {} resolved",
        owner_ids.len(),
        owners.len()
    );

    for emote in list.iter_mut() {
        let Some(owner_id) = emote.get("ownerId").and_then(json_id_string) else {
            continue;
        };
        let Some(u) = owners.get(&owner_id) else {
            continue;
        };
        let login = u.get("login").cloned().unwrap_or(Value::Null);
        let name = u
            .get("display_name")
            .cloned()
            .or_else(|| u.get("login").cloned())
            .unwrap_or(Value::Null);
        let avatar = u.get("profile_image_url").cloned().unwrap_or(Value::Null);
        if let Some(obj) = emote.as_object_mut() {
            obj.insert("ownerLogin".into(), login);
            obj.insert("ownerName".into(), name);
            obj.insert("ownerProfileImageUrl".into(), avatar);
        }
    }
}

async fn helix_get_users(state: &AppState, ids: &[String]) -> Result<Value> {
    // Build Helix /users query and route through the fenced Helix helper (B3).
    let mut path = String::from("/users?");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            path.push('&');
        }
        path.push_str("id=");
        path.push_str(&urlencoding::encode(id));
    }
    helix_get(state, &path).await
}

async fn fetch_all_user_emotes(state: &AppState, user_id: &str) -> Result<Vec<Value>> {
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..20 {
        let mut path = format!("/chat/emotes/user?user_id={user_id}&first=100");
        if let Some(c) = &cursor {
            path.push_str(&format!("&after={c}"));
        }
        let data = helix_get(state, &path).await?;
        if let Some(items) = data.get("data").and_then(|d| d.as_array()) {
            all.extend(items.iter().cloned());
        }
        cursor = data
            .get("pagination")
            .and_then(|p| p.get("cursor"))
            .and_then(|c| c.as_str())
            .map(String::from);
        if cursor.is_none() {
            break;
        }
    }
    Ok(all)
}

pub async fn apply_set_token(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    body: Value,
) -> Result<()> {
    let intent = services.bump_apply_intent();
    // Personal OAuth — keep any saved takeover session; just activate local.
    let access_token = body
        .get("accessToken")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing accessToken"))?;
    let validated = validate_token(access_token).await?;
    services.ensure_apply_intent_current(intent)?;
    let login = validated
        .get("login")
        .and_then(|v| v.as_str())
        .map(String::from);
    let user_id = validated
        .get("user_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let expires_in = validated.get("expires_in").and_then(|v| v.as_i64());
    let tokens = TwitchTokenFile {
        access_token: Some(access_token.to_string()),
        refresh_token: None,
        expires_in,
        obtainment_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        login: login.clone(),
        user_id: user_id.clone(),
        scopes: body.get("scope").and_then(|v| v.as_array()).map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        }),
    };
    // Durable commit before live publish; restore personal token file if mode commit fails (B2).
    {
        let _lifecycle = services.lifecycle_lock.lock().await;
        services.ensure_apply_intent_current(intent)?;
        let previous_personal = state.personal_tokens.read().await.clone();
        let previous_mode = *state.active_mode.read().await;
        *state.personal_tokens.write().await = tokens.clone();
        if let Err(e) = state.save_twitch_tokens().await {
            *state.personal_tokens.write().await = previous_personal;
            return Err(e);
        }
        *state.active_mode.write().await = TwitchActiveMode::Local;
        if let Err(e) = state.save_active_mode().await {
            *state.active_mode.write().await = previous_mode;
            *state.personal_tokens.write().await = previous_personal.clone();
            if let Err(rollback_err) = state.save_twitch_tokens().await {
                let marker_err = crate::storage::write_identity_rollback_pending(
                    &state.paths.twitch_tokens_rollback_pending,
                );
                if let Err(marker_write_err) = marker_err {
                    return Err(anyhow!(
                        "active mode save failed ({e:#}); personal token rollback also failed ({rollback_err:#}); rollback marker write failed ({marker_write_err:#})"
                    ));
                }
                return Err(anyhow!(
                    "active mode save failed ({e:#}); personal token rollback also failed ({rollback_err:#})"
                ));
            }
            return Err(e);
        }
        if previous_mode == TwitchActiveMode::Delegated {
            let generation = state.current_delegated_generation();
            stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, None)
                .await;
            if generation > 0 {
                services
                    .install_inactive_maintenance_lease(generation)
                    .await;
            }
        }
        let mut tw = state.twitch.write().await;
        clear_live_runtime_fields(&mut tw);
        tw.tokens = tokens;
    }
    restart_twitch_clients(state.clone(), services.clone()).await;
    ensure_delegated_refresh_loop(state, services).await;
    Ok(())
}

/// Remote validation gate before activating a saved delegated identity (B9/B3).
async fn validate_saved_delegated_for_activation(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    session: &DelegatedSessionFile,
    intent: u64,
) -> Result<DelegatedSessionFile> {
    services.ensure_apply_intent_current(intent)?;
    let generation = session.generation;
    services.install_pending_authority_lease(generation).await;
    match syndicate_connection::refresh(&session.connection_key).await {
        Ok(exchange) => {
            services.ensure_apply_intent_current(intent)?;
            Ok(crate::kick::merge_delegated_session_from_exchange(
                session.clone(),
                &exchange,
            ))
        }
        Err(e) => {
            if let Some(api) = e.downcast_ref::<SyndicateApiError>() {
                match api.code.as_str() {
                    "revoked" | "expired" | "invalid_key" => {
                        remove_delegated_session(&state, &services, None, generation).await?;
                        Err(anyhow!(
                            "Takeover connection key is no longer valid ({})",
                            api.code
                        ))
                    }
                    _ => {
                        services.clear_authority_lease().await;
                        Err(anyhow!("Takeover connection could not be validated: {api}"))
                    }
                }
            } else {
                services.clear_authority_lease().await;
                Err(anyhow!("Takeover connection could not be validated: {e:#}"))
            }
        }
    }
}

/// Publish a validated delegated session as the live identity under lifecycle lock.
async fn commit_delegated_activation(
    state: &AppState,
    services: &TwitchServices,
    intent: u64,
    validated: DelegatedSessionFile,
    saved: Option<&DelegatedSessionFile>,
) -> Result<()> {
    let _lifecycle = services.lifecycle_lock.lock().await;
    services.ensure_apply_intent_current(intent)?;
    if saved.is_none_or(|s| validated != *s) {
        state.persist_delegated_session(&validated)?;
    }
    services.ensure_apply_intent_current(intent)?;
    let previous_mode = *state.active_mode.read().await;
    *state.active_mode.write().await = TwitchActiveMode::Delegated;
    if let Err(e) = state.save_active_mode().await {
        *state.active_mode.write().await = previous_mode;
        return Err(e);
    }
    services.ensure_apply_intent_current(intent)?;
    services
        .renew_after_successful_remote_validation(
            validated.generation,
            validated.connection_expires_at.as_deref(),
        )
        .await
        .map_err(|e| anyhow!(e))?;
    *state.delegated.write().await = Some(validated.clone());
    let mut tw = state.twitch.write().await;
    clear_live_runtime_fields(&mut tw);
    tw.tokens = tokens_from_delegated_session(&validated);
    Ok(())
}

/// Switch the live identity between saved personal OAuth and a saved takeover key.
pub async fn use_connection(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    mode: TwitchActiveMode,
) -> Result<()> {
    let intent = services.bump_apply_intent();
    state.ensure_identity_coherent_for_platform()?;
    match mode {
        TwitchActiveMode::Local => {
            let personal = state.personal_tokens.read().await.clone();
            if personal.access_token.is_none() || personal.login.is_none() {
                return Err(anyhow!(
                    "No personal Twitch account saved. Connect with Twitch first."
                ));
            }
            {
                let _lifecycle = services.lifecycle_lock.lock().await;
                services.ensure_apply_intent_current(intent)?;
                // Stage durable mode first; only then publish live identity (B8).
                let previous_mode = *state.active_mode.read().await;
                *state.active_mode.write().await = TwitchActiveMode::Local;
                if let Err(e) = state.save_active_mode().await {
                    *state.active_mode.write().await = previous_mode;
                    return Err(e);
                }
                if previous_mode == TwitchActiveMode::Delegated {
                    // Keep saved takeover; switch to inactive maintenance lease (B4).
                    let generation = state.current_delegated_generation();
                    stop_delegated_worker_handles(
                        &services.refresh_handle,
                        &services.watch_handle,
                        None,
                    )
                    .await;
                    if generation > 0 {
                        services
                            .install_inactive_maintenance_lease(generation)
                            .await;
                    }
                }
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = personal;
            }
            restart_twitch_clients(state.clone(), services.clone()).await;
            ensure_delegated_refresh_loop(state.clone(), services).await;
            crate::kick::sync_live_identity(state).await;
        }
        TwitchActiveMode::Delegated => {
            let saved =
                state.delegated.read().await.clone().ok_or_else(|| {
                    anyhow!("No takeover connection key saved. Paste a key first.")
                })?;
            let validated = validate_saved_delegated_for_activation(
                state.clone(),
                services.clone(),
                &saved,
                intent,
            )
            .await?;
            commit_delegated_activation(&state, &services, intent, validated, Some(&saved)).await?;
            restart_twitch_clients(state.clone(), services.clone()).await;
            ensure_delegated_refresh_loop(state.clone(), services).await;
            crate::kick::sync_live_identity(state).await;
        }
    }
    Ok(())
}

fn clear_live_runtime_fields(tw: &mut crate::app_state::TwitchRuntime) {
    tw.connected = false;
    tw.channel = None;
    tw.name_color = None;
    tw.display_name = None;
    tw.badges_raw.clear();
}

fn tokens_saved(t: &TwitchTokenFile) -> bool {
    t.access_token.is_some() && t.login.is_some()
}

/// Remove the currently active connection. If the other identity is still saved, activate it.
pub async fn disconnect_twitch(state: Arc<AppState>, services: Arc<TwitchServices>) -> Result<()> {
    let intent = services.bump_apply_intent();
    disconnect_twitch_inner(state, services, intent).await
}

async fn disconnect_twitch_inner(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    intent: u64,
) -> Result<()> {
    state.ensure_identity_coherent_for_platform()?;
    let active = *state.active_mode.read().await;
    match active {
        TwitchActiveMode::Delegated => {
            let generation = state
                .delegated
                .read()
                .await
                .as_ref()
                .map(|s| s.generation)
                .unwrap_or_else(|| state.current_delegated_generation());
            remove_delegated_session(&state, &services, None, generation).await?;
            services.ensure_apply_intent_current(intent)?;
            let personal = state.personal_tokens.read().await.clone();
            if tokens_saved(&personal) {
                {
                    let _lifecycle = services.lifecycle_lock.lock().await;
                    services.ensure_apply_intent_current(intent)?;
                    let previous_mode = *state.active_mode.read().await;
                    *state.active_mode.write().await = TwitchActiveMode::Local;
                    if let Err(e) = state.save_active_mode().await {
                        *state.active_mode.write().await = previous_mode;
                        return Err(e);
                    }
                    let mut tw = state.twitch.write().await;
                    clear_live_runtime_fields(&mut tw);
                    tw.tokens = personal;
                }
                restart_twitch_clients(state.clone(), services).await;
                crate::kick::sync_live_identity(state).await;
            } else {
                stop_delegated_tasks(&services).await;
                stop_twitch_clients(&services).await;
                {
                    let _lifecycle = services.lifecycle_lock.lock().await;
                    services.ensure_apply_intent_current(intent)?;
                    let mut tw = state.twitch.write().await;
                    tw.tokens = TwitchTokenFile::default();
                    clear_live_runtime_fields(&mut tw);
                    *state.active_mode.write().await = TwitchActiveMode::Local;
                    state.save_active_mode().await?;
                }
                crate::kick::sync_live_identity(state).await;
            }
        }
        TwitchActiveMode::Local => {
            {
                let _lifecycle = services.lifecycle_lock.lock().await;
                services.ensure_apply_intent_current(intent)?;
                let previous = state.personal_tokens.read().await.clone();
                *state.personal_tokens.write().await = TwitchTokenFile::default();
                if let Err(e) = state.save_twitch_tokens().await {
                    *state.personal_tokens.write().await = previous;
                    return Err(e);
                }
            }
            let saved = state.delegated.read().await.clone();
            if let Some(session) = saved {
                let validated = validate_saved_delegated_for_activation(
                    state.clone(),
                    services.clone(),
                    &session,
                    intent,
                )
                .await?;
                commit_delegated_activation(&state, &services, intent, validated, Some(&session))
                    .await?;
                restart_twitch_clients(state.clone(), services.clone()).await;
                ensure_delegated_refresh_loop(state.clone(), services).await;
                crate::kick::sync_live_identity(state).await;
            } else {
                stop_delegated_tasks(&services).await;
                stop_twitch_clients(&services).await;
                {
                    let _lifecycle = services.lifecycle_lock.lock().await;
                    services.ensure_apply_intent_current(intent)?;
                    let mut tw = state.twitch.write().await;
                    tw.tokens = TwitchTokenFile::default();
                    clear_live_runtime_fields(&mut tw);
                }
                crate::kick::sync_live_identity(state).await;
            }
        }
    }
    Ok(())
}

/// Remove a specific saved identity without requiring it to be active.
pub async fn remove_connection(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    mode: TwitchActiveMode,
) -> Result<()> {
    let intent = services.bump_apply_intent();
    let active = *state.active_mode.read().await;
    if active == mode {
        return disconnect_twitch_inner(state, services, intent).await;
    }
    let _lifecycle = services.lifecycle_lock.lock().await;
    match mode {
        TwitchActiveMode::Local => {
            services.ensure_apply_intent_current(intent)?;
            let previous = state.personal_tokens.read().await.clone();
            *state.personal_tokens.write().await = TwitchTokenFile::default();
            if let Err(e) = state.save_twitch_tokens().await {
                *state.personal_tokens.write().await = previous;
                return Err(e);
            }
        }
        TwitchActiveMode::Delegated => {
            services.ensure_apply_intent_current(intent)?;
            let generation = state
                .delegated
                .read()
                .await
                .as_ref()
                .map(|s| s.generation)
                .unwrap_or_else(|| state.current_delegated_generation());
            remove_delegated_session_locked(&state, &services, None, generation).await?;
        }
    }
    Ok(())
}

fn expires_in_from_iso(iso: &str) -> Option<i64> {
    let exp = chrono::DateTime::parse_from_rfc3339(iso).ok()?;
    let secs = exp.timestamp() - chrono::Utc::now().timestamp();
    Some(secs.max(0))
}

fn install_tokens_from_exchange(
    exchange: &syndicate_connection::ExchangeSuccess,
) -> TwitchTokenFile {
    let expires_in = expires_in_from_iso(&exchange.twitch.expires_at);
    TwitchTokenFile {
        access_token: Some(exchange.twitch.access_token.clone()),
        refresh_token: None,
        expires_in,
        obtainment_timestamp: Some(chrono::Utc::now().timestamp_millis()),
        login: Some(exchange.channel.login.clone()),
        user_id: Some(exchange.channel.twitch_id.clone()),
        scopes: Some(exchange.twitch.scopes.clone()),
    }
}

async fn remove_delegated_session(
    state: &AppState,
    services: &TwitchServices,
    except: Option<DelegatedWorker>,
    generation: DelegatedGeneration,
) -> Result<()> {
    let _lifecycle = services.lifecycle_lock.lock().await;
    remove_delegated_session_locked(state, services, except, generation).await
}

/// Caller must already hold `services.lifecycle_lock`.
async fn remove_delegated_session_locked(
    state: &AppState,
    services: &TwitchServices,
    except: Option<DelegatedWorker>,
    generation: DelegatedGeneration,
) -> Result<()> {
    if state.current_delegated_generation() != generation {
        return Ok(());
    }
    {
        let delegated = state.delegated.read().await;
        if delegated
            .as_ref()
            .is_some_and(|s| s.generation != generation)
        {
            return Ok(());
        }
    }
    match state.durable_revoke_delegated().await {
        Ok(()) => {}
        Err(e) => {
            // B5: marker/durable write failure still strips live authority; route reports error.
            {
                let mut delegated = state.delegated.write().await;
                if delegated
                    .as_ref()
                    .is_some_and(|s| s.generation == generation)
                {
                    *delegated = None;
                }
            }
            stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, except)
                .await;
            return Err(e);
        }
    }
    {
        let mut delegated = state.delegated.write().await;
        if state.current_delegated_generation() != generation {
            return Ok(());
        }
        if delegated
            .as_ref()
            .is_some_and(|s| s.generation != generation)
        {
            return Ok(());
        }
        *delegated = None;
    }
    stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, except).await;
    Ok(())
}

async fn stop_delegated_tasks(services: &TwitchServices) {
    stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, None).await;
}

fn disk_active_mode_is_delegated(state: &AppState) -> bool {
    crate::storage::read_json_if_exists(
        &state.paths.twitch_active_mode,
        &TwitchActiveModeFile::default(),
    )
    .map(|f| f.mode == TwitchActiveMode::Delegated)
    .unwrap_or(false)
}

fn durable_revoke_still_needed(state: &AppState) -> bool {
    state.delegated_authority_artifacts_remain().unwrap_or(true)
}

/// Strip in-memory delegated authority for `generation` and stop platform/delegated workers.
/// Durable storage is left for a subsequent teardown retry.
async fn strip_in_memory_delegated_authority(
    state: &AppState,
    services: &TwitchServices,
    generation: DelegatedGeneration,
) {
    if state.current_delegated_generation() != generation {
        return;
    }
    {
        let mut delegated = state.delegated.write().await;
        if delegated
            .as_ref()
            .is_some_and(|s| s.generation == generation)
        {
            *delegated = None;
        }
    }
    {
        let mut lease = services.authority_lease.lock().await;
        if lease.generation() == generation {
            *lease = AuthorityLease::inactive();
        }
    }
    if *state.active_mode.read().await == TwitchActiveMode::Delegated {
        *state.active_mode.write().await = TwitchActiveMode::Local;
        let mut tw = state.twitch.write().await;
        clear_live_runtime_fields(&mut tw);
        tw.tokens = TwitchTokenFile::default();
    }
    stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, None).await;
    stop_twitch_clients(services).await;
    crate::kick::teardown_delegated_kick_live(state).await;
}

/// Absolute-lease fail-closed for *active* delegated mode.
/// When Local with a saved takeover session, reinstalls inactive maintenance lease and does
/// **not** revoke/delete the saved connection (identity switch ≠ revocation).
async fn fail_closed_lease_expired(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
) {
    if !state.is_delegated_mode().await {
        if state.session_still_current(generation).await {
            services
                .install_inactive_maintenance_lease(generation)
                .await;
        }
        return;
    }
    {
        let _lifecycle = services.lifecycle_lock.lock().await;
        // Invalidate live lease before durable I/O so fan-out cannot pass validation mid-revoke.
        strip_in_memory_delegated_authority(&state, &services, generation).await;
    }
    match services
        .signal_delegated_teardown(state.clone(), generation, "lease_expired")
        .await
    {
        Ok(()) => {}
        Err(e) => {
            warn!("durable teardown after lease expiry failed: {e}; scheduling autonomous retry");
            services.schedule_durable_revoke(state, generation, "lease_expired");
        }
    }
}

async fn run_autonomous_durable_revoke(
    services: &Arc<TwitchServices>,
    state: Arc<AppState>,
    generation: DelegatedGeneration,
    reason: &str,
) {
    let mut backoff = Duration::from_millis(200);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    loop {
        if !durable_revoke_still_needed(&state) {
            return;
        }
        match services
            .signal_delegated_teardown(state.clone(), generation, reason)
            .await
        {
            Ok(()) if !durable_revoke_still_needed(&state) => return,
            Ok(()) => {}
            Err(e) => {
                warn!("autonomous durable revoke retry pending: {e}");
            }
        }
        if !durable_revoke_still_needed(&state) {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
    }
}

/// Race a network future against the absolute lease deadline for `generation`.
/// Deadline wins over a hung request; completion rejects stale generation/deadline snapshots.
async fn race_against_lease_deadline<T>(
    services: &TwitchServices,
    generation: DelegatedGeneration,
    network: impl std::future::Future<Output = T>,
) -> Result<T, ()> {
    let snap = services.authority_lease_snapshot().await;
    if snap.generation != generation {
        return Err(());
    }
    let remaining = snap
        .deadline
        .saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err(());
    }
    tokio::select! {
        biased;
        _ = tokio::time::sleep(remaining) => Err(()),
        result = network => {
            let snap2 = services.authority_lease_snapshot().await;
            if snap2.generation != generation || snap2.deadline != snap.deadline {
                return Err(());
            }
            if snap2.deadline <= std::time::Instant::now() {
                return Err(());
            }
            Ok(result)
        }
    }
}

async fn execute_delegated_teardown(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    code: &str,
) -> Result<(), String> {
    let need_personal_fallback = {
        let _lifecycle = services.lifecycle_lock.lock().await;
        if state.current_delegated_generation() != generation {
            return Ok(());
        }
        {
            let delegated = state.delegated.read().await;
            if delegated
                .as_ref()
                .is_some_and(|s| s.generation != generation)
            {
                return Ok(());
            }
        }
        warn!("delegated session ended: {}", code);

        // 1. Durable revoke (idempotent): retry completes remaining work when file/tombstone incomplete.
        if durable_revoke_still_needed(&state) {
            if let Err(e) = state.durable_revoke_delegated().await {
                // Clear in-memory authority first, then surface Err for durable retry.
                strip_in_memory_delegated_authority(&state, &services, generation).await;
                return Err(e.to_string());
            }
        }

        // 2. Clear memory if gen matches.
        {
            let mut delegated = state.delegated.write().await;
            if state.current_delegated_generation() != generation {
                return Ok(());
            }
            if delegated
                .as_ref()
                .is_some_and(|s| s.generation == generation)
            {
                *delegated = None;
            } else if delegated
                .as_ref()
                .is_some_and(|s| s.generation != generation)
            {
                return Ok(());
            }
        }

        // 3. Stop workers.
        stop_delegated_worker_handles(&services.refresh_handle, &services.watch_handle, None).await;

        // 4. Mode persist: do not skip based solely on was_active after memory already cleared.
        let mem_delegated = *state.active_mode.read().await == TwitchActiveMode::Delegated;
        let disk_delegated = disk_active_mode_is_delegated(&state);
        let need_mode = mem_delegated || disk_delegated;
        if need_mode && state.current_delegated_generation() == generation {
            let personal = state.personal_tokens.read().await.clone();
            if tokens_saved(&personal) {
                {
                    let mut tw = state.twitch.write().await;
                    clear_live_runtime_fields(&mut tw);
                    tw.tokens = personal;
                    *state.active_mode.write().await = TwitchActiveMode::Local;
                }
                if let Err(e) = state.save_active_mode().await {
                    return Err(e.to_string());
                }
            } else {
                stop_twitch_clients(&services).await;
                let mut tw = state.twitch.write().await;
                tw.tokens = TwitchTokenFile::default();
                clear_live_runtime_fields(&mut tw);
                *state.active_mode.write().await = TwitchActiveMode::Local;
                drop(tw);
                if let Err(e) = state.save_active_mode().await {
                    return Err(e.to_string());
                }
            }
        }
        need_mode
    };

    // 5. Fallback/restart personal after mode persisted.
    crate::kick::teardown_delegated_kick_live(&state).await;
    if need_personal_fallback && state.current_delegated_generation() == generation {
        let personal_ok = tokens_saved(&state.personal_tokens.read().await.clone());
        if personal_ok {
            restart_twitch_clients(state.clone(), services.clone()).await;
        }
        crate::kick::sync_live_identity(state).await;
    } else if state.current_delegated_generation() == generation {
        crate::kick::sync_live_identity(state).await;
    }
    Ok(())
}

async fn end_delegated_session_after_key_invalid(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    code: &str,
    generation: DelegatedGeneration,
) -> Result<(), String> {
    match services
        .signal_delegated_teardown(state.clone(), generation, code)
        .await
    {
        Ok(()) => Ok(()),
        Err(err) => {
            warn!("delegated teardown failed ({code}): {err}; scheduling autonomous retry");
            services.schedule_durable_revoke(state, generation, code);
            Err(err)
        }
    }
}

/// Map a connection-key failure into `(error_code, user_message, http_status)`.
pub fn connection_key_error_parts(
    err: &anyhow::Error,
) -> Option<(String, String, axum::http::StatusCode)> {
    let api = err.downcast_ref::<SyndicateApiError>()?;
    let status = match api.code.as_str() {
        "invalid_key" | "expired" | "revoked" => axum::http::StatusCode::UNAUTHORIZED,
        "missing_scopes" => axum::http::StatusCode::FORBIDDEN,
        "rate_limited" => axum::http::StatusCode::TOO_MANY_REQUESTS,
        "token_unavailable" => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        _ => axum::http::StatusCode::BAD_GATEWAY,
    };
    Some((
        api.code.clone(),
        syndicate_connection::user_message_for_error(api),
        status,
    ))
}

/// Exchange a Syndicate connection key and start Twitch as that channel (takeover).
pub async fn apply_connection_key(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    key: &str,
) -> Result<()> {
    let intent = services.bump_apply_intent();
    let exchange = syndicate_connection::exchange(key).await?;
    services.ensure_apply_intent_current(intent)?;
    apply_exchange_session(state, services, key, exchange, true, Some(intent)).await
}

/// Persist an exchanged takeover session. When `activate` is true, make it the live identity.
async fn apply_exchange_session(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    key: &str,
    exchange: syndicate_connection::ExchangeSuccess,
    activate: bool,
    apply_intent: Option<u64>,
) -> Result<()> {
    let generation = {
        let _lifecycle = services.lifecycle_lock.lock().await;
        if let Some(intent) = apply_intent {
            services.ensure_apply_intent_current(intent)?;
        }
        let new_generation = state.peek_next_delegated_generation();
        let mut session = DelegatedSessionFile {
            generation: new_generation,
            connection_key: key.trim().to_string(),
            client_id: exchange.twitch.client_id.clone(),
            access_token: exchange.twitch.access_token.clone(),
            channel_login: exchange.channel.login.clone(),
            channel_twitch_id: exchange.channel.twitch_id.clone(),
            display_name: exchange.channel.display_name.clone(),
            label: exchange.connection.label.clone(),
            scopes: exchange.twitch.scopes.clone(),
            twitch_expires_at: exchange.twitch.expires_at.clone(),
            connection_expires_at: exchange.connection.expires_at.clone(),
            ..Default::default()
        };
        crate::kick::apply_kick_to_delegated(&mut session, &exchange);

        if let Some(intent) = apply_intent {
            services.ensure_apply_intent_current(intent)?;
        }

        state.persist_delegated_session(&session)?;
        if activate {
            storage_write_active_mode_delegated(&state)?;
        }
        state.clear_delegated_revoked_tombstone()?;

        if let Some(intent) = apply_intent {
            services.ensure_apply_intent_current(intent)?;
        }

        stop_delegated_tasks(&services).await;
        state.publish_delegated_generation(new_generation);
        services
            .teardown_coordinator
            .install_generation_async(new_generation)
            .await
            .map_err(|e| anyhow!(e))?;
        services
            .install_validated_authority_lease(
                new_generation,
                session.connection_expires_at.as_deref(),
            )
            .await;

        *state.delegated.write().await = Some(session);
        if activate {
            {
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = install_tokens_from_exchange(&exchange);
                *state.active_mode.write().await = TwitchActiveMode::Delegated;
            }
            drop(_lifecycle);
            restart_twitch_clients(state.clone(), services.clone()).await;
        }
        new_generation
    };

    start_delegated_refresh_loop(state.clone(), services.clone(), generation).await;
    start_delegated_watch_loop(state.clone(), services, generation).await;
    crate::kick::sync_live_identity(state).await;
    Ok(())
}

fn storage_write_active_mode_delegated(state: &AppState) -> Result<()> {
    if state.readonly {
        return Ok(());
    }
    state
        .durable_fail
        .fail(&state.durable_fail.save_active_mode, "save_active_mode")
        .map_err(|e| anyhow!(e))?;
    crate::storage::write_json(
        &state.paths.twitch_active_mode,
        &crate::config_types::TwitchActiveModeFile {
            mode: TwitchActiveMode::Delegated,
        },
    )
}

async fn ensure_delegated_refresh_loop(state: Arc<AppState>, services: Arc<TwitchServices>) {
    if state.delegated.read().await.is_none() {
        return;
    }
    let generation = state.current_delegated_generation();
    clear_finished_generation_task(&services.refresh_handle, generation).await;
    clear_finished_generation_task(&services.watch_handle, generation).await;
    let running = services
        .refresh_handle
        .read()
        .await
        .as_ref()
        .is_some_and(|t| generation_task_alive(t, generation));
    let watch_running = services
        .watch_handle
        .read()
        .await
        .as_ref()
        .is_some_and(|t| generation_task_alive(t, generation));
    // Restored sessions must revalidate promptly — never start workers on an inactive lease.
    if !running || !watch_running {
        let needs_pending = {
            let lease = services.authority_lease.lock().await;
            lease.generation() != generation || lease.is_expired()
        };
        if needs_pending {
            services.install_pending_authority_lease(generation).await;
        }
    }
    if !running {
        start_delegated_refresh_loop(state.clone(), services.clone(), generation).await;
    }
    if !watch_running {
        start_delegated_watch_loop(state, services, generation).await;
    }
}

#[cfg(test)]
static REFRESH_COMMIT_GATE: OnceLock<RefreshGateMutex<Option<RefreshCommitGateState>>> =
    OnceLock::new();

#[cfg(test)]
static REFRESH_LIVE_GATE: OnceLock<RefreshGateMutex<Option<RefreshCommitGateState>>> =
    OnceLock::new();

#[cfg(test)]
static REFRESH_TWITCH_PUBLISH_GATE: OnceLock<RefreshGateMutex<Option<RefreshCommitGateState>>> =
    OnceLock::new();

#[cfg(test)]
static REFRESH_BYPASS_SLEEP: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
pub(crate) fn set_refresh_bypass_sleep(enabled: bool) {
    REFRESH_BYPASS_SLEEP.store(enabled, Ordering::SeqCst);
}

#[cfg(test)]
struct RefreshCommitGateState {
    arrived: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[cfg(test)]
pub(crate) struct RefreshGateCleanup;

#[cfg(test)]
pub(crate) fn clear_refresh_twitch_gates_blocking() {
    for slot in [
        REFRESH_COMMIT_GATE.get(),
        REFRESH_LIVE_GATE.get(),
        REFRESH_TWITCH_PUBLISH_GATE.get(),
    ]
    .into_iter()
    .flatten()
    {
        if let Ok(mut guard) = slot.lock() {
            *guard = None;
        }
    }
}

#[cfg(test)]
impl Drop for RefreshGateCleanup {
    fn drop(&mut self) {
        clear_refresh_twitch_gates_blocking();
        crate::kick::clear_refresh_kick_gates_blocking();
        crate::delegated_refresh_observability::reset_side_effect_counters();
        REFRESH_BYPASS_SLEEP.store(false, Ordering::SeqCst);
    }
}

/// Pause delegated refresh immediately before durable commit (deterministic race tests).
#[cfg(test)]
pub(crate) async fn install_refresh_commit_gate() -> (
    RefreshGateCleanup,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let slot = REFRESH_COMMIT_GATE.get_or_init(|| RefreshGateMutex::new(None));
    *slot.lock().unwrap() = Some(RefreshCommitGateState {
        arrived: arrived_tx,
        resume: resume_rx,
    });
    (RefreshGateCleanup, arrived_rx, resume_tx)
}

/// Pause delegated refresh immediately before publishing IRC/EventSub handles.
#[cfg(test)]
pub(crate) async fn install_refresh_twitch_publish_gate() -> (
    RefreshGateCleanup,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let slot = REFRESH_TWITCH_PUBLISH_GATE.get_or_init(|| RefreshGateMutex::new(None));
    *slot.lock().unwrap() = Some(RefreshCommitGateState {
        arrived: arrived_tx,
        resume: resume_rx,
    });
    (RefreshGateCleanup, arrived_rx, resume_tx)
}

/// Pause delegated refresh immediately before live Twitch/Kick publication.
#[cfg(test)]
pub(crate) async fn install_refresh_live_gate() -> (
    RefreshGateCleanup,
    oneshot::Receiver<()>,
    oneshot::Sender<()>,
) {
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let slot = REFRESH_LIVE_GATE.get_or_init(|| RefreshGateMutex::new(None));
    *slot.lock().unwrap() = Some(RefreshCommitGateState {
        arrived: arrived_tx,
        resume: resume_rx,
    });
    (RefreshGateCleanup, arrived_rx, resume_tx)
}

#[cfg(test)]
async fn pause_refresh_gate(slot: Option<&RefreshGateMutex<Option<RefreshCommitGateState>>>) {
    let Some(slot) = slot else {
        return;
    };
    let gate = {
        let mut guard = slot.lock().unwrap();
        guard.take()
    };
    let Some(gate) = gate else {
        return;
    };
    let _ = gate.arrived.send(());
    let _ = gate.resume.await;
}

#[cfg(test)]
async fn refresh_commit_gate_pause_if_installed() {
    pause_refresh_gate(REFRESH_COMMIT_GATE.get()).await;
}

#[cfg(test)]
async fn refresh_live_gate_pause_if_installed() {
    pause_refresh_gate(REFRESH_LIVE_GATE.get()).await;
}

#[cfg(test)]
async fn refresh_twitch_publish_gate_pause_if_installed() {
    pause_refresh_gate(REFRESH_TWITCH_PUBLISH_GATE.get()).await;
}

#[cfg(not(test))]
async fn refresh_commit_gate_pause_if_installed() {}

#[cfg(not(test))]
async fn refresh_live_gate_pause_if_installed() {}

#[cfg(not(test))]
async fn refresh_twitch_publish_gate_pause_if_installed() {}

#[cfg(test)]
async fn delegated_refresh_sleep(duration: Duration) {
    if !REFRESH_BYPASS_SLEEP.load(Ordering::SeqCst) {
        tokio::time::sleep(duration).await;
    }
}

#[cfg(not(test))]
async fn delegated_refresh_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

async fn delegated_refresh_may_commit(
    state: &AppState,
    services: &TwitchServices,
    generation: DelegatedGeneration,
) -> bool {
    if state.current_delegated_generation() != generation {
        return false;
    }
    if !state.session_still_current(generation).await {
        return false;
    }
    if state.paths.twitch_delegated_revoke_pending.is_file() {
        return false;
    }
    if state.paths.twitch_delegated_revoked.is_file() && !state.paths.twitch_delegated.is_file() {
        return false;
    }
    if services.authority_lease_expired().await {
        return false;
    }
    let lease = services.read_authority_lease().await;
    lease.allows_syndicate_revalidation(generation)
}

async fn delegated_refresh_live_may_publish(
    state: &AppState,
    services: &TwitchServices,
    generation: DelegatedGeneration,
) -> bool {
    if !state.is_delegated_mode().await {
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

/// Publish refreshed Twitch/Kick live state only when `generation` remains current.
async fn publish_delegated_refresh_live_if_current(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    exchange: &syndicate_connection::ExchangeSuccess,
    channel_login: &str,
) {
    let should_restart_twitch = {
        let _lifecycle = services.lifecycle_lock.lock().await;
        if !delegated_refresh_live_may_publish(&state, &services, generation).await {
            false
        } else {
            {
                let mut tw = state.twitch.write().await;
                tw.tokens = install_tokens_from_exchange(exchange);
            }
            info!("delegated Twitch token refreshed for {}", channel_login);
            stop_twitch_clients(&services).await;
            delegated_refresh_live_may_publish(&state, &services, generation).await
        }
    };

    if should_restart_twitch {
        if let Err(e) = start_irc(state.clone(), services.clone(), Some(generation)).await {
            warn!("IRC start failed: {e}");
        }
        if let Err(e) = start_eventsub(state.clone(), services.clone(), Some(generation)).await {
            warn!("EventSub start failed: {e}");
        }
    } else if state.session_still_current(generation).await {
        info!(
            "delegated Twitch token refreshed (inactive) for {}",
            channel_login
        );
    }

    crate::kick::sync_live_identity_for_generation(state, generation, Some(&services)).await;
}

/// Commit a successful delegated refresh under lifecycle lock.
///
/// Returns `Ok(true)` when committed, `Ok(false)` when generation N was superseded or revoked
/// (zero durable, memory, lease, token, or worker side effects).
async fn apply_delegated_refresh_commit(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    merged: DelegatedSessionFile,
    exchange: &syndicate_connection::ExchangeSuccess,
    connection_key: &str,
) -> Result<bool> {
    refresh_commit_gate_pause_if_installed().await;

    let _lifecycle = services.lifecycle_lock.lock().await;
    if !delegated_refresh_may_commit(&state, &services, generation).await {
        return Ok(false);
    }

    if let Err(e) = state.persist_delegated_session(&merged) {
        warn!(
            "delegated refresh persist failed: {}",
            redact_connection_key(&format!("{e:#}"), connection_key)
        );
        return Err(e);
    }

    *state.delegated.write().await = Some(merged.clone());
    if let Err(e) = services
        .renew_after_successful_remote_validation(
            generation,
            exchange.connection.expires_at.as_deref(),
        )
        .await
    {
        warn!(
            "delegated refresh lease renew rejected: {}",
            redact_connection_key(&e, connection_key)
        );
        drop(_lifecycle);
        fail_closed_lease_expired(state.clone(), services.clone(), generation).await;
        return Ok(false);
    }
    drop(_lifecycle);

    refresh_live_gate_pause_if_installed().await;

    publish_delegated_refresh_live_if_current(
        state.clone(),
        services.clone(),
        generation,
        exchange,
        &merged.channel_login,
    )
    .await;

    Ok(true)
}

async fn start_delegated_refresh_loop(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
) {
    let (grant_tx, grant_rx) = oneshot::channel();
    let state2 = state.clone();
    let services2 = services.clone();
    let handle = tokio::spawn(async move {
        if grant_rx.await.is_err() {
            return;
        }
        loop {
            if services2.authority_lease_expired().await {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }

            let (key, expires_at, conn_exp) = {
                let d = state2.delegated.read().await;
                match d.as_ref() {
                    Some(s) if s.generation == generation => (
                        s.connection_key.clone(),
                        s.twitch_expires_at.clone(),
                        s.connection_expires_at.clone(),
                    ),
                    _ => break,
                }
            };
            // Local stored expiry may only shorten — never renew — the lease.
            if let Some(ref iso) = conn_exp {
                services2
                    .cap_authority_lease_by_connection_expiry(Some(iso.as_str()))
                    .await;
            }

            let sleep_for = match chrono::DateTime::parse_from_rfc3339(&expires_at) {
                Ok(exp) => {
                    let refresh_at = exp - chrono::Duration::minutes(2);
                    let now = chrono::Utc::now();
                    let wait = refresh_at.signed_duration_since(now);
                    if wait.num_seconds() > 0 {
                        Duration::from_secs(wait.num_seconds() as u64)
                    } else {
                        Duration::from_secs(5)
                    }
                }
                Err(_) => MAX_DELEGATED_REVOCATION_DELAY,
            };
            let sleep_for = services2.authority_sleep_budget(sleep_for).await;
            if sleep_for.is_zero() {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }
            delegated_refresh_sleep(sleep_for).await;

            // Absolute lease: recheck generation + expiry after every sleep before network.
            if !state2.session_still_current(generation).await {
                break;
            }
            if services2.authority_lease_expired().await {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }

            let req_timeout = services2
                .authority_request_timeout(SYNDICATE_HTTP_TIMEOUT)
                .await;
            if req_timeout.is_zero() {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }

            let refresh_result = race_against_lease_deadline(&services2, generation, async {
                tokio::time::timeout(req_timeout, syndicate_connection::refresh(&key)).await
            })
            .await;

            let refresh_result = match refresh_result {
                Err(()) => {
                    fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                    break;
                }
                Ok(Err(_)) => Err(anyhow!("Syndicate refresh timed out within lease budget")),
                Ok(Ok(r)) => r,
            };

            match refresh_result {
                Ok(exchange) => {
                    let merged = {
                        let guard = state2.delegated.read().await;
                        let Some(ref s) = *guard else {
                            break;
                        };
                        if s.generation != generation {
                            break;
                        }
                        crate::kick::merge_delegated_session_from_exchange(s.clone(), &exchange)
                    };
                    match apply_delegated_refresh_commit(
                        state2.clone(),
                        services2.clone(),
                        generation,
                        merged,
                        &exchange,
                        &key,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(_) => continue,
                    }
                }
                Err(e) => {
                    if let Some(api) = e.downcast_ref::<SyndicateApiError>() {
                        match api.code.as_str() {
                            "revoked" | "expired" | "invalid_key" => {
                                match end_delegated_session_after_key_invalid(
                                    state2.clone(),
                                    services2.clone(),
                                    &api.code,
                                    generation,
                                )
                                .await
                                {
                                    Ok(()) => break,
                                    Err(err) => {
                                        warn!("teardown after hard refresh fail: {err}");
                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                    }
                                }
                            }
                            "token_unavailable" | "rate_limited" => {
                                warn!("delegated refresh soft-fail: {} — retrying", api.code);
                                let wait = services2
                                    .authority_sleep_budget(Duration::from_secs(60))
                                    .await;
                                if wait.is_zero() {
                                    fail_closed_lease_expired(
                                        state2.clone(),
                                        services2.clone(),
                                        generation,
                                    )
                                    .await;
                                    break;
                                }
                                tokio::time::sleep(wait).await;
                                if !state2.session_still_current(generation).await {
                                    break;
                                }
                                if services2.authority_lease_expired().await {
                                    fail_closed_lease_expired(
                                        state2.clone(),
                                        services2.clone(),
                                        generation,
                                    )
                                    .await;
                                    break;
                                }
                            }
                            _ => {
                                warn!("delegated refresh failed: {} — retrying", api.code);
                                let wait = services2
                                    .authority_sleep_budget(Duration::from_secs(60))
                                    .await;
                                if wait.is_zero() {
                                    fail_closed_lease_expired(
                                        state2.clone(),
                                        services2.clone(),
                                        generation,
                                    )
                                    .await;
                                    break;
                                }
                                tokio::time::sleep(wait).await;
                                if !state2.session_still_current(generation).await {
                                    break;
                                }
                                if services2.authority_lease_expired().await {
                                    fail_closed_lease_expired(
                                        state2.clone(),
                                        services2.clone(),
                                        generation,
                                    )
                                    .await;
                                    break;
                                }
                            }
                        }
                    } else {
                        warn!(
                            "delegated refresh error: {} — retrying",
                            redact_connection_key(&format!("{e:#}"), &key)
                        );
                        let wait = services2
                            .authority_sleep_budget(Duration::from_secs(60))
                            .await;
                        if wait.is_zero() {
                            fail_closed_lease_expired(
                                state2.clone(),
                                services2.clone(),
                                generation,
                            )
                            .await;
                            break;
                        }
                        tokio::time::sleep(wait).await;
                        if !state2.session_still_current(generation).await {
                            break;
                        }
                        if services2.authority_lease_expired().await {
                            fail_closed_lease_expired(
                                state2.clone(),
                                services2.clone(),
                                generation,
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
        }
        release_generation_slot_if_owned(&services2.refresh_handle, generation).await;
    });
    if install_generation_task(&services.refresh_handle, generation, handle).await {
        let _ = grant_tx.send(());
        observe_and_restart_finished_worker(state, services, generation, DelegatedWorker::Refresh);
    }
}

async fn consume_connection_key_events(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    key: &str,
) -> Result<bool> {
    if !state.session_still_current(generation).await {
        return Ok(false);
    }
    if services.authority_lease_expired().await {
        fail_closed_lease_expired(state, services, generation).await;
        return Ok(true);
    }

    let connect_timeout = services
        .authority_request_timeout(SYNDICATE_HTTP_TIMEOUT)
        .await;
    if connect_timeout.is_zero() {
        fail_closed_lease_expired(state, services, generation).await;
        return Ok(true);
    }

    let url = connection_key_events_url(&syndicate_connection::api_base());
    let send_fut = syndicate_connection::syndicate_http_client()
        .get(&url)
        .header("Accept", "text/event-stream")
        .header("Authorization", connection_key_authorization(key))
        .send();
    let res = match race_against_lease_deadline(&services, generation, async {
        tokio::time::timeout(connect_timeout, send_fut).await
    })
    .await
    {
        Err(()) => {
            fail_closed_lease_expired(state, services, generation).await;
            return Ok(true);
        }
        Ok(Err(_)) => {
            return Err(anyhow!(
                "connection key watch connect timed out within lease budget"
            ));
        }
        Ok(Ok(r)) => r.map_err(|e| {
            anyhow!(
                "connection key watch request failed: {}",
                redact_connection_key(&e.to_string(), key)
            )
        })?,
    };
    if res.status() == reqwest::StatusCode::UNAUTHORIZED {
        let _ =
            end_delegated_session_after_key_invalid(state, services, "revoked", generation).await;
        return Ok(true);
    }
    if !res.status().is_success() {
        return Err(anyhow!("connection key watch HTTP {}", res.status()));
    }
    // Successful remote stream open counts as validation for this generation.
    if state.session_still_current(generation).await {
        if services.authority_lease_expired().await {
            fail_closed_lease_expired(state, services, generation).await;
            return Ok(true);
        }
        let expires = state
            .delegated
            .read()
            .await
            .as_ref()
            .and_then(|s| s.connection_expires_at.clone());
        if let Err(e) = services
            .renew_after_successful_remote_validation(generation, expires.as_deref())
            .await
        {
            warn!("lease renew rejected after watch connect: {e}");
            fail_closed_lease_expired(state, services, generation).await;
            return Ok(true);
        }
    }
    let mut stream = res.bytes_stream();
    let mut buf = String::new();
    loop {
        if !state.session_still_current(generation).await {
            return Ok(false);
        }
        if services.authority_lease_expired().await {
            fail_closed_lease_expired(state, services, generation).await;
            return Ok(true);
        }
        let read_timeout = services
            .authority_request_timeout(SYNDICATE_SSE_READ_TIMEOUT)
            .await;
        if read_timeout.is_zero() {
            fail_closed_lease_expired(state, services, generation).await;
            return Ok(true);
        }
        let chunk = match race_against_lease_deadline(&services, generation, async {
            tokio::time::timeout(read_timeout, stream.next()).await
        })
        .await
        {
            Err(()) => {
                fail_closed_lease_expired(state, services, generation).await;
                return Ok(true);
            }
            Ok(Err(_)) => {
                return Err(anyhow!(
                    "connection key watch read timed out within lease budget"
                ));
            }
            Ok(Ok(chunk)) => chunk,
        };
        let Some(chunk) = chunk else {
            return Err(anyhow!("connection key watch stream ended"));
        };
        let bytes = chunk.map_err(|e| {
            anyhow!(
                "connection key watch stream error: {}",
                redact_connection_key(&e.to_string(), key)
            )
        })?;
        let frames = append_sse_chunk(&mut buf, &String::from_utf8_lossy(&bytes))
            .map_err(|SseBufferError::Overflow| anyhow!("connection key watch buffer overflow"))?;
        for frame in frames {
            if let Some(event) = parse_sse_json_data(&frame) {
                if event.get("type").and_then(|v| v.as_str()) == Some("revoked") {
                    let _ = end_delegated_session_after_key_invalid(
                        state.clone(),
                        services.clone(),
                        "revoked",
                        generation,
                    )
                    .await;
                    return Ok(true);
                }
            }
        }
    }
}

async fn start_delegated_watch_loop(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
) {
    let (grant_tx, grant_rx) = oneshot::channel();
    let state2 = state.clone();
    let services2 = services.clone();
    let handle = tokio::spawn(async move {
        if grant_rx.await.is_err() {
            return;
        }
        let mut consecutive_failures: u32 = 0;
        loop {
            if services2.authority_lease_expired().await {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }

            let key = {
                let d = state2.delegated.read().await;
                match d.as_ref() {
                    Some(s) if s.generation == generation => s.connection_key.clone(),
                    _ => break,
                }
            };

            match consume_connection_key_events(state2.clone(), services2.clone(), generation, &key)
                .await
            {
                Ok(true) => break,
                Ok(false) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    warn!(
                        "connection key watch error: {}",
                        redact_connection_key(&format!("{e:#}"), &key)
                    );
                    if consecutive_failures >= 3 {
                        if !state2.session_still_current(generation).await {
                            break;
                        }
                        if services2.authority_lease_expired().await {
                            fail_closed_lease_expired(
                                state2.clone(),
                                services2.clone(),
                                generation,
                            )
                            .await;
                            break;
                        }
                        let req_timeout = services2
                            .authority_request_timeout(SYNDICATE_HTTP_TIMEOUT)
                            .await;
                        if req_timeout.is_zero() {
                            fail_closed_lease_expired(
                                state2.clone(),
                                services2.clone(),
                                generation,
                            )
                            .await;
                            break;
                        }
                        let refresh_result =
                            race_against_lease_deadline(&services2, generation, async {
                                tokio::time::timeout(
                                    req_timeout,
                                    syndicate_connection::refresh(&key),
                                )
                                .await
                            })
                            .await;
                        match refresh_result {
                            Err(()) => {
                                fail_closed_lease_expired(
                                    state2.clone(),
                                    services2.clone(),
                                    generation,
                                )
                                .await;
                                break;
                            }
                            Ok(Err(_)) => {
                                warn!("watch revalidation timed out within lease budget");
                            }
                            Ok(Ok(Ok(exchange))) => {
                                if state2.session_still_current(generation).await {
                                    match services2
                                        .renew_after_successful_remote_validation(
                                            generation,
                                            exchange.connection.expires_at.as_deref(),
                                        )
                                        .await
                                    {
                                        Ok(()) => {
                                            consecutive_failures = 0;
                                        }
                                        Err(e) => {
                                            warn!(
                                                "watch revalidation lease renew rejected: {}",
                                                redact_connection_key(&e, &key)
                                            );
                                            fail_closed_lease_expired(
                                                state2.clone(),
                                                services2.clone(),
                                                generation,
                                            )
                                            .await;
                                            break;
                                        }
                                    }
                                }
                            }
                            Ok(Ok(Err(err))) => {
                                if let Some(api) = err.downcast_ref::<SyndicateApiError>() {
                                    match api.code.as_str() {
                                        "revoked" | "expired" | "invalid_key" => {
                                            match end_delegated_session_after_key_invalid(
                                                state2.clone(),
                                                services2.clone(),
                                                &api.code,
                                                generation,
                                            )
                                            .await
                                            {
                                                Ok(()) => break,
                                                Err(e) => {
                                                    warn!("teardown after watch revalidation: {e}");
                                                    tokio::time::sleep(Duration::from_millis(200))
                                                        .await;
                                                }
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !state2.session_still_current(generation).await {
                break;
            }
            let wait = services2
                .authority_sleep_budget(Duration::from_secs(5))
                .await;
            if wait.is_zero() {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }
            tokio::time::sleep(wait).await;
            // Recheck after sleep before next network attempt.
            if !state2.session_still_current(generation).await {
                break;
            }
            if services2.authority_lease_expired().await {
                fail_closed_lease_expired(state2.clone(), services2.clone(), generation).await;
                break;
            }
        }
        release_generation_slot_if_owned(&services2.watch_handle, generation).await;
    });
    if install_generation_task(&services.watch_handle, generation, handle).await {
        let _ = grant_tx.send(());
        observe_and_restart_finished_worker(state, services, generation, DelegatedWorker::Watch);
    }
}

/// Observe a generation worker until it finishes; restart via ensure if still current.
fn observe_and_restart_finished_worker(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
    which: DelegatedWorker,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !state.session_still_current(generation).await {
                return;
            }
            let slot = match which {
                DelegatedWorker::Refresh => &services.refresh_handle,
                DelegatedWorker::Watch => &services.watch_handle,
            };
            let status = {
                let guard = slot.read().await;
                match guard.as_ref() {
                    None => Some(true),                              // empty — may need restart
                    Some(t) if t.generation != generation => return, // superseded
                    Some(t) if t.handle.is_finished() => Some(true),
                    Some(_) => None, // still running
                }
            };
            if status == Some(true) {
                clear_finished_generation_task(slot, generation).await;
                if state.session_still_current(generation).await {
                    ensure_delegated_refresh_loop(state, services).await;
                }
                return;
            }
        }
    });
}

pub async fn restart_twitch_clients(state: Arc<AppState>, services: Arc<TwitchServices>) {
    stop_twitch_clients(&services).await;
    if let Err(e) = start_irc(state.clone(), services.clone(), None).await {
        warn!("IRC start failed: {e}");
    }
    if let Err(e) = start_eventsub(state.clone(), services.clone(), None).await {
        warn!("EventSub start failed: {e}");
    }
}

async fn stop_twitch_clients(services: &TwitchServices) {
    *services.irc_client.write().await = None;
    if let Some(h) = services.irc_handle.write().await.take() {
        h.abort();
    }
    if let Some(h) = services.eventsub_handle.write().await.take() {
        h.abort();
    }
}

async fn start_irc(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: Option<DelegatedGeneration>,
) -> Result<()> {
    let (login, token) = {
        let tw = state.twitch.read().await;
        let login = tw.tokens.login.clone().ok_or_else(|| anyhow!("no login"))?;
        let token = tw
            .tokens
            .access_token
            .clone()
            .ok_or_else(|| anyhow!("no token"))?;
        (login, token)
    };
    ensure_valid_token(&state).await?;
    services.ensure_delegated_authority(&state).await?;

    use twitch_irc::message::ServerMessage;

    let credentials = StaticLoginCredentials::new(login.clone(), Some(token));
    let config = ClientConfig::new_simple(credentials);
    let (mut incoming, client) = StreamSyncIrcClient::new(config);

    let channel = login.clone();

    if let Some(generation) = generation {
        refresh_twitch_publish_gate_pause_if_installed().await;
        let _lifecycle = services.lock_lifecycle().await;
        if !delegated_refresh_live_may_publish(&state, &services, generation).await {
            return Ok(());
        }
        let provenance = services
            .select_platform_credentials_under_lock(&state)
            .await?
            .provenance();

        let (grant_tx, grant_rx) = oneshot::channel();
        let broadcaster_login = login.clone();
        let state_irc = state.clone();
        let channel_log = channel.clone();
        let channel_join = channel.clone();
        let feed = state.feed.clone();
        let services_task = services.clone();
        let handle = tokio::spawn(async move {
            if grant_rx.await.is_err() {
                return;
            }
            crate::delegated_refresh_observability::record_irc_join();
            client.join(channel_join.clone()).ok();
            *services_task.irc_client.write().await = Some(IrcClientBundle { client, provenance });
            {
                let mut tw = state_irc.twitch.write().await;
                tw.connected = true;
                tw.channel = Some(channel_join);
            }
            while let Some(message) = incoming.recv().await {
                if !state_irc.session_still_current(generation).await {
                    break;
                }
                match message {
                    ServerMessage::GlobalUserState(msg) => {
                        store_broadcaster_user_state(
                            &state_irc,
                            msg.name_color.as_ref(),
                            Some(msg.user_name.as_str()),
                            &msg.badges,
                        );
                    }
                    ServerMessage::UserState(msg) => {
                        store_broadcaster_user_state(
                            &state_irc,
                            msg.name_color.as_ref(),
                            Some(msg.user_name.as_str()),
                            &msg.badges,
                        );
                    }
                    ServerMessage::Privmsg(msg) => {
                        if !state_irc.session_still_current(generation).await {
                            break;
                        }
                        let is_self = msg.sender.login.eq_ignore_ascii_case(&broadcaster_login);
                        if is_self {
                            store_broadcaster_user_state(
                                &state_irc,
                                msg.name_color.as_ref(),
                                Some(msg.sender.name.as_str()),
                                &msg.badges,
                            );
                        }
                        let evt = privmsg_to_chat_event(&msg, is_self);
                        crate::delegated_refresh_observability::record_delegated_chat_fanout();
                        feed.broadcast_all(&evt).await;
                    }
                    ServerMessage::Notice(_) => {}
                    _ => {}
                }
            }
            info!("IRC incoming ended for {channel_log}");
        });

        let _ = grant_tx.send(());
        *services.irc_handle.write().await = Some(handle);
        drop(_lifecycle);
        refresh_broadcaster_chat_color(&state).await;
        return Ok(());
    }

    let broadcaster_login = login.clone();
    let state_irc = state.clone();
    let channel_log = channel.clone();
    let feed = state.feed.clone();
    client.join(channel.clone()).ok();

    let handle = tokio::spawn(async move {
        while let Some(message) = incoming.recv().await {
            match message {
                ServerMessage::GlobalUserState(msg) => {
                    store_broadcaster_user_state(
                        &state_irc,
                        msg.name_color.as_ref(),
                        Some(msg.user_name.as_str()),
                        &msg.badges,
                    );
                }
                ServerMessage::UserState(msg) => {
                    store_broadcaster_user_state(
                        &state_irc,
                        msg.name_color.as_ref(),
                        Some(msg.user_name.as_str()),
                        &msg.badges,
                    );
                }
                ServerMessage::Privmsg(msg) => {
                    let is_self = msg.sender.login.eq_ignore_ascii_case(&broadcaster_login);
                    if is_self {
                        store_broadcaster_user_state(
                            &state_irc,
                            msg.name_color.as_ref(),
                            Some(msg.sender.name.as_str()),
                            &msg.badges,
                        );
                    }
                    let evt = privmsg_to_chat_event(&msg, is_self);
                    feed.broadcast_all(&evt).await;
                }
                ServerMessage::Notice(_) => {}
                _ => {}
            }
        }
        info!("IRC incoming ended for {channel_log}");
    });

    let provenance = {
        let _lifecycle = services.lock_lifecycle().await;
        services
            .select_platform_credentials_under_lock(&state)
            .await?
            .provenance()
    };
    *services.irc_client.write().await = Some(IrcClientBundle { client, provenance });

    {
        let mut tw = state.twitch.write().await;
        tw.connected = true;
        tw.channel = Some(channel.clone());
    }

    *services.irc_handle.write().await = Some(handle);
    refresh_broadcaster_chat_color(&state).await;
    Ok(())
}

async fn refresh_broadcaster_chat_color(state: &AppState) {
    if let Ok(color) = fetch_broadcaster_chat_color(state).await {
        let mut tw = state.twitch.write().await;
        tw.name_color = Some(color);
    }
}

async fn fetch_broadcaster_chat_color(state: &AppState) -> Result<String> {
    let user_id = state
        .twitch
        .read()
        .await
        .tokens
        .user_id
        .clone()
        .ok_or_else(|| anyhow!("no user_id"))?;
    let body = helix_get(state, &format!("/chat/color?user_id={user_id}")).await?;
    let color = body
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|row| row.get("color"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| anyhow!("no chat color in Helix response"))?;
    Ok(color.to_string())
}

fn store_broadcaster_user_state(
    state: &AppState,
    color: Option<&twitch_irc::message::RGBColor>,
    display_name: Option<&str>,
    badges: &[twitch_irc::message::Badge],
) {
    if let Ok(mut tw) = state.twitch.try_write() {
        if let Some(c) = color {
            tw.name_color = Some(c.to_string());
        }
        if let Some(name) = display_name {
            if !name.is_empty() {
                tw.display_name = Some(name.to_string());
            }
        }
        // USERSTATE after join/send is authoritative for channel badges.
        // GLOBALUSERSTATE often has an empty badge list — don't wipe channel badges with it.
        if !badges.is_empty() {
            tw.badges_raw.clear();
            for badge in badges {
                tw.badges_raw
                    .insert(badge.name.clone(), badge.version.clone());
            }
        }
    }
}

pub async fn send_chat_from_dock(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    text: &str,
) -> Result<()> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    if trimmed.starts_with('/') {
        handle_dock_command(state, services, trimmed).await?;
        return Ok(());
    }
    send_plain_chat(&state, &services, trimmed).await
}

/// Convert twitch-irc emotes (exclusive end index) to TMI.js shape `{ id: ["start-end"] }`.
fn emotes_to_json_map(emotes: &[twitch_irc::message::Emote]) -> Option<Value> {
    if emotes.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for emote in emotes {
        let start = emote.char_range.start;
        let end_inclusive = emote.char_range.end.saturating_sub(1);
        let range = format!("{start}-{end_inclusive}");
        let entry = map.entry(emote.id.clone()).or_insert_with(|| json!([]));
        if let Some(arr) = entry.as_array_mut() {
            arr.push(json!(range));
        }
    }
    Some(Value::Object(map))
}

fn badges_to_json(badges: &[twitch_irc::message::Badge]) -> (Vec<String>, Value) {
    let names: Vec<String> = badges.iter().map(|b| b.name.clone()).collect();
    let mut badges_raw = serde_json::Map::new();
    for badge in badges {
        badges_raw.insert(badge.name.clone(), json!(badge.version));
    }
    (names, Value::Object(badges_raw))
}

fn privmsg_to_chat_event(msg: &twitch_irc::message::PrivmsgMessage, is_self: bool) -> Value {
    let (badges, badges_raw) = badges_to_json(&msg.badges);
    let color = msg.name_color.as_ref().map(|c| c.to_string());
    let mut evt = json!({
        "type": "chat",
        "platform": "twitch",
        "ts": chrono::Utc::now().timestamp_millis(),
        "user": {
            "name": msg.sender.login.clone(),
            "displayName": msg.sender.name.clone(),
            "color": color,
            "badges": badges,
            "badgesRaw": badges_raw,
        },
        "message": msg.message_text,
        "self": is_self,
    });
    if let Some(emotes) = emotes_to_json_map(&msg.emotes) {
        evt["emotes"] = emotes;
    }
    evt
}

/// Push dock-sent chat to dock + overlay immediately (Twitch often does not IRC-echo your own sends).
/// Always includes badgesRaw + emotes when known; dock/overlay hide badges via showBadges config.
async fn broadcast_outgoing_chat(state: &AppState, services: &TwitchServices, message: &str) {
    if state.twitch.read().await.name_color.is_none() {
        refresh_broadcaster_chat_color(state).await;
    }

    let emotes = resolve_outgoing_emotes(state, services, message).await;

    let tw = state.twitch.read().await;
    let login = tw.tokens.login.clone().unwrap_or_default();
    let display = tw.display_name.clone().unwrap_or_else(|| login.clone());
    let color = tw.name_color.clone();
    let badges: Vec<String> = tw.badges_raw.keys().cloned().collect();
    let badges_raw = tw
        .badges_raw
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect::<serde_json::Map<String, Value>>();
    let mut evt = json!({
        "type": "chat",
        "platform": "twitch",
        "ts": chrono::Utc::now().timestamp_millis(),
        "user": {
            "name": login,
            "displayName": display,
            "color": color,
            "badges": badges,
            "badgesRaw": badges_raw,
        },
        "message": message,
        "self": true,
    });
    if let Some(emotes) = emotes {
        evt["emotes"] = emotes;
    }
    drop(tw);
    state.feed.broadcast_all(&evt).await;
}

/// Match whitespace-delimited tokens in `message` against the Helix emote catalog by name.
async fn resolve_outgoing_emotes(
    state: &AppState,
    services: &TwitchServices,
    message: &str,
) -> Option<Value> {
    let list = get_merged_emotes(state, services).await.ok()?;
    let mut by_name: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for emote in &list {
        let Some(name) = emote.get("name").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(id) = emote.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        by_name
            .entry(name.to_string())
            .or_insert_with(|| id.to_string());
    }
    emotes_from_message_text(message, &by_name)
}

/// Build TMI-style `{ id: ["start-end"] }` by matching whole whitespace-separated tokens.
fn emotes_from_message_text(
    message: &str,
    by_name: &std::collections::HashMap<String, String>,
) -> Option<Value> {
    if by_name.is_empty() || message.is_empty() {
        return None;
    }
    let chars: Vec<char> = message.chars().collect();
    let mut map = serde_json::Map::new();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        while i < chars.len() && !chars[i].is_whitespace() {
            i += 1;
        }
        let token: String = chars[start..i].iter().collect();
        if let Some(id) = by_name.get(&token) {
            let end_inclusive = i - 1;
            let range = format!("{start}-{end_inclusive}");
            let entry = map.entry(id.clone()).or_insert_with(|| json!([]));
            if let Some(arr) = entry.as_array_mut() {
                arr.push(json!(range));
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

async fn send_plain_chat(state: &AppState, services: &TwitchServices, trimmed: &str) -> Result<()> {
    let (provenance, channel, client) = services.select_irc_send_bundle(state).await?;
    services
        .race_delegated_platform_with_provenance(state, provenance, async {
            client
                .say(channel.clone(), trimmed.to_string())
                .await
                .map_err(|e| anyhow!("IRC say failed: {e}"))
        })
        .await??;
    broadcast_outgoing_chat(state, services, trimmed).await;
    info!("Sent chat to #{channel}: {trimmed}");
    Ok(())
}

async fn send_dock_privmsg(state: &AppState, services: &TwitchServices, text: &str) -> Result<()> {
    let (provenance, channel, client) = services.select_irc_send_bundle(state).await?;
    services
        .race_delegated_platform_with_provenance(state, provenance, async {
            client
                .privmsg(channel.clone(), text.to_string())
                .await
                .map_err(|e| anyhow!("IRC privmsg failed: {e}"))
        })
        .await??;
    info!("Sent IRC command to #{channel}: {text}");
    Ok(())
}

async fn handle_dock_command(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    text: &str,
) -> Result<()> {
    let parts: Vec<&str> = text.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("").to_lowercase();
    let args: Vec<&str> = parts.into_iter().skip(1).collect();
    match cmd.as_str() {
        "/slow" => {
            let raw = args.first().copied();
            let disable = match raw {
                None => true,
                Some(v) => {
                    v == "0" || v.eq_ignore_ascii_case("off") || v.eq_ignore_ascii_case("disable")
                }
            };
            if disable {
                update_chat_settings(&state, json!({ "slow_mode": false })).await?;
            } else {
                let mut wait: i64 = raw.unwrap_or("30").parse().unwrap_or(30);
                if wait <= 0 {
                    wait = 30;
                }
                wait = wait.clamp(3, 180);
                update_chat_settings(
                    &state,
                    json!({ "slow_mode": true, "slow_mode_wait_time": wait }),
                )
                .await?;
            }
        }
        "/slowoff" => {
            update_chat_settings(&state, json!({ "slow_mode": false })).await?;
        }
        "/ban" | "/unban" | "/timeout" => {
            send_dock_privmsg(&state, &services, text).await?;
        }
        _ => {
            send_dock_privmsg(&state, &services, text).await?;
        }
    }
    Ok(())
}

// ─── EventSub ───────────────────────────────────────────────────────────────

/// Twitch plan tier → display tier 1–3 (`1000`/`2000`/`3000`, Prime, or `1`/`2`/`3`).
pub fn twitch_tier_display_number(tier: &Value) -> Option<u8> {
    let s = match tier {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.trim().to_string(),
        _ => return None,
    };
    if s.is_empty() {
        return None;
    }
    if s == "1000" || s.eq_ignore_ascii_case("prime") {
        return Some(1);
    }
    if s == "2000" {
        return Some(2);
    }
    if s == "3000" {
        return Some(3);
    }
    if let Ok(n) = s.parse::<u32>() {
        if (1..=3).contains(&n) {
            return Some(n as u8);
        }
        if n == 1000 {
            return Some(1);
        }
        if n == 2000 {
            return Some(2);
        }
        if n == 3000 {
            return Some(3);
        }
    }
    None
}

pub fn format_sub_dock_detail(user: &str, tier: &Value) -> String {
    match twitch_tier_display_number(tier) {
        Some(n) => format!("{user} subscribed — Tier {n}"),
        None => format!("{user} subscribed"),
    }
}

pub fn format_resub_dock_detail(user: &str, months: &Value, tier: &Value, msg: &str) -> String {
    let tn = twitch_tier_display_number(tier);
    let months_s = value_display_string(months);
    let mut detail = user.to_string();
    if !months_s.is_empty() {
        detail = format!("{user} — {months_s} months");
        if let Some(n) = tn {
            detail.push_str(&format!(" — Tier {n}"));
        }
    } else if let Some(n) = tn {
        detail.push_str(&format!(" — Tier {n}"));
    }
    if !msg.is_empty() {
        detail.push_str(&format!(": {msg}"));
    }
    detail
}

pub fn format_gift_dock_detail(
    gifter: &str,
    total: &Value,
    tier: &Value,
    recipient: &str,
) -> String {
    let tn = twitch_tier_display_number(tier);
    let qty = total
        .as_u64()
        .or_else(|| total.as_str().and_then(|s| s.trim().parse::<u64>().ok()));
    if let Some(q) = qty {
        if let Some(n) = tn {
            return format!("{gifter} gifted {q} Tier {n} subs");
        }
        return format!("{gifter} gifted {q} subs");
    }
    if !recipient.is_empty() {
        if let Some(n) = tn {
            return format!("{gifter} gifted {recipient} a Tier {n} sub");
        }
        return format!("{gifter} gifted {recipient} a sub");
    }
    if let Some(n) = tn {
        return format!("{gifter} gifted a Tier {n} sub");
    }
    format!("{gifter} gifted a sub")
}

fn value_display_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

pub fn normalize_event_variables(vars: &Value) -> Value {
    let name = vars
        .get("name")
        .or(vars.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let user = vars
        .get("user")
        .or(vars.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    json!({
        "user": user,
        "name": name,
        "amount": vars.get("amount").or(vars.get("tier")).or(vars.get("bits")).unwrap_or(&Value::Null),
        "months": vars.get("months").unwrap_or(&Value::Null),
        "reward": vars.get("reward").or(vars.get("title")).unwrap_or(&Value::Null),
        "input": vars.get("input").or(vars.get("message")).unwrap_or(&Value::Null),
        "recipient": vars.get("recipient").unwrap_or(&Value::Null),
        "tier": vars.get("tier").or(vars.get("amount")).unwrap_or(&Value::Null),
        "bits": vars.get("bits").unwrap_or(&Value::Null),
        "raiders": vars.get("raiders").or(vars.get("viewers")).unwrap_or(&Value::Null),
    })
}

async fn handle_eventsub_notification(
    state: &AppState,
    feed: &FeedHub,
    sub_type: &str,
    event: &Value,
) {
    match sub_type {
        "channel.follow" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "follow",
                "data": { "variables": normalize_event_variables(&json!({ "name": user })) },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "follow",
                &format!("{user} followed"),
                Some("Follow"),
                None,
            ))
            .await;
        }
        "channel.subscribe" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "sub",
                    "data": { "variables": normalize_event_variables(&json!({ "name": user, "amount": tier })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "sub",
                &format_sub_dock_detail(user, &tier),
                Some("Sub"),
                None,
            ))
            .await;
        }
        "channel.cheer" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let bits = event.get("bits").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "cheer",
                    "data": { "variables": normalize_event_variables(&json!({ "name": user, "amount": bits })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "bits",
                &format!("{user} cheered {bits}"),
                Some("Bits"),
                None,
            ))
            .await;
        }
        "channel.subscription.message" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let months = event
                .get("cumulative_months")
                .or_else(|| event.get("streak_months"))
                .cloned()
                .unwrap_or(Value::Null);
            let msg = event
                .pointer("/message/text")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            let vars = normalize_event_variables(&json!({
                "name": user,
                "months": months,
                "input": msg,
                "amount": tier,
            }));
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "resub",
                "data": { "variables": vars },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "resub",
                &format_resub_dock_detail(user, &months, &tier, msg),
                Some("Resub"),
                None,
            ))
            .await;
        }
        "channel.subscription.gift" => {
            let gifter = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("Anonymous");
            let recipient = event
                .get("recipient_user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let total = event.get("total").cloned().unwrap_or(Value::Null);
            let tier = event.get("tier").cloned().unwrap_or(Value::Null);
            let vars = normalize_event_variables(&json!({
                "name": gifter,
                "recipient": recipient,
                "amount": total,
                "tier": tier,
            }));
            feed.broadcast_all(&json!({
                "type": "event-alert",
                "eventType": "gift",
                "data": { "variables": vars },
            }))
            .await;
            feed.broadcast_all(&make_dock_event(
                "gift",
                &format_gift_dock_detail(gifter, &total, &tier, recipient),
                Some("Gift"),
                None,
            ))
            .await;
        }
        "channel.raid" => {
            let from = event
                .get("from_broadcaster_user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let viewers = event.get("viewers").cloned().unwrap_or(Value::Null);
            feed
                .broadcast_all(&json!({
                    "type": "event-alert",
                    "eventType": "raid",
                    "data": { "variables": normalize_event_variables(&json!({ "name": from, "amount": viewers })) },
                }))
                .await;
            feed.broadcast_all(&make_dock_event(
                "raid",
                &format!(
                    "{from} raided{}",
                    if viewers.is_null() {
                        String::new()
                    } else {
                        format!(" with {viewers}")
                    }
                ),
                Some("Raid"),
                None,
            ))
            .await;
        }
        "channel.channel_points_custom_reward_redemption.add" => {
            let user = event
                .get("user_name")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let title = event
                .pointer("/reward/title")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let input = event
                .get("user_input")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let cost = event.pointer("/reward/cost").and_then(|v| v.as_u64());
            let mut detail = format!("{title} — {user}");
            if let Some(c) = cost {
                detail.push_str(&format!(" ({c} pts)"));
            }
            // Channel points are dock-only — never reach public overlays.
            feed.broadcast_readonly_dock(
                "default",
                &make_dock_event("redeem", &detail, Some("Channel Points"), None),
            )
            .await;
            if !input.is_empty() {
                let mut private = format!("{title} — {user}: {input}");
                if let Some(c) = cost {
                    private.push_str(&format!(" ({c} pts)"));
                }
                feed.broadcast_private_dock(
                    "default",
                    &make_dock_event("redeem", &private, Some("Channel Points"), None),
                    &state.dock_credentials,
                )
                .await;
            }
        }
        _ => {}
    }
}

async fn start_eventsub(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: Option<DelegatedGeneration>,
) -> Result<()> {
    let client_id = state.helix_client_id().await;
    if client_id.is_empty() {
        return Err(anyhow!("TWITCH_CLIENT_ID missing"));
    }
    ensure_valid_token(&state).await?;
    services.ensure_delegated_authority(&state).await?;
    let feed = state.feed.clone();
    let state2 = state.clone();

    if let Some(generation) = generation {
        let (grant_tx, grant_rx) = oneshot::channel();
        let handle = tokio::spawn(async move {
            if grant_rx.await.is_err() {
                return;
            }
            loop {
                if !state2.session_still_current(generation).await {
                    break;
                }
                if let Err(e) =
                    eventsub_session(state2.clone(), feed.clone(), Some(generation)).await
                {
                    warn!("EventSub session error: {e}");
                }
                if !state2.session_still_current(generation).await {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                let tw = state2.twitch.read().await;
                if tw.tokens.access_token.is_none() {
                    break;
                }
            }
        });
        refresh_twitch_publish_gate_pause_if_installed().await;
        let _lifecycle = services.lock_lifecycle().await;
        if !delegated_refresh_live_may_publish(&state, &services, generation).await {
            handle.abort();
            return Ok(());
        }
        let _ = grant_tx.send(());
        *services.eventsub_handle.write().await = Some(handle);
        return Ok(());
    }

    let handle = tokio::spawn(async move {
        loop {
            if let Err(e) = eventsub_session(state2.clone(), feed.clone(), None).await {
                warn!("EventSub session error: {e}");
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
            let tw = state2.twitch.read().await;
            if tw.tokens.access_token.is_none() {
                break;
            }
        }
    });
    *services.eventsub_handle.write().await = Some(handle);
    Ok(())
}

async fn eventsub_session(
    state: Arc<AppState>,
    feed: FeedHub,
    generation: Option<DelegatedGeneration>,
) -> Result<()> {
    if let Some(gen) = generation {
        if !state.session_still_current(gen).await {
            return Ok(());
        }
        crate::delegated_refresh_observability::record_eventsub_connect();
    }
    let (ws, _) = connect_async("wss://eventsub.wss.twitch.tv/ws").await?;
    let (_write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        if let Some(gen) = generation {
            if !state.session_still_current(gen).await {
                break;
            }
        }
        let msg = msg?;
        if !msg.is_text() {
            continue;
        }
        let parsed: EventSubEnvelope = serde_json::from_str(msg.to_text()?)?;
        let message_type = parsed.metadata.message_type.as_str();
        match message_type {
            "session_welcome" | "session_reconnect" => {
                if let Some(gen) = generation {
                    if !state.session_still_current(gen).await {
                        break;
                    }
                }
                if let Some(sid) = parsed.payload.session.as_ref().and_then(|s| s.id.clone()) {
                    subscribe_topics(&state, &sid, generation).await;
                }
            }
            "notification" => {
                if let Some(gen) = generation {
                    if !state.session_still_current(gen).await {
                        break;
                    }
                }
                let Some(sub_type) = parsed.metadata.subscription_type.as_deref() else {
                    continue;
                };
                if let Some(event) = parsed.payload.event.as_ref() {
                    handle_eventsub_notification(&state, &feed, sub_type, event).await;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

async fn subscribe_topics(
    state: &AppState,
    session_id: &str,
    generation: Option<DelegatedGeneration>,
) {
    if let Some(gen) = generation {
        if !state.session_still_current(gen).await {
            return;
        }
    }
    let user_id = match state.twitch.read().await.tokens.user_id.clone() {
        Some(id) => id,
        None => return,
    };
    let transport = json!({
        "method": "websocket",
        "session_id": session_id
    });
    let subs: Vec<(&str, &str, Value)> = vec![
        (
            "channel.follow",
            "2",
            json!({ "broadcaster_user_id": user_id, "moderator_user_id": user_id }),
        ),
        (
            "channel.subscribe",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.subscription.message",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.subscription.gift",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.cheer",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
        (
            "channel.raid",
            "1",
            json!({ "to_broadcaster_user_id": user_id }),
        ),
        (
            "channel.channel_points_custom_reward_redemption.add",
            "1",
            json!({ "broadcaster_user_id": user_id }),
        ),
    ];
    for (ty, ver, condition) in subs {
        let body = json!({
            "type": ty,
            "version": ver,
            "condition": condition,
            "transport": transport,
        });
        match helix_post(state, "/eventsub/subscriptions", body).await {
            Ok(_) => info!("EventSub subscribed: {ty}"),
            Err(e) => warn!("EventSub subscribe {ty}: {e}"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct EventSubEnvelope {
    metadata: EventSubMetadata,
    payload: EventSubPayload,
}

#[derive(Debug, Deserialize)]
struct EventSubMetadata {
    #[serde(rename = "message_type")]
    message_type: String,
    /// Present on `notification` only; omitted on `session_welcome` / keepalives.
    #[serde(rename = "subscription_type", default)]
    subscription_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventSubPayload {
    session: Option<EventSubSession>,
    event: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct EventSubSession {
    id: Option<String>,
}

/// Install pending lease and start revocation workers without activating platform clients.
async fn start_delegated_revalidation_only(
    state: Arc<AppState>,
    services: Arc<TwitchServices>,
    generation: DelegatedGeneration,
) {
    services.install_pending_authority_lease(generation).await;
    start_delegated_refresh_loop(state.clone(), services.clone(), generation).await;
    start_delegated_watch_loop(state, services, generation).await;
}

pub async fn maybe_autostart(state: Arc<AppState>, services: Arc<TwitchServices>) {
    if state.identity_rollback_pending() {
        warn!("identity rollback pending — skipping ambiguous Twitch autostart");
        return;
    }
    // Crash-persistent pending revoke: never activate stored delegated authority.
    if state.durable_revoke_pending() || state.paths.twitch_delegated_revoked.is_file() {
        // Resume durable cleanup from marker presence even when generation is 0 (B9).
        services.schedule_durable_revoke(state.clone(), 0, "startup_pending");
        let personal = state.personal_tokens.read().await.clone();
        if tokens_saved(&personal) {
            {
                let mut tw = state.twitch.write().await;
                clear_live_runtime_fields(&mut tw);
                tw.tokens = personal;
                *state.active_mode.write().await = TwitchActiveMode::Local;
            }
            let _ = state.save_active_mode().await;
            restart_twitch_clients(state, services).await;
        }
        return;
    }

    let active = *state.active_mode.read().await;
    let delegated_key = {
        state
            .delegated
            .read()
            .await
            .as_ref()
            .map(|d| d.connection_key.clone())
    };

    // Prefer refreshing a saved takeover key so tokens stay valid even if inactive.
    if let Some(key) = delegated_key {
        match syndicate_connection::refresh(&key).await {
            Ok(exchange) => {
                let activate = active == TwitchActiveMode::Delegated;
                match apply_exchange_session(
                    state.clone(),
                    services.clone(),
                    &key,
                    exchange,
                    activate,
                    None,
                )
                .await
                {
                    Ok(()) if activate => return,
                    Ok(()) => {
                        // Inactive takeover refreshed — continue to start personal if active is local.
                    }
                    Err(e) => {
                        warn!("delegated autostart apply failed: {e:#}");
                        if activate {
                            // Pending lease + workers first; do not activate platform clients until
                            // remote validation succeeds.
                            let generation = state.current_delegated_generation();
                            start_delegated_revalidation_only(
                                state.clone(),
                                services.clone(),
                                generation,
                            )
                            .await;
                            // Fall through to personal if available.
                        }
                    }
                }
            }
            Err(e) => {
                if let Some(api) = e.downcast_ref::<SyndicateApiError>() {
                    match api.code.as_str() {
                        "revoked" | "expired" | "invalid_key" => {
                            warn!("delegated session invalid on launch: {}", api.code);
                            let generation = state
                                .delegated
                                .read()
                                .await
                                .as_ref()
                                .map(|s| s.generation)
                                .unwrap_or_else(|| state.current_delegated_generation());
                            let _ =
                                remove_delegated_session(&state, &services, None, generation).await;
                            if active == TwitchActiveMode::Delegated {
                                // Fall through to personal if available.
                            }
                        }
                        _ => {
                            warn!(
                                "delegated refresh on launch failed: {api} — revalidating without platform activation"
                            );
                            let generation = state.current_delegated_generation();
                            start_delegated_revalidation_only(
                                state.clone(),
                                services.clone(),
                                generation,
                            )
                            .await;
                            if active == TwitchActiveMode::Delegated {
                                // Do not restart_twitch_clients with stored delegated tokens.
                                // Fall through to personal if available.
                            }
                        }
                    }
                } else {
                    warn!("delegated refresh on launch failed: {e:#}");
                    let generation = state.current_delegated_generation();
                    start_delegated_revalidation_only(state.clone(), services.clone(), generation)
                        .await;
                    if active == TwitchActiveMode::Delegated {
                        // Fall through to personal — never activate stored delegated on network fail.
                    }
                }
            }
        }
    }

    // Active mode Local (or delegated cleared): start personal OAuth if present.
    let personal = state.personal_tokens.read().await.clone();
    if tokens_saved(&personal) {
        {
            let mut tw = state.twitch.write().await;
            clear_live_runtime_fields(&mut tw);
            tw.tokens = personal;
            *state.active_mode.write().await = TwitchActiveMode::Local;
        }
        let _ = state.save_active_mode().await;
        restart_twitch_clients(state.clone(), services.clone()).await;
        ensure_delegated_refresh_loop(state, services).await;
        return;
    }

    // No personal — keep delegated workers revalidating, but do not activate platform clients
    // until remote validation succeeds (apply_exchange_session / refresh success path).
    if state.delegated.read().await.is_some() {
        let generation = state.current_delegated_generation();
        start_delegated_revalidation_only(state, services, generation).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twitch_irc::message::{Badge, Emote};

    #[test]
    fn connection_key_error_parts_maps_codes() {
        let err: anyhow::Error = SyndicateApiError {
            code: "revoked".into(),
            message: "gone".into(),
            http_status: 401,
        }
        .into();
        let (code, message, status) = connection_key_error_parts(&err).expect("mapped");
        assert_eq!(code, "revoked");
        assert!(message.contains("revoked"));
        assert_eq!(status, axum::http::StatusCode::UNAUTHORIZED);
    }

    fn make_emote(id: &str, start: usize, end_exclusive: usize) -> Emote {
        Emote {
            id: id.to_string(),
            char_range: start..end_exclusive,
            code: String::new(),
        }
    }

    #[test]
    fn emotes_to_json_map_single_range() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5)]).unwrap();
        assert_eq!(map["25"], json!(["0-4"]));
    }

    #[test]
    fn emotes_to_json_map_groups_same_id() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5), make_emote("25", 10, 15)]).unwrap();
        assert_eq!(map["25"], json!(["0-4", "10-14"]));
    }

    #[test]
    fn emotes_to_json_map_multiple_ids() {
        let map = emotes_to_json_map(&[make_emote("25", 0, 5), make_emote("1902", 6, 11)]).unwrap();
        assert_eq!(map["25"], json!(["0-4"]));
        assert_eq!(map["1902"], json!(["6-10"]));
    }

    #[test]
    fn emotes_to_json_map_empty_returns_none() {
        assert!(emotes_to_json_map(&[]).is_none());
    }

    #[test]
    fn badges_to_json_maps_names_and_versions() {
        let badges = vec![
            Badge {
                name: "moderator".to_string(),
                version: "1".to_string(),
            },
            Badge {
                name: "subscriber".to_string(),
                version: "12".to_string(),
            },
        ];
        let (names, raw) = badges_to_json(&badges);
        assert_eq!(names, vec!["moderator", "subscriber"]);
        assert_eq!(raw["moderator"], json!("1"));
        assert_eq!(raw["subscriber"], json!("12"));
    }

    #[test]
    fn privmsg_to_chat_event_includes_emotes_and_badges() {
        let msg = twitch_irc::message::PrivmsgMessage {
            channel_login: "channel".to_string(),
            channel_id: "1".to_string(),
            sender: twitch_irc::message::TwitchUserBasics {
                id: "2".to_string(),
                login: "viewer".to_string(),
                name: "Viewer".to_string(),
            },
            badge_info: vec![],
            badges: vec![Badge {
                name: "subscriber".to_string(),
                version: "3".to_string(),
            }],
            bits: None,
            name_color: None,
            emotes: vec![make_emote("25", 0, 5)],
            message_id: "msg-1".to_string(),
            server_timestamp: chrono::Utc::now(),
            message_text: "Kappa".to_string(),
            is_action: false,
            source: twitch_irc::message::IRCMessage::parse(
                "@badges=subscriber/3;emotes=25:0-4;display-name=Viewer;user-id=2 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channel :Kappa",
            )
            .unwrap(),
        };

        let evt = privmsg_to_chat_event(&msg, false);
        assert_eq!(evt["emotes"]["25"], json!(["0-4"]));
        assert_eq!(evt["user"]["badges"], json!(["subscriber"]));
        assert_eq!(evt["user"]["badgesRaw"]["subscriber"], json!("3"));
        assert_eq!(evt["message"], json!("Kappa"));
        assert_eq!(evt["self"], json!(false));
    }

    #[test]
    fn add_emote_batch_uses_helix_owner_id_for_channel_sidebar() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([{
            "id": "em1",
            "name": "CoolEmote",
            "images": { "url_1x": "https://example.com/1.png" },
            "emote_type": "subscriptions",
            "owner_id": "999",
            "emote_set_id": "set1"
        }]);
        add_emote_batch(&mut by_id, Some(&helix), None, None, "111");
        let emote = by_id.get("em1").unwrap();
        assert_eq!(emote["ownerId"], json!("999"));
        assert_eq!(emote["ownerType"], json!("channel"));
        assert_eq!(emote["ownerIsSelf"], json!(false));
    }

    #[test]
    fn add_emote_batch_marks_own_channel_as_self() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([{
            "id": "em2",
            "name": "MyEmote",
            "images": {},
            "owner_id": "111"
        }]);
        add_emote_batch(
            &mut by_id,
            Some(&helix),
            Some("channel"),
            Some("111"),
            "111",
        );
        let emote = by_id.get("em2").unwrap();
        assert_eq!(emote["ownerId"], json!("111"));
        assert_eq!(emote["ownerType"], json!("channel"));
        assert_eq!(emote["ownerIsSelf"], json!(true));
    }

    #[test]
    fn add_emote_batch_skips_empty_and_zero_owner_ids() {
        let mut by_id = std::collections::HashMap::new();
        let helix = json!([
            {
                "id": "g1",
                "name": "GlobalThing",
                "images": {},
                "emote_type": "globals",
                "owner_id": "0"
            },
            {
                "id": "g2",
                "name": "NoOwner",
                "images": {},
                "owner_id": ""
            },
            {
                "id": "c1",
                "name": "RealChannel",
                "images": {},
                "emote_type": "subscriptions",
                "owner_id": "999"
            }
        ]);
        add_emote_batch(&mut by_id, Some(&helix), None, None, "111");
        assert!(by_id.get("g1").unwrap().get("ownerId").unwrap().is_null());
        assert!(by_id.get("g2").unwrap().get("ownerId").unwrap().is_null());
        assert_eq!(by_id.get("c1").unwrap()["ownerId"], json!("999"));
        assert_eq!(by_id.get("c1").unwrap()["ownerType"], json!("channel"));
    }

    #[test]
    fn json_id_string_coerces_numbers() {
        assert_eq!(json_id_string(&json!("123")), Some("123".into()));
        assert_eq!(json_id_string(&json!(456u64)), Some("456".into()));
        assert_eq!(json_id_string(&json!("0")), None);
        assert_eq!(json_id_string(&json!("")), None);
        assert_eq!(json_id_string(&json!(0)), None);
    }

    #[test]
    fn emotes_from_message_text_matches_tokens() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        by_name.insert("PogChamp".into(), "305954156".into());
        let map = emotes_from_message_text("hi Kappa there PogChamp", &by_name).unwrap();
        assert_eq!(map["25"], json!(["3-7"]));
        assert_eq!(map["305954156"], json!(["15-22"]));
    }

    #[test]
    fn emotes_from_message_text_groups_repeats() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        let map = emotes_from_message_text("Kappa Kappa", &by_name).unwrap();
        assert_eq!(map["25"], json!(["0-4", "6-10"]));
    }

    #[test]
    fn emotes_from_message_text_no_match_returns_none() {
        let mut by_name = std::collections::HashMap::new();
        by_name.insert("Kappa".into(), "25".into());
        assert!(emotes_from_message_text("hello world", &by_name).is_none());
    }

    static PHASE2_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn phase2_userdata() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "streamsync-phase2-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn phase2_app() -> (Arc<AppState>, Arc<TwitchServices>) {
        let userdata = phase2_userdata();
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root")
            .to_path_buf();
        let config = crate::OverlayConfig {
            port: 0,
            repo_root,
            readonly: false,
            userdata_root: Some(userdata),
        };
        let (_router, state, services) = crate::OverlayServer::new(config)
            .build_app()
            .await
            .expect("build_app");
        (state, services)
    }

    fn sample_delegated() -> DelegatedSessionFile {
        DelegatedSessionFile {
            generation: 1,
            connection_key: "ssk_phase2_test_placeholder_not_a_real_key".into(),
            client_id: "cid".into(),
            access_token: "delegated-access".into(),
            channel_login: "takeover_chan".into(),
            channel_twitch_id: "999".into(),
            twitch_expires_at: (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
            kick_access_token: Some("delegated-kick".into()),
            kick_id: Some("k1".into()),
            ..Default::default()
        }
    }

    async fn install_delegated_session(state: &AppState, services: &TwitchServices) {
        let session = sample_delegated();
        state
            .delegated_generation
            .store(session.generation, std::sync::atomic::Ordering::SeqCst);
        services
            .teardown_coordinator
            .install_generation_async(session.generation)
            .await
            .expect("install generation");
        *state.delegated.write().await = Some(session);
    }

    fn sample_personal() -> TwitchTokenFile {
        TwitchTokenFile {
            access_token: Some("personal-access".into()),
            refresh_token: Some("personal-refresh".into()),
            login: Some("personal_login".into()),
            user_id: Some("111".into()),
            ..Default::default()
        }
    }

    async fn install_fake_platform_workers(state: &AppState, services: &TwitchServices) {
        let forever = || {
            tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            })
        };
        *services.irc_handle.write().await = Some(forever());
        *services.eventsub_handle.write().await = Some(forever());
        *state.kick_feed_handle.write().await = Some(forever());
        {
            let mut k = state.kick.write().await;
            k.tokens.access_token = Some("delegated-kick".into());
            k.connected = true;
        }
    }

    /// External coordinator completes teardown with personal fallback.
    #[tokio::test]
    async fn watcher_teardown_completes_personal_fallback_and_stops_platforms() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;

        install_delegated_session(&state, &services).await;
        state.save_delegated().await.unwrap();
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile {
                access_token: Some("delegated-access".into()),
                login: Some("takeover_chan".into()),
                user_id: Some("999".into()),
                ..Default::default()
            };
            *state.active_mode.write().await = TwitchActiveMode::Delegated;
        }
        state.save_active_mode().await.unwrap();
        install_fake_platform_workers(&state, &services).await;

        services
            .signal_delegated_teardown(state.clone(), 1, "revoked")
            .await
            .expect("teardown");

        assert!(state.delegated.read().await.is_none());
        assert!(!state.delegated_file_exists().await);
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
        assert_eq!(
            state.twitch.read().await.tokens.login.as_deref(),
            Some("personal_login")
        );
        assert!(services.refresh_handle.read().await.is_none());
        assert!(services.watch_handle.read().await.is_none());
        assert!(state.kick_feed_handle.read().await.is_none());
        assert!(!state.kick.read().await.tokens.is_linked());
    }

    #[tokio::test]
    async fn watcher_teardown_without_personal_stops_all_platform_workers() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;

        install_delegated_session(&state, &services).await;
        *state.personal_tokens.write().await = TwitchTokenFile::default();
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile {
                access_token: Some("delegated-access".into()),
                login: Some("takeover_chan".into()),
                user_id: Some("999".into()),
                ..Default::default()
            };
            *state.active_mode.write().await = TwitchActiveMode::Delegated;
        }
        install_fake_platform_workers(&state, &services).await;

        services
            .signal_delegated_teardown(state.clone(), 1, "revoked")
            .await
            .expect("teardown");

        assert!(state.delegated.read().await.is_none());
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
        assert!(services.irc_handle.read().await.is_none());
        assert!(services.eventsub_handle.read().await.is_none());
        assert!(state.kick_feed_handle.read().await.is_none());
    }

    #[tokio::test]
    async fn teardown_is_idempotent_across_simultaneous_callers() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        install_fake_platform_workers(&state, &services).await;

        let a =
            end_delegated_session_after_key_invalid(state.clone(), services.clone(), "revoked", 1);
        let b =
            end_delegated_session_after_key_invalid(state.clone(), services.clone(), "expired", 1);
        let (ra, rb) = tokio::join!(a, b);
        assert!(ra.is_ok());
        assert!(rb.is_ok());
        assert!(state.delegated.read().await.is_none());
        assert_eq!(
            services.teardown_coordinator.phase_for(1).await,
            crate::delegated_lifecycle::TeardownPhase::Completed
        );
    }

    #[tokio::test]
    async fn independent_watch_handles_per_services_instance() {
        let a = Arc::new(TwitchServices::new());
        let b = Arc::new(TwitchServices::new());
        *a.watch_handle.write().await = Some(GenerationTask {
            generation: 1,
            handle: tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            }),
        });
        *b.watch_handle.write().await = Some(GenerationTask {
            generation: 1,
            handle: tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            }),
        });
        stop_delegated_worker_handles(&a.refresh_handle, &a.watch_handle, None).await;
        assert!(a.watch_handle.read().await.is_none());
        assert!(b.watch_handle.read().await.is_some());
        stop_delegated_worker_handles(&b.refresh_handle, &b.watch_handle, None).await;
        assert!(b.watch_handle.read().await.is_none());
    }

    #[test]
    fn parse_sse_revoked_event() {
        use crate::delegated_lifecycle::parse_sse_json_data;
        let frame = "event: message\ndata: {\"type\":\"revoked\"}\n";
        let v = parse_sse_json_data(frame).unwrap();
        assert_eq!(v.get("type").and_then(|x| x.as_str()), Some("revoked"));
    }

    #[tokio::test]
    async fn sse_401_uses_authorization_header_and_tears_down() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let saw_auth = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_auth2 = saw_auth.clone();
        let saw_query_key = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let saw_query_key2 = saw_query_key.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            loop {
                let n = socket.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                if buf.len() > 16_384 {
                    break;
                }
            }
            let req = String::from_utf8_lossy(&buf).to_ascii_lowercase();
            if req.contains("authorization:") && req.contains("bearer ssk_") {
                saw_auth2.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            if req.contains("?key=") {
                saw_query_key2.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            let body =
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(body).await;
        });
        std::env::set_var("SYNDICATE_API_BASE", format!("http://127.0.0.1:{port}"));

        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        services.install_pending_authority_lease(1).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        install_fake_platform_workers(&state, &services).await;

        let ended = consume_connection_key_events(
            state.clone(),
            services.clone(),
            1,
            "ssk_phase2_test_placeholder_not_a_real_key",
        )
        .await
        .unwrap();
        assert!(ended);
        assert!(
            saw_auth.load(std::sync::atomic::Ordering::SeqCst),
            "events request must send Authorization Bearer"
        );
        assert!(!saw_query_key.load(std::sync::atomic::Ordering::SeqCst));
        assert!(state.delegated.read().await.is_none());
    }

    #[tokio::test]
    async fn refresh_hard_fail_codes_end_delegated_session() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_fake_platform_workers(&state, &services).await;

        for code in ["revoked", "expired", "invalid_key"] {
            install_delegated_session(&state, &services).await;
            *state.active_mode.write().await = TwitchActiveMode::Delegated;
            end_delegated_session_after_key_invalid(state.clone(), services.clone(), code, 1)
                .await
                .expect("teardown");
            assert!(
                state.delegated.read().await.is_none(),
                "code {code} must clear delegated"
            );
            assert!(!state.delegated_file_exists().await);
        }
    }

    #[tokio::test]
    async fn use_connection_advances_apply_intent_and_wins_over_stale_apply() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        let stale_intent = services.bump_apply_intent();
        use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
            .await
            .expect("use_connection local");
        assert_ne!(services.apply_intent_for_test(), stale_intent);
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
    }

    #[tokio::test]
    async fn finished_refresh_worker_is_restarted_by_ensure_loop() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        services.install_validated_authority_lease(1, None).await;
        // Keep ensure from starting the SSE watch loop (real network) in this unit test.
        let watch_nop = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(install_generation_task(&services.watch_handle, 1, watch_nop).await);
        let finished = tokio::spawn(async {});
        assert!(install_generation_task(&services.refresh_handle, 1, finished).await);
        for _ in 0..20 {
            if services
                .refresh_handle
                .read()
                .await
                .as_ref()
                .is_some_and(|t| t.handle.is_finished())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ensure_delegated_refresh_loop(state.clone(), services.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let slot = services.refresh_handle.read().await;
        assert!(
            slot.as_ref().is_some_and(|t| generation_task_alive(t, 1)),
            "finished worker must be replaced"
        );
        drop(slot);
        stop_all_platform_workers(&state, &services).await;
    }

    #[tokio::test]
    async fn failed_replacement_persist_keeps_old_generation_workers() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        state.save_delegated().await.unwrap();
        services.install_validated_authority_lease(1, None).await;
        let watch = tokio::spawn(async { std::future::pending::<()>().await });
        assert!(install_generation_task(&services.watch_handle, 1, watch).await);

        let exchange = syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: "cid".into(),
                access_token: "new-access".into(),
                expires_at: (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                scopes: vec![],
            },
            channel: syndicate_connection::ExchangeChannel {
                login: "takeover_chan".into(),
                twitch_id: "999".into(),
                display_name: Some("Takeover".into()),
            },
            connection: syndicate_connection::ExchangeConnection {
                label: Some("test".into()),
                expires_at: None,
            },
            kick: None,
        };
        state
            .durable_fail
            .save_session
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = apply_exchange_session(
            state.clone(),
            services.clone(),
            "ssk_phase2_test_placeholder_not_a_real_key",
            exchange,
            true,
            None,
        )
        .await;
        assert!(err.is_err());
        assert_eq!(state.current_delegated_generation(), 1);
        assert_eq!(
            services
                .watch_handle
                .read()
                .await
                .as_ref()
                .map(|t| t.generation),
            Some(1)
        );
    }

    #[tokio::test]
    async fn post_deadline_helix_is_rejected_before_network() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        {
            let mut tw = state.twitch.write().await;
            tw.tokens.access_token = Some("delegated-access".into());
            tw.tokens.login = Some("takeover_chan".into());
            tw.tokens.obtainment_timestamp = Some(chrono::Utc::now().timestamp_millis());
            tw.tokens.expires_in = Some(3600);
        }
        services
            .set_authority_deadline_for_test(1, std::time::Instant::now() - Duration::from_secs(1))
            .await;
        let err = helix_get(&state, "/users").await.unwrap_err();
        assert!(
            err.to_string().contains("Delegated authority"),
            "unexpected: {err:#}"
        );
    }

    #[tokio::test]
    async fn race_rejects_when_generation_changes_mid_request() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        services.install_validated_authority_lease(1, None).await;
        let services2 = services.clone();
        let result = race_against_lease_deadline(&services, 1, async {
            services2.install_validated_authority_lease(2, None).await;
            "late"
        })
        .await;
        assert!(
            result.is_err(),
            "stale generation completion must be rejected"
        );
    }

    #[tokio::test]
    async fn race_rejects_when_deadline_passes_during_request() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (_state, services) = phase2_app().await;
        services
            .set_authority_deadline_for_test(
                1,
                std::time::Instant::now() + Duration::from_millis(30),
            )
            .await;
        let result = race_against_lease_deadline(&services, 1, async {
            tokio::time::sleep(Duration::from_millis(80)).await;
            "late"
        })
        .await;
        assert!(result.is_err(), "post-deadline completion must be rejected");
    }

    #[tokio::test]
    async fn race_rejects_stale_completion_after_switch_to_local() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        services.install_validated_authority_lease(1, None).await;
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();

        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let services2 = services.clone();
        let state2 = state.clone();
        let pending = tokio::spawn(async move {
            services2
                .race_delegated_platform(&state2, async {
                    let _ = gate_rx.await;
                    "stale-delegated-body"
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
            .await
            .expect("switch local");
        let _ = gate_tx.send(());
        let result = pending.await.unwrap();
        assert!(
            result.is_err(),
            "stale delegated completion after Local switch must be rejected: {result:?}"
        );
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
    }

    /// B2: headers can arrive before the lease deadline while the body completes after.
    /// The full send+body future must remain under the fence so the late body is rejected.
    #[tokio::test]
    async fn race_rejects_http_body_completed_after_deadline() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            loop {
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                if buf[..n].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            // Headers before deadline…
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\n")
                .await;
            // …body after deadline.
            tokio::time::sleep(Duration::from_millis(120)).await;
            let _ = socket.write_all(b"later").await;
        });

        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        services
            .set_authority_deadline_for_test(
                1,
                std::time::Instant::now() + Duration::from_millis(40),
            )
            .await;

        let url = format!("http://127.0.0.1:{port}/helix/users");
        let result = services
            .race_delegated_platform(&state, async {
                let res = reqwest::Client::new().get(&url).send().await?;
                let status = res.status();
                let text = res.text().await?;
                anyhow::Ok((status.as_u16(), text))
            })
            .await;
        assert!(
            result.is_err(),
            "late HTTP body after lease deadline must be rejected: {result:?}"
        );
    }

    #[tokio::test]
    async fn failed_mode_save_on_use_connection_keeps_intent_advanced() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        state.save_active_mode().await.unwrap();
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        let before = services.apply_intent_for_test();
        state
            .durable_fail
            .save_active_mode
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = use_connection(state.clone(), services.clone(), TwitchActiveMode::Local).await;
        assert!(err.is_err());
        assert!(services.apply_intent_for_test() > before);
        // Durable-before-publish: live identity must remain coherent with pre-failure mode.
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Delegated);
    }

    #[tokio::test]
    async fn inactive_delegated_removal_does_not_deadlock() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        state.save_delegated().await.unwrap();
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        *state.active_mode.write().await = TwitchActiveMode::Local;
        state.save_active_mode().await.unwrap();
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = sample_personal();
        }
        let remove = tokio::time::timeout(
            Duration::from_secs(3),
            remove_connection(state.clone(), services.clone(), TwitchActiveMode::Delegated),
        )
        .await;
        assert!(remove.is_ok(), "remove_connection timed out (deadlock)");
        remove.unwrap().expect("remove_connection");
        assert!(state.delegated.read().await.is_none());
        assert!(!state.paths.twitch_delegated.is_file());
        assert!(!state.paths.twitch_delegated.with_extension("bak").is_file());
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
        assert_eq!(
            state.twitch.read().await.tokens.login.as_deref(),
            Some("personal_login")
        );
    }

    #[tokio::test]
    async fn revalidation_only_installs_pending_lease_before_workers() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        // No lease yet — simulate startup revalidation path.
        {
            let mut lease = services.authority_lease.lock().await;
            *lease = AuthorityLease::inactive();
        }
        start_delegated_revalidation_only(state.clone(), services.clone(), 1).await;
        let lease = services.authority_lease.lock().await;
        assert_eq!(lease.generation(), 1);
        assert!(!lease.is_expired() || lease.remaining() <= SYNDICATE_HTTP_TIMEOUT);
        drop(lease);
        // Platform clients must not have been started by revalidation-only.
        assert!(services.irc_handle.read().await.is_none());
        assert!(services.eventsub_handle.read().await.is_none());
    }

    #[test]
    fn max_revocation_delay_is_documented_bound() {
        use crate::delegated_lifecycle::AuthorityLease;
        assert_eq!(MAX_DELEGATED_REVOCATION_DELAY, Duration::from_secs(300));
        let mut lease = AuthorityLease::inactive();
        lease.install_validated_generation(1, None);
        assert!(lease.sleep_budget(Duration::from_secs(10_000)) <= MAX_DELEGATED_REVOCATION_DELAY);
    }

    #[test]
    fn ensure_apply_intent_rejects_stale_sequence() {
        let services = TwitchServices::new();
        let intent = services.bump_apply_intent();
        services.bump_apply_intent();
        assert!(services.ensure_apply_intent_current(intent).is_err());
    }

    #[test]
    fn merge_delegated_session_updates_all_exchange_fields() {
        use crate::kick::merge_delegated_session_from_exchange;
        let session = DelegatedSessionFile {
            generation: 1,
            connection_key: "ssk_test".into(),
            client_id: "old".into(),
            access_token: "old-token".into(),
            channel_login: "old_chan".into(),
            channel_twitch_id: "1".into(),
            twitch_expires_at: "2020-01-01T00:00:00Z".into(),
            ..Default::default()
        };
        let exchange = syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: "new-cid".into(),
                access_token: "new-token".into(),
                expires_at: "2026-01-01T00:00:00Z".into(),
                scopes: vec!["chat:read".into()],
            },
            channel: syndicate_connection::ExchangeChannel {
                login: "new_chan".into(),
                twitch_id: "99".into(),
                display_name: Some("New".into()),
            },
            connection: syndicate_connection::ExchangeConnection {
                label: Some("label".into()),
                expires_at: Some("2026-06-01T00:00:00Z".into()),
            },
            kick: Some(syndicate_connection::ExchangeKick {
                kick_id: Some("12345".into()),
                login: Some("kickuser".into()),
                access_token: Some("kick-at".into()),
                refresh_token: None,
                expires_at: None,
                scopes: vec![],
                error: None,
            }),
        };
        let merged = merge_delegated_session_from_exchange(session, &exchange);
        assert!(crate::kick::delegated_session_matches_exchange(
            &merged, &exchange
        ));
        assert_eq!(merged.access_token, "new-token");
        assert_eq!(merged.client_id, "new-cid");
        assert_eq!(merged.kick_id.as_deref(), Some("12345"));
        assert_eq!(
            merged.connection_expires_at.as_deref(),
            Some("2026-06-01T00:00:00Z")
        );
    }

    #[test]
    fn merge_exchange_rotates_twitch_only() {
        use crate::kick::{
            delegated_session_matches_exchange, merge_delegated_session_from_exchange,
        };
        let session = sample_delegated();
        let exchange = syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: "rotated-cid".into(),
                access_token: "rotated-at".into(),
                expires_at: "2099-02-01T00:00:00Z".into(),
                scopes: vec!["chat:edit".into()],
            },
            channel: syndicate_connection::ExchangeChannel {
                twitch_id: session.channel_twitch_id.clone(),
                login: session.channel_login.clone(),
                display_name: session.display_name.clone(),
            },
            kick: Some(syndicate_connection::ExchangeKick {
                kick_id: session.kick_id.clone(),
                login: session.kick_login.clone(),
                access_token: session.kick_access_token.clone(),
                refresh_token: session.kick_refresh_token.clone(),
                expires_at: session.kick_expires_at.clone(),
                scopes: session.kick_scopes.clone(),
                error: None,
            }),
            connection: syndicate_connection::ExchangeConnection {
                label: session.label.clone(),
                expires_at: session.connection_expires_at.clone(),
            },
        };
        let merged = merge_delegated_session_from_exchange(session, &exchange);
        assert!(delegated_session_matches_exchange(&merged, &exchange));
        assert_eq!(merged.access_token, "rotated-at");
        assert_eq!(merged.kick_access_token.as_deref(), Some("delegated-kick"));
    }

    #[test]
    fn merge_exchange_rotates_kick_only() {
        use crate::kick::{
            delegated_session_matches_exchange, merge_delegated_session_from_exchange,
        };
        let session = sample_delegated();
        let exchange = syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: session.client_id.clone(),
                access_token: session.access_token.clone(),
                expires_at: session.twitch_expires_at.clone(),
                scopes: session.scopes.clone(),
            },
            channel: syndicate_connection::ExchangeChannel {
                twitch_id: session.channel_twitch_id.clone(),
                login: session.channel_login.clone(),
                display_name: session.display_name.clone(),
            },
            kick: Some(syndicate_connection::ExchangeKick {
                kick_id: Some("kick-rotated".into()),
                login: Some("kickuser".into()),
                access_token: Some("kick-rotated-at".into()),
                refresh_token: Some("kick-rotated-rt".into()),
                expires_at: Some("2099-03-01T00:00:00Z".into()),
                scopes: vec!["chat:write".into()],
                error: None,
            }),
            connection: syndicate_connection::ExchangeConnection {
                label: session.label.clone(),
                expires_at: session.connection_expires_at.clone(),
            },
        };
        let merged = merge_delegated_session_from_exchange(session, &exchange);
        assert!(delegated_session_matches_exchange(&merged, &exchange));
        assert_eq!(merged.access_token, "delegated-access");
        assert_eq!(merged.kick_access_token.as_deref(), Some("kick-rotated-at"));
    }

    #[test]
    fn merge_exchange_rotates_scopes_and_expiry_only() {
        use crate::kick::{
            delegated_session_matches_exchange, merge_delegated_session_from_exchange,
        };
        let session = sample_delegated();
        let exchange = syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: session.client_id.clone(),
                access_token: session.access_token.clone(),
                expires_at: "2099-12-31T00:00:00Z".into(),
                scopes: vec!["channel:manage:broadcast".into(), "chat:read".into()],
            },
            channel: syndicate_connection::ExchangeChannel {
                twitch_id: session.channel_twitch_id.clone(),
                login: session.channel_login.clone(),
                display_name: Some("Display".into()),
            },
            kick: Some(syndicate_connection::ExchangeKick {
                kick_id: session.kick_id.clone(),
                login: session.kick_login.clone(),
                access_token: session.kick_access_token.clone(),
                refresh_token: session.kick_refresh_token.clone(),
                expires_at: session.kick_expires_at.clone(),
                scopes: session.kick_scopes.clone(),
                error: None,
            }),
            connection: syndicate_connection::ExchangeConnection {
                label: Some("new-label".into()),
                expires_at: Some("2099-11-01T00:00:00Z".into()),
            },
        };
        let merged = merge_delegated_session_from_exchange(session, &exchange);
        assert!(delegated_session_matches_exchange(&merged, &exchange));
        assert_eq!(merged.scopes, vec!["channel:manage:broadcast", "chat:read"]);
        assert_eq!(
            merged.connection_expires_at.as_deref(),
            Some("2099-11-01T00:00:00Z")
        );
    }

    #[tokio::test]
    async fn use_connection_delegated_aborts_when_superseded_by_local_switch() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let _ = gate_rx.await;
                let body = serde_json::json!({
                    "ok": true,
                    "twitch": {
                        "client_id": "cid",
                        "access_token": "new-delegated",
                        "expires_at": "2099-01-01T00:00:00Z",
                        "scopes": []
                    },
                    "channel": { "twitch_id": "999", "login": "takeover_chan" },
                    "connection": { "expires_at": "2099-01-01T00:00:00Z" }
                });
                let payload = body.to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    payload.len(),
                    payload
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            }
        });
        std::env::set_var("SYNDICATE_API_BASE", format!("http://127.0.0.1:{port}"));

        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        state
            .persist_delegated_session(state.delegated.read().await.as_ref().unwrap())
            .unwrap();
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        *state.active_mode.write().await = TwitchActiveMode::Local;
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = sample_personal();
        }

        let state2 = state.clone();
        let services2 = services.clone();
        let pending = tokio::spawn(async move {
            use_connection(state2, services2, TwitchActiveMode::Delegated).await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
            .await
            .expect("switch local");
        let _ = gate_tx.send(());
        let result = pending.await.unwrap();
        assert!(
            result.is_err(),
            "stale delegated activation must not commit after newer Local switch: {result:?}"
        );
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
        assert_eq!(
            state.twitch.read().await.tokens.access_token.as_deref(),
            Some("personal-access")
        );
    }

    #[tokio::test]
    async fn identity_rollback_blocks_use_connection_and_kick_send() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        crate::storage::write_identity_rollback_pending(
            &state.paths.twitch_tokens_rollback_pending,
        )
        .unwrap();
        assert!(state.identity_recovery_required());
        assert!(
            use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
                .await
                .is_err()
        );
        assert!(crate::kick::send_chat_from_dock(state.clone(), "hello")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn local_switch_preserves_saved_delegated_credential() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        state.save_active_mode().await.unwrap();
        services.install_validated_authority_lease(1, None).await;
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        state.save_delegated().await.unwrap();

        use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
            .await
            .expect("switch local");
        // Simulate lease expiry while Local — must not delete saved takeover.
        fail_closed_lease_expired(state.clone(), services.clone(), 1).await;
        assert!(state.paths.twitch_delegated.is_file());
        assert!(state.delegated.read().await.is_some());
        assert_eq!(*state.active_mode.read().await, TwitchActiveMode::Local);
        // No platform actions while Local: helix provenance is Local.
        let prov = services.capture_platform_provenance(&state).await.unwrap();
        assert!(matches!(prov, PlatformCredentialProvenance::Local));
    }

    #[tokio::test]
    async fn provenance_rejects_stale_delegated_after_local_switch() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        services.install_validated_authority_lease(1, None).await;
        let provenance = services.capture_platform_provenance(&state).await.unwrap();
        assert!(matches!(
            provenance,
            PlatformCredentialProvenance::Delegated { .. }
        ));
        *state.personal_tokens.write().await = sample_personal();
        state.save_twitch_tokens().await.unwrap();
        use_connection(state.clone(), services.clone(), TwitchActiveMode::Local)
            .await
            .unwrap();
        let err = services
            .race_delegated_platform_with_provenance(&state, provenance, async { "stale" })
            .await;
        assert!(
            err.is_err(),
            "captured delegated provenance must stay fenced: {err:?}"
        );
    }

    #[tokio::test]
    async fn irc_send_rejects_stale_delegated_client_after_local_mode() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        services.install_validated_authority_lease(1, None).await;
        let delegated_snap = services.authority_lease_snapshot_public().await;
        services
            .install_irc_bundle_for_test(PlatformCredentialProvenance::Delegated {
                snap: delegated_snap,
            })
            .await;
        {
            let mut tw = state.twitch.write().await;
            tw.channel = Some("takeover_chan".into());
        }
        *state.active_mode.write().await = TwitchActiveMode::Local;
        let err = services.select_irc_send_bundle(&state).await;
        assert!(
            err.is_err(),
            "stale delegated IRC client must not send under Local provenance: {err:?}"
        );
    }

    #[tokio::test]
    async fn refresh_persist_failure_leaves_memory_unchanged() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        let (port, server) = spawn_mock_syndicate_refresh_server("rotated-access").await;
        let _env = ProductionTestEnvGuard::install(port);

        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        let mut session = state.delegated.read().await.clone().unwrap();
        session.twitch_expires_at = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        state.persist_delegated_session(&session).unwrap();
        *state.delegated.write().await = Some(session);
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        services.install_validated_authority_lease(1, None).await;
        let before = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .access_token
            .clone();
        state
            .durable_fail
            .save_session
            .store(true, std::sync::atomic::Ordering::SeqCst);
        start_delegated_refresh_loop(state.clone(), services.clone(), 1).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        let after = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .access_token
            .clone();
        assert_eq!(
            before, after,
            "refresh persist failure must not mutate memory"
        );
        state
            .durable_fail
            .save_session
            .store(false, std::sync::atomic::Ordering::SeqCst);
        server.abort();
        stop_all_platform_workers(&state, &services).await;
    }

    fn stale_refresh_exchange(access_token: &str) -> syndicate_connection::ExchangeSuccess {
        syndicate_connection::ExchangeSuccess {
            ok: true,
            twitch: syndicate_connection::ExchangeTwitch {
                client_id: "rotated-cid".into(),
                access_token: access_token.into(),
                expires_at: "2099-02-01T00:00:00Z".into(),
                scopes: vec![],
            },
            channel: syndicate_connection::ExchangeChannel {
                twitch_id: "999".into(),
                login: "takeover_chan".into(),
                display_name: None,
            },
            kick: Some(syndicate_connection::ExchangeKick {
                kick_id: Some("k2".into()),
                login: Some("kick_winner".into()),
                access_token: Some("gen2-kick".into()),
                refresh_token: None,
                expires_at: None,
                scopes: vec![],
                error: None,
            }),
            connection: syndicate_connection::ExchangeConnection {
                label: None,
                expires_at: Some("2099-06-01T00:00:00Z".into()),
            },
        }
    }

    const GATE_WAIT: Duration = Duration::from_secs(5);

    fn begin_refresh_side_effect_race() {
        crate::delegated_refresh_observability::reset_side_effect_counters();
    }

    async fn gate_wait(rx: oneshot::Receiver<()>, label: &str) {
        tokio::time::timeout(GATE_WAIT, rx)
            .await
            .unwrap_or_else(|_| panic!("timeout waiting for {label}"))
            .expect(label);
    }

    async fn task_join_with_timeout<T>(task: tokio::task::JoinHandle<T>, label: &str) -> T {
        tokio::time::timeout(GATE_WAIT, task)
            .await
            .unwrap_or_else(|_| panic!("timeout joining {label}"))
            .expect(label)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PlatformWorkerSnapshot {
        irc: Option<tokio::task::Id>,
        eventsub: Option<tokio::task::Id>,
        refresh_generation: Option<u64>,
        watch_generation: Option<u64>,
        kick_feed: Option<tokio::task::Id>,
    }

    async fn platform_worker_snapshot(
        state: &AppState,
        services: &TwitchServices,
    ) -> PlatformWorkerSnapshot {
        PlatformWorkerSnapshot {
            irc: services.irc_handle.read().await.as_ref().map(|h| h.id()),
            eventsub: services
                .eventsub_handle
                .read()
                .await
                .as_ref()
                .map(|h| h.id()),
            refresh_generation: services
                .refresh_handle
                .read()
                .await
                .as_ref()
                .map(|t| t.generation),
            watch_generation: services
                .watch_handle
                .read()
                .await
                .as_ref()
                .map(|t| t.generation),
            kick_feed: state.kick_feed_handle.read().await.as_ref().map(|h| h.id()),
        }
    }

    #[derive(Debug, Clone)]
    struct LiveIdentitySnapshot {
        mode: TwitchActiveMode,
        twitch_tokens: TwitchTokenFile,
        kick_tokens: crate::config_types::KickTokenFile,
        kick_connected: bool,
        lease: AuthorityLeaseSnapshot,
        delegated: Option<DelegatedSessionFile>,
    }

    async fn live_identity_snapshot(
        state: &AppState,
        services: &TwitchServices,
    ) -> LiveIdentitySnapshot {
        LiveIdentitySnapshot {
            mode: *state.active_mode.read().await,
            twitch_tokens: state.twitch.read().await.tokens.clone(),
            kick_tokens: state.kick.read().await.tokens.clone(),
            kick_connected: state.kick.read().await.connected,
            lease: services.authority_lease_snapshot_public().await,
            delegated: state.delegated.read().await.clone(),
        }
    }

    fn read_delegated_disk(state: &AppState) -> Option<DelegatedSessionFile> {
        if !state.paths.twitch_delegated.is_file() {
            return None;
        }
        let session = crate::storage::read_json_if_exists(
            &state.paths.twitch_delegated,
            &DelegatedSessionFile::default(),
        )
        .expect("read delegated disk");
        if session.generation == 0 && session.access_token.is_empty() {
            None
        } else {
            Some(session)
        }
    }

    fn restart_state_at(state: &AppState) -> Arc<AppState> {
        let paths =
            crate::storage::paths_for_root(&state.paths.root, false).expect("paths_for_root");
        AppState::new(paths, state.repo_root.clone(), 0, false).expect("AppState::new")
    }

    async fn activate_delegated_gen1(state: &AppState, services: &TwitchServices) {
        install_delegated_session(state, services).await;
        let session = state.delegated.read().await.clone().unwrap();
        state.persist_delegated_session(&session).unwrap();
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        state.save_active_mode().await.unwrap();
        services.install_validated_authority_lease(1, None).await;
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile {
                access_token: Some("delegated-access".into()),
                login: Some("takeover_chan".into()),
                user_id: Some("999".into()),
                ..Default::default()
            };
        }
        install_fake_platform_workers(state, services).await;
    }

    async fn commit_replacement_generation(
        state: &AppState,
        services: &TwitchServices,
        generation: u64,
        access_token: &str,
    ) {
        let _lifecycle = services.lifecycle_lock.lock().await;
        let mut session = sample_delegated();
        session.generation = generation;
        session.access_token = access_token.into();
        session.connection_key = format!("ssk_phase2_replacement_{generation}");
        session.kick_access_token = Some(format!("{access_token}-kick"));
        state.persist_delegated_session(&session).unwrap();
        state.publish_delegated_generation(generation);
        services
            .teardown_coordinator
            .install_generation_async(generation)
            .await
            .expect("install generation");
        services
            .install_validated_authority_lease(generation, None)
            .await;
        *state.delegated.write().await = Some(session);
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        state.save_active_mode().await.unwrap();
        {
            let mut tw = state.twitch.write().await;
            tw.tokens = TwitchTokenFile {
                access_token: Some(access_token.into()),
                login: Some("takeover_chan".into()),
                user_id: Some("999".into()),
                ..Default::default()
            };
        }
        {
            let mut k = state.kick.write().await;
            k.tokens.access_token = Some(format!("{access_token}-kick"));
            k.tokens.kick_id = Some("k2".into());
            k.connected = true;
        }
    }

    async fn install_winner_platform_workers(
        state: &AppState,
        services: &TwitchServices,
        generation: u64,
    ) -> PlatformWorkerSnapshot {
        let forever = || {
            tokio::spawn(async {
                loop {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
            })
        };
        *services.irc_handle.write().await = Some(forever());
        *services.eventsub_handle.write().await = Some(forever());
        *state.kick_feed_handle.write().await = Some(forever());
        *services.refresh_handle.write().await = Some(GenerationTask {
            generation,
            handle: forever(),
        });
        *services.watch_handle.write().await = Some(GenerationTask {
            generation,
            handle: forever(),
        });
        platform_worker_snapshot(state, services).await
    }

    async fn assert_live_identity_eq(
        state: &AppState,
        services: &TwitchServices,
        expected: &LiveIdentitySnapshot,
    ) {
        let actual = live_identity_snapshot(state, services).await;
        assert_eq!(actual.mode, expected.mode);
        assert_eq!(
            actual.twitch_tokens.access_token,
            expected.twitch_tokens.access_token
        );
        assert_eq!(
            actual.kick_tokens.access_token,
            expected.kick_tokens.access_token
        );
        assert_eq!(actual.kick_connected, expected.kick_connected);
        assert_eq!(actual.lease, expected.lease);
        assert_eq!(
            actual.delegated.as_ref().map(|s| s.generation),
            expected.delegated.as_ref().map(|s| s.generation)
        );
        assert_eq!(
            actual.delegated.as_ref().map(|s| s.access_token.as_str()),
            expected.delegated.as_ref().map(|s| s.access_token.as_str())
        );
    }

    async fn refresh_stale_after_replacement_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("stale-refresh-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_commit_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before commit").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;
        resume_tx.send(()).expect("release stale refresh");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(!committed, "stale refresh must not commit");

        let disk = read_delegated_disk(&state).expect("gen2 primary on disk");
        assert_eq!(disk.generation, 2);
        assert_eq!(disk.access_token, "gen2-token");
        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );

        let restarted = restart_state_at(&state);
        let restarted_identity = live_identity_snapshot(&restarted, &services).await;
        assert_eq!(restarted_identity.mode, TwitchActiveMode::Delegated);
        assert_eq!(
            restarted_identity.twitch_tokens.access_token.as_deref(),
            Some("gen2-token")
        );
        assert_eq!(
            restarted_identity.delegated.as_ref().map(|s| s.generation),
            Some(2)
        );
        assert_eq!(
            restarted_identity
                .delegated
                .as_ref()
                .map(|s| s.access_token.as_str()),
            Some("gen2-token")
        );
        assert_eq!(restarted_identity.lease.generation, 2);
    }

    #[tokio::test]
    async fn refresh_stale_after_replacement_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_after_replacement_once().await;
        }
    }

    async fn refresh_stale_after_revocation_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;
        let workers_before = platform_worker_snapshot(&state, &services).await;

        let exchange = stale_refresh_exchange("stale-refresh-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_commit_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before commit").await;
        remove_delegated_session(&state, &services, None, 1)
            .await
            .expect("revoke");
        resume_tx.send(()).expect("release stale refresh");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(!committed, "stale refresh must not commit after revocation");

        assert!(!state.paths.twitch_delegated.is_file());
        assert!(state.paths.twitch_delegated_revoked.is_file());
        assert!(
            !crate::storage::delegated_committing_path(&state.paths.twitch_delegated).is_file()
        );
        assert!(
            !crate::storage::delegated_replace_pending_path(&state.paths.twitch_delegated)
                .is_file()
        );
        assert!(!state.paths.twitch_delegated.with_extension("bak").is_file());
        assert!(state.delegated.read().await.is_none());
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            workers_before
        );
        assert_eq!(
            live_identity_snapshot(&state, &services).await.delegated,
            None
        );

        let restarted = restart_state_at(&state);
        assert!(!restarted.paths.twitch_delegated.is_file());
        assert!(restarted.paths.twitch_delegated_revoked.is_file());
        assert!(restarted.delegated.read().await.is_none());
        assert_ne!(
            *restarted.active_mode.read().await,
            TwitchActiveMode::Delegated
        );
    }

    #[tokio::test]
    async fn refresh_stale_after_revocation_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_after_revocation_once().await;
        }
    }

    async fn refresh_stale_cannot_renew_lease_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;
        let lease_before = services.authority_lease_snapshot_public().await;

        let exchange = stale_refresh_exchange("stale-refresh-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_commit_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before commit").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_lease = services.authority_lease_snapshot_public().await;
        resume_tx.send(()).expect("release stale refresh");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(
            !committed,
            "stale refresh must not renew generation-1 lease"
        );

        let lease_after = services.authority_lease_snapshot_public().await;
        assert_eq!(lease_after, winner_lease);
        assert_eq!(lease_after.generation, 2);
        assert_ne!(lease_after.generation, lease_before.generation);
    }

    #[tokio::test]
    async fn refresh_stale_cannot_renew_lease() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_cannot_renew_lease_once().await;
        }
    }

    async fn refresh_stale_after_commit_before_restart_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_live_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before live publish").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;
        resume_tx.send(()).expect("release stale refresh live path");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(committed, "durable refresh commit should succeed");

        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
        assert_eq!(
            state.twitch.read().await.tokens.access_token.as_deref(),
            Some("gen2-token")
        );
        assert_eq!(
            state.kick.read().await.tokens.access_token.as_deref(),
            Some("gen2-token-kick")
        );
    }

    #[tokio::test]
    async fn refresh_stale_after_commit_before_restart_no_live_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_after_commit_before_restart_once().await;
        }
    }

    async fn refresh_stale_after_commit_before_sync_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_live_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before live publish").await;
        remove_delegated_session(&state, &services, None, 1)
            .await
            .expect("revoke");
        let workers_after_revoke = platform_worker_snapshot(&state, &services).await;
        let identity_after_revoke = live_identity_snapshot(&state, &services).await;
        resume_tx.send(()).expect("release stale refresh live path");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(
            committed,
            "durable refresh commit may complete before live gate"
        );

        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            workers_after_revoke
        );
        assert_eq!(
            live_identity_snapshot(&state, &services).await.delegated,
            identity_after_revoke.delegated
        );
        assert_eq!(
            live_identity_snapshot(&state, &services).await.lease,
            identity_after_revoke.lease
        );
        assert!(state.delegated.read().await.is_none());
        assert!(!state.paths.twitch_delegated.is_file());
    }

    #[tokio::test]
    async fn refresh_stale_after_commit_before_sync_no_live_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_after_commit_before_sync_once().await;
        }
    }

    struct ProductionTestEnvGuard {
        syndicate_api_base: Option<String>,
    }

    impl ProductionTestEnvGuard {
        fn install(port: u16) -> Self {
            let prev = std::env::var("SYNDICATE_API_BASE").ok();
            std::env::set_var("SYNDICATE_API_BASE", format!("http://127.0.0.1:{port}"));
            super::set_refresh_bypass_sleep(true);
            Self {
                syndicate_api_base: prev,
            }
        }
    }

    impl Drop for ProductionTestEnvGuard {
        fn drop(&mut self) {
            match self.syndicate_api_base.take() {
                Some(v) => std::env::set_var("SYNDICATE_API_BASE", v),
                None => std::env::remove_var("SYNDICATE_API_BASE"),
            }
            super::set_refresh_bypass_sleep(false);
        }
    }

    async fn stop_all_platform_workers(state: &AppState, services: &TwitchServices) {
        *state.delegated.write().await = None;
        state
            .delegated_generation
            .store(0, std::sync::atomic::Ordering::SeqCst);
        crate::delegated_lifecycle::stop_delegated_worker_handles(
            &services.refresh_handle,
            &services.watch_handle,
            None,
        )
        .await;
        if let Some(h) = services.irc_handle.write().await.take() {
            h.abort();
        }
        if let Some(h) = services.eventsub_handle.write().await.take() {
            h.abort();
        }
        crate::kick::teardown_delegated_kick_live(state).await;
    }

    async fn spawn_mock_syndicate_refresh_server(
        access_token: &str,
    ) -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let access_token = access_token.to_string();
        let server = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                loop {
                    let n = socket.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let body = serde_json::json!({
                    "ok": true,
                    "twitch": {
                        "client_id": "cid",
                        "access_token": access_token,
                        "expires_at": "2099-01-01T00:00:00Z",
                        "scopes": []
                    },
                    "channel": { "twitch_id": "999", "login": "takeover_chan" },
                    "connection": { "expires_at": "2099-01-01T00:00:00Z" }
                });
                let payload = body.to_string();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    payload.len(),
                    payload
                );
                let _ = socket.write_all(resp.as_bytes()).await;
            }
        });
        (port, server)
    }

    async fn refresh_production_path_race_once(iteration: u32) {
        let token = format!("loop-refresh-token-{iteration}");
        let (port, server) = spawn_mock_syndicate_refresh_server(&token).await;
        let _env = ProductionTestEnvGuard::install(port);

        let (state, services) = phase2_app().await;
        install_delegated_session(&state, &services).await;
        let mut session = state.delegated.read().await.clone().unwrap();
        session.twitch_expires_at = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        state.persist_delegated_session(&session).unwrap();
        *state.delegated.write().await = Some(session);
        *state.active_mode.write().await = TwitchActiveMode::Delegated;
        state.save_active_mode().await.unwrap();
        services.install_validated_authority_lease(1, None).await;
        install_fake_platform_workers(&state, &services).await;

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_commit_gate().await;
        start_delegated_refresh_loop(state.clone(), services.clone(), 1).await;
        gate_wait(
            arrived_rx,
            "production refresh must pause before durable commit",
        )
        .await;

        let refresh_before_gen = services
            .refresh_handle
            .read()
            .await
            .as_ref()
            .map(|t| t.generation);
        let replacement = stale_refresh_exchange("gen2-token");
        apply_exchange_session(
            state.clone(),
            services.clone(),
            "ssk_phase2_production_replacement",
            replacement,
            true,
            None,
        )
        .await
        .expect("production replacement apply");
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = platform_worker_snapshot(&state, &services).await;

        let _ = resume_tx.send(());
        if let Some(gen) = refresh_before_gen {
            tokio::time::timeout(GATE_WAIT, async {
                loop {
                    let finished = {
                        let guard = services.refresh_handle.read().await;
                        guard
                            .as_ref()
                            .map(|t| t.generation != gen || t.handle.is_finished())
                            .unwrap_or(true)
                    };
                    if finished {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("production refresh worker should finish after replacement");
        }

        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
        let disk = read_delegated_disk(&state).expect("winner primary on disk");
        assert_eq!(disk.generation, 2);
        assert_eq!(disk.access_token, "gen2-token");
        assert_ne!(
            state.twitch.read().await.tokens.access_token.as_deref(),
            Some(token.as_str())
        );

        server.abort();
        stop_all_platform_workers(&state, &services).await;
    }

    #[tokio::test]
    async fn refresh_production_path_cannot_escape_generation_fence() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for i in 0..20 {
            refresh_production_path_race_once(i).await;
        }
    }

    async fn refresh_worker_abort_during_kick_feed_take_once() {
        let token = "kick-take-abort-token";
        let (port, server) = spawn_mock_syndicate_refresh_server(token).await;
        let _env = ProductionTestEnvGuard::install(port);

        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;
        let mut session = state.delegated.read().await.clone().unwrap();
        session.twitch_expires_at = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        state.persist_delegated_session(&session).unwrap();
        *state.delegated.write().await = Some(session);
        services.install_validated_authority_lease(1, None).await;
        *state.kick_feed_handle.write().await = Some(tokio::spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        }));
        let pre_refresh_feed_id = state.kick_feed_handle.read().await.as_ref().map(|h| h.id());

        let (_gate, arrived_rx, resume_tx) =
            crate::kick::install_refresh_kick_feed_take_gate().await;
        start_delegated_refresh_loop(state.clone(), services.clone(), 1).await;
        gate_wait(arrived_rx, "refresh worker must pause at kick feed take").await;

        let stale_refresh_gen = services
            .refresh_handle
            .read()
            .await
            .as_ref()
            .map(|t| t.generation);
        let replacement = stale_refresh_exchange("gen2-kick-abort");
        apply_exchange_session(
            state.clone(),
            services.clone(),
            "ssk_phase2_kick_abort_replacement",
            replacement,
            true,
            None,
        )
        .await
        .expect("replacement during kick feed take");

        let _ = resume_tx.send(());
        if let Some(gen) = stale_refresh_gen {
            tokio::time::timeout(GATE_WAIT, async {
                loop {
                    let finished = {
                        let guard = services.refresh_handle.read().await;
                        guard
                            .as_ref()
                            .map(|t| t.generation != gen || t.handle.is_finished())
                            .unwrap_or(true)
                    };
                    if finished {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("stale refresh worker should terminate after abort");
        }

        if let Some(handle) = state.kick_feed_handle.read().await.as_ref() {
            if Some(handle.id()) == pre_refresh_feed_id {
                assert!(
                    handle.is_finished(),
                    "pre-refresh Kick feed must not survive as an orphan in the supervised slot"
                );
            }
        }

        server.abort();
        stop_all_platform_workers(&state, &services).await;
    }

    #[tokio::test]
    async fn refresh_worker_abort_during_kick_feed_take_terminates_orphan() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_worker_abort_during_kick_feed_take_once().await;
        }
    }

    async fn refresh_stale_before_twitch_publish_once() {
        begin_refresh_side_effect_race();
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_twitch_publish_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before twitch publish").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;
        resume_tx.send(()).expect("release stale twitch publish");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(committed);

        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
        crate::delegated_refresh_observability::assert_zero_pre_grant_side_effects();
    }

    async fn refresh_production_worker_before_twitch_publish_once(iteration: u32) {
        begin_refresh_side_effect_race();
        let token = format!("prod-twitch-grant-{iteration}");
        let (port, server) = spawn_mock_syndicate_refresh_server(&token).await;
        let _env = ProductionTestEnvGuard::install(port);

        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;
        let mut session = state.delegated.read().await.clone().unwrap();
        session.twitch_expires_at = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        state.persist_delegated_session(&session).unwrap();
        *state.delegated.write().await = Some(session);
        services.install_validated_authority_lease(1, None).await;
        install_fake_platform_workers(&state, &services).await;

        let (_gate, arrived_rx, resume_tx) = super::install_refresh_twitch_publish_gate().await;
        start_delegated_refresh_loop(state.clone(), services.clone(), 1).await;
        gate_wait(
            arrived_rx,
            "production worker must pause before IRC/EventSub grant",
        )
        .await;

        let stale_gen = services
            .refresh_handle
            .read()
            .await
            .as_ref()
            .map(|t| t.generation);
        commit_replacement_generation(&state, &services, 2, "gen2-prod-grant").await;
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;

        let _ = resume_tx.send(());
        if let Some(gen) = stale_gen {
            tokio::time::timeout(GATE_WAIT, async {
                loop {
                    let finished = {
                        let guard = services.refresh_handle.read().await;
                        guard
                            .as_ref()
                            .map(|t| t.generation != gen || t.handle.is_finished())
                            .unwrap_or(true)
                    };
                    if finished {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("stale production refresh worker should finish");
        }

        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
        crate::delegated_refresh_observability::assert_zero_pre_grant_side_effects();

        server.abort();
        stop_all_platform_workers(&state, &services).await;
    }

    #[tokio::test]
    async fn refresh_production_worker_before_twitch_publish_zero_pre_grant_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for i in 0..20 {
            refresh_production_worker_before_twitch_publish_once(i).await;
        }
    }
    #[tokio::test]
    async fn refresh_stale_before_twitch_publish_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_before_twitch_publish_once().await;
        }
    }

    async fn refresh_stale_before_kick_token_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) = crate::kick::install_refresh_kick_token_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before kick token write").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_identity = live_identity_snapshot(&state, &services).await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;
        resume_tx.send(()).expect("release stale kick token write");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(committed);

        assert_live_identity_eq(&state, &services, &winner_identity).await;
        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
    }

    #[tokio::test]
    async fn refresh_stale_before_kick_token_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_before_kick_token_once().await;
        }
    }

    async fn refresh_stale_before_kick_feed_once() {
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) =
            crate::kick::install_refresh_kick_feed_take_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before kick feed take").await;
        remove_delegated_session(&state, &services, None, 1)
            .await
            .expect("revoke");
        let workers_after_revoke = platform_worker_snapshot(&state, &services).await;
        let identity_after_revoke = live_identity_snapshot(&state, &services).await;
        resume_tx.send(()).expect("release stale kick feed take");
        let committed = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");
        assert!(committed);

        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            workers_after_revoke
        );
        assert_eq!(
            live_identity_snapshot(&state, &services).await.delegated,
            identity_after_revoke.delegated
        );
    }

    #[tokio::test]
    async fn refresh_stale_before_kick_feed_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_before_kick_feed_once().await;
        }
    }

    async fn refresh_stale_before_kick_feed_publish_once() {
        begin_refresh_side_effect_race();
        let (state, services) = phase2_app().await;
        activate_delegated_gen1(&state, &services).await;

        let exchange = stale_refresh_exchange("refresh-committed-token");
        let merged = crate::kick::merge_delegated_session_from_exchange(
            state.delegated.read().await.clone().unwrap(),
            &exchange,
        );
        let key = state
            .delegated
            .read()
            .await
            .as_ref()
            .unwrap()
            .connection_key
            .clone();

        let (_gate, arrived_rx, resume_tx) =
            crate::kick::install_refresh_kick_feed_publish_gate().await;
        let state2 = state.clone();
        let services2 = services.clone();
        let exchange2 = exchange.clone();
        let commit_task = tokio::spawn(async move {
            super::apply_delegated_refresh_commit(state2, services2, 1, merged, &exchange2, &key)
                .await
        });

        gate_wait(arrived_rx, "refresh must pause before kick feed publish").await;
        commit_replacement_generation(&state, &services, 2, "gen2-token").await;
        let winner_workers = install_winner_platform_workers(&state, &services, 2).await;
        resume_tx.send(()).expect("release stale kick feed publish");
        let _ = task_join_with_timeout(commit_task, "stale refresh commit")
            .await
            .expect("commit result");

        assert_eq!(
            platform_worker_snapshot(&state, &services).await,
            winner_workers
        );
        assert_eq!(
            crate::delegated_refresh_observability::kick_sse_connect_count(),
            0,
            "stale refresh must not connect Kick SSE before feed grant"
        );
    }

    #[tokio::test]
    async fn refresh_stale_before_kick_feed_publish_no_side_effects() {
        let _guard = PHASE2_TEST_LOCK.lock().await;
        for _ in 0..20 {
            refresh_stale_before_kick_feed_publish_once().await;
        }
    }
}
