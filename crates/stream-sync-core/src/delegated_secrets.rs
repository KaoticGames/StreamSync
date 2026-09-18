//! Bounded two-slot delegated secret journal (fail-closed load, collision-safe persist).
//!
//! **Crash model:** metadata commit points at `(secret_revision, bundle_slot)`; the bound
//! bundle must carry matching `transaction_id` (= revision) and `generation`. Persist writes
//! the alternate slot first, then commits metadata. Previous slot is retained until revoke
//! clears both slots — superseded apply rollback can therefore restore the prior pair.

use crate::config_types::DelegatedSessionFile;
use crate::secret_store::{
    SecretStore, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_CONNECTION_KEY,
    TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
};
use crate::store_lock::with_cross_process_lock;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

pub const DELEGATED_BUNDLE_SLOT_COUNT: u8 = 2;
pub const TWITCH_DELEGATED_BUNDLE_SLOT_PREFIX: &str = "twitch.delegated.bundle.slot.";

/// Legacy revision-key prefix (removed on migrate/revoke; not used for new writes).
pub const TWITCH_DELEGATED_BUNDLE_KEY_PREFIX: &str = "twitch.delegated.bundle.";

/// Committed delegated transaction identity from metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelegatedCommittedIdentity {
    pub secret_revision: u64,
    pub generation: u64,
    pub bundle_slot: u8,
}

/// Where a bound bundle was loaded from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundBundleProvenance {
    Slot(u8),
    LegacyRevisionKey(u64),
}

/// Complete delegated durable authority epoch (captured/restored under one lock).
#[derive(Debug, Clone)]
pub struct DelegatedAuthorityEpoch {
    pub metadata: Option<Vec<u8>>,
    pub active_mode: Option<Vec<u8>>,
    pub tombstone: bool,
    pub pending: bool,
    pub bundle_slots: [Option<Vec<u8>>; DELEGATED_BUNDLE_SLOT_COUNT as usize],
    pub legacy_revision_bundle: Option<(String, Vec<u8>)>,
    pub committed: Option<DelegatedCommittedIdentity>,
}

pub fn delegated_bundle_slot_key(slot: u8) -> String {
    format!("{TWITCH_DELEGATED_BUNDLE_SLOT_PREFIX}{slot}")
}

pub fn all_delegated_bundle_slot_keys() -> [String; DELEGATED_BUNDLE_SLOT_COUNT as usize] {
    [delegated_bundle_slot_key(0), delegated_bundle_slot_key(1)]
}

pub fn legacy_revision_bundle_key(revision: u64) -> String {
    format!("{TWITCH_DELEGATED_BUNDLE_KEY_PREFIX}{revision}")
}

pub fn alternate_bundle_slot(slot: u8) -> Result<u8> {
    validate_slot(slot)?;
    Ok(1 - slot)
}

fn delegated_authority_lock_path(delegated_metadata: &Path) -> std::path::PathBuf {
    delegated_metadata.with_extension("authority.lock")
}

/// Serialize persist, revoke, and bundle rollback through one durable-authority lock.
pub fn with_delegated_authority_lock<R>(
    delegated_metadata: &Path,
    f: impl FnOnce() -> Result<R>,
) -> Result<R> {
    with_cross_process_lock(&delegated_authority_lock_path(delegated_metadata), f)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedSecretBundle {
    /// Immutable transaction identity — must equal metadata `secret_revision`.
    pub transaction_id: u64,
    /// Session generation this bundle is bound to.
    pub generation: u64,
    /// Fixed journal slot (0 or 1).
    pub slot: u8,
    pub connection_key: String,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kick_access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kick_refresh_token: Option<String>,
}

/// Legacy on-disk bundle shape (revision-key era).
#[derive(Debug, Deserialize)]
struct LegacyRevisionBundle {
    revision: u64,
    connection_key: String,
    access_token: String,
    kick_access_token: Option<String>,
    kick_refresh_token: Option<String>,
}

fn nonempty_owned(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn read_secret_value(store: &dyn SecretStore, key: &str) -> Result<Option<String>> {
    let Some(bytes) = store.get(key)? else {
        return Ok(None);
    };
    let decoded = String::from_utf8(bytes)
        .map_err(|_| anyhow!("secret store value for '{key}' is not valid UTF-8"))?;
    Ok(nonempty_owned(Some(decoded)))
}

pub fn classify_inline_delegated_secrets(
    connection_key: &str,
    access_token: &str,
) -> Result<InlineDelegatedSecretState> {
    let has_conn = !connection_key.trim().is_empty();
    let has_at = !access_token.trim().is_empty();
    match (has_conn, has_at) {
        (true, true) => Ok(InlineDelegatedSecretState::BothInline),
        (false, false) => Ok(InlineDelegatedSecretState::BothAbsent),
        _ => Err(anyhow!("delegated session has partial inline secrets")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineDelegatedSecretState {
    BothInline,
    BothAbsent,
}

fn validate_slot(slot: u8) -> Result<()> {
    if slot >= DELEGATED_BUNDLE_SLOT_COUNT {
        return Err(anyhow!("delegated bundle slot out of range: {slot}"));
    }
    Ok(())
}

/// Reject corrupt committed metadata before selecting an alternate slot.
pub fn validate_committed_bundle_slot(slot: u8) -> Result<()> {
    validate_slot(slot)
}

fn validate_bundle_secrets(bundle: &DelegatedSecretBundle) -> Result<()> {
    if bundle.transaction_id == 0 {
        return Err(anyhow!(
            "delegated secret bundle transaction_id must be non-zero"
        ));
    }
    validate_slot(bundle.slot)?;
    if bundle.connection_key.trim().is_empty() || bundle.access_token.trim().is_empty() {
        return Err(anyhow!(
            "delegated secret bundle is missing required secrets"
        ));
    }
    Ok(())
}

/// Kick optional state must be internally coherent before activating delegated authority.
pub fn validate_delegated_session_coherence(session: &DelegatedSessionFile) -> Result<()> {
    let kick_access = nonempty_owned(session.kick_access_token.clone());
    let kick_refresh = nonempty_owned(session.kick_refresh_token.clone());
    let kick_id = session
        .kick_id
        .as_ref()
        .is_some_and(|s| !s.trim().is_empty());

    if kick_refresh.is_some() && kick_access.is_none() {
        return Err(anyhow!(
            "delegated Kick refresh token present without access token"
        ));
    }
    if kick_id && kick_access.is_none() {
        return Err(anyhow!(
            "delegated metadata claims Kick identity without delegated Kick access token"
        ));
    }
    if kick_access.is_some() && !kick_id {
        return Err(anyhow!(
            "delegated Kick access token present without metadata kick_id"
        ));
    }
    Ok(())
}

pub fn validate_bundle_session_binding(
    bundle: &DelegatedSecretBundle,
    session: &DelegatedSessionFile,
) -> Result<()> {
    if bundle.transaction_id != session.secret_revision {
        return Err(anyhow!(
            "delegated bundle transaction_id {0} does not match metadata secret_revision {1}",
            bundle.transaction_id,
            session.secret_revision
        ));
    }
    if bundle.generation != session.generation {
        return Err(anyhow!(
            "delegated bundle generation {0} does not match metadata generation {1}",
            bundle.generation,
            session.generation
        ));
    }
    if bundle.slot != session.bundle_slot {
        return Err(anyhow!(
            "delegated bundle slot {0} does not match metadata bundle_slot {1}",
            bundle.slot,
            session.bundle_slot
        ));
    }
    let mut bound = session.clone();
    bound.connection_key = bundle.connection_key.clone();
    bound.access_token = bundle.access_token.clone();
    bound.kick_access_token = bundle.kick_access_token.clone();
    bound.kick_refresh_token = bundle.kick_refresh_token.clone();
    validate_delegated_session_coherence(&bound)?;
    Ok(())
}

pub fn read_delegated_bundle_at_slot(
    store: &dyn SecretStore,
    slot: u8,
) -> Result<Option<DelegatedSecretBundle>> {
    validate_slot(slot)?;
    let key = delegated_bundle_slot_key(slot);
    let Some(bytes) = store.get(&key)? else {
        return Ok(None);
    };
    let bundle: DelegatedSecretBundle = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse delegated secret bundle at slot {slot}"))?;
    if bundle.slot != slot {
        return Err(anyhow!(
            "delegated secret bundle slot mismatch (expected {slot}, got {0})",
            bundle.slot
        ));
    }
    validate_bundle_secrets(&bundle)?;
    Ok(Some(bundle))
}

/// Read the bundle bound by committed metadata; fails closed on mismatch.
pub fn read_bound_delegated_bundle(
    store: &dyn SecretStore,
    session: &DelegatedSessionFile,
) -> Result<Option<DelegatedSecretBundle>> {
    read_bound_delegated_bundle_with_provenance(store, session)
        .map(|opt| opt.map(|(bundle, _)| bundle))
}

/// Read the bound bundle and report whether it came from a slot or legacy revision key.
pub fn read_bound_delegated_bundle_with_provenance(
    store: &dyn SecretStore,
    session: &DelegatedSessionFile,
) -> Result<Option<(DelegatedSecretBundle, BoundBundleProvenance)>> {
    if session.secret_revision == 0 {
        return Ok(None);
    }
    let bundle = match read_delegated_bundle_at_slot(store, session.bundle_slot)? {
        Some(bundle) => bundle,
        None => {
            if let Some(mut legacy) = read_legacy_revision_bundle(store, session.secret_revision)? {
                legacy.generation = session.generation;
                legacy.slot = session.bundle_slot;
                validate_bundle_session_binding(&legacy, session)?;
                return Ok(Some((
                    legacy,
                    BoundBundleProvenance::LegacyRevisionKey(session.secret_revision),
                )));
            }
            return Ok(None);
        }
    };
    validate_bundle_session_binding(&bundle, session)?;
    Ok(Some((
        bundle,
        BoundBundleProvenance::Slot(session.bundle_slot),
    )))
}

fn read_legacy_revision_bundle(
    store: &dyn SecretStore,
    revision: u64,
) -> Result<Option<DelegatedSecretBundle>> {
    if revision == 0 {
        return Ok(None);
    }
    let key = format!("{TWITCH_DELEGATED_BUNDLE_KEY_PREFIX}{revision}");
    let Some(bytes) = store.get(&key)? else {
        return Ok(None);
    };
    let legacy: LegacyRevisionBundle = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse legacy delegated bundle at revision {revision}"))?;
    if legacy.revision != revision {
        return Err(anyhow!("legacy delegated bundle revision mismatch"));
    }
    Ok(Some(DelegatedSecretBundle {
        transaction_id: legacy.revision,
        generation: 0, // caller must set generation before binding validation
        slot: 0,
        connection_key: legacy.connection_key,
        access_token: legacy.access_token,
        kick_access_token: legacy.kick_access_token,
        kick_refresh_token: legacy.kick_refresh_token,
    }))
}

fn bundle_canonical_bytes(bundle: &DelegatedSecretBundle) -> Result<Vec<u8>> {
    serde_json::to_vec(bundle).context("canonical delegated bundle bytes")
}

/// Collision-safe slot write: never overwrite a different active transaction.
pub fn write_delegated_bundle_create(
    store: &dyn SecretStore,
    bundle: &DelegatedSecretBundle,
) -> Result<()> {
    validate_bundle_secrets(bundle)?;
    let key = delegated_bundle_slot_key(bundle.slot);
    let new_bytes = bundle_canonical_bytes(bundle)?;
    if let Some(existing) = read_delegated_bundle_at_slot(store, bundle.slot)? {
        let same_transaction = existing.transaction_id == bundle.transaction_id
            && existing.generation == bundle.generation;
        if same_transaction {
            if bundle_canonical_bytes(&existing)? != new_bytes {
                return Err(anyhow!(
                    "delegated bundle transaction ({}, gen {}) collision: secrets differ",
                    bundle.transaction_id,
                    bundle.generation
                ));
            }
            return Ok(());
        }
        let supersedes_older = bundle.transaction_id > existing.transaction_id;
        if !supersedes_older {
            return Err(anyhow!(
                "delegated bundle slot {} occupied by transaction ({}, gen {})",
                bundle.slot,
                existing.transaction_id,
                existing.generation
            ));
        }
    }
    store.set(&key, &new_bytes)
}

pub fn delete_all_delegated_bundle_slots(store: &dyn SecretStore) -> Result<()> {
    for key in all_delegated_bundle_slot_keys() {
        store.delete(&key)?;
    }
    Ok(())
}

pub fn delete_legacy_delegated_secret_keys(store: &dyn SecretStore) -> Result<()> {
    store.delete(TWITCH_DELEGATED_CONNECTION_KEY)?;
    store.delete(TWITCH_DELEGATED_ACCESS_TOKEN_KEY)?;
    store.delete(TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
    store.delete(TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
    Ok(())
}

pub fn legacy_revision_bundle_authority_remain(
    store: &dyn SecretStore,
    revision: u64,
) -> Result<bool> {
    if revision == 0 {
        return Ok(false);
    }
    Ok(store.get(&legacy_revision_bundle_key(revision))?.is_some())
}

pub fn delegated_secret_store_authority_remain(
    store: &dyn SecretStore,
    metadata_revision: Option<u64>,
) -> Result<bool> {
    for key in all_delegated_bundle_slot_keys() {
        if store.get(&key)?.is_some() {
            return Ok(true);
        }
    }
    if let Some(revision) = metadata_revision {
        if legacy_revision_bundle_authority_remain(store, revision)? {
            return Ok(true);
        }
    }
    for key in [
        TWITCH_DELEGATED_CONNECTION_KEY,
        TWITCH_DELEGATED_ACCESS_TOKEN_KEY,
        TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY,
        TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
    ] {
        if store.get(key)?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn delete_legacy_revision_bundle(store: &dyn SecretStore, revision: u64) -> Result<()> {
    if revision == 0 {
        return Ok(());
    }
    store.delete(&legacy_revision_bundle_key(revision))
}

pub fn read_legacy_delegated_secret_pair(
    store: &dyn SecretStore,
) -> Result<Option<(String, String)>> {
    let conn = read_secret_value(store, TWITCH_DELEGATED_CONNECTION_KEY)?;
    let at = read_secret_value(store, TWITCH_DELEGATED_ACCESS_TOKEN_KEY)?;
    match (conn, at) {
        (Some(c), Some(a)) => Ok(Some((c, a))),
        (None, None) => Ok(None),
        _ => Err(anyhow!("delegated secret store has partial legacy pair")),
    }
}

pub fn bundle_from_session(
    session: &DelegatedSessionFile,
    transaction_id: u64,
    slot: u8,
) -> DelegatedSecretBundle {
    DelegatedSecretBundle {
        transaction_id,
        generation: session.generation,
        slot,
        connection_key: session.connection_key.clone(),
        access_token: session.access_token.clone(),
        kick_access_token: session.kick_access_token.clone(),
        kick_refresh_token: session.kick_refresh_token.clone(),
    }
}

pub fn apply_bundle_to_session(session: &mut DelegatedSessionFile, bundle: &DelegatedSecretBundle) {
    session.secret_revision = bundle.transaction_id;
    session.bundle_slot = bundle.slot;
    session.connection_key = bundle.connection_key.clone();
    session.access_token = bundle.access_token.clone();
    session.kick_access_token = bundle.kick_access_token.clone();
    session.kick_refresh_token = bundle.kick_refresh_token.clone();
}

pub fn hydrate_inline_secrets_in_memory(session: &mut DelegatedSessionFile) -> Result<()> {
    classify_inline_delegated_secrets(&session.connection_key, &session.access_token)?;
    session.secret_revision = 1;
    session.bundle_slot = 0;
    validate_delegated_session_coherence(session)?;
    Ok(())
}

pub fn hydrate_legacy_store_pair_in_memory(
    store: &dyn SecretStore,
    session: &mut DelegatedSessionFile,
    connection_key: String,
    access_token: String,
) -> Result<()> {
    let kick_access = read_secret_value(store, TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
    let kick_refresh = read_secret_value(store, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
    session.connection_key = connection_key;
    session.access_token = access_token;
    session.kick_access_token = kick_access;
    session.kick_refresh_token = kick_refresh;
    session.secret_revision = 1;
    session.bundle_slot = 0;
    validate_delegated_session_coherence(session)?;
    Ok(())
}

pub fn migrate_legacy_inline_to_bundle(
    store: &dyn SecretStore,
    session: &DelegatedSessionFile,
) -> Result<DelegatedSecretBundle> {
    let bundle = bundle_from_session(session, 1, 0);
    validate_delegated_session_coherence(session)?;
    write_delegated_bundle_create(store, &bundle)?;
    delete_legacy_delegated_secret_keys(store)?;
    Ok(bundle)
}

pub fn migrate_legacy_store_pair_to_bundle(
    store: &dyn SecretStore,
    session: &DelegatedSessionFile,
    connection_key: String,
    access_token: String,
) -> Result<DelegatedSecretBundle> {
    let mut hydrated = session.clone();
    hydrate_legacy_store_pair_in_memory(store, &mut hydrated, connection_key, access_token)?;
    migrate_legacy_inline_to_bundle(store, &hydrated)
}

pub fn capture_delegated_bundle_slots(
    store: &dyn SecretStore,
) -> Result<[Option<Vec<u8>>; DELEGATED_BUNDLE_SLOT_COUNT as usize]> {
    let mut out = [None, None];
    for slot in 0..DELEGATED_BUNDLE_SLOT_COUNT {
        let key = delegated_bundle_slot_key(slot);
        out[slot as usize] = match store.get(&key)? {
            Some(bytes) => Some(bytes),
            None => None,
        };
    }
    Ok(out)
}

pub fn restore_delegated_bundle_slots(
    store: &dyn SecretStore,
    slots: &[Option<Vec<u8>>; DELEGATED_BUNDLE_SLOT_COUNT as usize],
) -> Result<()> {
    for slot in 0..DELEGATED_BUNDLE_SLOT_COUNT {
        let key = delegated_bundle_slot_key(slot);
        match &slots[slot as usize] {
            Some(bytes) => store.set(&key, bytes)?,
            None => store.delete(&key)?,
        }
    }
    Ok(())
}

pub fn parse_committed_identity_from_metadata_bytes(
    bytes: &[u8],
) -> Result<Option<DelegatedCommittedIdentity>> {
    let meta: serde_json::Value =
        serde_json::from_slice(bytes).context("parse delegated metadata for committed identity")?;
    let secret_revision = meta
        .get("secret_revision")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    if secret_revision == 0 {
        return Ok(None);
    }
    let generation = meta.get("generation").and_then(|v| v.as_u64()).unwrap_or(0);
    let bundle_slot = meta
        .get("bundle_slot")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u8;
    validate_slot(bundle_slot)?;
    Ok(Some(DelegatedCommittedIdentity {
        secret_revision,
        generation,
        bundle_slot,
    }))
}

pub fn capture_delegated_authority_epoch(
    delegated_metadata: &Path,
    active_mode_path: &Path,
    revoked_tombstone_path: &Path,
    revoke_pending_path: &Path,
    store: &dyn SecretStore,
) -> Result<DelegatedAuthorityEpoch> {
    let metadata = read_path_bytes_if_exists(delegated_metadata)?;
    let committed = metadata
        .as_deref()
        .and_then(|bytes| parse_committed_identity_from_metadata_bytes(bytes).ok())
        .flatten();
    let legacy_revision_bundle = committed
        .map(|id| legacy_revision_bundle_key(id.secret_revision))
        .and_then(|key| store.get(&key).ok().flatten().map(|bytes| (key, bytes)));
    Ok(DelegatedAuthorityEpoch {
        metadata,
        active_mode: read_path_bytes_if_exists(active_mode_path)?,
        tombstone: revoked_tombstone_path.is_file(),
        pending: revoke_pending_path.is_file(),
        bundle_slots: capture_delegated_bundle_slots(store)?,
        legacy_revision_bundle,
        committed,
    })
}

pub fn restore_delegated_authority_epoch(
    delegated_metadata: &Path,
    active_mode_path: &Path,
    revoked_tombstone_path: &Path,
    revoke_pending_path: &Path,
    store: &dyn SecretStore,
    epoch: &DelegatedAuthorityEpoch,
) -> Result<()> {
    restore_delegated_bundle_slots(store, &epoch.bundle_slots)?;
    if let Some((key, bytes)) = &epoch.legacy_revision_bundle {
        store.set(key, bytes)?;
    } else if let Some(committed) = epoch.committed {
        delete_legacy_revision_bundle(store, committed.secret_revision)?;
    }
    restore_or_remove_path(delegated_metadata, epoch.metadata.as_deref(), true)?;
    restore_or_remove_path(active_mode_path, epoch.active_mode.as_deref(), false)?;
    restore_marker_file(revoked_tombstone_path, epoch.tombstone, |path| {
        crate::storage::write_delegated_revoked_tombstone(path)
    })?;
    restore_marker_file(revoke_pending_path, epoch.pending, |path| {
        crate::storage::write_delegated_revoke_pending(path)
    })?;
    Ok(())
}

/// Refuse rollback when durable authority advanced past the stale apply's post-persist identity.
pub fn rollback_refused_durable_advanced() -> anyhow::Error {
    anyhow!("stale rollback refused: durable state advanced")
}

pub fn assert_rollback_cas(
    current: Option<DelegatedCommittedIdentity>,
    expected_post_persist: Option<DelegatedCommittedIdentity>,
    pre_apply: &DelegatedAuthorityEpoch,
) -> Result<()> {
    match expected_post_persist {
        Some(expected) => {
            if current.as_ref() != Some(&expected) {
                return Err(rollback_refused_durable_advanced());
            }
        }
        None => {
            if current != pre_apply.committed {
                return Err(rollback_refused_durable_advanced());
            }
        }
    }
    Ok(())
}

pub fn migrate_legacy_revision_bundle_to_slot(
    delegated_metadata: &Path,
    store: &dyn SecretStore,
    _session: &DelegatedSessionFile,
    bundle: &DelegatedSecretBundle,
    revision: u64,
) -> Result<()> {
    with_delegated_authority_lock(delegated_metadata, || {
        write_delegated_bundle_create(store, bundle)?;
        delete_legacy_revision_bundle(store, revision)?;
        Ok(())
    })
}

fn read_path_bytes_if_exists(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn restore_or_remove_path(path: &Path, bytes: Option<&[u8]>, authority_secret: bool) -> Result<()> {
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
    path: &Path,
    should_exist: bool,
    write: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    if should_exist {
        write(path)
    } else {
        crate::storage::remove_file_durable(path)
    }
}

/// Deterministic concurrency gates for integration tests (no-op when not installed).
pub mod authority_gates {
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::sync::{Mutex, OnceLock};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum DelegatedAuthorityBoundary {
        PersistAfterBundleWrite,
        PersistBeforeMarkerClear,
        RollbackBeforeRestore,
    }

    struct GateState {
        boundary: DelegatedAuthorityBoundary,
        arrived_tx: Option<Sender<()>>,
        resume_rx: Option<Receiver<()>>,
    }

    static AUTHORITY_GATE: OnceLock<Mutex<Option<GateState>>> = OnceLock::new();

    fn slot() -> &'static Mutex<Option<GateState>> {
        AUTHORITY_GATE.get_or_init(|| Mutex::new(None))
    }

    pub fn clear() {
        if let Ok(mut guard) = slot().lock() {
            *guard = None;
        }
    }

    pub struct GateGuard;

    impl Drop for GateGuard {
        fn drop(&mut self) {
            clear();
        }
    }

    pub fn install(boundary: DelegatedAuthorityBoundary) -> (GateGuard, Receiver<()>, Sender<()>) {
        clear();
        let (arrived_tx, arrived_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        slot()
            .lock()
            .expect("authority gate lock")
            .replace(GateState {
                boundary,
                arrived_tx: Some(arrived_tx),
                resume_rx: Some(resume_rx),
            });
        (GateGuard, arrived_rx, resume_tx)
    }

    pub fn pause_blocking(boundary: DelegatedAuthorityBoundary) {
        let (resume_rx, arrived_tx) = {
            let mut guard = slot().lock().expect("authority gate lock");
            let Some(state) = guard.as_mut() else {
                return;
            };
            if state.boundary != boundary {
                return;
            }
            let arrived_tx = state.arrived_tx.take();
            let resume_rx = state.resume_rx.take();
            (resume_rx, arrived_tx)
        };
        if let Some(tx) = arrived_tx {
            let _ = tx.send(());
        }
        if let Some(rx) = resume_rx {
            let _ = rx.recv();
        }
    }
}
