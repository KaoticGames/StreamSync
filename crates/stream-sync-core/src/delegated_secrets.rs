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
const TWITCH_DELEGATED_BUNDLE_KEY_PREFIX: &str = "twitch.delegated.bundle.";

pub fn delegated_bundle_slot_key(slot: u8) -> String {
    format!("{TWITCH_DELEGATED_BUNDLE_SLOT_PREFIX}{slot}")
}

pub fn all_delegated_bundle_slot_keys() -> [String; DELEGATED_BUNDLE_SLOT_COUNT as usize] {
    [delegated_bundle_slot_key(0), delegated_bundle_slot_key(1)]
}

pub fn alternate_bundle_slot(slot: u8) -> u8 {
    1 - (slot % DELEGATED_BUNDLE_SLOT_COUNT)
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
                return Ok(Some(legacy));
            }
            return Ok(None);
        }
    };
    validate_bundle_session_binding(&bundle, session)?;
    Ok(Some(bundle))
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

/// Collision-safe slot write: never overwrite a different active transaction.
pub fn write_delegated_bundle_create(
    store: &dyn SecretStore,
    bundle: &DelegatedSecretBundle,
) -> Result<()> {
    validate_bundle_secrets(bundle)?;
    let key = delegated_bundle_slot_key(bundle.slot);
    if let Some(existing) = read_delegated_bundle_at_slot(store, bundle.slot)? {
        let same_transaction = existing.transaction_id == bundle.transaction_id
            && existing.generation == bundle.generation;
        let supersedes_older = bundle.transaction_id > existing.transaction_id;
        if !same_transaction && !supersedes_older {
            return Err(anyhow!(
                "delegated bundle slot {} occupied by transaction ({}, gen {})",
                bundle.slot,
                existing.transaction_id,
                existing.generation
            ));
        }
    }
    let bytes = serde_json::to_vec(bundle)?;
    store.set(&key, &bytes)
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

pub fn delegated_secret_store_authority_remain(store: &dyn SecretStore) -> Result<bool> {
    for key in all_delegated_bundle_slot_keys() {
        if store.get(&key)?.is_some() {
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
