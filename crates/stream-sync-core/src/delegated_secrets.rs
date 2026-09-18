//! Revision-bound delegated secret bundles (fail-closed load, atomic persist ordering).

use crate::config_types::DelegatedSessionFile;
use crate::secret_store::{
    SecretStore, TWITCH_DELEGATED_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_CONNECTION_KEY,
    TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY,
};
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

pub const TWITCH_DELEGATED_BUNDLE_KEY_PREFIX: &str = "twitch.delegated.bundle.";

pub fn delegated_bundle_store_key(revision: u64) -> String {
    format!("{TWITCH_DELEGATED_BUNDLE_KEY_PREFIX}{revision}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegatedSecretBundle {
    pub revision: u64,
    pub connection_key: String,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kick_access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kick_refresh_token: Option<String>,
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

pub fn read_delegated_bundle(store: &dyn SecretStore, revision: u64) -> Result<Option<DelegatedSecretBundle>> {
    if revision == 0 {
        return Ok(None);
    }
    let key = delegated_bundle_store_key(revision);
    let Some(bytes) = store.get(&key)? else {
        return Ok(None);
    };
    let bundle: DelegatedSecretBundle = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse delegated secret bundle at revision {revision}"))?;
    if bundle.revision != revision {
        return Err(anyhow!(
            "delegated secret bundle revision mismatch (expected {revision}, got {0})",
            bundle.revision
        ));
    }
    if bundle.connection_key.trim().is_empty() || bundle.access_token.trim().is_empty() {
        return Err(anyhow!("delegated secret bundle is missing required secrets"));
    }
    Ok(Some(bundle))
}

pub fn write_delegated_bundle(store: &dyn SecretStore, bundle: &DelegatedSecretBundle) -> Result<()> {
    if bundle.revision == 0 {
        return Err(anyhow!("delegated secret bundle revision must be non-zero"));
    }
    if bundle.connection_key.trim().is_empty() || bundle.access_token.trim().is_empty() {
        return Err(anyhow!("delegated secret bundle is missing required secrets"));
    }
    let key = delegated_bundle_store_key(bundle.revision);
    let bytes = serde_json::to_vec(bundle)?;
    store.set(&key, &bytes)
}

pub fn delete_delegated_bundle(store: &dyn SecretStore, revision: u64) -> Result<()> {
    if revision == 0 {
        return Ok(());
    }
    store.delete(&delegated_bundle_store_key(revision))
}

pub fn delete_legacy_delegated_secret_keys(store: &dyn SecretStore) -> Result<()> {
    store.delete(TWITCH_DELEGATED_CONNECTION_KEY)?;
    store.delete(TWITCH_DELEGATED_ACCESS_TOKEN_KEY)?;
    store.delete(TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
    store.delete(TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
    Ok(())
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

pub fn bundle_from_session(session: &DelegatedSessionFile, revision: u64) -> DelegatedSecretBundle {
    DelegatedSecretBundle {
        revision,
        connection_key: session.connection_key.clone(),
        access_token: session.access_token.clone(),
        kick_access_token: session.kick_access_token.clone(),
        kick_refresh_token: session.kick_refresh_token.clone(),
    }
}

pub fn apply_bundle_to_session(session: &mut DelegatedSessionFile, bundle: &DelegatedSecretBundle) {
    session.secret_revision = bundle.revision;
    session.connection_key = bundle.connection_key.clone();
    session.access_token = bundle.access_token.clone();
    session.kick_access_token = bundle.kick_access_token.clone();
    session.kick_refresh_token = bundle.kick_refresh_token.clone();
}

pub fn migrate_legacy_inline_to_bundle(
    store: &dyn SecretStore,
    connection_key: &str,
    access_token: &str,
    kick_access: Option<&str>,
    kick_refresh: Option<&str>,
) -> Result<DelegatedSecretBundle> {
    let bundle = DelegatedSecretBundle {
        revision: 1,
        connection_key: connection_key.trim().to_string(),
        access_token: access_token.trim().to_string(),
        kick_access_token: nonempty_owned(kick_access.map(|s| s.to_string())),
        kick_refresh_token: nonempty_owned(kick_refresh.map(|s| s.to_string())),
    };
    write_delegated_bundle(store, &bundle)?;
    delete_legacy_delegated_secret_keys(store)?;
    Ok(bundle)
}

pub fn migrate_legacy_store_pair_to_bundle(
    store: &dyn SecretStore,
    connection_key: String,
    access_token: String,
) -> Result<DelegatedSecretBundle> {
    let kick_access = read_secret_value(store, TWITCH_DELEGATED_KICK_ACCESS_TOKEN_KEY)?;
    let kick_refresh = read_secret_value(store, TWITCH_DELEGATED_KICK_REFRESH_TOKEN_KEY)?;
    migrate_legacy_inline_to_bundle(
        store,
        &connection_key,
        &access_token,
        kick_access.as_deref(),
        kick_refresh.as_deref(),
    )
}
