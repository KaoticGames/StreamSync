//! Strict `DirectSessionBinding` parse and device binding checks (Slice-1 capability JSON).

use crate::voice_delivery::direct_local::limits::JS_MAX_SAFE_INTEGER;
use crate::voice_delivery::finalized_manifest::validate_discord_snowflake;
use crate::voice_delivery::finalized_manifest::FinalizedManifestError;
use serde_json::Value;
use std::net::IpAddr;
use thiserror::Error;
use url::Host;
use url::Url;
use uuid::Uuid;

const CAPABILITY_TOKEN_LEN: usize = 43;

#[derive(Clone, PartialEq, Eq)]
pub struct DirectSessionBinding {
    pub session_id: String,
    pub guild_id: String,
    pub device_id: String,
    pub delivery_generation: u64,
    pub discord_instance_id: String,
    pub leader_fence_generation: u64,
    pub token: String,
    pub expires_at_ms: u64,
    pub wss_url: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectSessionBindingError {
    #[error("expected json object")]
    NotJSONObject,
    #[error("unknown field")]
    UnknownField,
    #[error("missing field: {field}")]
    MissingField { field: &'static str },
    #[error("malformed id: {0}")]
    MalformedId(String),
    #[error("unsafe integer: {field}")]
    UnsafeInteger { field: &'static str },
    #[error("malformed capability token")]
    MalformedCapabilityToken,
    #[error("invalid wss url")]
    InvalidWssUrl,
    #[error("device mismatch")]
    DeviceMismatch,
}

const BINDING_KEYS: &[&str] = &[
    "sessionId",
    "guildId",
    "deviceId",
    "deliveryGeneration",
    "discordInstanceId",
    "leaderFenceGeneration",
    "token",
    "expiresAtMs",
    "wssUrl",
];

impl DirectSessionBinding {
    pub fn from_json(json: &Value) -> Result<Self, DirectSessionBindingError> {
        let obj = match json {
            Value::Object(map) => map,
            _ => return Err(DirectSessionBindingError::NotJSONObject),
        };
        for key in obj.keys() {
            if !BINDING_KEYS.contains(&key.as_str()) {
                return Err(DirectSessionBindingError::UnknownField);
            }
        }
        for key in BINDING_KEYS {
            if !obj.contains_key(*key) {
                return Err(DirectSessionBindingError::MissingField { field: key });
            }
        }

        let session_id = require_session_id(obj.get("sessionId").expect("checked"))?;
        let guild_id = require_guild_id(obj.get("guildId").expect("checked"))?;
        let device_id =
            require_non_empty_string(obj.get("deviceId").expect("checked"), "deviceId")?;
        let delivery_generation = require_positive_js_safe_u64(
            obj.get("deliveryGeneration").expect("checked"),
            "deliveryGeneration",
        )?;
        let discord_instance_id =
            require_discord_instance_id(obj.get("discordInstanceId").expect("checked"))?;
        let leader_fence_generation = require_positive_js_safe_u64(
            obj.get("leaderFenceGeneration").expect("checked"),
            "leaderFenceGeneration",
        )?;
        let token = require_capability_token(obj.get("token").expect("checked"))?;
        let expires_at_ms =
            require_positive_js_safe_u64(obj.get("expiresAtMs").expect("checked"), "expiresAtMs")?;
        let wss_url = require_wss_url(obj.get("wssUrl").expect("checked"))?;

        Ok(Self {
            session_id,
            guild_id,
            device_id,
            delivery_generation,
            discord_instance_id,
            leader_fence_generation,
            token,
            expires_at_ms,
            wss_url,
        })
    }

    pub fn assert_matches_local_device(
        &self,
        local_device_id: &str,
    ) -> Result<(), DirectSessionBindingError> {
        if self.device_id != local_device_id {
            return Err(DirectSessionBindingError::DeviceMismatch);
        }
        Ok(())
    }
}

impl std::fmt::Debug for DirectSessionBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectSessionBinding")
            .field("session_id", &self.session_id)
            .field("guild_id", &self.guild_id)
            .field("device_id", &self.device_id)
            .field("delivery_generation", &self.delivery_generation)
            .field("discord_instance_id", &self.discord_instance_id)
            .field("leader_fence_generation", &self.leader_fence_generation)
            .field("token", &"[REDACTED]")
            .field("expires_at_ms", &self.expires_at_ms)
            .field("wss_url", &self.wss_url)
            .finish()
    }
}

fn require_non_empty_string(
    v: &Value,
    field: &'static str,
) -> Result<String, DirectSessionBindingError> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        _ => Err(DirectSessionBindingError::MalformedId(field.into())),
    }
}

fn require_session_id(v: &Value) -> Result<String, DirectSessionBindingError> {
    let s = match v {
        Value::String(s) => s.as_str(),
        _ => return Err(DirectSessionBindingError::MalformedId("sessionId".into())),
    };
    if s.trim().is_empty() || s != s.trim() {
        return Err(DirectSessionBindingError::MalformedId("sessionId".into()));
    }
    if Uuid::parse_str(s).is_err() {
        return Err(DirectSessionBindingError::MalformedId("sessionId".into()));
    }
    Ok(s.to_string())
}

fn require_guild_id(v: &Value) -> Result<String, DirectSessionBindingError> {
    let s = match v {
        Value::String(s) => s.as_str(),
        _ => return Err(DirectSessionBindingError::MalformedId("guildId".into())),
    };
    if s.is_empty() {
        return Err(DirectSessionBindingError::MalformedId("guildId".into()));
    }
    match validate_discord_snowflake(s, "guildId") {
        Ok(id) => Ok(id),
        Err(FinalizedManifestError::Invalid(_)) => {
            Err(DirectSessionBindingError::MalformedId("guildId".into()))
        }
    }
}

fn require_discord_instance_id(v: &Value) -> Result<String, DirectSessionBindingError> {
    let s = require_non_empty_string(v, "discordInstanceId")?;
    if s.len() > 128 {
        return Err(DirectSessionBindingError::MalformedId(
            "discordInstanceId".into(),
        ));
    }
    Ok(s)
}

fn require_positive_js_safe_u64(
    v: &Value,
    field: &'static str,
) -> Result<u64, DirectSessionBindingError> {
    let n = require_js_safe_u64(v, field)?;
    if n == 0 {
        return Err(DirectSessionBindingError::UnsafeInteger { field });
    }
    Ok(n)
}

fn require_js_safe_u64(v: &Value, field: &'static str) -> Result<u64, DirectSessionBindingError> {
    match v {
        Value::Number(n) if n.is_u64() => {
            let u = n.as_u64().unwrap();
            if u > JS_MAX_SAFE_INTEGER {
                return Err(DirectSessionBindingError::UnsafeInteger { field });
            }
            Ok(u)
        }
        Value::Number(n) if n.is_i64() => {
            let i = n.as_i64().unwrap();
            if i < 0 {
                return Err(DirectSessionBindingError::UnsafeInteger { field });
            }
            let u = i as u64;
            if u > JS_MAX_SAFE_INTEGER {
                return Err(DirectSessionBindingError::UnsafeInteger { field });
            }
            Ok(u)
        }
        _ => Err(DirectSessionBindingError::UnsafeInteger { field }),
    }
}

fn require_capability_token(v: &Value) -> Result<String, DirectSessionBindingError> {
    let s = match v {
        Value::String(s) => s.as_str(),
        _ => return Err(DirectSessionBindingError::MalformedCapabilityToken),
    };
    if s.len() != CAPABILITY_TOKEN_LEN {
        return Err(DirectSessionBindingError::MalformedCapabilityToken);
    }
    if !s
        .bytes()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'))
    {
        return Err(DirectSessionBindingError::MalformedCapabilityToken);
    }
    use base64::Engine;
    if base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s)
        .is_err()
    {
        return Err(DirectSessionBindingError::MalformedCapabilityToken);
    }
    Ok(s.to_string())
}

fn require_wss_url(v: &Value) -> Result<String, DirectSessionBindingError> {
    let s = require_non_empty_string(v, "wssUrl")?;
    if !validate_public_wss_url(&s) {
        return Err(DirectSessionBindingError::InvalidWssUrl);
    }
    Ok(s)
}

fn validate_public_wss_url(url_str: &str) -> bool {
    if !url_str.starts_with("wss://") {
        return false;
    }
    if url_str.contains('#') {
        return false;
    }
    let authority = url_str
        .strip_prefix("wss://")
        .unwrap_or("")
        .split('/')
        .next()
        .unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return false;
    }
    let url = match Url::parse(url_str) {
        Ok(u) => u,
        Err(_) => return false,
    };
    if url.scheme() != "wss" {
        return false;
    }
    if !url.username().is_empty() || url.password().is_some() {
        return false;
    }
    if url.fragment().is_some() || url.query().is_some() {
        return false;
    }
    let host = match url.host() {
        Some(h) => h,
        None => return false,
    };
    match host {
        Host::Domain(domain) => {
            if domain.is_empty() || domain.eq_ignore_ascii_case("localhost") {
                return false;
            }
            if let Ok(ip) = domain.parse::<IpAddr>() {
                return is_public_ip(ip);
            }
            true
        }
        Host::Ipv4(ip) => is_public_ip(IpAddr::V4(ip)),
        Host::Ipv6(ip) => is_public_ip(IpAddr::V6(ip)),
    }
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_loopback() && !v4.is_private() && !v4.is_link_local() && !v4.is_unspecified()
        }
        IpAddr::V6(v6) => !v6.is_loopback() && !v6.is_unspecified() && !v6.is_unique_local(),
    }
}
