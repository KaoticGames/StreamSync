//! Strict local-v1 StreamSync cloud contract parsers (fail closed).

use super::manifest::{
    compute_local_finalization_manifest_digest, parse_local_finalization_manifest,
    LocalFinalizationManifest,
};
use crate::voice_delivery::client::VoiceV2ClientError;
use serde_json::Value;

const SHA256_HEX_LEN: usize = 64;

pub const LEASE_GENERATION_HEADER: &str = "X-Voice-Lease-Generation";
pub const LEASE_TOKEN_HEADER: &str = "X-Voice-Lease-Token";

fn is_plain_object(v: &Value) -> bool {
    v.is_object()
}

fn reject_unknown_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
    label: &str,
) -> Result<(), VoiceV2ClientError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(VoiceV2ClientError::Parse(format!(
                "{label} unknown key: {key}"
            )));
        }
    }
    Ok(())
}

fn parse_manifest_digest_field(raw: &Value) -> Result<String, VoiceV2ClientError> {
    let s = raw
        .as_str()
        .ok_or_else(|| VoiceV2ClientError::Parse("manifestDigest invalid".into()))?;
    if s.len() != SHA256_HEX_LEN || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(VoiceV2ClientError::Parse("manifestDigest invalid".into()));
    }
    let lower = s.to_ascii_lowercase();
    if lower != s {
        return Err(VoiceV2ClientError::Parse(
            "manifestDigest must be lowercase".into(),
        ));
    }
    Ok(lower)
}

fn parse_session_id_field(raw: &Value) -> Result<String, VoiceV2ClientError> {
    let s = raw
        .as_str()
        .ok_or_else(|| VoiceV2ClientError::Parse("sessionId invalid".into()))?;
    let t = s.trim();
    if t.is_empty() || t.len() > 128 {
        return Err(VoiceV2ClientError::Parse("sessionId invalid".into()));
    }
    Ok(t.to_string())
}

fn parse_sealed_at_ms_field(raw: &Value) -> Result<i64, VoiceV2ClientError> {
    let n = raw
        .as_i64()
        .ok_or_else(|| VoiceV2ClientError::Parse("sealedAtMs invalid".into()))?;
    if n < 0 {
        return Err(VoiceV2ClientError::Parse("sealedAtMs invalid".into()));
    }
    Ok(n)
}

fn parse_safe_positive_int(raw: &Value, label: &str) -> Result<i64, VoiceV2ClientError> {
    let n = raw
        .as_i64()
        .ok_or_else(|| VoiceV2ClientError::Parse(format!("{label} invalid")))?;
    if n < 1 {
        return Err(VoiceV2ClientError::Parse(format!("{label} invalid")));
    }
    Ok(n)
}

fn parse_lease_token_field(raw: &Value) -> Result<String, VoiceV2ClientError> {
    let s = raw
        .as_str()
        .ok_or_else(|| VoiceV2ClientError::Parse("leaseToken invalid".into()))?;
    if s.len() < 16 || s.len() > 256 {
        return Err(VoiceV2ClientError::Parse("leaseToken invalid".into()));
    }
    Ok(s.to_string())
}

fn parse_device_id_field(raw: &Value) -> Result<String, VoiceV2ClientError> {
    let s = raw
        .as_str()
        .ok_or_else(|| VoiceV2ClientError::Parse("deviceId invalid".into()))?;
    if s.is_empty() || s.len() > 128 {
        return Err(VoiceV2ClientError::Parse("deviceId invalid".into()));
    }
    Ok(s.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalPendingItem {
    pub session_id: String,
    pub manifest_digest: String,
    pub sealed_at_ms: i64,
}

pub fn parse_local_pending_item(raw: &Value) -> Result<LocalPendingItem, VoiceV2ClientError> {
    let obj = raw
        .as_object()
        .ok_or_else(|| VoiceV2ClientError::Parse("pending item must be object".into()))?;
    reject_unknown_keys(
        obj,
        &["sessionId", "manifestDigest", "sealedAtMs"],
        "pending item",
    )?;
    Ok(LocalPendingItem {
        session_id: parse_session_id_field(obj.get("sessionId").unwrap_or(&Value::Null))?,
        manifest_digest: parse_manifest_digest_field(
            obj.get("manifestDigest").unwrap_or(&Value::Null),
        )?,
        sealed_at_ms: parse_sealed_at_ms_field(obj.get("sealedAtMs").unwrap_or(&Value::Null))?,
    })
}

pub fn parse_local_pending_response(
    body: &Value,
) -> Result<Vec<LocalPendingItem>, VoiceV2ClientError> {
    if !is_plain_object(body) {
        return Err(VoiceV2ClientError::Parse(
            "pending response must be object".into(),
        ));
    }
    let obj = body.as_object().unwrap();
    reject_unknown_keys(obj, &["ok", "deliveries"], "pending response")?;
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(VoiceV2ClientError::Parse(
            "pending response ok must be true".into(),
        ));
    }
    let deliveries = body
        .get("deliveries")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            VoiceV2ClientError::Parse("pending response deliveries must be array".into())
        })?;
    let mut out = Vec::with_capacity(deliveries.len());
    for (i, item) in deliveries.iter().enumerate() {
        let parsed = parse_local_pending_item(item).map_err(|e| {
            VoiceV2ClientError::Parse(format!("pending response deliveries[{i}]: {e}"))
        })?;
        out.push(parsed);
    }
    out.sort_by(|a, b| {
        a.sealed_at_ms
            .cmp(&b.sealed_at_ms)
            .then(a.session_id.cmp(&b.session_id))
    });
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct LeaseGrant {
    pub session_id: String,
    pub manifest_digest: String,
    pub generation: i64,
    pub token: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone)]
pub struct LeaseProof {
    pub generation: i64,
    pub token: String,
}

impl LeaseProof {
    pub fn generation_header_value(&self) -> String {
        self.generation.to_string()
    }
}

impl LeaseGrant {
    pub fn proof(&self) -> LeaseProof {
        LeaseProof {
            generation: self.generation,
            token: self.token.clone(),
        }
    }
}

pub fn parse_local_claim_response(body: &Value) -> Result<LeaseGrant, VoiceV2ClientError> {
    if !is_plain_object(body) {
        return Err(VoiceV2ClientError::Parse(
            "claim response must be object".into(),
        ));
    }
    let obj = body.as_object().unwrap();
    reject_unknown_keys(
        obj,
        &[
            "ok",
            "sessionId",
            "manifestDigest",
            "leaseGeneration",
            "leaseToken",
            "leaseExpiresAtMs",
        ],
        "claim response",
    )?;
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(VoiceV2ClientError::Parse(
            "claim response ok must be true".into(),
        ));
    }
    let expires = body
        .get("leaseExpiresAtMs")
        .ok_or_else(|| VoiceV2ClientError::Parse("leaseExpiresAtMs invalid".into()))?;
    let expires_at_ms = expires
        .as_i64()
        .filter(|n| *n >= 0)
        .ok_or_else(|| VoiceV2ClientError::Parse("leaseExpiresAtMs invalid".into()))?;
    Ok(LeaseGrant {
        session_id: parse_session_id_field(body.get("sessionId").unwrap_or(&Value::Null))?,
        manifest_digest: parse_manifest_digest_field(
            body.get("manifestDigest").unwrap_or(&Value::Null),
        )?,
        generation: parse_safe_positive_int(
            body.get("leaseGeneration").unwrap_or(&Value::Null),
            "leaseGeneration",
        )?,
        token: parse_lease_token_field(body.get("leaseToken").unwrap_or(&Value::Null))?,
        expires_at_ms,
    })
}

pub fn parse_lease_renew_release_body(
    body: &Value,
) -> Result<(String, LeaseProof), VoiceV2ClientError> {
    if !is_plain_object(body) {
        return Err(VoiceV2ClientError::Parse("body must be object".into()));
    }
    let obj = body.as_object().unwrap();
    reject_unknown_keys(
        obj,
        &["deviceId", "leaseGeneration", "leaseToken"],
        "renew request",
    )?;
    let device_id = parse_device_id_field(body.get("deviceId").unwrap_or(&Value::Null))?;
    let proof = LeaseProof {
        generation: parse_safe_positive_int(
            body.get("leaseGeneration").unwrap_or(&Value::Null),
            "leaseGeneration",
        )?,
        token: parse_lease_token_field(body.get("leaseToken").unwrap_or(&Value::Null))?,
    };
    Ok((device_id, proof))
}

pub fn parse_local_manifest_response(
    body: &Value,
) -> Result<(LocalFinalizationManifest, String), VoiceV2ClientError> {
    if !is_plain_object(body) {
        return Err(VoiceV2ClientError::Parse(
            "manifest response must be object".into(),
        ));
    }
    let obj = body.as_object().unwrap();
    reject_unknown_keys(
        obj,
        &["ok", "manifest", "manifestDigest"],
        "manifest response",
    )?;
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(VoiceV2ClientError::Parse(
            "manifest response ok must be true".into(),
        ));
    }
    let manifest_raw = body
        .get("manifest")
        .ok_or_else(|| VoiceV2ClientError::Parse("manifest missing".into()))?;
    let manifest = parse_local_finalization_manifest(manifest_raw)
        .map_err(|e| VoiceV2ClientError::Parse(e.to_string()))?;
    let manifest_digest =
        parse_manifest_digest_field(body.get("manifestDigest").unwrap_or(&Value::Null))?;
    let computed = compute_local_finalization_manifest_digest(&manifest);
    if computed != manifest_digest {
        return Err(VoiceV2ClientError::IdentityConflict);
    }
    Ok((manifest, manifest_digest))
}

pub fn sealed_at_rfc3339_from_ms(sealed_at_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(sealed_at_ms)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| sealed_at_ms.to_string())
}
