//! Strict Syndicate voice-delivery v2 finalized manifest parsing and API digest.

use crate::voice_delivery::bounds::STEREO_PCM_FRAME_BYTES;
use crate::voice_delivery::fs::{PortableParentComponent, ValidatedFinalName};
use crate::voice_delivery::hash::hex_digest;
use crate::voice_delivery::identity::{DeliveryImmutableIdentity, IdentityError};
use crate::voice_delivery::ids::validate_stage_basename;
use crate::voice_delivery::manifest::{StemManifestEntry, ValidatedManifest};
use crate::voice_delivery::wav::minimal_wav_header;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const FINALIZED_MANIFEST_VERSION: u32 = 1;
pub const JOURNAL_FORMAT_VERSION_V2: u32 = 2;
pub const WAV_HEADER_BYTES: u64 = 44;
pub const DISCORD_SNOWFLAKE_MIN: u128 = 4_194_304;
pub const DISCORD_SNOWFLAKE_MAX: u128 = 9_223_372_036_854_775_807;
pub const STEM_ID_MAX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyndicateFinalizedStem {
    pub discord_user_id: String,
    pub path_nick: String,
    pub wav_relative_path: String,
    pub wav_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyndicateFinalizedManifest {
    pub version: u32,
    pub session_id: String,
    pub guild_id: String,
    pub channel_id: String,
    pub session_start_wall_ms: i64,
    pub session_start_monotonic_ns: String,
    pub sealed_stop_wall_ms: i64,
    pub sealed_stop_monotonic_ns: String,
    pub target_sample_count_48k: u64,
    pub journal_format_version: u32,
    pub journal_sha256: String,
    pub stems: Vec<SyndicateFinalizedStem>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FinalizedManifestError {
    #[error("invalid finalized manifest: {0}")]
    Invalid(String),
}

fn fail(msg: impl Into<String>) -> FinalizedManifestError {
    FinalizedManifestError::Invalid(msg.into())
}

fn is_plain_object(v: &Value) -> bool {
    matches!(v, Value::Object(_))
}

fn reject_unknown_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<(), FinalizedManifestError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(fail(format!("unknown key: {key}")));
        }
    }
    Ok(())
}

fn require_string(v: &Value, field: &str) -> Result<String, FinalizedManifestError> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        _ => Err(fail(format!("{field} must be non-empty string"))),
    }
}

fn require_i64(v: &Value, field: &str) -> Result<i64, FinalizedManifestError> {
    match v {
        Value::Number(n) if n.is_i64() => {
            n.as_i64().ok_or_else(|| fail(format!("{field} invalid")))
        }
        _ => Err(fail(format!("{field} must be safe integer"))),
    }
}

fn require_u64_field(v: &Value, field: &str) -> Result<u64, FinalizedManifestError> {
    match v {
        Value::Number(n) if n.is_u64() => {
            n.as_u64().ok_or_else(|| fail(format!("{field} invalid")))
        }
        Value::Number(n) if n.is_i64() && n.as_i64().unwrap_or(0) >= 0 => {
            Ok(n.as_i64().unwrap() as u64)
        }
        _ => Err(fail(format!("{field} must be non-negative integer"))),
    }
}

fn require_bigint_string(v: &Value, field: &str) -> Result<String, FinalizedManifestError> {
    let s = require_string(v, field)?;
    if s.parse::<u128>().is_err() {
        return Err(fail(format!("{field} not valid bigint string")));
    }
    Ok(s)
}

fn validate_sha256_hex(s: &str, field: &str) -> Result<String, FinalizedManifestError> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(fail(format!("{field} must be 64 lowercase hex sha256")));
    }
    Ok(s.to_string())
}

fn validate_discord_snowflake(v: &str, field: &str) -> Result<String, FinalizedManifestError> {
    if !v.bytes().all(|b| b.is_ascii_digit()) {
        return Err(fail(format!("{field} must be numeric snowflake")));
    }
    let as_big = v
        .parse::<u128>()
        .map_err(|_| fail(format!("{field} snowflake out of Discord bounds")))?;
    if !(DISCORD_SNOWFLAKE_MIN..=DISCORD_SNOWFLAKE_MAX).contains(&as_big) {
        return Err(fail(format!("{field} snowflake out of Discord bounds")));
    }
    Ok(v.to_string())
}

pub fn is_valid_stem_id(stem_id: &str) -> bool {
    if stem_id.is_empty() || stem_id.len() > STEM_ID_MAX_LEN {
        return false;
    }
    let bytes = stem_id.as_bytes();
    if bytes.len() == 1 {
        return bytes[0].is_ascii_alphanumeric();
    }
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    if !first.is_ascii_alphanumeric() || !last.is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'.' || *b == b'_' || *b == b'-')
}

fn validate_path_nick(path_nick: &str, field: &str) -> Result<String, FinalizedManifestError> {
    if !is_valid_stem_id(path_nick) {
        return Err(fail(format!(
            "{field} pathNick is not portable filename safe"
        )));
    }
    if path_nick.contains("..") || path_nick.contains('/') || path_nick.contains('\\') {
        return Err(fail(format!(
            "{field} pathNick must not contain path separators"
        )));
    }
    Ok(path_nick.to_string())
}

pub fn expected_wav_file_bytes(target_sample_count: u64) -> u64 {
    WAV_HEADER_BYTES + target_sample_count * STEREO_PCM_FRAME_BYTES
}

fn compare_ascii(a: &str, b: &str) -> std::cmp::Ordering {
    let len = a.len().min(b.len());
    for i in 0..len {
        let ca = a.as_bytes()[i];
        let cb = b.as_bytes()[i];
        match ca.cmp(&cb) {
            std::cmp::Ordering::Equal => {}
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// Canonical JSON for manifest identity (stable key order, stems sorted by pathNick ASCII).
pub fn canonicalize_finalized_manifest_for_digest(manifest: &SyndicateFinalizedManifest) -> String {
    let mut stems: Vec<_> = manifest
        .stems
        .iter()
        .map(|s| {
            serde_json::json!({
                "discordUserId": s.discord_user_id,
                "pathNick": s.path_nick,
                "sha256": s.sha256,
                "wavBytes": s.wav_bytes,
                "wavRelativePath": s.wav_relative_path,
            })
        })
        .collect();
    stems.sort_by(|a, b| {
        compare_ascii(
            a.get("pathNick").and_then(|v| v.as_str()).unwrap_or(""),
            b.get("pathNick").and_then(|v| v.as_str()).unwrap_or(""),
        )
    });
    let payload = serde_json::json!({
        "channelId": manifest.channel_id,
        "guildId": manifest.guild_id,
        "journalFormatVersion": manifest.journal_format_version,
        "journalSha256": manifest.journal_sha256,
        "sealedStopMonotonicNs": manifest.sealed_stop_monotonic_ns,
        "sealedStopWallMs": manifest.sealed_stop_wall_ms,
        "sessionId": manifest.session_id,
        "sessionStartMonotonicNs": manifest.session_start_monotonic_ns,
        "sessionStartWallMs": manifest.session_start_wall_ms,
        "stems": stems,
        "targetSampleCount48k": manifest.target_sample_count_48k,
        "version": manifest.version,
    });
    serde_json::to_string(&payload).expect("manifest json")
}

pub fn compute_finalized_manifest_digest(manifest: &SyndicateFinalizedManifest) -> String {
    let canonical = canonicalize_finalized_manifest_for_digest(manifest);
    hex_digest(&Sha256::digest(canonical.as_bytes()))
}

pub fn parse_syndicate_finalized_manifest(
    raw: &Value,
) -> Result<SyndicateFinalizedManifest, FinalizedManifestError> {
    if !is_plain_object(raw) {
        return Err(fail("finalized manifest must be object"));
    }
    let obj = raw.as_object().expect("checked");
    reject_unknown_keys(
        obj,
        &[
            "version",
            "sessionId",
            "guildId",
            "channelId",
            "sessionStartWallMs",
            "sessionStartMonotonicNs",
            "sealedStopWallMs",
            "sealedStopMonotonicNs",
            "targetSampleCount48k",
            "journalFormatVersion",
            "journalSha256",
            "stems",
        ],
    )?;
    let version = require_u64_field(&obj["version"], "finalized manifest version")?;
    if version != FINALIZED_MANIFEST_VERSION as u64 {
        return Err(fail("unsupported finalized manifest version"));
    }
    let stems_raw = obj
        .get("stems")
        .ok_or_else(|| fail("finalized manifest stems must be non-empty array"))?;
    let stems_arr = match stems_raw {
        Value::Array(a) if !a.is_empty() => a,
        _ => return Err(fail("finalized manifest stems must be non-empty array")),
    };
    let mut stems = Vec::with_capacity(stems_arr.len());
    let mut path_nicks = std::collections::HashSet::new();
    let mut user_ids = std::collections::HashSet::new();
    for (i, stem) in stems_arr.iter().enumerate() {
        if !is_plain_object(stem) {
            return Err(fail(format!(
                "finalized manifest stems[{i}] must be object"
            )));
        }
        let stem_obj = stem.as_object().expect("checked");
        reject_unknown_keys(
            stem_obj,
            &[
                "discordUserId",
                "pathNick",
                "wavRelativePath",
                "wavBytes",
                "sha256",
            ],
        )?;
        let label = format!("finalized manifest stems[{i}]");
        let discord_user_id = validate_discord_snowflake(
            &require_string(
                &stem_obj["discordUserId"],
                &format!("{label}.discordUserId"),
            )?,
            &format!("{label}.discordUserId"),
        )?;
        let path_nick = validate_path_nick(
            &require_string(&stem_obj["pathNick"], &format!("{label}.pathNick"))?,
            &format!("{label}.pathNick"),
        )?;
        let wav_relative_path = require_string(
            &stem_obj["wavRelativePath"],
            &format!("{label}.wavRelativePath"),
        )?;
        let wav_bytes = require_u64_field(&stem_obj["wavBytes"], &format!("{label}.wavBytes"))?;
        let sha256 = validate_sha256_hex(
            &require_string(&stem_obj["sha256"], &format!("{label}.sha256"))?,
            &format!("{label}.sha256"),
        )?;
        if wav_bytes == 0 {
            return Err(fail(format!("{label}.wavBytes invalid")));
        }
        if wav_relative_path != format!("{path_nick}.wav") {
            return Err(fail(format!(
                "{label} wavRelativePath must be {{pathNick}}.wav"
            )));
        }
        if path_nicks.contains(&path_nick) || user_ids.contains(&discord_user_id) {
            return Err(fail(format!(
                "finalized manifest duplicate stem at index {i}"
            )));
        }
        path_nicks.insert(path_nick.clone());
        user_ids.insert(discord_user_id.clone());
        stems.push(SyndicateFinalizedStem {
            discord_user_id,
            path_nick,
            wav_relative_path,
            wav_bytes,
            sha256,
        });
    }

    let session_id = require_string(&obj["sessionId"], "finalized manifest sessionId")?;
    if session_id != session_id.to_lowercase() {
        return Err(fail("finalized manifest sessionId must be lowercase UUID"));
    }
    if Uuid::parse_str(&session_id).is_err() {
        return Err(fail("finalized manifest sessionId must be UUID"));
    }
    let target_sample_count_48k = require_u64_field(
        &obj["targetSampleCount48k"],
        "finalized manifest targetSampleCount48k",
    )?;
    let journal_format_version = require_u64_field(
        &obj["journalFormatVersion"],
        "finalized manifest journalFormatVersion",
    )?;
    if journal_format_version != JOURNAL_FORMAT_VERSION_V2 as u64 {
        return Err(fail(
            "finalized manifest journalFormatVersion must match v2 journal",
        ));
    }
    let expected_stem_bytes = expected_wav_file_bytes(target_sample_count_48k);
    for (i, stem) in stems.iter().enumerate() {
        if stem.wav_bytes != expected_stem_bytes {
            return Err(fail(format!(
                "finalized manifest stems[{i}].wavBytes mismatch target sample count"
            )));
        }
        minimal_wav_header(stem.wav_bytes - WAV_HEADER_BYTES).map_err(|e| fail(e.to_string()))?;
    }

    Ok(SyndicateFinalizedManifest {
        version: FINALIZED_MANIFEST_VERSION,
        session_id,
        guild_id: validate_discord_snowflake(
            &require_string(&obj["guildId"], "finalized manifest guildId")?,
            "finalized manifest guildId",
        )?,
        channel_id: validate_discord_snowflake(
            &require_string(&obj["channelId"], "finalized manifest channelId")?,
            "finalized manifest channelId",
        )?,
        session_start_wall_ms: require_i64(
            &obj["sessionStartWallMs"],
            "finalized manifest sessionStartWallMs",
        )?,
        session_start_monotonic_ns: require_bigint_string(
            &obj["sessionStartMonotonicNs"],
            "finalized manifest sessionStartMonotonicNs",
        )?,
        sealed_stop_wall_ms: require_i64(
            &obj["sealedStopWallMs"],
            "finalized manifest sealedStopWallMs",
        )?,
        sealed_stop_monotonic_ns: require_bigint_string(
            &obj["sealedStopMonotonicNs"],
            "finalized manifest sealedStopMonotonicNs",
        )?,
        target_sample_count_48k,
        journal_format_version: journal_format_version as u32,
        journal_sha256: validate_sha256_hex(
            &require_string(&obj["journalSha256"], "finalized manifest journalSha256")?,
            "finalized manifest journalSha256",
        )?,
        stems,
    })
}

pub fn syndicate_to_validated_manifest(
    manifest: &SyndicateFinalizedManifest,
) -> Result<ValidatedManifest, FinalizedManifestError> {
    let stems = manifest
        .stems
        .iter()
        .map(|s| StemManifestEntry {
            file_name: format!("{}.wav", s.path_nick),
            byte_count: s.wav_bytes,
            sha256: s.sha256.clone(),
        })
        .collect();
    ValidatedManifest::validate(stems).map_err(|e| fail(e.to_string()))
}

/// Deterministic publication layout from trusted API manifest metadata.
pub fn delivery_layout_from_manifest(
    manifest: &SyndicateFinalizedManifest,
) -> Result<(Vec<PortableParentComponent>, String, String), FinalizedManifestError> {
    let parent_root = PortableParentComponent::validate("syndicate-discord-voice")
        .map_err(|e| fail(e.to_string()))?;
    let guild =
        PortableParentComponent::validate(&manifest.guild_id).map_err(|e| fail(e.to_string()))?;
    let channel_slug = channel_slug_for_final_path(&manifest.channel_id);
    let session_name = format!(
        "{}-{}-{}",
        manifest.sealed_stop_wall_ms, channel_slug, manifest.session_id
    );
    let final_session =
        ValidatedFinalName::validate(&session_name).map_err(|e| fail(e.to_string()))?;
    let staging = stable_stage_token_for_session(&manifest.session_id)?;
    Ok((
        vec![parent_root, guild],
        staging,
        final_session.as_str().to_string(),
    ))
}

fn channel_slug_for_final_path(channel_id: &str) -> String {
    let digest = hex_digest(&Sha256::digest(channel_id.as_bytes()));
    digest[..8].to_string()
}

fn stable_stage_token_for_session(session_id: &str) -> Result<String, FinalizedManifestError> {
    let digest = hex_digest(&Sha256::digest(session_id.as_bytes()));
    let token = format!(".streamsync-stage-{}", &digest[..32]);
    validate_stage_basename(&token).map_err(|e| fail(e.to_string()))?;
    Ok(token)
}

pub fn bind_delivery_identity(
    manifest: &SyndicateFinalizedManifest,
    expected_digest: &str,
) -> Result<(ValidatedManifest, DeliveryImmutableIdentity), FinalizedManifestError> {
    let computed = compute_finalized_manifest_digest(manifest);
    if computed != expected_digest {
        return Err(fail("manifest digest mismatch"));
    }
    let validated = syndicate_to_validated_manifest(manifest)?;
    let (parent, staging, final_name) = delivery_layout_from_manifest(manifest)?;
    let identity = DeliveryImmutableIdentity::new_bound(
        manifest.session_id.clone(),
        computed,
        &validated,
        staging,
        parent,
        &final_name,
    )
    .map_err(|e: IdentityError| fail(e.to_string()))?;
    Ok((validated, identity))
}

#[cfg(test)]
pub(crate) mod syndicate_manifest_tests {
    use super::*;
    use crate::voice_delivery::hash::hex_digest;
    use sha2::{Digest, Sha256};

    pub(crate) fn minimal_wav_value() -> Value {
        let header_len = 44u64;
        let mut wav = vec![0u8; header_len as usize];
        wav[0..4].copy_from_slice(b"RIFF");
        wav[8..12].copy_from_slice(b"WAVE");
        wav[12..16].copy_from_slice(b"fmt ");
        wav[36..40].copy_from_slice(b"data");
        let sha = hex_digest(&Sha256::digest(&wav));
        serde_json::json!({
            "version": 1,
            "sessionId": "550e8400-e29b-41d4-a716-446655440000",
            "guildId": "123456789012345678",
            "channelId": "234567890123456789",
            "sessionStartWallMs": 1,
            "sessionStartMonotonicNs": "1",
            "sealedStopWallMs": 2,
            "sealedStopMonotonicNs": "2",
            "targetSampleCount48k": 0,
            "journalFormatVersion": 2,
            "journalSha256": "a".repeat(64),
            "stems": [{
                "discordUserId": "123456789012345678",
                "pathNick": "user1",
                "wavRelativePath": "user1.wav",
                "wavBytes": header_len,
                "sha256": sha,
            }],
        })
    }

    #[test]
    fn parse_minimal_manifest_and_digest_stable() {
        let raw = minimal_wav_value();
        let m1 = parse_syndicate_finalized_manifest(&raw).unwrap();
        let d1 = compute_finalized_manifest_digest(&m1);
        let m2 = parse_syndicate_finalized_manifest(&raw).unwrap();
        assert_eq!(d1, compute_finalized_manifest_digest(&m2));
        bind_delivery_identity(&m1, &d1).unwrap();
    }

    #[test]
    fn rejects_unknown_keys_and_traversal_path_nick() {
        let raw = minimal_wav_value();
        let mut obj = raw.as_object().unwrap().clone();
        obj.insert("extra".into(), Value::Null);
        assert!(parse_syndicate_finalized_manifest(&Value::Object(obj)).is_err());

        let mut raw2 = minimal_wav_value();
        raw2["stems"][0]["pathNick"] = serde_json::json!("../evil");
        assert!(parse_syndicate_finalized_manifest(&raw2).is_err());
    }

    #[test]
    fn rejects_uppercase_session_uuid() {
        let mut raw = minimal_wav_value();
        raw["sessionId"] = serde_json::json!("550E8400-E29B-41D4-A716-446655440000");
        assert!(parse_syndicate_finalized_manifest(&raw).is_err());
    }

    #[test]
    fn final_session_dir_includes_full_session_uuid() {
        let raw = minimal_wav_value();
        let m = parse_syndicate_finalized_manifest(&raw).unwrap();
        let (_, _, final_name) = delivery_layout_from_manifest(&m).unwrap();
        assert!(final_name.contains(&m.session_id));
        assert_ne!(
            final_name,
            format!("{}-{}", m.sealed_stop_wall_ms, &m.session_id[..8])
        );
    }

    #[test]
    fn same_timestamp_prefix_uuid_different_suffix_yields_distinct_final_dirs() {
        let mut raw_a = minimal_wav_value();
        raw_a["sessionId"] = serde_json::json!("550e8400-e29b-41d4-a716-446655440000");
        let mut raw_b = minimal_wav_value();
        raw_b["sessionId"] = serde_json::json!("550e8400-e29b-41d4-a716-446655440001");
        let ma = parse_syndicate_finalized_manifest(&raw_a).unwrap();
        let mb = parse_syndicate_finalized_manifest(&raw_b).unwrap();
        assert_eq!(ma.sealed_stop_wall_ms, mb.sealed_stop_wall_ms);
        let (_, _, fa) = delivery_layout_from_manifest(&ma).unwrap();
        let (_, _, fb) = delivery_layout_from_manifest(&mb).unwrap();
        assert_ne!(fa, fb);
    }

    #[test]
    fn cross_language_golden_manifest_digest() {
        let manifest = serde_json::json!({
            "version": 1,
            "sessionId": "550e8400-e29b-41d4-a716-446655440000",
            "guildId": "123456789012345678",
            "channelId": "234567890123456789",
            "sessionStartWallMs": 1,
            "sessionStartMonotonicNs": "100",
            "sealedStopWallMs": 2,
            "sealedStopMonotonicNs": "200",
            "targetSampleCount48k": 0,
            "journalFormatVersion": 2,
            "journalSha256": "a".repeat(64),
            "stems": [{
                "discordUserId": "123456789012345678",
                "pathNick": "user1",
                "wavRelativePath": "user1.wav",
                "wavBytes": 44,
                "sha256": "b".repeat(64),
            }],
        });
        let parsed = parse_syndicate_finalized_manifest(&manifest).unwrap();
        assert_eq!(
            compute_finalized_manifest_digest(&parsed),
            "96881d02814a25a59b548b0ef30a4da3c0109f6371bc4b6ee23382447485168e"
        );
    }

    #[test]
    fn stem_id_pattern_matches_syndicate() {
        assert!(is_valid_stem_id("user1"));
        assert!(is_valid_stem_id("a"));
        assert!(!is_valid_stem_id(""));
        assert!(!is_valid_stem_id(".hidden"));
    }
}
