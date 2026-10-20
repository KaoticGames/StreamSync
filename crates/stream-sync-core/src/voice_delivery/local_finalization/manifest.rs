//! Local finalization manifest v1 — cross-language contract (mirrors ChatBot protocol).

use crate::voice_delivery::finalized_manifest::{
    compare_ascii_for_digest, expected_wav_file_bytes, is_valid_stem_id,
    validate_discord_snowflake, validate_sha256_hex, SyndicateFinalizedManifest,
    SyndicateFinalizedStem, SyndicateRecoveryReport, FINALIZED_MANIFEST_VERSION,
    JOURNAL_FORMAT_VERSION_V2,
};
use crate::voice_delivery::hash::hex_digest;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const LOCAL_FINALIZATION_MANIFEST_VERSION: u32 = 1;
pub const LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION: &str =
    "syndicate-local-opus-finalizer-v1";
pub const LOCAL_FINALIZATION_WAV_FORMAT: &str = "pcm_s16le_stereo_48000";
const VOICE_DELIVERY_V2_R2_PREFIX: &str = "voice-delivery-v2";
const GUILD_ID_PATTERN_MIN: usize = 17;
const GUILD_ID_PATTERN_MAX: usize = 20;
const OPAQUE_STEM_ID_HEX_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalFinalizationObjectRef {
    pub key: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalFinalizationSegment {
    pub segment_seq: u32,
    pub key: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalFinalizationUser {
    pub discord_user_id: String,
    pub path_nick: String,
    pub opaque_stem_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalFinalizationOutputPolicy {
    pub wav_format: String,
    pub recovery_report_relative_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalFinalizationManifest {
    pub version: u32,
    pub finalizer_algorithm_version: String,
    pub session_id: String,
    pub guild_id: String,
    pub channel_id: String,
    pub session_start_wall_ms: i64,
    pub session_start_monotonic_ns: String,
    pub sealed_stop_wall_ms: i64,
    pub sealed_stop_monotonic_ns: String,
    pub target_sample_count_48k: u64,
    pub journal_sha256: String,
    pub recording_manifest: LocalFinalizationObjectRef,
    pub sealed_index: LocalFinalizationObjectRef,
    pub segments: Vec<LocalFinalizationSegment>,
    pub users: Vec<LocalFinalizationUser>,
    pub output: LocalFinalizationOutputPolicy,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocalFinalizationManifestError {
    #[error("invalid local finalization manifest: {0}")]
    Invalid(String),
}

fn fail(msg: impl Into<String>) -> LocalFinalizationManifestError {
    LocalFinalizationManifestError::Invalid(msg.into())
}

fn is_plain_object(v: &Value) -> bool {
    matches!(v, Value::Object(_))
}

fn reject_unknown_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
    label: &str,
) -> Result<(), LocalFinalizationManifestError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(fail(format!("{label} unknown key: {key}")));
        }
    }
    Ok(())
}

fn require_string(v: &Value, field: &str) -> Result<String, LocalFinalizationManifestError> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        _ => Err(fail(format!("{field} must be non-empty string"))),
    }
}

fn require_i64(v: &Value, field: &str) -> Result<i64, LocalFinalizationManifestError> {
    match v {
        Value::Number(n) if n.is_i64() => {
            n.as_i64().ok_or_else(|| fail(format!("{field} invalid")))
        }
        _ => Err(fail(format!("{field} must be safe integer"))),
    }
}

fn require_u32_segment_seq(v: &Value, field: &str) -> Result<u32, LocalFinalizationManifestError> {
    let n = require_i64(v, field)?;
    if n < 0 || n > i64::from(u32::MAX) {
        return Err(fail(format!("{field} invalid")));
    }
    Ok(n as u32)
}

fn require_bigint_string(v: &Value, field: &str) -> Result<String, LocalFinalizationManifestError> {
    let s = require_string(v, field)?;
    if s.parse::<u128>().is_err() {
        return Err(fail(format!("{field} not valid bigint string")));
    }
    Ok(s)
}

fn validate_opaque_stem_id(
    v: &Value,
    field: &str,
) -> Result<String, LocalFinalizationManifestError> {
    let s = require_string(v, field)?;
    if s.len() != OPAQUE_STEM_ID_HEX_LEN
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(fail(format!("{field} must be 32 lowercase hex")));
    }
    Ok(s)
}

fn validate_path_nick(
    path_nick: &str,
    field: &str,
) -> Result<String, LocalFinalizationManifestError> {
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

pub fn derive_voice_v2_opaque_stem_id(
    session_id: &str,
    guild_id: &str,
    discord_user_id: &str,
    path_nick: &str,
) -> String {
    let payload = format!("voice-v2-stem:{session_id}:{guild_id}:{discord_user_id}:{path_nick}");
    let digest = hex_digest(&Sha256::digest(payload.as_bytes()));
    digest[..OPAQUE_STEM_ID_HEX_LEN].to_string()
}

fn session_prefix(guild_id: &str, session_id: &str) -> String {
    format!("{VOICE_DELIVERY_V2_R2_PREFIX}/{guild_id}/{session_id}")
}

fn assert_valid_r2_object_key(
    key: &str,
    guild_id: &str,
    session_id: &str,
    field: &str,
) -> Result<(), LocalFinalizationManifestError> {
    if key.contains("://") {
        return Err(fail(format!("{field} must not be URL")));
    }
    if key.contains("..") {
        return Err(fail(format!("{field} must not contain parent segments")));
    }
    let prefix = session_prefix(guild_id, session_id);
    if !key.starts_with(&format!("{prefix}/")) {
        return Err(fail(format!("{field} must be under session R2 prefix")));
    }
    Ok(())
}

fn expected_journal_segment_key(guild_id: &str, session_id: &str, segment_seq: u32) -> String {
    let padded = format!("{:08}", segment_seq);
    format!(
        "{}/journal/{}.synj",
        session_prefix(guild_id, session_id),
        padded
    )
}

fn expected_sealed_index_key(guild_id: &str, session_id: &str) -> String {
    format!(
        "{}/journal/index.sealed.json",
        session_prefix(guild_id, session_id)
    )
}

fn legacy_recording_manifest_key(guild_id: &str, session_id: &str) -> String {
    format!(
        "{}/manifest/recording.manifest.json",
        session_prefix(guild_id, session_id)
    )
}

fn recording_manifest_embedded_sha256_from_key<'a>(
    key: &'a str,
    guild_id: &str,
    session_id: &str,
) -> Option<&'a str> {
    let dir = format!(
        "{}/manifest/recording/",
        session_prefix(guild_id, session_id)
    );
    let rest = key.strip_prefix(&dir)?;
    let embedded = rest.strip_suffix(".manifest.json")?;
    if embedded.len() != 64
        || !embedded
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return None;
    }
    Some(embedded)
}

fn is_valid_recording_manifest_key(key: &str, guild_id: &str, session_id: &str) -> bool {
    if key == legacy_recording_manifest_key(guild_id, session_id) {
        return true;
    }
    recording_manifest_embedded_sha256_from_key(key, guild_id, session_id).is_some()
}

fn assert_recording_manifest_key_matches_sha256(
    key: &str,
    sha256: &str,
    guild_id: &str,
    session_id: &str,
) -> Result<(), LocalFinalizationManifestError> {
    if key == legacy_recording_manifest_key(guild_id, session_id) {
        return Ok(());
    }
    let embedded = recording_manifest_embedded_sha256_from_key(key, guild_id, session_id)
        .ok_or_else(|| fail("local finalization manifest recordingManifest.key invalid"))?;
    if embedded != sha256 {
        return Err(fail(
            "local finalization manifest recordingManifest.key sha256 mismatch",
        ));
    }
    Ok(())
}

fn parse_object_ref(
    raw: &Value,
    label: &str,
    guild_id: &str,
    session_id: &str,
    key_validator: impl Fn(&str) -> Result<(), LocalFinalizationManifestError>,
) -> Result<LocalFinalizationObjectRef, LocalFinalizationManifestError> {
    if !is_plain_object(raw) {
        return Err(fail(format!("{label} must be object")));
    }
    let obj = raw.as_object().expect("checked");
    reject_unknown_keys(obj, &["key", "byteLength", "sha256"], label)?;
    let key = require_string(&obj["key"], &format!("{label}.key"))?;
    assert_valid_r2_object_key(&key, guild_id, session_id, &format!("{label}.key"))?;
    key_validator(&key)?;
    let byte_length = require_i64(&obj["byteLength"], &format!("{label}.byteLength"))?;
    if byte_length <= 0 {
        return Err(fail(format!("{label}.byteLength invalid")));
    }
    let sha256 = validate_sha256_hex(
        &require_string(&obj["sha256"], &format!("{label}.sha256"))?,
        &format!("{label}.sha256"),
    )
    .map_err(|e| fail(e.to_string()))?;
    Ok(LocalFinalizationObjectRef {
        key,
        byte_length: byte_length as u64,
        sha256,
    })
}

pub fn parse_local_finalization_manifest(
    raw: &Value,
) -> Result<LocalFinalizationManifest, LocalFinalizationManifestError> {
    if !is_plain_object(raw) {
        return Err(fail("local finalization manifest must be object"));
    }
    let obj = raw.as_object().expect("checked");
    reject_unknown_keys(
        obj,
        &[
            "version",
            "finalizerAlgorithmVersion",
            "sessionId",
            "guildId",
            "channelId",
            "sessionStartWallMs",
            "sessionStartMonotonicNs",
            "sealedStopWallMs",
            "sealedStopMonotonicNs",
            "targetSampleCount48k",
            "journalSha256",
            "recordingManifest",
            "sealedIndex",
            "segments",
            "users",
            "output",
        ],
        "local finalization manifest",
    )?;
    let version = require_i64(&obj["version"], "local finalization manifest version")?;
    if version != i64::from(LOCAL_FINALIZATION_MANIFEST_VERSION) {
        return Err(fail("unsupported local finalization manifest version"));
    }
    let finalizer_algorithm_version = require_string(
        &obj["finalizerAlgorithmVersion"],
        "local finalization manifest finalizerAlgorithmVersion",
    )?;
    if finalizer_algorithm_version != LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION {
        return Err(fail(
            "local finalization manifest finalizerAlgorithmVersion invalid",
        ));
    }
    let session_id = require_string(&obj["sessionId"], "local finalization manifest sessionId")?;
    if session_id != session_id.to_lowercase() {
        return Err(fail(
            "local finalization manifest sessionId must be lowercase UUID",
        ));
    }
    if Uuid::parse_str(&session_id).is_err() {
        return Err(fail("local finalization manifest sessionId must be UUID"));
    }
    let guild_id = validate_discord_snowflake(
        &require_string(&obj["guildId"], "local finalization manifest guildId")?,
        "local finalization manifest guildId",
    )
    .map_err(|e| fail(e.to_string()))?;
    if guild_id.len() < GUILD_ID_PATTERN_MIN || guild_id.len() > GUILD_ID_PATTERN_MAX {
        return Err(fail("local finalization manifest guildId invalid length"));
    }
    let channel_id = validate_discord_snowflake(
        &require_string(&obj["channelId"], "local finalization manifest channelId")?,
        "local finalization manifest channelId",
    )
    .map_err(|e| fail(e.to_string()))?;

    let recording_manifest = parse_object_ref(
        &obj["recordingManifest"],
        "local finalization manifest recordingManifest",
        &guild_id,
        &session_id,
        |key| {
            if !is_valid_recording_manifest_key(key, &guild_id, &session_id) {
                Err(fail(
                    "local finalization manifest recordingManifest.key invalid",
                ))
            } else {
                Ok(())
            }
        },
    )?;
    assert_recording_manifest_key_matches_sha256(
        &recording_manifest.key,
        &recording_manifest.sha256,
        &guild_id,
        &session_id,
    )?;
    let sealed_index = parse_object_ref(
        &obj["sealedIndex"],
        "local finalization manifest sealedIndex",
        &guild_id,
        &session_id,
        |key| {
            if key != expected_sealed_index_key(&guild_id, &session_id) {
                Err(fail("local finalization manifest sealedIndex.key invalid"))
            } else {
                Ok(())
            }
        },
    )?;

    let segments_raw = obj
        .get("segments")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| fail("local finalization manifest segments must be non-empty array"))?;
    let mut segments = Vec::with_capacity(segments_raw.len());
    for (i, seg) in segments_raw.iter().enumerate() {
        if !is_plain_object(seg) {
            return Err(fail(format!(
                "local finalization manifest segments[{i}] must be object"
            )));
        }
        let seg_obj = seg.as_object().expect("checked");
        reject_unknown_keys(
            seg_obj,
            &["segmentSeq", "key", "byteLength", "sha256"],
            &format!("local finalization manifest segments[{i}]"),
        )?;
        let segment_seq = require_u32_segment_seq(
            &seg_obj["segmentSeq"],
            &format!("local finalization manifest segments[{i}].segmentSeq"),
        )?;
        if segment_seq != i as u32 {
            return Err(fail(format!(
                "local finalization manifest segments[{i}] segmentSeq gap or reorder"
            )));
        }
        let key = require_string(
            &seg_obj["key"],
            &format!("local finalization manifest segments[{i}].key"),
        )?;
        assert_valid_r2_object_key(
            &key,
            &guild_id,
            &session_id,
            &format!("local finalization manifest segments[{i}].key"),
        )?;
        if key != expected_journal_segment_key(&guild_id, &session_id, segment_seq) {
            return Err(fail(format!(
                "local finalization manifest segments[{i}].key invalid"
            )));
        }
        let byte_length = require_i64(
            &seg_obj["byteLength"],
            &format!("local finalization manifest segments[{i}].byteLength"),
        )?;
        if byte_length <= 0 {
            return Err(fail(format!(
                "local finalization manifest segments[{i}].byteLength invalid"
            )));
        }
        let sha256 = validate_sha256_hex(
            &require_string(
                &seg_obj["sha256"],
                &format!("local finalization manifest segments[{i}].sha256"),
            )?,
            &format!("local finalization manifest segments[{i}].sha256"),
        )
        .map_err(|e| fail(e.to_string()))?;
        segments.push(LocalFinalizationSegment {
            segment_seq,
            key,
            byte_length: byte_length as u64,
            sha256,
        });
    }

    let users_raw = obj
        .get("users")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| fail("local finalization manifest users must be non-empty array"))?;
    let mut users = Vec::with_capacity(users_raw.len());
    let mut path_nicks = std::collections::HashSet::new();
    let mut user_ids = std::collections::HashSet::new();
    let mut stem_ids = std::collections::HashSet::new();
    for (i, user) in users_raw.iter().enumerate() {
        if !is_plain_object(user) {
            return Err(fail(format!(
                "local finalization manifest users[{i}] must be object"
            )));
        }
        let user_obj = user.as_object().expect("checked");
        reject_unknown_keys(
            user_obj,
            &["discordUserId", "pathNick", "opaqueStemId"],
            &format!("local finalization manifest users[{i}]"),
        )?;
        let discord_user_id = validate_discord_snowflake(
            &require_string(
                &user_obj["discordUserId"],
                &format!("local finalization manifest users[{i}].discordUserId"),
            )?,
            &format!("local finalization manifest users[{i}].discordUserId"),
        )
        .map_err(|e| fail(e.to_string()))?;
        let path_nick = validate_path_nick(
            &require_string(
                &user_obj["pathNick"],
                &format!("local finalization manifest users[{i}].pathNick"),
            )?,
            &format!("local finalization manifest users[{i}].pathNick"),
        )?;
        let opaque_stem_id = validate_opaque_stem_id(
            &user_obj["opaqueStemId"],
            &format!("local finalization manifest users[{i}].opaqueStemId"),
        )?;
        let expected_opaque =
            derive_voice_v2_opaque_stem_id(&session_id, &guild_id, &discord_user_id, &path_nick);
        if opaque_stem_id != expected_opaque {
            return Err(fail(format!(
                "local finalization manifest users[{i}].opaqueStemId mismatch"
            )));
        }
        if path_nicks.contains(&path_nick)
            || user_ids.contains(&discord_user_id)
            || stem_ids.contains(&opaque_stem_id)
        {
            return Err(fail(format!(
                "local finalization manifest duplicate user at index {i}"
            )));
        }
        path_nicks.insert(path_nick.clone());
        user_ids.insert(discord_user_id.clone());
        stem_ids.insert(opaque_stem_id.clone());
        users.push(LocalFinalizationUser {
            discord_user_id,
            path_nick,
            opaque_stem_id,
        });
    }

    let output_raw = obj
        .get("output")
        .filter(|v| is_plain_object(v))
        .ok_or_else(|| fail("local finalization manifest output must be object"))?;
    let output_obj = output_raw.as_object().expect("checked");
    reject_unknown_keys(
        output_obj,
        &["wavFormat", "recoveryReportRelativePath"],
        "local finalization manifest output",
    )?;
    let wav_format = require_string(
        &output_obj["wavFormat"],
        "local finalization manifest output.wavFormat",
    )?;
    if wav_format != LOCAL_FINALIZATION_WAV_FORMAT {
        return Err(fail("local finalization manifest output.wavFormat invalid"));
    }
    let recovery_report_relative_path = require_string(
        &output_obj["recoveryReportRelativePath"],
        "local finalization manifest output.recoveryReportRelativePath",
    )?;
    if recovery_report_relative_path != RECOVERY_REPORT_RELATIVE_PATH {
        return Err(fail(
            "local finalization manifest output.recoveryReportRelativePath invalid",
        ));
    }

    let target_sample_count_48k = require_i64(
        &obj["targetSampleCount48k"],
        "local finalization manifest targetSampleCount48k",
    )?;
    if target_sample_count_48k < 0 {
        return Err(fail(
            "local finalization manifest targetSampleCount48k invalid",
        ));
    }

    let journal_sha256 = validate_sha256_hex(
        &require_string(
            &obj["journalSha256"],
            "local finalization manifest journalSha256",
        )?,
        "local finalization manifest journalSha256",
    )
    .map_err(|e| fail(e.to_string()))?;

    Ok(LocalFinalizationManifest {
        version: LOCAL_FINALIZATION_MANIFEST_VERSION,
        finalizer_algorithm_version: LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION.to_string(),
        session_id,
        guild_id,
        channel_id,
        session_start_wall_ms: require_i64(
            &obj["sessionStartWallMs"],
            "local finalization manifest sessionStartWallMs",
        )?,
        session_start_monotonic_ns: require_bigint_string(
            &obj["sessionStartMonotonicNs"],
            "local finalization manifest sessionStartMonotonicNs",
        )?,
        sealed_stop_wall_ms: require_i64(
            &obj["sealedStopWallMs"],
            "local finalization manifest sealedStopWallMs",
        )?,
        sealed_stop_monotonic_ns: require_bigint_string(
            &obj["sealedStopMonotonicNs"],
            "local finalization manifest sealedStopMonotonicNs",
        )?,
        target_sample_count_48k: target_sample_count_48k as u64,
        journal_sha256,
        recording_manifest,
        sealed_index,
        segments,
        users,
        output: LocalFinalizationOutputPolicy {
            wav_format: LOCAL_FINALIZATION_WAV_FORMAT.to_string(),
            recovery_report_relative_path: RECOVERY_REPORT_RELATIVE_PATH.to_string(),
        },
    })
}

pub fn canonicalize_local_finalization_manifest_for_digest(
    manifest: &LocalFinalizationManifest,
) -> String {
    let mut users: Vec<_> = manifest
        .users
        .iter()
        .map(|u| {
            serde_json::json!({
                "discordUserId": u.discord_user_id,
                "opaqueStemId": u.opaque_stem_id,
                "pathNick": u.path_nick,
            })
        })
        .collect();
    users.sort_by(|a, b| {
        compare_ascii_for_digest(
            a.get("pathNick").and_then(|v| v.as_str()).unwrap_or(""),
            b.get("pathNick").and_then(|v| v.as_str()).unwrap_or(""),
        )
    });
    let segments: Vec<_> = manifest
        .segments
        .iter()
        .map(|s| {
            serde_json::json!({
                "byteLength": s.byte_length,
                "key": s.key,
                "segmentSeq": s.segment_seq,
                "sha256": s.sha256,
            })
        })
        .collect();
    let payload = serde_json::json!({
        "channelId": manifest.channel_id,
        "finalizerAlgorithmVersion": manifest.finalizer_algorithm_version,
        "guildId": manifest.guild_id,
        "journalSha256": manifest.journal_sha256,
        "output": {
            "recoveryReportRelativePath": manifest.output.recovery_report_relative_path,
            "wavFormat": manifest.output.wav_format,
        },
        "recordingManifest": {
            "byteLength": manifest.recording_manifest.byte_length,
            "key": manifest.recording_manifest.key,
            "sha256": manifest.recording_manifest.sha256,
        },
        "sealedIndex": {
            "byteLength": manifest.sealed_index.byte_length,
            "key": manifest.sealed_index.key,
            "sha256": manifest.sealed_index.sha256,
        },
        "sealedStopMonotonicNs": manifest.sealed_stop_monotonic_ns,
        "sealedStopWallMs": manifest.sealed_stop_wall_ms,
        "segments": segments,
        "sessionId": manifest.session_id,
        "sessionStartMonotonicNs": manifest.session_start_monotonic_ns,
        "sessionStartWallMs": manifest.session_start_wall_ms,
        "targetSampleCount48k": manifest.target_sample_count_48k,
        "users": users,
        "version": manifest.version,
    });
    serde_json::to_string(&payload).expect("local manifest json")
}

pub fn compute_local_finalization_manifest_digest(manifest: &LocalFinalizationManifest) -> String {
    let canonical = canonicalize_local_finalization_manifest_for_digest(manifest);
    hex_digest(&Sha256::digest(canonical.as_bytes()))
}

/// Build a finalized manifest v1 for publication after local decode (recovery report optional).
pub fn local_manifest_to_syndicate_finalized(
    manifest: &LocalFinalizationManifest,
    stems: Vec<SyndicateFinalizedStem>,
    recovery_report: Option<SyndicateRecoveryReport>,
) -> Result<SyndicateFinalizedManifest, LocalFinalizationManifestError> {
    if stems.len() != manifest.users.len() {
        return Err(fail("stem count mismatch"));
    }
    let expected_bytes = expected_wav_file_bytes(manifest.target_sample_count_48k);
    for stem in &stems {
        if stem.wav_bytes != expected_bytes {
            return Err(fail(format!(
                "stem {} wav bytes must be {}",
                stem.path_nick, expected_bytes
            )));
        }
    }
    Ok(SyndicateFinalizedManifest {
        version: FINALIZED_MANIFEST_VERSION,
        session_id: manifest.session_id.clone(),
        guild_id: manifest.guild_id.clone(),
        channel_id: manifest.channel_id.clone(),
        session_start_wall_ms: manifest.session_start_wall_ms,
        session_start_monotonic_ns: manifest.session_start_monotonic_ns.clone(),
        sealed_stop_wall_ms: manifest.sealed_stop_wall_ms,
        sealed_stop_monotonic_ns: manifest.sealed_stop_monotonic_ns.clone(),
        target_sample_count_48k: manifest.target_sample_count_48k,
        journal_format_version: JOURNAL_FORMAT_VERSION_V2,
        journal_sha256: manifest.journal_sha256.clone(),
        stems,
        recovery_report,
    })
}
