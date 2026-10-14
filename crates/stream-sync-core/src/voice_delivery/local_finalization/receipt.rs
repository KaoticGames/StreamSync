//! Local finalization receipt v1.

use super::manifest::{
    compute_local_finalization_manifest_digest, LocalFinalizationManifest,
    LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION,
};
use crate::voice_delivery::finalized_manifest::expected_wav_file_bytes;
use crate::voice_delivery::finalized_manifest::{
    compare_ascii_for_digest, validate_discord_snowflake, validate_sha256_hex,
};
use crate::voice_delivery::hash::hex_digest;
use crate::voice_delivery::recovery_report::RECOVERY_REPORT_RELATIVE_PATH;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

pub const LOCAL_FINALIZATION_RECEIPT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFinalizationReceiptStem {
    pub discord_user_id: String,
    pub relative_path: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFinalizationReceiptRecoveryReport {
    pub relative_path: String,
    pub byte_length: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalFinalizationReceipt {
    pub version: u32,
    pub session_id: String,
    pub delivery_manifest_digest: String,
    pub finalizer_algorithm_version: String,
    pub device_id: String,
    pub completed_at_ms: i64,
    pub output_manifest_digest: String,
    pub stems: Vec<LocalFinalizationReceiptStem>,
    pub recovery_report: Option<LocalFinalizationReceiptRecoveryReport>,
    pub discarded_packet_count: u64,
    pub silence_range_count: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LocalFinalizationReceiptError {
    #[error("invalid local finalization receipt: {0}")]
    Invalid(String),
    #[error("local finalization receipt manifest mismatch: {0}")]
    ManifestMismatch(String),
}

fn fail(msg: impl Into<String>) -> LocalFinalizationReceiptError {
    LocalFinalizationReceiptError::Invalid(msg.into())
}

fn mismatch(msg: impl Into<String>) -> LocalFinalizationReceiptError {
    LocalFinalizationReceiptError::ManifestMismatch(msg.into())
}

fn is_plain_object(v: &Value) -> bool {
    matches!(v, Value::Object(_))
}

fn reject_unknown_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
    label: &str,
) -> Result<(), LocalFinalizationReceiptError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(fail(format!("{label} unknown key: {key}")));
        }
    }
    Ok(())
}

fn require_string(v: &Value, field: &str) -> Result<String, LocalFinalizationReceiptError> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        _ => Err(fail(format!("{field} must be non-empty string"))),
    }
}

fn require_i64(v: &Value, field: &str) -> Result<i64, LocalFinalizationReceiptError> {
    match v {
        Value::Number(n) if n.is_i64() => {
            n.as_i64().ok_or_else(|| fail(format!("{field} invalid")))
        }
        _ => Err(fail(format!("{field} must be safe integer"))),
    }
}

fn validate_device_id(device_id: &str) -> Result<(), LocalFinalizationReceiptError> {
    if device_id.is_empty() || device_id.len() > 128 {
        return Err(fail("local finalization receipt deviceId invalid"));
    }
    Ok(())
}

fn validate_relative_path(
    relative_path: &str,
    field: &str,
) -> Result<String, LocalFinalizationReceiptError> {
    if relative_path.starts_with('/')
        || relative_path.starts_with('\\')
        || relative_path.len() > 256
        || relative_path.contains("..")
        || relative_path.contains('/')
        || relative_path.contains('\\')
        || relative_path
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() && relative_path.contains(':'))
            .unwrap_or(false)
    {
        return Err(fail(format!("{field} must be safe relative filename")));
    }
    if relative_path.is_empty() {
        return Err(fail(format!("{field} invalid length")));
    }
    Ok(relative_path.to_string())
}

pub fn parse_local_finalization_receipt(
    raw: &Value,
) -> Result<LocalFinalizationReceipt, LocalFinalizationReceiptError> {
    if !is_plain_object(raw) {
        return Err(fail("local finalization receipt must be object"));
    }
    let obj = raw.as_object().expect("checked");
    reject_unknown_keys(
        obj,
        &[
            "version",
            "sessionId",
            "deliveryManifestDigest",
            "finalizerAlgorithmVersion",
            "deviceId",
            "completedAtMs",
            "outputManifestDigest",
            "stems",
            "recoveryReport",
            "discardedPacketCount",
            "silenceRangeCount",
        ],
        "local finalization receipt",
    )?;
    let version = require_i64(&obj["version"], "local finalization receipt version")?;
    if version != i64::from(LOCAL_FINALIZATION_RECEIPT_VERSION) {
        return Err(fail("unsupported local finalization receipt version"));
    }
    let session_id = require_string(&obj["sessionId"], "local finalization receipt sessionId")?;
    if session_id != session_id.to_lowercase() {
        return Err(fail(
            "local finalization receipt sessionId must be lowercase UUID",
        ));
    }
    if Uuid::parse_str(&session_id).is_err() {
        return Err(fail("local finalization receipt sessionId must be UUID"));
    }
    let delivery_manifest_digest = validate_sha256_hex(
        &require_string(
            &obj["deliveryManifestDigest"],
            "local finalization receipt deliveryManifestDigest",
        )?,
        "local finalization receipt deliveryManifestDigest",
    )
    .map_err(|e| fail(e.to_string()))?;
    let finalizer_algorithm_version = require_string(
        &obj["finalizerAlgorithmVersion"],
        "local finalization receipt finalizerAlgorithmVersion",
    )?;
    if finalizer_algorithm_version != LOCAL_FINALIZATION_FINALIZER_ALGORITHM_VERSION {
        return Err(fail(
            "local finalization receipt finalizerAlgorithmVersion invalid",
        ));
    }
    let device_id = require_string(&obj["deviceId"], "local finalization receipt deviceId")?;
    validate_device_id(&device_id)?;
    let completed_at_ms = require_i64(
        &obj["completedAtMs"],
        "local finalization receipt completedAtMs",
    )?;
    if completed_at_ms < 0 {
        return Err(fail("local finalization receipt completedAtMs invalid"));
    }
    let output_manifest_digest = validate_sha256_hex(
        &require_string(
            &obj["outputManifestDigest"],
            "local finalization receipt outputManifestDigest",
        )?,
        "local finalization receipt outputManifestDigest",
    )
    .map_err(|e| fail(e.to_string()))?;

    let stems_raw = obj
        .get("stems")
        .and_then(|v| v.as_array())
        .filter(|a| !a.is_empty())
        .ok_or_else(|| fail("local finalization receipt stems must be non-empty array"))?;
    let mut stems = Vec::with_capacity(stems_raw.len());
    let mut user_ids = std::collections::HashSet::new();
    let mut relative_paths = std::collections::HashSet::new();
    for (i, stem) in stems_raw.iter().enumerate() {
        if !is_plain_object(stem) {
            return Err(fail(format!(
                "local finalization receipt stems[{i}] must be object"
            )));
        }
        let stem_obj = stem.as_object().expect("checked");
        reject_unknown_keys(
            stem_obj,
            &["discordUserId", "relativePath", "byteLength", "sha256"],
            &format!("local finalization receipt stems[{i}]"),
        )?;
        let discord_user_id = validate_discord_snowflake(
            &require_string(
                &stem_obj["discordUserId"],
                &format!("local finalization receipt stems[{i}].discordUserId"),
            )?,
            &format!("local finalization receipt stems[{i}].discordUserId"),
        )
        .map_err(|e| fail(e.to_string()))?;
        let relative_path = validate_relative_path(
            &require_string(
                &stem_obj["relativePath"],
                &format!("local finalization receipt stems[{i}].relativePath"),
            )?,
            &format!("local finalization receipt stems[{i}].relativePath"),
        )?;
        let byte_length = require_i64(
            &stem_obj["byteLength"],
            &format!("local finalization receipt stems[{i}].byteLength"),
        )?;
        if byte_length <= 0 {
            return Err(fail(format!(
                "local finalization receipt stems[{i}].byteLength invalid"
            )));
        }
        let sha256 = validate_sha256_hex(
            &require_string(
                &stem_obj["sha256"],
                &format!("local finalization receipt stems[{i}].sha256"),
            )?,
            &format!("local finalization receipt stems[{i}].sha256"),
        )
        .map_err(|e| fail(e.to_string()))?;
        if user_ids.contains(&discord_user_id) || relative_paths.contains(&relative_path) {
            return Err(fail(format!(
                "local finalization receipt duplicate stem at index {i}"
            )));
        }
        user_ids.insert(discord_user_id.clone());
        relative_paths.insert(relative_path.clone());
        stems.push(LocalFinalizationReceiptStem {
            discord_user_id,
            relative_path,
            byte_length: byte_length as u64,
            sha256,
        });
    }

    let recovery_report = match obj.get("recoveryReport") {
        None => None,
        Some(raw_report) => {
            if !is_plain_object(raw_report) {
                return Err(fail(
                    "local finalization receipt recoveryReport must be object",
                ));
            }
            let report_obj = raw_report.as_object().expect("checked");
            reject_unknown_keys(
                report_obj,
                &["relativePath", "byteLength", "sha256"],
                "local finalization receipt recoveryReport",
            )?;
            let relative_path = require_string(
                &report_obj["relativePath"],
                "local finalization receipt recoveryReport.relativePath",
            )?;
            if relative_path != RECOVERY_REPORT_RELATIVE_PATH {
                return Err(fail(
                    "local finalization receipt recoveryReport.relativePath invalid",
                ));
            }
            let byte_length = require_i64(
                &report_obj["byteLength"],
                "local finalization receipt recoveryReport.byteLength",
            )?;
            if byte_length <= 0 {
                return Err(fail(
                    "local finalization receipt recoveryReport.byteLength invalid",
                ));
            }
            let sha256 = validate_sha256_hex(
                &require_string(
                    &report_obj["sha256"],
                    "local finalization receipt recoveryReport.sha256",
                )?,
                "local finalization receipt recoveryReport.sha256",
            )
            .map_err(|e| fail(e.to_string()))?;
            Some(LocalFinalizationReceiptRecoveryReport {
                relative_path: RECOVERY_REPORT_RELATIVE_PATH.to_string(),
                byte_length: byte_length as u64,
                sha256,
            })
        }
    };

    let discarded_packet_count = require_i64(
        &obj["discardedPacketCount"],
        "local finalization receipt discardedPacketCount",
    )?;
    if discarded_packet_count < 0 {
        return Err(fail(
            "local finalization receipt discardedPacketCount invalid",
        ));
    }
    let silence_range_count = require_i64(
        &obj["silenceRangeCount"],
        "local finalization receipt silenceRangeCount",
    )?;
    if silence_range_count < 0 {
        return Err(fail("local finalization receipt silenceRangeCount invalid"));
    }

    Ok(LocalFinalizationReceipt {
        version: LOCAL_FINALIZATION_RECEIPT_VERSION,
        session_id,
        delivery_manifest_digest,
        finalizer_algorithm_version,
        device_id,
        completed_at_ms,
        output_manifest_digest,
        stems,
        recovery_report,
        discarded_packet_count: discarded_packet_count as u64,
        silence_range_count: silence_range_count as u64,
    })
}

pub fn canonicalize_local_finalization_receipt_for_digest(
    receipt: &LocalFinalizationReceipt,
) -> String {
    let mut stems: Vec<_> = receipt
        .stems
        .iter()
        .map(|s| {
            serde_json::json!({
                "byteLength": s.byte_length,
                "discordUserId": s.discord_user_id,
                "relativePath": s.relative_path,
                "sha256": s.sha256,
            })
        })
        .collect();
    stems.sort_by(|a, b| {
        compare_ascii_for_digest(
            a.get("relativePath").and_then(|v| v.as_str()).unwrap_or(""),
            b.get("relativePath").and_then(|v| v.as_str()).unwrap_or(""),
        )
    });
    let mut payload = serde_json::Map::new();
    payload.insert(
        "completedAtMs".into(),
        serde_json::json!(receipt.completed_at_ms),
    );
    payload.insert(
        "deliveryManifestDigest".into(),
        serde_json::Value::String(receipt.delivery_manifest_digest.clone()),
    );
    payload.insert(
        "deviceId".into(),
        serde_json::Value::String(receipt.device_id.clone()),
    );
    payload.insert(
        "discardedPacketCount".into(),
        serde_json::json!(receipt.discarded_packet_count),
    );
    payload.insert(
        "finalizerAlgorithmVersion".into(),
        serde_json::Value::String(receipt.finalizer_algorithm_version.clone()),
    );
    payload.insert(
        "outputManifestDigest".into(),
        serde_json::Value::String(receipt.output_manifest_digest.clone()),
    );
    payload.insert(
        "sessionId".into(),
        serde_json::Value::String(receipt.session_id.clone()),
    );
    payload.insert(
        "silenceRangeCount".into(),
        serde_json::json!(receipt.silence_range_count),
    );
    payload.insert("stems".into(), serde_json::Value::Array(stems));
    payload.insert("version".into(), serde_json::json!(receipt.version));
    if let Some(report) = &receipt.recovery_report {
        payload.insert(
            "recoveryReport".into(),
            serde_json::json!({
                "byteLength": report.byte_length,
                "relativePath": report.relative_path,
                "sha256": report.sha256,
            }),
        );
    }
    serde_json::to_string(&serde_json::Value::Object(payload)).expect("receipt json")
}

pub fn compute_local_finalization_receipt_digest(receipt: &LocalFinalizationReceipt) -> String {
    let canonical = canonicalize_local_finalization_receipt_for_digest(receipt);
    hex_digest(&Sha256::digest(canonical.as_bytes()))
}

pub fn assert_local_finalization_receipt_matches_manifest(
    receipt: &LocalFinalizationReceipt,
    manifest: &LocalFinalizationManifest,
) -> Result<(), LocalFinalizationReceiptError> {
    if receipt.session_id != manifest.session_id {
        return Err(mismatch(
            "local finalization receipt sessionId does not match manifest",
        ));
    }
    let manifest_digest = compute_local_finalization_manifest_digest(manifest);
    if receipt.delivery_manifest_digest != manifest_digest {
        return Err(mismatch(
            "local finalization receipt deliveryManifestDigest does not match manifest",
        ));
    }
    if receipt.finalizer_algorithm_version != manifest.finalizer_algorithm_version {
        return Err(mismatch(
            "local finalization receipt finalizerAlgorithmVersion does not match manifest",
        ));
    }
    if receipt.stems.len() != manifest.users.len() {
        return Err(mismatch(
            "local finalization receipt stem count does not match manifest users",
        ));
    }
    let expected_wav_bytes = expected_wav_file_bytes(manifest.target_sample_count_48k);
    let stems_by_user: std::collections::HashMap<_, _> = receipt
        .stems
        .iter()
        .map(|s| (s.discord_user_id.as_str(), s))
        .collect();
    for user in &manifest.users {
        let stem = stems_by_user
            .get(user.discord_user_id.as_str())
            .ok_or_else(|| {
                mismatch(format!(
                    "local finalization receipt missing stem for user {}",
                    user.discord_user_id
                ))
            })?;
        let expected_path = format!("{}.wav", user.path_nick);
        if stem.relative_path != expected_path {
            return Err(mismatch(format!(
                "local finalization receipt stem path must be {}",
                expected_path
            )));
        }
        if stem.byte_length != expected_wav_bytes {
            return Err(mismatch(format!(
                "local finalization receipt stem byteLength must be {}",
                expected_wav_bytes
            )));
        }
    }
    if let Some(report) = &receipt.recovery_report {
        if report.relative_path != manifest.output.recovery_report_relative_path {
            return Err(mismatch(
                "local finalization receipt recovery report path does not match manifest policy",
            ));
        }
    }
    Ok(())
}
