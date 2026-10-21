//! Strict camelCase direct WSS control JSON (protocol v1).

use crate::voice_delivery::direct_local::checkpoint::{
    DirectCumulativeCheckpoint, NO_SEGMENTS_YET,
};
use crate::voice_delivery::direct_local::DirectSessionBinding;
use crate::voice_delivery::fs::durability::NamespaceDurability;
use serde_json::Value;
use thiserror::Error;

pub const DIRECT_WSS_PROTOCOL_VERSION: u32 = 1;
pub const MAX_DIRECT_WSS_CONTROL_JSON_BYTES: usize = 16 * 1024;
/// Largest integer representable exactly in IEEE-754 double (ECMAScript `Number`).
const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectWssControlParseError {
    #[error("control payload too large")]
    ControlPayloadTooLarge,
    #[error("not json object")]
    NotJsonObject,
    #[error("unknown field")]
    UnknownField,
    #[error("unknown message type")]
    UnknownMessageType,
    #[error("unsupported protocol version")]
    UnsupportedProtocolVersion,
    #[error("malformed control message")]
    MalformedMessage,
    #[error("unknown reject code")]
    UnknownRejectCode,
    #[error("unknown revoke code")]
    UnknownRevokeCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectWssSessionRejectCode {
    BindingMismatch,
    CapabilityExpired,
}

impl DirectWssSessionRejectCode {
    pub fn parse(s: &str) -> Result<Self, DirectWssControlParseError> {
        match s {
            "bindingMismatch" => Ok(Self::BindingMismatch),
            "capabilityExpired" => Ok(Self::CapabilityExpired),
            _ => Err(DirectWssControlParseError::UnknownRejectCode),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::BindingMismatch => "bindingMismatch",
            Self::CapabilityExpired => "capabilityExpired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectWssSessionRevokeCode {
    GenerationAdvanced,
    LeaderFenced,
}

impl DirectWssSessionRevokeCode {
    pub fn parse(s: &str) -> Result<Self, DirectWssControlParseError> {
        match s {
            "generationAdvanced" => Ok(Self::GenerationAdvanced),
            "leaderFenced" => Ok(Self::LeaderFenced),
            _ => Err(DirectWssControlParseError::UnknownRevokeCode),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::GenerationAdvanced => "generationAdvanced",
            Self::LeaderFenced => "leaderFenced",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssResumeCheckpoint {
    pub highest_contiguous_sequence: Option<u64>,
    pub last_segment_byte_length: u64,
    pub last_segment_sha256_hex: String,
}

impl DirectWssResumeCheckpoint {
    pub fn from_durable_checkpoint(checkpoint: &DirectCumulativeCheckpoint) -> Self {
        Self {
            highest_contiguous_sequence: (checkpoint.highest_contiguous_sequence
                != NO_SEGMENTS_YET)
                .then_some(checkpoint.highest_contiguous_sequence),
            last_segment_byte_length: checkpoint.last_segment_byte_length,
            last_segment_sha256_hex: checkpoint.last_segment_sha256_hex.clone(),
        }
    }

    pub fn is_no_segments_yet(&self) -> bool {
        self.highest_contiguous_sequence.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssHello {
    pub protocol_version: u32,
    pub session_id: String,
    pub guild_id: String,
    pub device_id: String,
    pub delivery_generation: u64,
    pub discord_instance_id: String,
    pub leader_fence_generation: u64,
    pub resume: DirectWssResumeCheckpoint,
}

impl DirectWssHello {
    pub fn from_binding_and_checkpoint(
        binding: &DirectSessionBinding,
        resume: DirectWssResumeCheckpoint,
    ) -> Self {
        Self {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: binding.session_id.clone(),
            guild_id: binding.guild_id.clone(),
            device_id: binding.device_id.clone(),
            delivery_generation: binding.delivery_generation,
            discord_instance_id: binding.discord_instance_id.clone(),
            leader_fence_generation: binding.leader_fence_generation,
            resume,
        }
    }

    pub fn to_json_text(&self) -> Result<String, DirectWssControlParseError> {
        DirectWssControlMessage::Hello(self.clone()).to_json_text()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssSessionAccept {
    pub protocol_version: u32,
    pub session_id: String,
    pub guild_id: String,
    pub device_id: String,
    pub delivery_generation: u64,
    pub discord_instance_id: String,
    pub leader_fence_generation: u64,
    pub next_expected_segment_sequence: u64,
}

impl DirectWssSessionAccept {
    pub fn echo_binding(
        binding: &DirectSessionBinding,
        next_expected_segment_sequence: u64,
    ) -> Self {
        Self {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: binding.session_id.clone(),
            guild_id: binding.guild_id.clone(),
            device_id: binding.device_id.clone(),
            delivery_generation: binding.delivery_generation,
            discord_instance_id: binding.discord_instance_id.clone(),
            leader_fence_generation: binding.leader_fence_generation,
            next_expected_segment_sequence,
        }
    }

    pub fn matches_binding(&self, binding: &DirectSessionBinding) -> bool {
        self.protocol_version == DIRECT_WSS_PROTOCOL_VERSION
            && self.session_id == binding.session_id
            && self.guild_id == binding.guild_id
            && self.device_id == binding.device_id
            && self.delivery_generation == binding.delivery_generation
            && self.discord_instance_id == binding.discord_instance_id
            && self.leader_fence_generation == binding.leader_fence_generation
    }

    pub fn to_json_text(&self) -> Result<String, DirectWssControlParseError> {
        DirectWssControlMessage::SessionAccept(self.clone()).to_json_text()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssSegmentOffer {
    pub protocol_version: u32,
    pub session_id: String,
    pub guild_id: String,
    pub device_id: String,
    pub delivery_generation: u64,
    pub discord_instance_id: String,
    pub leader_fence_generation: u64,
    pub segment_sequence: u64,
    pub byte_length: u64,
    pub sha256_hex: String,
}

impl DirectWssSegmentOffer {
    pub fn to_json_text(&self) -> Result<String, DirectWssControlParseError> {
        DirectWssControlMessage::SegmentOffer(self.clone()).to_json_text()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssCumulativeAck {
    pub protocol_version: u32,
    pub session_id: String,
    pub delivery_generation: u64,
    pub highest_contiguous_sequence: u64,
    pub last_segment_byte_length: u64,
    pub last_segment_sha256_hex: String,
    pub namespace_durability: NamespaceDurability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssSessionReject {
    pub protocol_version: u32,
    pub code: DirectWssSessionRejectCode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectWssSessionRevoke {
    pub protocol_version: u32,
    pub code: DirectWssSessionRevokeCode,
    pub delivery_generation: u64,
    pub leader_fence_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DirectWssControlMessage {
    Hello(DirectWssHello),
    SessionAccept(DirectWssSessionAccept),
    SegmentOffer(DirectWssSegmentOffer),
    CumulativeAck(DirectWssCumulativeAck),
    SessionReject(DirectWssSessionReject),
    SessionRevoke(DirectWssSessionRevoke),
}

impl DirectWssControlMessage {
    pub fn cumulative_ack_from_checkpoint(checkpoint: &DirectCumulativeCheckpoint) -> Self {
        Self::CumulativeAck(DirectWssCumulativeAck {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: checkpoint.session_id.clone(),
            delivery_generation: checkpoint.delivery_generation,
            highest_contiguous_sequence: checkpoint.highest_contiguous_sequence,
            last_segment_byte_length: checkpoint.last_segment_byte_length,
            last_segment_sha256_hex: checkpoint.last_segment_sha256_hex.clone(),
            namespace_durability: checkpoint.namespace_durability,
        })
    }

    pub fn is_cumulative_ack(&self) -> bool {
        matches!(self, Self::CumulativeAck(_))
    }

    pub fn as_hello(&self) -> Option<&DirectWssHello> {
        match self {
            Self::Hello(h) => Some(h),
            _ => None,
        }
    }

    pub fn as_cumulative_ack(&self) -> Option<&DirectWssCumulativeAck> {
        match self {
            Self::CumulativeAck(a) => Some(a),
            _ => None,
        }
    }

    pub fn to_json_text(&self) -> Result<String, DirectWssControlParseError> {
        let value = self.to_json_value()?;
        serde_json::to_string(&value).map_err(|_| DirectWssControlParseError::MalformedMessage)
    }

    fn to_json_value(&self) -> Result<Value, DirectWssControlParseError> {
        match self {
            Self::Hello(h) => Ok(serde_json::json!({
                "type": "hello",
                "protocolVersion": h.protocol_version,
                "sessionId": h.session_id,
                "guildId": h.guild_id,
                "deviceId": h.device_id,
                "deliveryGeneration": h.delivery_generation,
                "discordInstanceId": h.discord_instance_id,
                "leaderFenceGeneration": h.leader_fence_generation,
                "resume": {
                    "highestContiguousSequence": resume_highest_contiguous_sequence_json(
                        h.resume.highest_contiguous_sequence,
                    ),
                    "lastSegmentByteLength": h.resume.last_segment_byte_length,
                    "lastSegmentSha256Hex": h.resume.last_segment_sha256_hex,
                }
            })),
            Self::SessionAccept(a) => Ok(serde_json::json!({
                "type": "sessionAccept",
                "protocolVersion": a.protocol_version,
                "sessionId": a.session_id,
                "guildId": a.guild_id,
                "deviceId": a.device_id,
                "deliveryGeneration": a.delivery_generation,
                "discordInstanceId": a.discord_instance_id,
                "leaderFenceGeneration": a.leader_fence_generation,
                "nextExpectedSegmentSequence": a.next_expected_segment_sequence,
            })),
            Self::SegmentOffer(o) => Ok(serde_json::json!({
                "type": "segmentOffer",
                "protocolVersion": o.protocol_version,
                "sessionId": o.session_id,
                "guildId": o.guild_id,
                "deviceId": o.device_id,
                "deliveryGeneration": o.delivery_generation,
                "discordInstanceId": o.discord_instance_id,
                "leaderFenceGeneration": o.leader_fence_generation,
                "segmentSequence": o.segment_sequence,
                "byteLength": o.byte_length,
                "sha256Hex": o.sha256_hex,
            })),
            Self::CumulativeAck(a) => Ok(serde_json::json!({
                "type": "cumulativeAck",
                "protocolVersion": a.protocol_version,
                "sessionId": a.session_id,
                "deliveryGeneration": a.delivery_generation,
                "highestContiguousSequence": a.highest_contiguous_sequence,
                "lastSegmentByteLength": a.last_segment_byte_length,
                "lastSegmentSha256Hex": a.last_segment_sha256_hex,
                "namespaceDurability": a.namespace_durability,
            })),
            Self::SessionReject(r) => Ok(serde_json::json!({
                "type": "sessionReject",
                "protocolVersion": r.protocol_version,
                "code": r.code.as_str(),
            })),
            Self::SessionRevoke(r) => Ok(serde_json::json!({
                "type": "sessionRevoke",
                "protocolVersion": r.protocol_version,
                "code": r.code.as_str(),
                "deliveryGeneration": r.delivery_generation,
                "leaderFenceGeneration": r.leader_fence_generation,
            })),
        }
    }

    pub fn parse_inbound_text(json: &str) -> Result<Self, DirectWssControlParseError> {
        if json.len() > MAX_DIRECT_WSS_CONTROL_JSON_BYTES {
            return Err(DirectWssControlParseError::ControlPayloadTooLarge);
        }
        let root: Value =
            serde_json::from_str(json).map_err(|_| DirectWssControlParseError::NotJsonObject)?;
        let obj = match root {
            Value::Object(map) => map,
            _ => return Err(DirectWssControlParseError::NotJsonObject),
        };
        let msg_type = obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or(DirectWssControlParseError::MalformedMessage)?;
        let protocol_version = require_u32_field(&obj, "protocolVersion")?;
        if protocol_version != DIRECT_WSS_PROTOCOL_VERSION {
            return Err(DirectWssControlParseError::UnsupportedProtocolVersion);
        }
        match msg_type {
            "hello" => parse_hello(&obj),
            "sessionAccept" => parse_session_accept(&obj),
            "segmentOffer" => parse_segment_offer(&obj),
            "cumulativeAck" => parse_cumulative_ack(&obj),
            "sessionReject" => parse_session_reject(&obj),
            "sessionRevoke" => parse_session_revoke(&obj),
            _ => Err(DirectWssControlParseError::UnknownMessageType),
        }
    }
}

fn parse_hello(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &[
        "type",
        "protocolVersion",
        "sessionId",
        "guildId",
        "deviceId",
        "deliveryGeneration",
        "discordInstanceId",
        "leaderFenceGeneration",
        "resume",
    ];
    reject_unknown_keys(obj, KEYS)?;
    let resume_obj = obj
        .get("resume")
        .and_then(|v| v.as_object())
        .ok_or(DirectWssControlParseError::MalformedMessage)?;
    const RESUME_KEYS: &[&str] = &[
        "highestContiguousSequence",
        "lastSegmentByteLength",
        "lastSegmentSha256Hex",
    ];
    reject_unknown_keys(resume_obj, RESUME_KEYS)?;
    Ok(DirectWssControlMessage::Hello(DirectWssHello {
        protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
        session_id: require_string_field(obj, "sessionId")?,
        guild_id: require_string_field(obj, "guildId")?,
        device_id: require_string_field(obj, "deviceId")?,
        delivery_generation: require_js_safe_u64_field(obj, "deliveryGeneration")?,
        discord_instance_id: require_string_field(obj, "discordInstanceId")?,
        leader_fence_generation: require_js_safe_u64_field(obj, "leaderFenceGeneration")?,
        resume: {
            let highest_contiguous_sequence = require_resume_highest_contiguous_sequence_field(
                resume_obj,
                "highestContiguousSequence",
            )?;
            let last_segment_byte_length =
                require_js_safe_u64_field(resume_obj, "lastSegmentByteLength")?;
            let last_segment_sha256_hex = require_string_field(resume_obj, "lastSegmentSha256Hex")?;
            if highest_contiguous_sequence.is_none()
                && (last_segment_byte_length != 0 || !last_segment_sha256_hex.is_empty())
            {
                return Err(DirectWssControlParseError::MalformedMessage);
            }
            DirectWssResumeCheckpoint {
                highest_contiguous_sequence,
                last_segment_byte_length,
                last_segment_sha256_hex,
            }
        },
    }))
}

fn parse_session_accept(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &[
        "type",
        "protocolVersion",
        "sessionId",
        "guildId",
        "deviceId",
        "deliveryGeneration",
        "discordInstanceId",
        "leaderFenceGeneration",
        "nextExpectedSegmentSequence",
    ];
    reject_unknown_keys(obj, KEYS)?;
    Ok(DirectWssControlMessage::SessionAccept(
        DirectWssSessionAccept {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: require_string_field(obj, "sessionId")?,
            guild_id: require_string_field(obj, "guildId")?,
            device_id: require_string_field(obj, "deviceId")?,
            delivery_generation: require_js_safe_u64_field(obj, "deliveryGeneration")?,
            discord_instance_id: require_string_field(obj, "discordInstanceId")?,
            leader_fence_generation: require_js_safe_u64_field(obj, "leaderFenceGeneration")?,
            next_expected_segment_sequence: require_js_safe_u64_field(
                obj,
                "nextExpectedSegmentSequence",
            )?,
        },
    ))
}

fn parse_segment_offer(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &[
        "type",
        "protocolVersion",
        "sessionId",
        "guildId",
        "deviceId",
        "deliveryGeneration",
        "discordInstanceId",
        "leaderFenceGeneration",
        "segmentSequence",
        "byteLength",
        "sha256Hex",
    ];
    reject_unknown_keys(obj, KEYS)?;
    Ok(DirectWssControlMessage::SegmentOffer(
        DirectWssSegmentOffer {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: require_string_field(obj, "sessionId")?,
            guild_id: require_string_field(obj, "guildId")?,
            device_id: require_string_field(obj, "deviceId")?,
            delivery_generation: require_js_safe_u64_field(obj, "deliveryGeneration")?,
            discord_instance_id: require_string_field(obj, "discordInstanceId")?,
            leader_fence_generation: require_js_safe_u64_field(obj, "leaderFenceGeneration")?,
            segment_sequence: require_js_safe_u64_field(obj, "segmentSequence")?,
            byte_length: require_js_safe_u64_field(obj, "byteLength")?,
            sha256_hex: require_string_field(obj, "sha256Hex")?,
        },
    ))
}

fn parse_cumulative_ack(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &[
        "type",
        "protocolVersion",
        "sessionId",
        "deliveryGeneration",
        "highestContiguousSequence",
        "lastSegmentByteLength",
        "lastSegmentSha256Hex",
        "namespaceDurability",
    ];
    reject_unknown_keys(obj, KEYS)?;
    let ns = obj
        .get("namespaceDurability")
        .ok_or(DirectWssControlParseError::MalformedMessage)?;
    let namespace_durability: NamespaceDurability = serde_json::from_value(ns.clone())
        .map_err(|_| DirectWssControlParseError::MalformedMessage)?;
    Ok(DirectWssControlMessage::CumulativeAck(
        DirectWssCumulativeAck {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            session_id: require_string_field(obj, "sessionId")?,
            delivery_generation: require_js_safe_u64_field(obj, "deliveryGeneration")?,
            highest_contiguous_sequence: require_cumulative_ack_highest_contiguous_sequence_field(
                obj,
                "highestContiguousSequence",
            )?,
            last_segment_byte_length: require_js_safe_u64_field(obj, "lastSegmentByteLength")?,
            last_segment_sha256_hex: require_string_field(obj, "lastSegmentSha256Hex")?,
            namespace_durability,
        },
    ))
}

fn parse_session_reject(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &["type", "protocolVersion", "code"];
    reject_unknown_keys(obj, KEYS)?;
    let code = DirectWssSessionRejectCode::parse(&require_string_field(obj, "code")?)?;
    Ok(DirectWssControlMessage::SessionReject(
        DirectWssSessionReject {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            code,
        },
    ))
}

fn parse_session_revoke(
    obj: &serde_json::Map<String, Value>,
) -> Result<DirectWssControlMessage, DirectWssControlParseError> {
    const KEYS: &[&str] = &[
        "type",
        "protocolVersion",
        "code",
        "deliveryGeneration",
        "leaderFenceGeneration",
    ];
    reject_unknown_keys(obj, KEYS)?;
    let code = DirectWssSessionRevokeCode::parse(&require_string_field(obj, "code")?)?;
    Ok(DirectWssControlMessage::SessionRevoke(
        DirectWssSessionRevoke {
            protocol_version: DIRECT_WSS_PROTOCOL_VERSION,
            code,
            delivery_generation: require_js_safe_u64_field(obj, "deliveryGeneration")?,
            leader_fence_generation: require_js_safe_u64_field(obj, "leaderFenceGeneration")?,
        },
    ))
}

fn reject_unknown_keys(
    obj: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<(), DirectWssControlParseError> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(DirectWssControlParseError::UnknownField);
        }
    }
    Ok(())
}

fn require_string_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<String, DirectWssControlParseError> {
    match obj.get(field) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => Err(DirectWssControlParseError::MalformedMessage),
    }
}

fn require_u64_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<u64, DirectWssControlParseError> {
    match obj.get(field) {
        Some(Value::Number(n)) if n.is_u64() => Ok(n.as_u64().unwrap()),
        Some(Value::Number(n)) if n.is_i64() && n.as_i64().unwrap() >= 0 => {
            Ok(n.as_i64().unwrap() as u64)
        }
        _ => Err(DirectWssControlParseError::MalformedMessage),
    }
}

fn require_js_safe_u64_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<u64, DirectWssControlParseError> {
    let n = require_u64_field(obj, field)?;
    if n > JS_MAX_SAFE_INTEGER {
        return Err(DirectWssControlParseError::MalformedMessage);
    }
    Ok(n)
}

fn resume_highest_contiguous_sequence_json(seq: Option<u64>) -> Value {
    match seq {
        None => Value::Null,
        Some(n) => Value::Number(n.into()),
    }
}

fn require_resume_highest_contiguous_sequence_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<Option<u64>, DirectWssControlParseError> {
    match obj.get(field) {
        Some(Value::Null) => Ok(None),
        Some(_) => {
            let n = require_js_safe_u64_field(obj, field)?;
            if n == NO_SEGMENTS_YET {
                return Err(DirectWssControlParseError::MalformedMessage);
            }
            Ok(Some(n))
        }
        None => Err(DirectWssControlParseError::MalformedMessage),
    }
}

fn require_cumulative_ack_highest_contiguous_sequence_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<u64, DirectWssControlParseError> {
    require_js_safe_u64_field(obj, field)
}

fn require_u32_field(
    obj: &serde_json::Map<String, Value>,
    field: &'static str,
) -> Result<u32, DirectWssControlParseError> {
    let n = require_js_safe_u64_field(obj, field)?;
    if n > u32::MAX as u64 {
        return Err(DirectWssControlParseError::MalformedMessage);
    }
    Ok(n as u32)
}
