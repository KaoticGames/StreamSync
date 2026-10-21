//! Shared fixtures for direct-local vertical-1 tests.

use crate::voice_delivery::hash::hex_digest;
use crate::voice_delivery::local_finalization::{
    encode_journal_file, encode_journal_record, JournalRecord, JOURNAL_FORMAT_VERSION_V2,
};
use sha2::{Digest, Sha256};

pub const SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
pub const GUILD_ID: &str = "123456789012345678";
pub const DEVICE_ID: &str = "device-aaaaaaaaaaaaaaaa";
pub const DISCORD_INSTANCE_ID: &str = "discord-instance-01";
/// 32 raw bytes as base64url without padding (43 `[A-Za-z0-9_-]` chars).
pub const CAPABILITY_TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
pub const WSS_URL: &str = "wss://discord-worker.example/voice-direct/v1";
pub const EXPIRES_AT_MS: u64 = 1_761_062_400_000;

pub const JS_MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

pub fn minimal_synj_segment_bytes() -> Vec<u8> {
    let record = JournalRecord {
        discord_user_id: "123456789012345678".to_string(),
        ssrc_epoch: 1,
        sequence: 0,
        rtp_timestamp: 960,
        arrived_at_ns: 1,
        session_capture_offset_samples: Some(0),
        opus_payload: vec![0xF8, 0xFF, 0xFE],
    };
    let body = encode_journal_record(JOURNAL_FORMAT_VERSION_V2, &record);
    encode_journal_file(JOURNAL_FORMAT_VERSION_V2, &[body])
}

pub fn sha256_lower_hex(bytes: &[u8]) -> String {
    hex_digest(&Sha256::digest(bytes))
}

/// JSON shape returned by ChatBot `issueDirectDeliveryCapability` plus Slice-2 `wssUrl`.
pub fn valid_binding_json() -> serde_json::Value {
    serde_json::json!({
        "sessionId": SESSION_ID,
        "guildId": GUILD_ID,
        "deviceId": DEVICE_ID,
        "deliveryGeneration": 1,
        "discordInstanceId": DISCORD_INSTANCE_ID,
        "leaderFenceGeneration": 1,
        "token": CAPABILITY_TOKEN,
        "expiresAtMs": EXPIRES_AT_MS,
        "wssUrl": WSS_URL,
    })
}
