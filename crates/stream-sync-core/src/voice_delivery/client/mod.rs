//! StreamSync host HTTP client for Syndicate voice delivery v2.

mod errors;
mod transport;

pub use errors::{VoiceV2ClientError, VoiceV2ErrorClass};
pub use transport::{
    parse_pending_response, validate_stem_range_meta, HttpVoiceV2Client, MockVoiceV2Client,
    PendingDelivery, ReceiptRequestBody, VoiceV2Client,
};

pub const VOICE_V2_MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
pub const PENDING_PATH: &str = "/api/stream-sync/voice/v2/deliveries/pending";
