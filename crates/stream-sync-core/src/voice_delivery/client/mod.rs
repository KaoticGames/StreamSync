//! StreamSync host HTTP client for Syndicate voice delivery v2.

mod errors;
mod redirect;
mod transport;

pub use errors::{VoiceV2ClientError, VoiceV2ErrorClass};
pub use transport::{
    bind_opaque_object_etag, parse_pending_response, validate_recovery_report_range_meta,
    validate_stem_range_meta, HttpVoiceV2Client, MockVoiceV2Client, PendingDelivery,
    ReceiptRequestBody, StemRangeMeta, VoiceV2Client,
};

pub const VOICE_V2_MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
pub const PENDING_PATH: &str = "/api/stream-sync/voice/v2/deliveries/pending";
