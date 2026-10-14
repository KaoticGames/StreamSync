//! StreamSync host HTTP client for Syndicate voice delivery v2.

mod errors;
mod redirect;
mod transport;

pub use errors::{network_from_reqwest, VoiceV2ClientError, VoiceV2ErrorClass};
pub use redirect::{redirect_status, validate_stem_redirect_location};
pub use redirect::{
    VOICE_V2_PRESIGN_RETRY_BACKOFF, VOICE_V2_PRESIGN_RETRY_MAX, VOICE_V2_STEM_REDIRECT_MAX,
};
pub use transport::{
    bind_opaque_object_etag, map_http_status, parse_error_code, parse_pending_response,
    parse_retry_after, read_bounded_range_body, validate_recovery_report_range_meta,
    validate_stem_range_meta, HttpVoiceV2Client, MockVoiceV2Client, PendingDelivery,
    ReceiptRequestBody, StemRangeMeta, VoiceV2Client, VOICE_V2_CONNECT_TIMEOUT,
    VOICE_V2_REQUEST_TIMEOUT,
};

pub const VOICE_V2_MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
pub const PENDING_PATH: &str = "/api/stream-sync/voice/v2/deliveries/pending";
