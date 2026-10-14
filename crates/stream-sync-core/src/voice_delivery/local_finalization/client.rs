//! HTTP client for `/api/stream-sync/voice/local-v1/*`.

use super::manifest::{
    compute_local_finalization_manifest_digest, parse_local_finalization_manifest,
    LocalFinalizationManifest,
};
use super::receipt::LocalFinalizationReceipt;
use crate::voice_delivery::client::{
    bind_opaque_object_etag, map_http_status, parse_error_code, parse_retry_after,
    read_bounded_range_body, validate_stem_range_meta, StemRangeMeta, VoiceV2ClientError,
    VOICE_V2_MAX_CHUNK_BYTES,
};
use crate::voice_delivery::client::{
    network_from_reqwest, redirect_status, validate_stem_redirect_location,
    VOICE_V2_CONNECT_TIMEOUT, VOICE_V2_PRESIGN_RETRY_BACKOFF, VOICE_V2_PRESIGN_RETRY_MAX,
    VOICE_V2_REQUEST_TIMEOUT, VOICE_V2_STEM_REDIRECT_MAX,
};
use serde_json::Value;
use std::io::Write;

pub const LOCAL_PENDING_PATH: &str = "/api/stream-sync/voice/local-v1/deliveries/pending";

#[derive(Debug, Clone)]
pub struct LocalPendingDelivery {
    pub session_id: String,
    pub manifest_digest: String,
    pub sealed_at: String,
    pub manifest: LocalFinalizationManifest,
}

pub trait LocalFinalizationClient: Send + Sync {
    fn fetch_pending(&self, limit: u32) -> Result<Vec<LocalPendingDelivery>, VoiceV2ClientError>;
    fn claim_delivery(
        &self,
        session_id: &str,
        device_id: &str,
    ) -> Result<Value, VoiceV2ClientError>;
    fn renew_lease(&self, session_id: &str, device_id: &str) -> Result<(), VoiceV2ClientError>;
    fn fetch_manifest(
        &self,
        session_id: &str,
    ) -> Result<LocalFinalizationManifest, VoiceV2ClientError>;
    fn fetch_segment_range(
        &self,
        session_id: &str,
        segment_seq: u32,
        offset: u64,
        length: u64,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError>;
    fn post_receipt(
        &self,
        session_id: &str,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), VoiceV2ClientError>;
    fn release_delivery(&self, session_id: &str, device_id: &str)
        -> Result<(), VoiceV2ClientError>;
}

pub struct HttpLocalFinalizationClient {
    base_url: String,
    bearer: String,
    http: reqwest::blocking::Client,
    stem_redirect_allowlist: Vec<String>,
    allow_http_stem_redirects: bool,
}

impl HttpLocalFinalizationClient {
    pub fn new(base_url: impl Into<String>, bearer: impl Into<String>) -> Self {
        let http = reqwest::blocking::Client::builder()
            .connect_timeout(VOICE_V2_CONNECT_TIMEOUT)
            .timeout(VOICE_V2_REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("local finalization HTTP client");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            bearer: bearer.into(),
            http,
            stem_redirect_allowlist: Vec::new(),
            allow_http_stem_redirects: false,
        }
    }

    fn auth_header(&self) -> String {
        crate::delegated_lifecycle::connection_key_authorization(&self.bearer)
    }

    fn fetch_ranged(
        &self,
        url: &str,
        offset: u64,
        length: u64,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        if length == 0 || length > VOICE_V2_MAX_CHUNK_BYTES {
            return Err(VoiceV2ClientError::InvalidRange);
        }
        let end = offset
            .checked_add(length)
            .and_then(|v| v.checked_sub(1))
            .ok_or(VoiceV2ClientError::InvalidRange)?;
        let range = format!("bytes={offset}-{end}");
        let mut last_presign_denied = VoiceV2ClientError::PresignDenied;
        for attempt in 0..VOICE_V2_PRESIGN_RETRY_MAX {
            if attempt > 0 {
                std::thread::sleep(VOICE_V2_PRESIGN_RETRY_BACKOFF);
            }
            let res = self
                .http
                .get(url)
                .header("Authorization", self.auth_header())
                .header(reqwest::header::RANGE, &range)
                .send()
                .map_err(network_from_reqwest)?;
            if let Some(wait) = parse_retry_after(res.headers()) {
                return Err(VoiceV2ClientError::RetryAfter(wait));
            }
            let status = res.status().as_u16();
            if status == 404 {
                return Err(VoiceV2ClientError::DeliveryNotFound);
            }
            if redirect_status(status) {
                let location = res
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| VoiceV2ClientError::InsecureRedirect("missing location".into()))?
                    .to_string();
                let _ = res.bytes();
                match self.follow_redirect(&location, &range, writer, length) {
                    Ok(meta) => return Ok(meta),
                    Err(VoiceV2ClientError::PresignDenied) => {
                        last_presign_denied = VoiceV2ClientError::PresignDenied;
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
            if status != 206 {
                let body: Value = res.json().unwrap_or(Value::Null);
                return Err(map_http_status(
                    status,
                    parse_error_code(&body).as_deref(),
                    None,
                ));
            }
            return self.parse_206(res, length, writer);
        }
        Err(last_presign_denied)
    }

    fn follow_redirect(
        &self,
        location: &str,
        range: &str,
        writer: &mut dyn Write,
        length: u64,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        validate_stem_redirect_location(
            location,
            &self.base_url,
            &self.stem_redirect_allowlist,
            self.allow_http_stem_redirects,
        )?;
        let mut bound_etag: Option<String> = None;
        let mut hops = 0u32;
        let mut current = location.to_string();
        while hops < VOICE_V2_STEM_REDIRECT_MAX {
            hops += 1;
            let res = self
                .http
                .get(&current)
                .header(reqwest::header::RANGE, range)
                .send()
                .map_err(network_from_reqwest)?;
            let status = res.status().as_u16();
            if redirect_status(status) {
                current = res
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| VoiceV2ClientError::InsecureRedirect("missing location".into()))?
                    .to_string();
                validate_stem_redirect_location(
                    &current,
                    &self.base_url,
                    &self.stem_redirect_allowlist,
                    self.allow_http_stem_redirects,
                )?;
                continue;
            }
            if status == 401 || status == 403 {
                let _ = res.bytes();
                return Err(VoiceV2ClientError::PresignDenied);
            }
            if status != 206 {
                let body: Value = res.json().unwrap_or(Value::Null);
                return Err(map_http_status(
                    status,
                    parse_error_code(&body).as_deref(),
                    None,
                ));
            }
            let etag = res
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            bind_opaque_object_etag(&mut bound_etag, etag)?;
            return self.parse_206(res, length, writer);
        }
        Err(VoiceV2ClientError::InsecureRedirect(
            "too many redirects".into(),
        ))
    }

    fn parse_206(
        &self,
        res: reqwest::blocking::Response,
        length: u64,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        let status = res.status().as_u16();
        let content_range = res
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let content_length = res
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .unwrap_or(length);
        let etag = res
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let total_length = content_range
            .split('/')
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        read_bounded_range_body(res, length, VOICE_V2_MAX_CHUNK_BYTES, writer)?;
        Ok(StemRangeMeta {
            status,
            content_range,
            content_length,
            etag,
            total_length,
        })
    }
}

pub fn parse_local_pending_response(
    body: &Value,
) -> Result<Vec<LocalPendingDelivery>, VoiceV2ClientError> {
    if body.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        return Err(VoiceV2ClientError::Parse("ok:false".into()));
    }
    let deliveries = body
        .get("deliveries")
        .and_then(|v| v.as_array())
        .ok_or_else(|| VoiceV2ClientError::Parse("deliveries array missing".into()))?;
    let mut out = Vec::with_capacity(deliveries.len());
    for (i, item) in deliveries.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| VoiceV2ClientError::Parse(format!("deliveries[{i}] not object")))?;
        let session_id = obj
            .get("sessionId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| VoiceV2ClientError::Parse("sessionId".into()))?
            .to_string();
        let manifest_digest = obj
            .get("manifestDigest")
            .and_then(|v| v.as_str())
            .ok_or_else(|| VoiceV2ClientError::Parse("manifestDigest".into()))?
            .to_string();
        let sealed_at = obj
            .get("sealedAt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| VoiceV2ClientError::Parse("sealedAt".into()))?
            .to_string();
        let manifest_raw = obj
            .get("manifest")
            .ok_or_else(|| VoiceV2ClientError::Parse("manifest required".into()))?;
        let manifest = parse_local_finalization_manifest(manifest_raw)
            .map_err(|e| VoiceV2ClientError::Parse(e.to_string()))?;
        if manifest.session_id != session_id {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        let computed = compute_local_finalization_manifest_digest(&manifest);
        if computed != manifest_digest {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        out.push(LocalPendingDelivery {
            session_id,
            manifest_digest,
            sealed_at,
            manifest,
        });
    }
    out.sort_by(|a, b| {
        a.sealed_at
            .cmp(&b.sealed_at)
            .then(a.session_id.cmp(&b.session_id))
    });
    Ok(out)
}

impl LocalFinalizationClient for HttpLocalFinalizationClient {
    fn fetch_pending(&self, limit: u32) -> Result<Vec<LocalPendingDelivery>, VoiceV2ClientError> {
        let url = format!("{}{}?limit={}", self.base_url, LOCAL_PENDING_PATH, limit);
        let res = self
            .http
            .get(url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/json")
            .send()
            .map_err(network_from_reqwest)?;
        if let Some(wait) = parse_retry_after(res.headers()) {
            return Err(VoiceV2ClientError::RetryAfter(wait));
        }
        let status = res.status();
        let body: Value = res
            .json()
            .map_err(|e| VoiceV2ClientError::Parse(e.to_string()))?;
        if !status.is_success() {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&body).as_deref(),
                None,
            ));
        }
        parse_local_pending_response(&body)
    }

    fn claim_delivery(
        &self,
        session_id: &str,
        device_id: &str,
    ) -> Result<Value, VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/claim",
            self.base_url, session_id
        );
        let res = self
            .http
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&serde_json::json!({ "deviceId": device_id }))
            .send()
            .map_err(network_from_reqwest)?;
        let status = res.status();
        let headers = res.headers().clone();
        let body: Value = res.json().unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&body).as_deref(),
                parse_retry_after(&headers),
            ));
        }
        Ok(body)
    }

    fn renew_lease(&self, session_id: &str, device_id: &str) -> Result<(), VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/lease/renew",
            self.base_url, session_id
        );
        let res = self
            .http
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&serde_json::json!({ "deviceId": device_id }))
            .send()
            .map_err(network_from_reqwest)?;
        let status = res.status();
        let headers = res.headers().clone();
        if !status.is_success() {
            let body: Value = res.json().unwrap_or(Value::Null);
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&body).as_deref(),
                parse_retry_after(&headers),
            ));
        }
        Ok(())
    }

    fn fetch_manifest(
        &self,
        session_id: &str,
    ) -> Result<LocalFinalizationManifest, VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/manifest",
            self.base_url, session_id
        );
        let res = self
            .http
            .get(url)
            .header("Authorization", self.auth_header())
            .send()
            .map_err(network_from_reqwest)?;
        let status = res.status();
        let headers = res.headers().clone();
        let body: Value = res.json().unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&body).as_deref(),
                parse_retry_after(&headers),
            ));
        }
        let manifest_raw = body
            .get("manifest")
            .ok_or_else(|| VoiceV2ClientError::Parse("manifest missing".into()))?;
        parse_local_finalization_manifest(manifest_raw)
            .map_err(|e| VoiceV2ClientError::Parse(e.to_string()))
    }

    fn fetch_segment_range(
        &self,
        session_id: &str,
        segment_seq: u32,
        offset: u64,
        length: u64,
        writer: &mut dyn Write,
    ) -> Result<StemRangeMeta, VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/segments/{}",
            self.base_url, session_id, segment_seq
        );
        let meta = self.fetch_ranged(&url, offset, length, writer)?;
        validate_stem_range_meta(&meta, offset, meta.total_length, length)?;
        Ok(meta)
    }

    fn post_receipt(
        &self,
        session_id: &str,
        receipt: &LocalFinalizationReceipt,
    ) -> Result<(), VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/receipt",
            self.base_url, session_id
        );
        let body = serde_json::json!({
            "version": receipt.version,
            "sessionId": receipt.session_id,
            "deliveryManifestDigest": receipt.delivery_manifest_digest,
            "finalizerAlgorithmVersion": receipt.finalizer_algorithm_version,
            "deviceId": receipt.device_id,
            "completedAtMs": receipt.completed_at_ms,
            "outputManifestDigest": receipt.output_manifest_digest,
            "stems": receipt.stems.iter().map(|s| serde_json::json!({
                "discordUserId": s.discord_user_id,
                "relativePath": s.relative_path,
                "byteLength": s.byte_length,
                "sha256": s.sha256,
            })).collect::<Vec<_>>(),
            "recoveryReport": receipt.recovery_report.as_ref().map(|r| serde_json::json!({
                "relativePath": r.relative_path,
                "byteLength": r.byte_length,
                "sha256": r.sha256,
            })),
            "discardedPacketCount": receipt.discarded_packet_count,
            "silenceRangeCount": receipt.silence_range_count,
        });
        let res = self
            .http
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&body)
            .send()
            .map_err(network_from_reqwest)?;
        let status = res.status();
        let headers = res.headers().clone();
        let parsed: Value = res.json().unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&parsed).as_deref(),
                parse_retry_after(&headers),
            ));
        }
        Ok(())
    }

    fn release_delivery(
        &self,
        session_id: &str,
        device_id: &str,
    ) -> Result<(), VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/local-v1/deliveries/{}/release",
            self.base_url, session_id
        );
        let res = self
            .http
            .post(url)
            .header("Authorization", self.auth_header())
            .json(&serde_json::json!({ "deviceId": device_id }))
            .send()
            .map_err(network_from_reqwest)?;
        let status = res.status();
        let headers = res.headers().clone();
        if !status.is_success() {
            let body: Value = res.json().unwrap_or(Value::Null);
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&body).as_deref(),
                parse_retry_after(&headers),
            ));
        }
        Ok(())
    }
}
