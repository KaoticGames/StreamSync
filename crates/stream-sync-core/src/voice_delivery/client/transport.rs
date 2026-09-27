use super::errors::{VoiceV2ClientError, VoiceV2ErrorClass};
use crate::voice_delivery::finalized_manifest::{
    compute_finalized_manifest_digest, parse_syndicate_finalized_manifest,
    SyndicateFinalizedManifest,
};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub session_id: String,
    pub manifest_digest: String,
    pub sealed_at: String,
    pub manifest: SyndicateFinalizedManifest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptRequestBody {
    pub manifest_digest: String,
    pub device_id: String,
    pub local_receipt_id: String,
    pub local_publication_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StemRangeResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_range: Option<String>,
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub total_length: Option<u64>,
}

pub trait VoiceV2Client: Send + Sync {
    fn fetch_pending(&self, limit: u32) -> Result<Vec<PendingDelivery>, VoiceV2ClientError>;
    fn fetch_stem_range(
        &self,
        session_id: &str,
        stem_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<StemRangeResponse, VoiceV2ClientError>;
    fn post_receipt(
        &self,
        session_id: &str,
        body: &ReceiptRequestBody,
    ) -> Result<(), VoiceV2ClientError>;
}

pub fn parse_pending_response(body: &Value) -> Result<Vec<PendingDelivery>, VoiceV2ClientError> {
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
        for key in obj.keys() {
            if !matches!(
                key.as_str(),
                "sessionId" | "manifestDigest" | "sealedAt" | "manifest"
            ) {
                return Err(VoiceV2ClientError::Parse(format!("unknown key {key}")));
            }
        }
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
        let manifest = parse_syndicate_finalized_manifest(manifest_raw)
            .map_err(|e| VoiceV2ClientError::Parse(e.to_string()))?;
        if manifest.session_id != session_id {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        let computed = compute_finalized_manifest_digest(&manifest);
        if computed != manifest_digest {
            return Err(VoiceV2ClientError::IdentityConflict);
        }
        out.push(PendingDelivery {
            session_id,
            manifest_digest,
            sealed_at,
            manifest,
        });
    }
    out.sort_by(|a, b| {
        use std::cmp::Ordering;
        match a.sealed_at.cmp(&b.sealed_at) {
            Ordering::Equal => a.session_id.cmp(&b.session_id),
            other => other,
        }
    });
    Ok(out)
}

pub fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())?;
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(Duration::from_secs(secs.min(60)));
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(raw) {
        let now = Utc::now();
        let target = dt.with_timezone(&Utc);
        if target > now {
            let secs = (target - now).num_seconds().max(0) as u64;
            return Some(Duration::from_secs(secs.min(60)));
        }
    }
    None
}

pub fn map_http_status(status: u16, code: Option<&str>) -> VoiceV2ClientError {
    match status {
        401 => VoiceV2ClientError::Unauthorized,
        403 => VoiceV2ClientError::ForbiddenBinding,
        404 => VoiceV2ClientError::DeliveryNotFound,
        409 if code == Some("receipt_conflict") => VoiceV2ClientError::ReceiptConflict,
        409 if code == Some("identity_conflict") => VoiceV2ClientError::IdentityConflict,
        416 => VoiceV2ClientError::InvalidRange,
        429 | 503 | 500 | 502 | 504 => VoiceV2ClientError::Network(format!("HTTP {status}")),
        _ => VoiceV2ClientError::Network(format!("HTTP {status}")),
    }
}

pub fn parse_error_code(body: &Value) -> Option<String> {
    body.get("code")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub fn validate_stem_range_response(
    resp: &StemRangeResponse,
    offset: u64,
    expected_total: u64,
    expected_sha: &str,
) -> Result<(), VoiceV2ClientError> {
    if resp.status != 206 {
        return Err(VoiceV2ClientError::RangeMismatch(format!(
            "expected 206 got {}",
            resp.status
        )));
    }
    let len = resp.body.len() as u64;
    if resp.content_length != Some(len) {
        return Err(VoiceV2ClientError::RangeMismatch("content-length".into()));
    }
    let end = offset + len - 1;
    let cr = resp
        .content_range
        .as_deref()
        .ok_or_else(|| VoiceV2ClientError::RangeMismatch("content-range missing".into()))?;
    let expected_cr = format!("bytes {offset}-{end}/{expected_total}");
    if cr != expected_cr {
        return Err(VoiceV2ClientError::RangeMismatch(format!(
            "content-range {cr} != {expected_cr}"
        )));
    }
    if resp.total_length != Some(expected_total) {
        return Err(VoiceV2ClientError::RangeMismatch("total length".into()));
    }
    let etag = resp
        .etag
        .as_deref()
        .ok_or_else(|| VoiceV2ClientError::RangeMismatch("etag missing".into()))?;
    let quoted = format!("\"{expected_sha}\"");
    if etag != quoted {
        return Err(VoiceV2ClientError::RangeMismatch("etag mismatch".into()));
    }
    Ok(())
}

pub struct HttpVoiceV2Client {
    base_url: String,
    bearer: String,
    http: reqwest::blocking::Client,
}

impl HttpVoiceV2Client {
    pub fn new(base_url: impl Into<String>, bearer: impl Into<String>) -> Self {
        let http = reqwest::blocking::Client::builder()
            .timeout(crate::delegated_lifecycle::SYNDICATE_HTTP_TIMEOUT)
            .build()
            .expect("voice v2 blocking HTTP client");
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            bearer: bearer.into(),
            http,
        }
    }

    fn auth_header(&self) -> String {
        crate::delegated_lifecycle::connection_key_authorization(&self.bearer)
    }
}

impl VoiceV2Client for HttpVoiceV2Client {
    fn fetch_pending(&self, limit: u32) -> Result<Vec<PendingDelivery>, VoiceV2ClientError> {
        let url = format!("{}{}?limit={}", self.base_url, super::PENDING_PATH, limit);
        let res = self
            .http
            .get(url)
            .header("Authorization", self.auth_header())
            .header("Accept", "application/json")
            .send()
            .map_err(|e| VoiceV2ClientError::Network(e.to_string()))?;
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
            ));
        }
        parse_pending_response(&body)
    }

    fn fetch_stem_range(
        &self,
        session_id: &str,
        stem_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<StemRangeResponse, VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/v2/deliveries/{}/stems/{}?offset={}&length={}",
            self.base_url, session_id, stem_id, offset, length
        );
        let res = self
            .http
            .get(url)
            .header("Authorization", self.auth_header())
            .send()
            .map_err(|e| VoiceV2ClientError::Network(e.to_string()))?;
        if let Some(wait) = parse_retry_after(res.headers()) {
            return Err(VoiceV2ClientError::RetryAfter(wait));
        }
        let status = res.status().as_u16();
        if status == 404 {
            return Err(VoiceV2ClientError::StemNotFound);
        }
        if !res.status().is_success() && status != 206 {
            let body: Value = res.json().unwrap_or(Value::Null);
            return Err(map_http_status(status, parse_error_code(&body).as_deref()));
        }
        let content_range = res
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let content_length = res
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok());
        let etag = res
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let total_length = content_range
            .as_deref()
            .and_then(|cr| cr.rsplit('/').next())
            .and_then(|s| s.parse().ok());
        let body = res
            .bytes()
            .map_err(|e| VoiceV2ClientError::Network(e.to_string()))?
            .to_vec();
        Ok(StemRangeResponse {
            status,
            body,
            content_range,
            content_length,
            etag,
            total_length,
        })
    }

    fn post_receipt(
        &self,
        session_id: &str,
        body: &ReceiptRequestBody,
    ) -> Result<(), VoiceV2ClientError> {
        let url = format!(
            "{}/api/stream-sync/voice/v2/deliveries/{}/receipt",
            self.base_url, session_id
        );
        let res = self
            .http
            .post(url)
            .header("Authorization", self.auth_header())
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "manifestDigest": body.manifest_digest,
                "deviceId": body.device_id,
                "localReceiptId": body.local_receipt_id,
                "localPublicationState": body.local_publication_state,
            }))
            .send()
            .map_err(|e| VoiceV2ClientError::Network(e.to_string()))?;
        if let Some(wait) = parse_retry_after(res.headers()) {
            return Err(VoiceV2ClientError::RetryAfter(wait));
        }
        let status = res.status();
        let parsed: Value = res.json().unwrap_or(Value::Null);
        if status == 409 {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&parsed).as_deref(),
            ));
        }
        if !status.is_success() {
            return Err(map_http_status(
                status.as_u16(),
                parse_error_code(&parsed).as_deref(),
            ));
        }
        if parsed.get("ok").and_then(|v| v.as_bool()) != Some(true) {
            return Err(VoiceV2ClientError::Parse("receipt ok:false".into()));
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct MockVoiceV2Client {
    inner: Arc<Mutex<MockVoiceV2State>>,
}

#[derive(Default)]
struct MockVoiceV2State {
    pending: Vec<PendingDelivery>,
    stems: HashMap<(String, String), Vec<u8>>,
    receipt_calls: usize,
    fail_next_range: bool,
    retry_after_next: bool,
}

impl MockVoiceV2Client {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pending(&self, pending: Vec<PendingDelivery>) {
        self.inner.lock().unwrap().pending = pending;
    }

    pub fn set_stem_bytes(&self, session_id: &str, stem_id: &str, bytes: Vec<u8>) {
        self.inner
            .lock()
            .unwrap()
            .stems
            .insert((session_id.to_string(), stem_id.to_string()), bytes);
    }

    pub fn receipt_calls(&self) -> usize {
        self.inner.lock().unwrap().receipt_calls
    }

    pub fn set_fail_next_range(&self, fail: bool) {
        self.inner.lock().unwrap().fail_next_range = fail;
    }
}

impl VoiceV2Client for MockVoiceV2Client {
    fn fetch_pending(&self, limit: u32) -> Result<Vec<PendingDelivery>, VoiceV2ClientError> {
        let state = self.inner.lock().unwrap();
        if state.retry_after_next {
            return Err(VoiceV2ClientError::RetryAfter(Duration::from_secs(2)));
        }
        Ok(state.pending.iter().take(limit as usize).cloned().collect())
    }

    fn fetch_stem_range(
        &self,
        session_id: &str,
        stem_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<StemRangeResponse, VoiceV2ClientError> {
        let state = self.inner.lock().unwrap();
        if state.fail_next_range {
            return Err(VoiceV2ClientError::RangeMismatch("injected".into()));
        }
        let key = (session_id.to_string(), stem_id.to_string());
        let file = state
            .stems
            .get(&key)
            .ok_or(VoiceV2ClientError::StemNotFound)?;
        if offset >= file.len() as u64 {
            return Err(VoiceV2ClientError::InvalidRange);
        }
        let end = (offset + length).min(file.len() as u64);
        let slice = file[offset as usize..end as usize].to_vec();
        let slice_len = slice.len() as u64;
        let total = file.len() as u64;
        let end_inclusive = offset + slice_len - 1;
        use sha2::Digest;
        let etag = format!(
            "\"{}\"",
            crate::voice_delivery::hash::hex_digest(&sha2::Sha256::digest(file.as_slice()))
        );
        Ok(StemRangeResponse {
            status: 206,
            body: slice,
            content_range: Some(format!("bytes {offset}-{end_inclusive}/{total}")),
            content_length: Some(slice_len),
            etag: Some(etag),
            total_length: Some(total),
        })
    }

    fn post_receipt(
        &self,
        session_id: &str,
        _body: &ReceiptRequestBody,
    ) -> Result<(), VoiceV2ClientError> {
        let mut state = self.inner.lock().unwrap();
        state.receipt_calls += 1;
        if session_id.is_empty() {
            return Err(VoiceV2ClientError::DeliveryNotFound);
        }
        Ok(())
    }
}

#[cfg(test)]
mod client_parse_tests {
    use super::*;
    use crate::voice_delivery::finalized_manifest::syndicate_manifest_tests::minimal_wav_value;

    #[test]
    fn pending_requires_manifest_and_digest_match() {
        let m = minimal_wav_value();
        let digest =
            compute_finalized_manifest_digest(&parse_syndicate_finalized_manifest(&m).unwrap());
        let body = serde_json::json!({
            "ok": true,
            "deliveries": [{
                "sessionId": "550e8400-e29b-41d4-a716-446655440000",
                "manifestDigest": digest,
                "sealedAt": "2020-01-01T00:00:00.000Z",
                "manifest": m,
            }],
        });
        let parsed = parse_pending_response(&body).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn unauthorized_maps_terminal() {
        assert_eq!(
            VoiceV2ClientError::Unauthorized.class(),
            VoiceV2ErrorClass::Terminal
        );
    }
}
