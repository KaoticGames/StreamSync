use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoiceV2ErrorClass {
    Retryable,
    Terminal,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum VoiceV2ClientError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden binding")]
    ForbiddenBinding,
    #[error("delivery not found")]
    DeliveryNotFound,
    #[error("stem not found")]
    StemNotFound,
    #[error("invalid range")]
    InvalidRange,
    #[error("receipt conflict")]
    ReceiptConflict,
    #[error("identity conflict")]
    IdentityConflict,
    #[error("response parse: {0}")]
    Parse(String),
    #[error("range response mismatch: {0}")]
    RangeMismatch(String),
    #[error("insecure redirect: {0}")]
    InsecureRedirect(String),
    #[error("presigned stem access denied")]
    PresignDenied,
    #[error("network: {message}")]
    Network {
        message: String,
        retry_after: Option<std::time::Duration>,
    },
    #[error("retry after {0:?}")]
    RetryAfter(std::time::Duration),
}

impl VoiceV2ClientError {
    pub fn class(&self) -> VoiceV2ErrorClass {
        match self {
            VoiceV2ClientError::RetryAfter(_) | VoiceV2ClientError::Network { .. } => {
                VoiceV2ErrorClass::Retryable
            }
            VoiceV2ClientError::Unauthorized
            | VoiceV2ClientError::ForbiddenBinding
            | VoiceV2ClientError::DeliveryNotFound
            | VoiceV2ClientError::StemNotFound
            | VoiceV2ClientError::InvalidRange
            | VoiceV2ClientError::ReceiptConflict
            | VoiceV2ClientError::IdentityConflict
            | VoiceV2ClientError::Parse(_)
            | VoiceV2ClientError::RangeMismatch(_)
            | VoiceV2ClientError::InsecureRedirect(_) => VoiceV2ErrorClass::Terminal,
            VoiceV2ClientError::PresignDenied => VoiceV2ErrorClass::Retryable,
        }
    }

    pub fn retry_after(&self) -> Option<std::time::Duration> {
        match self {
            VoiceV2ClientError::RetryAfter(d) => Some(*d),
            VoiceV2ClientError::Network { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    pub fn network(message: impl Into<String>, retry_after: Option<std::time::Duration>) -> Self {
        VoiceV2ClientError::Network {
            message: sanitize_error_message(&message.into()),
            retry_after,
        }
    }
}

/// Strip URLs and presigned query material from reqwest or other error text before storage.
pub fn sanitize_error_message(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(i) = rest.find("http://").or_else(|| rest.find("https://")) {
        out.push_str(&rest[..i]);
        let url_tail = &rest[i..];
        let end = url_tail
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ')' | ']' | ','))
            .unwrap_or(url_tail.len());
        out.push_str("[redacted-url]");
        rest = &rest[i + end..];
    }
    out.push_str(rest);
    let out = redact_query_param(&out, "X-Amz-Signature");
    let out = redact_query_param(&out, "X-Amz-Credential");
    let out = redact_query_param(&out, "X-Amz-Token");
    redact_query_param(&out, "token")
}

fn redact_query_param(input: &str, name: &str) -> String {
    let needle = format!("{name}=");
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(i) = rest.to_ascii_lowercase().find(&needle.to_ascii_lowercase()) {
        out.push_str(&rest[..i]);
        let value_start = i + needle.len();
        let value_tail = &rest[value_start..];
        let value_end = value_tail
            .find(|c: char| c.is_whitespace() || matches!(c, '&' | '"' | '\'' | ')' | ']' | ','))
            .unwrap_or(value_tail.len());
        out.push_str(&format!("{name}=[redacted]"));
        rest = &rest[value_start + value_end..];
        if rest.starts_with('&') {
            out.push('&');
            rest = &rest[1..];
        }
    }
    out.push_str(rest);
    out
}

pub fn network_from_reqwest(e: reqwest::Error) -> VoiceV2ClientError {
    VoiceV2ClientError::network(e.without_url().to_string(), None)
}

pub fn parse_from_reqwest(e: reqwest::Error) -> VoiceV2ClientError {
    VoiceV2ClientError::Parse(sanitize_error_message(&e.without_url().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_strips_presigned_query_and_urls() {
        let raw = "GET http://127.0.0.1:9/x?X-Amz-Signature=TOPSECRET&token=sectok failed";
        let s = sanitize_error_message(raw);
        assert!(!s.contains("TOPSECRET"));
        assert!(!s.contains("sectok"));
        assert!(!s.contains("127.0.0.1"));
        assert!(s.contains("[redacted-url]"));
    }

    #[test]
    fn network_constructor_sanitizes_message() {
        let err =
            VoiceV2ClientError::network("error for https://host/path?X-Amz-Credential=AKIA", None);
        let text = err.to_string();
        assert!(!text.contains("AKIA"));
        assert!(!text.contains("https://"));
    }
}
