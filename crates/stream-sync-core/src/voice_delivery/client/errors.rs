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
            | VoiceV2ClientError::RangeMismatch(_) => VoiceV2ErrorClass::Terminal,
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
            message: message.into(),
            retry_after,
        }
    }
}
