use crate::types::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type ProviderId = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<crate::ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolDefinition>,
    pub stream: bool,
    pub auth_context: AuthContext,
    pub options: RequestOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateResponse {
    pub content: String,
    pub finish_reason: String,
    pub usage: TokenUsage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderErrorCode {
    Authentication,
    RateLimited,
    ContextOverflow,
    BadRequest,
    ServerError,
    Timeout,
    StreamInterrupted,
    UnsupportedModel,
    InvalidToolDefinition,
    ContentFiltered,
    CircuitOpen,
    Unknown,
}

impl std::fmt::Display for ProviderErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authentication => write!(f, "authentication"),
            Self::RateLimited => write!(f, "rate_limited"),
            Self::ContextOverflow => write!(f, "context_overflow"),
            Self::BadRequest => write!(f, "bad_request"),
            Self::ServerError => write!(f, "server_error"),
            Self::Timeout => write!(f, "timeout"),
            Self::StreamInterrupted => write!(f, "stream_interrupted"),
            Self::UnsupportedModel => write!(f, "unsupported_model"),
            Self::InvalidToolDefinition => write!(f, "invalid_tool_definition"),
            Self::ContentFiltered => write!(f, "content_filtered"),
            Self::CircuitOpen => write!(f, "circuit_open"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
#[error("ProviderError(code={code}, message={message})")]
pub struct ProviderError {
    pub code: ProviderErrorCode,
    pub message: String,
    pub retryable: bool,
}

impl ProviderError {
    pub fn new(code: ProviderErrorCode, message: impl Into<String>) -> Self {
        let retryable = matches!(
            code,
            ProviderErrorCode::RateLimited
                | ProviderErrorCode::ServerError
                | ProviderErrorCode::Timeout
                | ProviderErrorCode::StreamInterrupted
        );
        Self {
            code,
            message: message.into(),
            retryable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_error_retryable_codes() {
        let err = ProviderError::new(ProviderErrorCode::RateLimited, "too many");
        assert!(err.retryable);

        let err = ProviderError::new(ProviderErrorCode::ServerError, "500");
        assert!(err.retryable);

        let err = ProviderError::new(ProviderErrorCode::Authentication, "unauthorized");
        assert!(!err.retryable);
    }

    #[test]
    fn test_provider_error_display() {
        let err = ProviderError::new(ProviderErrorCode::BadRequest, "invalid model");
        let display = format!("{}", err);
        assert!(display.contains("bad_request"));
        assert!(display.contains("invalid model"));
    }

    #[test]
    fn test_provider_error_serde_roundtrip() {
        let err = ProviderError::new(ProviderErrorCode::Timeout, "request timed out");
        let json = serde_json::to_string(&err).unwrap();
        let back: ProviderError = serde_json::from_str(&json).unwrap();
        assert_eq!(back.code, ProviderErrorCode::Timeout);
        assert_eq!(back.message, "request timed out");
        assert!(back.retryable);
    }

    #[test]
    fn test_generate_request_serde_roundtrip() {
        let req = GenerateRequest {
            model: "gpt-4".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                tool_call_id: None,
                tool_calls: Vec::new(),
                name: None,
            }],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: GenerateRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.model, "gpt-4");
        assert!(back.stream);
    }
}
