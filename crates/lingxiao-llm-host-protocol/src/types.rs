use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub cache_creation_input_tokens: Option<u32>,
    pub cache_read_input_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelInfo {
    pub model_id: String,
    pub provider: String,
    pub context_limit: u32,
    pub output_limit: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestOptions {
    pub max_retries_hint: Option<u32>,
    pub timeout_ms_hint: Option<u64>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub stop: Option<Vec<String>>,
    pub metadata: Option<serde_json::Value>,
}

/// Resolved credential/auth material passed from Core to Provider Executor.
///
/// # Security contract
/// - `Debug` is safe: all secret fields are `<redacted>`.
/// - Serde **preserves** secrets: `AuthContext` is serialized across the
///   Core → Provider Executor boundary (process / sidecar IPC).  JSON output
///   is **transport-sensitive** — never log or persist the serialized form.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuthContext {
    ApiKey {
        provider: String,
        key: String,
    },
    BearerToken {
        provider: String,
        token: String,
    },
    AwsSignature {
        region: String,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
    },
    AzureToken {
        endpoint: String,
        deployment_id: String,
        api_version: String,
        api_key: String,
    },
    None,
}

impl fmt::Debug for AuthContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey { provider, .. } => f
                .debug_struct("AuthContext::ApiKey")
                .field("provider", provider)
                .field("key", &"<redacted>")
                .finish(),
            Self::BearerToken { provider, .. } => f
                .debug_struct("AuthContext::BearerToken")
                .field("provider", provider)
                .field("token", &"<redacted>")
                .finish(),
            Self::AwsSignature {
                region,
                session_token,
                ..
            } => f
                .debug_struct("AuthContext::AwsSignature")
                .field("region", region)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field(
                    "session_token",
                    &session_token.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
            Self::AzureToken {
                endpoint,
                deployment_id,
                api_version,
                ..
            } => f
                .debug_struct("AuthContext::AzureToken")
                .field("endpoint", endpoint)
                .field("deployment_id", deployment_id)
                .field("api_version", api_version)
                .field("api_key", &"<redacted>")
                .finish(),
            Self::None => f.debug_struct("AuthContext::None").finish(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_context_api_key_debug_redacted() {
        let ctx = AuthContext::ApiKey {
            provider: "openai".into(),
            key: "sk-secret-123".into(),
        };
        let debug_str = format!("{:?}", ctx);
        assert!(
            !debug_str.contains("sk-secret-123"),
            "Debug should not leak secret"
        );
        assert!(
            debug_str.contains("<redacted>"),
            "Debug should show redacted"
        );
        assert!(debug_str.contains("openai"), "Debug should show provider");
    }

    #[test]
    fn test_auth_context_bearer_token_debug_redacted() {
        let ctx = AuthContext::BearerToken {
            provider: "anthropic".into(),
            token: "tk-anthropic-secret".into(),
        };
        let debug_str = format!("{:?}", ctx);
        assert!(!debug_str.contains("tk-anthropic-secret"));
        assert!(debug_str.contains("<redacted>"));
    }

    #[test]
    fn test_auth_context_aws_debug_redacted() {
        let ctx = AuthContext::AwsSignature {
            region: "us-east-1".into(),
            access_key_id: "AKID123".into(),
            secret_access_key: "secret-key".into(),
            session_token: Some("session-token".into()),
        };
        let debug_str = format!("{:?}", ctx);
        assert!(!debug_str.contains("AKID123"));
        assert!(!debug_str.contains("secret-key"));
        assert!(!debug_str.contains("session-token"));
        assert!(debug_str.contains("<redacted>"));
    }

    #[test]
    fn test_auth_context_clone_roundtrip() {
        let ctx = AuthContext::ApiKey {
            provider: "openai".into(),
            key: "sk-test".into(),
        };
        let cloned = ctx.clone();
        match cloned {
            AuthContext::ApiKey { provider, .. } => {
                assert_eq!(provider, "openai");
            }
            _ => panic!("Expected ApiKey variant"),
        }
    }

    #[test]
    fn test_auth_context_serde_roundtrip_preserves_secret() {
        // Serde round-trip MUST preserve secrets because AuthContext is the
        // Core → Provider Executor handoff (process / sidecar IPC boundary).
        let ctx = AuthContext::ApiKey {
            provider: "openai".into(),
            key: "sk-secret-456".into(),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(
            json.contains("sk-secret-456"),
            "JSON must carry secret for provider executor handoff"
        );
        let back: AuthContext = serde_json::from_str(&json).unwrap();
        match &back {
            AuthContext::ApiKey { provider, key } => {
                assert_eq!(provider, "openai");
                assert_eq!(
                    key, "sk-secret-456",
                    "Serde round-trip must preserve secret material"
                );
            }
            _ => panic!("Expected ApiKey variant"),
        }
    }

    #[test]
    fn test_auth_context_bearer_serde_roundtrip() {
        let ctx = AuthContext::BearerToken {
            provider: "anthropic".into(),
            token: "sk-ant-secret".into(),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("sk-ant-secret"));
        let back: AuthContext = serde_json::from_str(&json).unwrap();
        match &back {
            AuthContext::BearerToken { provider, token } => {
                assert_eq!(provider, "anthropic");
                assert_eq!(token, "sk-ant-secret");
            }
            _ => panic!("Expected BearerToken variant"),
        }
    }

    #[test]
    fn test_auth_context_aws_serde_roundtrip() {
        let ctx = AuthContext::AwsSignature {
            region: "us-east-1".into(),
            access_key_id: "AKID123".into(),
            secret_access_key: "s3kr1t".into(),
            session_token: Some("tok".into()),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("AKID123"));
        assert!(json.contains("s3kr1t"));
        assert!(json.contains("tok"));
        let back: AuthContext = serde_json::from_str(&json).unwrap();
        match &back {
            AuthContext::AwsSignature {
                region,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                assert_eq!(region, "us-east-1");
                assert_eq!(access_key_id, "AKID123");
                assert_eq!(secret_access_key, "s3kr1t");
                assert_eq!(session_token.as_deref(), Some("tok"));
            }
            _ => panic!("Expected AwsSignature variant"),
        }
    }

    #[test]
    fn test_auth_context_azure_serde_roundtrip() {
        let ctx = AuthContext::AzureToken {
            endpoint: "https://my-openai.openai.azure.com".into(),
            deployment_id: "gpt-4".into(),
            api_version: "2024-02-01".into(),
            api_key: "az-key-789".into(),
        };
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("az-key-789"));
        let back: AuthContext = serde_json::from_str(&json).unwrap();
        match &back {
            AuthContext::AzureToken { api_key, .. } => {
                assert_eq!(api_key, "az-key-789");
            }
            _ => panic!("Expected AzureToken variant"),
        }
    }

    #[test]
    fn test_token_usage_defaults() {
        let usage = TokenUsage {
            prompt_tokens: 10,
            completion_tokens: 20,
            total_tokens: 30,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_tokens: None,
        };
        assert_eq!(usage.total_tokens, 30);
    }

    #[test]
    fn test_request_options_default() {
        let opts = RequestOptions::default();
        assert!(opts.max_tokens.is_none());
        assert!(opts.temperature.is_none());
    }
}
