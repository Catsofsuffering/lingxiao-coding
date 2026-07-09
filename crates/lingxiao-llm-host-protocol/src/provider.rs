use crate::types::*;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use thiserror::Error;

pub type ProviderId = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImageUrlContentPart {
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImageBlobRefContentPart {
    pub blob_id: String,
    pub mime: String,
    pub size: u64,
    pub blob_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContentPart {
    Text {
        text: String,
    },
    Thinking {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    RedactedThinking {
        data: String,
    },
    ImageUrl {
        image_url: ImageUrlContentPart,
    },
    ImageBlobRef {
        #[serde(flatten)]
        image: ImageBlobRefContentPart,
    },
}

impl MessageContentPart {
    pub fn plain_text(&self) -> String {
        match self {
            Self::Text { text } | Self::Thinking { text, .. } => text.clone(),
            Self::RedactedThinking { .. } => "[redacted thinking]".to_string(),
            Self::ImageUrl { .. } => "[image]".to_string(),
            Self::ImageBlobRef { image } => {
                let kb = image.size.max(1).div_ceil(1024);
                let short_id = image.blob_id.chars().take(12).collect::<String>();
                format!(
                    "[image: {}, {}KB stored as blob:{}]",
                    image.mime, kb, short_id
                )
            }
        }
    }
}

pub fn content_parts_to_plain_text(parts: &[MessageContentPart]) -> String {
    parts
        .iter()
        .map(MessageContentPart::plain_text)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Read an `image_blob_ref`'s stored file from disk and re-encode it as a
/// `data:<mime>;base64,...` URI so a provider can receive the real image
/// bytes instead of a textual placeholder. Returns `None` when the blob file
/// is missing or unreadable — callers fall back to the placeholder text.
///
/// Mirrors TS `rehydrateImageBlobRef` in `src/llm/image_blob_store.ts`.
pub fn rehydrate_image_blob_ref(image: &ImageBlobRefContentPart) -> Option<String> {
    use std::fs;
    let bytes = fs::read(&image.blob_path).ok()?;
    let encoded = base64_encode(&bytes);
    Some(format!("data:{};base64,{}", image.mime, encoded))
}

/// RFC 4648 standard base64 encoder (no external dependency on the protocol
/// crate, which must stay lightweight). Used only for image-blob rehydration.
fn base64_encode(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((triple >> 18) & 0x3F) as usize] as char);
        out.push(TABLE[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(TABLE[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// Rehydrate an `image_blob_ref` only when it falls inside the recent-rounds
/// retain window. When `eligible` is `false` (the blob belongs to an older
/// round beyond `DEFAULT_RETAIN_IMAGE_ROUNDS`), this returns `None` so callers
/// degrade to the safe `plain_text()` placeholder instead of sending the full
/// image bytes — controlling token/context cost the way TS
/// `rehydrateRecentImageBlobRefs` does. When `eligible` is `true` this is
/// equivalent to [`rehydrate_image_blob_ref`].
pub fn rehydrate_image_blob_ref_if(
    image: &ImageBlobRefContentPart,
    eligible: bool,
) -> Option<String> {
    if eligible {
        rehydrate_image_blob_ref(image)
    } else {
        None
    }
}

/// Default number of trailing user rounds whose `image_blob_ref` parts are
/// rehydrated to real image bytes. Mirrors TS
/// `DEFAULT_RETAIN_IMAGE_ROUNDS` (`src/llm/image_blob_store.ts`). Older blobs
/// degrade to the safe text placeholder to bound token/context cost.
pub const DEFAULT_RETAIN_IMAGE_ROUNDS: usize = 2;

/// Normalize a configured retain-rounds value the way TS
/// `normalizeImageRetainRounds` does: non-finite/`None` → `fallback` (default
/// 2), otherwise the floored value clamped to a minimum of 1. Accepts the raw
/// `Option<f64>` read from request metadata so providers can pass
/// `metadata["image_history_retain_rounds"]` through verbatim.
pub fn normalize_image_retain_rounds(value: Option<f64>, fallback: usize) -> usize {
    match value {
        Some(v) if v.is_finite() => v.floor().max(1.0) as usize,
        _ => fallback,
    }
}

/// Resolve the retain-window `cutoffIndex` for a message list, mirroring TS
/// `rehydrateRecentImageBlobRefs`: scan from the end counting `user` messages,
/// and the moment the count reaches `retain_rounds`, record that message's
/// index. Messages at index `>= cutoff_index` are eligible for blob
/// rehydration; older messages degrade to the placeholder. When fewer than
/// `retain_rounds` user rounds exist, the window covers every message
/// (`cutoff_index == 0`) so no image is silently dropped — matching TS, where
/// the initial `cutoffIndex = 0` is left in place when the loop never breaks.
///
/// Rust has no per-message round id, so "round" is approximated by `user`
/// message position from the end of the sequence (the same approximation TS
/// uses, since it too counts `role === 'user'` messages). This is an exact
/// match for the TS semantics over the same message array; the only divergence
/// is that Rust counts at the provider projection point rather than a separate
/// pre-projection pass (see the parity matrix R-5 note).
pub fn image_retain_cutoff(messages: &[Message], retain_rounds: usize) -> usize {
    let retain = retain_rounds.max(1);
    let mut user_rounds = 0usize;
    for (index, message) in messages.iter().enumerate().rev() {
        if message.role == "user" {
            user_rounds += 1;
            if user_rounds >= retain {
                return index;
            }
        }
    }
    0
}

/// True when the message at `index` falls inside the retain window and its
/// `image_blob_ref` parts should be rehydrated to real image bytes. Convenience
/// over [`image_retain_cutoff`] for per-message provider loops.
pub fn message_rehydrates_blob_at(
    messages: &[Message],
    index: usize,
    retain_rounds: usize,
) -> bool {
    index >= image_retain_cutoff(messages, retain_rounds)
}

/// Read `image_history_retain_rounds` from request `metadata` (the Rust analog
/// of TS `advanced.image_history_retain_rounds`), normalizing to a minimum-1
/// `usize` with [`DEFAULT_RETAIN_IMAGE_ROUNDS`] as the fallback. Returns the
/// default when `metadata` is absent or the key is missing/non-numeric.
pub fn retain_rounds_from_metadata(metadata: Option<&serde_json::Value>) -> usize {
    let value = metadata
        .and_then(|m| m.get("image_history_retain_rounds"))
        .and_then(|v| v.as_f64());
    normalize_image_retain_rounds(value, DEFAULT_RETAIN_IMAGE_ROUNDS)
}

/// An image-bearing content part resolved to a sendable URL (a real `http(s)`
/// URL, a `data:` URI from an `image_url` part, or a rehydrated `data:` URI
/// from a stored blob) plus an optional detail hint. Providers that support
/// vision consume this; providers that do not fall back to plain text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedImageContent {
    pub url: String,
    pub detail: Option<String>,
}

impl MessageContentPart {
    /// Returns the sendable image URL for `ImageUrl` and rehydratable
    /// `ImageBlobRef` parts. `None` for text/thinking parts or blobs whose
    /// backing file is missing.
    pub fn resolved_image_url(&self) -> Option<ResolvedImageContent> {
        match self {
            Self::ImageUrl { image_url } => Some(ResolvedImageContent {
                url: image_url.url.clone(),
                detail: image_url.detail.clone(),
            }),
            Self::ImageBlobRef { image } => rehydrate_image_blob_ref(image)
                .map(|url| ResolvedImageContent { url, detail: None }),
            _ => None,
        }
    }

    /// True when this part carries an image (URL or blob ref), regardless of
    /// whether the blob file is currently rehydratable.
    pub fn is_image(&self) -> bool {
        matches!(self, Self::ImageUrl { .. } | Self::ImageBlobRef { .. })
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Message {
    pub role: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub content_parts: Vec<MessageContentPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<crate::ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl Message {
    pub fn plain_text_content(&self) -> String {
        if self.content_parts.is_empty() {
            self.content.clone()
        } else {
            content_parts_to_plain_text(&self.content_parts)
        }
    }

    pub fn has_structured_content(&self) -> bool {
        !self.content_parts.is_empty()
    }

    /// True when this message carries at least one image part (URL or blob
    /// ref). Providers use this to decide whether to emit a multimodal
    /// content array instead of flattening to plain text.
    pub fn has_image_content(&self) -> bool {
        self.content_parts.iter().any(MessageContentPart::is_image)
    }

    /// Project this message's `content_parts` into OpenAI Chat-Completions
    /// content-part JSON values (`{type:"text",text}` and
    /// `{type:"image_url",image_url:{url,detail}}`).
    ///
    /// Image parts are rehydrated from disk when they are blob refs; a blob
    /// whose backing file is missing degrades to a textual placeholder so the
    /// provider still receives an explanation instead of silently dropping the
    /// image. Non-image parts (thinking/redacted_thinking) are emitted as text
    /// because the Chat-Completions user role does not carry a thinking part.
    ///
    /// Returns `None` when the message has no structured content, so callers
    /// can fall back to `plain_text_content()`.
    ///
    /// Equivalent to [`Self::openai_chat_content_parts_with_rehydrate`] with
    /// `rehydrate = true` (always rehydrate blob refs). Provider loops that
    /// enforce the retain-rounds window pass `rehydrate` per message instead.
    pub fn openai_chat_content_parts(&self) -> Option<Vec<Value>> {
        self.openai_chat_content_parts_with_rehydrate(true)
    }

    /// Same projection as [`Self::openai_chat_content_parts`], but blob-ref
    /// rehydration is gated by `rehydrate`. When `rehydrate` is `false` the
    /// blob degrades to the safe `plain_text()` placeholder without touching
    /// disk, so older rounds outside the retain window never send full image
    /// bytes. `image_url` parts are always emitted (they are not blob refs).
    pub fn openai_chat_content_parts_with_rehydrate(&self, rehydrate: bool) -> Option<Vec<Value>> {
        if self.content_parts.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(self.content_parts.len());
        for part in &self.content_parts {
            match part {
                MessageContentPart::Text { text } => {
                    parts.push(json!({"type": "text", "text": text}));
                }
                MessageContentPart::Thinking { text, .. } => {
                    if !text.is_empty() {
                        parts.push(json!({"type": "text", "text": text}));
                    }
                }
                MessageContentPart::RedactedThinking { .. } => {
                    parts.push(json!({"type": "text", "text": "[redacted thinking]"}));
                }
                MessageContentPart::ImageUrl { image_url } => {
                    let mut image = json!({"url": image_url.url});
                    if let Some(detail) = &image_url.detail {
                        image["detail"] = Value::String(detail.clone());
                    }
                    parts.push(json!({"type": "image_url", "image_url": image}));
                }
                MessageContentPart::ImageBlobRef { image } => {
                    match rehydrate_image_blob_ref_if(image, rehydrate) {
                        Some(url) => {
                            parts.push(json!({"type": "image_url", "image_url": {"url": url}}));
                        }
                        None => parts.push(json!({"type": "text", "text": part.plain_text()})),
                    }
                }
            }
        }
        Some(parts)
    }

    /// Project this message's `content_parts` into OpenAI Responses-API
    /// input-part JSON values (`{type:"input_text",text}` and
    /// `{type:"input_image",image_url}`). Rehydration and fallback semantics
    /// match `openai_chat_content_parts`.
    ///
    /// Equivalent to [`Self::openai_responses_content_parts_with_rehydrate`]
    /// with `rehydrate = true`.
    pub fn openai_responses_content_parts(&self) -> Option<Vec<Value>> {
        self.openai_responses_content_parts_with_rehydrate(true)
    }

    /// Same projection as [`Self::openai_responses_content_parts`], but
    /// blob-ref rehydration is gated by `rehydrate`; see
    /// [`Self::openai_chat_content_parts_with_rehydrate`].
    pub fn openai_responses_content_parts_with_rehydrate(
        &self,
        rehydrate: bool,
    ) -> Option<Vec<Value>> {
        if self.content_parts.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(self.content_parts.len());
        for part in &self.content_parts {
            match part {
                MessageContentPart::Text { text } => {
                    parts.push(json!({"type": "input_text", "text": text}));
                }
                MessageContentPart::Thinking { text, .. } => {
                    if !text.is_empty() {
                        parts.push(json!({"type": "input_text", "text": text}));
                    }
                }
                MessageContentPart::RedactedThinking { .. } => {
                    parts.push(json!({"type": "input_text", "text": "[redacted thinking]"}));
                }
                MessageContentPart::ImageUrl { image_url } => {
                    parts.push(json!({"type": "input_image", "image_url": image_url.url}));
                }
                MessageContentPart::ImageBlobRef { image } => {
                    match rehydrate_image_blob_ref_if(image, rehydrate) {
                        Some(url) => {
                            parts.push(json!({"type": "input_image", "image_url": url}));
                        }
                        None => {
                            parts.push(json!({"type": "input_text", "text": part.plain_text()}))
                        }
                    }
                }
            }
        }
        Some(parts)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawMessage {
            role: String,
            #[serde(default)]
            content: Value,
            #[serde(default)]
            content_parts: Vec<MessageContentPart>,
            #[serde(default)]
            tool_call_id: Option<String>,
            #[serde(default)]
            tool_calls: Vec<crate::ToolCall>,
            #[serde(default)]
            name: Option<String>,
        }

        let mut raw = RawMessage::deserialize(deserializer)?;
        let content_parts = if raw.content_parts.is_empty() {
            content_value_to_parts(&raw.content)
        } else {
            std::mem::take(&mut raw.content_parts)
        };
        let content = content_value_to_plain_text(&raw.content, &content_parts);
        Ok(Self {
            role: raw.role,
            content,
            content_parts,
            tool_call_id: raw.tool_call_id,
            tool_calls: raw.tool_calls,
            name: raw.name,
        })
    }
}

fn content_value_to_parts(value: &Value) -> Vec<MessageContentPart> {
    let Some(parts) = value.as_array() else {
        return Vec::new();
    };
    parts
        .iter()
        .filter_map(|part| serde_json::from_value::<MessageContentPart>(part.clone()).ok())
        .collect()
}

fn content_value_to_plain_text(value: &Value, parts: &[MessageContentPart]) -> String {
    if !parts.is_empty() {
        return content_parts_to_plain_text(parts);
    }
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
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

impl GenerateRequest {
    /// True when any message in the request carries an image part (URL or blob
    /// ref). The routing layer uses this to gate providers by `supports_vision`
    /// so a non-vision model never receives an image content array (which would
    /// otherwise trigger a provider 400).
    pub fn needs_vision(&self) -> bool {
        self.messages.iter().any(Message::has_image_content)
    }
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
                content_parts: Vec::new(),
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

    #[test]
    fn test_message_deserializes_legacy_string_content() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "role": "user",
            "content": "hello"
        }))
        .unwrap();
        assert_eq!(message.content, "hello");
        assert_eq!(message.plain_text_content(), "hello");
        assert!(message.content_parts.is_empty());
    }

    #[test]
    fn test_message_deserializes_content_part_array() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "role": "user",
            "content": [
                {"type": "text", "text": "look"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,abc", "detail": "low"}},
                {
                    "type": "image_blob_ref",
                    "blob_id": "blob_1234567890abcdef",
                    "mime": "image/png",
                    "size": 2048,
                    "blob_path": "C:/secret/path/blob.png",
                    "source": "screenshot"
                }
            ]
        }))
        .unwrap();
        assert_eq!(message.content_parts.len(), 3);
        let text = message.plain_text_content();
        assert!(text.contains("look"));
        assert!(text.contains("[image]"));
        assert!(text.contains("blob_123456"));
        assert!(!text.contains("C:/secret/path"));
    }

    #[test]
    fn test_message_deserializes_explicit_content_parts() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "role": "assistant",
            "content": "",
            "content_parts": [
                {"type": "thinking", "text": "plan", "signature": "sig"},
                {"type": "redacted_thinking", "data": "opaque"}
            ]
        }))
        .unwrap();
        assert!(message.has_structured_content());
        assert_eq!(message.plain_text_content(), "plan\n[redacted thinking]");
    }

    #[test]
    fn test_base64_encode_matches_standard() {
        // Known vectors from RFC 4648.
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn test_rehydrate_image_blob_ref_reads_file_and_encodes_data_uri() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        // "foobar" -> base64 "Zm9vYmFy"
        std::fs::write(&path, b"foobar").unwrap();
        let image = ImageBlobRefContentPart {
            blob_id: "blob_x".into(),
            mime: "image/png".into(),
            size: 6,
            blob_path: path.display().to_string(),
            source: None,
        };
        let data_uri = rehydrate_image_blob_ref(&image).unwrap();
        assert_eq!(data_uri, "data:image/png;base64,Zm9vYmFy");
    }

    #[test]
    fn test_rehydrate_image_blob_ref_missing_file_returns_none() {
        let image = ImageBlobRefContentPart {
            blob_id: "blob_y".into(),
            mime: "image/png".into(),
            size: 6,
            blob_path: "/definitely/does/not/exist/blob_y.png".into(),
            source: None,
        };
        assert!(rehydrate_image_blob_ref(&image).is_none());
    }

    #[test]
    fn test_resolved_image_url_for_url_and_blob_parts() {
        let url_part = MessageContentPart::ImageUrl {
            image_url: ImageUrlContentPart {
                url: "https://example.test/x.png".into(),
                detail: Some("low".into()),
            },
        };
        let resolved = url_part.resolved_image_url().unwrap();
        assert_eq!(resolved.url, "https://example.test/x.png");
        assert_eq!(resolved.detail.as_deref(), Some("low"));

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.bin");
        std::fs::write(&path, b"foo").unwrap();
        let blob_part = MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_z".into(),
                mime: "image/jpeg".into(),
                size: 3,
                blob_path: path.display().to_string(),
                source: None,
            },
        };
        let resolved = blob_part.resolved_image_url().unwrap();
        assert_eq!(resolved.url, "data:image/jpeg;base64,Zm9v");
        assert!(blob_part.is_image());
        assert!(!MessageContentPart::Text { text: "x".into() }.is_image());
    }

    #[test]
    fn test_openai_chat_content_parts_emits_text_and_image_url() {
        let message = Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                MessageContentPart::Text { text: "hi".into() },
                MessageContentPart::ImageUrl {
                    image_url: ImageUrlContentPart {
                        url: "https://example.test/y.png".into(),
                        detail: Some("high".into()),
                    },
                },
            ],
            ..Default::default()
        };
        let parts = message.openai_chat_content_parts().unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "hi");
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["url"], "https://example.test/y.png");
        assert_eq!(parts[1]["image_url"]["detail"], "high");
    }

    #[test]
    fn test_openai_chat_content_parts_blob_falls_back_to_text_when_missing() {
        let message = Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![MessageContentPart::ImageBlobRef {
                image: ImageBlobRefContentPart {
                    blob_id: "blob_missing".into(),
                    mime: "image/png".into(),
                    size: 2048,
                    blob_path: "/no/such/blob.png".into(),
                    source: None,
                },
            }],
            ..Default::default()
        };
        let parts = message.openai_chat_content_parts().unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "text");
        let placeholder = parts[0]["text"].as_str().unwrap();
        assert!(placeholder.contains("blob_missing"));
        assert!(!placeholder.contains("/no/such/blob.png"));
    }

    #[test]
    fn test_openai_chat_content_parts_none_for_plain_message() {
        let message = Message {
            role: "user".into(),
            content: "plain".into(),
            content_parts: Vec::new(),
            ..Default::default()
        };
        assert!(message.openai_chat_content_parts().is_none());
    }

    #[test]
    fn test_generate_request_needs_vision_detects_image_parts() {
        fn request_with_parts(parts: Vec<MessageContentPart>) -> GenerateRequest {
            GenerateRequest {
                model: "any".into(),
                messages: vec![Message {
                    role: "user".into(),
                    content: String::new(),
                    content_parts: parts,
                    ..Default::default()
                }],
                tools: vec![],
                stream: false,
                auth_context: AuthContext::None,
                options: RequestOptions::default(),
            }
        }

        // Plain text only → no vision needed.
        let text_only = request_with_parts(vec![MessageContentPart::Text { text: "hi".into() }]);
        assert!(!text_only.needs_vision());

        // ImageUrl part → vision needed.
        let with_url = request_with_parts(vec![
            MessageContentPart::Text {
                text: "look".into(),
            },
            MessageContentPart::ImageUrl {
                image_url: ImageUrlContentPart {
                    url: "data:image/png;base64,abc".into(),
                    detail: None,
                },
            },
        ]);
        assert!(with_url.needs_vision());

        // ImageBlobRef part → vision needed (regardless of file rehydratability).
        let with_blob = request_with_parts(vec![MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_missing".into(),
                mime: "image/png".into(),
                size: 2048,
                blob_path: "/no/such/blob.png".into(),
                source: None,
            },
        }]);
        assert!(with_blob.needs_vision());

        // No messages at all → no vision needed.
        let empty = GenerateRequest {
            model: "any".into(),
            messages: vec![],
            tools: vec![],
            stream: false,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };
        assert!(!empty.needs_vision());
    }

    // ── R-5: blob rehydration retain-rounds cutoff ──────────────────────────

    fn blob_ref_part(path: &str) -> MessageContentPart {
        MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_abcdef0123456789".into(),
                mime: "image/png".into(),
                size: 2048,
                blob_path: path.into(),
                source: Some("screenshot".into()),
            },
        }
    }

    fn user_msg_with(parts: Vec<MessageContentPart>) -> Message {
        Message {
            role: "user".into(),
            content: String::new(),
            content_parts: parts,
            ..Default::default()
        }
    }

    #[test]
    fn test_normalize_image_retain_rounds_mirrors_ts() {
        // Finite values floor and clamp to a minimum of 1.
        assert_eq!(normalize_image_retain_rounds(Some(2.0), 2), 2);
        assert_eq!(normalize_image_retain_rounds(Some(3.9), 2), 3);
        assert_eq!(normalize_image_retain_rounds(Some(1.0), 2), 1);
        // Sub-1 values clamp up to 1 (TS Math.max(1, floor(value))).
        assert_eq!(normalize_image_retain_rounds(Some(0.0), 2), 1);
        assert_eq!(normalize_image_retain_rounds(Some(-5.0), 2), 1);
        // Non-finite → fallback.
        assert_eq!(
            normalize_image_retain_rounds(Some(f64::NAN), 2),
            DEFAULT_RETAIN_IMAGE_ROUNDS
        );
        assert_eq!(normalize_image_retain_rounds(Some(f64::INFINITY), 2), 2);
        // None → fallback.
        assert_eq!(normalize_image_retain_rounds(None, 2), 2);
        assert_eq!(
            normalize_image_retain_rounds(None, DEFAULT_RETAIN_IMAGE_ROUNDS),
            DEFAULT_RETAIN_IMAGE_ROUNDS
        );
    }

    #[test]
    fn test_image_retain_cutoff_mirrors_ts_scan() {
        let messages = vec![
            user_msg_with(vec![MessageContentPart::Text {
                text: "old q".into(),
            }]), // 0 user
            Message {
                role: "assistant".into(),
                content: "old a".into(),
                ..Default::default()
            }, // 1
            user_msg_with(vec![MessageContentPart::Text {
                text: "mid q".into(),
            }]), // 2 user
            Message {
                role: "assistant".into(),
                content: "mid a".into(),
                ..Default::default()
            }, // 3
            user_msg_with(vec![MessageContentPart::Text {
                text: "new q".into(),
            }]), // 4 user
        ];

        // retain=2: scanning from the end, the 2nd user round is at index 2 →
        // cutoff 2, so indices 2,3,4 are eligible; index 0,1 are not.
        assert_eq!(image_retain_cutoff(&messages, 2), 2);
        assert!(message_rehydrates_blob_at(&messages, 4, 2));
        assert!(message_rehydrates_blob_at(&messages, 2, 2));
        assert!(!message_rehydrates_blob_at(&messages, 1, 2));
        assert!(!message_rehydrates_blob_at(&messages, 0, 2));

        // retain=1: only the most recent user round (index 4) is the cutoff.
        assert_eq!(image_retain_cutoff(&messages, 1), 4);
        assert!(message_rehydrates_blob_at(&messages, 4, 1));
        assert!(!message_rehydrates_blob_at(&messages, 3, 1));

        // retain larger than available user rounds → cutoff 0 (every message
        // eligible; mirrors TS leaving the initial cutoffIndex=0 in place).
        assert_eq!(image_retain_cutoff(&messages, 10), 0);
        assert!(message_rehydrates_blob_at(&messages, 0, 10));

        // Empty message list → cutoff 0, never any eligible index in range.
        assert_eq!(image_retain_cutoff(&[], 2), 0);
    }

    #[test]
    fn test_retain_rounds_from_metadata_reads_config_key() {
        // Explicit numeric config → normalized.
        let meta = serde_json::json!({"image_history_retain_rounds": 4});
        assert_eq!(retain_rounds_from_metadata(Some(&meta)), 4);
        // Sub-1 clamps to 1.
        let meta = serde_json::json!({"image_history_retain_rounds": 0});
        assert_eq!(retain_rounds_from_metadata(Some(&meta)), 1);
        // Missing key → default.
        let meta = serde_json::json!({"other": 1});
        assert_eq!(
            retain_rounds_from_metadata(Some(&meta)),
            DEFAULT_RETAIN_IMAGE_ROUNDS
        );
        // Non-numeric value → default.
        let meta = serde_json::json!({"image_history_retain_rounds": "two"});
        assert_eq!(
            retain_rounds_from_metadata(Some(&meta)),
            DEFAULT_RETAIN_IMAGE_ROUNDS
        );
        // No metadata at all → default.
        assert_eq!(
            retain_rounds_from_metadata(None),
            DEFAULT_RETAIN_IMAGE_ROUNDS
        );
    }

    #[test]
    fn test_rehydrate_image_blob_ref_if_gates_on_eligibility() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        std::fs::write(&path, b"foobar").unwrap();
        let image = ImageBlobRefContentPart {
            blob_id: "blob_x".into(),
            mime: "image/png".into(),
            size: 6,
            blob_path: path.display().to_string(),
            source: None,
        };
        // Eligible → rehydrates the real data URI.
        let data_uri = rehydrate_image_blob_ref_if(&image, true).unwrap();
        assert_eq!(data_uri, "data:image/png;base64,Zm9vYmFy");
        // Not eligible → None without touching disk (older round outside the
        // retain window). The caller degrades to plain_text().
        assert!(rehydrate_image_blob_ref_if(&image, false).is_none());
    }

    #[test]
    fn test_openai_chat_content_parts_with_rehydrate_false_degrades_blob_to_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recent.bin");
        std::fs::write(&path, b"foo").unwrap();
        let path_str = path.display().to_string();
        let message = user_msg_with(vec![
            MessageContentPart::Text {
                text: "look".into(),
            },
            MessageContentPart::ImageBlobRef {
                image: ImageBlobRefContentPart {
                    blob_id: "blob_recent".into(),
                    mime: "image/png".into(),
                    size: 3,
                    blob_path: path_str.clone(),
                    source: None,
                },
            },
        ]);

        // rehydrate=true → image_url part carries the real data URI.
        let parts = message
            .openai_chat_content_parts_with_rehydrate(true)
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1]["type"], "image_url");
        assert!(parts[1]["image_url"]["url"]
            .as_str()
            .unwrap()
            .starts_with("data:image/png;base64,"));

        // rehydrate=false → blob degrades to a text placeholder; no image_url
        // block is emitted, and the blob_path never leaks.
        let parts = message
            .openai_chat_content_parts_with_rehydrate(false)
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1]["type"], "text");
        let placeholder = parts[1]["text"].as_str().unwrap();
        assert!(placeholder.contains("blob_recent"));
        assert!(!placeholder.contains(&path_str));
        assert!(!parts
            .iter()
            .any(|p| p["type"] == "image_url" || p.get("image_url").is_some()));
    }

    #[test]
    fn test_openai_chat_content_parts_with_rehydrate_false_keeps_image_url_parts() {
        // image_url parts are NOT blob refs, so rehydrate=false must still
        // emit them as real image_url blocks (the retain window only governs
        // blob-ref rehydration, never inline data: URIs).
        let message = user_msg_with(vec![
            MessageContentPart::Text {
                text: "look".into(),
            },
            MessageContentPart::ImageUrl {
                image_url: ImageUrlContentPart {
                    url: "data:image/png;base64,abc".into(),
                    detail: Some("low".into()),
                },
            },
        ]);
        let parts = message
            .openai_chat_content_parts_with_rehydrate(false)
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["url"], "data:image/png;base64,abc");
        assert_eq!(parts[1]["image_url"]["detail"], "low");
    }

    #[test]
    fn test_openai_responses_content_parts_with_rehydrate_false_degrades_blob_to_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.bin");
        std::fs::write(&path, b"foo").unwrap();
        let path_str = path.display().to_string();
        let message = user_msg_with(vec![MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_old".into(),
                mime: "image/png".into(),
                size: 3,
                blob_path: path_str.clone(),
                source: None,
            },
        }]);
        let parts = message
            .openai_responses_content_parts_with_rehydrate(false)
            .unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "input_text");
        let placeholder = parts[0]["text"].as_str().unwrap();
        assert!(placeholder.contains("blob_old"));
        assert!(!placeholder.contains(&path_str));
    }

    #[test]
    fn test_message_rehydrates_blob_at_applies_retain_window_over_message_list() {
        // End-to-end retain-window decision over a realistic turn sequence:
        // an old user round with a blob (round 1) and a recent user round with
        // a blob (round 2). With retain=2 both are eligible; with retain=1
        // only the recent round is.
        let messages = vec![
            user_msg_with(vec![blob_ref_part("/old/round1/blob.png")]), // 0 user (round 1)
            Message {
                role: "assistant".into(),
                content: "a1".into(),
                ..Default::default()
            }, // 1
            user_msg_with(vec![blob_ref_part("/old/round2/blob.png")]), // 2 user (round 2)
            Message {
                role: "assistant".into(),
                content: "a2".into(),
                ..Default::default()
            }, // 3
            user_msg_with(vec![blob_ref_part("/new/round3/blob.png")]), // 4 user (round 3)
        ];
        // retain=2: cutoff = index 2 (the 2nd user round from the end).
        assert_eq!(image_retain_cutoff(&messages, 2), 2);
        assert!(!message_rehydrates_blob_at(&messages, 0, 2)); // round 1 → degrade
        assert!(message_rehydrates_blob_at(&messages, 2, 2)); // round 2 → rehydrate
        assert!(message_rehydrates_blob_at(&messages, 4, 2)); // round 3 → rehydrate
    }
}
