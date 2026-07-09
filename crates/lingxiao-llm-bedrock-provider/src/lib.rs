use aws_credential_types::Credentials;
use aws_sdk_bedrockruntime::config::{BehaviorVersion, Region};
use aws_sdk_bedrockruntime::types::ResponseStream;
use aws_sdk_bedrockruntime::{Client, Config};
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::Blob;
use lingxiao_llm_host_protocol::{
    message_rehydrates_blob_at, rehydrate_image_blob_ref_if, retain_rounds_from_metadata,
    AuthContext, FinishReason, GenerateRequest, Message, MessageContentPart, ProviderError,
    ProviderErrorCode, StreamEvent, TokenUsage, ToolCall, ToolCallDelta,
};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BedrockProviderError {
    #[error("stdin read failed: {0}")]
    Stdin(String),
    #[error("request decode failed: {0}")]
    Decode(String),
    #[error("request encode failed: {0}")]
    Encode(String),
    #[error("unsupported auth context")]
    UnsupportedAuth,
    #[error("request build failed: {0}")]
    RequestBuild(String),
    #[error("provider request failed: {0}")]
    Provider(String),
}

pub fn run_stdio() -> Result<(), BedrockProviderError> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| BedrockProviderError::Stdin(e.to_string()))?;
    let request: GenerateRequest =
        serde_json::from_str(&line).map_err(|e| BedrockProviderError::Decode(e.to_string()))?;
    let events = execute_invoke_model_blocking(&request)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for event in events {
        let line = serde_json::to_string(&event)
            .map_err(|e| BedrockProviderError::Encode(e.to_string()))?;
        writeln!(stdout, "{line}").map_err(|e| BedrockProviderError::Stdin(e.to_string()))?;
    }
    stdout
        .flush()
        .map_err(|e| BedrockProviderError::Stdin(e.to_string()))?;
    Ok(())
}

pub fn execute_invoke_model_blocking(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, BedrockProviderError> {
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| BedrockProviderError::Provider(e.to_string()))?;
    runtime.block_on(execute_invoke_model(request))
}

pub async fn execute_invoke_model(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, BedrockProviderError> {
    if request.stream {
        return execute_invoke_model_stream(request).await;
    }
    let resolved = ResolvedBedrockConfig::from_request(request)?;
    let client = Client::from_conf(resolved.to_sdk_config());
    let body = build_invoke_body(request)?;
    let response = match client
        .invoke_model()
        .model_id(request.model.clone())
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(serde_json::to_vec(&body).map_err(|e| {
            BedrockProviderError::RequestBuild(e.to_string())
        })?))
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => return Ok(vec![StreamEvent::Error(provider_error_from_bedrock(error))]),
    };
    Ok(response_to_stream_events(response.body().as_ref()))
}

/// Streaming variant: invokes `InvokeModelWithResponseStream` and parses the
/// Anthropic-on-Bedrock event stream into host `StreamEvent`s.
///
/// Bedrock exposes Anthropic Claude models over the Messages API. When
/// streaming, each AWS event-stream `chunk` frame carries a `PayloadPart`
/// whose `bytes` blob (already base64-decoded by the SDK) is the raw JSON of a
/// single Anthropic Messages SSE event (`{"type":"content_block_delta",...}`).
/// There is no SSE text framing to split — each chunk is one complete event —
/// so this is simpler than the Anthropic raw-SSE path. The event JSON is parsed
/// by [`bedrock_stream_chunk_to_events`], which mirrors the Anthropic provider's
/// `raw_anthropic_sse_value_to_events` (text/thinking/tool-use deltas, usage,
/// finish reason, duplicate-`Finished` suppression, safe error handling).
///
/// Non-streaming behavior, the `bedrock_body` metadata escape hatch, and the
/// R-3 multimodal `build_invoke_body`/`bedrock_message` projections are shared
/// with the non-streaming path and unchanged.
async fn execute_invoke_model_stream(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, BedrockProviderError> {
    let resolved = ResolvedBedrockConfig::from_request(request)?;
    let client = Client::from_conf(resolved.to_sdk_config());
    let body = build_invoke_body(request)?;
    let output = match client
        .invoke_model_with_response_stream()
        .model_id(request.model.clone())
        .content_type("application/json")
        .accept("application/json")
        .body(Blob::new(serde_json::to_vec(&body).map_err(|e| {
            BedrockProviderError::RequestBuild(e.to_string())
        })?))
        .send()
        .await
    {
        Ok(output) => output,
        Err(error) => return Ok(vec![StreamEvent::Error(provider_error_from_bedrock(error))]),
    };

    let mut events = Vec::new();
    // Per-tool-use-block accumulator: (id, name, accumulated input_json string).
    // Mirrors the Anthropic provider's `tool_blocks` Vec.
    let mut tool_blocks: Vec<Option<(String, String, String)>> = Vec::new();
    let mut finished_emitted = false;
    let mut saw_event = false;
    // `InvokeModelWithResponseStreamOutput.body` is a public field; move the
    // owned `EventReceiver` out so we can call `recv(&mut self)` on it. (The
    // `body(&self)` accessor only lends a shared ref, which cannot satisfy
    // `recv`'s `&mut self`.)
    let mut stream = output.body;
    loop {
        match stream.recv().await {
            Ok(Some(ResponseStream::Chunk(chunk))) => {
                saw_event = true;
                let Some(bytes) = chunk.bytes.as_ref() else {
                    continue;
                };
                let value = match serde_json::from_slice::<Value>(bytes.as_ref()) {
                    Ok(value) => value,
                    Err(error) => {
                        // A malformed chunk is a recoverable stream error rather
                        // than a fatal provider failure: emit a StreamInterrupted
                        // error and stop, so the caller's retry/fallback can act.
                        events.push(StreamEvent::Error(ProviderError::new(
                            ProviderErrorCode::StreamInterrupted,
                            format!("Bedrock stream chunk decode failed: {error}"),
                        )));
                        break;
                    }
                };
                for event in bedrock_stream_chunk_to_events(&value, &mut tool_blocks) {
                    if matches!(event, StreamEvent::Finished(_)) {
                        if finished_emitted {
                            continue;
                        }
                        finished_emitted = true;
                    }
                    events.push(event);
                }
            }
            Ok(Some(_)) => {
                // Forward-compat: an unrecognized chunk variant (e.g.
                // `ResponseStream::Unknown`) from a newer Bedrock API. Ignore it
                // rather than failing the stream. `ResponseStream` is
                // `#[non_exhaustive]`, so this wildcard is required.
                saw_event = true;
            }
            Ok(None) => break,
            Err(error) => {
                // A transport/service error mid-stream. Map to a provider error
                // and stop. `provider_error_from_bedrock` redacts AKIA/ASIA.
                if !saw_event {
                    // No events yet → treat like a failed request.
                    return Ok(vec![StreamEvent::Error(provider_error_from_bedrock(error))]);
                }
                events.push(StreamEvent::Error(provider_error_from_bedrock(error)));
                break;
            }
        }
    }

    if !saw_event {
        return Ok(vec![StreamEvent::Error(ProviderError::new(
            ProviderErrorCode::StreamInterrupted,
            "Bedrock stream ended without events",
        ))]);
    }
    // Bedrock always emits a terminal `message_delta`/`message_stop` carrying a
    // finish reason; if for any reason none arrived (e.g. an early error frame),
    // emit a defensive `Finished` so consumers never see an unterminated stream.
    if !finished_emitted {
        events.push(StreamEvent::Finished(FinishReason::Unknown));
    }
    Ok(events)
}

#[derive(Clone, Debug)]
struct ResolvedBedrockConfig {
    region: String,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    endpoint_url: Option<String>,
}

impl ResolvedBedrockConfig {
    fn from_request(request: &GenerateRequest) -> Result<Self, BedrockProviderError> {
        let AuthContext::AwsSignature {
            region,
            access_key_id,
            secret_access_key,
            session_token,
        } = &request.auth_context
        else {
            return Err(BedrockProviderError::UnsupportedAuth);
        };
        let endpoint_url = request
            .options
            .metadata
            .as_ref()
            .and_then(|value| value.get("endpoint_url").or_else(|| value.get("base_url")))
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(Self {
            region: region.clone(),
            access_key_id: access_key_id.clone(),
            secret_access_key: secret_access_key.clone(),
            session_token: session_token.clone(),
            endpoint_url,
        })
    }

    fn to_sdk_config(&self) -> Config {
        let credentials = Credentials::new(
            self.access_key_id.clone(),
            self.secret_access_key.clone(),
            self.session_token.clone(),
            None,
            "lingxiao-core",
        );
        let mut builder = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            .credentials_provider(credentials)
            .retry_config(RetryConfig::disabled());
        if let Some(endpoint_url) = &self.endpoint_url {
            builder = builder.endpoint_url(endpoint_url);
        }
        builder.build()
    }
}

fn build_invoke_body(request: &GenerateRequest) -> Result<Value, BedrockProviderError> {
    if let Some(body) = request
        .options
        .metadata
        .as_ref()
        .and_then(|value| value.get("bedrock_body"))
    {
        return Ok(body.clone());
    }

    // Compute the retain-rounds window over the ORIGINAL message array so the
    // cutoff index aligns with TS `rehydrateRecentImageBlobRefs` (which scans
    // the full array, system messages included). System messages are filtered
    // out of the wire body below, but each surviving message keeps its original
    // index for the per-message rehydrate decision.
    let retain_rounds = retain_rounds_from_metadata(request.options.metadata.as_ref());
    let messages = request
        .messages
        .iter()
        .enumerate()
        .filter(|(_, message)| message.role != "system")
        .map(|(index, message)| {
            let rehydrate = message_rehydrates_blob_at(&request.messages, index, retain_rounds);
            bedrock_message(message, rehydrate)
        })
        .collect::<Vec<_>>();
    let mut body = json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": request.options.max_tokens.unwrap_or(1024),
        "messages": messages,
    });
    if let Some(system) = system_prompt(&request.messages) {
        body["system"] = Value::String(system);
    }
    if let Some(temperature) = request.options.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.options.top_p {
        body["top_p"] = json!(top_p);
    }
    if let Some(stop) = &request.options.stop {
        body["stop_sequences"] = json!(stop);
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.input_schema,
                })
            })
            .collect::<Vec<_>>());
    }
    Ok(body)
}

fn bedrock_message(message: &Message, rehydrate: bool) -> Value {
    if message.role == "assistant" {
        let text = message.plain_text_content();
        let mut content = Vec::new();
        if !text.is_empty() {
            content.push(json!({"type": "text", "text": text}));
        }
        for call in &message.tool_calls {
            content.push(json!({
                "type": "tool_use",
                "id": call.id,
                "name": call.name,
                "input": call.arguments,
            }));
        }
        if content.is_empty() {
            content.push(json!({"type": "text", "text": ""}));
        }
        return json!({
            "role": "assistant",
            "content": content,
        });
    }
    if message.role == "tool" {
        let text = message.plain_text_content();
        return json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": message.tool_call_id.clone().unwrap_or_default(),
                "content": text,
            }],
        });
    }
    json!({
        "role": "user",
        "content": bedrock_user_content(message, rehydrate),
    })
}

/// Build the `content` array for a Bedrock user-role message.
///
/// Plain-text messages (no structured `content_parts`) stay a single
/// `{type:"text",text}` block built from `plain_text_content()` so existing
/// text-only behavior is byte-for-byte unchanged. When the message carries
/// typed `content_parts`, the parts are projected into Anthropic-on-Bedrock
/// content blocks: `text` → text block, `image_url` data URI → `image` block
/// with a base64 source, and `image_blob_ref` → rehydrated from disk into an
/// `image` block. A blob whose backing file is missing/unreadable, a blob
/// outside the retain window (`rehydrate == false`), or a non-data-URI
/// `image_url` (a remote http URL Bedrock's base64 image source cannot
/// express), degrades to a safe text placeholder (short blob id only; never a
/// `blob_path` leak; never a panic) — mirroring the OpenAI/Anthropic/Gemini
/// providers, the TS degrading semantics, and TS
/// `rehydrateRecentImageBlobRefs`'s retain-rounds cutoff.
///
/// Bedrock exposes Anthropic Claude models via the
/// `anthropic_version: bedrock-2023-05-31` Messages API, whose image content
/// block is `{ "type": "image", "source": { "type": "base64",
/// "media_type": "image/png", "data": "..." } }`.
fn bedrock_user_content(message: &Message, rehydrate: bool) -> Vec<Value> {
    if message.content_parts.is_empty() {
        return vec![json!({"type": "text", "text": message.plain_text_content()})];
    }
    let mut parts = Vec::with_capacity(message.content_parts.len());
    for part in &message.content_parts {
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
                if let Some(source) = bedrock_image_source_from_url(&image_url.url) {
                    parts.push(json!({"type": "image", "source": source}));
                } else {
                    // Non data-URI image_url (e.g. a remote http URL) cannot be
                    // expressed as a Bedrock base64 image source; degrade to a
                    // textual marker the way the other providers do.
                    parts.push(
                        json!({"type": "text", "text": format!("[image] {}", image_url.url)}),
                    );
                }
            }
            MessageContentPart::ImageBlobRef { image } => {
                match rehydrate_image_blob_ref_if(image, rehydrate) {
                    Some(url) => {
                        if let Some(source) = bedrock_image_source_from_url(&url) {
                            parts.push(json!({"type": "image", "source": source}));
                        } else {
                            parts.push(json!({"type": "text", "text": part.plain_text()}));
                        }
                    }
                    // Missing/unreadable blob file, or a blob outside the
                    // retain window: degrade to the safe placeholder (short
                    // blob id only; no path leak) instead of panicking or
                    // dropping the part silently.
                    None => parts.push(json!({"type": "text", "text": part.plain_text()})),
                }
            }
        }
    }
    // Defensive: if structured parts produced no usable blocks (e.g. only empty
    // thinking parts, all skipped), fall back to the plain-text projection so we
    // never emit an empty content array — Bedrock rejects empty content.
    if parts.is_empty() {
        parts.push(json!({"type": "text", "text": message.plain_text_content()}));
    }
    parts
}

/// Parse a `data:<media>;base64,<data>` URI into a Bedrock Anthropic image
/// `source` object `{type:"base64", media_type, data}`. Returns `None` for any
/// non-data URI so callers can degrade to a text marker. Mirrors the
/// OpenAI/Anthropic/Gemini `parseDataUrl` helpers.
fn bedrock_image_source_from_url(url: &str) -> Option<Value> {
    let rest = url.strip_prefix("data:")?;
    let (media_type, data) = rest.split_once(";base64,")?;
    if media_type.is_empty() || data.is_empty() {
        return None;
    }
    Some(json!({
        "type": "base64",
        "media_type": media_type,
        "data": data,
    }))
}

fn system_prompt(messages: &[Message]) -> Option<String> {
    let system_parts = messages
        .iter()
        .filter(|message| message.role == "system")
        .map(Message::plain_text_content)
        .collect::<Vec<_>>();
    (!system_parts.is_empty()).then(|| system_parts.join("\n"))
}

fn response_to_stream_events(body: &[u8]) -> Vec<StreamEvent> {
    let value = match serde_json::from_slice::<Value>(body) {
        Ok(value) => value,
        Err(error) => {
            return vec![StreamEvent::Error(ProviderError::new(
                ProviderErrorCode::BadRequest,
                error.to_string(),
            ))]
        }
    };
    let text = extract_text(&value).unwrap_or_default();
    let tool_calls = extract_tool_calls(&value);
    let usage = extract_usage(&value);
    let finish_reason = extract_finish_reason(&value);
    let mut events = Vec::new();
    if !text.is_empty() {
        events.push(StreamEvent::TextDelta(text));
    }
    for (index, call) in tool_calls.into_iter().enumerate() {
        events.push(StreamEvent::ToolCallDelta(ToolCallDelta {
            index: index as u32,
            id: Some(call.id.clone()),
            name: Some(call.name.clone()),
            partial_json: Some(call.arguments.to_string()),
        }));
        events.push(StreamEvent::ToolCall(call));
    }
    events.push(StreamEvent::Usage(usage));
    events.push(StreamEvent::Finished(finish_reason));
    events
}

fn extract_text(value: &Value) -> Option<String> {
    if let Some(text) = value.get("outputText").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    if let Some(text) = value.get("generation").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    if let Some(text) = value.get("text").and_then(Value::as_str) {
        return Some(text.to_string());
    }
    if let Some(content) = value.get("content").and_then(Value::as_array) {
        let text = content
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join("");
        if !text.is_empty() {
            return Some(text);
        }
    }
    value
        .get("results")
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .and_then(extract_text)
}

fn extract_usage(value: &Value) -> TokenUsage {
    let usage = value.get("usage").unwrap_or(value);
    let prompt_tokens = usage_u32(usage, &["input_tokens", "inputTokens", "prompt_tokens"])
        .or_else(|| usage_u32(value, &["inputTextTokenCount"]))
        .unwrap_or(0);
    let completion_tokens = usage_u32(
        usage,
        &["output_tokens", "outputTokens", "completion_tokens"],
    )
    .or_else(|| usage_u32(value, &["resultsTokenCount"]))
    .unwrap_or(0);
    let total_tokens = usage_u32(usage, &["total_tokens", "totalTokens"])
        .unwrap_or(prompt_tokens + completion_tokens);
    TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        reasoning_tokens: None,
    }
}

fn extract_tool_calls(value: &Value) -> Vec<ToolCall> {
    value
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|part| {
            let tool = part
                .get("tool_use")
                .or_else(|| part.get("toolUse"))
                .or_else(|| {
                    (part.get("type").and_then(Value::as_str) == Some("tool_use")).then_some(part)
                })?;
            let name = tool.get("name").and_then(Value::as_str)?;
            let id = tool
                .get("id")
                .or_else(|| tool.get("toolUseId"))
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("bedrock_call_{name}"));
            let arguments = tool
                .get("input")
                .or_else(|| tool.get("args"))
                .or_else(|| tool.get("arguments"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            Some(ToolCall {
                id,
                name: name.to_string(),
                arguments,
            })
        })
        .collect()
}

fn usage_u32(value: &Value, names: &[&str]) -> Option<u32> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_u64))
        .and_then(|v| u32::try_from(v).ok())
}

fn extract_finish_reason(value: &Value) -> FinishReason {
    if let Some(reason) = value
        .get("results")
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .map(extract_finish_reason)
        .filter(|reason| *reason != FinishReason::Unknown)
    {
        return reason;
    }
    let reason = value
        .get("stop_reason")
        .or_else(|| value.get("stopReason"))
        .or_else(|| value.get("completionReason"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    match reason.as_str() {
        "end_turn" | "stop_sequence" | "stop" | "finished" | "finish" => FinishReason::Stop,
        "max_tokens" | "length" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "content_filtered" | "guardrail_intervened" => FinishReason::ContentFiltered,
        "" => FinishReason::Unknown,
        _ => FinishReason::Unknown,
    }
}

/// Map an Anthropic Messages `stop_reason` string to a host `FinishReason`.
/// Mirrors the Anthropic provider's `map_stop_reason_str`.
fn map_stop_reason_str(reason: &str) -> FinishReason {
    match reason {
        "end_turn" | "stop_sequence" | "stop" => FinishReason::Stop,
        "max_tokens" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "content_filtered" => FinishReason::ContentFiltered,
        _ => FinishReason::Unknown,
    }
}

/// Parse one Anthropic-on-Bedrock streaming event JSON object into host
/// `StreamEvent`s. `tool_blocks` accumulates in-flight tool-use blocks across
/// `content_block_start`/`content_block_delta`/`content_block_stop` so a tool
/// call's `id`/`name`/accumulated `input_json` finalize into a single
/// `StreamEvent::ToolCall` on `content_block_stop`, exactly as the Anthropic
/// provider's `raw_anthropic_sse_value_to_events` does. Unknown event types
/// (`ping`, `message_start`, future variants) are ignored — the stream
/// continues. An `"error"` event becomes a `StreamEvent::Error`.
///
/// Unlike the Anthropic raw-SSE path, each Bedrock chunk is already one
/// complete event JSON (no `data:` prefix, no `\n\n` frame splitting), so this
/// parser is invoked once per chunk with the parsed `Value` directly.
fn bedrock_stream_chunk_to_events(
    value: &Value,
    tool_blocks: &mut Vec<Option<(String, String, String)>>,
) -> Vec<StreamEvent> {
    match value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "content_block_start" => {
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as usize;
            if tool_blocks.len() <= index {
                tool_blocks.resize_with(index + 1, || None);
            }
            let Some(content_block) = value.get("content_block") else {
                return Vec::new();
            };
            if content_block.get("type").and_then(Value::as_str) != Some("tool_use") {
                return Vec::new();
            }
            let id = content_block
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let name = content_block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let partial_json = content_block
                .get("input")
                .map(initial_tool_input_json)
                .unwrap_or_default();
            tool_blocks[index] = Some((id.clone(), name.clone(), partial_json.clone()));
            vec![StreamEvent::ToolCallDelta(ToolCallDelta {
                index: index.min(u32::MAX as usize) as u32,
                id: Some(id),
                name: Some(name),
                partial_json: if partial_json.is_empty() {
                    None
                } else {
                    Some(partial_json)
                },
            })]
        }
        "content_block_delta" => {
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as usize;
            let Some(delta) = value.get("delta") else {
                return Vec::new();
            };
            match delta
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
            {
                "text_delta" => delta
                    .get("text")
                    .and_then(Value::as_str)
                    .map(|text| vec![StreamEvent::TextDelta(text.to_string())])
                    .unwrap_or_default(),
                "thinking_delta" => delta
                    .get("thinking")
                    .and_then(Value::as_str)
                    .map(|thinking| vec![StreamEvent::ThinkingDelta(thinking.to_string())])
                    .unwrap_or_default(),
                "input_json_delta" => {
                    if tool_blocks.len() <= index {
                        tool_blocks.resize_with(index + 1, || None);
                    }
                    let partial_json = delta
                        .get("partial_json")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if let Some((_, _, accumulated)) = tool_blocks[index].as_mut() {
                        accumulated.push_str(&partial_json);
                    }
                    vec![StreamEvent::ToolCallDelta(ToolCallDelta {
                        index: index.min(u32::MAX as usize) as u32,
                        id: tool_blocks[index].as_ref().map(|(id, _, _)| id.clone()),
                        name: tool_blocks[index].as_ref().map(|(_, name, _)| name.clone()),
                        partial_json: Some(partial_json),
                    })]
                }
                "signature_delta" => {
                    // Extended-thinking signature deltas carry no host-visible
                    // text; ignore (mirrors Anthropic provider behavior).
                    Vec::new()
                }
                _ => Vec::new(),
            }
        }
        "content_block_stop" => {
            let index = value
                .get("index")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as usize;
            let Some(Some((id, name, arguments))) = tool_blocks.get_mut(index).map(Option::take)
            else {
                return Vec::new();
            };
            let parsed_arguments = serde_json::from_str(&arguments).unwrap_or(Value::Null);
            vec![StreamEvent::ToolCall(ToolCall {
                id,
                name,
                arguments: parsed_arguments,
            })]
        }
        "message_delta" => {
            let usage = value.get("usage");
            let prompt_tokens = usage
                .and_then(|usage| usage.get("input_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as u32;
            let completion_tokens = usage
                .and_then(|usage| usage.get("output_tokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .min(u32::MAX as u64) as u32;
            let mut events = vec![StreamEvent::Usage(TokenUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens.saturating_add(completion_tokens),
                cache_creation_input_tokens: usage
                    .and_then(|usage| usage.get("cache_creation_input_tokens"))
                    .and_then(Value::as_u64)
                    .map(|value| value.min(u32::MAX as u64) as u32),
                cache_read_input_tokens: usage
                    .and_then(|usage| usage.get("cache_read_input_tokens"))
                    .and_then(Value::as_u64)
                    .map(|value| value.min(u32::MAX as u64) as u32),
                reasoning_tokens: None,
            })];
            if let Some(reason) = value
                .get("delta")
                .and_then(|delta| delta.get("stop_reason"))
                .and_then(Value::as_str)
            {
                events.push(StreamEvent::Finished(map_stop_reason_str(reason)));
            }
            events
        }
        "message_stop" => vec![StreamEvent::Finished(FinishReason::Stop)],
        "error" => vec![StreamEvent::Error(ProviderError::new(
            ProviderErrorCode::ServerError,
            value
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("Bedrock stream error"),
        ))],
        // `message_start`, `ping`, and any future event types carry no
        // host-visible delta; ignore them so the stream continues.
        _ => Vec::new(),
    }
}

/// Initial `input` JSON for a tool-use `content_block_start`. Anthropic sends
/// `input: {}` (or omits it) at block start and streams the real args via
/// `input_json_delta` deltas; emit an empty partial string in that case so the
/// accumulator starts clean. Mirrors the Anthropic provider's
/// `initial_tool_input_json`.
fn initial_tool_input_json(input: &Value) -> String {
    if input.is_null() || input.as_object().is_some_and(serde_json::Map::is_empty) {
        String::new()
    } else {
        input.to_string()
    }
}

fn provider_error_from_bedrock<E: std::fmt::Display>(error: E) -> ProviderError {
    let text = error.to_string();
    let lower = text.to_ascii_lowercase();
    let code = if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("accessdenied")
        || lower.contains("credentials")
    {
        ProviderErrorCode::Authentication
    } else if lower.contains("throttl") || lower.contains("rate") {
        ProviderErrorCode::RateLimited
    } else if lower.contains("validation") || lower.contains("bad request") {
        ProviderErrorCode::BadRequest
    } else if lower.contains("timeout") {
        ProviderErrorCode::Timeout
    } else if lower.contains("internal") || lower.contains("serviceunavailable") {
        ProviderErrorCode::ServerError
    } else {
        ProviderErrorCode::Unknown
    };
    ProviderError::new(code, redact_error_message(text))
}

fn redact_error_message(message: String) -> String {
    if message.contains("AKIA") || message.contains("ASIA") {
        "provider request failed".to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolved_config_uses_auth_context_not_env_defaults() {
        let request = sample_request();
        let config = ResolvedBedrockConfig::from_request(&request).unwrap();
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.access_key_id, "AKIDTEST");
        assert_eq!(config.secret_access_key, "secret-test");
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:4566")
        );
        let sdk_config = config.to_sdk_config();
        assert_eq!(sdk_config.region().unwrap().as_ref(), "us-east-1");
        assert_eq!(sdk_config.retry_config().unwrap().max_attempts(), 1);
    }

    #[test]
    fn test_build_invoke_body_defaults_to_anthropic_bedrock_shape() {
        let request = sample_request();
        let body = build_invoke_body(&request).unwrap();
        assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(body["system"], "system prompt");
        assert_eq!(body["max_tokens"], 12);
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"][0]["text"], "hello");
        assert!((body["temperature"].as_f64().unwrap() - 0.2).abs() < 0.00001);
        assert!((body["top_p"].as_f64().unwrap() - 0.9).abs() < 0.00001);
        assert_eq!(body["stop_sequences"][0], "\n\nHuman:");
    }

    #[test]
    fn test_build_invoke_body_accepts_explicit_bedrock_body_metadata() {
        let mut request = sample_request();
        request.options.metadata = Some(json!({
            "bedrock_body": {"inputText": "hello titan"}
        }));
        let body = build_invoke_body(&request).unwrap();
        assert_eq!(body["inputText"], "hello titan");
    }

    #[test]
    fn test_build_invoke_body_includes_tool_schemas() {
        let mut request = sample_request();
        request.tools = vec![lingxiao_llm_host_protocol::ToolDefinition {
            name: "file_read".into(),
            description: "Read file".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }),
        }];
        let body = build_invoke_body(&request).unwrap();
        assert_eq!(body["tools"][0]["name"], "file_read");
        assert_eq!(body["tools"][0]["input_schema"]["required"][0], "path");
    }

    #[test]
    fn test_response_to_stream_events_maps_anthropic_bedrock_response() {
        let body = br#"{"content":[{"type":"text","text":"bedrock response"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3}}"#;
        let events = response_to_stream_events(body);
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "bedrock response"));
        assert!(matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 5));
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_response_to_stream_events_maps_tool_use() {
        let body = br#"{"content":[{"type":"tool_use","id":"toolu_1","name":"file_read","input":{"path":"README.md"}}],"stop_reason":"tool_use","usage":{"input_tokens":2,"output_tokens":3}}"#;
        let events = response_to_stream_events(body);
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolCallDelta(delta)
                if delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolCall(call)
                if call.id == "toolu_1"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        )));
        assert!(matches!(
            events.last().unwrap(),
            StreamEvent::Finished(FinishReason::ToolCalls)
        ));
    }

    #[test]
    fn test_response_to_stream_events_maps_titan_response() {
        let body = br#"{"results":[{"outputText":"titan response","completionReason":"FINISH"}],"inputTextTokenCount":4,"resultsTokenCount":5}"#;
        let events = response_to_stream_events(body);
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "titan response"));
        assert!(matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 9));
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_unsupported_auth_rejected_before_sdk_config() {
        let mut request = sample_request();
        request.auth_context = AuthContext::None;
        assert!(matches!(
            ResolvedBedrockConfig::from_request(&request),
            Err(BedrockProviderError::UnsupportedAuth)
        ));
    }

    #[test]
    fn test_stream_true_routes_to_streaming_path_not_unsupported() {
        // Regression guard: before R-9, stream=true returned an explicit
        // UnsupportedModel error. Streaming is now implemented, so stream=true
        // must NOT short-circuit with an unsupported error. Pointing the endpoint
        // at an unreachable port yields a dispatch/connection error (mapped to a
        // ProviderError), never an UnsupportedModel code — proving the streaming
        // branch is taken and InvokeModelWithResponseStream is attempted rather
        // than the old explicit-unsupported short-circuit.
        let mut request = sample_request();
        request.stream = true;
        request.options.metadata = Some(json!({"endpoint_url": "http://127.0.0.1:9"}));

        let events = execute_invoke_model_blocking(&request).unwrap();
        assert!(!events.is_empty(), "stream=true must produce events");
        let err = events
            .iter()
            .find_map(|event| match event {
                StreamEvent::Error(err) => Some(err),
                _ => None,
            })
            .expect("connection to closed port must surface as a StreamEvent::Error");
        // The error must NOT be UnsupportedModel (the old short-circuit code).
        assert_ne!(
            err.code,
            ProviderErrorCode::UnsupportedModel,
            "stream=true must not return UnsupportedModel: {err:?}"
        );
    }

    #[test]
    fn test_stream_true_rejects_unsupported_auth_before_sdk_call() {
        // The streaming path shares ResolvedBedrockConfig::from_request with the
        // non-streaming path, so unsupported auth is rejected before any SDK
        // call (no AWS request is attempted).
        let mut request = sample_request();
        request.stream = true;
        request.auth_context = AuthContext::None;
        assert!(matches!(
            execute_invoke_model_blocking(&request),
            Err(BedrockProviderError::UnsupportedAuth)
        ));
    }

    // -----------------------------------------------------------------------
    // R-9: Bedrock streaming — Anthropic-on-Bedrock event-stream chunk parsing
    // -----------------------------------------------------------------------

    /// Decode an Anthropic-on-Bedrock streaming event JSON string into events,
    /// the same way production does: `serde_json::from_slice` of the chunk
    /// `bytes` blob → `bedrock_stream_chunk_to_events`. Each Bedrock chunk is
    /// one complete Anthropic Messages SSE event JSON object (the AWS event-
    /// stream `PayloadPart.bytes` already carries the decoded event JSON; there
    /// is no SSE text framing to split).
    fn decode_chunk(
        json_str: &str,
        tool_blocks: &mut Vec<Option<(String, String, String)>>,
    ) -> Vec<StreamEvent> {
        let value: Value = serde_json::from_str(json_str).unwrap();
        bedrock_stream_chunk_to_events(&value, tool_blocks)
    }

    #[test]
    fn test_stream_chunk_text_delta_emits_text_delta() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hel"}}"#,
            &mut tool_blocks,
        );
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            StreamEvent::TextDelta(text) if text == "Hel"
        ));
    }

    #[test]
    fn test_stream_chunk_thinking_delta_emits_thinking_delta() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reasoning"}}"#,
            &mut tool_blocks,
        );
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            StreamEvent::ThinkingDelta(text) if text == "reasoning"
        ));
    }

    #[test]
    fn test_stream_chunk_signature_delta_is_ignored() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"abc"}}"#,
            &mut tool_blocks,
        );
        assert!(
            events.is_empty(),
            "signature deltas carry no host-visible text"
        );
    }

    #[test]
    fn test_stream_chunk_tool_use_lifecycle_emits_delta_then_tool_call() {
        // content_block_start (tool_use) → ToolCallDelta with id/name
        // content_block_delta (input_json_delta) → ToolCallDelta with partial_json
        // content_block_stop → finalized ToolCall with parsed arguments
        let mut tool_blocks = Vec::new();

        let start = decode_chunk(
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"file_read","input":{}}}"#,
            &mut tool_blocks,
        );
        assert_eq!(start.len(), 1);
        assert!(matches!(
            &start[0],
            StreamEvent::ToolCallDelta(delta)
                if delta.index == 1
                    && delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
                    && delta.partial_json.is_none()
        ));

        // Two partial-json deltas accumulate into the tool block.
        let d1 = decode_chunk(
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\""}}"#,
            &mut tool_blocks,
        );
        assert_eq!(d1.len(), 1);
        assert!(matches!(
            &d1[0],
            StreamEvent::ToolCallDelta(delta)
                if delta.index == 1
                    && delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
                    && delta.partial_json.as_deref() == Some("{\"path\"")
        ));
        let d2 = decode_chunk(
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":":\"README.md\"}"}}"#,
            &mut tool_blocks,
        );
        assert_eq!(d2.len(), 1);
        // The delta carries this chunk's partial_json fragment (not the
        // accumulated value); accumulation lives in the tool_blocks state.
        assert!(matches!(
            &d2[0],
            StreamEvent::ToolCallDelta(delta)
                if delta.index == 1
                    && delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
                    && delta.partial_json.as_deref() == Some(r#":"README.md"}"#)
        ));

        let stop = decode_chunk(
            r#"{"type":"content_block_stop","index":1}"#,
            &mut tool_blocks,
        );
        assert_eq!(stop.len(), 1);
        assert!(matches!(
            &stop[0],
            StreamEvent::ToolCall(call)
                if call.id == "toolu_1"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        ));
    }

    #[test]
    fn test_stream_chunk_message_delta_emits_usage_and_finish() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":7,"output_tokens":9}}"#,
            &mut tool_blocks,
        );
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            StreamEvent::Usage(usage)
                if usage.prompt_tokens == 7
                    && usage.completion_tokens == 9
                    && usage.total_tokens == 16
        ));
        assert!(matches!(
            &events[1],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_stream_chunk_message_delta_tool_use_finish_reason() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":2,"output_tokens":3}}"#,
            &mut tool_blocks,
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, StreamEvent::Finished(FinishReason::ToolCalls))));
    }

    #[test]
    fn test_stream_chunk_message_stop_emits_finished_stop() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(r#"{"type":"message_stop"}"#, &mut tool_blocks);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_stream_chunk_error_emits_provider_error() {
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"error","error":{"type":"overloaded_error","message":"InternalServerError"}}"#,
            &mut tool_blocks,
        );
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            StreamEvent::Error(err)
                if err.code == ProviderErrorCode::ServerError
                    && err.message == "InternalServerError"
        ));
    }

    #[test]
    fn test_stream_chunk_message_start_and_ping_are_ignored() {
        let mut tool_blocks = Vec::new();
        let start = decode_chunk(
            r#"{"type":"message_start","message":{"id":"msg_1","usage":{"input_tokens":3}}}"#,
            &mut tool_blocks,
        );
        assert!(
            start.is_empty(),
            "message_start carries no host-visible delta"
        );
        let ping = decode_chunk(r#"{"type":"ping"}"#, &mut tool_blocks);
        assert!(ping.is_empty(), "ping carries no host-visible delta");
    }

    #[test]
    fn test_stream_chunk_full_sequence_text_then_finish_no_duplicate_finished() {
        // A realistic Bedrock stream for a plain text response: a couple of
        // text deltas, a message_delta (usage + finish), then a message_stop.
        // The caller-side duplicate-Finished suppression (in
        // execute_invoke_model_stream) is exercised at the parser boundary by
        // confirming message_delta and message_stop each independently yield a
        // Finished; the streaming loop dedups the second.
        let mut tool_blocks = Vec::new();
        let mut all = Vec::new();
        for chunk in [
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi "}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"there"}}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":1,"output_tokens":2}}"#,
            r#"{"type":"message_stop"}"#,
        ] {
            all.extend(decode_chunk(chunk, &mut tool_blocks));
        }
        // 2 text deltas + 1 usage + 2 finished (one from message_delta, one
        // from message_stop). The streaming loop suppresses the duplicate.
        assert_eq!(all.len(), 5);
        assert!(matches!(&all[0], StreamEvent::TextDelta(t) if t == "Hi "));
        assert!(matches!(&all[1], StreamEvent::TextDelta(t) if t == "there"));
        assert!(matches!(&all[2], StreamEvent::Usage(_)));
        let finished: Vec<_> = all
            .iter()
            .filter(|event| matches!(event, StreamEvent::Finished(_)))
            .collect();
        assert_eq!(
            finished.len(),
            2,
            "parser emits one Finished per terminal event"
        );
        assert!(finished
            .iter()
            .all(|event| matches!(event, StreamEvent::Finished(FinishReason::Stop))));
    }

    #[test]
    fn test_stream_chunk_with_non_tool_content_block_start_is_ignored() {
        // A text content_block_start (Anthropic sends these for each block)
        // must not produce a ToolCallDelta; only tool_use blocks do.
        let mut tool_blocks = Vec::new();
        let events = decode_chunk(
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            &mut tool_blocks,
        );
        assert!(events.is_empty());
    }

    // -----------------------------------------------------------------------
    // R-9: Bedrock streaming — genuine AWS event-stream framing round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn test_bedrock_event_stream_frame_round_trips_chunk_payload() {
        // Proves the smithy event-stream wire framing is sound: a real Bedrock
        // `chunk` frame (headers `:message-type=event`, `:event-type=chunk`,
        // `:content-type=application/json`; payload = the Anthropic event JSON
        // wrapped as `{"bytes": <base64>}`) survives `write_message_to` →
        // `read_message_from`, the headers parse via the public
        // `parse_response_headers` (the same call the SDK's
        // `ResponseStreamUnmarshaller` uses), and the payload's `bytes` blob
        // base64-decodes back to the original Anthropic event JSON — which then
        // parses through `bedrock_stream_chunk_to_events` into the expected
        // TextDelta. This is the exact decode contract the production
        // `EventReceiver`/`ResponseStreamUnmarshaller` performs; we replicate
        // it here with public smithy primitives because the SDK's unmarshaller
        // module is crate-private.
        use aws_smithy_eventstream::frame::{read_message_from, write_message_to};
        use aws_smithy_eventstream::smithy::parse_response_headers;
        use aws_smithy_types::event_stream::{Header, HeaderValue, Message};

        // The Anthropic event JSON a Bedrock chunk carries (after SDK base64
        // decode, this is exactly `PayloadPart.bytes.as_ref()` in production).
        let event_json = br#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"framed"}}"#;
        // Production payload shape: {"bytes": "<base64 of event_json>"}.
        // The SDK's `expect_blob_or_null` decodes this base64 back to event_json.
        let b64 = base64_encode(event_json);
        let payload = format!(r#"{{"bytes":"{b64}"}}"#);

        let message = Message::new_from_parts(
            vec![
                Header::new(":message-type", HeaderValue::String("event".into())),
                Header::new(":event-type", HeaderValue::String("chunk".into())),
                Header::new(
                    ":content-type",
                    HeaderValue::String("application/json".into()),
                ),
            ],
            payload.as_bytes().to_vec(),
        );

        // Serialize to the binary event-stream wire format and back.
        let mut wire = Vec::new();
        write_message_to(&message, &mut wire).unwrap();
        let round_tripped = read_message_from(&mut &wire[..]).unwrap();

        // Headers parse as a `chunk` event (the unmarshaller's first step).
        let headers = parse_response_headers(&round_tripped).unwrap();
        assert_eq!(headers.message_type.as_str(), "event");
        assert_eq!(headers.smithy_type.as_str(), "chunk");

        // Replicate `de_payload_part_payload`: parse payload JSON, read the
        // `bytes` blob, base64-decode it → the original Anthropic event JSON.
        let payload_value: Value = serde_json::from_slice(round_tripped.payload()).unwrap();
        let b64_back = payload_value.get("bytes").and_then(Value::as_str).unwrap();
        let decoded = base64_decode(b64_back);
        assert_eq!(decoded, event_json);

        // The decoded bytes are exactly what production feeds to
        // serde_json::from_slice → bedrock_stream_chunk_to_events.
        let value: Value = serde_json::from_slice(&decoded).unwrap();
        let mut tool_blocks = Vec::new();
        let events = bedrock_stream_chunk_to_events(&value, &mut tool_blocks);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            StreamEvent::TextDelta(text) if text == "framed"
        ));
    }

    /// Minimal RFC 4648 base64 encoder for the framing round-trip test (the
    /// Bedrock crate has no base64 dependency; test-only, mirrors the
    /// host-protocol `base64_encode` table).
    fn base64_encode(input: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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

    /// Minimal RFC 4648 base64 decoder for the framing round-trip test (the
    /// Bedrock crate has no base64 dependency; this mirrors the encoder's table
    /// in reverse and is test-only).
    fn base64_decode(input: &str) -> Vec<u8> {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::with_capacity(input.len() * 3 / 4);
        let mut buf: u32 = 0;
        let mut bits: u32 = 0;
        for ch in input.bytes() {
            if ch == b'=' {
                continue;
            }
            let Some(pos) = TABLE.iter().position(|&t| t == ch) else {
                continue;
            };
            buf = (buf << 6) | pos as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buf >> bits) as u8);
            }
        }
        out
    }

    fn sample_request() -> GenerateRequest {
        GenerateRequest {
            model: "anthropic.claude-3-haiku-20240307-v1:0".into(),
            messages: vec![
                Message {
                    role: "system".into(),
                    content: "system prompt".into(),
                    ..Default::default()
                },
                Message {
                    role: "user".into(),
                    content: "hello".into(),
                    ..Default::default()
                },
            ],
            tools: Vec::new(),
            stream: true,
            auth_context: AuthContext::AwsSignature {
                region: "us-east-1".into(),
                access_key_id: "AKIDTEST".into(),
                secret_access_key: "secret-test".into(),
                session_token: Some("session-test".into()),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(12),
                temperature: Some(0.2),
                top_p: Some(0.9),
                stop: Some(vec!["\n\nHuman:".into()]),
                metadata: Some(json!({"endpoint_url": "http://localhost:4566"})),
                ..Default::default()
            },
        }
    }

    // -----------------------------------------------------------------------
    // P0: Bedrock provider — tool history mapping contract tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bedrock_message_assistant_with_tool_calls_maps_to_tool_use_content() {
        // An assistant message that contains tool_calls must be rendered as an
        // Anthropic Bedrock content array with type="tool_use" entries.
        use lingxiao_llm_host_protocol::ToolCall as ProtocolToolCall;
        let message = Message {
            role: "assistant".into(),
            content: String::new(),
            tool_calls: vec![ProtocolToolCall {
                id: "toolu_hist".into(),
                name: "file_read".into(),
                arguments: json!({"path": "AGENTS.md"}),
            }],
            ..Default::default()
        };
        let mapped = bedrock_message(&message, true);
        assert_eq!(mapped["role"], "assistant");
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        let tool_use = content
            .iter()
            .find(|part| part["type"] == "tool_use")
            .expect("expected a tool_use content part");
        assert_eq!(tool_use["id"], "toolu_hist");
        assert_eq!(tool_use["name"], "file_read");
        assert_eq!(tool_use["input"]["path"], "AGENTS.md");
    }

    #[test]
    fn test_bedrock_message_tool_role_maps_to_user_tool_result_content() {
        // A "tool" role message must be mapped to a user message with a
        // type="tool_result" content block and the correct tool_use_id.
        let message = Message {
            role: "tool".into(),
            content: "the file contents".into(),
            tool_call_id: Some("toolu_hist".into()),
            ..Default::default()
        };
        let mapped = bedrock_message(&message, true);
        assert_eq!(mapped["role"], "user");
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        let result = content
            .iter()
            .find(|part| part["type"] == "tool_result")
            .expect("expected a tool_result content part");
        assert_eq!(result["tool_use_id"], "toolu_hist");
        assert_eq!(result["content"], "the file contents");
    }

    // -----------------------------------------------------------------------
    // P2: Bedrock provider — native multimodal image input contract tests
    // (Anthropic-on-Bedrock Messages API image content blocks)
    // -----------------------------------------------------------------------

    use lingxiao_llm_host_protocol::{
        ImageBlobRefContentPart, ImageUrlContentPart, MessageContentPart,
    };

    fn user_message_with_parts(parts: Vec<MessageContentPart>) -> Message {
        Message {
            role: "user".into(),
            content: String::new(),
            content_parts: parts,
            ..Default::default()
        }
    }

    #[test]
    fn test_bedrock_message_data_uri_image_url_emits_image_block() {
        // An image_url data URI part must become an Anthropic-on-Bedrock
        // `{type:"image", source:{type:"base64", media_type, data}}` block.
        let message = user_message_with_parts(vec![
            MessageContentPart::Text {
                text: "what is this".into(),
            },
            MessageContentPart::ImageUrl {
                image_url: ImageUrlContentPart {
                    url: "data:image/png;base64,Zm9vYmFy".into(),
                    detail: None,
                },
            },
        ]);
        let mapped = bedrock_message(&message, true);
        assert_eq!(mapped["role"], "user");
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "what is this");
        let image = &content[1];
        assert_eq!(image["type"], "image");
        assert_eq!(image["source"]["type"], "base64");
        assert_eq!(image["source"]["media_type"], "image/png");
        assert_eq!(image["source"]["data"], "Zm9vYmFy");
    }

    #[test]
    fn test_bedrock_message_rehydrates_blob_ref_into_image_block() {
        // An image_blob_ref part must be rehydrated from disk into a real
        // Bedrock image source block (base64 of the file bytes).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        // "foobar" -> base64 "Zm9vYmFy"
        std::fs::write(&path, b"foobar").unwrap();
        let message = user_message_with_parts(vec![MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_1234567890abcdef".into(),
                mime: "image/png".into(),
                size: 6,
                blob_path: path.display().to_string(),
                source: None,
            },
        }]);
        let mapped = bedrock_message(&message, true);
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "image");
        assert_eq!(content[0]["source"]["type"], "base64");
        assert_eq!(content[0]["source"]["media_type"], "image/png");
        assert_eq!(content[0]["source"]["data"], "Zm9vYmFy");
    }

    #[test]
    fn test_bedrock_message_missing_blob_falls_back_to_safe_text_placeholder() {
        // A blob whose backing file is missing must degrade to the safe
        // `[image: …]` text placeholder (short blob id only; no path leak) and
        // never panic. The placeholder exposes only the first 12 chars of the
        // blob id (see host-protocol `MessageContentPart::plain_text`).
        let blob_id = "blob_aaaaaaaa1111";
        let message = user_message_with_parts(vec![MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: blob_id.into(),
                mime: "image/png".into(),
                size: 2048,
                blob_path: "/definitely/does/not/exist/blob_aaaaaaaa1111.png".into(),
                source: None,
            },
        }]);
        let mapped = bedrock_message(&message, true);
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        let placeholder = content[0]["text"].as_str().unwrap();
        let short_id: String = blob_id.chars().take(12).collect();
        assert!(placeholder.contains(&short_id));
        assert!(!placeholder.contains("/definitely/does/not/exist"));
    }

    #[test]
    fn test_bedrock_message_remote_image_url_degrades_to_text_marker() {
        // A non-data-URI image_url (a remote http URL) cannot be expressed as a
        // Bedrock base64 image source; degrade to a `[image] <url>` text marker.
        let message = user_message_with_parts(vec![MessageContentPart::ImageUrl {
            image_url: ImageUrlContentPart {
                url: "https://example.test/cat.png".into(),
                detail: None,
            },
        }]);
        let mapped = bedrock_message(&message, true);
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "[image] https://example.test/cat.png");
    }

    #[test]
    fn test_bedrock_message_pure_text_stays_compatible() {
        // A user message with NO structured content_parts must keep using
        // plain_text_content() — byte-for-byte the legacy single text block.
        let message = Message {
            role: "user".into(),
            content: "hello".into(),
            content_parts: Vec::new(),
            ..Default::default()
        };
        let mapped = bedrock_message(&message, true);
        let content = mapped["content"]
            .as_array()
            .expect("content must be an array");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "hello");
    }

    #[test]
    fn test_bedrock_message_assistant_and_tool_paths_unaffected_by_multimodal() {
        // The multimodal change is confined to the user branch; assistant
        // (tool_use) and tool (tool_result) paths must be unchanged so
        // tool-use replay and tool-result history are preserved.
        use lingxiao_llm_host_protocol::ToolCall as ProtocolToolCall;
        let assistant = Message {
            role: "assistant".into(),
            content: String::new(),
            tool_calls: vec![ProtocolToolCall {
                id: "toolu_x".into(),
                name: "file_read".into(),
                arguments: json!({"path": "a.md"}),
            }],
            ..Default::default()
        };
        let mapped = bedrock_message(&assistant, true);
        assert_eq!(mapped["role"], "assistant");
        let tool_use = mapped["content"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["type"] == "tool_use")
            .expect("expected tool_use");
        assert_eq!(tool_use["id"], "toolu_x");
        assert_eq!(tool_use["input"]["path"], "a.md");

        let tool = Message {
            role: "tool".into(),
            content: "result text".into(),
            tool_call_id: Some("toolu_x".into()),
            ..Default::default()
        };
        let mapped = bedrock_message(&tool, true);
        assert_eq!(mapped["role"], "user");
        let result = mapped["content"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["type"] == "tool_result")
            .expect("expected tool_result");
        assert_eq!(result["tool_use_id"], "toolu_x");
        assert_eq!(result["content"], "result text");
    }

    #[test]
    fn test_build_invoke_body_emits_image_block_on_wire_for_multimodal_request() {
        // build_invoke_body must serialize a user image part into the
        // Anthropic-on-Bedrock image block in the request body JSON, alongside
        // the text part, system prompt, and anthropic_version envelope.
        let mut request = sample_request();
        request.messages = vec![
            Message {
                role: "system".into(),
                content: "system prompt".into(),
                ..Default::default()
            },
            Message {
                role: "user".into(),
                content: String::new(),
                content_parts: vec![
                    MessageContentPart::Text {
                        text: "describe this".into(),
                    },
                    MessageContentPart::ImageUrl {
                        image_url: ImageUrlContentPart {
                            url: "data:image/jpeg;base64,/9j/4AAQ".into(),
                            detail: None,
                        },
                    },
                ],
                ..Default::default()
            },
        ];
        let body = build_invoke_body(&request).unwrap();
        assert_eq!(body["anthropic_version"], "bedrock-2023-05-31");
        assert_eq!(body["system"], "system prompt");
        // System message is filtered out of the messages array.
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
        let content = &body["messages"][0]["content"];
        let content = content.as_array().expect("content must be an array");
        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "describe this");
        assert_eq!(content[1]["type"], "image");
        assert_eq!(content[1]["source"]["type"], "base64");
        assert_eq!(content[1]["source"]["media_type"], "image/jpeg");
        assert_eq!(content[1]["source"]["data"], "/9j/4AAQ");
    }

    #[test]
    fn test_bedrock_retain_window_degrades_old_blob_to_text_on_wire() {
        // Two user rounds each carrying a rehydratable blob (plus a leading
        // system message that is filtered out of the wire body). With retain=1
        // only the most recent user round rehydrates an image block on the
        // wire; the older round's blob degrades to a text block (never an
        // image block, never a blob_path leak). The retain window is computed
        // over the ORIGINAL message array, so the system message does not shift
        // the user-round count.
        let dir = tempfile::tempdir().unwrap();
        let old_path = dir.path().join("old.png");
        let new_path = dir.path().join("new.png");
        std::fs::write(&old_path, [0x89, 0x50, 0x4E, 0x47]).unwrap();
        std::fs::write(&new_path, [0xFF, 0xD8, 0xFF]).unwrap();
        let old_path_str = old_path.display().to_string();
        let new_path_str = new_path.display().to_string();

        let blob_ref = |path: &str, id: &str| MessageContentPart::ImageBlobRef {
            image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                blob_id: id.into(),
                mime: "image/png".into(),
                size: 4,
                blob_path: path.into(),
                source: None,
            },
        };

        let mut request = sample_request();
        request.messages = vec![
            Message {
                role: "system".into(),
                content: "system prompt".into(),
                ..Default::default()
            }, // filtered out of wire body (original index 0)
            Message {
                role: "user".into(),
                content: String::new(),
                content_parts: vec![blob_ref(&old_path_str, "blob_old_bed")],
                ..Default::default()
            }, // round 1 (original index 1)
            Message {
                role: "assistant".into(),
                content: "ack".into(),
                ..Default::default()
            }, // original index 2
            Message {
                role: "user".into(),
                content: String::new(),
                content_parts: vec![blob_ref(&new_path_str, "blob_new_bed")],
                ..Default::default()
            }, // round 2 (original index 3)
        ];
        // retain=1 → only the most recent user round rehydrates.
        request.options.metadata = Some(json!({
            "endpoint_url": "http://localhost:4566",
            "image_history_retain_rounds": 1
        }));

        let body = build_invoke_body(&request).unwrap();
        let messages = body["messages"].as_array().unwrap();
        // System filtered out → 3 wire messages: old user, assistant, new user.
        assert_eq!(messages.len(), 3);

        // messages[0] is the old user round → text block, never an image block.
        let old_content = messages[0]["content"].as_array().unwrap();
        assert_eq!(old_content.len(), 1);
        assert_eq!(old_content[0]["type"], "text");
        let old_placeholder = old_content[0]["text"].as_str().unwrap();
        assert!(
            old_placeholder.contains("blob_old_bed"),
            "placeholder should carry the short blob id: {old_placeholder}"
        );
        assert!(
            !old_placeholder.contains(&old_path_str),
            "blob_path must not leak: {old_placeholder}"
        );
        assert!(
            !old_content.iter().any(|p| p["type"] == "image"),
            "old-round blob must NOT rehydrate to an image block, got: {body}"
        );

        // messages[2] is the recent user round → real image block.
        let new_content = messages[2]["content"].as_array().unwrap();
        assert_eq!(new_content.len(), 1);
        assert_eq!(new_content[0]["type"], "image");
        assert_eq!(new_content[0]["source"]["type"], "base64");
        assert_eq!(new_content[0]["source"]["media_type"], "image/png");
        assert!(!new_content[0]["source"]["data"]
            .as_str()
            .unwrap()
            .is_empty());
    }
}
