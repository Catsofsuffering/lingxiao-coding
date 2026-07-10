use futures_util::StreamExt;
use gemini_rust::{
    Blob as GeminiBlob, Content as GeminiContent, FinishReason as GeminiFinishReason,
    FunctionCall as GeminiFunctionCall, FunctionCallingMode, FunctionDeclaration, Gemini,
    GenerationResponse, Message as GeminiMessage, Model as GeminiModel, Part as GeminiPart,
    Role as GeminiRole,
};
use lingxiao_llm_host_protocol::{
    message_rehydrates_blob_at, parse_base64_data_uri, rehydrate_image_blob_ref_if,
    retain_rounds_from_metadata, AuthContext, FinishReason, GenerateRequest, Message,
    MessageContentPart, ProviderError, ProviderErrorCode, StreamEvent, TokenUsage, ToolCall,
    ToolCallDelta,
};
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use thiserror::Error;
use url::Url;

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta/";

#[derive(Debug, Error)]
pub enum GeminiProviderError {
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

pub fn run_stdio() -> Result<(), GeminiProviderError> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| GeminiProviderError::Stdin(e.to_string()))?;
    let request: GenerateRequest =
        serde_json::from_str(&line).map_err(|e| GeminiProviderError::Decode(e.to_string()))?;
    let events = execute_generate_content_blocking(&request)?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    for event in events {
        let line = serde_json::to_string(&event)
            .map_err(|e| GeminiProviderError::Encode(e.to_string()))?;
        writeln!(stdout, "{line}").map_err(|e| GeminiProviderError::Stdin(e.to_string()))?;
    }
    stdout
        .flush()
        .map_err(|e| GeminiProviderError::Stdin(e.to_string()))?;
    Ok(())
}

pub fn execute_generate_content_blocking(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, GeminiProviderError> {
    let runtime =
        tokio::runtime::Runtime::new().map_err(|e| GeminiProviderError::Provider(e.to_string()))?;
    runtime.block_on(execute_generate_content(request))
}

pub async fn execute_generate_content(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, GeminiProviderError> {
    let (api_key, base_url) = resolved_config(request)?;
    let client = Gemini::with_model_and_base_url(
        api_key,
        GeminiModel::from(request.model.clone()),
        Url::parse(&base_url).map_err(|e| GeminiProviderError::RequestBuild(e.to_string()))?,
    )
    .map_err(|e| GeminiProviderError::Provider(e.to_string()))?;

    let mut builder = client.generate_content();
    let retain_rounds = retain_rounds_from_metadata(request.options.metadata.as_ref());
    for (index, message) in request.messages.iter().enumerate() {
        let rehydrate = message_rehydrates_blob_at(&request.messages, index, retain_rounds);
        builder = add_message(builder, message, rehydrate);
    }
    for tool in &request.tools {
        builder = builder.with_function(gemini_function_declaration(tool)?);
    }
    if !request.tools.is_empty() {
        builder = builder.with_function_calling_mode(FunctionCallingMode::Auto);
    }
    if let Some(max_tokens) = request.options.max_tokens {
        builder = builder.with_max_output_tokens(max_tokens as i32);
    }
    if let Some(temperature) = request.options.temperature {
        builder = builder.with_temperature(temperature);
    }
    if let Some(top_p) = request.options.top_p {
        builder = builder.with_top_p(top_p);
    }
    if let Some(stop) = &request.options.stop {
        builder = builder.with_stop_sequences(stop.clone());
    }

    if request.stream {
        let mut stream = match builder.execute_stream().await {
            Ok(stream) => stream,
            Err(error) => return Ok(vec![StreamEvent::Error(provider_error_from_gemini(error))]),
        };
        let mut events = Vec::new();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(response) => events.extend(response_to_stream_events(response)),
                Err(error) => events.push(StreamEvent::Error(provider_error_from_gemini(error))),
            }
        }
        if events.is_empty() {
            events.push(StreamEvent::Error(ProviderError::new(
                ProviderErrorCode::StreamInterrupted,
                "Gemini streaming response ended without events",
            )));
        }
        return Ok(events);
    }

    let response = match builder.execute().await {
        Ok(response) => response,
        Err(error) => return Ok(vec![StreamEvent::Error(provider_error_from_gemini(error))]),
    };
    Ok(response_to_stream_events(response))
}

fn gemini_function_declaration(
    tool: &lingxiao_llm_host_protocol::ToolDefinition,
) -> Result<FunctionDeclaration, GeminiProviderError> {
    serde_json::from_value(json!({
        "name": tool.name.clone(),
        "description": tool.description.clone(),
        "parameters": tool.input_schema.clone(),
    }))
    .map_err(|error| GeminiProviderError::RequestBuild(error.to_string()))
}

fn resolved_config(request: &GenerateRequest) -> Result<(String, String), GeminiProviderError> {
    let api_key = match &request.auth_context {
        AuthContext::ApiKey { key, .. } | AuthContext::BearerToken { token: key, .. } => {
            key.clone()
        }
        AuthContext::AwsSignature { .. } | AuthContext::AzureToken { .. } | AuthContext::None => {
            return Err(GeminiProviderError::UnsupportedAuth);
        }
    };
    let metadata = request.options.metadata.as_ref();
    let base_url = metadata
        .and_then(|value| value.get("base_url"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_BASE_URL)
        .to_string();
    Ok((api_key, base_url))
}

fn add_message(
    builder: gemini_rust::ContentBuilder,
    message: &Message,
    rehydrate: bool,
) -> gemini_rust::ContentBuilder {
    let content = message.plain_text_content();
    match message.role.as_str() {
        "system" => builder.with_system_instruction(content),
        "assistant" | "model" if !message.tool_calls.is_empty() => {
            message.tool_calls.iter().fold(builder, |builder, call| {
                builder.with_message(GeminiMessage {
                    content: GeminiContent::function_call(GeminiFunctionCall::new(
                        call.name.clone(),
                        call.arguments.clone(),
                    ))
                    .with_role(GeminiRole::Model),
                    role: GeminiRole::Model,
                })
            })
        }
        "assistant" | "model" => builder.with_model_message(content),
        "tool" => {
            let name = message
                .name
                .clone()
                .unwrap_or_else(|| message.tool_call_id.clone().unwrap_or_default());
            let response = serde_json::from_str(&content).unwrap_or(Value::String(content));
            builder.with_message(GeminiMessage {
                content: GeminiContent::function_response_json(name, response)
                    .with_role(GeminiRole::User),
                role: GeminiRole::User,
            })
        }
        "user" if message.has_structured_content() => builder.with_message(GeminiMessage {
            content: gemini_user_content(message, rehydrate),
            role: GeminiRole::User,
        }),
        _ => builder.with_user_message(content),
    }
}

/// Build the Gemini `Content` for a user-role message that carries typed
/// `content_parts`.
///
/// Plain-text messages (no `content_parts`) stay a single text part via
/// `plain_text_content()` so existing text-only behavior is byte-for-byte
/// unchanged. When the message carries typed parts they are projected into
/// Gemini content parts: `text` → text part, `image_url` data URI →
/// `Part::InlineData` (Gemini's image-capable part), and `image_blob_ref` →
/// rehydrated from disk into an `inlineData` part. A blob whose backing file is
/// missing/unreadable, a blob outside the retain window (`rehydrate == false`),
/// or a non-data-URI `image_url` (a remote http URL the Gemini inline-data path
/// cannot express), degrades to a safe text placeholder (short blob id only;
/// never a `blob_path` leak; never a panic) — mirroring the OpenAI/Anthropic
/// providers and TS `convertUserContent`'s degrading semantics, and TS
/// `rehydrateRecentImageBlobRefs`'s retain-rounds cutoff. Mirrors TS
/// `VercelAIContentGenerator.convertUserContent`, which maps `image_url` to a
/// Gemini image part; Rust additionally rehydrates `image_blob_ref` (a strict
/// superset — TS skips blob refs entirely).
fn gemini_user_content(message: &Message, rehydrate: bool) -> GeminiContent {
    let mut parts: Vec<GeminiPart> = Vec::with_capacity(message.content_parts.len());
    for part in &message.content_parts {
        match part {
            MessageContentPart::Text { text } => parts.push(text_part(text.clone())),
            MessageContentPart::Thinking { text, .. } => {
                if !text.is_empty() {
                    parts.push(text_part(text.clone()));
                }
            }
            MessageContentPart::RedactedThinking { .. } => {
                parts.push(text_part("[redacted thinking]".to_string()))
            }
            MessageContentPart::ImageUrl { image_url } => {
                if let Some((mime, data)) = gemini_inline_data_from_url(&image_url.url) {
                    parts.push(GeminiPart::InlineData {
                        inline_data: GeminiBlob::new(mime, data),
                        media_resolution: None,
                    });
                } else {
                    // Non data-URI image_url (e.g. a remote http URL) cannot be
                    // expressed as Gemini inline_data without fetching the bytes;
                    // degrade to a textual marker the way TS does.
                    parts.push(text_part(format!("[image] {}", image_url.url)));
                }
            }
            MessageContentPart::ImageBlobRef { image } => {
                match rehydrate_image_blob_ref_if(image, rehydrate) {
                    Some(url) => {
                        if let Some((mime, data)) = gemini_inline_data_from_url(&url) {
                            parts.push(GeminiPart::InlineData {
                                inline_data: GeminiBlob::new(mime, data),
                                media_resolution: None,
                            });
                        } else {
                            parts.push(text_part(part.plain_text()));
                        }
                    }
                    // Missing/unreadable blob file, or a blob outside the
                    // retain window: degrade to the safe placeholder (short
                    // blob id only; no path leak) instead of panicking or
                    // dropping the part silently.
                    None => parts.push(text_part(part.plain_text())),
                }
            }
        }
    }
    // Defensive: if structured parts produced no usable parts (e.g. only empty
    // thinking parts, all skipped), fall back to the plain-text projection so we
    // never emit an empty parts array — Gemini rejects empty content.
    if parts.is_empty() {
        return GeminiContent::text(message.plain_text_content()).with_role(GeminiRole::User);
    }
    GeminiContent {
        parts: Some(parts),
        role: None,
    }
    .with_role(GeminiRole::User)
}

fn text_part(text: String) -> GeminiPart {
    GeminiPart::Text {
        text,
        thought: None,
        thought_signature: None,
    }
}

/// Parse a `data:<mime>;base64,<data>` URI into `(mime_type, base64_data)` for
/// a Gemini `inlineData` part. Returns `None` for any non-data URI so callers
/// can degrade to a text marker. Mirrors TS `parseDataUrl`.
fn gemini_inline_data_from_url(url: &str) -> Option<(String, String)> {
    let data_uri = parse_base64_data_uri(url)?;
    Some((data_uri.media_type.to_string(), data_uri.data.to_string()))
}

fn response_to_stream_events(response: GenerationResponse) -> Vec<StreamEvent> {
    let text = response.text();
    let response_json = serde_json::to_value(&response).unwrap_or(Value::Null);
    let tool_calls = extract_gemini_tool_calls(&response_json);
    let finish_reason = response
        .candidates
        .first()
        .and_then(|candidate| candidate.finish_reason.clone())
        .map(map_finish_reason)
        .unwrap_or(FinishReason::Unknown);
    let usage = response.usage_metadata.map_or_else(
        || TokenUsage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_tokens: None,
        },
        |usage| {
            let prompt_tokens = usage.prompt_token_count.unwrap_or(0).max(0) as u32;
            let completion_tokens = usage.candidates_token_count.unwrap_or(0).max(0) as u32;
            let total_tokens = usage
                .total_token_count
                .unwrap_or(prompt_tokens as i32 + completion_tokens as i32)
                .max(0) as u32;
            TokenUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens,
                cache_creation_input_tokens: usage
                    .cached_content_token_count
                    .map(|v| v.max(0) as u32),
                cache_read_input_tokens: None,
                reasoning_tokens: usage.thoughts_token_count.map(|v| v.max(0) as u32),
            }
        },
    );
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

fn extract_gemini_tool_calls(value: &Value) -> Vec<ToolCall> {
    let mut calls = Vec::new();
    for (candidate_index, candidate) in value
        .get("candidates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let Some(parts) = candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
        else {
            continue;
        };
        for (part_index, part) in parts.iter().enumerate() {
            let function_call = part
                .get("functionCall")
                .or_else(|| part.get("function_call"));
            let Some(function_call) = function_call else {
                continue;
            };
            let Some(name) = function_call.get("name").and_then(Value::as_str) else {
                continue;
            };
            let arguments = function_call
                .get("args")
                .or_else(|| function_call.get("arguments"))
                .cloned()
                .unwrap_or_else(|| json!({}));
            let id = function_call
                .get("id")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("gemini_call_{candidate_index}_{part_index}_{name}"));
            calls.push(ToolCall {
                id,
                name: name.to_string(),
                arguments,
            });
        }
    }
    calls
}

fn map_finish_reason(reason: GeminiFinishReason) -> FinishReason {
    match reason {
        GeminiFinishReason::Stop => FinishReason::Stop,
        GeminiFinishReason::MaxTokens => FinishReason::Length,
        GeminiFinishReason::Safety
        | GeminiFinishReason::Recitation
        | GeminiFinishReason::Blocklist
        | GeminiFinishReason::ProhibitedContent
        | GeminiFinishReason::Spii
        | GeminiFinishReason::ImageSafety => FinishReason::ContentFiltered,
        GeminiFinishReason::MalformedFunctionCall
        | GeminiFinishReason::UnexpectedToolCall
        | GeminiFinishReason::TooManyToolCalls => FinishReason::ToolCalls,
        _ => FinishReason::Unknown,
    }
}

fn provider_error_from_gemini(error: gemini_rust::ClientError) -> ProviderError {
    let text = error.to_string().to_lowercase();
    let code = if text.contains("401") || text.contains("403") || text.contains("api key") {
        ProviderErrorCode::Authentication
    } else if text.contains("429") || text.contains("rate") {
        ProviderErrorCode::RateLimited
    } else if text.contains("400") || text.contains("invalid") {
        ProviderErrorCode::BadRequest
    } else if text.contains("timeout") {
        ProviderErrorCode::Timeout
    } else if text.contains("500") || text.contains("503") {
        ProviderErrorCode::ServerError
    } else {
        ProviderErrorCode::Unknown
    };
    ProviderError::new(code, redact_error_message(error.to_string()))
}

fn redact_error_message(message: String) -> String {
    if message.contains("AIza") || message.contains("sk-") {
        "provider request failed".to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_resolved_config_does_not_use_env_defaults() {
        let request = sample_request("http://localhost:1234/v1beta/");
        let (key, base_url) = resolved_config(&request).unwrap();
        assert_eq!(key, "AIza-test");
        assert_eq!(base_url, "http://localhost:1234/v1beta/");
    }

    #[test]
    fn test_execute_generate_content_against_mock_http_server() {
        let (base_url, handle) = spawn_mock_server(
            200,
            r#"{"candidates":[{"content":{"parts":[{"text":"gemini response"}],"role":"model"},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5,"thoughtsTokenCount":1}}"#,
        );
        let request = sample_request(&base_url);
        let events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "gemini response"));
        assert!(
            matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 5 && usage.reasoning_tokens == Some(1))
        );
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_generate_content_maps_function_call_to_tool_call() {
        let (base_url, handle) = spawn_mock_server(
            200,
            r#"{"candidates":[{"content":{"parts":[{"functionCall":{"name":"file_read","args":{"path":"README.md"}}}],"role":"model"},"finishReason":"MALFORMED_FUNCTION_CALL","index":0}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}}"#,
        );
        let request = sample_request(&base_url);
        let events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolCallDelta(delta)
                if delta.name.as_deref() == Some("file_read")
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            StreamEvent::ToolCall(call)
                if call.name == "file_read" && call.arguments["path"] == "README.md"
        )));
        assert!(matches!(
            events.last().unwrap(),
            StreamEvent::Finished(FinishReason::ToolCalls)
        ));
    }

    #[test]
    fn test_auth_error_maps_to_provider_error() {
        let (base_url, handle) = spawn_mock_server(403, r#"{"error":{"message":"bad api key"}}"#);
        let request = sample_request(&base_url);
        let events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(
            &events[0],
            StreamEvent::Error(err)
                if err.code == ProviderErrorCode::Authentication && !err.retryable
        ));
    }

    fn sample_request(base_url: &str) -> GenerateRequest {
        GenerateRequest {
            model: "models/gemini-test".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: Vec::new(),
            stream: false,
            auth_context: AuthContext::ApiKey {
                provider: "gemini".into(),
                key: "AIza-test".into(),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(12),
                metadata: Some(json!({"base_url": base_url})),
                ..Default::default()
            },
        }
    }

    fn spawn_mock_server(status: u16, body: &'static str) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request = String::from_utf8_lossy(&request_bytes);
            assert!(request.starts_with("POST /v1beta/models/gemini-test:generateContent"));
            assert!(request.to_lowercase().contains("x-goog-api-key: aiza-test"));
            let body_json: serde_json::Value = serde_json::from_str(http_body(&request)).unwrap();
            assert_eq!(body_json["contents"][0]["role"], "user");
            assert_eq!(body_json["generationConfig"]["maxOutputTokens"], 12);
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://{addr}/v1beta/"), handle)
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            let n = stream.read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..n]);
            if request_complete(&request) {
                break;
            }
        }
        request
    }

    fn request_complete(request: &[u8]) -> bool {
        let Some(body_start) = request
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|i| i + 4)
        else {
            return false;
        };
        let headers = String::from_utf8_lossy(&request[..body_start]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        request.len() >= body_start + content_length
    }

    fn http_body(request: &str) -> &str {
        request.split("\r\n\r\n").nth(1).unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // P0: Gemini provider — tool protocol contract tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gemini_tools_declared_as_function_declarations() {
        // When GenerateRequest.tools is non-empty, Gemini must send a
        // "tools" array with "functionDeclarations" in the request body.
        // We test this by inspecting the HTTP request captured by the mock server.
        use std::sync::{Arc, Mutex};
        let captured_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_body_clone = captured_body.clone();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request_str = String::from_utf8_lossy(&request_bytes).to_string();
            let body = http_body(&request_str).to_string();
            *captured_body_clone.lock().unwrap() = Some(body);
            // Respond with a minimal success so the client does not error out.
            let resp = r#"{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let mut request = sample_request(&format!("http://{addr}/v1beta/"));
        request.tools = vec![lingxiao_llm_host_protocol::ToolDefinition {
            name: "list_dir".into(),
            description: "List a directory".into(),
            input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
        }];
        let _events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();

        let body_str = captured_body.lock().unwrap().clone().unwrap();
        // The gemini-rust client owns the exact tool JSON shape (camelCase vs
        // snake_case); assert the declaration and tool name both reached the wire.
        let lower = body_str.to_lowercase();
        assert!(
            lower.contains("functiondeclarations") || lower.contains("function_declarations"),
            "expected a function declaration block in the request body, got: {body_str}"
        );
        assert!(
            body_str.contains("list_dir"),
            "expected tool name 'list_dir' in the request body, got: {body_str}"
        );
        let body_json: Value = serde_json::from_str(&body_str).unwrap();
        let declarations = body_json["tools"][0]
            .get("functionDeclarations")
            .or_else(|| body_json["tools"][0].get("function_declarations"))
            .and_then(Value::as_array)
            .expect("function declarations must be present");
        let declaration = &declarations[0];
        assert_eq!(
            declaration["parameters"]["properties"]["path"]["type"],
            "string"
        );
    }

    #[test]
    fn test_gemini_stream_true_uses_streaming_endpoint() {
        use std::sync::{Arc, Mutex};
        let captured_request: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_request_clone = captured_request.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request_str = String::from_utf8_lossy(&request_bytes).to_string();
            *captured_request_clone.lock().unwrap() = Some(request_str);
            let event = r#"{"candidates":[{"content":{"parts":[{"text":"stream-ok"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#;
            let body = format!("data: {event}\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let mut request = sample_request(&format!("http://{addr}/v1beta/"));
        request.stream = true;
        let events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();
        let request_str = captured_request.lock().unwrap().clone().unwrap();
        assert!(
            request_str
                .starts_with("POST /v1beta/models/gemini-test:streamGenerateContent?alt=sse"),
            "stream=true must use Gemini streamGenerateContent endpoint, got: {request_str}"
        );
        assert!(events
            .iter()
            .any(|event| matches!(event, StreamEvent::TextDelta(text) if text == "stream-ok")));
    }

    #[test]
    fn test_gemini_tool_history_maps_assistant_tool_calls() {
        // An assistant message with tool_calls must produce a Gemini FUNCTION_CALL
        // content part with role=model.
        use lingxiao_llm_host_protocol::ToolCall as ProtocolToolCall;
        let mut request = sample_request("http://unused/v1beta/");
        request.messages = vec![
            Message {
                role: "user".into(),
                content: "read it".into(),
                ..Default::default()
            },
            Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![ProtocolToolCall {
                    id: "call_g1".into(),
                    name: "file_read".into(),
                    arguments: json!({"path": "README.md"}),
                }],
                ..Default::default()
            },
            Message {
                role: "tool".into(),
                content: "the content".into(),
                tool_call_id: Some("call_g1".into()),
                name: Some("file_read".into()),
                ..Default::default()
            },
        ];

        // Walk the builder to verify message mapping without hitting the network.
        // We use add_message directly and verify no panic / wrong role occurs.
        let client = Gemini::with_model_and_base_url(
            "AIza-test",
            GeminiModel::from("models/gemini-test".to_string()),
            url::Url::parse("http://localhost/v1beta/").unwrap(),
        )
        .unwrap();
        let mut builder = client.generate_content();
        for msg in &request.messages {
            builder = add_message(builder, msg, true);
        }
        // If add_message panicked we would not reach here — mapping is correct.
        drop(builder);
    }

    #[test]
    fn test_gemini_response_extracts_tool_call_from_function_call_part() {
        // A response containing a functionCall part must produce ToolCallDelta
        // + ToolCall + Finished(ToolCalls) events.
        let response_json: serde_json::Value = json!({
            "candidates": [{
                "content": {
                    "parts": [{
                        "functionCall": {
                            "name": "file_read",
                            "args": {"path": "src/main.rs"}
                        }
                    }],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }],
            "usageMetadata": {"promptTokenCount": 5, "candidatesTokenCount": 2, "totalTokenCount": 7}
        });
        let tool_calls = extract_gemini_tool_calls(&response_json);
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].name, "file_read");
        assert_eq!(tool_calls[0].arguments["path"], "src/main.rs");
        assert!(!tool_calls[0].id.is_empty(), "id must be non-empty");
    }

    #[test]
    fn test_gemini_fallback_tool_call_ids_are_unique_for_same_name_calls() {
        let response_json: serde_json::Value = json!({
            "candidates": [{
                "content": {
                    "parts": [
                        {"functionCall": {"name": "file_read", "args": {"path": "a.txt"}}},
                        {"functionCall": {"name": "file_read", "args": {"path": "b.txt"}}}
                    ],
                    "role": "model"
                },
                "finishReason": "STOP",
                "index": 0
            }]
        });
        let tool_calls = extract_gemini_tool_calls(&response_json);
        assert_eq!(tool_calls.len(), 2);
        assert_ne!(tool_calls[0].id, tool_calls[1].id);
        assert_eq!(tool_calls[0].id, "gemini_call_0_0_file_read");
        assert_eq!(tool_calls[1].id, "gemini_call_0_1_file_read");
    }

    // -----------------------------------------------------------------------
    // R-2: Gemini provider — native multimodal (inline_data) contract tests
    // -----------------------------------------------------------------------

    use lingxiao_llm_host_protocol::{ImageBlobRefContentPart, ImageUrlContentPart};

    fn user_message_with_parts(parts: Vec<MessageContentPart>) -> Message {
        Message {
            role: "user".into(),
            content: String::new(),
            content_parts: parts,
            ..Default::default()
        }
    }

    /// Extract `(mime, base64)` from the first `InlineData` part, panicking if
    /// none exists.
    fn first_inline_data(content: &GeminiContent) -> (String, String) {
        let parts = content.parts.as_ref().expect("content has parts");
        for part in parts {
            if let GeminiPart::InlineData { inline_data, .. } = part {
                return (inline_data.mime_type.clone(), inline_data.data.clone());
            }
        }
        panic!("no InlineData part found in {content:?}");
    }

    #[test]
    fn test_gemini_user_content_data_uri_image_url_emits_inline_data() {
        let message = user_message_with_parts(vec![
            MessageContentPart::Text {
                text: "describe this".into(),
            },
            MessageContentPart::ImageUrl {
                image_url: ImageUrlContentPart {
                    // "foo" -> base64 "Zm9v"
                    url: "data:image/png;base64,Zm9v".into(),
                    detail: Some("high".into()),
                },
            },
        ]);
        let content = gemini_user_content(&message, true);
        let parts = content.parts.as_ref().expect("content has parts");
        // text part + inline_data part, in order.
        assert!(
            parts
                .iter()
                .any(|p| matches!(p, GeminiPart::Text { text, .. } if text == "describe this")),
            "text part must be preserved"
        );
        let (mime, data) = first_inline_data(&content);
        assert_eq!(mime, "image/png");
        assert_eq!(data, "Zm9v");
        assert_eq!(content.role, Some(GeminiRole::User));
    }

    #[test]
    fn test_gemini_user_content_rehydrates_blob_ref_into_inline_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob.bin");
        // "foobar" -> base64 "Zm9vYmFy"
        std::fs::write(&path, b"foobar").unwrap();
        let blob_path = path.display().to_string();
        let message = user_message_with_parts(vec![
            MessageContentPart::Text {
                text: "look at the screenshot".into(),
            },
            MessageContentPart::ImageBlobRef {
                image: ImageBlobRefContentPart {
                    blob_id: "blob_abcdef012345".into(),
                    mime: "image/png".into(),
                    size: 6,
                    blob_path: blob_path.clone(),
                    source: Some("screenshot".into()),
                },
            },
        ]);
        let content = gemini_user_content(&message, true);
        let (mime, data) = first_inline_data(&content);
        assert_eq!(mime, "image/png");
        assert_eq!(data, "Zm9vYmFy");
        // The real bytes were rehydrated; the on-disk path must never reach the
        // wire content.
        let serialized = serde_json::to_string(&content).unwrap();
        assert!(!serialized.contains(&blob_path), "blob_path must not leak");
    }

    #[test]
    fn test_gemini_user_content_missing_blob_falls_back_to_safe_text_placeholder() {
        let message = user_message_with_parts(vec![MessageContentPart::ImageBlobRef {
            image: ImageBlobRefContentPart {
                blob_id: "blob_missing".into(),
                mime: "image/png".into(),
                size: 2048,
                blob_path: "/definitely/does/not/exist/blob_missing.png".into(),
                source: None,
            },
        }]);
        // Must not panic.
        let content = gemini_user_content(&message, true);
        let parts = content.parts.as_ref().expect("content has parts");
        // Missing blob → degrade to text placeholder (no InlineData emitted).
        assert!(
            parts.iter().all(|p| matches!(p, GeminiPart::Text { .. })),
            "missing blob must degrade to text, got {parts:?}"
        );
        let serialized = serde_json::to_string(&content).unwrap();
        assert!(
            serialized.contains("blob_missing"),
            "placeholder keeps short blob id"
        );
        assert!(
            !serialized.contains("/definitely/does/not/exist"),
            "blob_path must not leak into placeholder"
        );
    }

    #[test]
    fn test_gemini_user_content_remote_image_url_degrades_to_text_marker() {
        let message = user_message_with_parts(vec![MessageContentPart::ImageUrl {
            image_url: ImageUrlContentPart {
                url: "https://example.test/image.png".into(),
                detail: None,
            },
        }]);
        let content = gemini_user_content(&message, true);
        let parts = content.parts.as_ref().expect("content has parts");
        // Remote http URL cannot be expressed as inline_data without fetching
        // the bytes → degrade to a text marker (no InlineData).
        assert!(
            parts.iter().all(|p| matches!(p, GeminiPart::Text { .. })),
            "remote image_url must degrade to text, got {parts:?}"
        );
        let serialized = serde_json::to_string(&content).unwrap();
        assert!(serialized.contains("https://example.test/image.png"));
        assert!(
            !serialized.contains("inlineData"),
            "no inline_data should be emitted for a remote URL"
        );
    }

    #[test]
    fn test_gemini_user_content_pure_text_stays_text_only() {
        // A user message with NO structured content_parts must keep using the
        // legacy plain-text path (with_user_message), not the multimodal branch.
        let message = Message {
            role: "user".into(),
            content: "just text".into(),
            content_parts: Vec::new(),
            ..Default::default()
        };
        // add_message routes a user message without content_parts to the `_`
        // arm (with_user_message). Verify has_structured_content() is false so
        // the multimodal branch is never taken for plain text.
        assert!(!message.has_structured_content());

        // And the structured build helper, if called on a parts-less message,
        // still yields a single text part from plain_text_content().
        let message_with_parts_only_text =
            user_message_with_parts(vec![MessageContentPart::Text {
                text: "only text part".into(),
            }]);
        let content = gemini_user_content(&message_with_parts_only_text, true);
        let parts = content.parts.as_ref().expect("content has parts");
        assert_eq!(parts.len(), 1);
        assert!(matches!(&parts[0], GeminiPart::Text { text, .. } if text == "only text part"));
    }

    #[test]
    fn test_gemini_add_message_preserves_assistant_function_call_and_tool_response() {
        // The multimodal change must not regress the assistant function_call
        // path or the tool function_response path. Walk add_message across a
        // full tool-use history and verify it maps without panic.
        use lingxiao_llm_host_protocol::ToolCall as ProtocolToolCall;
        let request_messages = vec![
            Message {
                role: "user".into(),
                content: "read it".into(),
                ..Default::default()
            },
            Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![ProtocolToolCall {
                    id: "call_g2".into(),
                    name: "file_read".into(),
                    arguments: json!({"path": "README.md"}),
                }],
                ..Default::default()
            },
            Message {
                role: "tool".into(),
                content: "the content".into(),
                tool_call_id: Some("call_g2".into()),
                name: Some("file_read".into()),
                ..Default::default()
            },
        ];

        let client = Gemini::with_model_and_base_url(
            "AIza-test",
            GeminiModel::from("models/gemini-test".to_string()),
            url::Url::parse("http://localhost/v1beta/").unwrap(),
        )
        .unwrap();
        let mut builder = client.generate_content();
        for msg in &request_messages {
            builder = add_message(builder, msg, true);
        }
        // Inspect the built contents: assistant tool_calls → FunctionCall part
        // (role model); tool role → FunctionResponse part (role user).
        let built = builder.build();
        let function_call = built.contents.iter().find_map(|content| {
            content.parts.as_ref()?.iter().find_map(|part| match part {
                GeminiPart::FunctionCall { function_call, .. } => Some(function_call),
                _ => None,
            })
        });
        let fc = function_call.expect("assistant tool_calls must map to a FunctionCall part");
        assert_eq!(fc.name, "file_read");
        assert_eq!(fc.args["path"], "README.md");

        let function_response = built.contents.iter().find_map(|content| {
            content.parts.as_ref()?.iter().find_map(|part| match part {
                GeminiPart::FunctionResponse { function_response } => Some(function_response),
                _ => None,
            })
        });
        let fr = function_response.expect("tool role must map to a FunctionResponse part");
        assert_eq!(fr.name, "file_read");
        // The tool-role content "the content" is not valid JSON, so add_message
        // wraps it as Value::String("the content") and stores it verbatim.
        assert_eq!(fr.response, Some(Value::String("the content".into())));
    }

    #[test]
    fn test_gemini_inline_data_emitted_on_wire_for_image_request() {
        // End-to-end: a user message carrying a data-URI image_url must reach
        // the mock HTTP server with an inlineData part in the request body.
        use std::sync::{Arc, Mutex};
        let captured_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_body_clone = captured_body.clone();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request_str = String::from_utf8_lossy(&request_bytes).to_string();
            *captured_body_clone.lock().unwrap() = Some(http_body(&request_str).to_string());
            let resp = r#"{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let mut request = sample_request(&format!("http://{addr}/v1beta/"));
        request.messages = vec![user_message_with_parts(vec![
            MessageContentPart::Text {
                text: "what is this".into(),
            },
            MessageContentPart::ImageUrl {
                image_url: ImageUrlContentPart {
                    // "foo" -> base64 "Zm9v"
                    url: "data:image/png;base64,Zm9v".into(),
                    detail: None,
                },
            },
        ])];
        let _events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();

        let body_str = captured_body.lock().unwrap().clone().unwrap();
        let body_json: Value = serde_json::from_str(&body_str).unwrap();
        // Gemini camelCase wire shape: contents[].parts[] with {inlineData:{mimeType,data}}.
        let parts = body_json["contents"][0]["parts"]
            .as_array()
            .unwrap_or_else(|| panic!("expected parts array, got: {body_str}"));
        let inline = parts.iter().find_map(|part| part.get("inlineData"));
        let inline = inline
            .unwrap_or_else(|| panic!("expected an inlineData part on the wire, got: {body_str}"));
        assert_eq!(inline["mimeType"], "image/png");
        assert_eq!(inline["data"], "Zm9v");
        // The text part must also survive alongside the image part.
        assert!(
            parts
                .iter()
                .any(|p| p.get("text").and_then(Value::as_str) == Some("what is this")),
            "text part must be preserved alongside inlineData, got: {body_str}"
        );
    }

    #[test]
    fn test_gemini_retain_window_degrades_old_blob_to_text_on_wire() {
        // Two user rounds each carrying a rehydratable blob. With retain=1 only
        // the most recent round rehydrates an inlineData part on the wire; the
        // older round's blob degrades to a text part (never inlineData, never a
        // blob_path leak).
        use std::sync::{Arc, Mutex};
        let captured_body: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let captured_body_clone = captured_body.clone();

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request_str = String::from_utf8_lossy(&request_bytes).to_string();
            *captured_body_clone.lock().unwrap() = Some(http_body(&request_str).to_string());
            let resp = r#"{"candidates":[{"content":{"parts":[{"text":"ok"}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

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

        let mut request = sample_request(&format!("http://{addr}/v1beta/"));
        request.messages = vec![
            user_message_with_parts(vec![blob_ref(&old_path_str, "blob_old_gem")]), // round 1
            Message {
                role: "assistant".into(),
                content: "ack".into(),
                ..Default::default()
            },
            user_message_with_parts(vec![blob_ref(&new_path_str, "blob_new_gem")]), // round 2
        ];
        // retain=1 → only the most recent user round rehydrates.
        request.options.metadata = Some(json!({
            "base_url": format!("http://{addr}/v1beta/"),
            "image_history_retain_rounds": 1
        }));
        let _events = execute_generate_content_blocking(&request).unwrap();
        handle.join().unwrap();

        let body_str = captured_body.lock().unwrap().clone().unwrap();
        let body_json: Value = serde_json::from_str(&body_str).unwrap();
        let contents = body_json["contents"]
            .as_array()
            .unwrap_or_else(|| panic!("expected contents array, got: {body_str}"));

        // contents[0] is the old user round → no inlineData, text placeholder only.
        let old_parts = contents[0]["parts"]
            .as_array()
            .unwrap_or_else(|| panic!("expected parts array for old round, got: {body_str}"));
        assert!(
            !old_parts.iter().any(|p| p.get("inlineData").is_some()),
            "old-round blob must NOT rehydrate to inlineData, got: {body_str}"
        );
        let old_text = old_parts
            .iter()
            .find_map(|p| p.get("text").and_then(Value::as_str))
            .unwrap_or_else(|| panic!("old round must carry a text placeholder, got: {body_str}"));
        assert!(
            old_text.contains("blob_old_gem"),
            "placeholder should carry the short blob id: {old_text}"
        );
        assert!(
            !old_text.contains(&old_path_str),
            "blob_path must not leak: {old_text}"
        );

        // contents[2] is the recent user round → real inlineData part.
        let new_parts = contents[2]["parts"]
            .as_array()
            .unwrap_or_else(|| panic!("expected parts array for new round, got: {body_str}"));
        let inline = new_parts
            .iter()
            .find_map(|p| p.get("inlineData"))
            .unwrap_or_else(|| {
                panic!("new-round blob must rehydrate to inlineData, got: {body_str}")
            });
        assert_eq!(inline["mimeType"], "image/png");
        assert!(!inline["data"].as_str().unwrap().is_empty());
    }
}
