use anthropic_sdk::types::ToolInputSchema;
use anthropic_sdk::{
    Anthropic, AuthMethod, ClientConfig, ContentBlock, ContentBlockDelta, ContentBlockParam,
    LogLevel, MessageContent, MessageCreateBuilder, MessageStreamEvent, Role, StopReason, Tool,
};
use futures::StreamExt;
use lingxiao_llm_host_protocol::{
    AuthContext, FinishReason, GenerateRequest, Message, ProviderError, ProviderErrorCode,
    StreamEvent, TokenUsage,
};
use serde_json::{Map, Value};
use std::io::{self, BufRead, Write};
use std::time::Duration;
use thiserror::Error;

const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

#[derive(Debug, Error)]
pub enum AnthropicProviderError {
    #[error("stdin read failed: {0}")]
    Stdin(String),
    #[error("request decode failed: {0}")]
    Decode(String),
    #[error("request encode failed: {0}")]
    Encode(String),
    #[error("unsupported auth context")]
    UnsupportedAuth,
    #[error("provider request failed: {0}")]
    Provider(String),
}

pub fn run_stdio() -> Result<(), AnthropicProviderError> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| AnthropicProviderError::Stdin(e.to_string()))?;
    let request: GenerateRequest =
        serde_json::from_str(&line).map_err(|e| AnthropicProviderError::Decode(e.to_string()))?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stream_messages_blocking(&request, |event| {
        let line = serde_json::to_string(&event)
            .map_err(|e| AnthropicProviderError::Encode(e.to_string()))?;
        writeln!(stdout, "{line}").map_err(|e| AnthropicProviderError::Stdin(e.to_string()))?;
        stdout
            .flush()
            .map_err(|e| AnthropicProviderError::Stdin(e.to_string()))
    })?;
    stdout
        .flush()
        .map_err(|e| AnthropicProviderError::Stdin(e.to_string()))?;
    Ok(())
}

pub fn execute_messages_blocking(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, AnthropicProviderError> {
    let mut events = Vec::new();
    stream_messages_blocking(request, |event| {
        events.push(event);
        Ok(())
    })?;
    Ok(events)
}

pub fn stream_messages_blocking<F>(
    request: &GenerateRequest,
    mut sink: F,
) -> Result<(), AnthropicProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), AnthropicProviderError>,
{
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| AnthropicProviderError::Provider(e.to_string()))?;
    runtime.block_on(execute_messages_streaming(request, &mut sink))
}

pub async fn execute_messages(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, AnthropicProviderError> {
    let mut events = Vec::new();
    execute_messages_streaming(request, &mut |event| {
        events.push(event);
        Ok(())
    })
    .await?;
    Ok(events)
}

async fn execute_messages_streaming<F>(
    request: &GenerateRequest,
    sink: &mut F,
) -> Result<(), AnthropicProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), AnthropicProviderError>,
{
    let config = resolved_config(request)?;
    let client = Anthropic::with_config(config)
        .map_err(|e| AnthropicProviderError::Provider(e.to_string()))?;
    if request.stream {
        let timeout = Duration::from_millis(request.options.timeout_ms_hint.unwrap_or(600_000));
        return match tokio::time::timeout(timeout, execute_messages_stream(&client, request, sink))
            .await
        {
            Ok(result) => result,
            Err(_) => sink(StreamEvent::Error(ProviderError::new(
                ProviderErrorCode::Timeout,
                format!(
                    "Anthropic streaming request timed out after {} ms",
                    timeout.as_millis()
                ),
            ))),
        };
    }
    let response = match client.messages().create(to_message_params(request)).await {
        Ok(response) => response,
        Err(error) => {
            return sink(StreamEvent::Error(provider_error_from_anthropic(error)));
        }
    };
    for event in message_to_stream_events(response) {
        sink(event)?;
    }
    Ok(())
}

async fn execute_messages_stream<F>(
    client: &Anthropic,
    request: &GenerateRequest,
    sink: &mut F,
) -> Result<(), AnthropicProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), AnthropicProviderError>,
{
    let mut stream = match client
        .messages()
        .create_stream(to_message_params(request))
        .await
    {
        Ok(stream) => stream,
        Err(error) => return sink(StreamEvent::Error(provider_error_from_anthropic(error))),
    };
    let mut tool_blocks: Vec<Option<(String, String, String)>> = Vec::new();
    let mut finished_emitted = false;
    while let Some(event) = stream.next().await {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                sink(StreamEvent::Error(provider_error_from_anthropic(error)))?;
                continue;
            }
        };
        emit_anthropic_stream_events(event, &mut tool_blocks, &mut finished_emitted, sink)?;
    }
    Ok(())
}

fn emit_anthropic_stream_events<F>(
    event: MessageStreamEvent,
    tool_blocks: &mut Vec<Option<(String, String, String)>>,
    finished_emitted: &mut bool,
    sink: &mut F,
) -> Result<(), AnthropicProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), AnthropicProviderError>,
{
    for protocol_event in anthropic_stream_event_to_events(event, tool_blocks) {
        if matches!(protocol_event, StreamEvent::Finished(_)) {
            if *finished_emitted {
                continue;
            }
            *finished_emitted = true;
        }
        sink(protocol_event)?;
    }
    Ok(())
}

fn resolved_config(request: &GenerateRequest) -> Result<ClientConfig, AnthropicProviderError> {
    let (key, auth_method) = match &request.auth_context {
        AuthContext::ApiKey { key, .. } => (key.clone(), AuthMethod::Anthropic),
        AuthContext::BearerToken { token, .. } => (token.clone(), AuthMethod::Bearer),
        AuthContext::AwsSignature { .. } | AuthContext::AzureToken { .. } | AuthContext::None => {
            return Err(AnthropicProviderError::UnsupportedAuth);
        }
    };
    let metadata = request.options.metadata.as_ref();
    let base_url = metadata
        .and_then(|value| value.get("base_url"))
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_BASE_URL);
    let timeout = request.options.timeout_ms_hint.unwrap_or(600_000);
    Ok(ClientConfig::new(key)
        .with_base_url(base_url)
        .with_timeout(Duration::from_millis(timeout))
        .with_max_retries(0)
        .with_log_level(LogLevel::Off)
        .with_auth_method(auth_method))
}

fn to_message_params(request: &GenerateRequest) -> anthropic_sdk::MessageCreateParams {
    let mut builder = MessageCreateBuilder::new(
        request.model.clone(),
        request.options.max_tokens.unwrap_or(1024),
    )
    .stream(false);

    for message in &request.messages {
        builder = add_message(builder, message);
    }
    if let Some(temperature) = request.options.temperature {
        builder = builder.temperature(temperature);
    }
    if let Some(top_p) = request.options.top_p {
        builder = builder.top_p(top_p);
    }
    if let Some(stop) = &request.options.stop {
        builder = builder.stop_sequences(stop.clone());
    }
    if !request.tools.is_empty() {
        builder = builder.tools(anthropic_tools(request));
    }
    builder.build()
}

fn add_message(builder: MessageCreateBuilder, message: &Message) -> MessageCreateBuilder {
    match message.role.as_str() {
        "system" => builder.system(message.content.clone()),
        "assistant" => {
            if message.tool_calls.is_empty() {
                builder.message(
                    Role::Assistant,
                    MessageContent::Text(message.content.clone()),
                )
            } else {
                let mut blocks = Vec::new();
                if !message.content.is_empty() {
                    blocks.push(ContentBlockParam::Text {
                        text: message.content.clone(),
                    });
                }
                for tool_call in &message.tool_calls {
                    blocks.push(ContentBlockParam::ToolUse {
                        id: tool_call.id.clone(),
                        name: tool_call.name.clone(),
                        input: tool_call.arguments.clone(),
                    });
                }
                builder.message(Role::Assistant, MessageContent::Blocks(blocks))
            }
        }
        "tool" => builder.message(
            Role::User,
            MessageContent::Blocks(vec![ContentBlockParam::ToolResult {
                tool_use_id: message.tool_call_id.clone().unwrap_or_default(),
                content: Some(message.content.clone()),
                is_error: None,
            }]),
        ),
        _ => builder.message(Role::User, MessageContent::Text(message.content.clone())),
    }
}

fn anthropic_tools(request: &GenerateRequest) -> Vec<Tool> {
    request
        .tools
        .iter()
        .map(|tool| {
            let schema = tool.input_schema.as_object();
            let properties = schema
                .and_then(|schema| schema.get("properties"))
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default();
            let required = schema
                .and_then(|schema| schema.get("required"))
                .and_then(Value::as_array)
                .map(|required| {
                    required
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default();
            let mut additional = Map::new();
            if let Some(schema) = schema {
                for (key, value) in schema {
                    if !matches!(key.as_str(), "type" | "properties" | "required") {
                        additional.insert(key.clone(), value.clone());
                    }
                }
            }
            Tool {
                name: tool.name.clone(),
                description: tool.description.clone(),
                input_schema: ToolInputSchema {
                    schema_type: "object".into(),
                    properties,
                    required,
                    additional,
                },
            }
        })
        .collect()
}

fn anthropic_stream_event_to_events(
    event: MessageStreamEvent,
    tool_blocks: &mut Vec<Option<(String, String, String)>>,
) -> Vec<StreamEvent> {
    match event {
        MessageStreamEvent::ContentBlockStart {
            content_block,
            index,
        } => {
            if tool_blocks.len() <= index {
                tool_blocks.resize_with(index + 1, || None);
            }
            if let ContentBlock::ToolUse { id, name, input } = content_block {
                let partial_json = if input.is_null() {
                    String::new()
                } else {
                    input.to_string()
                };
                tool_blocks[index] = Some((id.clone(), name.clone(), partial_json.clone()));
                return vec![StreamEvent::ToolCallDelta(
                    lingxiao_llm_host_protocol::ToolCallDelta {
                        index: index.min(u32::MAX as usize) as u32,
                        id: Some(id),
                        name: Some(name),
                        partial_json: if partial_json.is_empty() {
                            None
                        } else {
                            Some(partial_json)
                        },
                    },
                )];
            }
            Vec::new()
        }
        MessageStreamEvent::ContentBlockDelta { delta, index } => match delta {
            ContentBlockDelta::TextDelta { text } => vec![StreamEvent::TextDelta(text)],
            ContentBlockDelta::ThinkingDelta { thinking } => {
                vec![StreamEvent::ThinkingDelta(thinking)]
            }
            ContentBlockDelta::InputJsonDelta { partial_json } => {
                if tool_blocks.len() <= index {
                    tool_blocks.resize_with(index + 1, || None);
                }
                if let Some((_, _, accumulated)) = tool_blocks[index].as_mut() {
                    accumulated.push_str(&partial_json);
                }
                vec![StreamEvent::ToolCallDelta(
                    lingxiao_llm_host_protocol::ToolCallDelta {
                        index: index.min(u32::MAX as usize) as u32,
                        id: tool_blocks[index].as_ref().map(|(id, _, _)| id.clone()),
                        name: tool_blocks[index].as_ref().map(|(_, name, _)| name.clone()),
                        partial_json: Some(partial_json),
                    },
                )]
            }
            ContentBlockDelta::CitationsDelta { .. } | ContentBlockDelta::SignatureDelta { .. } => {
                Vec::new()
            }
        },
        MessageStreamEvent::ContentBlockStop { index } => {
            let Some(Some((id, name, arguments))) = tool_blocks.get_mut(index).map(Option::take)
            else {
                return Vec::new();
            };
            let parsed_arguments = serde_json::from_str(&arguments).unwrap_or(Value::Null);
            vec![StreamEvent::ToolCall(
                lingxiao_llm_host_protocol::ToolCall {
                    id,
                    name,
                    arguments: parsed_arguments,
                },
            )]
        }
        MessageStreamEvent::MessageDelta { delta, usage } => {
            let mut events = vec![StreamEvent::Usage(TokenUsage {
                prompt_tokens: usage.input_tokens.unwrap_or(0),
                completion_tokens: usage.output_tokens,
                total_tokens: usage
                    .input_tokens
                    .unwrap_or(0)
                    .saturating_add(usage.output_tokens),
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
                reasoning_tokens: None,
            })];
            if let Some(reason) = delta.stop_reason {
                events.push(StreamEvent::Finished(map_stop_reason(reason)));
            }
            events
        }
        MessageStreamEvent::MessageStop => vec![StreamEvent::Finished(FinishReason::Stop)],
        MessageStreamEvent::MessageStart { .. } => Vec::new(),
    }
}

fn message_to_stream_events(message: anthropic_sdk::Message) -> Vec<StreamEvent> {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for block in &message.content {
        match block {
            ContentBlock::Text { text: part } => text.push_str(part),
            ContentBlock::ToolUse { id, name, input } => {
                tool_calls.push(lingxiao_llm_host_protocol::ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    arguments: input.clone(),
                });
            }
            _ => {}
        }
    }
    let finish_reason = message
        .stop_reason
        .map(map_stop_reason)
        .unwrap_or(FinishReason::Unknown);
    let usage = TokenUsage {
        prompt_tokens: message.usage.input_tokens,
        completion_tokens: message.usage.output_tokens,
        total_tokens: message.usage.total_tokens(),
        cache_creation_input_tokens: message.usage.cache_creation_input_tokens,
        cache_read_input_tokens: message.usage.cache_read_input_tokens,
        reasoning_tokens: None,
    };
    let mut events = Vec::new();
    if !text.is_empty() {
        events.push(StreamEvent::TextDelta(text));
    }
    for (index, tool_call) in tool_calls.iter().enumerate() {
        events.push(StreamEvent::ToolCallDelta(
            lingxiao_llm_host_protocol::ToolCallDelta {
                index: index as u32,
                id: Some(tool_call.id.clone()),
                name: Some(tool_call.name.clone()),
                partial_json: Some(tool_call.arguments.to_string()),
            },
        ));
        events.push(StreamEvent::ToolCall(tool_call.clone()));
    }
    events.push(StreamEvent::Usage(usage));
    events.push(StreamEvent::Finished(if tool_calls.is_empty() {
        finish_reason
    } else {
        FinishReason::ToolCalls
    }));
    events
}

fn map_stop_reason(reason: StopReason) -> FinishReason {
    match reason {
        StopReason::EndTurn | StopReason::StopSequence => FinishReason::Stop,
        StopReason::MaxTokens => FinishReason::Length,
        StopReason::ToolUse => FinishReason::ToolCalls,
    }
}

fn provider_error_from_anthropic(error: anthropic_sdk::AnthropicError) -> ProviderError {
    let code = match error {
        anthropic_sdk::AnthropicError::Authentication { .. }
        | anthropic_sdk::AnthropicError::InvalidApiKey => ProviderErrorCode::Authentication,
        anthropic_sdk::AnthropicError::RateLimit { .. } => ProviderErrorCode::RateLimited,
        anthropic_sdk::AnthropicError::BadRequest { .. }
        | anthropic_sdk::AnthropicError::UnprocessableEntity { .. }
        | anthropic_sdk::AnthropicError::Configuration { .. } => ProviderErrorCode::BadRequest,
        anthropic_sdk::AnthropicError::Timeout
        | anthropic_sdk::AnthropicError::ConnectionTimeout => ProviderErrorCode::Timeout,
        anthropic_sdk::AnthropicError::InternalServer { .. }
        | anthropic_sdk::AnthropicError::ServiceUnavailable { .. } => {
            ProviderErrorCode::ServerError
        }
        anthropic_sdk::AnthropicError::StreamError(_) => ProviderErrorCode::StreamInterrupted,
        _ => ProviderErrorCode::Unknown,
    };
    ProviderError::new(code, redact_error_message(error.to_string()))
}

fn redact_error_message(message: String) -> String {
    if message.contains("sk-") {
        "provider request failed".to_string()
    } else {
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anthropic_sdk::{MessageDelta, MessageDeltaUsage};
    use serde_json::json;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_request_shape_uses_anthropic_sdk_types() {
        let request = sample_request("http://localhost:1234");
        let params = to_message_params(&request);
        let json = serde_json::to_value(params).unwrap();
        assert_eq!(json["model"], "claude-test");
        assert_eq!(json["max_tokens"], 12);
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["content"], "hello");
        assert_eq!(json["stream"], false);
    }

    #[test]
    fn test_resolved_config_disables_sdk_retry_and_env_defaults() {
        let request = sample_request("http://localhost:1234");
        let config = resolved_config(&request).unwrap();
        assert_eq!(config.base_url, "http://localhost:1234");
        assert_eq!(config.max_retries, 0);
        assert_eq!(config.api_key, "sk-ant-test");
    }

    #[test]
    fn test_execute_messages_against_mock_http_server() {
        let (base_url, handle) = spawn_mock_server(
            200,
            r#"{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"text","text":"anthropic response"}],"model":"claude-test","stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":2,"output_tokens":3}}"#,
        );
        let mut request = sample_request(&base_url);
        request.stream = false;
        let events = execute_messages_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "anthropic response"));
        assert!(matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 5));
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_message_tool_use_maps_to_stream_tool_call() {
        let message = anthropic_sdk::Message {
            id: "msg_tool".into(),
            type_: "message".into(),
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "file_read".into(),
                input: json!({"path": "README.md"}),
            }],
            model: "claude-test".into(),
            stop_reason: Some(StopReason::ToolUse),
            stop_sequence: None,
            usage: anthropic_sdk::types::Usage {
                input_tokens: 5,
                output_tokens: 2,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                server_tool_use: None,
                service_tier: None,
            },
            request_id: None,
        };

        let events = message_to_stream_events(message);
        assert!(matches!(
            &events[0],
            StreamEvent::ToolCallDelta(delta)
                if delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
        ));
        assert!(matches!(
            &events[1],
            StreamEvent::ToolCall(call)
                if call.id == "toolu_1"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        ));
        assert!(matches!(
            events.last().unwrap(),
            StreamEvent::Finished(FinishReason::ToolCalls)
        ));
    }

    #[test]
    fn test_anthropic_stream_event_maps_tool_and_thinking_deltas() {
        let mut tool_blocks = Vec::new();
        let mut events = Vec::new();
        events.extend(anthropic_stream_event_to_events(
            MessageStreamEvent::ContentBlockDelta {
                delta: ContentBlockDelta::ThinkingDelta {
                    thinking: "reasoning".into(),
                },
                index: 0,
            },
            &mut tool_blocks,
        ));
        events.extend(anthropic_stream_event_to_events(
            MessageStreamEvent::ContentBlockStart {
                content_block: ContentBlock::ToolUse {
                    id: "toolu_1".into(),
                    name: "file_read".into(),
                    input: Value::Null,
                },
                index: 1,
            },
            &mut tool_blocks,
        ));
        events.extend(anthropic_stream_event_to_events(
            MessageStreamEvent::ContentBlockDelta {
                delta: ContentBlockDelta::InputJsonDelta {
                    partial_json: r#"{"path":"README.md"}"#.into(),
                },
                index: 1,
            },
            &mut tool_blocks,
        ));
        events.extend(anthropic_stream_event_to_events(
            MessageStreamEvent::ContentBlockStop { index: 1 },
            &mut tool_blocks,
        ));

        assert!(matches!(&events[0], StreamEvent::ThinkingDelta(text) if text == "reasoning"));
        assert!(matches!(
            &events[1],
            StreamEvent::ToolCallDelta(delta)
                if delta.id.as_deref() == Some("toolu_1")
                    && delta.name.as_deref() == Some("file_read")
        ));
        assert!(matches!(
            &events[3],
            StreamEvent::ToolCall(call)
                if call.id == "toolu_1"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        ));
    }

    #[test]
    fn test_anthropic_stream_does_not_emit_double_finished() {
        let mut tool_blocks = Vec::new();
        let mut finished_emitted = false;
        let mut events = Vec::new();
        emit_anthropic_stream_events(
            MessageStreamEvent::MessageDelta {
                delta: MessageDelta {
                    stop_reason: Some(StopReason::ToolUse),
                    stop_sequence: None,
                },
                usage: MessageDeltaUsage {
                    input_tokens: Some(3),
                    output_tokens: 4,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    server_tool_use: None,
                },
            },
            &mut tool_blocks,
            &mut finished_emitted,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();
        emit_anthropic_stream_events(
            MessageStreamEvent::MessageStop,
            &mut tool_blocks,
            &mut finished_emitted,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();

        let finished = events
            .iter()
            .filter(|event| matches!(event, StreamEvent::Finished(_)))
            .collect::<Vec<_>>();
        assert_eq!(finished.len(), 1, "events: {events:?}");
        assert!(matches!(
            finished[0],
            StreamEvent::Finished(FinishReason::ToolCalls)
        ));
    }

    #[test]
    fn test_auth_error_maps_to_provider_error() {
        let (base_url, handle) =
            spawn_mock_server(401, r#"{"error":{"message":"invalid api key"}}"#);
        let mut request = sample_request(&base_url);
        request.stream = false;
        let events = execute_messages_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(
            &events[0],
            StreamEvent::Error(err)
                if err.code == ProviderErrorCode::Authentication && !err.retryable
        ));
    }

    fn sample_request(base_url: &str) -> GenerateRequest {
        GenerateRequest {
            model: "claude-test".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: Vec::new(),
            stream: true,
            auth_context: AuthContext::ApiKey {
                provider: "anthropic".into(),
                key: "sk-ant-test".into(),
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
            assert!(request.starts_with("POST /v1/messages"));
            assert!(request.to_lowercase().contains("x-api-key: sk-ant-test"));
            assert!(request
                .to_lowercase()
                .contains("anthropic-version: 2023-06-01"));
            let body_json: serde_json::Value = serde_json::from_str(http_body(&request)).unwrap();
            assert_eq!(body_json["model"], "claude-test");
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://{addr}"), handle)
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
}
