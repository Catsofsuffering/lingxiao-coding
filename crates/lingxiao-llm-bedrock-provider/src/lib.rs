use aws_credential_types::Credentials;
use aws_sdk_bedrockruntime::config::{BehaviorVersion, Region};
use aws_sdk_bedrockruntime::{Client, Config};
use aws_smithy_types::retry::RetryConfig;
use aws_smithy_types::Blob;
use lingxiao_llm_host_protocol::{
    AuthContext, FinishReason, GenerateRequest, Message, ProviderError, ProviderErrorCode,
    StreamEvent, TokenUsage, ToolCall, ToolCallDelta,
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

    let mut body = json!({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": request.options.max_tokens.unwrap_or(1024),
        "messages": request.messages.iter().filter(|message| message.role != "system").map(bedrock_message).collect::<Vec<_>>(),
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

fn bedrock_message(message: &Message) -> Value {
    if message.role == "assistant" {
        let mut content = Vec::new();
        if !message.content.is_empty() {
            content.push(json!({"type": "text", "text": message.content}));
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
        return json!({
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": message.tool_call_id.clone().unwrap_or_default(),
                "content": message.content,
            }],
        });
    }
    json!({
        "role": "user",
        "content": [{"type": "text", "text": message.content}],
    })
}

fn system_prompt(messages: &[Message]) -> Option<String> {
    let system_parts = messages
        .iter()
        .filter(|message| message.role == "system")
        .map(|message| message.content.as_str())
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
        let mapped = bedrock_message(&message);
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
        let mapped = bedrock_message(&message);
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
}
