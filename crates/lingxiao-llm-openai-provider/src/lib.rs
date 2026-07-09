use async_openai::config::Config;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCalls,
    ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestAssistantMessageContent,
    ChatCompletionRequestMessage, ChatCompletionRequestMessageContentPartImage,
    ChatCompletionRequestMessageContentPartText, ChatCompletionRequestSystemMessageArgs,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestToolMessageArgs,
    ChatCompletionRequestToolMessageContent, ChatCompletionRequestUserMessageArgs,
    ChatCompletionRequestUserMessageContent, ChatCompletionRequestUserMessageContentPart,
    ChatCompletionTool, ChatCompletionToolChoiceOption, ChatCompletionTools,
    CreateChatCompletionRequest, CreateChatCompletionRequestArgs, CreateChatCompletionResponse,
    CreateChatCompletionStreamResponse, FinishReason as OpenAiFinishReason, FunctionCall,
    FunctionObject, ImageDetail, ImageUrl, ToolChoiceOptions,
};
use async_openai::Client;
use futures_util::StreamExt;
use lingxiao_llm_host_protocol::{
    message_rehydrates_blob_at, retain_rounds_from_metadata, AuthContext, FinishReason,
    GenerateRequest, Message, ProviderError, ProviderErrorCode, StreamEvent, TokenUsage, ToolCall,
    ToolCallAccumulator,
};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use secrecy::SecretString;
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::time::Duration;
use thiserror::Error;

const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

#[derive(Debug, Error)]
pub enum OpenAiProviderError {
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

pub fn run_stdio() -> Result<(), OpenAiProviderError> {
    let stdin = io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| OpenAiProviderError::Stdin(e.to_string()))?;
    let request: GenerateRequest =
        serde_json::from_str(&line).map_err(|e| OpenAiProviderError::Decode(e.to_string()))?;
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    stream_chat_completion_blocking(&request, |event| {
        let line = serde_json::to_string(&event)
            .map_err(|e| OpenAiProviderError::Encode(e.to_string()))?;
        writeln!(stdout, "{line}").map_err(|e| OpenAiProviderError::Stdin(e.to_string()))?;
        stdout
            .flush()
            .map_err(|e| OpenAiProviderError::Stdin(e.to_string()))
    })?;
    stdout
        .flush()
        .map_err(|e| OpenAiProviderError::Stdin(e.to_string()))?;
    Ok(())
}

pub fn execute_chat_completion_blocking(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, OpenAiProviderError> {
    let mut events = Vec::new();
    stream_chat_completion_blocking(request, |event| {
        events.push(event);
        Ok(())
    })?;
    Ok(events)
}

pub fn stream_chat_completion_blocking<F>(
    request: &GenerateRequest,
    mut sink: F,
) -> Result<(), OpenAiProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    let runtime =
        tokio::runtime::Runtime::new().map_err(|e| OpenAiProviderError::Provider(e.to_string()))?;
    let timeout_ms = request.options.timeout_ms_hint.unwrap_or(600_000);
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            execute_chat_completion_streaming(request, &mut sink),
        )
        .await
        .map_err(|_| OpenAiProviderError::Provider("provider request timeout".to_string()))?
    })
}

pub async fn execute_chat_completion(
    request: &GenerateRequest,
) -> Result<Vec<StreamEvent>, OpenAiProviderError> {
    let mut events = Vec::new();
    execute_chat_completion_streaming(request, &mut |event| {
        events.push(event);
        Ok(())
    })
    .await?;
    Ok(events)
}

async fn execute_chat_completion_streaming<F>(
    request: &GenerateRequest,
    sink: &mut F,
) -> Result<(), OpenAiProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    if use_responses_api(request) {
        return execute_responses_api(request, sink).await;
    }

    let config = ResolvedOpenAiConfig::from_request(request)?;
    let client = Client::with_config(config);
    if request.stream {
        return execute_chat_completion_stream(&client, request, sink).await;
    }
    match client
        .chat()
        .create(to_chat_completion_request(request)?)
        .await
    {
        Ok(response) => {
            for event in response_to_stream_events(response) {
                sink(event)?;
            }
            Ok(())
        }
        Err(error) => sink(StreamEvent::Error(provider_error_from_openai(error))),
    }
}

async fn execute_chat_completion_stream<C, F>(
    client: &Client<C>,
    request: &GenerateRequest,
    sink: &mut F,
) -> Result<(), OpenAiProviderError>
where
    C: Config,
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    let mut stream = match client
        .chat()
        .create_stream(to_chat_completion_request(request)?)
        .await
    {
        Ok(stream) => stream,
        Err(error) => return sink(StreamEvent::Error(provider_error_from_openai(error))),
    };
    let mut tool_accumulator = ToolCallAccumulator::new();
    let mut saw_tool_delta = false;
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                sink(StreamEvent::Error(provider_error_from_openai(error)))?;
                continue;
            }
        };
        for event in chat_stream_chunk_to_events(chunk) {
            match event {
                StreamEvent::ToolCallDelta(delta) => {
                    saw_tool_delta = true;
                    tool_accumulator.append(delta.clone());
                    sink(StreamEvent::ToolCallDelta(delta))?;
                }
                StreamEvent::Finished(FinishReason::ToolCalls) => {
                    for call in tool_accumulator.finalize() {
                        sink(StreamEvent::ToolCall(call))?;
                    }
                    sink(StreamEvent::Finished(FinishReason::ToolCalls))?;
                }
                other => sink(other)?,
            }
        }
    }
    if saw_tool_delta {
        for call in tool_accumulator.finalize() {
            sink(StreamEvent::ToolCall(call))?;
        }
    }
    Ok(())
}

fn use_responses_api(request: &GenerateRequest) -> bool {
    request
        .options
        .metadata
        .as_ref()
        .and_then(|value| {
            value
                .get("api")
                .or_else(|| value.get("api_kind"))
                .or_else(|| value.get("endpoint"))
        })
        .and_then(Value::as_str)
        .map(|value| matches!(value, "response" | "responses"))
        .unwrap_or(false)
}

async fn execute_responses_api<F>(
    request: &GenerateRequest,
    sink: &mut F,
) -> Result<(), OpenAiProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    let config = ResolvedOpenAiConfig::from_request(request)?;
    let mut body = json!({
        "model": request.model,
        "input": responses_input(request),
    });
    if !request.tools.is_empty() {
        body["tools"] = json!(responses_tools(request));
    }
    if let Some(max_tokens) = request.options.max_tokens {
        body["max_output_tokens"] = json!(max_tokens);
    }
    if let Some(temperature) = request.options.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(top_p) = request.options.top_p {
        body["top_p"] = json!(top_p);
    }
    if request.stream {
        body["stream"] = json!(true);
    }

    let response = reqwest::Client::new()
        .post(format!(
            "{}/responses",
            config.api_base.trim_end_matches('/')
        ))
        .headers(config.headers())
        .json(&body)
        .send()
        .await
        .map_err(|e| OpenAiProviderError::Provider(e.to_string()))?;
    let status = response.status();
    if request.stream && status.is_success() {
        return consume_responses_sse(response, sink).await;
    }
    let value: Value = response
        .json()
        .await
        .map_err(|e| OpenAiProviderError::Provider(e.to_string()))?;
    if !status.is_success() {
        return sink(StreamEvent::Error(ProviderError::new(
            map_response_status(status.as_u16()),
            response_error_message(status.as_u16(), &value),
        )));
    }
    for event in responses_value_to_stream_events(value) {
        sink(event)?;
    }
    Ok(())
}

fn responses_input(request: &GenerateRequest) -> Value {
    let retain_rounds = retain_rounds_from_metadata(request.options.metadata.as_ref());
    let mut input = Vec::new();
    for (index, message) in request.messages.iter().enumerate() {
        if message.role == "tool" {
            let content = message.plain_text_content();
            input.push(json!({
                "type": "function_call_output",
                "call_id": message.tool_call_id.clone().unwrap_or_default(),
                "output": content,
            }));
            continue;
        }
        if message.role == "assistant" && !message.tool_calls.is_empty() {
            let content = message.plain_text_content();
            for call in &message.tool_calls {
                input.push(json!({
                    "type": "function_call",
                    "call_id": call.id,
                    "name": call.name,
                    "arguments": call.arguments.to_string(),
                }));
            }
            if !content.is_empty() {
                input.push(json!({
                    "role": "assistant",
                    "content": content,
                }));
            }
            continue;
        }
        // Multimodal user/system messages carry real image parts as a
        // Responses-API content array (`input_text`/`input_image`) so vision
        // models receive the image bytes instead of a flattened placeholder.
        // Mirrors TS `toOpenAIContentParts` over the Responses wire format.
        // Blob-ref rehydration is gated by the retain-rounds window: only the
        // recent N user rounds rehydrate real image bytes; older blobs degrade
        // to the safe text placeholder (TS `rehydrateRecentImageBlobRefs`).
        let rehydrate = message_rehydrates_blob_at(&request.messages, index, retain_rounds);
        if let Some(parts) = message.openai_responses_content_parts_with_rehydrate(rehydrate) {
            input.push(json!({
                "role": message.role,
                "content": parts,
            }));
            continue;
        }
        let content = message.plain_text_content();
        input.push(json!({
            "role": message.role,
            "content": content,
        }));
    }
    Value::Array(input)
}

fn responses_tools(request: &GenerateRequest) -> Vec<Value> {
    request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
            })
        })
        .collect()
}

#[derive(Clone, Debug)]
struct ResolvedOpenAiConfig {
    api_base: String,
    api_key: SecretString,
    mode: OpenAiConfigMode,
}

#[derive(Clone, Debug)]
enum OpenAiConfigMode {
    OpenAiCompatible {
        organization: Option<String>,
        project: Option<String>,
    },
    Azure {
        deployment_id: String,
        api_version: String,
    },
}

impl ResolvedOpenAiConfig {
    fn from_request(request: &GenerateRequest) -> Result<Self, OpenAiProviderError> {
        if let AuthContext::AzureToken {
            endpoint,
            deployment_id,
            api_version,
            api_key,
        } = &request.auth_context
        {
            return Ok(Self {
                api_base: endpoint.trim_end_matches('/').to_string(),
                api_key: SecretString::from(api_key.clone()),
                mode: OpenAiConfigMode::Azure {
                    deployment_id: deployment_id.clone(),
                    api_version: api_version.clone(),
                },
            });
        }

        let api_key = match &request.auth_context {
            AuthContext::ApiKey { key, .. } | AuthContext::BearerToken { token: key, .. } => {
                key.clone()
            }
            AuthContext::AwsSignature { .. } | AuthContext::None => {
                return Err(OpenAiProviderError::UnsupportedAuth);
            }
            AuthContext::AzureToken { .. } => unreachable!("AzureToken handled above"),
        };
        let metadata = request.options.metadata.as_ref();
        let api_base = metadata
            .and_then(|value| value.get("base_url"))
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_BASE_URL)
            .trim_end_matches('/')
            .to_string();
        let organization = metadata
            .and_then(|value| value.get("organization"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let project = metadata
            .and_then(|value| value.get("project"))
            .and_then(Value::as_str)
            .map(str::to_string);
        Ok(Self {
            api_base,
            api_key: SecretString::from(api_key),
            mode: OpenAiConfigMode::OpenAiCompatible {
                organization,
                project,
            },
        })
    }
}

impl Config for ResolvedOpenAiConfig {
    fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        match &self.mode {
            OpenAiConfigMode::OpenAiCompatible {
                organization,
                project,
            } => {
                let auth = format!(
                    "Bearer {}",
                    secrecy::ExposeSecret::expose_secret(&self.api_key)
                );
                headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth).unwrap());
                if let Some(organization) = organization {
                    headers.insert(
                        "OpenAI-Organization",
                        HeaderValue::from_str(organization).unwrap(),
                    );
                }
                if let Some(project) = project {
                    headers.insert("OpenAI-Project", HeaderValue::from_str(project).unwrap());
                }
            }
            OpenAiConfigMode::Azure { .. } => {
                headers.insert(
                    "api-key",
                    HeaderValue::from_str(secrecy::ExposeSecret::expose_secret(&self.api_key))
                        .unwrap(),
                );
            }
        }
        headers
    }

    fn url(&self, path: &str) -> String {
        match &self.mode {
            OpenAiConfigMode::OpenAiCompatible { .. } => format!("{}{}", self.api_base, path),
            OpenAiConfigMode::Azure { deployment_id, .. } => format!(
                "{}/openai/deployments/{}{}",
                self.api_base, deployment_id, path
            ),
        }
    }

    fn query(&self) -> Vec<(&str, &str)> {
        match &self.mode {
            OpenAiConfigMode::OpenAiCompatible { .. } => Vec::new(),
            OpenAiConfigMode::Azure { api_version, .. } => {
                vec![("api-version", api_version.as_str())]
            }
        }
    }

    fn api_base(&self) -> &str {
        &self.api_base
    }

    fn api_key(&self) -> &SecretString {
        &self.api_key
    }
}

fn to_chat_completion_request(
    request: &GenerateRequest,
) -> Result<CreateChatCompletionRequest, OpenAiProviderError> {
    let retain_rounds = retain_rounds_from_metadata(request.options.metadata.as_ref());
    let messages = request
        .messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let rehydrate = message_rehydrates_blob_at(&request.messages, index, retain_rounds);
            to_chat_message(message, rehydrate)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut builder = CreateChatCompletionRequestArgs::default();
    builder.model(request.model.clone()).messages(messages);
    if !request.tools.is_empty() {
        builder.tools(to_chat_tools(request));
        builder.tool_choice(chat_tool_choice(request));
    }
    if let Some(max_tokens) = request.options.max_tokens {
        builder.max_completion_tokens(max_tokens);
    }
    if let Some(temperature) = request.options.temperature {
        builder.temperature(temperature);
    }
    if let Some(top_p) = request.options.top_p {
        builder.top_p(top_p);
    }
    if let Some(stop) = &request.options.stop {
        builder.stop(stop.clone());
    }
    builder
        .build()
        .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))
}

fn to_chat_message(
    message: &Message,
    rehydrate: bool,
) -> Result<ChatCompletionRequestMessage, OpenAiProviderError> {
    // User messages can carry multimodal content parts (text + image_url /
    // image_blob_ref). When they do, emit a real content-part array so the
    // provider receives the image bytes instead of a flattened "[image]"
    // placeholder. Mirrors TS `toOpenAIContentParts`. Blob-ref rehydration is
    // gated by the retain-rounds window (`rehydrate`): only the recent N user
    // rounds send real image bytes; older blobs degrade to the safe text
    // placeholder (TS `rehydrateRecentImageBlobRefs`).
    if message.role == "user" {
        if let Some(parts) = user_message_content_parts(message, rehydrate) {
            return Ok(ChatCompletionRequestUserMessageArgs::default()
                .content(ChatCompletionRequestUserMessageContent::Array(parts))
                .build()
                .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))?
                .into());
        }
    }

    let content = message.plain_text_content();
    match message.role.as_str() {
        "system" => Ok(ChatCompletionRequestSystemMessageArgs::default()
            .content(ChatCompletionRequestSystemMessageContent::Text(content))
            .build()
            .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))?
            .into()),
        "assistant" => {
            let mut builder = ChatCompletionRequestAssistantMessageArgs::default();
            if !content.is_empty() || message.tool_calls.is_empty() {
                builder.content(ChatCompletionRequestAssistantMessageContent::Text(content));
            }
            if !message.tool_calls.is_empty() {
                builder.tool_calls(
                    message
                        .tool_calls
                        .iter()
                        .map(chat_tool_call_from_protocol)
                        .collect::<Vec<_>>(),
                );
            }
            Ok(builder
                .build()
                .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))?
                .into())
        }
        "tool" => Ok(ChatCompletionRequestToolMessageArgs::default()
            .content(ChatCompletionRequestToolMessageContent::Text(content))
            .tool_call_id(message.tool_call_id.clone().unwrap_or_default())
            .build()
            .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))?
            .into()),
        _ => Ok(ChatCompletionRequestUserMessageArgs::default()
            .content(ChatCompletionRequestUserMessageContent::Text(content))
            .build()
            .map_err(|e| OpenAiProviderError::RequestBuild(e.to_string()))?
            .into()),
    }
}

/// Build an OpenAI Chat-Completions user content-part array when the message
/// carries structured content. Returns `None` when the message has no
/// `content_parts`, so callers fall back to plain-text content. Image blob
/// refs are rehydrated from disk when `rehydrate` is true (i.e. the message is
/// inside the retain-rounds window); otherwise they degrade to a text
/// placeholder. Missing blobs always degrade to a text placeholder.
fn user_message_content_parts(
    message: &Message,
    rehydrate: bool,
) -> Option<Vec<ChatCompletionRequestUserMessageContentPart>> {
    let json_parts = message.openai_chat_content_parts_with_rehydrate(rehydrate)?;
    let mut parts = Vec::with_capacity(json_parts.len());
    for part in json_parts {
        let kind = part.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "text" => {
                let text = part
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                parts.push(ChatCompletionRequestUserMessageContentPart::Text(
                    ChatCompletionRequestMessageContentPartText { text },
                ));
            }
            "image_url" => {
                let url = part
                    .get("image_url")
                    .and_then(|v| v.get("url"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                let detail = part
                    .get("image_url")
                    .and_then(|v| v.get("detail"))
                    .and_then(Value::as_str)
                    .and_then(parse_image_detail);
                let image = ChatCompletionRequestMessageContentPartImage {
                    image_url: ImageUrl { url, detail },
                };
                parts.push(ChatCompletionRequestUserMessageContentPart::ImageUrl(image));
            }
            _ => {}
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}

fn parse_image_detail(detail: &str) -> Option<ImageDetail> {
    match detail {
        "auto" => Some(ImageDetail::Auto),
        "low" => Some(ImageDetail::Low),
        "high" => Some(ImageDetail::High),
        "original" => Some(ImageDetail::Original),
        _ => None,
    }
}

fn chat_tool_call_from_protocol(call: &ToolCall) -> ChatCompletionMessageToolCalls {
    ChatCompletionMessageToolCalls::Function(ChatCompletionMessageToolCall {
        id: call.id.clone(),
        function: FunctionCall {
            name: call.name.clone(),
            arguments: call.arguments.to_string(),
        },
    })
}

fn to_chat_tools(request: &GenerateRequest) -> Vec<ChatCompletionTools> {
    request
        .tools
        .iter()
        .map(|tool| {
            ChatCompletionTools::Function(ChatCompletionTool {
                function: FunctionObject {
                    name: tool.name.clone(),
                    description: Some(tool.description.clone()),
                    parameters: Some(tool.input_schema.clone()),
                    strict: None,
                },
            })
        })
        .collect()
}

fn chat_tool_choice(request: &GenerateRequest) -> ChatCompletionToolChoiceOption {
    let choice = request
        .options
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("tool_choice"))
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let mode = match choice {
        "none" => ToolChoiceOptions::None,
        "required" => ToolChoiceOptions::Required,
        _ => ToolChoiceOptions::Auto,
    };
    ChatCompletionToolChoiceOption::Mode(mode)
}

fn response_to_stream_events(response: CreateChatCompletionResponse) -> Vec<StreamEvent> {
    let choice = response.choices.into_iter().next();
    let content = choice
        .as_ref()
        .and_then(|choice| choice.message.content.clone())
        .unwrap_or_default();
    let tool_calls = choice
        .as_ref()
        .map(|choice| extract_chat_response_tool_calls(&choice.message))
        .unwrap_or_default();
    let finish_reason = choice
        .and_then(|choice| choice.finish_reason)
        .map(map_finish_reason)
        .unwrap_or(FinishReason::Unknown);
    let usage = response
        .usage
        .map(|usage| TokenUsage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_tokens: None,
        })
        .unwrap_or(TokenUsage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_tokens: None,
        });

    let mut events = Vec::new();
    if !content.is_empty() {
        events.push(StreamEvent::TextDelta(content));
    }
    for (index, call) in tool_calls.iter().enumerate() {
        events.push(StreamEvent::ToolCallDelta(
            lingxiao_llm_host_protocol::ToolCallDelta {
                index: index as u32,
                id: Some(call.id.clone()),
                name: Some(call.name.clone()),
                partial_json: Some(call.arguments.to_string()),
            },
        ));
        events.push(StreamEvent::ToolCall(call.clone()));
    }
    events.push(StreamEvent::Usage(usage));
    events.push(StreamEvent::Finished(if tool_calls.is_empty() {
        finish_reason
    } else {
        FinishReason::ToolCalls
    }));
    events
}

fn extract_chat_response_tool_calls(
    message: &async_openai::types::chat::ChatCompletionResponseMessage,
) -> Vec<ToolCall> {
    let mut calls = Vec::new();
    if let Some(tool_calls) = &message.tool_calls {
        for tool_call in tool_calls {
            match tool_call {
                ChatCompletionMessageToolCalls::Function(function_call) => {
                    calls.push(ToolCall {
                        id: function_call.id.clone(),
                        name: function_call.function.name.clone(),
                        arguments: serde_json::from_str(&function_call.function.arguments)
                            .unwrap_or(Value::String(function_call.function.arguments.clone())),
                    });
                }
                ChatCompletionMessageToolCalls::Custom(custom_call) => {
                    calls.push(ToolCall {
                        id: custom_call.id.clone(),
                        name: custom_call.custom_tool.name.clone(),
                        arguments: serde_json::from_str(&custom_call.custom_tool.input)
                            .unwrap_or(Value::String(custom_call.custom_tool.input.clone())),
                    });
                }
            }
        }
    }
    #[allow(deprecated)]
    if let Some(function_call) = &message.function_call {
        calls.push(ToolCall {
            id: String::new(),
            name: function_call.name.clone(),
            arguments: serde_json::from_str(&function_call.arguments)
                .unwrap_or(Value::String(function_call.arguments.clone())),
        });
    }
    calls
}

fn chat_stream_chunk_to_events(chunk: CreateChatCompletionStreamResponse) -> Vec<StreamEvent> {
    let mut events = Vec::new();
    for choice in chunk.choices {
        if let Some(content) = choice.delta.content {
            if !content.is_empty() {
                events.push(StreamEvent::TextDelta(content));
            }
        }
        #[allow(deprecated)]
        if let Some(function_call) = choice.delta.function_call {
            events.push(StreamEvent::ToolCallDelta(
                lingxiao_llm_host_protocol::ToolCallDelta {
                    index: choice.index,
                    id: None,
                    name: function_call.name,
                    partial_json: function_call.arguments,
                },
            ));
        }
        if let Some(tool_calls) = choice.delta.tool_calls {
            for tool_call in tool_calls {
                events.push(StreamEvent::ToolCallDelta(
                    lingxiao_llm_host_protocol::ToolCallDelta {
                        index: tool_call.index,
                        id: tool_call.id,
                        name: tool_call
                            .function
                            .as_ref()
                            .and_then(|function| function.name.clone()),
                        partial_json: tool_call.function.and_then(|function| function.arguments),
                    },
                ));
            }
        }
        if let Some(reason) = choice.finish_reason {
            events.push(StreamEvent::Finished(map_finish_reason(reason)));
        }
    }
    if let Some(usage) = chunk.usage {
        events.push(StreamEvent::Usage(TokenUsage {
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
            reasoning_tokens: None,
        }));
    }
    events
}

fn responses_value_to_stream_events(value: Value) -> Vec<StreamEvent> {
    let content = value
        .get("output_text")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| extract_responses_text(&value));
    let tool_calls = extract_responses_tool_calls(&value);
    let usage_value = value.get("usage");
    let usage = TokenUsage {
        prompt_tokens: response_usage_u32(usage_value, "input_tokens"),
        completion_tokens: response_usage_u32(usage_value, "output_tokens"),
        total_tokens: response_usage_u32(usage_value, "total_tokens"),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        reasoning_tokens: usage_value
            .and_then(|usage| usage.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
            .map(saturating_u32),
    };
    let finish_reason = value
        .get("status")
        .and_then(Value::as_str)
        .map(|status| match status {
            "completed" => FinishReason::Stop,
            "incomplete" => FinishReason::Length,
            _ => FinishReason::Unknown,
        })
        .unwrap_or(FinishReason::Unknown);

    let mut events = Vec::new();
    if !content.is_empty() {
        events.push(StreamEvent::TextDelta(content));
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

async fn consume_responses_sse<F>(
    response: reqwest::Response,
    sink: &mut F,
) -> Result<(), OpenAiProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    let mut stream = response.bytes_stream();
    let mut buffer = String::new();
    let mut saw_event = false;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| OpenAiProviderError::Provider(e.to_string()))?;
        let text = std::str::from_utf8(&chunk)
            .map_err(|e| OpenAiProviderError::Provider(format!("non-utf8 SSE chunk: {e}")))?;
        buffer.push_str(text);

        while let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_string();
            buffer = buffer[index + 2..].to_string();
            if process_responses_sse_frame(&frame, sink)? {
                return Ok(());
            }
            saw_event = true;
        }
    }

    if !buffer.trim().is_empty() {
        process_responses_sse_frame(&buffer, sink)?;
        saw_event = true;
    }
    if !saw_event {
        return Err(OpenAiProviderError::Provider(
            "responses stream ended without events".to_string(),
        ));
    }
    Ok(())
}

fn process_responses_sse_frame<F>(frame: &str, sink: &mut F) -> Result<bool, OpenAiProviderError>
where
    F: FnMut(StreamEvent) -> Result<(), OpenAiProviderError>,
{
    let mut data = String::new();
    for line in frame.lines() {
        let line = line.trim_end_matches('\r');
        if let Some(value) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(value.trim_start());
        }
    }
    if data.trim().is_empty() {
        return Ok(false);
    }
    if data.trim() == "[DONE]" {
        return Ok(true);
    }
    let value: Value = serde_json::from_str(&data)
        .map_err(|e| OpenAiProviderError::Provider(format!("responses SSE decode failed: {e}")))?;
    for event in responses_sse_value_to_events(&value) {
        sink(event)?;
    }
    Ok(false)
}

fn responses_sse_value_to_events(value: &Value) -> Vec<StreamEvent> {
    let event_type = value
        .get("type")
        .or_else(|| value.get("event"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    match event_type {
        "response.output_text.delta" => value
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| vec![StreamEvent::TextDelta(delta.to_string())])
            .unwrap_or_default(),
        "response.reasoning_summary_text.delta"
        | "response.reasoning_text.delta"
        | "response.reasoning.delta" => value
            .get("delta")
            .and_then(Value::as_str)
            .map(|delta| vec![StreamEvent::ThinkingDelta(delta.to_string())])
            .unwrap_or_default(),
        "response.function_call_arguments.delta" => {
            vec![StreamEvent::ToolCallDelta(
                lingxiao_llm_host_protocol::ToolCallDelta {
                    index: value
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                        .min(u64::from(u32::MAX)) as u32,
                    id: value
                        .get("call_id")
                        .or_else(|| value.get("item_id"))
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    name: value
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                    partial_json: value
                        .get("delta")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                },
            )]
        }
        "response.output_item.done" => value
            .get("item")
            .and_then(extract_responses_tool_call_from_item)
            .map(|tool_call| vec![StreamEvent::ToolCall(tool_call)])
            .unwrap_or_default(),
        "response.completed" => value
            .get("response")
            .map(responses_completion_events)
            .unwrap_or_default(),
        "response.failed" | "error" => vec![StreamEvent::Error(ProviderError::new(
            ProviderErrorCode::ServerError,
            value
                .get("error")
                .and_then(|error| {
                    error
                        .get("message")
                        .and_then(Value::as_str)
                        .or_else(|| error.as_str())
                })
                .or_else(|| value.get("message").and_then(Value::as_str))
                .unwrap_or("responses stream error"),
        ))],
        _ => {
            if value.get("output").is_some() {
                responses_value_to_stream_events(value.clone())
            } else {
                Vec::new()
            }
        }
    }
}

fn responses_completion_events(response: &Value) -> Vec<StreamEvent> {
    let usage_value = response.get("usage");
    let usage = TokenUsage {
        prompt_tokens: response_usage_u32(usage_value, "input_tokens"),
        completion_tokens: response_usage_u32(usage_value, "output_tokens"),
        total_tokens: response_usage_u32(usage_value, "total_tokens"),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: None,
        reasoning_tokens: usage_value
            .and_then(|usage| usage.get("output_tokens_details"))
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64)
            .map(saturating_u32),
    };
    let finish_reason = response
        .get("status")
        .and_then(Value::as_str)
        .map(|status| match status {
            "completed" => FinishReason::Stop,
            "incomplete" => FinishReason::Length,
            _ => FinishReason::Unknown,
        })
        .unwrap_or(FinishReason::Unknown);
    vec![
        StreamEvent::Usage(usage),
        StreamEvent::Finished(finish_reason),
    ]
}

fn response_usage_u32(usage_value: Option<&Value>, key: &str) -> u32 {
    usage_value
        .and_then(|usage| usage.get(key))
        .and_then(Value::as_u64)
        .map(saturating_u32)
        .unwrap_or(0)
}

fn extract_responses_tool_calls(value: &Value) -> Vec<lingxiao_llm_host_protocol::ToolCall> {
    let Some(items) = value.get("output").and_then(Value::as_array) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let item_type = item.get("type").and_then(Value::as_str)?;
            if !matches!(item_type, "function_call" | "tool_call") {
                return None;
            }
            let name = item
                .get("name")
                .or_else(|| {
                    item.get("function")
                        .and_then(|function| function.get("name"))
                })
                .and_then(Value::as_str)?
                .to_string();
            let id = item
                .get("call_id")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let raw_arguments = item.get("arguments").or_else(|| {
                item.get("function")
                    .and_then(|function| function.get("arguments"))
            });
            let arguments = raw_arguments
                .and_then(Value::as_str)
                .and_then(|raw| serde_json::from_str(raw).ok())
                .or_else(|| raw_arguments.cloned())
                .unwrap_or(Value::Null);
            Some(lingxiao_llm_host_protocol::ToolCall {
                id,
                name,
                arguments,
            })
        })
        .collect()
}

fn extract_responses_tool_call_from_item(
    item: &Value,
) -> Option<lingxiao_llm_host_protocol::ToolCall> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    if !matches!(item_type, "function_call" | "tool_call") {
        return None;
    }
    let name = item
        .get("name")
        .or_else(|| {
            item.get("function")
                .and_then(|function| function.get("name"))
        })
        .and_then(Value::as_str)?
        .to_string();
    let id = item
        .get("call_id")
        .or_else(|| item.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let raw_arguments = item.get("arguments").or_else(|| {
        item.get("function")
            .and_then(|function| function.get("arguments"))
    });
    let arguments = raw_arguments
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str(raw).ok())
        .or_else(|| raw_arguments.cloned())
        .unwrap_or(Value::Null);
    Some(lingxiao_llm_host_protocol::ToolCall {
        id,
        name,
        arguments,
    })
}

fn saturating_u32(value: u64) -> u32 {
    value.min(u64::from(u32::MAX)) as u32
}

fn extract_responses_text(value: &Value) -> String {
    let mut text = String::new();
    if let Some(items) = value.get("output").and_then(Value::as_array) {
        for item in items {
            if let Some(content) = item.get("content").and_then(Value::as_array) {
                for block in content {
                    if let Some(part) = block.get("text").and_then(Value::as_str) {
                        text.push_str(part);
                    } else if let Some(part) = block
                        .get("content")
                        .and_then(Value::as_array)
                        .and_then(|nested| nested.first())
                        .and_then(|nested| nested.get("text"))
                        .and_then(Value::as_str)
                    {
                        text.push_str(part);
                    }
                }
            }
        }
    }
    text
}

fn provider_error_from_openai(error: OpenAIError) -> ProviderError {
    ProviderError::new(map_openai_error_code(&error), redact_error(&error))
}

fn map_response_status(status: u16) -> ProviderErrorCode {
    match status {
        401 | 403 => ProviderErrorCode::Authentication,
        400 | 404 => ProviderErrorCode::BadRequest,
        408 => ProviderErrorCode::Timeout,
        429 => ProviderErrorCode::RateLimited,
        500..=599 => ProviderErrorCode::ServerError,
        _ => ProviderErrorCode::Unknown,
    }
}

fn response_error_message(status: u16, value: &Value) -> String {
    let message = value
        .get("error")
        .and_then(|error| {
            error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.as_str())
        })
        .or_else(|| value.get("message").and_then(Value::as_str))
        .unwrap_or("provider request failed");
    if message.contains("sk-") {
        format!("{status} provider request failed")
    } else {
        format!("{status} {message}")
    }
}

fn map_openai_error_code(error: &OpenAIError) -> ProviderErrorCode {
    let text = error.to_string().to_lowercase();
    if text.contains("401") || text.contains("unauthorized") || text.contains("authentication") {
        ProviderErrorCode::Authentication
    } else if text.contains("429") || text.contains("rate") {
        ProviderErrorCode::RateLimited
    } else if text.contains("context") {
        ProviderErrorCode::ContextOverflow
    } else if text.contains("400") || text.contains("invalid") {
        ProviderErrorCode::BadRequest
    } else if text.contains("timeout") {
        ProviderErrorCode::Timeout
    } else if text.contains("500") || text.contains("503") {
        ProviderErrorCode::ServerError
    } else {
        ProviderErrorCode::Unknown
    }
}

fn redact_error(error: &OpenAIError) -> String {
    let message = error.to_string();
    if message.contains("sk-") {
        "provider request failed".to_string()
    } else {
        message
    }
}

fn map_finish_reason(reason: OpenAiFinishReason) -> FinishReason {
    match reason {
        OpenAiFinishReason::Stop => FinishReason::Stop,
        OpenAiFinishReason::Length => FinishReason::Length,
        OpenAiFinishReason::ToolCalls | OpenAiFinishReason::FunctionCall => FinishReason::ToolCalls,
        OpenAiFinishReason::ContentFilter => FinishReason::ContentFiltered,
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
    fn test_chat_completion_request_shape_uses_async_openai_types() {
        let request = sample_request("http://localhost:1234/v1");
        let body = to_chat_completion_request(&request).unwrap();
        let json = serde_json::to_value(body).unwrap();
        assert_eq!(json["model"], "gpt-test");
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["content"], "hello");
        assert_eq!(json["max_completion_tokens"], 12);
        assert!(json.get("stream").is_none());
    }

    #[test]
    fn test_resolved_config_does_not_use_env_defaults() {
        let request = sample_request("http://localhost:1234/v1");
        let config = ResolvedOpenAiConfig::from_request(&request).unwrap();
        assert_eq!(config.api_base(), "http://localhost:1234/v1");
        let headers = config.headers();
        assert_eq!(headers.get(AUTHORIZATION).unwrap(), "Bearer sk-test");
    }

    #[test]
    fn test_execute_chat_completion_against_mock_http_server() {
        let (base_url, handle) = spawn_mock_server(
            200,
            r#"{"id":"chatcmpl-test","object":"chat.completion","created":1,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"real http response"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}"#,
        );
        let mut request = sample_request(&base_url);
        request.stream = false;
        let events = execute_chat_completion_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "real http response"));
        assert!(matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 5));
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_execute_azure_chat_completion_against_mock_http_server() {
        let (endpoint, handle) = spawn_azure_mock_server(
            200,
            r#"{"id":"chatcmpl-azure","object":"chat.completion","created":1,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"azure response"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6}}"#,
        );
        let mut request = sample_azure_request(&endpoint);
        request.stream = false;
        let config = ResolvedOpenAiConfig::from_request(&request).unwrap();
        assert_eq!(
            config.url("/chat/completions"),
            format!("{endpoint}/openai/deployments/deploy-test/chat/completions")
        );
        assert_eq!(config.query(), vec![("api-version", "2024-02-01")]);

        let events = execute_chat_completion_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "azure response"));
        assert!(matches!(&events[1], StreamEvent::Usage(usage) if usage.total_tokens == 6));
        assert!(matches!(
            &events[2],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_http_auth_error_maps_to_provider_error() {
        let (base_url, handle) =
            spawn_mock_server(401, r#"{"error":{"message":"invalid api key"}}"#);
        let request = sample_request(&base_url);
        let events = execute_chat_completion_blocking(&request).unwrap();
        handle.join().unwrap();
        assert!(matches!(
            &events[0],
            StreamEvent::Error(err)
                if err.code == ProviderErrorCode::Authentication && !err.retryable
        ));
    }

    #[test]
    fn test_responses_api_tool_call_is_not_dropped() {
        let value = json!({
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_123",
                "name": "file_read",
                "arguments": "{\"path\":\"README.md\"}"
            }],
            "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}
        });
        let events = responses_value_to_stream_events(value);

        assert!(matches!(
            &events[0],
            StreamEvent::ToolCallDelta(delta)
                if delta.id.as_deref() == Some("call_123")
                    && delta.name.as_deref() == Some("file_read")
        ));
        assert!(matches!(
            &events[1],
            StreamEvent::ToolCall(call)
                if call.id == "call_123"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        ));
        assert!(matches!(
            events.last().unwrap(),
            StreamEvent::Finished(FinishReason::ToolCalls)
        ));
    }

    #[test]
    fn test_responses_sse_maps_realtime_deltas() {
        let mut events = Vec::new();
        process_responses_sse_frame(
            r#"event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"hello"}"#,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();
        process_responses_sse_frame(
            r#"event: response.function_call_arguments.delta
data: {"type":"response.function_call_arguments.delta","output_index":0,"item_id":"call_123","name":"file_read","delta":"{\"path\""}"#,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();
        process_responses_sse_frame(
            r#"event: response.output_item.done
data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_123","name":"file_read","arguments":"{\"path\":\"README.md\"}"}}"#,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();
        process_responses_sse_frame(
            r#"event: response.completed
data: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":4,"total_tokens":7}}}"#,
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .unwrap();

        assert!(matches!(&events[0], StreamEvent::TextDelta(text) if text == "hello"));
        assert!(matches!(
            &events[1],
            StreamEvent::ToolCallDelta(delta)
                if delta.id.as_deref() == Some("call_123")
                    && delta.name.as_deref() == Some("file_read")
        ));
        assert!(matches!(
            &events[2],
            StreamEvent::ToolCall(call)
                if call.id == "call_123"
                    && call.name == "file_read"
                    && call.arguments["path"] == "README.md"
        ));
        assert!(matches!(&events[3], StreamEvent::Usage(usage) if usage.total_tokens == 7));
        assert!(matches!(
            &events[4],
            StreamEvent::Finished(FinishReason::Stop)
        ));
    }

    #[test]
    fn test_to_chat_message_emits_image_url_content_part_for_user() {
        use async_openai::types::chat::{
            ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        };
        let message = Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                lingxiao_llm_host_protocol::MessageContentPart::Text {
                    text: "describe this".into(),
                },
                lingxiao_llm_host_protocol::MessageContentPart::ImageUrl {
                    image_url: lingxiao_llm_host_protocol::ImageUrlContentPart {
                        url: "data:image/png;base64,iVBOR".into(),
                        detail: Some("high".into()),
                    },
                },
            ],
            ..Default::default()
        };
        let built = to_chat_message(&message, true).unwrap();
        let ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) = built
        else {
            panic!("expected a user message, got {:?}", built);
        };
        let parts = match content {
            ChatCompletionRequestUserMessageContent::Array(parts) => parts,
            other => panic!("expected multimodal content array, got {other:?}"),
        };
        assert_eq!(parts.len(), 2);
        assert!(matches!(
            &parts[0],
            ChatCompletionRequestUserMessageContentPart::Text(t) if t.text == "describe this"
        ));
        match &parts[1] {
            ChatCompletionRequestUserMessageContentPart::ImageUrl(image) => {
                assert_eq!(image.image_url.url, "data:image/png;base64,iVBOR");
                assert_eq!(image.image_url.detail, Some(ImageDetail::High));
            }
            other => panic!("expected image_url part, got {other:?}"),
        }
    }

    #[test]
    fn test_to_chat_message_rehydrates_blob_ref_into_image_url() {
        use async_openai::types::chat::{
            ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        };
        let dir = tempfile::tempdir().unwrap();
        let blob_path = dir.path().join("shot.png");
        std::fs::write(&blob_path, [0x89, 0x50, 0x4E, 0x47]).unwrap();
        let message = Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                lingxiao_llm_host_protocol::MessageContentPart::ImageBlobRef {
                    image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                        blob_id: "blob_rehydrate_chat".into(),
                        mime: "image/png".into(),
                        size: 4,
                        blob_path: blob_path.display().to_string(),
                        source: None,
                    },
                },
            ],
            ..Default::default()
        };
        let built = to_chat_message(&message, true).unwrap();
        let ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) = built
        else {
            panic!("expected a user message");
        };
        let parts = match content {
            ChatCompletionRequestUserMessageContent::Array(parts) => parts,
            other => panic!("expected multimodal content array, got {other:?}"),
        };
        assert_eq!(parts.len(), 1);
        match &parts[0] {
            ChatCompletionRequestUserMessageContentPart::ImageUrl(image) => {
                assert!(
                    image.image_url.url.starts_with("data:image/png;base64,"),
                    "got: {}",
                    image.image_url.url
                );
                assert!(image.image_url.url.contains("iVBORw=="));
            }
            other => panic!("expected rehydrated image_url part, got {other:?}"),
        }
    }

    #[test]
    fn test_to_chat_message_text_only_user_stays_plain_text() {
        use async_openai::types::chat::ChatCompletionRequestUserMessageContent;
        let message = Message {
            role: "user".into(),
            content: "just text".into(),
            content_parts: Vec::new(),
            ..Default::default()
        };
        let built = to_chat_message(&message, true).unwrap();
        let content = match built {
            async_openai::types::chat::ChatCompletionRequestMessage::User(m) => m.content,
            other => panic!("expected user message, got {other:?}"),
        };
        assert!(matches!(
            content,
            ChatCompletionRequestUserMessageContent::Text(ref t) if t == "just text"
        ));
    }

    #[test]
    fn test_responses_input_replays_function_call_before_tool_output() {
        let mut request = sample_request("http://localhost:1234/v1");
        request.messages = vec![
            Message {
                role: "user".into(),
                content: "read file".into(),
                ..Default::default()
            },
            Message {
                role: "assistant".into(),
                content: String::new(),
                tool_calls: vec![lingxiao_llm_host_protocol::ToolCall {
                    id: "call_123".into(),
                    name: "file_read".into(),
                    arguments: json!({"path": "README.md"}),
                }],
                ..Default::default()
            },
            Message {
                role: "tool".into(),
                content: "{\"content\":\"ok\"}".into(),
                tool_call_id: Some("call_123".into()),
                name: Some("file_read".into()),
                ..Default::default()
            },
        ];
        let input = responses_input(&request);
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[1]["call_id"], "call_123");
        assert_eq!(input[1]["name"], "file_read");
        assert_eq!(input[2]["type"], "function_call_output");
        assert_eq!(input[2]["call_id"], "call_123");
    }

    #[test]
    fn test_responses_input_emits_multimodal_content_array() {
        let mut request = sample_request("http://localhost:1234/v1");
        request.messages = vec![Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                lingxiao_llm_host_protocol::MessageContentPart::Text {
                    text: "inspect this".into(),
                },
                lingxiao_llm_host_protocol::MessageContentPart::ImageUrl {
                    image_url: lingxiao_llm_host_protocol::ImageUrlContentPart {
                        url: "https://example.test/image.png".into(),
                        detail: Some("low".into()),
                    },
                },
                lingxiao_llm_host_protocol::MessageContentPart::ImageBlobRef {
                    image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                        blob_id: "blob_1234567890abcdef".into(),
                        mime: "image/png".into(),
                        size: 2048,
                        // Missing backing file -> degrades to a text placeholder
                        // that must NOT leak the blob filesystem path.
                        blob_path: "C:/secret/blob.png".into(),
                        source: Some("screenshot".into()),
                    },
                },
            ],
            ..Default::default()
        }];
        let input = responses_input(&request);
        // User message is emitted as a content-parts array, not flattened text.
        let content = input[0]["content"].as_array().unwrap();
        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "input_text");
        assert_eq!(content[0]["text"], "inspect this");
        assert_eq!(content[1]["type"], "input_image");
        assert_eq!(content[1]["image_url"], "https://example.test/image.png");
        // Missing blob -> placeholder text part, never an image_url, never the path.
        assert_eq!(content[2]["type"], "input_text");
        let placeholder = content[2]["text"].as_str().unwrap();
        assert!(placeholder.contains("blob_123456"));
        assert!(!placeholder.contains("C:/secret/blob.png"));
    }

    #[test]
    fn test_responses_input_rehydrates_blob_ref_to_data_uri() {
        let dir = tempfile::tempdir().unwrap();
        let blob_path = dir.path().join("blob.png");
        // 2 transparent PNG bytes -> known base64 "iVBORw0KGgo=".
        std::fs::write(&blob_path, [0x89, 0x50, 0x4E, 0x47]).unwrap();

        let mut request = sample_request("http://localhost:1234/v1");
        request.messages = vec![Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                lingxiao_llm_host_protocol::MessageContentPart::Text {
                    text: "look at this".into(),
                },
                lingxiao_llm_host_protocol::MessageContentPart::ImageBlobRef {
                    image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                        blob_id: "blob_rehydrate".into(),
                        mime: "image/png".into(),
                        size: 4,
                        blob_path: blob_path.display().to_string(),
                        source: None,
                    },
                },
            ],
            ..Default::default()
        }];
        let input = responses_input(&request);
        let content = input[0]["content"].as_array().unwrap();
        assert_eq!(content[0]["type"], "input_text");
        assert_eq!(content[0]["text"], "look at this");
        assert_eq!(content[1]["type"], "input_image");
        let url = content[1]["image_url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"), "got: {url}");
        assert!(
            url.contains("iVBORw=="),
            "rehydrated data URI must carry the real base64 bytes, got: {url}"
        );
    }

    #[test]
    fn test_responses_input_retain_window_degrades_old_blob_but_rehydrates_recent() {
        // Two user rounds, each carrying a rehydratable blob. With the default
        // retain window (2) both are eligible, but with retain=1 only the most
        // recent round rehydrates; the older round's blob degrades to a text
        // placeholder on the wire.
        let dir = tempfile::tempdir().unwrap();
        let old_path = dir.path().join("old.png");
        let new_path = dir.path().join("new.png");
        std::fs::write(&old_path, [0x89, 0x50, 0x4E, 0x47]).unwrap();
        std::fs::write(&new_path, [0xFF, 0xD8, 0xFF]).unwrap();
        let old_path_str = old_path.display().to_string();
        let new_path_str = new_path.display().to_string();

        let blob_ref =
            |path: &str, id: &str| lingxiao_llm_host_protocol::MessageContentPart::ImageBlobRef {
                image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                    blob_id: id.into(),
                    mime: "image/png".into(),
                    size: 4,
                    blob_path: path.into(),
                    source: None,
                },
            };
        let user_with = |part| Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![part],
            ..Default::default()
        };
        let assistant = |text: &str| Message {
            role: "assistant".into(),
            content: text.into(),
            ..Default::default()
        };

        let mut request = sample_request("http://localhost:1234/v1");
        request.messages = vec![
            user_with(blob_ref(&old_path_str, "blob_old")), // round 1 (index 0)
            assistant("ack"),                               // index 1
            user_with(blob_ref(&new_path_str, "blob_new")), // round 2 (index 2)
        ];

        // retain=1 → only the most recent user round (index 2) rehydrates.
        request.options.metadata = Some(json!({
            "base_url": "http://localhost:1234/v1",
            "image_history_retain_rounds": 1
        }));
        let input = responses_input(&request);
        let old_content = input[0]["content"].as_array().unwrap();
        // Old round blob degrades to a text placeholder (never an image block,
        // never the filesystem path).
        assert_eq!(old_content[0]["type"], "input_text");
        let old_placeholder = old_content[0]["text"].as_str().unwrap();
        assert!(old_placeholder.contains("blob_old"));
        assert!(!old_placeholder.contains(&old_path_str));
        assert!(!old_content
            .iter()
            .any(|p| p["type"] == "input_image" || p.get("image_url").is_some()));
        // Recent round blob rehydrates into a real input_image block.
        let new_content = input[2]["content"].as_array().unwrap();
        assert_eq!(new_content[0]["type"], "input_image");
        let url = new_content[0]["image_url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"), "got: {url}");

        // retain=2 → both rounds rehydrate (window covers everything from the
        // 2nd user round from the end onward).
        request.options.metadata = Some(json!({
            "base_url": "http://localhost:1234/v1",
            "image_history_retain_rounds": 2
        }));
        let input = responses_input(&request);
        let old_content = input[0]["content"].as_array().unwrap();
        assert_eq!(old_content[0]["type"], "input_image");
    }

    #[test]
    fn test_to_chat_message_retain_window_degrades_old_blob_to_text() {
        // Chat-Completions path: an old-round blob (outside retain=1 window)
        // degrades to a text placeholder, never an image_url block.
        use async_openai::types::chat::{
            ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        };
        let dir = tempfile::tempdir().unwrap();
        let old_path = dir.path().join("old_chat.png");
        std::fs::write(&old_path, [0x89, 0x50, 0x4E, 0x47]).unwrap();
        let old_path_str = old_path.display().to_string();
        let message = Message {
            role: "user".into(),
            content: String::new(),
            content_parts: vec![
                lingxiao_llm_host_protocol::MessageContentPart::ImageBlobRef {
                    image: lingxiao_llm_host_protocol::ImageBlobRefContentPart {
                        blob_id: "blob_old_chat".into(),
                        mime: "image/png".into(),
                        size: 4,
                        blob_path: old_path_str.clone(),
                        source: None,
                    },
                },
            ],
            ..Default::default()
        };
        let built = to_chat_message(&message, false).unwrap();
        let ChatCompletionRequestMessage::User(ChatCompletionRequestUserMessage {
            content, ..
        }) = built
        else {
            panic!("expected a user message");
        };
        let parts = match content {
            ChatCompletionRequestUserMessageContent::Array(parts) => parts,
            other => panic!("expected multimodal content array, got {other:?}"),
        };
        assert_eq!(parts.len(), 1);
        match &parts[0] {
            ChatCompletionRequestUserMessageContentPart::Text(text) => {
                // plain_text() truncates the blob id to 12 chars.
                assert!(text.text.contains("blob_old_cha"));
                assert!(!text.text.contains(&old_path_str));
            }
            other => panic!("expected degraded text placeholder, got {other:?}"),
        }
    }

    fn sample_request(base_url: &str) -> GenerateRequest {
        GenerateRequest {
            model: "gpt-test".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: Vec::new(),
            stream: true,
            auth_context: AuthContext::ApiKey {
                provider: "openai".into(),
                key: "sk-test".into(),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(12),
                metadata: Some(json!({"base_url": base_url})),
                ..Default::default()
            },
        }
    }

    fn sample_azure_request(endpoint: &str) -> GenerateRequest {
        GenerateRequest {
            model: "ignored-by-azure-path".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: Vec::new(),
            stream: true,
            auth_context: AuthContext::AzureToken {
                endpoint: endpoint.into(),
                deployment_id: "deploy-test".into(),
                api_version: "2024-02-01".into(),
                api_key: "az-test".into(),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(12),
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
            assert!(request.starts_with("POST /v1/chat/completions"));
            assert!(request
                .to_lowercase()
                .contains("authorization: bearer sk-test"));
            let body_json: serde_json::Value = serde_json::from_str(http_body(&request)).unwrap();
            assert_eq!(body_json["model"], "gpt-test");
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
        });
        (format!("http://{addr}/v1"), handle)
    }

    fn spawn_azure_mock_server(
        status: u16,
        body: &'static str,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let request_bytes = read_http_request(&mut stream);
            let request = String::from_utf8_lossy(&request_bytes);
            assert!(request.starts_with(
                "POST /openai/deployments/deploy-test/chat/completions?api-version=2024-02-01"
            ));
            assert!(request.to_lowercase().contains("api-key: az-test"));
            assert!(!request.to_lowercase().contains("authorization: bearer"));
            let body_json: serde_json::Value = serde_json::from_str(http_body(&request)).unwrap();
            assert_eq!(body_json["messages"][0]["role"], "user");
            assert_eq!(body_json["max_completion_tokens"], 12);
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

    // -----------------------------------------------------------------------
    // P0: OpenAI Chat Completions — tool protocol contract tests
    // -----------------------------------------------------------------------

    fn sample_tool_request(base_url: &str) -> GenerateRequest {
        GenerateRequest {
            model: "gpt-test".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "list the files".into(),
                ..Default::default()
            }],
            tools: vec![lingxiao_llm_host_protocol::ToolDefinition {
                name: "list_dir".into(),
                description: "List a directory".into(),
                input_schema: json!({"type": "object", "properties": {"path": {"type": "string"}}}),
            }],
            stream: false,
            auth_context: AuthContext::ApiKey {
                provider: "openai".into(),
                key: "sk-test".into(),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(64),
                metadata: Some(json!({"base_url": base_url})),
                ..Default::default()
            },
        }
    }

    /// A request with a two-turn tool history: user → assistant(tool_calls) → tool result.
    fn sample_history_request(base_url: &str) -> GenerateRequest {
        use lingxiao_llm_host_protocol::ToolCall as ProtocolToolCall;
        GenerateRequest {
            model: "gpt-test".into(),
            messages: vec![
                Message {
                    role: "user".into(),
                    content: "read file".into(),
                    ..Default::default()
                },
                Message {
                    role: "assistant".into(),
                    content: "I will read the file.".into(),
                    tool_calls: vec![ProtocolToolCall {
                        id: "call_xyz".into(),
                        name: "file_read".into(),
                        arguments: json!({"path": "README.md"}),
                    }],
                    ..Default::default()
                },
                Message {
                    role: "tool".into(),
                    content: "{\"content\":\"README content\"}".into(),
                    tool_call_id: Some("call_xyz".into()),
                    name: Some("file_read".into()),
                    ..Default::default()
                },
            ],
            tools: vec![lingxiao_llm_host_protocol::ToolDefinition {
                name: "file_read".into(),
                description: "Read a file".into(),
                input_schema: json!({"type": "object"}),
            }],
            stream: false,
            auth_context: AuthContext::ApiKey {
                provider: "openai".into(),
                key: "sk-test".into(),
            },
            options: lingxiao_llm_host_protocol::RequestOptions {
                max_tokens: Some(64),
                metadata: Some(json!({"base_url": base_url})),
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_chat_tools_included_in_request_body() {
        // When tools are present, the Chat Completions request must include both
        // "tools" and "tool_choice" fields.
        let request = sample_tool_request("http://localhost:1234/v1");
        let body = to_chat_completion_request(&request).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        let tools = json["tools"].as_array().expect("tools must be an array");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["function"]["name"], "list_dir");
        assert_eq!(tools[0]["function"]["description"], "List a directory");
        assert!(
            json.get("tool_choice").is_some(),
            "tool_choice must be present"
        );
    }

    #[test]
    fn test_chat_assistant_tool_calls_in_history() {
        // An assistant message with tool_calls must map to a ChatCompletionRequestMessage
        // with both content (if non-empty) and tool_calls populated.
        let request = sample_history_request("http://localhost:1234/v1");
        let body = to_chat_completion_request(&request).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        let msgs = json["messages"].as_array().unwrap();

        // Message[0] = user, [1] = assistant with tool_calls, [2] = tool result.
        let assistant = &msgs[1];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(assistant["content"], "I will read the file.");
        let tool_calls = assistant["tool_calls"]
            .as_array()
            .expect("assistant must have tool_calls");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["id"], "call_xyz");
        assert_eq!(tool_calls[0]["function"]["name"], "file_read");
    }

    #[test]
    fn test_responses_input_preserves_assistant_content_with_tool_calls() {
        let request = sample_history_request("http://localhost:1234/v1");
        let input = responses_input(&request);
        let items = input.as_array().unwrap();
        assert!(items.iter().any(|item| {
            item["type"] == "function_call"
                && item["call_id"] == "call_xyz"
                && item["name"] == "file_read"
        }));
        assert!(items.iter().any(|item| {
            item["role"] == "assistant" && item["content"] == "I will read the file."
        }));
    }

    #[test]
    fn test_chat_tool_role_maps_to_tool_message() {
        // A "tool" role message must map to a Chat API tool-result message with
        // "role": "tool" and the correct "tool_call_id".
        let request = sample_history_request("http://localhost:1234/v1");
        let body = to_chat_completion_request(&request).unwrap();
        let json = serde_json::to_value(&body).unwrap();
        let msgs = json["messages"].as_array().unwrap();
        let tool_msg = &msgs[2];
        assert_eq!(tool_msg["role"], "tool");
        assert_eq!(tool_msg["tool_call_id"], "call_xyz");
        assert!(tool_msg["content"].as_str().is_some());
    }

    #[test]
    fn test_chat_non_stream_response_extracts_tool_call() {
        // A non-streaming Chat response whose finish_reason is "tool_calls" must
        // produce a StreamEvent::ToolCall with the correct id, name, and arguments.
        let response_json = r#"{
            "id":"chatcmpl-tc","object":"chat.completion","created":1,"model":"gpt-test",
            "choices":[{
                "index":0,
                "message":{
                    "role":"assistant",
                    "content":null,
                    "tool_calls":[{
                        "id":"call_nonstream",
                        "type":"function",
                        "function":{"name":"list_dir","arguments":"{\"path\":\"/tmp\"}"}
                    }]
                },
                "finish_reason":"tool_calls"
            }],
            "usage":{"prompt_tokens":5,"completion_tokens":8,"total_tokens":13}
        }"#;
        let value: serde_json::Value = serde_json::from_str(response_json).unwrap();
        let response: CreateChatCompletionResponse = serde_json::from_value(value).unwrap();
        let events = response_to_stream_events(response);
        assert!(
            events.iter().any(|e| matches!(
                e,
                StreamEvent::ToolCall(call)
                    if call.id == "call_nonstream"
                       && call.name == "list_dir"
                       && call.arguments["path"] == "/tmp"
            )),
            "expected ToolCall(list_dir) in non-stream events, got: {events:?}"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, StreamEvent::Finished(FinishReason::ToolCalls))),
            "expected Finished(ToolCalls) in non-stream events"
        );
    }

    #[test]
    fn test_chat_streaming_accumulates_tool_call_from_deltas() {
        // The streaming path must accumulate ToolCallDelta chunks and emit a final
        // StreamEvent::ToolCall on Finished(ToolCalls). Chunks are deserialised from
        // the OpenAI wire JSON to avoid depending on deprecated/private struct fields.
        let chunk1_json = r#"{
            "id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-test",
            "choices":[{
                "index":0,
                "delta":{"tool_calls":[{
                    "index":0,"id":"call_stream","type":"function",
                    "function":{"name":"file_read","arguments":""}
                }]},
                "finish_reason":null
            }]
        }"#;
        let chunk2_json = r#"{
            "id":"chatcmpl-s","object":"chat.completion.chunk","created":1,"model":"gpt-test",
            "choices":[{
                "index":0,
                "delta":{"tool_calls":[{
                    "index":0,
                    "function":{"arguments":"{\"path\":\"/home\"}"}
                }]},
                "finish_reason":"tool_calls"
            }]
        }"#;

        let chunks: Vec<CreateChatCompletionStreamResponse> = [chunk1_json, chunk2_json]
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("valid stream chunk"))
            .collect();

        let mut accumulated = ToolCallAccumulator::new();
        let mut got_final_tool_call = false;
        for chunk in chunks {
            for event in chat_stream_chunk_to_events(chunk) {
                match event {
                    StreamEvent::ToolCallDelta(delta) => {
                        accumulated.append(delta);
                    }
                    StreamEvent::Finished(FinishReason::ToolCalls) => {
                        for call in accumulated.finalize() {
                            if call.id == "call_stream" && call.name == "file_read" {
                                assert_eq!(call.arguments["path"], "/home");
                                got_final_tool_call = true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        assert!(
            got_final_tool_call,
            "expected accumulated ToolCall from streaming deltas"
        );
    }
}
