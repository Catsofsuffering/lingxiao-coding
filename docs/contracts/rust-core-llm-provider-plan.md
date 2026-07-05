# Rust Core LLM Provider 迁移计划

> Rust Core-first；LLM routing/budget/retry/usage/circuit breaker 属于 Rust Core。Provider 执行优先 Rust-native：优先官方 Rust SDK/API；没有官方方案时用成熟社区 Rust crate；社区方案不足时允许 fork/二开；只有不可行才用 llm-host sidecar。短期不需要旧 TUI/Web/Electron 兼容。

## 1. 现有 TS LLM 代码盘点

### 1.1 文件清单

| 路径 | 行数 | 功能 |
|------|------|------|
| `src/llm/ContentGenerator.ts` | 191 | 统一 ContentGenerator 接口 |
| `src/llm/OpenAIContentGenerator.ts` | 777 | OpenAI SDK native 实现 |
| `src/llm/AnthropicContentGenerator.ts` | 1114 | Anthropic SDK native 实现 |
| `src/llm/ContentGenerationPipeline.ts` | 165 | 共享 streaming pipeline |
| `src/llm/LoggingContentGenerator.ts` | 132 | 审计日志 + TTFT 装饰器 |
| `src/llm/Client.ts` | 221 | LLMClientManager v4 — 客户端工厂 |
| `src/llm/ModelGateway.ts` | 350 | 模型选择与网关路由 |
| `src/llm/RetryEngine.ts` | 176 | 重试引擎 + 退避 |
| `src/llm/CircuitBreaker.ts` | 215 | 断路器 |
| `src/llm/provider_runtime.ts` | 232 | retryProviderOperation + heartbeat |
| `src/llm/errors.ts` | 808 | 错误分类 (30+ 规则) |
| `src/llm/types.ts` | 224 | 核心类型定义 |
| `src/llm/StreamingToolCallParser.ts` | 323 | streaming tool call JSON 解析 |
| `src/llm/usageExtractor.ts` | 220 | token usage 标准化 |
| `src/llm/token_counter.ts` | 219 | 令牌计数 |
| `src/llm/tokenLimits.ts` | 100 | 自适应 output token 调节 |
| `src/llm/CostService.ts` | 388 | 模型定价 + 成本计算 |
| `src/llm/model_capabilities.ts` | 526 | 模型能力管理 |
| `src/llm/ModelsDevRegistry.ts` | 420 | 模型注册表客户端 |
| `src/llm/models-snapshot.json` | 185K | 700+ 模型内置快照 |
| `src/llm/providers/index.ts` | 57 | Vercel AI SDK 提供者工厂 |
| `src/llm/providers/openai.ts` | 15 | `@ai-sdk/openai` 封装 |
| `src/llm/providers/anthropic.ts` | 15 | `@ai-sdk/anthropic` 封装 |
| `src/llm/providers/google.ts` | 15 | `@ai-sdk/google` 封装 |
| `src/llm/providers/bedrock.ts` | 17 | `@ai-sdk/amazon-bedrock` 封装 |
| `src/llm/providers/custom.ts` | 19 | 自定义 OpenAI-compat (Ollama/vLLM) |
| `src/llm/http_dispatcher.ts` | 160 | 共享 HTTP dispatcher |
| `src/llm/image_blob_store.ts` | 166 | 图片 blob 持久化与恢复 |
| `src/llm/local_vision_fallback.ts` | 363 | OCR fallback (Tesseract) |
| `src/llm/message_sanitizer.ts` | 560 | 消息序列消毒 |
| `src/llm/reasoningSampling.ts` | 77 | reasoning temperature 保护 |
| `src/llm/promptCacheKey.ts` | 62 | prompt cache key 计算 |
| `src/core/LocalLlmGateway.ts` | 503 | 本地 LLM 网关服务器 |
| `src/agents/LlmGuard.ts` | 869 | 重试循环 + 挂起看门狗 + fallback |
| `src/config.ts` | — | `gateway_routes`, `gateway_fallback_models` 配置 |

### 1.2 LLM 相关依赖

| 包 | 版本 | 用途 |
|----|------|------|
| `openai` | ^6.33.0 | OpenAI 官方 TS SDK (native path) |
| `@anthropic-ai/sdk` | ^0.82.0 | Anthropic 官方 TS SDK (native path) |
| `@ai-sdk/openai` | ^2.0.106 | Vercel AI SDK OpenAI provider |
| `@ai-sdk/anthropic` | ^2.0.81 | Vercel AI SDK Anthropic provider |
| `@ai-sdk/google` | ^2.0.74 | Vercel AI SDK Google provider |
| `@ai-sdk/amazon-bedrock` | ^3.0.101 | Vercel AI SDK Bedrock provider |
| `ai` | ^5.0.198 | Vercel AI SDK core |
| `js-tiktoken` | ^1.0.21 | 令牌编码 |
| `undici` | ^8.0.2 | HTTP dispatcher |
| `@langfuse/client` | ^5.5.3 | LLM 可观测性 |

### 1.3 现有架构概要

```
LlmGuard (重试 + 断路器 + 看门狗 + fallback)
  └─ ContentGenerator (接口)
       ├─ OpenAIContentGenerator (native SDK, maxRetries=0)
       ├─ AnthropicContentGenerator (native SDK, maxRetries=0)
       └─ VercelAIContentGenerator (ai SDK + provider registry)
  └─ LoggingContentGenerator (装饰器)
  └─ LLMClientManager (工厂)
Infra: RetryEngine, CircuitBreaker, provider_runtime, errors.ts, usageExtractor
Model: ModelGateway, ModelsDevRegistry, CostService, model_capabilities
```

所有 ContentGenerator 实现设 `maxRetries=0`，重试权威完全归 LlmGuard（单一重试控制器，避免双重重试）。

---

## 2. Provider-by-Provider Rust 方案矩阵

### 2.1 官方 Rust SDK 状态总览

| Provider | 官方 Rust SDK | 证据 |
|----------|-------------|------|
| OpenAI | ❌ 不存在 | 官方 SDK: Python (`openai/openai-python`), TypeScript/JS (`openai/openai-node`)。OpenAI GitHub org 下无 Rust 项目。<br>OpenAI 官方 Libraries 页面 (<https://platform.openai.com/docs/libraries>) 无 Rust。<br>社区 Rust 库 `async-openai` 在 README 中自述 "unofficial Rust library"。 |
| Anthropic | ❌ 不存在 | 官方 SDK: Python (`anthropics/anthropic-sdk-python`), TypeScript (`anthropics/anthropic-sdk-typescript`)。Anthropic 官方 API 文档列出 Python/TS/C#/Go/Java/PHP/Ruby，无 Rust。 |
| Google Gemini | ❌ 不存在 | 官方 SDK: Python, TypeScript, Go, Java, Swift, Kotlin。Google <https://ai.google.dev/gemini-api/docs/libraries> 无 Rust。 |
| AWS Bedrock | ✅ 存在 | `aws-sdk-bedrockruntime` (awslabs/aws-sdk-rust) — AWS 官方 Rust SDK 的一部分。 |
| Azure OpenAI | ❌ 不存在 | 无独立官方 Rust SDK。Azure SDK for Rust 中不含 OpenAI 资源。 |

### 2.2 OpenAI

**选型前提**：OpenAI 无官方 Rust SDK。`async-openai` 是当前最成熟的社区 crate，也是 OpenAI 社区生态中事实标准。

| 维度 | 方案 |
|------|------|
| **推荐** | **`async-openai`** (v0.41.1, MIT) — 社区 crate，事实标准 |
| Grading | community (64bit, 非官方) — 6,042,936+ downloads (crates.io)，120 版本，自 2022-12 |
| 仓库 | <https://github.com/64bit/async-openai> |
| crates.io | <https://crates.io/crates/async-openai> |
| GitHub | 2k stars, 380 forks, 386 commits, 107 releases |
| 流式 | ✅ SSE streaming (chat completion stream, response stream) |
| Tool calls | ✅ `ChatCompletionMessageToolCall`, Function |
| 多模态/视觉 | ✅ image_url, 多模态输入 |
| 结构输出 | ✅ response_format + json_schema |
| Usage | ✅ 完整 usage 字段 |
| Retry/rate limit | ⚠️ 内置 exponential backoff，但 **必须禁用 SDK retry** (设 `max_retries=0`)，重试只归 Rust Core |
| Proxy/base_url | ✅ `OpenAIConfig` 自定义 base_url, OpenAI-compatible |
| Azure | ✅ 通过自定义 `Config` adapter 支持 endpoint/deployment_id/api_version |
| Responses API | ✅ 官方 Responses API 支持 |
| Realtime API | ✅ WebSocket Realtime |
| Middleware | ✅ Tower ecosystem middleware |
| WASM | ✅ 完整 WASM 支持 |
| Reasoning | ⚠️ `reasoning_effort` (o-series) |
| 额外 | Dynamic dispatch (`Box<dyn Config>`), BYOT (bring your own types), webhook |
| **候选** | **`openai-oxide`** (v0.15.0, MIT, fortunto2) — 更新但下载量低 (1,004 total)，特色：1:1 Python SDK parity claim、内置 `anthropic`/`azure`/`openrouter` 模块、hedged request、middleware 拦截器。需实测后决定是否作为补充/替代。 |
| openai-oxide 仓库 | <https://github.com/fortunto2/openai-oxide> |
| **回退** | 自建 reqwest+serde 最小 client — 如果社区 crate 均不满足需求 |

**评估建议**：Phase 2 先以 `async-openai` 为主路线实施。若发现 Streaming tool call delta 格式、Responses API 事件结构等不满足需求，同期评估 `openai-oxide` 作为备选。最终可二选一或同时维护 adapter。

### 2.3 自定义 OpenAI-compatible (Ollama, vLLM, LiteLLM, LocalAI)

| 维度 | 方案 |
|------|------|
| **推荐** | **`async-openai`** 或 **`openai-oxide`** — 都支持自定义 `base_url` |
| 使用方式 | `OpenAIConfig::new().with_api_base("http://localhost:11434/v1")` |

### 2.4 Anthropic (Claude)

**选型前提**：Anthropic 无官方 Rust SDK。社区 `anthropic-sdk-rust` 存在但存在供应链风险（见下文）。按"社区优先"原则，**先评估 `anthropic-sdk-rust`；若不满足则自研 minimal client**。

| 维度 | 方案 |
|------|------|
| **推荐** | **先评估 `anthropic-sdk-rust` (v0.1.1)** |
| Grading | community (dimichgh, 非 Anthropic 官方) |
| crates.io | <https://crates.io/crates/anthropic-sdk-rust> |
| 下载量 | 9,474 total |
| 流式 | ✅ 流式 messages 支持 |
| Tool calls | ✅ tools, tool_use block |
| 视觉 | ✅ vision |
| Extended thinking | ✅ thinking block + signature |
| Usage/cache | ✅ usage 含 cache metrics |
| Files/batch | ✅ file upload, batch processing |
| **供应链风险** | v0.1.0 (2025-06-11) 在 crates.io metadata 中谎报 `homepage` 和 `repository` 为 `https://github.com/anthropics/anthropic-sdk-rust`（伪称 Anthropic 官方仓库）。v0.1.1 才修正为 `dimichgh/anthropic-sdk-rust`。此行为表明 crate 维护者曾经意图误导用户认为 crate 是官方的。<br>来源：对比 crates.io API v0.1.0 (`"homepage":"https://github.com/anthropics/anthropic-sdk-rust"`) 与 v0.1.1 (`"homepage":"https://github.com/dimichgh/anthropic-sdk-rust"`)。<br>影响：供应链安全低信任度；持续维护能力存疑（仅 2 个版本）。 |
| **备选** | **自研 minimal Messages API HTTP client** — Anthropic Messages API 协议简洁 (`POST /v1/messages`，SSE `text/event-stream` 响应)。只需 `reqwest` + `serde` + `eventsource-stream`。 |
| **备选 2** | `openai-oxide` 的 `anthropic` 模块 — 如果采用 `openai-oxide` 路线，可用其 `decorate_request` + `is_anthropic_model`。 |
| **兜底** | llm-host sidecar（仅当所有 Rust-native 方案不可行时） |

**评估建议**：Phase 2 优先快速评估 `anthropic-sdk-rust` 功能完整性（特别是 streaming thinking block 解析、tool call delta 格式）。若发现问题或对供应链风险不可接受，立即切换自研 minimal client（预计 2-3 天工作量）。

### 2.5 Google Gemini

| 维度 | 方案 |
|------|------|
| **推荐** | **`gemini-rust`** (v1.7.1, MIT/Apache-2.0) — 社区 crate，非 Google 官方 |
| Grading | community |
| 仓库 | <https://github.com/longportapp/gemini-rust> |
| 文档 | <https://docs.rs/gemini-rust> |
| 下载量 | crates.io current |
| 流式 | ✅ stream generation |
| Tool calls | ✅ `FunctionCall`, `FunctionDeclaration`, `ToolConfig` |
| 视觉/多模态 | ✅ `Blob`, `FileDataRef`, `Modality` |
| Thinking | ✅ `ThinkingConfig`, `ThinkingLevel` |
| Usage | ✅ `UsageMetadata` |
| Caching | ✅ `CacheBuilder`, `CachedContentHandle` |
| Embedding | ✅ embedding |
| Vertex AI | ✅ feature flag `vertex` (GCP Vertex AI) |
| Pricing | ✅ `GeminiPricing`, `estimate_cost` |
| **风险** | `adk-gemini` v1.0.0 依赖 `adk-core`，当前会拉高 Rust MSRV，不适合本工程 `rust-version=1.75` / 实测 rustc 1.86。现阶段采用 `gemini-rust`，可注入 API key/base_url，不读 env。 |

### 2.6 AWS Bedrock

| 维度 | 方案 |
|------|------|
| **推荐** | **`aws-sdk-bedrockruntime`** (pinned v1.27.0, Apache-2.0) — 唯一 official 的 Rust provider SDK |
| Grading | official (AWS 官方维护, awslabs) |
| 仓库 | <https://github.com/awslabs/aws-sdk-rust> |
| 下载量 | 3,512,087 (非常成熟) |
| 流式 | ✅ `InvokeModelWithResponseStream` 暴露 Smithy event stream / typed `ConverseStreamOutput`，非普通 SSE/JSONL |
| Tool calls | ✅ Converse API tool configuration |
| 多模态 | ✅ Claude on Bedrock vision (Converse API) |
| Usage | ✅ 从 `ConverseStreamOutput` 中提取 token usage |
| Credential | Core 传入 `AuthContext::AwsSignature`；Provider Executor 不读 env/profile/IRSA/IMDS |
| **实现要点** | v1.27.0 暴露 `InvokeModel` / `InvokeModelWithResponseStream`。本阶段使用 official SDK `InvokeModel` 接路，默认 Anthropic-on-Bedrock JSON body，同时支持 `metadata.bedrock_body` 覆盖；SDK retry 必须 disabled (`max_attempts=1`)。升级 SDK/rustc 后再补 `ConverseStreamOutput` adapter。 |

### 2.7 Azure OpenAI

| 维度 | 方案 |
|------|------|
| **推荐** | **`async-openai`** + 自定义 `Config` adapter — API key + endpoint + deployment_id + api_version |
| Grading | community (同父 crate) |
| **备选** | `openai-oxide::azure::AzureConfig` (如果采用 openai-oxide) |
| **兜底** | `siumai-provider-azure` — 下载量低 (393)，不推荐优先 |

---

## 3. Rust Core Trait 设计

### 3.1 `LlmProvider` trait

```rust
#[async_trait]
pub trait LlmProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    async fn list_models(&self) -> Result<Vec<ModelInfo>>;

    fn supports_model(&self, model_id: &str) -> bool;

    async fn generate(
        &self,
        request: GenerateRequest,
    ) -> Result<GenerateResponse, LlmError>;

    async fn generate_stream(
        &self,
        request: GenerateRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError>;

    fn count_tokens(&self, text: &str, model: &str) -> Result<u32>;
}
```

### 3.2 统一事件标准化 `StreamEvent`

```rust
pub enum StreamEvent {
    Text(String),
    Thinking(String),
    ToolCallDelta(ToolCallDeltaInfo),    // index, id?, name?, partial_json
    ToolCall(ToolCall),                   // 完整 tool call（完成时）
    Usage(TokenUsage),
    Error(LlmError),
    FirstToken,
    Progress { elapsed: Duration, status: String },
    Retry { attempt: u32, error: LlmError },
    StreamRetry { attempt: u32, error: LlmError },
}
```

对应 TS `LlmRoundEvent`。

### 3.3 Tool-Call Delta 累加

```rust
pub struct ToolCallAccumulator {
    calls: Vec<ToolCallBuilder>,
}

impl ToolCallAccumulator {
    pub fn append(&mut self, delta: ToolCallDeltaInfo);
    pub fn finalize(&mut self) -> Vec<ToolCall>;
}
```

对应 TS `StreamingToolCallParser.ts` — 4 级 fallback JSON 修复策略。

### 3.4 Usage 标准化

```rust
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
    pub cache_creation_input_tokens: Option<u32>,
    pub cache_read_input_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
}
```

对应 TS `usageExtractor.ts`。

### 3.5 Retry 与断路器

```rust
pub struct RetryConfig {
    pub max_retries: u32,
    pub backoff: Vec<Duration>,      // [500ms, 1000ms, 2000ms] cyclic
    pub max_delay: Duration,
}

impl RetryEngine {
    pub async fn execute<F, T>(&self, f: F) -> Result<T, LlmError>
    where F: Fn() -> Future<Output = Result<T, LlmError>>;
}

pub struct CircuitBreaker {
    state: AtomicU8,                  // CLOSED/OPEN/HALF_OPEN
    failure_count: AtomicU32,
    threshold: u32,                   // 8
    reset_timeout: Duration,          // 15s
}

impl CircuitBreaker {
    pub fn before_request() -> Result<(), CircuitOpenError>;
    pub fn on_success();
    pub fn on_failure();
    pub fn reset();
}
```

### 3.6 Model Registry

```rust
pub struct ModelRegistry {
    snapshot: HashMap<String, ModelInfo>,
    remote: Option<ModelsDevClient>,
    cache: Option<LocalCache>,
}

pub struct ModelInfo {
    pub model_id: String,
    pub provider: String,
    pub context_limit: u32,
    pub output_limit: u32,
    pub capabilities: ModelCapabilities,   // vision, reasoning, tool_call, streaming
    pub pricing: ModelPricing,
    pub thinking_config: Option<ThinkingConfig>,
}
```

### 3.7 Provider Executor 注册

```rust
pub struct ProviderRegistry {
    providers: HashMap<&'static str, Arc<dyn LlmProvider>>,
}

impl ProviderRegistry {
    pub fn register(provider: Arc<dyn LlmProvider>);
    pub fn get(&self, provider_id: &str) -> Option<&Arc<dyn LlmProvider>>;
    pub fn resolve(&self, model_id: &str) -> Result<&Arc<dyn LlmProvider>>;
}
```

---

## 4. Rust Core vs Provider Executor 边界

### 4.1 属于 Rust Core (lingxiao-core::llm)

| 功能 | TS 来源 | 说明 |
|------|---------|------|
| **Credential/config 管理** | `LocalLlmGateway`, `config.ts` | 凭证解析（环境变量/配置文件/secret store）、provider 配置读取、虚拟密钥管理。Provider Executor 只接收已解析的 `AuthContext`，不负责 credential 来源。 |
| **Permission/resource accounting** | `LocalLlmGateway` | 密钥校验、配额预留与提交 |
| **Model routing** | `ModelGateway.ts` | 根据目的、成本上限、能力要求选择模型链 |
| **Budget management** | `CostService.ts`, `LocalLlmGateway` quota | RPM/TPM/daily budget, cost ceiling |
| **Retry loop** | `LlmGuard.ts`, `RetryEngine.ts` | 统一重试 + 退避 + fallback 链。**必须禁用 SDK/crate 内置 retry**（设 max_retries=0），重试只归 Rust Core。 |
| **Circuit breaker** | `CircuitBreaker.ts` | 每 provider-scope 断路器。Rust Core 同步断路器状态给 Provider Executor。 |
| **Usage tracking** | `usageExtractor.ts` | 归一化 token usage，持久化成本核算 |
| **Usage commit** | `LocalLlmGateway` | 实际用量提交到预算系统。Provider Executor 只报告 raw usage。 |
| **Error classification** | `errors.ts` | 30+ 分类规则。Provider Executor 返回网络/协议错误，Rust Core 分类并决定重试/降级/熔断。 |
| **Stream normalization** | `ContentGenerationPipeline.ts` | 统一事件流 |
| **Tool-call delta accumulation** | `StreamingToolCallParser.ts` | 增量 JSON 累加与修复 |
| **Model registry** | `ModelsDevRegistry.ts` | 本地快照 + 远程刷新 |
| **Provider health check** | — | 周期性健康探测 |
| **SDK retry 禁用** | — | 确保所有 Provider Executor 在构造时设置 `max_retries=0`，防止 Rust Core 外独立重试 |

### 4.2 属于 Provider Executor (各自的 crate / impl)

| 功能 | 说明 |
|------|------|
| HTTP transport | 各 crate 自带 reqwest 客户端 |
| Auth header/signature | 接收已解析的 `AuthContext`，构造 provider-specific 认证头 (API key header, AWS SigV4, Bearer token 等) |
| Request serialization | 将 Rust Core 通用 `GenerateRequest` 转换为 provider-specific wire format |
| Response deserialization | 解析 provider-specific 响应体 |
| Event stream/SSE parsing | 解析原始 SSE 事件或 Smithy event stream 为半结构化 chunk |
| Streaming chunk → delta | 将 provider-specific chunk 转换为 `StreamEvent` delta |
| **禁用项** | SDK 内置 retry (`max_retries=0`)、SDK 内置 rate-limit、SDK 内置 timeout（由 Rust Core 统一控制） |

**关键约束**：Provider Executor 不读取环境变量/配置文件/secret store，不独立处理重试逻辑，不独立做 rate limiting，不独立上报 usage。所有凭证来源、重试决策、预算控制、usage commit 只归 Rust Core 统一管理。

---

## 5. 边界风险与 Fallback 策略

| 风险 | 级别 | Fallback |
|------|------|----------|
| `anthropic-sdk-rust` 供应链风险（曾伪称官方仓库 URL） | 高 | 自研 minimal reqwest+serde Messages API client |
| `async-openai` 停止维护或 API 不兼容 | 中 | 切换到 `openai-oxide`，或 fork 自维护 |
| `openai-oxide` 下载量低、社区小 | 中 | 回退 `async-openai`，不优先依赖 |
| `gemini-rust` API 覆盖不足 | 中 | 评估 `adk-gemini`（需升级 rustc/MSRV）或自建轻量 Gemini adapter |
| AWS Bedrock event stream (Smithy) 映射复杂 | 低 | 逐步实现 `ConverseStreamOutput` → `StreamEvent` adapter |
| Azure Entra ID OAuth 流程复杂 | 低 | 先用 API key 认证，OAuth 后加 |
| 社区 crate 不满足需求 | 低 | fork 二开；自研；最后 sidecar |
| Provider 新增需快速接入 | 低 | 统一 HTTP 模板 + serde 类型生成 |

---

## 6. Phase 1/2/3 迁移任务

### Phase 1 — 基础设施与核心 trait (2-3 周)

- [ ] 在 `crates/lingxiao-core/llm/` 下建立模块骨架
- [ ] 定义核心 trait: `LlmProvider`, `StreamEvent`, `TokenUsage`, `ToolCallAccumulator`
- [ ] 定义 `AuthContext` 结构 — 统一凭证传递 (API key, Bearer token, AWS 凭证, Azure token 等)
- [ ] 实现 `ModelRegistry` + 内置 snapshot 加载 (移植 700+ 模型数据)
- [ ] 实现 `CircuitBreaker` 状态机
- [ ] 实现 `RetryEngine` + 退避策略 (full-jitter)
- [ ] 实现 Error 分类系统 (`LlmErrorKind`)
- [ ] 集成 `tiktoken-rs` (v0.12.0) 令牌计数
- [ ] 集成 `governor` crate 或自建 RPM/TPM rate limiter

### Phase 2 — Provider Executors (3-4 周)

- [ ] **OpenAI** — 集成 `async-openai`，同期评估 `openai-oxide`
  - [ ] 流式 chat completion / Responses API
  - [ ] 设 `max_retries=0`，禁用内置 retry
  - [ ] Tool calls + delta accumulation
  - [ ] Vision/multimodal
  - [ ] Reasoning/thinking (o-series)
  - [ ] Usage 提取
  - [ ] Proxy/base_url 支持
  - [ ] Create `StreamEvent` adapter for `async-openai` streaming events
- [ ] **Anthropic** — 先评估 `anthropic-sdk-rust` v0.1.1
  - [ ] 验证 streaming thinking block 解析完整性
  - [ ] 验证 tool call delta 格式兼容性
  - [ ] 验证供应链风险可接受度
  - [ ] 若评估不通过，切换自研 minimal Messages API client
  - [ ] Create `StreamEvent` adapter
- [ ] **Google Gemini** — 集成 `gemini-rust`
  - [ ] 流式生成 + StreamEvent adapter
  - [ ] Tool calls
  - [ ] ThinkingConfig
  - [ ] Vision/multimodal
  - [ ] Usage metadata
- [ ] **AWS Bedrock** — 集成 pinned `aws-sdk-bedrockruntime` v1.27.0
  - [ ] InvokeModel (non-streaming) first; Converse API after rustc/SDK upgrade
  - [ ] Tool configuration
  - [ ] 实现 Bedrock response → `StreamEvent` adapter
  - [ ] Core-resolved `AuthContext::AwsSignature` only; no AWS credential chain in executor
- [ ] **Azure OpenAI** — 复用 `async-openai` with custom `Config` adapter
  - [ ] API key auth
  - [ ] Entra ID OAuth (optional)
- [ ] **自定义 OpenAI-compatible** — 用相同 crate 改 base_url
  - [ ] 本地网关支持 (Ollama, vLLM, LiteLLM)
- [ ] Provider Registry 工厂函数
  - [ ] 禁用所有 SDK 内置 retry/rate-limit（设 `max_retries=0`）

### Phase 3 — 集成与替换 (2-3 周)

- [ ] Gateway — Rust 版模型路由 (基于目的/成本/能力)
- [ ] `UsageTracker` — 持久化 usage + 成本核算
- [ ] `LlmGuard` — Rust 版重试循环 + 看门狗 + fallback 链
- [ ] 统一 `ProviderRuntime` — retry + circuit breaker + usage commit 编排
- [ ] 本地 LLM 网关 (OpenAI-compatible / Anthropic 端点)
- [ ] 性能 benchmark: 对比 TS vs Rust
- [ ] 存量 TS 代码逐步适配 Rust Core
- [ ] 删除冗余 TS 文件

---

## 7. QA Checklist

### 7.1 功能正确性

| 检查项 | 验证方法 |
|--------|----------|
| 每个 provider 的非流式生成与 TS 输出一致 | 相同 prompt 对比 response text |
| 每个 provider 的流式生成与 TS 逐 token 一致 | 逐 chunk 对比 |
| Tool call 格式正确 (id, name, arguments) | 对比 TS 输出 JSON |
| tool-call delta 累加正确 | 模拟增量注入 + finalize |
| Usage 数值与 provider 原始响应一致 | mock response + extract |
| Thinking/reasoning block 内容正确 | Claude thinking + o-series reasoning_effort |
| 视觉输入 (base64/URL) 正确 | 相同图片对比 |
| 自定义 base_url 生效 | 本地 Ollama 测试 |
| 重试触发正确次数 (仅 Rust Core) | mock 5xx 响应, 验证 SDK max_retries=0 |
| 断路器状态转移正确 | mock 连续失败后 OPEN/HALF_OPEN |
| 模型注册表加载正确 | 700+ 模型信息可查询 |
| Token 计数与 js-tiktoken 一致 | 相同文本对比 |
| SDK 内置 retry 全部禁用 | 所有 Provider Executor 构造时验证 max_retries=0 |

### 7.2 错误处理

| 检查项 | 验证方法 |
|--------|----------|
| 网络错误分类 | mock connection refused |
| 4xx 错误分类 | mock 401, 403, 429 |
| 5xx 错误分类 + 重试 | mock 500, 502, 503 |
| Context overflow 检测 | 超长 prompt |
| Rate limit 检测 + Retry-After | mock 429 + header |
| Auth error 非重试 | mock 401, 无需重试 |
| Stream 中断恢复 | mock 中途断开 |

### 7.3 性能 & 资源

| 检查项 | 验证方法 |
|--------|----------|
| 并发请求下无数据竞争 | `cargo test --test concurrent` |
| 内存泄漏 (streaming) | 1000 次短流式，观测 RSS |
| HTTP 连接复用 | `reqwest` client 复用 |
| Token counting 性能 | 100KB 文本 < 10ms |
| Circuit breaker 不退化性能 | fast-fail < 1μs |
| Model registry 启动时间 | 加载 snapshot < 100ms |

### 7.4 配置 & 集成

| 检查项 | 验证方法 |
|--------|----------|
| 凭证从 Rust Core 正确传递到 Provider Executor | mock `AuthContext` |
| Provider Executor 不读取 env var | 环境变量未设置时不应 fallback 到默认值 |
| 默认 API key 缺失时优雅降级 | 缺失 key 返回清晰错误 |
| Proxy 配置生效 | `HTTP_PROXY` / `HTTPS_PROXY` |
| 自定义 base_url 不影响官方 endpoint | 隔离测试 |

---

## 8. 依赖汇总表

| Crate | 版本 | License | 用途 | 方案层级 |
|-------|------|---------|------|----------|
| `async-openai` | 0.41.1 | MIT | OpenAI, Azure, OpenAI-compat (主选) | community |
| `openai-oxide` | 0.15.0 | MIT | OpenAI, Azure (候选评估) | community |
| `anthropic-sdk-rust` | 0.1.1 | MIT | Anthropic (先评估，有供应链风险) | community |
| `gemini-rust` | 1.7.1 | MIT/Apache-2.0 | Google Gemini | community |
| `aws-sdk-bedrockruntime` | pinned 1.27.0 | Apache-2.0 | AWS Bedrock | official |
| `aws-config` | 1.x | Apache-2.0 | AWS 凭证链 | official |
| `eventsource-stream` | 0.2.x | — | SSE 流解析 (自研 Anthropic client 需要) | community |
| `tiktoken-rs` | 0.12.0 | MIT | Token 编码计数 | community |
| `governor` | 0.6.x | MIT | RPM/TPM rate limiting (可选项) | community |
| `reqwest` | 0.12.x | MIT/Apache-2.0 | HTTP 客户端 | standard |
| `serde` / `serde_json` | 1.x | MIT/Apache-2.0 | 序列化 | standard |
| `tokio` | 1.x | MIT | 异步运行时 | standard |
| `tracing` | 0.1.x | MIT | 结构化日志 | standard |
| `schemars` | 0.8.x | MIT | JSON Schema 生成 | standard |
| `thiserror` | 2.x | MIT/Apache-2.0 | 错误派生 | standard |

---

## 9. 来源与验证

### 官方 SDK 状态证据

| 来源 | 链接 |
|------|------|
| OpenAI Node.js SDK (官方) | <https://github.com/openai/openai-node> — 官方 org, TypeScript |
| OpenAI Python SDK (官方) | <https://github.com/openai/openai-python> — 官方 org, Python |
| OpenAI 官方 SDK 页面 | <https://platform.openai.com/docs/libraries> — 列出 Python/JS/Go/Java/C#/Ruby, **无 Rust** |
| Anthropic TS SDK (官方) | <https://github.com/anthropics/anthropic-sdk-typescript> — 官方 org, TypeScript |
| Anthropic Python SDK (官方) | <https://github.com/anthropics/anthropic-sdk-python> — 官方 org, Python |
| Anthropic API SDK 文档 | <https://docs.anthropic.com/en/docs/quickstart> — 列出 Python/TS/C#/Go/Java/PHP/Ruby, **无 Rust** |
| AWS Bedrock Runtime SDK (官方 Rust) | <https://github.com/awslabs/aws-sdk-rust> — awslabs org, 官方 |
| Google Gemini SDK 列表 | <https://ai.google.dev/gemini-api/docs/libraries> — Python/TS/Go/Java/Swift/Kotlin, **无 Rust** |

### Community Crate 来源

| Crate | crates.io | 仓库 |
|-------|-----------|------|
| `async-openai` | <https://crates.io/crates/async-openai> (6,042,936+ downloads, 120 versions) | <https://github.com/64bit/async-openai> |
| `openai-oxide` | <https://crates.io/crates/openai-oxide> (1,004 downloads, 24 versions) | <https://github.com/fortunto2/openai-oxide> |
| `anthropic-sdk-rust` | <https://crates.io/crates/anthropic-sdk-rust> (9,474 downloads) | <https://github.com/dimichgh/anthropic-sdk-rust> |
| `gemini-rust` | <https://crates.io/crates/gemini-rust> | <https://github.com/longportapp/gemini-rust> |
| `aws-sdk-bedrockruntime` | <https://crates.io/crates/aws-sdk-bedrockruntime> (3,512,087+ downloads) | <https://github.com/awslabs/aws-sdk-rust> |
| `tiktoken-rs` | <https://crates.io/crates/tiktoken-rs> | <https://github.com/zurawiki/tiktoken-rs> |

### 供应链风险证据

| 风险 | 证据 |
|------|------|
| `anthropic-sdk-rust` v0.1.0 伪称 anthropics 仓库 | <https://crates.io/api/v1/crates/anthropic-sdk-rust/0.1.0> — `"homepage":"https://github.com/anthropics/anthropic-sdk-rust"`, `"repository":"https://github.com/anthropics/anthropic-sdk-rust"` |
| v0.1.1 修正为真实仓库 | <https://crates.io/api/v1/crates/anthropic-sdk-rust/0.1.1> — `"homepage":"https://github.com/dimichgh/anthropic-sdk-rust"`, `"repository":"https://github.com/dimichgh/anthropic-sdk-rust"` |
| `async-openai` 自述 unofficial | <https://github.com/64bit/async-openai> — README: "async-openai is an unofficial Rust library for OpenAI" |

### 验证命令

```bash
# 确认 crate 存在和基本信息
cargo search async-openai --limit 1
cargo search openai-oxide --limit 1
cargo search anthropic-sdk-rust --limit 1
cargo search gemini-rust --limit 1
cargo search aws-sdk-bedrockruntime --limit 1
cargo search tiktoken-rs --limit 1

# 确认官方 org 下无 Rust SDK
# OpenAI: https://github.com/openai — 无 Rust 仓库
# Anthropic: https://github.com/anthropics — 无 Rust 仓库
# Google: https://github.com/google-gemini — 无 Rust 仓库

# 对比 anthropic-sdk-rust 版本元数据以验证供应链风险
# v0.1.0: cargo info anthropic-sdk-rust 0.1.0 | findstr homepage
# v0.1.1: cargo info anthropic-sdk-rust 0.1.1 | findstr homepage

# 确认现项目 TS 代码
Get-ChildItem -Path src/llm -Recurse -Name
Select-String -Path package.json -Pattern '"openai|@anthropic-ai|@ai-sdk|@google|@aws' | ForEach-Object { $_ }

# 确认 docs 目录
Get-ChildItem -Path docs/contracts -Name
```
