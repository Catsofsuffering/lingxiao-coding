# Rust Core 功能盘点

> 状态：Phase 0 初稿。
>
> 本文用于把现有 TS Core 的核心功能和业务语义盘点为 Rust Core 迁移任务。它不是 UI/API 兼容清单；旧 TUI/Web/Electron/ACP/SSE/REST 只作为历史消费者参考，不约束 Rust Core。

## 迁移边界

### 迁移来源

Rust Core 的功能来源是当前 TS Core 中已经存在的核心语义：

- session lifecycle
- task board
- leader orchestration
- agent runtime
- workflow engine
- permission system
- message bus
- event stream
- persistence schema
- LLM routing/provider abstraction
- tool registry/tool execution
- context/checkpoint/compression
- blackboard/graph
- scheduled tasks
- resource/process lifecycle

### 不作为核心约束

- 旧 `SessionRuntimeState` 的 UI 字段形状。
- 旧 ACP method 名和 SSE event 名。
- 旧 REST route。
- 旧 worker IPC。
- 旧多进程共享 SQLite 运行模式。
- 旧 TUI/Web/Electron 的状态拼装方式。

## 核心域清单

| ID | 功能域 | TS 来源 | Rust 模块 | 迁移优先级 | 初始验收方式 |
|---|---|---|---|---|---|
| F-001 | Session lifecycle | `src/runtime/SessionRuntime.ts`, `src/runtime/SessionManagerRuntime.ts`, `src/core/session/*` | `lingxiao-core::session` | P0 | unit + e2e |
| F-002 | Runtime state/projection | `src/core/SessionRuntimeState.ts`, `src/core/ModeRuntimeProjection.ts`, `src/core/EternalRuntimeProjection.ts` | `lingxiao-core::projection` | P0 | golden snapshot |
| F-003 | State semantics | `src/core/StateSemantics.ts` | `lingxiao-core::state` | P0 | transition table tests |
| F-004 | Task board | `src/core/TaskBoard.ts`, `src/core/TaskDisplayState.ts`, `src/core/TaskPriorityEngine.ts` | `lingxiao-core::task` | P0 | unit + e2e |
| F-005 | Leader orchestration | `src/agents/LeaderAgent.ts`, `src/agents/leader/*`, `src/agents/LeaderTools.ts` | `lingxiao-core::leader` | P1 | e2e |
| F-006 | Agent runtime | `src/agents/BaseAgentRuntime.ts`, `src/agents/AgentRoundExecutor.ts`, `src/agents/runtime/*` | `lingxiao-core::agent` | P1 | e2e |
| F-007 | Agent pool/scheduling | `src/agents/AgentPoolRuntime.ts`, `src/agents/pool/*`, `src/agents/UnifiedScheduler.ts` | `lingxiao-core::agent_pool` | P1 | e2e + stress |
| F-008 | Worker/process lifecycle | `src/core/WorkerProcessRunner.ts`, `src/agents/WorkerProcessEntry.ts`, `src/core/ipc/*` | `lingxiao-core::runtime` | P1 | stress |
| F-009 | Message bus | `src/core/MessageBus.ts`, `src/core/BusMessageTypes.ts` | `lingxiao-core::bus` | P0 | unit |
| F-010 | Event log | `src/core/EventEmitter.ts`, `src/contracts/types/Event.ts` | `lingxiao-core::event_log` | P0 | replay tests |
| F-011 | Persistence schema | `src/core/Database.ts`, `src/core/DatabaseRepositories.ts` | `lingxiao-core::persistence` | P0 | schema diff |
| F-012 | Permission | `src/core/PermissionSystem.ts`, `src/core/PermissionStore.ts`, `src/agents/LeaderPermissionManager.ts` | `lingxiao-core::permission` | P0 | unit + e2e |
| F-013 | LLM abstraction/routing | `src/llm/ContentGenerator.ts`, `src/llm/Client.ts`, `src/llm/ModelGateway.ts`, `src/agents/LlmGuard.ts` | `lingxiao-core::llm` | P1 | provider mock + e2e |
| F-014 | Tool registry | `src/tools/Registry.ts`, `src/tools/Tool.ts`, `src/tools/index.ts` | `lingxiao-core::tool` | P1 | registry tests |
| F-015 | Native tools | `src/tools/implementations/FileRead.ts`, `FileCreate.ts`, `StructuredPatchTool.ts`, `GlobTool.ts`, `CodeSearchTool.ts`, `GitTool.ts`, `Shell.ts` | `lingxiao-core::tool::native` | P1 | tool e2e |
| F-016 | Sidecar tools | browser, OCR, Office, MCP, Node/Python REPL, terminal | `lingxiao-tool-host-protocol` | P2 | sidecar contract |
| F-017 | Workflow | `src/core/workflow/*`, `src/tools/implementations/workflow/*` | `lingxiao-core::workflow` | P1 | workflow e2e |
| F-018 | Context/checkpoint/compression | `src/core/ContextManager.ts`, `src/core/checkpoint/*`, `src/core/compress/*` | `lingxiao-core::context` | P1 | crash recovery |
| F-019 | Blackboard/graph | `src/core/blackboard/*`, `src/agents/LeaderBlackboard.ts`, graph tools | `lingxiao-core::blackboard` | P1 | graph e2e |
| F-020 | Scheduled tasks | `src/core/ScheduledTaskManager.ts`, `src/core/workflow/ScheduleTriggerSync.ts` | `lingxiao-core::schedule` | P2 | timer tests |
| F-021 | Team/collaboration | `src/core/TeamMailbox.ts`, `TeamProtocol.ts`, team tools | `lingxiao-core::team` | P2 | e2e |
| F-022 | Memory/FTS | `src/memory/MemoryFTS.ts`, memory tools | `lingxiao-core::memory` | P2 | search tests |
| F-023 | Tracing/metrics/usage | `src/core/Tracing.ts`, `src/core/Metrics*`, `src/core/TokenTracker.ts`, `src/llm/usageExtractor.ts` | `lingxiao-core::telemetry` | P1 | unit + integration |
| F-024 | Worktree/workspace | `src/core/WorktreeService.ts`, `src/core/Workspace.ts`, `src/core/WorktreeManager.ts` | `lingxiao-core::workspace` | P2 | git e2e |
| F-025 | Runtime guards/resource budget | `src/core/RuntimeGuards.ts`, `ResourceBudgetService.ts`, `Process*`, `PidRegistry.ts` | `lingxiao-core::runtime` | P1 | stress |

## SQLite schema 盘点

Rust Core 不迁移现有本地数据，但 schema 需要与当前 TS 版本保持一致或可机械映射。当前 `src/core/Database.ts` 的 `SCHEMA_VERSION` 是 `15`。

### Core runtime tables

- `sessions`
- `tasks`
- `messages`
- `agent_logs`
- `agent_state`
- `session_state`
- `worktrees`

迁移要求：

- Rust schema 至少覆盖字段级等价。
- `session_state` 和 `agent_state` 属于旧运行时快照，不作为必须导入的数据，但其语义需要转成 Rust canonical state/projection。
- 新运行时不允许多进程写同一 DB。

### Conversation tables

- `leader_conversation`
- `agent_conversation`

迁移要求：

- 保留 role/content/tool_calls/tool_call_id/thinking_blocks 语义。
- compaction 只能产生 projection，不应覆盖原始 conversation 事实。

### Usage/tracing tables

- `token_usage`
- `llm_gateway_requests`
- `traces`
- `execution_trace_events`
- `execution_project_models`

迁移要求：

- 保留 token usage、cache usage、latency、attempts、trace/span 语义。
- Rust Core 统一 provider usage normalization。

### Workflow/schedule tables

- `workflows`
- `workflow_executions`
- `workflow_execution_logs`
- `scheduled_tasks`
- `health_reports`

迁移要求：

- `workflow_executions` 需要补 per-node durable progress，避免 crash 后永久 `running`。
- `scheduled_tasks` 由 Rust scheduler 持有触发权威。

### Blackboard/assumption tables

- `graph_nodes`
- `graph_edges`
- `assumptions`

迁移要求：

- 迁移为 Rust blackboard canonical model。
- graph 状态变化应生成 event。

### Tool/team tables

- `tool_registrations`
- `teams`
- `team_members`
- `team_messages`

迁移要求：

- tool registration 语义迁入 Rust registry 或 sidecar registry。
- team mailbox 作为 P2 核心协作能力迁移。

### Memory FTS tables

来源：`src/memory/MemoryFTS.ts`。

- `memory_entry`
- `memory_fts`

迁移要求：

- P2 阶段迁移。
- 可保持 SQLite FTS5，也可做可机械映射的 Rust memory schema。

## LLM provider 盘点

当前 TS Core 有三条路径：

- 原生 `OpenAIContentGenerator`
- 原生 `AnthropicContentGenerator`
- `VercelAIContentGenerator`，通过 provider registry 支持 `openai`、`anthropic`、`google`、`bedrock`、`custom`

Rust Core 迁移策略：

| Provider 类型 | Rust 实现策略 | 迁移优先级 |
|---|---|---|
| OpenAI | 优先官方 Rust 方案；没有官方方案时用成熟社区方案；必要时 fork/二开；必须支持 streaming/tool/usage/reasoning。 | P1 |
| OpenAI-compatible | 复用 OpenAI adapter，支持 base_url、model、headers、reasoning 参数差异。 | P1 |
| Anthropic | 优先官方 Rust 方案；没有官方方案时用社区方案或二开；必须支持 Messages API、thinking block/signature、tool_use、usage/cache。 | P1 |
| Google/Gemini | 优先官方 Rust/API 方案；否则社区方案；不可行时直接 REST adapter。 | P2 |
| Bedrock | 优先 AWS 官方 Rust SDK。 | P2 |
| Vercel AI SDK path | 不作为 core 依赖；仅迁移期作为 `llm-host` sidecar fallback。 | P2 |
| custom gateway | 作为 OpenAI-compatible 或 explicit sidecar。 | P2 |

Rust Core 必须拥有：

- routing
- budget
- retry/circuit
- token usage normalization
- stream event normalization
- provider key/circuit key
- cancellation

## Tool 盘点

### Rust-native 优先工具

这些工具应优先 Rust-native，因为它们是核心运行能力或可控系统能力：

- `file_read`
- `file_create`
- `structured_patch`
- `list_dir`
- `glob`
- `code_search`
- `ast_query`
- `git`
- `shell` 的 supervision/permission/cancellation 层
- `attempt_completion`
- `send_message`
- `session_info`
- `write_work_note` / `read_work_notes`
- `blackboard` / `write_fact` / `declare_intent` / `read_graph`
- `workflow`
- `tool_preflight`
- `find_tools`

### Sidecar 工具

这些工具不应阻塞 Rust Core 主线，先作为 sidecar：

- `browser_action`
- `browser_visual_verify`
- `ocr`
- `parse_file`
- `node_repl`
- `python_exec`
- Office/PDF/PPTX/DOCX/XLSX generation/edit/inspect/review/render
- `mcp`
- terminal PTY/control
- browser-backed web search fallback

要求：

- sidecar 不直接写核心 DB。
- sidecar 返回 result，由 Rust Core 接受后落事件和状态。
- sidecar 必须支持 timeout/cancellation/resource accounting。

## Golden scenarios

### G-001 Session lifecycle

步骤：

1. 创建 session。
2. 发送用户输入。
3. 查询 snapshot。
4. 取消当前 turn。
5. 删除 session。

验收：

- 每一步都有 ordered event。
- snapshot generation 单调递增。
- 删除后资源释放。

### G-002 Task/agent happy path

步骤：

1. Leader 创建 task。
2. Agent 接收 task。
3. Agent 调用只读工具。
4. Agent 调用写工具。
5. Agent 调用 `attempt_completion`。

验收：

- permission gate 生效。
- task 进入 terminal completed。
- late result 不覆盖 terminal state。

### G-003 Permission round-trip

步骤：

1. strict mode 下调用危险工具。
2. core 发出 permission request。
3. client resolve。
4. agent 恢复执行。

验收：

- 无 active grant 时工具不能执行。
- resolve 后 grant 带 scope/generation。
- mode change 后旧 grant 失效。

### G-004 LLM stream/tool call

步骤：

1. 使用 mock provider 产生 text delta。
2. 产生 thinking block。
3. 产生 tool call delta。
4. 产生 usage。
5. finish。

验收：

- Rust Core 生成标准 stream events。
- tool call argument 可增量组装。
- usage 归一化。

### G-005 Workflow durable execution

步骤：

1. 创建 workflow：data -> condition -> parallel -> tool -> agent。
2. 执行到中间节点。
3. 强制 kill core。
4. restart core。
5. resume workflow。

验收：

- `workflow_executions` 不永久 stuck running。
- per-node progress 可恢复。
- 输出 deterministic。

### G-006 Event replay

步骤：

1. client 订阅 event。
2. 记录 seq=N 后断开。
3. core 继续产生事件。
4. client 用 cursor reconnect。

验收：

- seq>N 全部补齐。
- gap 太大时 core 返回 snapshot-required。
- client 不需要 raw event 自行拼 canonical state。

### G-007 Sidecar tool contract

步骤：

1. 注册 sidecar tool。
2. core 发起 tool call。
3. sidecar 返回 success。
4. 再触发 timeout。
5. 再触发 cancellation。

验收：

- sidecar 不写 DB。
- timeout/cancel 有 canonical event。
- artifact metadata 可追踪。

### G-008 SQLite schema parity

步骤：

1. 从 TS schema 提取表和字段。
2. 从 Rust migration 输出 schema。
3. 生成 schema diff。

验收：

- 所有 P0/P1 表字段一致或存在 documented mechanical mapping。
- Rust schema 有 version。
- Rust Core 独占写入。

## 第一批可执行任务建议

### T-001 提取 TS SQLite schema 快照

目标：

- 生成机器可读 schema snapshot，作为 Rust schema parity 的验收基线。

涉及：

- `src/core/Database.ts`
- `src/memory/MemoryFTS.ts`
- 新增 `docs/contracts/rust-core-schema-inventory.md` 或测试 fixture

验收：

- 列出所有表、字段、索引、`SCHEMA_VERSION`。
- 标注 P0/P1/P2 迁移优先级。
- 不改业务代码。

推荐执行模型：

- `opencode-go/deepseek-v4-flash`

### T-002 提取核心状态机清单

目标：

- 从 `StateSemantics.ts`、`TaskBoard.ts`、session/agent/workflow 状态类型中提取 canonical transition inventory。

涉及：

- `src/core/StateSemantics.ts`
- `src/core/TaskBoard.ts`
- `src/core/workflow/types.ts`
- `src/agents/AgentExecutionResult.ts`
- `src/core/WorkerProcessRunner.ts`

验收：

- 输出状态枚举、合法转移、terminal 规则、缺失 guard。
- 不改业务代码。

推荐执行模型：

- `opencode-go/deepseek-v4-flash`

### T-003 起草 Rust Core protocol v0

目标：

- 定义 command/event/snapshot/error 的 v0 schema，不兼容旧 ACP。

涉及：

- `docs/contracts/rust-core-migration-spec.md`
- 新增 `docs/contracts/rust-core-protocol-v0.md`

验收：

- 覆盖 session/task/agent/workflow/permission/tool/llm/event。
- 明确 seq/generation/request_id/idempotency。
- 明确 sidecar 结果不是事实，必须由 core 接受。

推荐执行模型：

- `opencode-go/deepseek-v4-flash`

## QA 策略

每个执行任务完成后至少经过：

1. 自动检查：format/lint/test 或 docs consistency check。
2. 强模型 QA：GPT-5.5 或 Claude Opus。
3. OpenCode Go 旗舰模型 QA：至少 Qwen/Kimi/GLM/DeepSeek 中两个。

权重：

- GPT-5.5 / Opus：高权重，发现阻断问题时默认要求修复。
- OpenCode Go 旗舰：中权重，用于找遗漏、边界和实现细节。
- dsv4flash 执行结果必须有命令证据或文件 diff 证据。

重试规则：

```text
execute with dsv4flash attempt 1
  -> QA pass: merge/continue
  -> QA fail: dsv4flash attempt 2 with QA findings
  -> QA fail: dsv4flash attempt 3 with narrowed findings
  -> QA fail: reassign same task to GPT-5.5 implementation agent
```
