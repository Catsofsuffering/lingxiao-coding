# Rust Core 迁移规格

> 范围：凌霄核心功能从 TypeScript/Node.js 迁移到独立 headless Rust Core。
>
> 本文是迁移执行规格，不是新产品设计稿。现有 TS Core 是功能和业务语义来源，Rust Core 是最终权威实现。TUI/Web/Electron 等表现层后续适配 Rust Core，不反向约束内核。

## 目标

### 核心目标

将凌霄现有核心功能完整迁移到一个独立、可复用、headless 的 Rust Core 中。Rust Core 应能脱离现有 TUI/Web/Electron 单独运行，并通过稳定协议/API 暴露能力。

### 非目标

- 不重新设计一个脱离凌霄现有语义的新内核。
- 不为了兼容旧 TUI/Web/Electron 的事件形状、REST 形状、UI projection 字段而扭曲 Rust Core。
- 不把 Rust 作为 Node.js 的 FFI/N-API helper。
- 不以局部性能优化替代核心迁移。
- 不要求迁移期 Rust Core 与旧 TUI/Web/Electron 同时完整兼容使用。

### 允许 breaking change

本迁移允许 breaking change。旧表现层、旧 REST/SSE/ACP 事件、旧 worker IPC、旧 UI projection 均不能作为 Rust Core 的长期约束。迁移期如需兼容，应放在 adapter 层，而不是污染 core domain model。

## 设计原则

1. **Core-first**：Rust Core 定义 canonical domain model、状态机、命令、事件、错误码、权限和资源模型。
2. **功能迁移，不另写**：迁移来源是当前 TS Core 的真实核心功能和业务语义，不凭空发明另一套 agent/workflow/session 模型。
3. **表现层适配内核**：TUI/Web/Electron 后续消费 Rust Core protocol；内核不迎合旧表现层。
4. **单一权威**：Rust Core 是 session/task/agent/workflow/permission/tool/persistence 的唯一状态权威。
5. **单一 writer**：Rust Core 是核心 SQLite 的唯一 writer；sidecar/tool-host 不直接打开核心 DB。
6. **有序事件**：核心事件必须 durable、ordered、replayable，替代 in-process best-effort `EventEmitter`。
7. **迁移时修结构性问题**：多核心实例、非原子双写、状态 projection 不一致、worker 重进程重加载、SQLite 多 writer 等问题在迁移中一并消除。

## 当前核心功能清单

| 功能域 | TS 来源 | Rust 迁移目标 | 迁移要求 |
|---|---|---|---|
| Session lifecycle | `src/runtime/SessionRuntime.ts`, `src/runtime/SessionManagerRuntime.ts` | `lingxiao-core::session` | 保留 session 创建、恢复、输入、取消、销毁、active/focused 语义；用 Rust 状态机统一管理生命周期。 |
| Runtime projection | `SessionRuntimeState`, `ModeRuntimeProjection`, `EternalRuntimeProjection` | `lingxiao-core::projection` | projection 从 canonical state/event log 派生；不照搬旧 UI 字段作为核心状态。 |
| Task board | `TaskBoard`, `StateSemantics` | `lingxiao-core::task` | 保留任务状态、调度、依赖、终态语义；所有转移显式校验。 |
| Leader orchestration | `src/agents/LeaderAgent.ts` | `lingxiao-core::leader` | 保留 leader think/act/observe、任务拆解、工具调用、权限门控、完成判断。 |
| Agent runtime | `BaseAgentRuntime`, `AgentRoundExecutor`, `AgentPoolRuntime` | `lingxiao-core::agent` | Rust-native agent 默认 in-process supervised async task；保留角色、轮次、上下文、故障恢复语义。 |
| Workflow engine | `src/core/workflow/WorkflowEngine.ts` | `lingxiao-core::workflow` | 保留 DAG、node executor、pause/resume/cancel/retry 语义；补 durable recovery。 |
| Message bus | `src/core/MessageBus.ts` | `lingxiao-core::bus` | 保留优先级、投递、ack/dead-letter 语义；使用有界 channel 和 backpressure。 |
| Event system | `src/core/EventEmitter.ts`, `SseBridge` 相关事件 | `lingxiao-core::event_log` | 不迁移为 pub/sub；改为 ordered event log + cursor replay + snapshot/delta。 |
| Persistence | `src/core/Database.ts` | `lingxiao-core::persistence` | Rust 成为唯一 SQLite writer；Rust schema 与当前 TS 版本保持一致或可机械映射；不要求迁移现有本地 SQLite 数据。 |
| Permission system | `PermissionSystem`, `PermissionStore`, `LeaderPermissionManager` | `lingxiao-core::permission` | core 拥有 mode、grant、request、resolve、audit；UI 只返回用户选择。 |
| Tool registry | `src/tools/**` | `lingxiao-core::tool` | core 拥有 registry、schema、permission、timeout、audit；重型能力可走 sidecar。 |
| LLM routing | `src/llm/**`, `ContentGenerator`, `LlmGuard`, `ModelGateway` | `lingxiao-core::llm` | core 拥有 routing、budget、retry、usage、stream normalization；provider 实现优先使用官方 Rust 方案，没有官方方案时使用成熟社区方案，社区方案不可行时允许 fork/二开。 |
| Context/checkpoint | `ContextManager`, checkpoint/compression 相关模块 | `lingxiao-core::context` | 保留上下文压缩、恢复、摘要语义；消除 write-behind crash window。 |
| Scheduling | `ScheduledTaskManager` | `lingxiao-core::schedule` | 保留 cron/manual fire/system task 语义；调度状态持久化。 |
| Process/resource lifecycle | `WorkerProcessRunner`, `ResourceBudgetService`, `RuntimeGuards` | `lingxiao-core::runtime` | 将资源预算、取消、超时、子进程清理、shutdown 顺序内核化。 |
| Blackboard/shared state | blackboard 相关模块 | `lingxiao-core::blackboard` | 保留共享状态语义；不再通过多进程共享 DB 写入实现。 |

## Rust 模块划分

```text
crates/
  lingxiao-core/
    session/
    task/
    leader/
    agent/
    workflow/
    bus/
    event_log/
    persistence/
    permission/
    tool/
    llm/
    context/
    schedule/
    runtime/
    projection/
    blackboard/

  lingxiao-core-protocol/
    command schema
    event schema
    snapshot schema
    error schema
    tool-host schema
    llm-host schema

  lingxiao-core-daemon/
    stdio transport
    local socket transport
    websocket transport
    lifecycle/shutdown

  lingxiao-tool-host-protocol/
    external tool declarations
    tool call envelope
    cancellation
    artifact metadata

  lingxiao-llm-host-protocol/
    provider declarations
    stream event normalization
    token usage
```

## 协议原则

Rust Core protocol 从核心能力出发定义，不以旧 ACP/REST/SSE 形状为约束。

### Command

命令是用户、client、adapter、sidecar 对 core 的请求。每个命令必须有：

- `request_id`
- `method`
- `params`
- `actor`
- `session_id`，如适用
- idempotency key，如命令可重试

### Event

事件是 core 已接受状态变化后的事实。每个事件必须有：

- `event_id`
- `session_id`，如适用
- `seq`
- `generation`
- `event_type`
- `payload`
- `occurred_at`

事件可回放，不能只作为内存广播。

### Snapshot

snapshot 是 core 对 canonical state 的稳定投影。表现层可以请求不同 projection，但 projection 不能反向成为核心状态。

## 状态一致性模型

Rust Core 采用单写者模型：

```text
command
  -> validate
  -> state transition
  -> persist event/canonical state
  -> update projection
  -> publish event
```

要求：

- 每个 session 有单一 dispatcher/actor 串行处理命令。
- 状态转移使用 Rust enum/typed transition 表达。
- terminal state 不被旧 generation 的事件覆盖。
- worker/agent/tool 结果必须带 lease/generation。
- 客户端用 snapshot + seq delta 同步。
- reconnect 通过 cursor replay 恢复；gap 太大则重新 snapshot。

## Worker 和 sidecar 策略

### Rust-native agent

最终默认模型：

- agent 是 Rust Core 内的 supervised async task。
- 共享 core LLM client、tool registry、permission engine、event log、DB owner。
- 不为每个 agent 启动完整 Node.js worker。

### Sidecar

sidecar 仅用于确实不能或不应该直接 Rust-native 的能力：

- Playwright/browser automation
- OCR/Tesseract
- Sharp/image processing
- Office/PDF 文档处理
- MCP server host
- Node/Python REPL
- terminal PTY
- legacy TS worker，迁移期可临时保留

sidecar 规则：

- 通过 tool-host/llm-host protocol 通信。
- 不直接打开核心 SQLite。
- 不直接修改 canonical state。
- 只能返回 result/event suggestion，由 Rust Core 接受后变成事实。
- 必须支持 timeout、cancellation、resource accounting。

## LLM provider 策略

Rust Core 不默认依赖 TS/Vercel AI SDK 路径。provider 迁移优先级如下：

1. 优先使用 provider 官方 Rust SDK 或官方 Rust API 方案。
2. 官方没有 Rust 方案时，使用成熟社区 Rust crate。
3. 社区方案不能满足 LingXiao 的 streaming、tool call、thinking、usage、retry/circuit 需求时，允许 fork/二开。
4. 仍不可行时，才将该 provider 暂时放入 `llm-host` sidecar。

无论底层 provider 如何实现，routing、budget、permission、usage、stream normalization、retry/circuit 都归 Rust Core 管。

## SQLite schema 策略

Rust Core 不要求迁移用户现有本地 SQLite 数据。旧库只作为当前 TS Core 行为和 schema 的参考。

要求：

- Rust Core 的 SQLite schema 与当前 TS 版本保持一致，或有明确、可机械映射的等价 schema。
- 不为导入旧数据扭曲 Rust Core 的状态模型。
- 不把旧 `agent_state`、`session_state` 等运行时残留状态当作必须迁移数据。
- 新 Rust Core 运行时由 Rust 独占写入 SQLite。

## 迁移阶段

### Phase 0：Core Function Inventory

目标：锁定迁移范围和行为语义。

交付：

- 核心功能清单。
- TS 模块到 Rust 模块的映射。
- 每个功能的行为语义说明。
- breaking change 清单。
- sidecar 保留清单。
- golden scenario 清单。

验收：

- 所有核心功能都有 Rust 归属模块。
- 每个功能有至少一个验收场景。
- 表现层兼容项被标记为 adapter concern，而不是 core concern。

### Phase 1：Rust Core Skeleton

目标：直接搭建 Rust Core 主体，不先做旧 UI 兼容 facade。

交付：

- Rust workspace。
- core daemon。
- protocol schema。
- command router。
- ordered event log。
- SQLite owner。
- basic snapshot/delta。
- shutdown/recovery 基础能力。

验收：

- `lingxiao-core-daemon` 可独立启动。
- 可通过 minimal dev client 创建 session、查询 snapshot、订阅 event。
- event 可按 seq replay。
- DB 只有 core 一个 writer。

### Phase 2：Core Domain Migration

目标：迁移核心 domain，不接表现层。

交付：

- session 状态机。
- task board。
- agent/leader 基础状态。
- workflow 状态。
- permission 状态。
- tool/llm 抽象。
- runtime projection。

验收：

- 核心状态转移全部通过 Rust typed transition。
- 非法转移被拒绝。
- snapshot 与 event log 可互相校验。
- session/task/agent/workflow/permission golden scenario 通过。

### Phase 3：Agent Execution Migration

目标：迁移 leader loop、agent runtime、tool dispatch、LLM stream。

交付：

- leader think/act/observe。
- Rust-native agent async task。
- permission gate。
- tool registry。
- LLM routing 和 stream normalization。
- sidecar protocol。

验收：

- 用户输入到 agent 完成的核心链路可跑通。
- tool call 经过 permission、timeout、audit。
- LLM stream 产生有序 core event。
- agent crash/cancel/timeout 可恢复或终止。

### Phase 4：Workflow/Persistence/Recovery Hardening

目标：补齐长期运行和故障恢复。

交付：

- workflow durable execution。
- per-node progress。
- crash recovery。
- context write-through。
- event compaction。
- resource budget。
- soak/stress 测试。

验收：

- workflow kill/restart 后可恢复。
- context 不因 crash 丢失最近消息。
- 24 小时 soak 无 orphan sidecar、无 stuck running、无状态漂移。
- 多 agent 场景资源消耗低于 TS baseline。

### Phase 5：Adapter Rewrite

目标：表现层适配 Rust Core。

交付：

- TS client 或其他语言 client。
- 新 TUI/Web/Electron adapter。
- 旧 ACP/REST/SSE compatibility adapter，如仍需要。

验收：

- 表现层只消费 Rust Core protocol。
- 无表现层直接依赖 core 内部实现。
- 兼容层可删除，不影响 core。

## Breaking Change 清单

- Core protocol 不保证兼容旧 ACP method/event。
- `SessionRuntimeState` 不保证字段兼容旧 UI。
- 旧 REST route 不作为 Rust Core API 设计来源。
- 旧 SSE event 名称不作为 Rust Core event 名称来源。
- 旧 worker IPC 不作为长期协议。
- 旧 TS worker 不允许直接写 DB。
- 旧多进程共享 SQLite 模式废弃。
- 旧 in-process `EventEmitter` 模式废弃。
- 旧 UI 根据多个 raw event 自行拼状态的模式废弃。

## 验收场景清单

### Session

- 创建 session。
- 加载 session。
- 发送用户输入。
- 中断当前 turn。
- 删除 session。
- crash 后恢复 session。

### Task/Agent

- leader 创建任务。
- agent 接收任务。
- agent 调用工具。
- agent 完成任务。
- agent 失败并触发恢复策略。
- late result 不覆盖新 generation。

### Workflow

- 串行节点执行。
- 并行节点执行。
- 条件节点执行。
- pause/resume。
- cancel。
- crash 后继续。

### Permission

- strict 模式阻断危险工具。
- dev/networked/yolo 模式按规则放行。
- permission request 发出、resolve、恢复执行。
- mode change 废弃旧 grant。

### Tool/LLM

- file read/write。
- structured patch。
- shell command with timeout。
- browser sidecar call。
- LLM stream text/thinking/tool call。
- token usage accounting。

### Event/Projection

- snapshot at seq=N。
- replay seq>N。
- reconnect 后补齐事件。
- gap 太大后强制 snapshot。
- UI projection 不参与 core state mutation。

## 执行建议

第一批任务应是 `Phase 0`，不要直接开始写 Rust 业务逻辑：

1. 做完整 Core Function Inventory。
2. 为每个核心功能写 golden scenario。
3. 明确 Rust 模块归属。
4. 标出 breaking change 和 sidecar 保留项。
5. 再开始 `Phase 1` 的 Rust Core skeleton。

这样可以保证 Rust Core 是对凌霄核心功能的迁移，而不是另写一个看起来相似的新系统。
