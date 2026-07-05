# Rust Core Event/Log/Projection 盘点

> Phase 0 交付物 — P0-3。
>
> 本文盘点当前 TS Core 事件系统的来源、分类、边界，并为 Rust Core canonical event 给出分类建议、envelope 定义、snapshot/delta 规则以及 golden scenario。
>
> **Rust Core-first**：旧 EventEmitter/SSE 名称只作为历史 inventory，不约束 Rust Core canonical event 名称。旧 SSE 事件名见 `docs/contracts/sse-events.md`，仅作历史参考。Rust canonical event 由 core domain fact 定义，不由旧 adapter 事件名决定。

---

## 1. 当前 TS Core 事件来源分类

### 1.1 EventEmitter (`src/core/EventEmitter.ts`)

全局 in-process pub/sub。所有核心域模块通过共享 emitter 实例发射事件。`EventMap` 接口定义 169 个带冒号前缀的事件键（命令 `rg -c "^\s+'[a-z]+:[a-z_]+" src/core/EventEmitter.ts` 得 169，包含所有 `'prefix:name': { payload }` 条目）。

按前缀分组（命令 `Select-String "^\s+'([a-z]+):[a-z_]" src/core/EventEmitter.ts | ForEach-Object { $_.Matches.Groups[1].Value } | Group-Object | Sort-Object Count -Descending`）：
`agent`(30)、`leader`(23)、`workflow`(21)、`session`(13)、`worker`(12)、`task`(7)、`context`(5)、`orchestration`(5)、`wiki`(5)、`memory`(4)、`permission`(4)、`blackboard`(3)、`message`(3)、`plan`(3)、`assumption`(3)、`collaboration`(3)、`team`(2)、`token`(2)、`user`(2)、`terminal`(2)、`notification`(2)，以及 `transport/conversation/eternal/git/canvas/tools/langfuse/llm/run/skill/bus/skills/roles/plugin/chat` 各 1。

核心消费者：
- `SseBridge` — 订阅后桥接到 SSE 客户端
- `SessionManagerRuntime` — 订阅后做 projection publish
- `LeaderAgent` / `AgentPoolRuntime` — 订阅任务、worker 生命周期事件
- `MessageBus` — 自身也向 emitter 发射 `message:bus:*` 事件

### 1.2 MessageBus (`src/core/MessageBus.ts`)

Agent 间带优先级（P0–P3）的异步消息传递。它不是一个事实记录系统，而是投递系统。内部也向 EventEmitter 发射诊断事件。

MessageBus 向 emitter 发射的事件（核对 `MessageBus.ts:this.emitter.emit`）：
- `message:bus:priority` — 每条消息的 priority 镜像，供 `SessionManagerRuntime` 监听以触发 runtime state publish
- `message:bus:handler_failed` — handler 异常
- `bus:dead_letter` — 可靠发送最终失败
- `message:bus:stale_p0p1_preserved` — 过时 inbox 清理事件
- `transport:envelope` — transport 层透传

### 1.3 SSE/ACP bridge (`src/web-server/SseBridge.ts`)

订阅 EventEmitter，将事件转换为 ACP `session/update` 格式推送给 SSE 客户端。事件转发分为三类：

- `SESSION_FORWARD_EVENTS` — 46 个事件，直接按 sessionId 转发
- `AGENT_FORWARD_EVENTS` — 14 个事件，经 agentId → sessionId 路由后转发
- `WIKI_GENERATION_EVENTS` — 5 个
- `MEMORY_MAINTENANCE_EVENTS` — 4 个
- 非对称事件 — 需要 throttle / transform / agent-session 学习（如 `leader:status` throttle、`conversation:message_saved` 角色过滤）

数量核对命令：
```
SESSION_FORWARD_EVENTS: lines 44-103 in SseBridge.ts → 46
AGENT_FORWARD_EVENTS:  lines 105-120 in SseBridge.ts → 14
WIKI_GENERATION_EVENTS: 5
MEMORY_MAINTENANCE_EVENTS: 4
```

### 1.4 Runtime projection publish (`src/runtime/SessionManagerRuntime.ts`)

`SessionManager` 监听大量 emitter 事件（`installRuntimeStateSyncBus` 订阅约 30+ 事件名），在变化发生后 debounce 式发射 `session:runtime_state`。这是一个派生 projection，不是 canonical 事实。

### 1.5 Worker IPC event (`src/core/WorkerProcessRunner.ts`)

Worker 进程通过 EventEmitter 发射低层事件。`EventEmitter.ts:EventMap` 中 `worker:*` 定义 12 种（`worker:started`、`progress`、`heartbeat`、`complete`、`failed`、`error`、`exit`、`stdout`、`stderr`、`usage`、`bus_message`、`event`）。

运行时额外发射 `worker:timeout`（`WorkerProcessRunner.ts:558`），该事件未在 `EventMap` 中定义，被 `WorkerEventHandlerBinder.ts:596` 订阅。

共计 13 种运行时 worker 事件。这些事件在 `WorkerEventHandlerBinder` 中被消费，转换为 `agent:*` 高层面事件。

注意：`worker:*` 事件均**不**在 `src/contracts/types/Event.ts` 的 `EventType` union 中（138 个成员均无 `worker:` 前缀），也不在 `EVENT_TYPES` 数组中（132 个成员均无 `worker:` 前缀）。说明 TS Core 自身已视 worker 事件为内部低级事件，非 canonical。

---

## 2. 旧事件分类矩阵

| 类别 | 定义 | 代表事件 | Rust Core 处理方式 |
|------|------|----------|------------------|
| **Durable core fact** | 代表 canonical 状态转移的事实。必须持久化、有序、可回放。 | `session:created`, `session:completed`, `task:created`, `task:completed`, `task:failed`, `permission:request`, `permission:resolved`, `conversation:message_saved`, `agent:spawned`, `agent:completed`, `agent:failed` | 入 `event_log`，成为 canonical event |
| **Projection update** | 从 canonical state 派生的运行时投影，不是新事实。 | `session:runtime_state`, `orchestration:run_state`, `orchestration:node_update`, `leader:status`, `leader:busy`, `agent:interactive_state`, `agent:status` | 由 Rust Core projection 层派生输出，不入 canonical log |
| **Adapter notification** | 给表现层（TUI/Web）的展示提示，无状态语义。 | `leader:text_chunk`, `leader:thinking_chunk`, `leader:tool_call_delta`, `agent:text_chunk`, `agent:thinking_chunk`, `agent:tool_call_delta`, `agent:tool_output`, `agent:shell_state`, `terminal:output`, `terminal:state`, `plan:submitted`, `plan:updated`, `plan:finalized`, `notification:new` | 成为 adapter-layer projection event，不入 canonical log |
| **Telemetry / audit** | 用量、追踪、审计。 | `token:usage`, `langfuse:trace`, `git:activity`, `permission:audit`, `agent:activity` | 独立 telemetry 流，或从 canonical event 派生 |
| **Legacy-only** | 仅适配层或旧 UI 消费者需要，Rust Core 不关心。 | `session:soul_extracted`, `skills:loaded`, `skill:invoked`, `session:focus`, `worker:*` (低层 13 种), `message:bus:*` (4 种), `bus:dead_letter`, `eternal:goal_changed` | 不作为 canonical event；如需等价功能由 Rust Core 另定义 |

### 2.1 口径说明

- **EventMap**（`src/core/EventEmitter.ts`）中带冒号前缀的条目：**169** 个。
- **EventType union**（`src/contracts/types/Event.ts` `export type EventType = | '...'`）：**138** 个成员。
- **EVENT_TYPES 数组**（`src/contracts/types/Event.ts` `export const EVENT_TYPES`）：**132** 个成员。

差异分析：
- EventMap(169) 包含 EventType union(138) 之外的 `worker:*`(12)、`message:bus:*`(4)、`assumption:*`(3)、`collaboration:*`(3)、`roles:changed`、`tools:changed`、`token:usage:persist_failed`、`leader:route`、`leader:tool_output`、`agent:llm_call`、`agent:message`、`agent:start`、`agent:stop`、`agent:thinking`、`agent:tool_failure_loop`、`llm:input_manifest`、`shutdown` 等——这些事件存在于 EventEmitter 接口但不在 canonical EventType 中。
- EVENT_TYPES(132) 比 EventType union(138) 少 6 个：union 中的 `orchestration:status`、`settings:changed`、`session:resync_failed`、`git:activity`、`canvas:version_pushed` 等被 `as const` 数组限制为仅运行时类型安全引用，EventPayloadMap 定义不全。
- 差异是 TS 类型设计的产物，对 Rust Core inventory 无实质影响。Rust Core 重新定义自己的 canonical EventType enum，不直接继承 TS 的 EventType union。

---

## 3. Rust Core Canonical Event 分类建议

Rust Core 产生三类事件/信号：

| 类别 | 持久化 | 可回放 | 用途 |
|------|--------|--------|------|
| **Canonical event** (入 event_log) | 是 | 是 | 状态重建、replay、snapshot/delta |
| **Projection event** (由 projection 层输出) | 否 | 否 | 表现层实时派生状态 |
| **Telemetry event** (独立流) | 按需 | 否 | 用量、追踪、审计 |

### 3.1 Canonical events（入 event_log，仅记录事实层，不含逐 chunk 流式增量）

| 类别 | 典型 event_type | 非 canonical 的相邻事件 |
|------|----------------|--------------------------|
| **session** | `session.created`, `session.input_received`, `session.interrupted`, `session.completed`, `session.deleted`, `session.failed` | `session:runtime_state` (projection) |
| **task** | `task.created`, `task.assigned`, `task.completed`, `task.failed`, `task.cancelled`, `task.generation_bumped` | |
| **agent** | `agent.spawned`, `agent.started`, `agent.completed`, `agent.failed`, `agent.crashed`, `agent.terminated` | `agent:heartbeat` → runtime health signal，不入 canonical log；`agent:text_chunk` / `agent:thinking_chunk` / `agent:tool_call_delta` → 实时 adapter channel |
| **workflow** | `workflow.execution_started`, `workflow.node_started`, `workflow.node_completed`, `workflow.node_failed`, `workflow.execution_paused`, `workflow.execution_resumed`, `workflow.execution_completed`, `workflow.execution_cancelled` | |
| **permission** | `permission.request_created`, `permission.request_resolved`, `permission.mode_changed`, `permission.grant_revoked` | |
| **tool** | `tool.call_initiated`, `tool.call_completed`, `tool.call_failed`, `tool.call_timeout` | `leader:tool_call_delta` / `agent:tool_call_delta` → 实时 adapter channel |
| **llm** | `llm.call_started`, `llm.call_finished`, `llm.usage_reported` | `llm.text_delta` / `llm.thinking_delta` / `llm.tool_call_delta` 等逐 chunk 流式增量 → **不入 canonical log**，仅通过 non-durable realtime channel 推送 |
| **persistence** | `persistence.snapshot_taken`, `persistence.compaction_started`, `persistence.compaction_completed`, `persistence.generation_gap_detected` | |
| **resource** | `resource.sidecar_started`, `resource.sidecar_completed`, `resource.sidecar_failed`, `resource.sidecar_cancelled`, `resource.sidecar_timeout`, `resource.budget_exceeded` | |
| **sidecar** | `sidecar.output_received`, `sidecar.error`, `sidecar.lease_expired` | |

### 3.2 非 canonical 的实时（realtime）channel 事件

以下事件**不入 durable event_log**，由 Rust Core 通过 non-durable realtime channel 直接推送给当前连接的 adapter/client。不进入 replay/compaction 流程：

- `realtime.llm.text_delta` — LLM 文本流式增量（对应旧 `leader:text_chunk`）
- `realtime.llm.thinking_delta` — LLM thinking 增量（对应旧 `leader:thinking_chunk`）
- `realtime.llm.tool_call_delta` — 工具参数流式增量（对应旧 `leader:tool_call_delta` / `agent:tool_call_delta`）
- `realtime.agent.text_delta` — Agent 文本流式增量（对应旧 `agent:text_chunk`）
- `realtime.agent.thinking_delta` — Agent thinking 增量（对应旧 `agent:thinking_chunk`）
- `realtime.agent.tool_output` — shell 实时输出
- `realtime.terminal.output` / `realtime.terminal.state`

注意：`rg "llm\.text_delta|llm\.thinking_delta|llm\.tool_call_delta" src` 在 TS Core 中**无命中**。这些名称是 Rust Core 目标侧可选 realtime 命名建议，不是 TS 已有事件名。

### 3.3 Runtime health 信号

以下信号由 Rust Core runtime 内部使用，不暴露为 event log：

- `agent.heartbeat` — runtime health monitor 的心跳信号，不入 canonical log
- `leader.status_change` — 运行时状态变化通知，由 projection 层派生

### 3.4 Projection events（由 `lingxiao-core::projection` 输出）

- `projection.runtime_state_updated` — 对应旧 `session:runtime_state`
- `projection.orchestration_status` — 对应旧 `orchestration:run_state`

### 3.5 Telemetry events（独立流，按需持久化）

- `telemetry.token_usage` — 对应旧 `token:usage`
- `telemetry.llm_latency`
- `telemetry.tool_execution_time`

---

## 4. 每类 canonical event 必须的 envelope 字段

```rust
// lingxiao-core-protocol crate
struct EventEnvelope {
    event_id: String,          // 全局唯一，如 "evt_{timestamp}_{seq}_{type}"
    session_id: Option<String>, // 所属会话（部分事件可能无 session，如 daemon 级）
    seq: u64,                   // 单调递增序列号，per event log
    generation: u64,            // session 代际——旧 generation 的事件不覆盖 terminal state
    causation_id: Option<String>, // 因果链：request_id / command_id 等
    event_type: EventType,      // 枚举：EventType::SessionCreated, EventType::TaskCompleted ...
    payload: Vec<u8>,           // 序列化 payload（protobuf / JSON / msgpack）
    occurred_at: Timestamp,     // wall clock
}
```

- `event_id` — 全局唯一，用于去重（配合 `seq` 做幂等回放）
- `session_id` — 会话绑定；部分 daemon 级事件（memory maintenance、schedule fire）可无
- `seq` — per-session 单调递增；由单一 writer 分配
- `generation` — session 代际，每次 terminal→active 转换递增；旧 generation 的事件被忽略
- `causation_id` — 命令 request_id 或父事件 event_id，形成因果链
- `event_type` — 强类型 enum
- `payload` — canonical payload，不包含 UI 展示字段
- `occurred_at` — core 记录时间

现有 `src/contracts/types/Event.ts:EventEnvelope` 包含 `schemaVersion/type/eventId/sequence/source/method/sessionId/timestamp/payload`，与 Rust 建议基本匹配，缺 `generation` 和 `causation_id`。

---

## 5. Snapshot + Delta / Replay 规则

### 5.1 基本模型

```
command → validate → state transition → persist event → update projection
```

每个 session 的 canonical state 可随时从 `base_snapshot + events[seq > snapshot.seq]` 重建。

### 5.2 Replay 流程

1. Client 持有 cursor `(session_id, last_known_seq)`。
2. Connect 时发送 cursor。Core 检查 gap：
   - `seq > last_known_seq` 的事件全部存在 → 返回 delta events。
   - 部分事件因 compaction 已删除 → 返回 `snapshot_required` 信号。
3. Client 收到 `snapshot_required` → 请求完整 snapshot → 从 `base_snapshot` 开始订阅后续事件。

### 5.3 Snapshot 触发条件

- 定期 compaction（根据事件数量或时间）
- gap 太大（超过 `MAX_REPLAY_EVENTS` 阈值）
- core restart 后首次 connect
- session generation bump 后

### 5.4 Late generation 规则

- 每个事件携带 `generation`。
- 如果 session 当前 generation > 事件 generation，该事件被忽略（但不影响 log 有序性）。
- 如果 session 已进入 terminal state，任何非 terminal 事件都被拒绝，无论 generation。

### 5.5 Compaction 策略

- Snapshot 后，seq ≤ snapshot.seq 的事件可删除，但必须保留 `snapshot.seq` 指针。
- 不删除原始 conversation 记录（projection 可压缩，原始 fact 不丢）。

---

## 6. 不应成为 Rust Canonical Event 的旧事件

以下旧 SSE/EventEmitter 事件只能作为 **adapter projection** 或 **realtime channel** 输出，不入 Rust Core event_log：

| 旧事件名 | 原因 | Rust Core 输出方式 |
|----------|------|--------------------|
| `leader:text_chunk` / `leader:thinking_chunk` | 流式中间产物，无状态语义 | `realtime.llm.text_delta` / `realtime.llm.thinking_delta` (non-durable realtime channel) |
| `agent:text_chunk` / `agent:thinking_chunk` | 同上，worker 流式中间产物 | `realtime.agent.text_delta` / `realtime.agent.thinking_delta` (non-durable realtime channel) |
| `leader:tool_call_delta` / `agent:tool_call_delta` | 流式参数增量，非事实 | `realtime.llm.tool_call_delta` (non-durable realtime channel)；最终 tool call 以 `tool.call_initiated` 记录 |
| `leader:tool_result` / `agent:tool_result` | 调用结果，状态由 task/agent 状态转移表达 | `tool.call_completed` / `tool.call_failed` (canonical) |
| `leader:phase_change` | 运行时阶段指示，非事实 | projection 派生，不入 log |
| `agent:heartbeat` | 运行时健康信号 | runtime health monitor 内部使用，不入 canonical log |
| `agent:tool_progress` | 工具执行进度提示 | projection，不入 log |
| `agent:tool_output` | shell 实时输出 | `realtime.agent.tool_output` (non-durable realtime channel) |
| `agent:shell_state` | shell 状态切换 | projection，不入 log |
| `terminal:output` / `terminal:state` | 终端实时输出/状态 | `realtime.terminal.output` / `realtime.terminal.state` (non-durable realtime channel) |
| `session:runtime_state` | 派生 projection，非新事实 | 由 Rust `projection` 模块计算输出 |
| `session:soul_extracted` | 纯旧 UI 内部 | Rust Core 不关心 |
| `skills:loaded` | 非事实，仅 UI 通知 | Rust Core 初始化时内部完成 |
| `worker:*` (13 种) | 低层进程事件 | Rust-native agent 取代 worker 进程后无此事件 |
| `message:bus:*` 、`bus:dead_letter` | MessageBus 内部诊断 | 不作为 event，日志级别 |
| `orchestration:event_applied` / `orchestration:event_rejected` | 旧编排内部路由 | Rust workflow 引擎用 canonical `workflow.*` 事件 |
| `notification:new` / `notification:mark_read` | UI 通知，非 domain fact | adapter projection |
| `langfuse:trace` | 纯 telemetry | 独立 telemetry 流 |
| `chat:user_message` | 用户消息 | 由 `session.input_received` (canonical) 替代 |
| `conversation:message_saved` | 既是事实又是 projection | 拆分为 `conversation.message_persisted` (canonical) + 实时消息推送 (adapter) |

---

## 7. MessageBus vs. Event Log 边界

| 维度 | MessageBus | Event Log |
|------|-----------|-----------|
| 定位 | 内部投递系统 | 事实记录系统 |
| 持久化 | 内存队列（可选可靠投递） | SQLite 持久化 |
| 排序 | 优先级 P0–P3，非严格 FIFO | 严格 seq 有序 |
| 回放 | 无 | 支持 cursor replay |
| 消费者 | Agent 进程间通信 | 所有状态重建、projection、adapter |
| 典型用途 | `task_complete` → leader 收件箱；`permission_response` → leader；agent 间消息 | 所有 canonical 状态转移 |
| Snapshot | 无 | 支持 base snapshot + delta |
| 生命周期 | 收件人销毁后消息丢弃 | 事件持久化直到 compaction |

**Rule**: Bus 消息**可以**触发事件 log entry（如 leader 收到 `task_complete` → 生成 `task.completed` event），但 bus 消息本身**不是** event log entry。

---

## 8. Golden Event Scenario

### G-EVT-001 Session lifecycle

1. `session.created` — 用户创建会话
2. `session.input_received` — 用户发送消息
3. `session.interrupted` — 用户中断当前 turn
4. `session.completed` — session 正常完成
5. `session.deleted` — 用户删除会话

验证：事件有序、generation 单调递增、中断后新输入可继续。

### G-EVT-002 Task terminal

1. `task.created` — leader 创建任务
2. `task.assigned` — 任务绑定 agent
3. `task.completed` / `task.failed` — 终态
4. 旧 generation 的 `task.completed` 到达当前 terminal 后——**被忽略**

验证：terminal 后拒绝旧 generation。

### G-EVT-003 Permission request / resolve

1. `permission.request_created` — 危险工具触发 permission gate
2. `permission.request_resolved` (`approved` / `rejected`) — 用户返回决策
3. Agent 恢复或终止执行

验证：resolve 前工具不执行；旧 grant 在 mode change 后失效。

### G-EVT-004 LLM call / tool call

1. `llm.call_started` — LLM 请求发出
2. `realtime.llm.thinking_delta` (×N) — thinking 实时增量（non-durable channel）
3. `realtime.llm.tool_call_delta` (×N) — 参数实时增量（non-durable channel）
4. `realtime.llm.text_delta` (×N) — 文本实时增量（non-durable channel）
5. `tool.call_initiated` — tool 开始执行（入 log）
6. `tool.call_completed` — tool 返回结果（入 log）
7. `llm.call_finished` — 含 usage（入 log）

验证：durable events ordered；tool call 有 permission gate；usage 归一化；delta 事件不入 log。

### G-EVT-005 Workflow pause / resume

1. `workflow.execution_started`
2. `workflow.node_started` → `workflow.node_completed` (×N)
3. `workflow.execution_paused` — 用户暂停
4. `workflow.execution_resumed` — 用户恢复
5. `workflow.execution_completed`

验证：pause 后不产生新 node event；resume 后从暂停点继续。

### G-EVT-006 Sidecar timeout / cancel

1. `resource.sidecar_started` — sidecar 进程启动
2. `resource.sidecar_cancelled` — 用户取消
3. 或 `resource.sidecar_timeout` — 超时自动终止

验证：sidecar 不写 core DB；timeout/cancel 有 canonical event。

### G-EVT-007 Reconnect replay

1. Client 持有 `last_known_seq=5` 后断开
2. Core 继续产生事件 seq=6,7,8
3. Client reconnect 带 `cursor=(session_id, seq=5)`
4. Core 返回 seq=6,7,8 的 delta events
5. Client 应用后状态一致

验证：gap 可接受时 delta 补齐；gap 太大返回 `snapshot_required`。

### G-EVT-008 Late generation ignored

1. Session 当前 `generation=2`
2. Worker 返回 `generation=1` 的 `task.completed`
3. Core 记录事件到 log 但**不应用**到 canonical state
4. State 保持当前 generation=2 的 task 状态

验证：事件持久化到 log（可审计），但 state transition 被拒绝。

---

## 9. Rust 模块归属

| 模块 | 职责 | 关键类型 |
|------|------|----------|
| `lingxiao-core::event_log` | 有序 event log 写入、读取、compaction；snapshot 指针管理 | `EventLog`, `EventEntry`, `SnapshotPointer`, `Cursor` |
| `lingxiao-core::projection` | 从 event log + snapshot 派生的运行时状态；adapter projection 输出；realtime channel 管理 | `RuntimeProjection`, `SessionProjector`, `AdapterEventBus`, `RealtimeChannel` |
| `lingxiao-core::bus` | Agent/模块间内部消息投递（非持久化）；优先级队列、backpressure | `MessageBus`, `Inbox`, `PriorityLevel` |
| `lingxiao-core::protocol` | Canonical event/command/snapshot/error schema 定义 | `EventEnvelope`, `Command`, `Snapshot`, `EventType` enum |

---

## 10. 验收检查记录

### 10.1 核对命令与结果

```powershell
# EventMap 数量（EventEmitter.ts 中所有 'prefix:name' 接口条目）
rg -c "^\s+'[a-z]+:[a-z_]+" src/core/EventEmitter.ts
# → 169

# EventType union 成员数（Event.ts 中 type EventType = | '...'）
rg -c "^\s+\| '[a-z]+:[a-z_]+" src/contracts/types/Event.ts
# → 138

# EVENT_TYPES 数组成员数（Event.ts 中 as const 数组）
# 手动提取数组正文得 132

# SESSION_FORWARD_EVENTS
# 直接数 SseBridge.ts 中 SESSION_FORWARD_EVENTS 数组 → 46

# AGENT_FORWARD_EVENTS
# 直接数 SseBridge.ts 中 AGENT_FORWARD_EVENTS 数组 → 14

# MessageBus emitter 事件
rg "this\.emitter\.emit\(" src/core/MessageBus.ts
# → 5 处：bus:dead_letter, transport:envelope, message:bus:priority,
#   message:bus:handler_failed, message:bus:stale_p0p1_preserved

# Worker 运行时 emit
rg "\.emit\('worker:" src/core/WorkerProcessRunner.ts
# → 13 种，含 worker:timeout (line 558)

# 检查 llm.*_delta 是否在 TS Core 中存在
rg "llm\.text_delta|llm\.thinking_delta|llm\.tool_call_delta" src
# → 无命中（这些是 Rust Core 目标侧可选命名，非 TS 既有事件）

# Delta/heartbeat 在 EventMap 中的存在性
rg "'agent:heartbeat|'leader:text_chunk|'agent:text_chunk|'agent:thinking_chunk|'leader:thinking_chunk|'leader:tool_call_delta|'agent:tool_call_delta" src/core/EventEmitter.ts
# → 全部存在（说明 TS Core 当前视为一等事件，但 Rust Core 不作为 canonical）
```

### 10.2 关键发现

- `worker:timeout` 在 `WorkerProcessRunner.ts:558` 被 emit，未在 `EventEmitter.ts:EventMap` 中定义类型签名，在 `WorkerEventHandlerBinder.ts:596` 被 subscribe。低层 worker 事件共 13 种运行时类型。
- Delta/heartbeat 事件（`leader:text_chunk`、`agent:heartbeat`、`tool_call_delta` 等）当前同时存在于 `EventMap`、`EventType` union 和 `EVENT_TYPES` 数组，但 Rust Core 应重新分类：这些是 non-durable realtime 信号，不入 canonical event_log。
- `EventType` union (138) 与 `EVENT_TYPES` array (132) 的差异是 TS 类型系统产物，对 Rust Core 无影响。

---

## 11. 仍有不确定的地方

1. **LLM stream event 的 event log 粒度**：本文建议逐 chunk stream delta 不入 durable event_log，仅通过 non-durable realtime channel 推送。但如果 replay 时新 client 需要看到中间 thinking/tool_call 过程（如 audit 场景），是否需要在 call 级别记录更细的中间 checkpoint？建议初始实现只记录 `call_started` / `call_finished` / `usage_reported`，后续按需补充。

2. **generation 精确定义**：TS Core 的 `generation` 在 session 和 task 两个层面都有出现但定义模糊。建议 Rust Core 明确定义：
   - `session.generation`：session 每次从 terminal 复活（如 interrupted → 重新输入）时递增。
   - `task.generation`：task 每次被重新 dispatch（respawning、recovery）时递增。
   - 事件中的 `generation` 指 session generation。

3. **`conversation:message_saved` 的 canonical 表示**：该事件既是事实 (message 已持久化) 又是 projection (发给前端显示)。建议拆分为 `conversation.message_persisted` (canonical) + realtime 消息推送 (adapter)。

4. **`agent:heartbeat` 的处理**：本文建议不入 canonical log，只作为 runtime health monitor 的信号。但如果 Rust Core 改为 supervised async task 模型（无独立 worker 进程），心跳语义是否会变化？建议 Phase 1 确定 agent 执行模型后重新评估 heartbeat 必要性。

5. **`realtime.*` event 命名空间**：本文使用 `realtime.llm.*` / `realtime.agent.*` 作为 non-durable channel 事件建议名。这些名称不是约束性设计——Phase 2 实施时可另定，只需明确定义哪些事件不入 event_log。
