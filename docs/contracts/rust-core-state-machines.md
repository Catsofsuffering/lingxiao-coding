# Rust Core 状态机和 Transition 盘点

> 状态：P0-4 初稿。
>
> 本文盘点当前 TS Core 所有状态域的状态值、类型来源、当前 transition guard，按 Rust Core 目标提出合法 transition 表和模块归属。不修改业务代码。

## 术语

- **canonical state**: Rust Core 中存储的权威状态，不等同于 UI projection。
- **transition guard**: 显式校验状态转移合法性的代码（`assertCore*Transition`、`CORE_*_TRANSITIONS` 表、`isTerminalStatus` 拦截等）。
- **missing guard**: 当前 TS 中没有显式 transition 表或 assert 拦截，依赖运行时自然路径。
- **generation**: 单调递增的计数器，用于防止 late result 覆盖新状态。
- **terminal state**: 不可被旧事件覆盖的死状态。

---

## 1. Session 状态域

### TS 类型来源

| 项 | 值 |
|---|---|
| 定义位置 | `src/contracts/types/Status.ts:42-57` |
| 类型名 | `SessionPhase` |
| 常量值 | `src/contracts/constants/statusValues.ts:5` |
| active 判断 | `isActiveSessionPhase()` at `src/contracts/types/Status.ts:97-99` |
| DB status 字段 | `SessionRecord.status` in `src/types/canonical.ts` |
| SessionManager 运行时状态 | `SessionState.status: 'active' \| 'completed' \| 'failed' \| 'interrupted'` at `src/runtime/SessionManagerRuntime.ts:93` |

### SessionPhase 类型

```typescript
export type SessionPhase =
  | 'idle'
  | 'preparing'
  | 'model_requesting'
  | 'streaming'
  | 'thinking'
  | 'tool_executing'
  | 'observing'
  | 'waiting_for_permission'
  | 'waiting_for_user'
  | 'retrying'
  | 'compacting'
  | 'cancelling'
  | 'done'
  | 'error'
  | 'interrupted';
```

### SessionManager 持久化状态

```typescript
SessionState.status: 'active' | 'completed' | 'failed' | 'interrupted';
```

### 当前 guard

- `SessionPhase` **没有**显式 transition 表或 assert 调用。`isActiveSessionPhase()` 只区分 active/non-active，不校验具体转移合法性。
- SessionManager 中的 `SessionState.status` 也没有 transition guard。
- 状态变化由 `emit('session:completed')`, `emit('session:failed')`, `emit('session:interrupted')` 等事件驱动，靠自然路径流转。

**结论：Session 状态域当前缺失 guard。**

### Rust Core 建议 transition 表

#### SessionPhase（Leader 运行时阶段）

| from | to | 触发事件 | guard |
|---|---|---|---|
| idle | preparing | 用户输入到达 | 必须存在 active session |
| preparing | model_requesting | context 准备完成 | 无 |
| model_requesting | streaming | LLM 返回首 token | 无 |
| model_requesting | thinking | LLM 返回 thinking block | 无 |
| streaming | thinking | LLM 切换 thinking 模式 | 无 |
| thinking | streaming | thinking 结束 | 无 |
| streaming | tool_executing | LLM 返回 tool_call | 无 |
| tool_executing | observing | tool result 返回 | 有 (lastGeneration 校验) |
| observing | preparing | 继续下一轮 | 有 (shouldContinue) |
| observing | done | attempt_completion 成功 | 有 (Leader 判断) |
| any | waiting_for_permission | permission 门触发 | 有 (permission guard) |
| any | waiting_for_user | ask_user 门触发 | 有 |
| any | cancelling | 中断命令 | 有 (cancel 权限) |
| any | interrupted | 中断完成 | 必须从 cancelling 来 |
| any | error | 未处理异常 | 有 (catch) |
| idle | done | 无任务 | 直接 done |
| * | retrying | LLM 空响应重试 | 有 (retry 策略) |
| * | compacting | context 超限 | 有 (token budget) |

#### SessionManager 持久状态

| from | to | guard |
|---|---|---|
| active | completed | 所有任务终态 + Leader stopped |
| active | failed | 致命错误 |
| active | interrupted | 用户中断 |
| interrupted | active | 恢复/resume |
| completed/failed/interrupted | (none) | terminal, 不可转移 |

> 说明：`SessionPhase` 是 Leader 执行阶段的细粒度反映，不是 canonical core state。Rust Core 的 canonical session state 应简化为离散枚举（如 `Created`, `Active`, `Paused`, `Completed`, `Failed`），`SessionPhase` 退化为 projection。

### Terminal state 规则

- `done`, `error`, `interrupted` 视为 SessionPhase 的终端状态。
- `completed`, `failed`, `interrupted` 是 SessionManager 持久状态的 terminal。

### Generation 要求

- 不适用（session 没有 generation 概念）。

### Rust 模块归属

- `lingxiao-core::session` — canonical session state machine
- `lingxiao-core::projection` — SessionPhase 作为 projection 派生

### Golden test 建议

- **Happy path**: idle → preparing → ... → done
- **Illegal transition**: done → preparing (must reject)
- **Cancel flow**: running → cancelling → interrupted

---

## 2. Task 状态域（TaskBoard）

### TS 类型来源

| 项 | 值 |
|---|---|
| canonical 定义 | `src/core/StateSemantics.ts:123-128` 及 `src/core/TaskBoard.ts:21-23` |
| 类型名 | `CoreTaskStatus` / `TaskStatus` |
| exitReason | `CoreTaskExitReason` at `StateSemantics.ts:124` |
| Normalized 展示 | `NormalizedTaskStatus` at `StateSemantics.ts:22` |
| 旧 TaskStatus | `src/contracts/types/Status.ts:59-69` — 10 个值，含 UI 展示态 |

### CoreTaskStatus（canonical）

```typescript
export type CoreTaskStatus = 'dispatchable' | 'running' | 'terminal';
export type CoreTaskExitReason = 'completed' | 'failed' | 'cancelled' | 'timeout';
```

### 当前 guard

**有显式 guard** — `CORE_TASK_TRANSITIONS` 表 + `assertCoreTaskTransition()` + `assertCoreTaskExitReason()`:

```
CORE_TASK_TRANSITIONS at src/core/StateSemantics.ts:144-148:
  dispatchable: ['running', 'terminal']
  running: ['terminal', 'dispatchable']
  terminal: []
```

调用点：
- `TaskBoard.updateTaskStatus()` — `assertCoreTaskTransition` + `assertCoreTaskExitReason`
- `TaskBoard.assignTask()` — `assertCoreTaskTransition(task.status, 'running')`
- `TaskBoard.cancelTask()` — `assertCoreTaskTransition(task.status, 'terminal')`
- `TaskBoard.reopenTask()` — **显式 reset command**（直接写 `task.status = 'dispatchable'`），是设计意图绕过普通 transition 表，只对 non-completed terminal 生效

### Transition 表

| from | to | exitReason | 触发方法 | guard |
|---|---|---|---|---|
| dispatchable | running | — | `assignTask()` | ✅ `assertCoreTaskTransition` |
| dispatchable | terminal | cancelled | `cancelTask()` | ✅ |
| dispatchable | terminal | failed | `failTask()` / `updateTaskStatus` | ✅ |
| dispatchable | terminal | completed | `completeTask()` | ✅ |
| running | terminal | completed | `completeTask()` | ✅ |
| running | terminal | failed | `failTask()` | ✅ |
| running | terminal | cancelled | `cancelTask()` | ✅ |
| running | dispatchable | — | `blockTask()` / `prepareTaskForRedispatch()` | ✅ (runtime→dispatchable via computeDerivedTaskStatus, 但 assignTask 时 validate) |
| terminal | dispatchable | — | `reopenTask()` | ⚠️ 显式 reset，不走普通 transition 表 |
| terminal | (none) | — | — | ✅ terminal → 任何状态被 `CORE_TASK_TRANSITIONS[terminal] = []` 拦截 |

> `reopenTask()` 不进入 `CORE_TASK_TRANSITIONS` 是因为 terminal 按设计是死状态，但 retry 场景需要重置。`StateSemantics.ts:142` 注释明确："terminal 是死状态；重开任务必须走 TaskBoard.reopenTask 的显式重置逻辑"。**这是设计意图，不是 bug**。Rust Core 应实现 canonical `task.reopen` command（带 generation bump 和 exitReason 清空），不走普通 transition guard。

### Terminal state 规则

- `terminal` 是唯一 terminal canon state。
- exitReason 在 `terminal` 状态时必须设置，非 `terminal` 时不能设置（`assertCoreTaskExitReason` 校验）。
- 内存中 terminal task 有 30 分钟 TTL 后 evict（`TERMINAL_TASK_TTL_MS`），DB 永久保留。
- terminal 状态不可被旧 generation 的完成事件覆盖。

### Generation 要求

- `task.runGeneration` — `bumpRunGeneration()` 在 `assignTask` 和 `prepareTaskForRedispatch` 时递增。
- Agent 完成回执必须携带 `taskRunGeneration`。旧 generation 的完成/失败事件必须被 Rust Core 丢弃。

### Rust 模块归属

- `lingxiao-core::task` — TaskBoard canonical state machine
- `lingxiao-core::state` — task transition validation table

### Golden test 建议

- **Happy path**: dispatchable → running → terminal(completed)
- **Redispatch**: running → dispatchable (blockTask) → running → terminal(failed)
- **Illegal**: terminal → running (must reject)
- **Late result**: old generation completion on running task → must check generation

---

## 3. Agent 状态域（AgentPool / AgentHandle）

### TS 类型来源

| 项 | 值 |
|---|---|
| canonical 定义 | `src/core/StateSemantics.ts:68` |
| 类型名 | `CoreAgentStatus` |
| AgentHandle 使用 | `src/agents/AgentPoolRuntime.ts:240` |

```typescript
export type CoreAgentStatus = 'starting' | 'running' | 'stopped';
```

AgentHandle 同时记录 `exitReason`:
```typescript
exitReason?: 'completed' | 'failed' | 'timeout' | 'crashed' | 'terminated';
```

### 当前 guard

**有显式 guard** — `CORE_AGENT_TRANSITIONS` 表 + `assertCoreAgentTransition()`:

```
CORE_AGENT_TRANSITIONS at src/core/StateSemantics.ts:134-138:
  starting: ['running', 'stopped']
  running: ['stopped']
  stopped: ['starting']
```

调用点：
- `AgentPool.transitionAgentStatus()` — `assertCoreAgentTransition`
- `LeaderAgent.ts:217` — 直接调用 `assertCoreAgentTransition(handle.status, 'stopped')`

### Transition 表

| from | to | 场景 | guard |
|---|---|---|---|
| starting | running | worker 启动完成 | ✅ |
| starting | stopped | 启动失败 | ✅ |
| running | stopped | 完成/失败/超时/崩溃/终止 | ✅ |
| stopped | starting | respawn | ✅ |

> `stopped` 是一个"终止容器": 实际原因由 `exitReason` 区分。Rust Core 应考虑将 `exitReason` 纳入状态机，用类似 `Stopped(ExitReason)` 的方式，而不是独立字段。

### NormalizedAgentStatus（UI projection）

```typescript
export type NormalizedAgentStatus = 'idle' | 'running' | 'recovering' | 'completed' | 'failed' | 'interrupted';
```

`normalizeAgentRuntimeStatus()` 合并 `status + exitReason` 产生归一化投影。

### AgentRunStatus vs CoreAgentStatus vs NormalizedAgentStatus

`AgentRunStatus`（`src/contracts/types/Status.ts:9-20`）是另一套 11 值状态类型，**不是 canonical core 状态**：

```typescript
export type AgentRunStatus =
  | 'spawning' | 'starting' | 'running' | 'completed' | 'failed'
  | 'interrupted' | 'stopped' | 'crashed' | 'recovering' | 'idle' | 'unknown';
```

三者的关系和用途：

| 类型 | 值数 | 角色 | Rust Core 定位 |
|---|---|---|---|
| `CoreAgentStatus` | 3 | AgentPool 内核三态：`starting`/`running`/`stopped` | canonical，有 guard |
| `AgentHandle.exitReason` | 5 | `stopped` 的细化原因 | canonical，与 status 组合使用 |
| `AgentRunStatus` | 11 | 旧事件/DB/TUI 中的原始状态文本 | **adapter/compat 输入归一化** |
| `NormalizedAgentStatus` | 6 | 跨端展示态 | projection |

关键区别：
- `CoreAgentStatus` + `exitReason` 是 `AgentHandle` 的权威描述。
- `AgentRunStatus` 是对外兼容层——`src/contracts/adapters/StatusAdapter.ts` 和 `normalizeAgentStatus()` 将其归一化到 `NormalizedAgentStatus`（如 `'crashed' → 'failed'`、`'interrupted' → 'interrupted'`）。
- `isRunTerminalStatus` / `isTerminalAgentStatus` 是 `AgentRunStatus` 的终态判断函数，被 CLI/Leader/`SessionManager`/TUI 依赖作展示判断。

**Rust Core-first 结论**：canonical 保持 `CoreAgentStatus(Starting | Running | Stopped(ExitReason)) + taskRunGeneration`；`AgentRunStatus` 不应成为 Rust Core 权威状态机，只在 protocol adapter 层作输入归一化。

### Terminal state 规则

- `stopped` 是唯一 terminal agent pool 状态。
- 所有 `exitReason` 值都是 terminal。
- 结合 `exitReason` 后：`completed`、`terminated`、`failed`、`timeout`、`crashed` 都不可再被旧事件覆盖。

### Generation 要求

- `taskRunGeneration` 在 AgentHandle 上记录。Worker 完成回执必须携带 generation。
- Rust Core 在 stopped 状态丢弃旧 generation 的 complete/failed/timeout/crashed 事件。

### Rust 模块归属

- `lingxiao-core::agent` — AgentHandle + AgentPool canonical state
- `lingxiao-core::state` — transition table

### Golden test 建议

- **Happy path**: starting → running → stopped(completed)
- **Respawn**: stopped → starting → running
- **Illegal**: running → starting (must reject)
- **Late result**: old generation complete on stopped handle → must check generation

---

## 4. Worker 状态域（WorkerProcessRunner）

### TS 类型来源

| 项 | 值 |
|---|---|
| canonical 定义 | `src/core/StateSemantics.ts:80` |
| 类型名 | `CoreWorkerStatus` |
| WorkerHandle 使用 | `src/core/WorkerProcessRunner.ts:41, 166` |

```typescript
export type CoreWorkerStatus = 'starting' | 'running' | 'completed' | 'failed' | 'timeout' | 'crashed' | 'terminated';
```

### 当前 guard

**有部分 guard** — 通过 `isCoreWorkerActiveStatus()` 和 `isCoreWorkerTerminalStatus()` 校验：

- `handleWorkerMessage()` — 拦截 late complete/failed/error（`!isCoreWorkerActiveStatus(handle.status)` 时忽略）
- `handleWorkerExit()` — 只有 active 状态才能被 exit 改写
- `markWorkerTimeout()` — 只有 active 才能标 timeout
- `spawnWorker()` 同名复用 — 检查 `endTime !== undefined || isCoreWorkerTerminalStatus()`

**但无显式 transition 表**。状态转移是过程式的（直接 `handle.status = 'xxx'`），分散在多个方法中。

### 当前转移

| from | to | 触发位置 | guard |
|---|---|---|---|
| (new) | starting | `spawnWorker()` | ✅ 同名 active 拒绝 |
| starting | running | `handleWorkerMessage('started')` | ✅ `isCoreWorkerActiveStatus` |
| starting | timeout | `waitForWorkerStart` timeout | ✅ |
| starting | completed | (不可能) | — |
| starting | failed | `handleWorkerError` | ✅ `isCoreWorkerActiveStatus` |
| starting | crashed | `handleWorkerExit` (code!=0 or signal) | ✅ `isCoreWorkerActiveStatus` |
| starting | terminated | `handleWorkerExit` (SIGTERM) | ✅ |
| running | completed | `handleWorkerMessage('complete')` | ✅ `isCoreWorkerActiveStatus` |
| running | failed | `handleWorkerMessage('failed')` / `handleWorkerError` | ✅ |
| running | timeout | `markWorkerTimeout` (heartbeat/max_runtime/RSS) | ✅ `isCoreWorkerActiveStatus` |
| running | crashed | `handleWorkerExit` (code!=0 or null+signal) | ✅ `isCoreWorkerActiveStatus` |
| running | terminated | `handleWorkerExit` (SIGTERM, code=0) | ✅ |
| completed/failed/timeout/crashed/terminated | (none) | — | ✅ late message 被 `isCoreWorkerActiveStatus` 拦截 |

### Rust Core 建议 transition 表

```rust
enum WorkerStatus {
    Starting,
    Running,
    Completed,
    Failed,
    Timeout,
    Crashed,
    Terminated,
}
```

Transitions:

| from | to | guard |
|---|---|---|
| Starting | Running | startup IPC 确认 |
| Starting | Failed | 启动失败错误 |
| Starting | Timeout | spawn 超时 |
| Running | Completed | worker:complete 消息 + generation 校验 |
| Running | Failed | worker:failed/error 消息 |
| Running | Timeout | 心跳超时 / max_runtime / RSS 超限 |
| Running | Crashed | 非正常 exit(code!=0 或无 signal) |
| Running | Terminated | SIGTERM 正常退出 |
| *Terminal* | *any* | ❌ 必须拒绝 |

### Generation 要求

- Worker 没有独立 generation（它代理 Agent 执行，generation 在 AgentHandle 上）。
- Worker 的 late message 拦截通过 `isCoreWorkerActiveStatus` 检查，Rust Core 应当保留。

### Rust 模块归属

- `lingxiao-core::runtime` — WorkerProcessRunner 进程管理
- `lingxiao-core::agent` — Worker 状态作为 Agent 的子状态

### Golden test 建议

- **Happy path**: Starting → Running → Completed
- **Crash**: Running → Crashed → agent recovery
- **Timeout**: Running → Timeout → agent recovery
- **Illegal**: Completed → Running (late complete must be rejected)

---

## 5. Workflow Execution 状态域

### TS 类型来源

| 项 | 值 |
|---|---|
| 定义位置 | `src/core/workflow/types.ts:321` (`ExecutionContext.status`) |
| 类型 | 内联 union |
| Normalized | `src/core/StateSemantics.ts:28` |

```typescript
// ExecutionContext.status (canonical)
status: 'running' | 'completed' | 'failed' | 'paused' | 'cancelled';

// Normalized
export type NormalizedWorkflowExecutionStatus = 'running' | 'completed' | 'failed' | 'paused' | 'cancelled';
```

### Node 状态

```typescript
// src/core/workflow/types.ts:62-70
export type NodeStatus =
  | 'idle' | 'waiting' | 'running' | 'completed'
  | 'failed' | 'skipped' | 'paused' | 'cancelled';

// Normalized (same values)
export type CoreWorkflowNodeStatus = NormalizedWorkflowNodeStatus;
```

### WorkflowState 映射

`WorkflowState`（`src/contracts/types/Status.ts:71-83`）是一套 12 值高层状态类型，**不是 ExecutionContext.status 的直接等同**：

```typescript
export type WorkflowState =
  | 'idle' | 'planning' | 'running' | 'blocked'
  | 'completed' | 'failed' | 'cancelled'
  | 'repairing' | 'evaluating'
  | 'waiting_for_dependency' | 'waiting_for_user' | 'working';
```

`WorkflowState` 与 `ExecutionContext.status` 的关系：

| 层面 | 类型 | 角色 |
|---|---|---|
| Canonical execution | `ExecutionContext.status` (5 值) | 运行引擎状态机 |
| Canonical node | `NodeStatus` (8 值) | 单节点执行状态 |
| High-level projection | `WorkflowState` (12 值) | 高层编排/UI/project runtime |

`WorkflowState` 包含 `ExecutionContext.status` 没有的值（`repairing`、`evaluating`、`waiting_for_dependency`、`waiting_for_user`、`working`），这些是 Leader/Orchestration 的聚合推断，不是 workflow engine 本身的持久状态。

**Rust Core-first 结论**：Rust canonical 仍以 `ExecutionContext.status` + 每个 node 的 `NodeStatus` 为权威状态。`WorkflowState` 可在 `lingxiao-core::projection` 中派生，或在 protocol event 中作为 enrichment field，但不能反向驱动 workflow 状态机。

### 当前 guard

**Workflow execution 没有显式 transition 表或 assert 调用。** 状态变化是过程式的：

- `WorkflowEngine.execute()`: context.status = 'running' (初始)
- `WorkflowEngine.execute()`: context.status = 'completed' (正常结束) or 'failed' (异常)
- `WorkflowEngine.cancel()`: context.status = 'failed' (注意：cancel 设 failed 而非 cancelled)
- `WorkflowEngine.pause()`: 仅当 `context.status === 'running'` 时才设为 'paused'（**有准 guard**）
- `WorkflowEngine.resume()`: 仅当 `context.status === 'paused'` 时才设为 'running'
- `WorkflowEngine.runWorkflow()` timeout 回调: 'running' | 'paused' → 'failed'

**结论：缺乏完整 guard，cancel 和 failed 状态同用 'failed' 值。**

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| (new) | running | 创建 execution |
| running | completed | 所有节点 completed/skipped |
| running | failed | 节点执行失败（重试耗尽） |
| running | paused | `pause()` 仅允许 running→paused |
| running | cancelled | `cancel()` → 应设 cancelled 而非 failed |
| paused | running | `resume()` 仅允许 paused→running |
| paused | failed | timeout 回调 |
| paused | cancelled | cancel 在 paused 时也允许 |
| completed/failed/cancelled | *any* | ❌ terminal |

> 修复：Rust Core 应将 cancel 路径改为 `cancelled` 而非 `failed`。

### Node 状态 transition 表（Rust Core 草案）

| from | to | guard |
|---|---|---|
| idle | waiting | 依赖就绪 |
| waiting | running | 所有前置节点 completed |
| running | completed | 执行成功 |
| running | failed | 执行失败，重试耗尽 |
| running | paused | execution pause |
| completed/skipped/cancelled/failed | *any* | ❌ node terminal |

### Terminal state 规则

- Execution: `completed`, `failed`, `cancelled` 是 terminal（paused 是 non-terminal）。
- Node: `completed`, `failed`, `skipped`, `cancelled` 是 terminal。

### Generation 要求

- 不适用（workflow 没有多 generation 概念）。

### Rust 模块归属

- `lingxiao-core::workflow` — WorkflowEngine + ExecutionContext

### Golden test 建议

- **Happy path**: running → completed (all nodes completed)
- **Pause/resume**: running → paused → running → completed
- **Cancel**: running → cancelled
- **Illegal**: completed → running (must reject)
- **Illegal node**: idle → completed (must go through waiting→running)

---

## 6. Tool Call 状态域

### TS 类型来源

```typescript
// src/core/StateSemantics.ts:106
export type CoreToolCallStatus = 'streaming_input' | 'pending' | 'running' | 'completed' | 'failed' | 'cancelled';

// src/contracts/types/Status.ts:1-7 (same)
export type ToolCallStatus =
  | 'streaming_input' | 'pending' | 'running'
  | 'completed' | 'failed' | 'cancelled';
```

### 当前 guard

**无显式 transition 表。** 工具调用状态由 Leader/Agent 运行时过程式设置。

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| (new) | pending | tool_call 命令发出 |
| pending | running | 工具开始执行 |
| pending | streaming_input | 流式输入开始 |
| pending | cancelled | 用户/超时取消 |
| streaming_input | running | 输入完成，开始执行 |
| running | completed | 工具返回结果 |
| running | failed | 工具异常 |
| running | cancelled | 执行过程中取消 |
| completed/failed/cancelled | *any* | ❌ terminal |

### Terminal state 规则

- `completed`, `failed`, `cancelled` 是 terminal。

### Generation 要求

- 工具调用结果应绑定到 tool call 实例 ID。late result 基于实例匹配而非 generation。
- Rust Core 应在 tool call terminal 后丢弃该实例的后续消息。

### Rust 模块归属

- `lingxiao-core::tool` — tool call lifecycle
- `lingxiao-core::leader` — tool call as Leader orchestration state

### Golden test 建议

- **Happy path**: pending → running → completed
- **Cancel**: pending → cancelled
- **Illegal**: completed → running (must reject)

---

## 7. Terminal Session 状态域

### TS 类型来源

```typescript
// src/core/StateSemantics.ts:86-93
export type CoreTerminalSessionStatus =
  | 'started' | 'running' | 'suspended'
  | 'resumed' | 'completed' | 'failed' | 'killed';

// Normalized
export type NormalizedTerminalSessionStatus = 'running' | 'suspended' | 'completed' | 'failed' | 'killed';
```

注意：`started` 和 `resumed` 是事件名而非持久状态（`normalizeTerminalSessionStatus` 归一为 `running`）。

### 当前 guard

**无显式 transition 表。** 只有 `isTerminalSessionActiveStatus` 和 `isTerminalSessionTerminalStatus` 辅助函数。

### Rust Core 建议 transition 表 (canonical)

| from | to | guard |
|---|---|---|
| running | suspended | 挂起 |
| running | completed | 正常结束 |
| running | failed | 异常 |
| running | killed | 强制终止 |
| suspended | running | 恢复 |
| suspended | killed | 强制终止 |
| completed/failed/killed | *any* | ❌ terminal |

### Terminal state 规则

- `completed`, `failed`, `killed` 是 terminal。

### Generation 要求

- 不适用。

### Rust 模块归属

- `lingxiao-core::runtime` — terminal process lifecycle

---

## 8. Daemon / Supervisor 状态域

### TS 类型来源

```typescript
// Daemon
export type CoreDaemonStatus = 'running' | 'stopped';
export type NormalizedDaemonStatus = 'running' | 'stopped';

// Supervisor
export type CoreSupervisorStatus = 'watching' | 'restarting' | 'given_up' | 'stopped';
export type NormalizedSupervisorStatus = 'watching' | 'restarting' | 'given_up' | 'stopped';
```

### 当前 guard

**无显式 transition 表。** `isDaemonActiveStatus`, `isSupervisorActiveStatus`, `isSupervisorTerminalStatus` 提供分类判断。

### Rust Core 建议 transition 表

#### Daemon

| from | to | guard |
|---|---|---|
| stopped | running | start 命令 |
| running | stopped | stop 命令 / crash |
| stopped | (none) | terminal，但可 restart |

#### Supervisor

| from | to | guard |
|---|---|---|
| watching | restarting | 进程崩溃 / 心跳超时 |
| restarting | watching | 重启成功 |
| restarting | given_up | 重试耗尽 |
| stopped | watching | start 命令 |

### Terminal state 规则

- Daemon: stopped 不严格 terminal（可 restart）。
- Supervisor: `given_up` 和 `stopped` 是 terminal（不可自动恢复）。

### Rust 模块归属

- `lingxiao-core::runtime` — daemon lifecycle
- `lingxiao-core::session` — supervisor per-session

---

## 9. Worktree 状态域

### TS 类型来源

```typescript
export type CoreWorktreeStatus = 'active' | 'dirty' | 'merged' | 'removed' | 'failed';
export type NormalizedWorktreeStatus = 'active' | 'dirty' | 'merged' | 'removed' | 'failed';
```

### 当前 guard

**无显式 transition 表。** `isWorktreeTerminalStatus` 提供终端判断（merged/removed/failed）。

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| active | dirty | 文件修改 |
| dirty | merged | PR/分支合并 |
| dirty | active | 变更丢弃/回滚 |
| active | removed | 删除 |
| active/removed | failed | 操作失败 |
| merged | removed | 清理 |
| merged/removed/failed | *any* | ❌ terminal |

### Rust 模块归属

- `lingxiao-core::workspace` — worktree lifecycle

---

## 10. QQBot 状态域

### TS 类型来源

| 项 | 值 |
|---|---|
| 定义位置 | `src/core/StateSemantics.ts:114`, `src/bot/types.ts:7,41` |
| 类型名 | `CoreQQBotStatus` |
| Normalized 类型 | `NormalizedQQBotStatus` at `StateSemantics.ts:33` |
| active 判断 | `isQQBotActiveStatus()` at `StateSemantics.ts:322-325` |
| terminal 判断 | `isQQBotTerminalStatus()` at `StateSemantics.ts:327-328` |
| normalize 函数 | `normalizeQQBotStatus()` at `StateSemantics.ts:314-320` |

```typescript
// src/core/StateSemantics.ts:114
export type CoreQQBotStatus = 'disconnected' | 'connecting' | 'connected' | 'error';

// src/bot/types.ts:41 — QQBotStatus alias
export type QQBotStatus = CoreQQBotStatus;

// src/bot/types.ts:44-51 — runtime status shape
export interface QQBotRuntimeStatus {
  status: QQBotStatus;
  appId?: string;
  connectedAt?: number;
  messageCount?: number;
  lastMessageAt?: number;
  error?: string;
}
```

Active: `connecting`, `connected` (definition per `isQQBotActiveStatus` at `StateSemantics.ts:322-325`).
Terminal: `disconnected`, `error` (per `isQQBotTerminalStatus`: `!isQQBotActiveStatus`).

### 当前 guard

**无显式 transition 表。** 仅有 `isQQBotActiveStatus` / `isQQBotTerminalStatus` 分类函数。

状态转移是过程式的，全部在 `src/bot/QQBot.ts` 中：

```typescript
// QQBot.ts 中的 this.status 写入点（共 7 处）:
// line 272: start() — this.status = 'connecting'
// line 295: start() catch — this.status = 'error'
// line 306: stop() — this.status = 'disconnected'
// line 442: ws.onclose invalidSession — this.status = 'error'
// line 449: ws.onclose reconnect retry — this.status = 'disconnected'
// line 458: ws.onclose reconnect exhausted — this.status = 'error'
// line 501: READY dispatch — this.status = 'connected'
```

### 基于实现的现行转移

| from | to | 触发位置 | guard |
|---|---|---|---|
| disconnected | connecting | `start()` line 272 | ✅ `isQQBotActiveStatus` — 仅 non-active 允许 start |
| connecting | connected | READY dispatch line 501 | ❌ 无主动检查（自然到达） |
| connecting | disconnected | WS close before READY (reconnect retry) line 449 | ❌ |
| connecting | error | `start()` catch line 295 / invalidSession line 442 | ❌ |
| connected | disconnected | `stop()` line 306 / WS close retry line 449 | ❌ |
| connected | error | WS close reconnect exhausted line 458 | ❌ |
| error | connecting | `start()` line 272 (手动重试) | ✅ `isQQBotActiveStatus` — error 非 active，允许 |
| error | disconnected | `stop()` line 306 | ❌ |

### Rust Core 建议 canonical transition 表

| from | to | guard |
|---|---|---|
| disconnected | connecting | 必须非 active（start 命令） |
| connecting | connected | READY 事件到达 |
| connecting | disconnected | WS 过早断开（自动重连前退避） |
| connecting | error | 认证/网关/INVALID_SESSION 失败 |
| connected | disconnected | 显式 stop / WS 断开触发重连 |
| connected | error | 重连耗尽 / 无法恢复的错误 |
| error | connecting | 手动 start（显式重试） |
| error | disconnected | 显式 stop |
| *terminal* | *any* | ❌ (disconnected 和 error 都 terminal) |

### Terminal state 规则

- `disconnected` 和 `error` 是 terminal（`isQQBotTerminalStatus` 判断）。
- `disconnected` 可被手动 `start()` 重连（从 terminal 恢复）。
- `error` 需要人工介入后手动 start 或 stop。

### Generation 要求

- 不适用（QQBot 无多 generation 概念）。

### Rust Core-first 说明

QQBot 是远程入口/adapter capability，**不约束 Rust Core 主状态机**。

- 当前实现：`src/bot/QQBot.ts` 是一个 WebSocket 客户端，通过 `SessionManager` 向 daemon 会话投递消息。
- Rust Core 定位：QQBot 属于 adapter layer，核心只接收 authenticated external input command。若迁移，可归入：
  - `lingxiao-core::bot`（如果需要 core 原生 bot 管理）
  - 或 sidecar/adapter boundary（推荐：Rust Core 不绑定 QQ 协议）

Core 不需要知道 QQBot 的内部状态（disconnected/connecting/connected/error），只需要知道「外部输入已认证并到达」这一单一事实。

### Golden test 建议

- **Happy path**: disconnected → connecting → connected
- **Auth failure**: connecting → error
- **Reconnect**: connected → disconnected (retry) → connecting → connected
- **Illegal**: disconnected → connected (must go through connecting)
- **Illegal**: connected → connecting (must go through disconnected first)

---

## 11. Blackboard Intent 状态域

### TS 类型来源

```typescript
export type CoreBlackboardIntentStatus = 'open' | 'claimed' | 'resolved';
export type NormalizedBlackboardIntentStatus = 'open' | 'claimed' | 'resolved';
```

### 当前 guard

**无显式 transition 表。** `isBlackboardIntentActiveStatus` 和 `isBlackboardIntentTerminalStatus` 提供分类。

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| open | claimed | agent pick up intent |
| claimed | resolved | intent 完成 |
| open | resolved | 直接解决不经过 claimed |
| resolved | *any* | ❌ terminal |

### Rust 模块归属

- `lingxiao-core::blackboard` — intent lifecycle

---

## 12. Team Delivery 状态域

### TS 类型来源

```typescript
export type CoreTeamDeliveryStatus = 'queued' | 'delivered' | 'read' | 'skipped' | 'failed';
export type NormalizedTeamDeliveryStatus = 'queued' | 'delivered' | 'read' | 'skipped' | 'failed';
```

### 当前 guard

**无显式 transition 表。** `isTeamDeliveryTerminalStatus` 提供分类。

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| queued | delivered | 发送成功 |
| queued | failed | 发送失败 |
| delivered | read | 收件人已读 |
| delivered | skipped | 收件人忽略 |
| read/skipped/failed | *any* | ❌ terminal |

### Rust 模块归属

- `lingxiao-core::team` — team mailbox delivery

---

## 13. External Agent 状态域

### TS 类型来源

```typescript
export type CoreExternalAgentStatus = 'starting' | 'running' | 'completed' | 'failed' | 'timeout' | 'crashed' | 'terminated';
```

### 当前 guard

**有部分 guard** — `isCoreExternalAgentTerminalStatus()` 和 `isCoreExternalAgentActiveStatus()` 用于拦截：

- `ExternalAgentRunner.ts:463` — 判断 terminal 时不再跟踪
- `ExternalAgentRunner.ts:470` — 只有 active 状态才检查 idle timeout

**但无显式 transition 表。**

### Rust Core 建议 transition 表

| from | to | guard |
|---|---|---|
| starting | running | 外部进程就绪信号 |
| starting | failed | 启动失败 |
| starting | crashed | 进程异常退出 |
| starting | terminated | 用户终止 |
| running | completed | 任务完成 |
| running | failed | 执行失败 |
| running | timeout | 超时 |
| running | crashed | 进程崩溃 |
| running | terminated | 用户终止 |
| *terminal* | *any* | ❌ |

### Rust 模块归属

- `lingxiao-core::agent` — external agent lifecycle (sidecar management)

---

## 14. AgentExecutionResult 状态域

### TS 类型来源

```typescript
// src/agents/AgentExecutionResult.ts:14
export type ExecutionStatus = 'completed' | 'failed' | 'blocked';
```

### 说明

这是 Worker 完成返回的结构化结果中的状态字段，不是 Agent 本身的 lifecycle 状态。

- `completed`: 任务正常完成
- `failed`: 任务执行失败
- `blocked`: 任务因外部条件阻塞，需要 Leader 判断下一步

### 当前 guard

**无显式 transition**（是结果返回值，不是 persistent 状态机）。

### Rust Core 要求

- `blocked` 结果必须触发 Leader 重路由/修复路径。
- Rust Core 应保留三种返回状态语义。

### Rust 模块归属

- `lingxiao-core::agent` — completion result processing
- `lingxiao-core::leader` — blocked result 的再路由

---

## 15. Project Runtime 状态族

### 说明

Project Runtime 是凌霄的项目级长周期运行态，包含多个子状态域。**这些状态属于高层 projection/compat status family，不是 canonical core 状态机。** Phase 0 必须盘点，具体迁移优先级可低（P2+）。

### TS 类型来源

| 子域 | TS 类型 | 定义位置 |
|---|---|---|
| Runtime mode | `ProjectRuntimeMode` (12 值) | `src/core/ProjectRuntimeState.ts:3-15` |
| Normalized mode | `NormalizedProjectRuntimeMode` (8 值) | `src/core/StateSemantics.ts:36-44` |
| Backlog item | `ProjectBacklogItemStatus` / `NormalizedProjectBacklogStatus` (6 值) | `ProjectRuntimeState.ts:17-23` / `StateSemantics.ts:45` |
| Milestone | `ProjectMilestoneStatus` / `NormalizedProjectMilestoneStatus` (4 值) | `ProjectRuntimeState.ts:25` / `StateSemantics.ts:46` |
| Risk | `ProjectRiskStatus` / `NormalizedProjectRiskStatus` (4 值) | `ProjectRuntimeState.ts:28` / `StateSemantics.ts:47` |
| Dependency | `ProjectDependencyStatus` / `NormalizedProjectDependencyStatus` (4 值) | `ProjectRuntimeState.ts:38` / `StateSemantics.ts:48` |

```typescript
// src/core/ProjectRuntimeState.ts
export type ProjectRuntimeMode =
  | 'draft' | 'planning' | 'sprint_in_flight' | 'evaluating'
  | 'repairing' | 'waiting_for_dependency' | 'blocked_external'
  | 'recovering' | 'replanning' | 'idle' | 'completed' | 'archived';

export type ProjectBacklogItemStatus =
  | 'planned' | 'ready' | 'in_progress' | 'blocked' | 'completed' | 'cancelled';

export type ProjectMilestoneStatus = 'pending' | 'at_risk' | 'completed' | 'missed';
export type ProjectRiskStatus = 'open' | 'mitigated' | 'accepted' | 'closed';
export type ProjectDependencyStatus = 'requested' | 'awaiting_input' | 'fulfilled' | 'failed';
```

### 当前 guard

**全部无显式 transition 表。** `StateSemantics.ts` 中仅提供 `normalize*` 和 `is*TerminalStatus` helpers（行 375-453）。状态由组件自行写入（`ProjectRuntimeState` 无类，是纯数据接口）。

### Normalized 投影关系

所有 Project Runtime 子域都有对应的 `Normalized*` 投影类型（`StateSemantics.ts:36-48`），将 `ProjectRuntimeState.ts` 的原始 raw 值归一化后供跨端展示。例如：

```typescript
normalizeProjectRuntimeMode('sprint_in_flight') → 'running'
normalizeProjectRuntimeMode('replanning') → 'planning'
normalizeProjectBacklogStatus('in_progress') → 'running'
```

### Terminal state 规则

| 子域 | terminal states |
|---|---|
| Runtime mode | `completed`, `archived`（per `isProjectRuntimeTerminalMode`） |
| Backlog item | `completed`, `cancelled`（per `isProjectBacklogTerminalStatus`） |
| Milestone | `completed`, `missed`（per `isProjectMilestoneTerminalStatus`） |
| Risk | `mitigated`, `accepted`, `closed`（per `isProjectRiskTerminalStatus`） |
| Dependency | `fulfilled`, `failed`（per `isProjectDependencyTerminalStatus`） |

### Generation 要求

- 不适用（Project Runtime 无 generation 概念）。

### Rust 模块归属

- `lingxiao-core::project` — canonical project runtime state machine（Phase 2+）
- `lingxiao-core::projection::project_runtime` — normalied projection（Phase 2+）

**Rust Core-first 结论**：Project Runtime 是高层业务语义，Phase 0 只盘点不迁移。Rust Core v1 可以在 `lingxiao-core::project` 中以简单 struct + transition method（无 enum guard）实现，或延迟到 Phase 3+。当前 `ProjectRuntimeState.ts` 的所有状态值在 Rust 中应保留语义等价映射。

### Golden test 建议

- **Happy path mode**: draft → planning → sprint_in_flight → completed
- **Normalization**: raw `'sprint_in_flight'` → normalized `'running'`
- **Backlog lifecycle**: planned → ready → in_progress → completed
- **Risk lifecycle**: open → mitigated → closed

---

## 16. 状态域概览

### Canonical 状态域（Phase 0-1 Rust Core 必须覆盖）

| # | 状态域 | Core 类型 | 有显式 transition 表 | 有 assert guard | terminal states | Rust 模块 |
|---|---|---|---|---|---|---|
| 1 | Session SessionPhase | `SessionPhase` | ❌ | ❌ | done, error, interrupted | `session` / `projection` |
| 2 | SessionManager status | `'active'\|'completed'\|'failed'\|'interrupted'` | ❌ | ❌ | completed, failed, interrupted | `session` |
| 3 | TaskBoard | `CoreTaskStatus` | ✅ | ✅ | terminal | `task` / `state` |
| 4 | AgentPool | `CoreAgentStatus` | ✅ | ✅ | stopped | `agent` / `state` |
| 5 | Worker | `CoreWorkerStatus` | ❌ | ⚠️ 部分 | completed, failed, timeout, crashed, terminated | `runtime` / `agent` |
| 6 | Workflow Execution | `'running'\|'completed'\|'failed'\|'paused'\|'cancelled'` | ❌ | ⚠️ pause/resume 有检查 | completed, failed, cancelled | `workflow` |
| 7 | Workflow Node | `NodeStatus` (= `CoreWorkflowNodeStatus`) | ❌ | ❌ | completed, failed, skipped, cancelled | `workflow` |
| 8 | Tool Call | `CoreToolCallStatus` | ❌ | ❌ | completed, failed, cancelled | `tool` |
| 9 | Terminal Session | `CoreTerminalSessionStatus` | ❌ | ❌ | completed, failed, killed | `runtime` |
| 10 | Daemon | `CoreDaemonStatus` | ❌ | ❌ | (restartable) | `runtime` |
| 11 | Supervisor | `CoreSupervisorStatus` | ❌ | ❌ | given_up, stopped | `runtime` / `session` |
| 12 | Worktree | `CoreWorktreeStatus` | ❌ | ❌ | merged, removed, failed | `workspace` |
| 13 | QQBot | `CoreQQBotStatus` | ❌ | ❌ | disconnected, error | `bot` (adapter) |
| 14 | Blackboard Intent | `CoreBlackboardIntentStatus` | ❌ | ❌ | resolved | `blackboard` |
| 15 | Team Delivery | `CoreTeamDeliveryStatus` | ❌ | ❌ | read, skipped, failed | `team` |
| 16 | External Agent | `CoreExternalAgentStatus` | ❌ | ⚠️ 部分 | completed, failed, timeout, crashed, terminated | `agent` |

> `CoreWorkflowExecutionStatus` (= `NormalizedWorkflowExecutionStatus`) 是 execution context 状态别名，已计入第 6 行。

### Projection / Compat Status Families（非 canonical core 状态机）

这些类型是跨端投影、旧事件归一化或高层业务语义，**不应直接约束 Rust Core 主状态机**。

| # | 状态族 | TS 来源 | 值数 | Rust Core 定位 | 迁移优先级 |
|---|---|---|---|---|---|
| A | `AgentRunStatus` | `src/contracts/types/Status.ts:9-20` | 11 | adapter/compat 输入归一化 → `NormalizedAgentStatus` | compat layer |
| B | `WorkflowState` | `src/contracts/types/Status.ts:71-83` | 12 | 高层编排/UI projection | `projection` |
| C | `TaskStatus` (旧 10 值) | `src/contracts/types/Status.ts:59-69` | 10 | 旧事件兼容入口 → `CoreTaskStatus + exitReason` | compat layer |
| D | `SessionPhase` | `src/contracts/types/Status.ts:42-57` | 15 | Leader 阶段 projection | `projection` |
| E | Project Runtime mode | `src/core/ProjectRuntimeState.ts:3-15` / 归一化 `StateSemantics.ts:36-44` | 12 raw / 8 norm | 高层项目态，Phase 3+ | P2+ |
| F | Project Backlog status | `ProjectRuntimeState.ts:17-23` / `StateSemantics.ts:45` | 6 | 同上 | P2+ |
| G | Project Milestone status | `ProjectRuntimeState.ts:25` / `StateSemantics.ts:46` | 4 | 同上 | P2+ |
| H | Project Risk status | `ProjectRuntimeState.ts:28` / `StateSemantics.ts:47` | 4 | 同上 | P2+ |
| I | Project Dependency status | `ProjectRuntimeState.ts:38` / `StateSemantics.ts:48` | 4 | 同上 | P2+ |

### 关键发现

1. **Canonical 域：仅有 Task 和 AgentPool 两个域有完整 guard（2/16）。**
2. **Canonical 域：Worker、Workflow Execution、External Agent 有部分拦截 guard 但无统一 transition 表（3/16 部分）。**
3. **Canonical 域：其余 11 个域完全缺失 guard（11/16 缺失）。**
4. **Projection 族：`AgentRunStatus`、`WorkflowState`、旧 `TaskStatus` 等是 adapter/compat 层，不应驱动 Rust Core 状态机。**
5. **Project Runtime 一族是高层业务语义，Phase 0 盘点，Phase 3+ 迁移。**
6. **Workflow cancel 错误使用 'failed' 而非 'cancelled'** — Rust Core 必须修复。
7. **Session 状态混乱** — `SessionPhase` (15 值, 无 guard) 和 `SessionManager.session.status` (4 值, 无 guard) 是两套独立状态；Rust Core 应合为一个 canonical enum。
8. **ExecutionStatus (completed/failed/blocked) 是结果返回值, 不是状态机**。

### Rust Core 迁移建议

1. **所有域必须使用 enum + 显式 transition 表** — 过程式 `status = 'xxx'` 全替换。
2. **Terminal state + generation check 是统一的跨域模式** — 考虑设计 `lingxiao-core::state` 宏/ trait。
3. **Worker 和 Agent 的状态机应合并** — Worker 是 AgentHandle 的子状态。
4. **Session 两个状态域应合并为一个 canonical enum**。
5. **`reopenTask()` 是设计意图的显式 reset/reopen command**，不走普通 transition 表。Rust Core 应实现 canonical `task.reopen` command（带 generation bump 和 exitReason 清空），但在 enum transition guard 中仍保留 `Terminal` → `Dispatchable` 为非法（迫使用户显式调用 reopen）。
6. **Projection 状态族不驱动 Rust Core 状态机**：`AgentRunStatus`、`WorkflowState`、旧 `TaskStatus`、`ProjectRuntime*Status` 等都在 adapter/projection 层归一化，不反向约束 canonical enum。
7. **`AgentRunStatus` 的 `isRunTerminalStatus` / `isTerminalAgentStatus` 等 helper 调用点（CLI、Leader、SessionManager、TUI）在 Rust Core 中应改为消费 canonical `CoreAgentStatus + exitReason` 的组合判断。

---

## 17. 核对证据

### rg 核对命令

```bash
# 所有 Core*Status 类型定义
rg -n "export type Core[A-Z]\w+Status" -g "*.ts" src/

# CORE_*_TRANSITIONS 表
rg -n "CORE_[A-Z]+_TRANSITIONS" -g "*.ts" src/

# assertCore*Transition 调用
rg -n "assertCore[A-Z]\w+Transition" -g "*.ts" src/

# isCore*TerminalStatus / isCore*ActiveStatus 定义
rg -n "export function isCore" -g "*.ts" src/

# Worker 状态直接写入点（无 guard 证据）
rg -n "handle\.status\s*=" -g "*.ts" src/core/WorkerProcessRunner.ts

# ExecutionContext status 写入点
rg -n "context\.status\s*=" -g "*.ts" src/core/workflow/WorkflowEngine.ts

# SessionPhase 无 guard 证据
rg -n "SessionPhase" -g "*.ts" src/contracts/types/Status.ts
rg -n "normalizeSessionPhase\|assertSessionPhase" -g "*.ts" src/
```

### 核对结果摘要

- `CORE_AGENT_TRANSITIONS` / `CORE_TASK_TRANSITIONS`: ✅ 存在，在 `StateSemantics.ts:134-148` 和 `StatusAdapter.ts:134-148` (两个副本)
- `assertCoreAgentTransition`: ✅ 调用于 `AgentPoolRuntime.ts:368`, `LeaderAgent.ts:217`
- `assertCoreTaskTransition`: ✅ 调用于 `TaskBoard.ts:499,525,675`
- Worker 无 `CORE_WORKER_TRANSITIONS`: ❌
- Workflow 无 `CORE_WORKFLOW_TRANSITIONS`: ❌
- ToolCall 无 `CORE_TOOL_CALL_TRANSITIONS`: ❌
- SessionPhase 无任何 assert: ❌

---

## 18. 验收检查

### 维度
1. ✅ Normalized/Core 状态类型全部列出 — 16 canonical 域 + 9 projection/compat 族 + AgentExecutionResult
2. ✅ 每个状态域的 TS 类型来源标注（文件 + 行号）
3. ✅ 每个状态域的当前 guard 状态（11/16 canonical 完全缺失，3/16 部分 guard，2/16 完整 guard）
4. ✅ 合法 transition 表草案（对有 guard 和无 guard 的域均提出）
5. ✅ Terminal state 规则
6. ✅ Generation/lease/idempotency 要求（Task, Agent, Worker）
7. ✅ Rust 模块归属
8. ✅ Golden test 建议
9. ✅ 明确 TaskBoard canonical 是 `dispatchable/running/terminal + exitReason`
10. ✅ QQBot 完整覆盖
11. ✅ `AgentRunStatus` 映射：11 值输入归一化 vs `CoreAgentStatus` 3 值 canonical vs `NormalizedAgentStatus` 6 值 projection
12. ✅ `WorkflowState` 映射：12 值高层 projection vs `ExecutionContext.status` 5 值 canonical
13. ✅ Project Runtime 族：5 个子域（mode/backlog/milestone/risk/dependency），Phase 0 盘点，P2+ 迁移
14. ✅ `reopenTask()` 措辞修正：是设计意图的显式 reset command，不是 bug
15. ✅ 总览分离 canonical domains（16）和 projection/compat families（9），避免混淆
16. ✅ rg 证据核对：`AgentRunStatus` / `WorkflowState` / `ProjectRuntime*Status` / 全部 `normalize*` / `is*TerminalStatus` 覆盖

### 注意

- `CORE_AGENT_TRANSITIONS` 和 `CORE_TASK_TRANSITIONS` 定义在 `src/core/StateSemantics.ts` 和 `src/contracts/adapters/StatusAdapter.ts` 两个文件——Rust Core 应以 `StateSemantics.ts` 为权威来源, `StatusAdapter.ts` 是 re-export/adapter。
- 本文件只盘点，不修改业务代码。
