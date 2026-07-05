# Rust Core Golden Scenarios

> Phase 0 交付物 — 任务 P0-7 A2。
>
> 本文定义 Rust Core 所有核心域的可执行验收场景。每条 scenario 是 Rust Core 实现必须通过的黑盒测试，不依赖旧 TUI/Web/Electron。
>
> 参考：`rust-core-migration-spec.md`、`rust-core-function-inventory.md`、`rust-core-event-inventory.md`、`rust-core-sqlite-schema-map.md`、`rust-core-state-machines.md`。

---

## Scenario 索引

| ID | 核心域 | 优先级 | Phase | 覆盖要点 |
|---|---|---|---|---|
| GS-001 | Session | P0 | 2 | 创建→输入→完成生命周期 |
| GS-002 | Session | P0 | 2 | 中断→恢复 |
| GS-003 | Session | P0 | 2 | 删除→资源释放 |
| GS-004 | Session | P0 | 2 | Crash 后恢复 session |
| GS-005 | TaskBoard | P0 | 2 | 任务完整生命周期 |
| GS-006 | TaskBoard | P0 | 2 | 任务重分发 (redispatch) |
| GS-007 | TaskBoard | P0 | 2 | Late generation 被拒绝 |
| GS-008 | Agent/Leader | P0 | 3 | Agent 启动→运行→完成 |
| GS-009 | Agent/Leader | P0 | 3 | Agent crash→recovery |
| GS-010 | Agent/Leader | P0 | 3 | Leader 拆解任务→agent 分配 |
| GS-011 | Permission | P0 | 2 | Permission request/resume 完整流程 |
| GS-012 | Permission | P0 | 2 | Mode change 后 grant 失效 |
| GS-013 | Permission | P0 | 2 | Permission resume 跨 crash |
| GS-014 | Workflow | P1 | 3 | 串行/并行节点执行 |
| GS-015 | Workflow | P1 | 3 | Pause/resume/cancel |
| GS-016 | Workflow | P1 | 4 | Crash 后恢复 workflow |
| GS-017 | Tool/Sidecar | P1 | 3 | Rust-native tool 调用 |
| GS-018 | Tool/Sidecar | P1 | 3 | Sidecar tool timeout/cancel |
| GS-019 | LLM stream | P1 | 3 | Stream text/thinking/tool_call |
| GS-020 | Event log replay | P0 | 1 | 有序 event replay |
| GS-021 | Event log replay | P0 | 1 | Generation gap 检测与拒绝 |
| GS-022 | Snapshot/delta reconnect | P0 | 1 | Cursor reconnect delta |
| GS-023 | Snapshot/delta reconnect | P0 | 1 | Gap 太大触发 snapshot |
| GS-024 | SQLite persistence | P0 | 1 | Schema parity |
| GS-025 | SQLite persistence | P0 | 1 | Core 唯一 writer |
| GS-026 | Recovery/crash | P0 | 4 | Kill→restart→resume session |
| GS-027 | Recovery/crash | P0 | 4 | Context 不因 crash 丢失 |
| GS-028 | Recovery/crash | P0 | 4 | Workflow execution 不 stuck running |
| GS-029 | Resource budget | P1 | 4 | Token budget 超限→compaction |
| GS-030 | Resource budget | P1 | 4 | Sidecar 资源会计 |
| GS-031 | MessageBus/backpressure | P1 | 3 | 优先级投递与 backpressure |
| GS-032 | MessageBus/backpressure | P1 | 3 | Dead-letter 处理 |
| GS-033 | Context/conversation persistence | P1 | 3 | 消息持久化→replay 一致 |
| GS-034 | Context/conversation persistence | P1 | 4 | Compaction 不丢原始事实 |
| GS-035 | Team/blackboard/memory | P2 | 5 | Blackboard intent 声明→解析 |
| GS-036 | Team/blackboard/memory | P2 | 5 | Team mailbox 消息投递 |

---

## GS-001：Session 生命周期（创建→输入→完成）

- **Phase**: 2
- **优先级**: P0
- **核心域**: Session

### 初始状态

- Core 已启动，SQLite 已初始化
- 无活跃 session

### 命令序列

```
1. command: session.create
   params: { workspace: "/tmp/test-ws" }
   actor: client

2. command: session.input
   params: { session_id: "<sid>", content: "完成调研报告" }
   actor: client

3. command: session.complete
   params: { session_id: "<sid>" }
   actor: client
```

### 期望 canonical events

```
seq=1  event_type: session.created
       generation: 1
       payload: { session_id: "<sid>", workspace: "/tmp/test-ws" }

seq=2  event_type: session.input_received
       generation: 1
       payload: { session_id: "<sid>", content: "完成调研报告" }

seq=3  event_type: session.completed
       generation: 1
       payload: { session_id: "<sid>", summary: "..." }
```

### 期望 snapshot/projection

```
snapshot at seq=3:
  session_id: "<sid>"
  generation: 1
  status: "completed"
  last_seq: 3
```

### 期望 DB 断言

```sql
-- sessions 表有一条记录
SELECT status FROM sessions WHERE id = '<sid>';
-- → 'completed'

-- 3 条 leader_conversation
SELECT COUNT(*) FROM leader_conversation WHERE session_id = '<sid>';
-- → 0 (纯 session 生命周期无 conversation 写入)
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| 重复 request_id | 第 1 步 command 带 idempotency_key | 重复命令返回已有 seq=1，不新增事件 |
| 空 session | create 后直接 complete | seq=1 session.created → seq=2 session.completed |

### 建议 e2e 测试形态

**Rust integration test** — `lingxiao-core` crate 内 `#[cfg(test)] mod session_lifecycle`。直接调用 `SessionCommandHandler::handle()` 验证事件和 DB。

---

## GS-002：Session 中断→恢复

- **Phase**: 2
- **优先级**: P0
- **核心域**: Session
- **覆盖**: session interruption, resume

### 初始状态

- Session 已创建，正在执行 LLM 请求（SessionPhase: model_requesting）
- seq=2 (session.input_received 已发出)

### 命令序列

```
1. command: session.interrupt
   params: { session_id: "<sid>" }
   actor: client

2. command: session.input
   params: { session_id: "<sid>", content: "换一个方向，调研竞品" }
   actor: client
```

### 期望 canonical events

```
seq=3  event_type: session.interrupted
       generation: 2          ← generation bumped
       payload: { session_id: "<sid>", previous_generation: 1 }

seq=4  event_type: session.input_received
       generation: 2
       payload: { session_id: "<sid>", content: "换一个方向，调研竞品" }
```

### 期望 snapshot/projection

```
snapshot:
  session_id: "<sid>"
  generation: 2
  status: "active"
  last_input: "换一个方向，调研竞品"
```

### 期望 DB 断言

```sql
SELECT status FROM sessions WHERE id = '<sid>';
-- → 'active'
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| 中断已 terminal session | interrupt 在 completed 后 | 错误码：`SessionAlreadyTerminal` |
| generation=1 的 late input | 中断后旧 worker 返回结果 | 事件可记录但 state transition 被拒绝 |

### 建议 e2e 测试形态

**Rust integration test** — 构造处于 `model_requesting` 状态的 session，注入中断命令。

---

## GS-003：Session 删除→资源释放

- **Phase**: 2
- **优先级**: P0
- **核心域**: Session

### 初始状态

- Session 已创建，含 inputs（seq=1~5）

### 命令序列

```
1. command: session.delete
   params: { session_id: "<sid>" }
   actor: client
```

### 期望 canonical events

```
seq=6  event_type: session.deleted
       generation: 1
       payload: { session_id: "<sid>" }
```

### 期望 snapshot/projection

```
session: Not found (或 marked deleted)
```

### 期望 DB 断言

```sql
SELECT status FROM sessions WHERE id = '<sid>';
-- → 'deleted'

-- 关联资源释放（event log 可 compaction）
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| 删除不存在的 session | session_id 非法 | 错误码：`SessionNotFound` |
| 删除进行中的 session | session 正在 tool_executing | 先 cancel 再 delete，或强制删除 |

### 建议 e2e 测试形态

**Rust integration test** + **Daemon protocol test**（验证删除后 client 无法再操作）。

---

## GS-004：Crash 后恢复 Session

- **Phase**: 2
- **优先级**: P0
- **核心域**: Session
- **覆盖**: crash recovery

### 初始状态

- Session seq=1~4（created, input_received, task.created, task.assigned）
- DB 已持久化上述事件
- 内存中 session 状态为 active

### 操作序列

```
1. kill core (SIGKILL / 进程崩溃)
2. restart core (同一 SQLite DB 路径)
3. command: session.list
```

### 期望行为

```
response: snapshot
  sessions: [
    { session_id: "<sid>", status: "active", generation: 1, last_seq: 4 }
  ]

-- 从 event log replay seq=1..4 重建状态
-- 内存状态与 DB event log 一致
```

### 期望 DB 断言（crash 后未变）

```sql
SELECT COUNT(*) FROM sessions WHERE id = '<sid>';
-- → 1
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Crash 在 event 写入期间 | event_log 原子写入 | 重启后 seq 连续，无 gap |
| Crash 后 DB 损坏 | WAL 回滚 | 恢复到上一个完整 checkpoint |
| 多 session crash | 3 个 active session | 全部从 event log 重建 |

### 建议 e2e 测试形态

**Rust integration test** — 构造 session 状态，直接 drop DB connection + 重新 open + 断言重建状态。**Daemon protocol test** — 真实 kill/restart daemon。

---

## GS-005：任务完整生命周期

- **Phase**: 2
- **优先级**: P0
- **核心域**: TaskBoard

### 初始状态

- Session 已创建，generation=1
- Leader 正在执行

### 命令序列

```
1. command: task.create
   params: { session_id: "<sid>", subject: "文件搜索",
             agent_type: "explore" }

2. command: task.assign
   params: { session_id: "<sid>", task_id: "<tid>",
             assigned_agent: "explore-1" }

3. command: task.complete
   params: { session_id: "<sid>", task_id: "<tid>",
             result: "找到 3 个匹配文件",
             run_generation: 1 }
```

### 期望 canonical events

```
seq=3  event_type: task.created
       generation: 1
       payload: { task_id: "<tid>", subject: "文件搜索", status: "dispatchable" }

seq=4  event_type: task.assigned
       generation: 1
       payload: { task_id: "<tid>", assigned_agent: "explore-1", run_generation: 1 }

seq=5  event_type: task.completed
       generation: 1
       payload: { task_id: "<tid>", result: "找到 3 个匹配文件",
                  exit_reason: "completed", run_generation: 1 }
```

### 期望 snapshot/projection

```
task "<tid>":
  status: "terminal"
  exit_reason: "completed"
  run_generation: 1
```

### 期望 DB 断言

```sql
SELECT status, exit_reason FROM tasks WHERE id = '<tid>' AND session_id = '<sid>';
-- → 'terminal', 'completed'
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Task fail | task.fail 代替 task.complete | status='terminal', exit_reason='failed' |
| Task cancel | task.cancel 同步 | status='terminal', exit_reason='cancelled' |

### 建议 e2e 测试形态

**Rust integration test** — `lingxiao-core::task` 的 `TaskBoard::handle_command()` 测试。

---

## GS-006：Task 重分发 (Redispatch)

- **Phase**: 2
- **优先级**: P0
- **核心域**: TaskBoard
- **覆盖**: dispatchable→running→dispatchable (blocked)

### 初始状态

- Task 已创建，status=dispatchable

### 命令序列

```
1. command: task.assign       → running (generation=1)
2. command: task.block
   params: { task_id: "<tid>", blocked_reason: "需要更多上下文" }
3. command: task.assign       → running (generation=2, bumped)
4. command: task.complete
   params: { run_generation: 2 }
```

### 期望 canonical events

```
seq=2  task.assigned          generation=1, run_generation=1
seq=3  task.updated           generation=1, status="dispatchable", blocked_reason=...
seq=4  task.assigned          generation=1, run_generation=2
seq=5  task.completed         generation=1, exit_reason="completed", run_generation=2
```

### 期望 snapshot/projection

```
task "<tid>":
  status: "terminal"
  run_generation: 2
  exit_reason: "completed"
```

### 期望 DB 断言

```sql
SELECT run_generation FROM tasks WHERE id = '<tid>' AND session_id = '<sid>';
-- → 2
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Old generation complete | generation=1 的 complete 在 assign generation=2 后到达 | 记录到 event log，但 state transition 拒绝 |
| 非法 reopen completed | reopen 已 completed 的 task | 错误码：`TaskTerminalCannotReopen` |

### 建议 e2e 测试形态

**Rust integration test** — 显式测试 `TaskBoard::assign_task()` → `TaskBoard::block_task()` → `TaskBoard::assign_task()` → `TaskBoard::complete_task()`。

---

## GS-007：Late Generation 被拒绝

- **Phase**: 2
- **优先级**: P0
- **核心域**: TaskBoard
- **覆盖**: late generation, terminal state 保护

### 初始状态

- Task 已 terminal（completed, run_generation=2）
- Session current generation=1

### 命令序列(1)：旧 run_generation 的 complete 到达

```
1. (模拟 late worker 结果)
   command: task.complete
   params: { task_id: "<tid>", run_generation: 1, result: "老结果" }
```

### 期望行为

```
事件写入 event_log（可审计），但：
- state transition 被拒绝
- task.status 保持 "terminal"
- task.result 保持原值，不被覆盖
```

### 期望 snapshot/projection

```
task "<tid>":
  status: "terminal"
  exit_reason: "completed"
  run_generation: 2
  result: "新结果"    ← 未被 old generation 覆盖
```

### 命令序列(2)：已 terminal session 的输入

```
1. command: session.input
   params: { session_id: "<sid>", content: "再来一题" }
```

### 期望行为

- session 为 terminal (completed/failed) → 错误码 `SessionTerminal`
- 如果 launch new generation → session.generation bump → session.input_received 写入

### 期望 DB 断言（序列 2 拒绝后）

```sql
SELECT status FROM sessions WHERE id = '<sid>';
-- → 保持 'completed'（如果未 bump generation）
```

### 建议 e2e 测试形态

**Rust integration test** — 模拟 late result 注入，验证 state 不被覆盖。**Unit test** — `StateTransitionGuard::reject_old_generation()`。

---

## GS-008：Agent 启动→运行→完成

- **Phase**: 3
- **优先级**: P0
- **核心域**: Agent/Leader
- **覆盖**: agent lifecycle

### 初始状态

- Session active，Leader 已创建 task
- task.status=dispatchable

### 命令序列

```
1. (Leader 分配 agent)
   command: agent.spawn
   params: { session_id: "<sid>", agent_name: "explore-1",
             agent_type: "explore", task_id: "<tid>" }

2. (Agent 异步启动后)
   event_internal: agent.started
   params: { agent_id: "<aid>", task_id: "<tid>" }

3. (Agent 完成任务)
   event_internal: agent.completed
   params: { agent_id: "<aid>", task_id: "<tid>",
             result: "done", exit_reason: "completed" }
```

### 期望 canonical events

```
seq=3  event_type: agent.spawned
       generation: 1
       payload: { agent_id: "<aid>", agent_type: "explore", task_id: "<tid>" }

seq=4  event_type: agent.started
       generation: 1
       payload: { agent_id: "<aid>", task_id: "<tid>" }

seq=5  event_type: agent.completed
       generation: 1
       payload: { agent_id: "<aid>", task_id: "<tid>",
                  exit_reason: "completed" }
```

### 期望 snapshot/projection

```
agent "<aid>":
  status: "stopped"
  exit_reason: "completed"
```

### 期望 DB 断言

```sql
-- agent_logs 有对应记录（audit log）
SELECT event_type FROM agent_logs WHERE agent_id = '<aid>' ORDER BY timestamp;
-- → 'spawned', 'started', 'completed'
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Agent 启动失败 | agent.start 超时 | agent.failed + task 回 dispatchable |
| Agent 超时 | runtime guard 触发 | agent.failed(exit_reason=timeout) |

### 建议 e2e 测试形态

**Rust integration test** — `lingxiao-core::agent` 模块内 `AgentPool::handle_agent_lifecycle()` 测试。**Daemon protocol test** — 通过协议发 spawn/complete。

---

## GS-009：Agent Crash→Recovery

- **Phase**: 3
- **优先级**: P0
- **核心域**: Agent/Leader
- **覆盖**: agent crash, supervisor respawn

### 初始状态

- Agent 正在 running，task 分配中

### 操作序列

```
1. (模拟 agent 崩溃)
   event_internal: agent.crashed
   params: { agent_id: "<aid>", exit_reason: "crashed", error: "OOM" }

2. (Supervisor 判断可恢复)
   command: agent.respawn
   params: { agent_id: "<aid>", new_generation: true }

3. (重启后)
   event_internal: agent.started (new generation)
```

### 期望 canonical events

```
seq=4  event_type: agent.crashed
       generation: 1
       payload: { agent_id: "<aid>", exit_reason: "crashed", error: "OOM" }

seq=5  event_type: agent.spawned     ← respawn
       generation: 1
       payload: { agent_id: "<aid>", agent_generation: 2, task_id: "<tid>" }

seq=6  event_type: agent.started
       generation: 1
       payload: { agent_id: "<aid>", agent_generation: 2 }
```

### 期望 snapshot/projection

```
agent "<aid>":
  status: "starting"  (respawned)
  agent_generation: 2
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| 重试耗尽无恢复 | crash 3 次连续 | supervisor.given_up → agent.terminated |
| Crash 后 task 未完成 | 同 task 重新分配 | task run_generation bump |

### 建议 e2e 测试形态

**Rust integration test** — 模拟 crash→respawn 循环。**Daemon protocol test** — supervisor policy 测试。

---

## GS-010：Leader 拆解任务→Agent 分配

- **Phase**: 3
- **优先级**: P0
- **核心域**: Agent/Leader
- **覆盖**: leader orchestration, task decomposition

### 初始状态

- Session active，用户输入到达，Leader think 完成
- Leader 决定拆解为 2 个子任务

### 命令序列

```
1. (Leader 创建子任务 1)
   command: task.create
   params: { session_id: "<sid>", subject: "搜索代码", agent_type: "explore" }

2. (Leader 创建子任务 2)
   command: task.create
   params: { session_id: "<sid>", subject: "分析结果", agent_type: "analyze",
             blocked_by: ["<tid1>"] }

3. task.assign → agent spawn → task.complete (子任务 1)

4. (子任务 1 完成 → 子任务 2 自动 unblock)
   task.assign → agent spawn → task.complete (子任务 2)

5. (Leader 判断全部完成)
   command: session.complete
```

### 期望 canonical events

```
seq=3  task.created        (tid1, dispatchable)
seq=4  task.created        (tid2, dispatchable, blocked_by: [tid1])
seq=5  task.assigned       (tid1, generation=1)
seq=6  agent.spawned       (aid1, task_id=tid1)
seq=7  agent.completed     (aid1, completed)
seq=8  task.completed      (tid1, completed)
seq=9  task.assigned       (tid2, generation=1)  ← unblocked
seq=10 agent.spawned       (aid2, task_id=tid2)
seq=11 agent.completed     (aid2, completed)
seq=12 task.completed      (tid2, completed)
seq=13 session.completed
```

### 期望 snapshot/projection

```
session status: "completed"
task tid1: terminal(completed)
task tid2: terminal(completed)
```

### 期望 DB 断言

```sql
SELECT COUNT(*) FROM tasks WHERE session_id = '<sid>';
-- → 2
SELECT status, exit_reason FROM tasks WHERE id = '<tid2>';
-- → 'terminal', 'completed'
```

### 建议 e2e 测试形态

**Rust integration test** — `lingxiao-core::leader` 模块中 LeaderOrchestrator 编排场景。**Daemon protocol test** — 使用 mock agent 响应。

---

## GS-011：Permission 请求→解析→恢复执行

- **Phase**: 2
- **优先级**: P0
- **核心域**: Permission
- **覆盖**: permission request/resolve, permission resume

### 初始状态

- Session active，Leader 正在 tool_executing
- Mode: strict

### 命令序列

```
1. (Leader 尝试调用危险工具 shell)
   command: tool.call
   params: { session_id: "<sid>", tool_name: "shell",
             args: { command: "rm -rf /" } }

2. (Core 拦截，发出 permission request)
   client 收到 permission.request_created

3. (Client resolve)
   command: permission.resolve
   params: { session_id: "<sid>", request_id: "<prid>",
             action: "approve", scope: { allowed_once: true } }

4. (Permission resolved → Leader 恢复)
   tool.call 继续执行
```

### 期望 canonical events

```
seq=5  event_type: permission.request_created
       generation: 1
       payload: { request_id: "<prid>", tool_name: "shell",
                  args: { command: "rm -rf /" }, mode: "strict" }

seq=6  event_type: permission.request_resolved
       generation: 1
       payload: { request_id: "<prid>", action: "approve",
                  scope: { allowed_once: true }, resolved_by: "user" }

seq=7  event_type: tool.call_initiated
       generation: 1
       payload: { tool_name: "shell", request_id: "<prid>" }
```

### 期望 snapshot/projection

```
permission request "<prid>":
  status: "resolved"
  action: "approve"
```

### 期望 DB 断言

```sql
-- permission audit trail（持久化在 permission 或 event_log 表）
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Permission 被拒绝 | resolve action="reject" | tool.call 不执行 + agent 继续 |
| 超时未 resolve | 30s 后 tool 等待超时 | permission.request_expired + tool.call_cancelled |
| 多 permission queue | 2 个危险工具连续触发 | 按序发出 request，逐个 resolve |

### 建议 e2e 测试形态

**Rust integration test** — Session + Permission state machine 联调。**Daemon protocol test** — 通过 protocol 发送 resolve 命令。

---

## GS-012：Mode Change 后旧 Grant 失效

- **Phase**: 2
- **优先级**: P0
- **核心域**: Permission
- **覆盖**: mode change, grant revocation

### 初始状态

- Mode: dev（允许文件写工具，无需确认）
- Grant: file_write 允许（无限制）

### 命令序列

```
1. command: permission.set_mode
   params: { session_id: "<sid>", mode: "strict" }

2. (Leader 尝试 file_write)
   command: tool.call
   params: { tool_name: "file_write", args: { path: "/tmp/test.txt" } }
```

### 期望行为

```
seq=4  event_type: permission.mode_changed
       generation: 1
       payload: { old_mode: "dev", new_mode: "strict" }

seq=5  event_type: permission.grant_revoked
       generation: 1
       payload: { tool_name: "file_write", reason: "mode_changed" }

seq=6  event_type: permission.request_created  ← 即使工具之前在 dev 模式下允许
       payload: { tool_name: "file_write", mode: "strict" }
```

### 期望 snapshot/projection

```
mode: "strict"
active_grants: []   ← 已清空
```

### 建议 e2e 测试形态

**Rust integration test** — `PermissionEngine::set_mode()` → 验证 grant 清空。**Daemon protocol test** — 切换 mode 后重复工具调用。

---

## GS-013：Permission Resume 跨 Crash

- **Phase**: 2
- **优先级**: P0
- **核心域**: Permission
- **覆盖**: permission resume 跨进程

### 初始状态

- Permission request 已发出，等待用户 resolve（seq=5）
- Core crash

### 操作序列

```
1. kill core
2. restart core (same DB)
3. client reconnect + 检测到 pending permission request
4. command: permission.resolve
   params: { request_id: "<prid>", action: "approve" }
```

### 期望行为

```
-- Reconnect 后 snapshot 包含 pending permission requests
persistence_snapshot:
  pending_requests: [
    { request_id: "<prid>", tool_name: "shell",
      created_at: "...", status: "pending" }
  ]

-- Resolve 后
seq=7 (post-crash seq 继续)
  permission.request_resolved
```

### 期望 DB 断言

```sql
-- permission request 在 crash 后仍存在（持久化状态）
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Crash 后 session terminal | 用户通过其他 client 删除 session | resolve 返回 SessionNotFound |
| 双重 resolve | crash 前 client 已发出但未收到 ack | idempotency key 防重 |

### 建议 e2e 测试形态

**Daemon protocol test** — kill → restart → resolve。

---

## GS-014：Workflow 串行/并行节点执行

- **Phase**: 3
- **优先级**: P1
- **核心域**: Workflow

### 初始状态

- Session active
- Workflow 模板已加载（3 个节点：A→B→C，其中 B 和 C 可并行）

### 命令序列

```
1. command: workflow.execute
   params: { session_id: "<sid>", workflow_id: "<wfid>",
             input: { data: "..." } }

2. (节点 A 完成)
   event_internal: workflow.node_completed
   params: { execution_id: "<weid>", node_id: "A", output: "..." }

3. (节点 B、C 同时启动)
   event_internal: workflow.node_completed (B)
   event_internal: workflow.node_completed (C)

4. (Workflow 完成)
```

### 期望 canonical events

```
seq=5  workflow.execution_started     status="running"
seq=6  workflow.node_started    node_id="A"
seq=7  workflow.node_completed  node_id="A"
seq=8  workflow.node_started    node_id="B"
seq=9  workflow.node_started    node_id="C"
seq=10 workflow.node_completed  node_id="B"
seq=11 workflow.node_completed  node_id="C"
seq=12 workflow.execution_completed  status="completed"
```

### 期望 snapshot/projection

```
execution "<weid>":
  status: "completed"
  nodes:
    A: completed
    B: completed
    C: completed
```

### 期望 DB 断言

```sql
SELECT status FROM workflow_executions WHERE id = '<weid>';
-- → 'completed'

SELECT status FROM workflow_execution_logs WHERE execution_id = '<weid>';
-- → 每个 node 有对应的 log entry
```

### 建议 e2e 测试形态

**Rust integration test** — `WorkflowEngine::execute()` + mock node handlers。**Daemon protocol test** — 通过 protocol 创建简单 workflow 并执行。

---

## GS-015：Workflow Pause/Resume/Cancel

- **Phase**: 3
- **优先级**: P1
- **核心域**: Workflow

### 初始状态

- Workflow execution 正在 running，节点 B 执行中

### 命令序列

```
1. command: workflow.pause
   params: { execution_id: "<weid>" }

2. (正在执行的节点 B 收到 cancel signal)

3. command: workflow.resume
   params: { execution_id: "<weid>" }

4. (节点 B 重新执行或跳过 → 继续)
```

### 期望 canonical events

```
seq=9  workflow.execution_paused     status="paused"
seq=10 workflow.node_cancelled  node_id="B", reason="paused"
seq=11 workflow.execution_resumed    status="running"
seq=12 workflow.node_started    node_id="B" (restart from paused state)
seq=13 workflow.execution_completed
```

### 期望 snapshot/projection

```
execution status: completed
node B: paused → running → completed
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Cancel 代替 pause | workflow.cancel | execution.status='cancelled' (不是 'failed') |
| Pause 后恢复不重跑已完成节点 | node B 原已部分 progress | 从 checkpoint 继续 |

### 建议 e2e 测试形态

**Rust integration test** — pause→resume→complete cycle。**Daemon protocol test** — pause/resume/cancel 命令验证。

---

## GS-016：Workflow Crash 后恢复

- **Phase**: 4
- **优先级**: P1
- **核心域**: Workflow
- **覆盖**: crash recovery, durable per-node progress

### 初始状态

- Workflow execution running，节点 A completed，节点 B 执行中
- DB 已记录 node A completed

### 操作序列

```
1. kill core
2. restart core
3. command: workflow.list
   params: { session_id: "<sid>" }
```

### 期望行为

```
-- Replay event log 重建 workflow state
execution "<weid>":
  status: "running" (not stuck)
  nodes:
    A: completed   ← 已持久化
    B: running     ← 从 per-node progress 恢复，或标记 failed
```

### 期望 DB 断言

```sql
SELECT status FROM workflow_executions WHERE id = '<weid>';
-- → 'running' (不是 'completed' 或凭空消失)

-- 如果 per-node progress 已持久化
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Crash 在 node 写入 event 后 | crash 前已写 node_completed | 恢复后 node 正确 completed |
| 无 per-node progress | 旧 TS 行为 | Rust Core 必须补 per-node durable progress |

### 建议 e2e 测试形态

**Rust integration test** — 构造 events → 重建 workflow state → 验证 node 状态。**Daemon protocol test** — kill/restart/resume。

---

## GS-017：Rust-native Tool 调用

- **Phase**: 3
- **优先级**: P1
- **核心域**: Tool/Sidecar

### 初始状态

- Session active, Leader 决定调用 `file_read`

### 命令序列

```
1. command: tool.call
   params: { session_id: "<sid>", tool_name: "file_read",
             args: { path: "/tmp/test.txt" }, timeout_ms: 5000 }

2. (Tool 执行完成)
   event_internal: tool.completed
   params: { tool_call_id: "<tcid>", result: "file content..." }
```

### 期望 canonical events

```
seq=4  event_type: tool.call_initiated
       generation: 1
       payload: { tool_name: "file_read", args: { path: "/tmp/test.txt" } }

seq=5  event_type: tool.call_completed
       generation: 1
       payload: { tool_name: "file_read", result: "file content..." }
```

### 期望 snapshot/projection

```
tool_call "<tcid>":
  status: "completed"
  result: "file content..."
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Tool not found | tool_name 不存在 | 错误码：`ToolNotFound` |
| Tool 异常 | file_read 目标文件不存在 | tool.call_failed + error 信息 |
| Tool timeout | 超时不返回 | tool.call_timeout |

### 建议 e2e 测试形态

**Rust integration test** — `ToolRegistry::invoke("file_read")`。**Daemon protocol test** — 通过 protocol 调用 native tool。

---

## GS-018：Sidecar Tool Timeout/Cancel

- **Phase**: 3
- **优先级**: P1
- **核心域**: Tool/Sidecar

### 初始状态

- Sidecar browser tool 已注册

### 命令序列

```
1. command: tool.call
   params: { session_id: "<sid>", tool_name: "browser_action",
             args: { action: "navigate", url: "..." },
             timeout_ms: 100 }

2. (Sidecar 进程启动)
   event_internal: resource.sidecar_started

3. (100ms 后超时)
   event_internal: resource.sidecar_timeout

4. (可选：用户主动 cancel)
   command: tool.cancel
   params: { tool_call_id: "<tcid>" }
```

### 期望 canonical events

最终状态（timeout 路径）：
```
seq=5  resource.sidecar_started
seq=6  tool.call_timeout
seq=7  resource.sidecar_cancelled  (core 发送 cancel → sidecar ack)
```

或（cancel 路径）：
```
seq=5  tool.call_cancelled
seq=6  resource.sidecar_cancelled
```

### 期望 snapshot/projection

```
tool_call "<tcid>":
  status: "timeout" or "cancelled"
```

### 期望 DB 断言

```sql
-- sidecar 不写入核心 DB
-- event log 记录了 timeout/cancel
SELECT event_type FROM event_log WHERE session_id = '<sid>' AND seq >= 5;
-- timeout 路径 → resource.sidecar_started, tool.call_timeout, resource.sidecar_cancelled
-- cancel 路径 → tool.call_cancelled, resource.sidecar_cancelled
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Sidecar 退出码非零 | 进程 crash | resource.sidecar_failed + tool.call_failed |
| Sidecar 返回超时后结果 | 超时后 sidecar 才返回 | result 被拒绝（tool 已 terminal），不产生 sidecar_completed |

### 建议 e2e 测试形态

**Sidecar fake test** — 启动一个模拟 sidecar 进程，通过 sidecar protocol 测试 timeout/cancel 信号发送。

---

## GS-019：LLM Stream Text/Thinking/Tool Call

- **Phase**: 3
- **优先级**: P1
- **核心域**: LLM stream

### 初始状态

- Mock LLM provider 已注册
- Session active, Leader 发出 LLM 请求

### 命令序列

```
1. (Leader 调用 LLM)
   command: llm.call
   params: { session_id: "<sid>", messages: [...], model: "mock/model" }

2. (Mock provider 逐 chunk 返回；以下为 provider raw chunk，不是 event_type)
   provider_chunk.kind: thinking_delta  (thinking content)
   provider_chunk.kind: text_delta       (text content)
   provider_chunk.kind: tool_call_delta  (tool call partial)
   provider_chunk.kind: tool_call_delta  (tool call complete)
   provider_chunk.kind: usage            (final usage)
   provider_chunk.kind: stop             (finish_reason="tool_calls")

3. (LLM 返回 tool_call → 工具执行)
   event_internal: tool.call_initiated
   event_internal: tool.call_completed

4. (LLM 完成)
   event_internal: llm.call_finished
```

### Canonical event_log events（仅 durable；delta 不在此表）

```
seq=5  event_type: llm.call_started
       generation: 1
       payload: { model: "mock/model", messages_count: 5 }

seq=6  event_type: tool.call_initiated
       generation: 1
       payload: { tool_name: "file_read", args: { path: "/tmp/test.txt" } }

seq=7  event_type: tool.call_completed
       generation: 1
       payload: { tool_name: "file_read", result: "file content..." }

seq=8  event_type: llm.call_finished
       generation: 1
       payload: { usage: { prompt_tokens: 100, completion_tokens: 50 },
                  finish_reason: "tool_calls" }
```

### 期望 realtime stream events（非 durable，通过 realtime channel 推送）

```
realtime.llm.thinking_delta  (多次)
  payload: { delta: "..." }

realtime.llm.text_delta      (多次)
  payload: { delta: "..." }

realtime.llm.tool_call_delta (多次)
  payload: { tool_call_id: "...", name: "file_read", args_delta: "..." }
```

Realtime delta 事件**不占 durable seq**，不写入 event_log。通过 fake subscriber 捕获并断言其顺序和内容。

### 期望 snapshot/projection

```
latest_llm_call:
  model: "mock/model"
  finish_reason: "tool_calls"
  usage: { prompt_tokens: 100, completion_tokens: 50 }
```

### 期望 DB 断言

```sql
-- event_log 只包含 durable events（无 delta）
SELECT event_type FROM event_log WHERE session_id = '<sid>' AND seq >= 5;
-- → llm.call_started, tool.call_initiated, tool.call_completed, llm.call_finished
```

### 期望 realtime transcript 断言

```rust
// 通过 MockRealtimeSubscriber 捕获
let transcript = subscriber.drain();
assert_eq!(transcript[0].event_type, "realtime.llm.thinking_delta");
assert_eq!(transcript[1].event_type, "realtime.llm.text_delta");
assert_eq!(transcript[2].event_type, "realtime.llm.tool_call_delta");
assert_eq!(transcript[3].event_type, "realtime.llm.tool_call_delta");
// 无 seq 字段——realtime delta 不分配 durable seq
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Provider error | mock provider 返回 500 | llm.call_finished + finish_reason="error"；无 tool.call_initiated |
| Empty response | mock provider 返回空 | retry 机制触发；无 realtime delta 事件 |

### 建议 e2e 测试形态

**Rust integration test** — `MockLlmProvider` + `LlmRouter::stream_call()` + `MockRealtimeSubscriber` 验证 stream events 顺序。**Daemon protocol test** — mock provider response 验证 durable + realtime 分离。

---

## GS-020：有序 Event Replay

- **Phase**: 1
- **优先级**: P0
- **核心域**: Event log replay

### 初始状态

- Session 已有 10 个事件（seq=1..10）

### 命令序列

```
1. command: event.replay
   params: { session_id: "<sid>", from_seq: 5, to_seq: nil (latest) }
```

### 期望行为

```
response:
  events: [
    { seq: 6, event_type: "session.input_received", ... },
    { seq: 7, event_type: "agent.spawned", ... },
    ...
    { seq: 10, event_type: "agent.completed", ... }
  ]
  has_more: false
```

### 期望 DB 断言

```sql
-- 从 event_log 表查询 seq>5 全部事件
-- 确保 seq 严格递增、无 gap
SELECT seq FROM event_log WHERE session_id = '<sid>' AND seq > 5 ORDER BY seq;
-- → 6, 7, 8, 9, 10
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Empty range | from_seq=10 (latest) | 返回空 events, has_more=false |
| Paginated | limit=2 | 返回 seq=6,7 + cursor for next |
| Cross-session | 2 个 session 同时 replay | 各自有序，互不影响 |

### 建议 e2e 测试形态

**Rust integration test** — `EventLog::replay(session_id, from_seq, limit)`。**Daemon protocol test** — 通过 protocol 验证 replay。

---

## GS-021：Generation Gap 检测与拒绝

- **Phase**: 1
- **优先级**: P0
- **核心域**: Event log replay

### 初始状态

- Session current generation=2, seq=15

### 命令序列

```
1. (模拟旧 generation 事件到达)
   command: session.input  (carrying generation=1, via worker late result)
```

### 期望行为

```
事件写入 event_log（可审计，seq=16），但：
  - state transition 拒绝
  - session.status 保持当前值
  - generation 不降级
```

### 期望 DB 断言

```sql
-- event log 中包含 seq=16（旧事件已记录）
SELECT seq, generation FROM event_log WHERE session_id = '<sid>' ORDER BY seq DESC LIMIT 1;
-- → 16, 1

-- 但 session 状态不变
SELECT generation FROM sessions WHERE id = '<sid>';
-- → 2
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Terminal session + 旧事件 | session 已 terminal | 所有旧事件被拒绝 |
| 相同 generation | generation 匹配 | 正常处理 |

### 建议 e2e 测试形态

**Rust integration test** — `EventLog::append_with_generation_check()` 测试。

---

## GS-022：Cursor Reconnect Delta

- **Phase**: 1
- **优先级**: P0
- **核心域**: Snapshot/delta reconnect

### 初始状态

- Client 已断开，last_known_seq=5
- Core 继续产生了 seq=6,7,8

### 命令序列

```
1. (Client reconnect)
   command: session.connect
   params: { session_id: "<sid>", cursor: { last_known_seq: 5 } }
```

### 期望行为

```
response:
  delta_type: "events"
  events: [
    { seq: 6, ... },
    { seq: 7, ... },
    { seq: 8, ... }
  ]
  snapshot: (不返回，client 已通过 seq 1..5 有完整状态)
```

### 期望 snapshot/projection

```
client 应用 seq=6,7,8 后状态与 core 完全一致
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Cursor 在 compaction 后 | seq≤3 已 compaction | delta_type: "snapshot_required" |
| 新 session | 无 cursor | 返回全 snapshot + 实时事件 |
| Cursor seq > latest | cursor=99, latest=10 | 返回 snapshot_required |

### 建议 e2e 测试形态

**Daemon protocol test** — 模拟 disconnect/reconnect 并验证 delta 或 snapshot_required 信号。**Rust integration test** — `Projection::connect(cursor)`。

---

## GS-023：Gap 太大触发 Snapshot

- **Phase**: 1
- **优先级**: P0
- **核心域**: Snapshot/delta reconnect

### 初始状态

- MAX_REPLAY_EVENTS=100
- Client last_known_seq=50，但当前 latest_seq=200（gap=150 > 100）

### 命令序列

```
1. command: session.connect
   params: { session_id: "<sid>", cursor: { last_known_seq: 50 } }
```

### 期望行为

```
response:
  delta_type: "snapshot_required"
  message: "Gap too large (150 > 100). Request full snapshot."

2. (Client 请求 snapshot)
   command: session.snapshot
   params: { session_id: "<sid>" }

   response:
     snapshot: { session_id, generation, status, tasks, ... }
     snapshot_seq: 200
```

### 期望 snapshot/projection

```
snapshot:
  session_id: "<sid>"
  snapshot_seq: 200
  generation: 2
  status: "active"
```

### 建议 e2e 测试形态

**Daemon protocol test** — 产生大量事件 → 断开 → 用旧 cursor 重连 → 验证 `snapshot_required`。

---

## GS-024：SQLite Schema Parity

- **Phase**: 1
- **优先级**: P0
- **核心域**: SQLite persistence

### 初始状态

- Rust Core 尚未初始化（空 SQLite DB）

### 操作序列

```
1. core start → 自动执行 DDL migration
2. 查询 schema 信息
```

### 期望行为

```sql
-- 所有 P0 表存在
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
-- → sessions, tasks, messages, leader_conversation,
--   event_log / event_log_meta / command_dedupe (P1 Rust Core 新增)
--   permission_modes / permission_requests / permission_grants (permission canonical state)

-- 每个表的列与 TS 版本一致
PRAGMA table_info(sessions);
-- → id TEXT PK, created_at REAL, workspace TEXT, status TEXT, ...

-- SCHEMA_VERSION
PRAGMA user_version;
-- → 15

-- PRAGMA 配置
PRAGMA journal_mode;
-- → wal
PRAGMA foreign_keys;
-- → 1
```

### 期望 DB 断言

- 所有 P0 表存在且字段与 TS `Database.ts` 一致
- 索引名、列、partial WHERE 一致
- `memory_embedding` 表已显式定义（TS 端缺失的补充）
- `command_dedupe` 表已显式定义，使用 `(idempotency_key, method)` 复合主键，支撑 command router 防重
- `permission_modes` / `permission_requests` / `permission_grants` 已显式定义，支撑 permission request/resume 与 mode-change grant revocation

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| 旧 DB 文件已存在 | user_version=15 | 复用，不重建 |
| 旧 DB 版本不匹配 | user_version=14 | 备份旧库(`.replaced-*`)，重建 schema |

### 建议 e2e 测试形态

**Rust integration test** — 启动 core 后执行 PRAGMA 查询验证。**Schema diff 脚本** — 对比 TS `Database.ts` DDL 与 Rust migration 输出。

---

## GS-025：Core 唯一 SQLite Writer

- **Phase**: 1
- **优先级**: P0
- **核心域**: SQLite persistence

### 初始状态

- Rust Core 运行中，SQLite 连接已建立

### 操作序列

```
1. (模拟 sidecar/TUI 尝试写入)
   external_conn 尝试打开同一 DB 并写操作
```

### 期望行为

```sql
-- Core 使用 PRAGMA busy_timeout=30000 + BEGIN IMMEDIATE
-- 外部写入被 WAL 锁阻止或排队
-- Core 仍是唯一语义写入者（sidecar 不写 DB 是 contract 要求）
```

### 期望 DB 断言

```sql
-- 检查 core 连接是独占 writer（非 literal SQLite 独占锁，而是通过 contract + busy_timeout 实现）
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Core 用 `BEGIN IMMEDIATE` | 并发写入尝试 | SQLITE_BUSY 返回给外部，core 不受影响 |

### 建议 e2e 测试形态

**Rust integration test** — 在 core 事务进行时尝试第二个连接写入，验证 `SQLITE_BUSY` 或等待行为。

---

## GS-026：Kill→Restart→Resume Session

- **Phase**: 4
- **优先级**: P0
- **核心域**: Recovery/crash
- **覆盖**: crash recovery, session resume

### 初始状态

- Session 已创建，用户已发送 3 条消息，seq=1..6

### 操作序列

```
1. kill core (force)
2. restart core (同一 DB 路径)
3. command: session.list

4. command: session.connect
   params: { session_id: "<sid>", cursor: { last_known_seq: 6 } }

5. command: session.input
   params: { session_id: "<sid>", content: "继续之前的工作" }
```

### 期望行为

```
Step 3 response:
  sessions: [{ id: "<sid>", status: "active", generation: 1, last_seq: 6 }]

-- Replay event log seq=1..6 重建 state
-- session 状态与 crash 前一致

Step 5 response:
  seq=7 session.input_received (generation=1, 或 bump generation)
```

### 期望 DB 断言

```sql
SELECT status, generation FROM sessions WHERE id = '<sid>';
-- → 'active', 1

SELECT COUNT(*) FROM event_log WHERE session_id = '<sid>';
-- → 6+
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Crash 期间用户用其他 client 操作 | 新 DB 文件 | 不适用（单 DB） |
| 多次 crash | 反复 kill/restart | 每次重启从 event log 重建，最终状态一致 |

### 建议 e2e 测试形态

**Daemon protocol test** — 发送命令 → kill daemon → 重启 → 验证 session 存活 → 发新命令。**Rust integration test** — drop + reopen DB + replay。

---

## GS-027：Context 不因 Crash 丢失

- **Phase**: 4
- **优先级**: P0
- **核心域**: Recovery/crash
- **覆盖**: conversation context persistence

### 初始状态

- Session active，已产生对话：用户 2 条消息，agent 2 条回复
- Conversation 已持久化到 `leader_conversation`

### 操作序列

```
1. kill core
2. restart core
3. command: conv.list
   params: { session_id: "<sid>" }
```

### 期望行为

```
response:
  messages: [
    { role: "user", content: "消息 1" },
    { role: "assistant", content: "回复 1" },
    { role: "user", content: "消息 2" },
    { role: "assistant", content: "回复 2" }
  ]
```

### 期望 DB 断言

```sql
SELECT COUNT(*) FROM leader_conversation WHERE session_id = '<sid>';
-- → 4
SELECT content FROM leader_conversation WHERE session_id = '<sid>' ORDER BY timestamp;
-- 逐条 match
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Crash 在 conversation 写入一半时 | WAL 事务保护 | 回滚到上一个完整 checkpoint |
| Crash 后 compaction 已执行 | compaction 删除原始 event | conversation 表不受 compaction 影响 |

### 建议 e2e 测试形态

**Daemon protocol test** — 发送消息 → kill → restart → 读取 conversation 历史。

---

## GS-028：Workflow Execution 不 Stuck Running

- **Phase**: 4
- **优先级**: P0
- **核心域**: Recovery/crash
- **覆盖**: workflow stuck running

### 初始状态

- Workflow execution 正在 running，节点 B 执行中（seq=10）
- Core crash

### 操作序列

```
1. kill core
2. restart core
3. command: workflow.list
   params: { session_id: "<sid>" }
```

### 期望行为

```
-- Event log 中最后一个 workflow 事件是 workflow.node_started (B)
-- 不是 terminal
-- 但 Rust Core 的 per-node durable progress 可恢复或标记
execution "<weid>":
  status: "running" (从 event log 重建)
  -- 或者：status: "failed" (如果 recovery policy 判定不可继续)

-- 不应出现：
   status: "running" (permanent stuck)
```

### 期望 DB 断言

```sql
SELECT status FROM workflow_executions WHERE id = '<weid>';
-- → 'running' (可恢复) 或 'failed' (如果有 recovery timeout)
-- 绝对不应是 'completed' (假完成) 或 'running' (无法恢复)
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Recovery timeout | crash 后 5 分钟无人恢复 | workflow 自动标记 failed |
| 节点有 checkpoint | B 有 per-node progress | 从 B 的 progress 点恢复 |

### 建议 e2e 测试形态

**Daemon protocol test** — 启动 workflow → crash → restart → 验证 execution 状态。

---

## GS-029：Token Budget 超限→Compaction

- **Phase**: 4
- **优先级**: P1
- **核心域**: Resource budget

### 初始状态

- Token budget: max_tokens=100000
- 当前 usage=95000（接近超限）

### 命令序列

```
1. (LLM 请求即将超 budget)
   event_internal: resource.budget_exceeded
   params: { session_id: "<sid>", current_usage: 95000, limit: 100000,
             next_estimated_cost: 8000 }

2. (Core 自动触发 context compaction)
   event_internal: persistence.compaction_started

3. (Compaction 完成)
   event_internal: persistence.compaction_completed
   params: { tokens_before: 95000, tokens_after: 40000 }
```

### 期望 canonical events

```
seq=12 resource.budget_exceeded
seq=13 persistence.compaction_started
seq=14 persistence.compaction_completed  (usage reduced)
```

### 期望 snapshot/projection

```
token_usage:
  before_compaction: 95000
  after_compaction: 40000
  budget_remaining: 60000
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Compaction 后还不够 | 仍超 budget | 触发 session interruption + 通知用户 |
| Budget 配置变更 | 动态调整 max_tokens | 下次 budget 检查使用新值 |

### 建议 e2e 测试形态

**Rust integration test** — `ResourceBudget::check()` → compaction trigger。**Daemon protocol test** — mock LLM 不断耗 token 直到触发 compaction。

---

## GS-030：Sidecar 资源会计

- **Phase**: 4
- **优先级**: P1
- **核心域**: Resource budget

### 初始状态

- Resource budget: max_sidecars=3, max_memory_mb=500
- 已有 2 个 sidecar 运行中（memory=200MB）

### 命令序列

```
1. command: tool.call (browser action - sidecar)
   params: { tool_name: "browser_action", args: {...} }

2. (Resource check: 1 more sidecar OK, 但 memory 可能超 limit)

3. event_internal: resource.sidecar_started  (if approved)

4. 或 event_internal: resource.budget_exceeded  (if rejected)
```

### 期望行为

```
-- 场景 A：资源足够
seq=10 resource.sidecar_started  (第 3 个 sidecar)

-- 场景 B：资源不足
seq=10 resource.budget_exceeded  (sidecar 启动被拒绝)
tool.call_failed, reason: "Resource budget exceeded"
```

### 期望 snapshot/projection

```
active_sidecars: 3 (或 2，如果被拒)
memory_usage_mb: 200 + 150 = 350 (或 200)
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Sidecar 释放后 budget 恢复 | sidecar completed | resource.sidecar_completed, budget 恢复 |
| 超限后 sidecar 被 kill | budget exceeded during execution | resource.sidecar_cancelled with reason |

### 建议 e2e 测试形态

**Sidecar fake test** — 启动多个 fake sidecar 直到触发 budget limit。**Rust integration test** — `ResourceBudget::try_acquire_sidecar()`。

---

## GS-031：MessageBus 优先级投递与 Backpressure

- **Phase**: 3
- **优先级**: P1
- **核心域**: MessageBus/backpressure

### 初始状态

- MessageBus 已初始化
- 有界 channel capacity=10
- 3 个 agent inbox 在监听

### 命令序列

```
1. command: bus.send
   params: { from: "leader", to: "agent-1",
             priority: "P0", content: "紧急：立即停止当前操作" }

2. command: bus.send
   params: { from: "leader", to: "agent-1",
             priority: "P2", content: "完成后更新日志" }

3. (Agent-1 开始按优先级消费)
   P0 消息先到 → P2 消息后到
```

### 期望行为

```
-- P0 消息先投递
event: message_bus.delivered  { priority: "P0", to: "agent-1" }

-- P2 消息后投递
event: message_bus.delivered  { priority: "P2", to: "agent-1" }
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Channel full (backpressure) | capacity=10, 10 条消息未消费 | 第 11 条 send 被阻塞或返回 backpressure 错误 |
| Dead-letter | 发送给不存在的 agent | bus.dead_letter event |

### 建议 e2e 测试形态

**Rust unit test** — `MessageBus::send_with_backpressure()` + 慢消费者测试 backpressure。

---

## GS-032：MessageBus Dead-letter 处理

- **Phase**: 3
- **优先级**: P1
- **核心域**: MessageBus/backpressure

### 初始状态

- Agent "explore-1" 已销毁（session 结束）

### 命令序列

```
1. command: bus.send
   params: { from: "leader", to: "explore-1", priority: "P1",
             content: "继续搜索" }
```

### 期望行为

```
-- Agent 不存在，消息无法投递
event: message_bus.dead_letter
  payload: { from: "leader", to: "explore-1",
             priority: "P1", reason: "recipient_not_found",
             message_id: "<mid>" }
```

### 期望 snapshot/projection

```
dead_letter_count: 1
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Inbox full → dead-letter | 收件人 inbox 满 + TTL 超时 | dead_letter with reason="inbox_full" |
| 重试后仍然是 dead-letter | 投递重试 3 次失败 | dead_letter with retry_count=3 |

### 建议 e2e 测试形态

**Rust unit test** — `MessageBus::send_to_nonexistent()` → verify dead_letter。**Daemon protocol test** — 销毁 agent 后 send message。

---

## GS-033：消息持久化→Replay 一致

- **Phase**: 3
- **优先级**: P1
- **核心域**: Context/conversation persistence

### 初始状态

- Session active，已产生 2 轮对话（user+assistant）

### 命令序列

```
1. command: conv.list
   params: { session_id: "<sid>", include_tool_calls: true }
```

### 期望行为

```
response:
  messages: [
    { role: "user", content: "请搜索 API 文档", tool_calls: null },
    { role: "assistant", content: "正在搜索...",
      tool_calls: [{ name: "code_search", args: { query: "API" } }] },
    { role: "tool", tool_call_id: "...", content: "搜索结果..." },
    { role: "assistant", content: "找到 5 个相关文件" }
  ]
```

### 期望 DB 断言

```sql
-- conversation 表有完整记录
SELECT role, content, tool_calls FROM leader_conversation
WHERE session_id = '<sid>' ORDER BY timestamp;
-- 匹配上述消息
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Replay 后 message 一致 | crash + restart + conv.list | 数据与 crash 前相同 |
| Tool calls 含完整参数 | tool_call_id 跨 message 关联 | tool_call_id 正确对应 |

### 建议 e2e 测试形态

**Daemon protocol test** — 发送消息 → 验证 conv.list → crash → restart → 再次验证 conv.list 一致。

---

## GS-034：Compaction 不丢原始事实

- **Phase**: 4
- **优先级**: P1
- **核心域**: Context/conversation persistence

### 初始状态

- Session 有 100 条 conversation 消息
- Event log 有 200 个事件

### 命令序列

```
1. command: persistence.compact
   params: { session_id: "<sid>" }
```

### 期望行为

```
event: persistence.compaction_started
event: persistence.compaction_completed
  payload: { seq_truncated_before: 150, snapshot_seq: 150,
             conversation_messages_before: 100, messages_after: 100 }

-- Conversation 消息不被 compaction 删除
-- Event log 中 seq≤150 的可被删除或标记为 compacted
-- Conversation 表保留所有原始消息
```

### 期望 DB 断言

```sql
-- Event log seq 指针已更新
SELECT snapshot_seq FROM event_log_meta WHERE session_id = '<sid>';
-- → 150

-- Conversation 数据不受影响
SELECT COUNT(*) FROM leader_conversation WHERE session_id = '<sid>';
-- → 100 (不变)
```

### 变体

| 变体 | 修改 | 预期 |
|------|------|------|
| Compact 后 replay | 从 seq=50 replay | snapshot_required（因为 seq<150 已 compaction） |
| 只 compact 单 session | 另一 session 不受影响 | 另一 session event log 完整 |

### 建议 e2e 测试形态

**Rust integration test** — `EventLog::compact()` 验证 conversation 表和 snapshot 指针。**Daemon protocol test** — 触发 compaction 后验证数据完整性。

---

## GS-035：Blackboard Intent 声明→解析

- **Phase**: 5
- **优先级**: P2
- **核心域**: Team/blackboard/memory

### 初始状态

- Session active
- Blackboard 模块已初始化

### 命令序列

```
1. command: blackboard.intent.create
   params: { session_id: "<sid>", title: "完成文件搜索",
             description: "在 /tmp 下查找所有 .md 文件",
             priority: 5 }

2. (Agent 领任务)
   command: blackboard.intent.claim
   params: { session_id: "<sid>", intent_id: "<iid>",
             agent_name: "explore-1" }

3. (Agent 完成)
   command: blackboard.intent.resolve
   params: { session_id: "<sid>", intent_id: "<iid>",
             result: "已找到 3 个文件" }
```

### 期望 canonical events

```
seq=3  blackboard.intent_created   status="open"
seq=4  blackboard.intent_claimed   status="claimed", agent="explore-1"
seq=5  blackboard.intent_resolved  status="resolved", result="已找到 3 个文件"
```

### 期望 DB 断言

```sql
SELECT status, title FROM graph_nodes WHERE id = '<iid>' AND session_id = '<sid>';
-- → 'resolved', '完成文件搜索'
```

### 建议 e2e 测试形态

**Rust integration test** — `Blackboard::create_intent() → claim() → resolve()`。

---

## GS-036：Team Mailbox 消息投递

- **Phase**: 5
- **优先级**: P2
- **核心域**: Team/blackboard/memory

### 初始状态

- Team "research" 已创建，含成员 "alice", "bob"

### 命令序列

```
1. command: team.send
   params: { session_id: "<sid>", from: "alice", to_team: "research",
             to_member: "bob", content: "请 review 最新报告",
             urgency: "high" }

2. (Bob 收到消息)
   command: team.mark_read
   params: { session_id: "<sid>", message_id: "<mid>" }
```

### 期望 canonical events

```
seq=3  team.message_sent    status="delivered", urgency="high"
seq=4  team.message_read    status="read"
```

### 期望 DB 断言

```sql
SELECT status FROM team_messages WHERE id = '<mid>';
-- → 'read'
```

### 建议 e2e 测试形态

**Rust integration test** — `TeamMailbox::send() → mark_read()`。**Daemon protocol test** — team message lifecycle。

---

## 报告模板：Scenario 通过/失败核对

每个 scenario 在实现后通过以下命令核对：

```powershell
# 1. Rust integration tests
cargo test --package lingxiao-core --test <scenario_name> -- --nocapture

# 2. Daemon protocol tests
cargo test --package lingxiao-core-daemon --test <protocol_test> -- --nocapture

# 3. DB assertions (manual verification)
# 启动 core，执行 scenario 命令序列
# 然后：
sqlite3 <data_dir>/lingxiao.db "SELECT status, generation FROM sessions;"
sqlite3 <data_dir>/lingxiao.db "SELECT event_type, seq, generation FROM event_log WHERE session_id = '<sid>' ORDER BY seq;"
sqlite3 <data_dir>/lingxiao.db "PRAGMA user_version;"
```

### 通过条件

1. 所有 canonical events 按预期顺序和 payload 生成
2. Snapshot/projection 与期望一致
3. DB 断言全部通过
4. 所有变体测试通过
5. 非法路径返回正确的错误码，不 panic、不数据损坏

### 完成核对命令

```powershell
# 统计完成度
Write-Output "=== Rust Core Golden Scenarios 完成状态 ==="
Write-Output "Total: 36"
Write-Output ""
Write-Output "Session: GS-001..GS-004 (4)"
Write-Output "TaskBoard: GS-005..GS-007 (3)"
Write-Output "Agent/Leader: GS-008..GS-010 (3)"
Write-Output "Permission: GS-011..GS-013 (3)"
Write-Output "Workflow: GS-014..GS-016 (3)"
Write-Output "Tool/Sidecar: GS-017..GS-018 (2)"
Write-Output "LLM stream: GS-019 (1)"
Write-Output "Event log replay: GS-020..GS-021 (2)"
Write-Output "Snapshot/delta reconnect: GS-022..GS-023 (2)"
Write-Output "SQLite persistence: GS-024..GS-025 (2)"
Write-Output "Recovery/crash: GS-026..GS-028 (3)"
Write-Output "Resource budget: GS-029..GS-030 (2)"
Write-Output "MessageBus/backpressure: GS-031..GS-032 (2)"
Write-Output "Context/conversation: GS-033..GS-034 (2)"
Write-Output "Team/blackboard/memory: GS-035..GS-036 (2)"

# 核对每个 scenario 的 e2e test 存在性（待实现后运行）
# cargo test --list --package lingxiao-core 2>$null | Select-String "gs_"
```

---

## 附录：Phase 对应关系

| Phase | Scenarios | 验收入口 |
|-------|-----------|----------|
| Phase 1: Core Skeleton | GS-020~GS-025 | Event log / Snapshot / Schema |
| Phase 2: Core Domain | GS-001~GS-007, GS-011~GS-013 | Session/Task/Permission 状态机 |
| Phase 3: Agent Execution | GS-008~GS-010, GS-014~GS-015, GS-017~GS-019, GS-031~GS-033 | Agent/Tool/LLM/Workflow/MessageBus |
| Phase 4: Hardening | GS-016, GS-026~GS-030, GS-034 | Crash/Recovery/Budget/Compaction |
| Phase 5: Adapter/Ext | GS-035~GS-036 | Blackboard/Team/Memory |
