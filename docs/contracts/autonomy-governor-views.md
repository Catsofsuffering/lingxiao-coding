# Autonomy Governor 视图可视化契约

> 范围：Web UI、TUI、Agent/Task/Permission/Cost/Trace 相关视图。
>
> 目标：确保 Autonomy Governor 不是只存在于 prompt、runtime 或内部状态里，而是在所有相关视图中可见、可解释、可追踪、可验收。

## 1. 背景

Autonomy Governor 解决的是“什么时候自主推进、什么时候停下、什么时候请求授权、什么时候止损”的系统行为问题。若状态只在 runtime 内部存在，用户仍会感到不可控：

- 不知道当前是 `review_first`、`balanced` 还是 `autonomous`；
- 不知道为什么系统没有继续修；
- 不知道为什么 Agent 被暂停；
- 不知道某个任务是 research 还是 implement；
- 不知道某次写入是在哪个 policy generation 下发生的；
- 不知道当前费用估算是否考虑 cache 命中。

因此所有关键状态都必须有 UI 可视化。

## 2. 可视化原则

1. **模式必须显眼但不打扰**
   - 主工作区展示当前 `autonomy_mode` 与 `lifecycle_phase`。
   - 不把模式解释塞满聊天区，除非用户展开查看。

2. **展示 effective policy，而不是只展示模式名**
   - `balanced + bootstrap` 和 `balanced + stable` 行为不同。
   - UI 必须展示当前实际生效行为。

3. **所有 gate / pause / block 都要解释原因**
   - 不只显示“已暂停”，要显示 `reason`、`needed_decision` 和建议下一步。

4. **所有跨端状态必须同步**
   - Web UI 和 TUI 显示同一个 `SessionAutonomyState`。
   - 切换端、接收端、Leader、Agent runtime 使用同一 generation。

5. **所有高风险动作必须可追踪**
   - 哪个 action 被 allow / deny / require_review；
   - 依据哪个 grant；
   - grant 来源是什么；
   - policy generation 是多少。

6. **不要把开发者视图强塞给普通用户**
   - 普通用户看到“当前模式 + 为什么停 + 下一步选项”。
   - 开发者视图可展开看 AutonomyDecision、Grant、GateResult、FailureLedger。

## 3. 必须可视化的数据模型

### 3.1 SessionAutonomyState

必须在 Web/TUI 主状态区可见。

```typescript
interface SessionAutonomyState {
  session_id: string;
  autonomy_mode: "review_first" | "balanced" | "autonomous";
  lifecycle_phase: "bootstrap" | "active" | "recovery" | "stable";
  mode_generation: number;
  effective_policy_id: string;
  effective_policy_hash: string;
  updated_at: number;
  updated_by: "web" | "tui" | "leader" | "runtime_policy";
  reason?: string;
}
```

### 3.2 EffectivePolicySummary

UI 不应直接展示长 prompt，而是展示压缩摘要。

```typescript
interface EffectivePolicySummary {
  can_read: boolean;
  can_research: boolean;
  can_run_low_risk_verify: boolean;
  can_small_write_without_review: boolean;
  can_dispatch_implement: boolean;
  requires_review_for_research_to_implement: boolean;
  requires_review_for_permission_expansion: boolean;
  hard_gates: string[];
  explanation: string;
}
```

示例：

```text
Autonomy: Balanced
Phase: Bootstrap
Effective behavior:
- Read/analyze: auto
- Small edits: only if user explicitly asked to fix/implement
- Research → implementation: review required
- Permission expansion: review required
```

### 3.3 GateResult

所有 high-risk action 的拦截、放行、待审阅都应可视化。

```typescript
type GateResult =
  | { status: "allow"; grant_id?: string }
  | { status: "deny"; reason: string }
  | { status: "require_review"; reason: string; suggested_prompt: string }
  | { status: "require_leader_help"; reason: string };
```

### 3.4 LeaderHelpRequest

Agent loop / 权限失败触发后必须在 Agent/Task 视图展示。

```typescript
interface LeaderHelpRequest {
  type: "leader_help_request";
  agent_id: string;
  task_id: string;
  reason:
    | "write_scope_denied"
    | "tool_not_allowed"
    | "network_denied"
    | "permission_denied"
    | "repeated_command_failure"
    | "no_progress";
  summary: string;
  attempted_actions: string[];
  needed_decision:
    | "expand_write_scope"
    | "change_tool_scope"
    | "request_network"
    | "change_task_scope"
    | "ask_user"
    | "terminate"
    | "retry_with_new_strategy";
}
```

### 3.5 CostBreakdown

费用视图必须能展示 cache-aware breakdown。

```typescript
interface CostBreakdown {
  provider: string;
  model: string;
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  cache_hit_ratio: number;
  input_rate: number;
  cached_input_rate?: number;
  output_rate: number;
  estimated_cost: number;
  rate_source: "provider_api" | "configured" | "default_table" | "unknown";
  confidence: "high" | "medium" | "low";
  note?: string;
}
```

## 4. Web UI 视图矩阵

| 视图 / 区域 | 必须展示 | 交互 | 验收 |
|---|---|---|---|
| Chat 顶部模式栏 / `ModeSplitControls` | `autonomy_mode`, `lifecycle_phase`, `effective behavior` 摘要 | 切换 `review_first` / `balanced` / `autonomous`；展开说明 | Web 切换后 TUI 同步；SSE 回流后状态一致 |
| Chat 消息区 | gate / pause 的用户可读解释 | 用户可点“继续实现 / 只生成方案 / 暂停” | research 请求不应直接出现 implement 任务 |
| Workbench / Agent Panel | running agent 当前 policy generation、是否被 gate 暂停、help request | 允许 Leader 处理 help request | Agent 权限失败后能看到原因和下一步 |
| Task / Orchestration 视图 | task kind、risk tier、grant status、blocked reason | 审阅后允许 dispatch / reject / revise | implement/repair 被 gate 时可见 blocked reason |
| Permission 控制区 | permission mode 与 autonomy mode 的差异 | 提权时展示 hard gate 原因 | `autonomous` 不等于 `yolo` 可被看见 |
| Changes / Diff 视图 | diff 产生时的 policy generation、是否 needs_review | review / continue / revert | 模式切换前的写入被标记为旧 policy 产物 |
| Stats / Usage / Sidebar UsageCard | cache-aware cost breakdown | 展开 input/cached/output 明细 | cached token 被计入 cache hit ratio |
| Logs / Traces / Metrics | AutonomyDecision、GateResult、Grant、LoopBreaker 事件 | 开发者展开审计 | 能追踪为什么 allow/deny/pause |
| Settings / Behavior | 默认 autonomy mode、autonomous 风险说明 | 修改默认值 | 新 session 默认值可配置 |

## 5. TUI 视图矩阵

| 区域 | 必须展示 | 交互 | 验收 |
|---|---|---|---|
| 状态栏 | `autonomy`, `phase`, `control_mode`, `collaboration_mode`, permission mode | 无或快捷切换 | Web 切换后 TUI 状态栏即时更新 |
| 命令输入 | `/autonomy review_first|balanced|autonomous` | 切换模式 | TUI 切换后 Web 同步 |
| Agent 列表 / 任务面板 | Agent paused/gated/help-needed 状态 | 查看 help request，执行 Leader 决策 | 权限 loop 后不再只显示“running” |
| 事件流 | `session:autonomy_mode_changed`, `gate:blocked`, `agent:help_request` | 展开详情 | 能看到 generation、reason、updated_by |
| 权限提示 | permission mode 与 autonomy mode 分开显示 | request_permission_update 或拒绝 | 网络/写入提权不被误解为模式切换 |
| 成本栏 | cache-aware input/cached/output/estimated | 展开详情 | 费用低置信时有明确标注 |

## 6. 事件可视化契约

新增或复用事件必须能在 Web/TUI 订阅并展示。

| 事件 | 来源 | 消费者 | 用途 |
|---|---|---|---|
| `session:autonomy_mode_changed` | SessionManagerRuntime | Web/TUI/Leader/Agent runtime | 模式切换同步 |
| `autonomy:gate_result` | RuntimeGate | Web/TUI/Logs/Task view | 展示 allow/deny/review/help |
| `agent:leader_help_request` | LoopBreaker / Agent runtime | Leader/Web/TUI | Agent loop 止损可视化 |
| `autonomy:grant_issued` | GrantManager | Logs/Task/Agent view | 审计 grant 来源 |
| `autonomy:grant_revoked` | GrantManager | Logs/Task/Agent view | 模式切换或过期后撤销 |
| `autonomy:policy_updated` | PromptSegment composer | Logs/Agent view | 查看 policy generation/hash |
| `billing:cost_breakdown_updated` | Billing tracker | Usage/Stats | cache-aware cost 展示 |

## 7. 交互文案契约

### 7.1 research → implement 被拦截

```text
我已完成分析，但当前请求没有授权我直接修改代码。
建议方案：...
你可以选择：
1. 只保留分析
2. 按方案进入修复
3. 先调整方案
```

### 7.2 Agent 权限 loop 被暂停

```text
Agent 已暂停：连续遇到 write_scope_denied，继续重试不会产生进展。
已尝试：structured_patch src/...
需要决策：扩大 write_scope / 修改任务范围 / 终止任务。
```

### 7.3 autonomous 仍被 hard gate 拦截

```text
当前是 Full Auto，但该操作属于 Hard Gate：git push / destructive / permission expansion。
需要显式授权后才能继续。
```

### 7.4 费用低置信

```text
费用为估算值，不代表 provider 实际账单。当前缺少 cached input rate，因此置信度为 low。
```

## 8. 不同用户层级展示

| 用户层级 | 默认展示 | 可展开 |
|---|---|---|
| 普通用户 | 当前模式、为什么停、下一步按钮 | effective behavior |
| 高级用户 | mode + phase + gate reason + grant status | AutonomyDecision / GateResult |
| 开发者 | 全部事件、policy hash、generation、FailureLedger | 原始 JSON |

## 9. 最小可视化切片

第一版必须完成：

1. Web Chat 顶部显示并切换 `autonomy_mode`。
2. TUI 状态栏显示 `autonomy_mode` / `lifecycle_phase`。
3. Web/TUI 通过同一事件同步。
4. implement/repair 被 gate 时，Task/Chat 至少一处显示 blocked reason。
5. Agent loop 被暂停时，Agent/Task 至少一处显示 `LeaderHelpRequest`。
6. UsageCard 显示 cached input token；没有数据时明确显示 unknown / low confidence。

## 10. 完成定义

视图可视化视为完成时，应满足：

1. 用户不需要读日志就能知道当前自主模式。
2. 用户不需要猜测为什么系统停下或没修。
3. 用户不需要猜测 Agent 为什么暂停。
4. 用户能在 Web 和 TUI 任一端切换模式，并在另一端同步看到。
5. 任务是否被 gate、被哪个 gate、需要什么决策，在 UI 可见。
6. 费用估算是否考虑 cache，在 UI 可见。
7. 开发者能从 trace/log 复盘完整 AutonomyDecision → Grant → Gate → Action 链路。
