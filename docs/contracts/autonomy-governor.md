# Autonomy Governor 契约

> 范围：Leader harness、Agent runtime、任务编排、Web UI、TUI、权限与工具执行边界。
>
> 目标：在不退化成硬编码规则引擎或低智确认流的前提下，让凌霄具备可解释、可切换、可执行、可同步的自主边界。

## 1. 背景

当前用户反馈集中在同一类问题：凌霄在用户尚未明确授权实现、修复或改写时，可能从只读分析自动升级到建任务、派发 worker、修改代码；Agent 遇到权限或工具范围问题时可能反复撞工具；模式切换如果只追加 prompt，会与旧提示词冲突。

本契约定义一个新的 harness governor：

```text
AutonomyMode
+ LifecyclePhase
+ AutonomyDecision
+ ActionGrant
+ RuntimeGate
+ FailureLedger / LoopBreaker
+ PromptSegment lifecycle
+ Web/TUI state sync
```

核心原则：

```text
LLM 理解意图；Leader 签发决策；Grant 限定边界；Runtime 执行 gate；FailureLedger 防循环；PromptSegment 防冲突；Web/TUI 同步状态。
```

## 2. 非目标

本设计明确不做：

1. 不做自然语言关键词规则引擎。
2. 不用 `if userText.includes("修复")` 这类脆弱判断决定权限。
3. 不把所有动作都改成确认弹窗。
4. 不让 `autonomous` 绕过安全、权限、成本、destructive hard gate。
5. 不让 prompt append-only 堆叠造成旧策略和新策略冲突。

## 3. 模式维度

现有模式继续保持职责边界：

| 维度 | 值 | 职责 |
|---|---|---|
| `control_mode` | `manual` / `eternal` | 决定 `ask_user` / `submit_plan` 是否等待用户 |
| `collaboration_mode` | `solo` / `team` | 决定协作形态 |
| `leader_execution_mode` | `direct` / `hybrid` / `delegate` | 决定 Leader 与 worker 执行倾向 |
| permission mode | `strict` / `dev` / `networked` / `yolo` | 决定底层工具权限 |
| `autonomy_mode` | `review_first` / `balanced` / `autonomous` | 决定自主推进边界 |

关系：

```text
permission mode 决定能不能做；
autonomy_mode 决定该不该做；
control_mode 决定需要用户输入时是否等待真实用户。
```

即使 permission mode 是 `yolo`，`review_first` 也不应擅自写代码；即使 `autonomy_mode=autonomous`，permission mode 是 `strict` 时也不能越权写入。

## 4. AutonomyMode

```typescript
type AutonomyMode =
  | "review_first"
  | "balanced"
  | "autonomous";
```

### 4.1 `review_first`

适合谨慎用户、已有仓库、生产项目、分析/梳理/诊断请求。

| 动作 | 策略 |
|---|---|
| 解释、总结、只读分析 | 自主 |
| 代码搜索、文件读取 | 自主 |
| 梳理 workflow / 架构 / 问题 | 自主 |
| 低风险本地检查 | 可自主，但输出说明 |
| 修改源代码 | 必须先 proposal / review |
| 派发 implement / repair worker | 必须 review |
| 多文件重构、API/schema/state-machine 变更 | 必须 review |
| 权限提升、网络、扩大 write scope | 必须 review |
| Agent 权限失败 | 立即暂停并请求 Leader |

### 4.2 `balanced`

建议默认模式。适合大多数开发任务。

| 动作 | 策略 |
|---|---|
| 只读分析 | 自主 |
| 用户明确授权的小范围修改 | 可自主 |
| 单文件 / 少量局部修改 | 可自主 |
| research 自动升级 implement | 默认不允许 |
| 多文件重构 | 先 proposal |
| API/schema/state-machine 变更 | 先 proposal |
| workflow 初次搭建 | 先 review checkpoint |
| smoke test 后的小修 | 可自主 |
| 权限失败循环 | 自动暂停并请求 Leader |

核心语义：用户明确说“修 / 实现 / 改”时，小范围可以直接做；用户只是“看看 / 梳理 / 分析 / 反馈”时，不能擅自修。

### 4.3 `autonomous`

适合 0→1 项目、用户明确授权全权交付、接受 DAG + worker 自动推进。

| 动作 | 策略 |
|---|---|
| 需求拆解、建 DAG、派发 worker | 自主 |
| 实现 / 修复 / 验证 / repair loop | 自主 |
| 最终验收 | 必须真实证据 |
| 权限提升、destructive、外部服务、生产数据 | 仍需 hard gate |
| Agent loop | 仍需止损 |

`autonomous` 不是 `yolo`；它授权系统做实现决策，不授权无限烧钱、越权、删除、发布、生产操作或无止损循环。

## 5. LifecyclePhase

```typescript
type LifecyclePhase =
  | "bootstrap"
  | "active"
  | "recovery"
  | "stable";
```

| 阶段 | 含义 | 策略 |
|---|---|---|
| `bootstrap` | 项目刚接触、workflow 未建立、测试链路未知、权限未知 | 收紧；允许读和 proposal；不宜大规模 implement |
| `active` | 目标明确、workflow 已建立、测试链路基本可用 | 按当前 `autonomy_mode` 推进 |
| `recovery` | 权限失败、Agent loop、构建连续失败、成本逼近、prompt 冲突 | 临时降级自主边界并请求决策 |
| `stable` | 项目结构清楚、权限正常、验证链路可靠 | 可适度放开，但 hard gate 仍有效 |

临时降级建议：

```text
autonomous → balanced
balanced → review_first
review_first → 保持克制并请求用户/Leader 决策
```

## 6. CapabilityTier

UI 不应只平铺显示 `read / compute / write`。应展示 capability tree：

```typescript
type CapabilityTier =
  | "observe"
  | "compute_local"
  | "verify_local"
  | "write_workspace"
  | "execute_side_effect"
  | "external_network"
  | "destructive";
```

| 层级 | 名称 | 示例 |
|---|---|---|
| L0 | `observe` | `file_read`, `glob`, `code_search`, `list_dir`, `memory_read` |
| L1 | `compute_local` | AST 分析、本地解析、token 估算、纯计算 |
| L2 | `verify_local` | lint、typecheck、test、build、browser verify |
| L3 | `write_workspace` | `file_create`, `structured_patch`, 生成文件 |
| L4 | `execute_side_effect` | shell 服务、安装依赖、迁移脚本 |
| L5 | `external_network` | HTTP、MCP 外部系统、联网 API |
| L6 | `destructive` | 删除、大规模重写、git push、生产 DB、发布 |

## 7. AutonomyDecision

`AutonomyDecision` 是 Leader 对当前用户意图和授权边界的结构化理解。它不是硬编码规则，不直接执行权限；它用于给 `GrantManager` 提供审计依据。

默认不额外调用 LLM。Leader 在处理用户消息的同一轮内部产出 decision，额外成本约 50-150 output tokens，零额外模型调用，零额外延迟。

```typescript
interface AutonomyDecision {
  decision_id: string;
  policy_generation: number;

  user_intent_summary: string;

  requested_work_type:
    | "answer"
    | "research"
    | "diagnose"
    | "proposal"
    | "implement"
    | "repair"
    | "verify"
    | "operate";

  authorization_level:
    | "read_only"
    | "proposal_only"
    | "small_write_allowed"
    | "implementation_allowed"
    | "autonomous_allowed";

  write_grant_requested: boolean;
  dispatch_grant_requested: boolean;
  permission_grant_requested: boolean;

  requires_user_review: boolean;
  review_reason?: string;

  safe_autonomous_scope: string[];
  blocked_actions: string[];

  grant_basis?: {
    source:
      | "user_message"
      | "approved_plan"
      | "mode_policy"
      | "leader_decision";
    source_message_id?: string;
    evidence_quote?: string;
    approved_plan_id?: string;
  };

  next_action:
    | "answer_now"
    | "research_then_summarize"
    | "propose_and_wait"
    | "implement_within_grant"
    | "ask_user"
    | "pause_for_leader";
}
```

### 7.1 开销策略

| 方案 | 额外 LLM 调用 | 额外 token | 延迟 | 结论 |
|---|---:|---:|---:|---|
| 独立 LLM 判断 | 1 次 / 轮 | 500-1500 | 0.5-2s | 仅用于复杂审计场景 |
| Leader 内嵌 decision | 0 | 50-150 output | 0 | 默认方案 |
| 纯规则引擎 | 0 | 0 | 0 | 禁止作为主方案 |
| 纯 prompt | 0 | 0 | 0 | 不可执行，不可靠 |

## 8. ActionGrant

Prompt 只能提醒，不能保证。高风险动作必须有 grant。

```typescript
interface ActionGrant {
  grant_id: string;

  kind:
    | "read"
    | "compute"
    | "verify"
    | "write"
    | "dispatch"
    | "permission_escalation"
    | "network"
    | "destructive";

  issued_by:
    | "user"
    | "leader"
    | "policy"
    | "approved_plan";

  scope: string[];
  reason: string;
  basis: AutonomyDecision["grant_basis"];
  policy_generation: number;

  expires_when?:
    | "after_research"
    | "after_implementation"
    | "after_verification"
    | "session_end";

  revoked?: boolean;
}
```

核心不变量：

```text
没有 write grant，不允许写源代码。
没有 dispatch grant，不允许派发 implement / repair worker。
没有 permission grant，不允许权限提升。
没有 network grant，不允许联网外部访问。
没有 destructive grant，不允许删除、发布、push、生产操作。
```

## 9. RuntimeGate

高风险动作前必须检查 gate。

```typescript
type GateResult =
  | { status: "allow"; grant_id?: string }
  | { status: "deny"; reason: string }
  | { status: "require_review"; reason: string; suggested_prompt: string }
  | { status: "require_leader_help"; reason: string };
```

检查顺序：

```text
1. autonomy_mode
2. lifecycle_phase
3. capability tier
4. grant 是否存在、有效、未过期
5. write_scope / tool_scope / permission mode
6. cost / retry / loop budget
7. hard gate
```

## 10. Research → Implement Gate

默认状态机：

```text
observe → research → summarize → proposal → review_checkpoint → implement → verify → complete
```

默认禁止：

```text
research → implement
```

除非存在：

1. 用户明确授权；
2. 已批准 plan；
3. 当前 `autonomous` 且 action 不属于 hard gate，且不在 `bootstrap` 收紧阶段；
4. Leader 明确签发 dispatch/write grant。

```typescript
function canPromoteResearchToImplement(context): GateResult {
  if (context.hasApprovedPlan) return { status: "allow" };
  if (context.hasUserWriteGrant) return { status: "allow" };
  if (
    context.autonomy_mode === "autonomous" &&
    context.lifecycle_phase !== "bootstrap" &&
    !context.action.isHardGate
  ) return { status: "allow" };

  return {
    status: "require_review",
    reason: "Research result cannot be promoted to implementation without grant.",
    suggested_prompt: "我已完成分析，是否按建议方案进入实现？"
  };
}
```

## 11. Task Dispatch Gate

任务类型风险分层：

| node_kind / role | 风险 |
|---|---|
| `research` / `explore` / `evaluate` | 低 |
| `plan` / `contract` | 中 |
| `implement` / `repair` | 高 |
| `backend` / `frontend` / `fullstack` 写代码 | 高 |
| migration / deploy / destructive | 最高 |

策略：

| mode | research task | implement task |
|---|---|---|
| `review_first` | 可派发 | 需 review / grant |
| `balanced` | 可派发 | 用户明确授权或小范围 grant |
| `autonomous` | 可派发 | 可派发，但 hard gate 仍拦截 |

## 12. Hard Gate

以下动作无论什么模式都必须停下确认或由更高层显式授权：

1. 删除大量文件 / destructive cleanup；
2. `git push` / merge / release / deploy；
3. 生产数据库 / 用户真实数据变更；
4. 权限提升到 `networked` / `yolo`；
5. 写入用户未授权目录；
6. 大规模自动重构；
7. API / schema / 数据迁移影响兼容性；
8. 高成本长任务超过预算；
9. 外部付费 API / 外部系统调用；
10. 安全敏感文件修改，如 credentials / auth / permission；
11. 修改插件、MCP、hook、脚本等会扩大系统能力面的文件。

## 13. FailureLedger 与 LoopBreaker

### 13.1 ToolFailureEvent

```typescript
interface ToolFailureEvent {
  agent_id: string;
  task_id: string;
  tool_name: string;

  failure_class:
    | "permission_denied"
    | "write_scope_denied"
    | "tool_not_allowed"
    | "network_denied"
    | "auth_missing"
    | "sandbox_denied"
    | "command_failed"
    | "not_found"
    | "timeout"
    | "unknown";

  target?: string;
  target_bucket?: string;
  normalized_signature: string;
  timestamp: number;
}
```

分类来源优先级：

1. 工具层结构化 error code；
2. Tool adapter 映射；
3. 轻量 classifier；
4. `unknown`。

不要依赖散乱 regex 作为主判断。

### 13.2 ProgressSignal

```typescript
interface ProgressSignal {
  files_changed: string[];
  artifacts_created: string[];
  tests_executed: string[];
  findings_added: string[];
  task_state_changed: boolean;
  last_meaningful_progress_at: number;
}
```

算 meaningful progress：

- 新的可信 finding；
- 新的文件读取结论；
- 目标文件改动且通过 gate；
- artifact 生成；
- 新验证命令运行；
- task 状态变化；
- 明确缩小问题范围。

不算 progress：

- 重复读同一文件；
- 重复调用失败工具；
- 无 evidence 的重复总结；
- 被拒绝后继续请求同一权限。

### 13.3 默认触发阈值

| 场景 | 触发 |
|---|---|
| 同一权限类失败 | 2 次无进展 |
| 同一命令失败 | 3 次无进展 |
| 连续工具调用无产出 | 15 次 |
| 单任务 token 超预算 | 触发 |
| 长时间无 meaningful progress | 触发 |
| 工具 scope 明显缺失 | 立即触发 |

这些是 runtime safety threshold，不是 NLP 规则引擎。

### 13.4 LeaderHelpRequest

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

Leader 决策：

| mode | 策略 |
|---|---|
| `review_first` | 多数问用户 |
| `balanced` | 小范围 Leader 自决，大范围问用户 |
| `autonomous` | Leader 可自决，但 destructive / external 仍 hard gate |

## 14. PromptSegment 生命周期

```typescript
interface PromptSegment {
  id: string;

  category:
    | "identity"
    | "autonomy_policy"
    | "role_definition"
    | "context_manifest"
    | "constraints"
    | "skills"
    | "ephemeral";

  content: string;
  generation: number;
  revocable: boolean;
  replaces?: string[];
  ttl_ms?: number;
}
```

规则：

```text
1. 同一个 category 同时只激活一个主 segment。
2. 新 autonomy_policy 必须 replaces 旧 autonomy_policy。
3. generation 单调递增。
4. 被 replaces 的 segment 不参与 prompt compose。
5. ephemeral segment 必须有 TTL。
6. mode switch 时旧 policy 明确 revoke。
7. LLM 只以最高 generation 的 autonomy_policy 为准。
```

Policy Card 示例：

```xml
<autonomy_policy id="balanced" generation="42">
You are operating under Balanced Autonomy.

Autonomous:
- Read/search/analyze project context.
- Run low-risk local verification.
- Make small local code changes only when the user explicitly authorized fix/implement/change.
- Continue implementation inside an approved scope.

Require review before:
- Turning research/analysis into implementation.
- Changing source code when user only asked to inspect/summarize/diagnose.
- Multi-file refactor, API/schema/state-machine changes.
- Creating or dispatching implementation/repair tasks from a research result.
- Expanding permissions, network, write scope, or tool access.

Stop and ask Leader when:
- Permission/tool/write-scope denial repeats.
- No progress after repeated attempts.
- The next step exceeds the current grant.

Older autonomy policies with lower generation are revoked.
</autonomy_policy>
```

## 15. Running Agent Policy Update

模式切换时，对 running agent 做两层更新：

1. 即时消息注入：通知新 policy、生效 generation、旧 policy revoked。
2. 下一次 LLM 调用前重组 system prompt：移除旧 `autonomy_policy` segment，只保留新 segment。

已经在执行的工具调用不被 prompt retroactively cancel；但下一次工具调用前必须重新 gate。长任务 / 后台命令必须登记，可由 Leader 选择 kill / pause。

## 16. SessionAutonomyState 与 Web/TUI 同步

```typescript
interface SessionAutonomyState {
  session_id: string;

  autonomy_mode:
    | "review_first"
    | "balanced"
    | "autonomous";

  lifecycle_phase:
    | "bootstrap"
    | "active"
    | "recovery"
    | "stable";

  mode_generation: number;

  effective_policy_id: string;
  effective_policy_hash: string;

  updated_at: number;

  updated_by:
    | "web"
    | "tui"
    | "leader"
    | "runtime_policy";

  reason?: string;
}
```

单一权威源建议：

```text
SessionStateStore
.lingxiao/sessions/<session_id>/state/autonomy.json
```

切换 API：

```typescript
setAutonomyMode({
  session_id,
  next_mode,
  reason,
  expected_generation
})
```

事件：

```typescript
interface AutonomyModeChangedEvent {
  type: "autonomy_mode_changed";
  session_id: string;

  previous_mode: AutonomyMode;
  next_mode: AutonomyMode;

  previous_generation: number;
  next_generation: number;

  lifecycle_phase: LifecyclePhase;

  updated_by: "web" | "tui" | "leader" | "runtime_policy";
  reason?: string;

  effective_policy_hash: string;
}
```

同步流：

```text
TUI/Web 切换
→ SessionStateStore 写入
→ EventBus 广播 autonomy_mode_changed
→ 另一端 UI 刷新
→ Leader 更新 PolicySegment
→ Running Agent 收到 Policy Update
```

## 17. UI 展示要求

UI 不应只显示 `Balanced`，应显示 effective policy：

```text
Autonomy: Balanced
Phase: Bootstrap
Effective behavior:
- Read/analyze: auto
- Small edits: only if user asked to fix/implement
- Research → implementation: review required
- Permission expansion: review required
```

解释来源：

```text
effective_policy = autonomy_mode + lifecycle_phase + control_mode + permission_mode + active_grants
```

## 18. Cache-aware Billing 契约

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

  rate_source:
    | "provider_api"
    | "configured"
    | "default_table"
    | "unknown";

  confidence:
    | "high"
    | "medium"
    | "low";

  note?: string;
}
```

UI 必须标注：

```text
Estimated cost, not provider bill.
```

当费率未知或 cache 计价未知时，`confidence = "low"`。

## 19. 验收用例

### Case 1：用户说“梳理 workflow”

期望：只读分析，输出总结和可选方案，不改代码，不派发 implement。

### Case 2：用户说“这个 bug 你修一下”

`balanced` 下期望：小范围修复可直接做；多文件/架构变更先 proposal。

### Case 3：Agent 写入权限不足

期望：同类失败 2 次内暂停，发 `LeaderHelpRequest`，不重复撞工具。

### Case 4：Web 切到 `review_first`

期望：TUI 同步显示，Leader prompt policy 更新，running agent 收到 policy update，pending implement task 被 gate。

### Case 5：`autonomous` 下 destructive 操作

期望：仍然 hard gate，必须显式授权。

## 20. 兼容迁移

最小兼容策略：

1. 默认 `autonomy_mode = balanced`。
2. 未实现 GrantManager 前，先用 task `node_kind`、`agent_type`、`write_scope`、permission mode 做粗 gate。
3. 未实现 PromptSegment 存储前，至少保证当前 active prompt 只注入一个 `autonomy_policy`。
4. 未实现 Web/TUI 全同步前，先把 state 写进 SessionStateStore，并由事件广播驱动 UI 刷新。
5. 未实现 cache-aware billing 前，费用 UI 标注低置信估算。
