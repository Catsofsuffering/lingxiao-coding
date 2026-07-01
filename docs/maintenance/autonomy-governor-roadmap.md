# Autonomy Governor 落地路线图

> 对应契约：[`docs/contracts/autonomy-governor.md`](../contracts/autonomy-governor.md)
>
> 目标：把“低干扰但不擅自推进”的产品行为落到 harness、runtime、Web UI、TUI、任务编排和计费展示中，避免研究任务自动升级为实现、Agent 权限循环、模式切换 prompt 冲突和双端状态不同步。

## 1. 当前问题总结

用户反馈指向同一组系统性问题：

1. **Research → Implement 自动升级**
   - 用户只要求梳理 workflow / 看问题，系统可能直接建实现任务或开修。
2. **Harness 推进过激**
   - 0→1 自动交付逻辑覆盖了已有仓库、谨慎用户、诊断类任务。
3. **Agent loop 缺少止损**
   - 权限、tool scope、write scope 不匹配时，Agent 可能重复调用失败工具。
4. **模式语义混乱**
   - `control_mode`、`collaboration_mode`、permission mode、Leader execution mode 与“自主推进边界”混在一起。
5. **Prompt append-only 冲突**
   - 旧 prompt 说自主推进，新 prompt 说修复前确认，LLM 会收到矛盾约束。
6. **Web UI / TUI 模式不同步**
   - 模式需要 session 级权威状态和事件广播。
7. **计费缺少 cache-aware breakdown**
   - 输入缓存命中没有体现在费用展示中，估算参考价值不足。

## 2. 设计原则

1. **拒绝硬编码自然语言规则引擎**
   - 不用关键词决定是否允许修复。
   - Leader 用语义理解产出 `AutonomyDecision`，runtime 用 grant/gate 执行边界。
2. **只读自主，写入审慎，高风险硬停**
   - 只读分析默认自主；写入、实现派发、权限扩大、destructive 操作必须经过 gate。
3. **所有关键状态必须可视化**
   - `autonomy_mode`、`lifecycle_phase`、GateResult、LeaderHelpRequest、Grant、Policy generation、CostBreakdown 必须在 Web/TUI/Agent/Task/Trace 对应视图可见。
   - 具体视图契约见 [`docs/contracts/autonomy-governor-views.md`](../contracts/autonomy-governor-views.md)。
4. **低干扰不是不确认**
   - 低风险少问；高风险必停；权限失败必须止损。
5. **模式切换必须可同步、可撤销、可审计**
   - Web UI / TUI / Leader / Agent runtime 共享 `SessionAutonomyState`。
6. **Prompt 策略必须替换，不是追加**
   - `autonomy_policy` segment 单激活，generation 单调递增，旧策略 revoked。
7. **autonomous 不是 yolo**
   - `autonomous` 允许自主实现，但不绕过 hard gate、permission mode、成本预算和 loop breaker。

## 3. 已定位的现有接入点

基于当前仓库初步搜索，主要接入点如下：

| 领域 | 现有文件 / 模块 | 说明 |
|---|---|---|
| Session state keys | `src/core/SessionStateKeys.ts` | 已有 `CONTROL_MODE`、`COLLABORATION_MODE`、`LEADER_EXECUTION_MODE`，新增 `AUTONOMY_MODE` / `LIFECYCLE_PHASE` 应放这里 |
| Runtime state | `src/runtime/SessionManagerRuntime.ts` | 已有 control/collaboration mode setter 和 runtime state publish；新增 `setAutonomyMode` 可复用该模式 |
| Event types | `src/core/EventEmitter.ts`, `src/contracts/types/Event.ts`, `src/contracts/adapters/EventAdapter.ts` | 新增 `session:autonomy_mode_changed` 事件 |
| SSE bridge | `src/web-server/SseBridge.ts` | 新事件需要桥接到 Web UI |
| ACP handler | `src/web-server/AcpHandler.ts` | 已有 `session/set_control_mode`、`session/set_collaboration_mode`；新增 `session/set_autonomy_mode` |
| TUI event bridge | `src/tui/runtime/useTuiEventBridge.ts` | 订阅 autonomy mode changed |
| TUI main | `src/tui/LingXiaoTUI.tsx` | 增加命令、状态栏、快捷切换入口 |
| Web session store | `web/src/stores/sessionStore.ts`, `web/src/stores/sessionStoreTypes.ts` | 保存 autonomy state |
| Web mode controls | `web/src/components/chat/ModeSplitControls.tsx`, `web/src/components/chat/ControlModeToggle.tsx` | 增加 autonomy mode 切换控件 |
| Web ACP client | `web/src/api/AcpClient.ts` | 增加 set autonomy mode API |
| Agent runtime | `src/agents/BaseAgentRuntime.ts`, `src/agents/runtime/ReasoningLoopDriver.ts`, `src/agents/runtime/ToolLoopDetector.ts` | 接入 FailureLedger / LoopBreaker / policy update |
| Dispatch decision | `src/agents/DispatchDecisionCoordinator.ts` | implement/repair dispatch 前检查 autonomy gate |
| Prompt | `src/agents/prompts/i18n/leader_system_prompt.ts` | 注入 Policy Card；后续迁移到 PromptSegment composer |
| Billing | `web/src/utils/costCalculator.ts`, `web/src/components/sidebar/UsageCard.tsx`, `web/src/components/stats/StatsView.tsx` | 加 cache-aware cost breakdown |

> 注：本路线图只记录接入点，不代表已经完成代码实现。

## 4. 新增核心对象

### 4.1 SessionAutonomyState

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

### 4.2 AutonomyDecision

Leader 对用户授权边界的结构化理解。默认不额外调用 LLM，而是作为 Leader 同轮内部结构化输出。

### 4.3 ActionGrant

Runtime 高风险动作的授权凭证，包括 write、dispatch、network、destructive 等。

### 4.4 RuntimeGate

高风险动作入口：写文件、派发 implement/repair、权限提升、外部网络、destructive、长耗时任务等必须通过 gate。

### 4.5 FailureLedger / LoopBreaker

记录 tool failure 与 meaningful progress，阻止 Agent 在权限或工具范围错误时重复撞墙。

### 4.6 PromptSegment

用于动态注入和清理 prompt。`autonomy_policy` 同时只能有一个 active segment。

## 5. 分阶段落地计划

## P0：安全止血与行为边界

目标：最快阻断当前用户反馈的关键问题。

### P0-1 Research → Implement Gate

**改动点：**

- 在 Leader 创建/派发 implement 或 repair 任务前检查 gate。
- 若当前用户请求类型是 research / diagnose / summarize / proposal，默认不能自动升级到 implement。
- `autonomous` 在 `bootstrap` 阶段也要收紧。

**建议接入：**

- `src/agents/DispatchDecisionCoordinator.ts`
- `src/agents/LeaderAgent.ts` 或 Leader dispatch 工具封装处
- `src/agents/LeaderTools.ts` 中 `create_task` / `dispatch_agent` wrapper，如存在集中入口则优先集中拦截

**验收：**

- 用户输入“帮我梳理旧仓库 workflow”，系统只读分析和总结，不创建 implement/repair 任务。
- 用户输入“分析这个 bug 是什么”，系统不自动修改代码。
- 用户输入“分析并修复这个 bug”，允许进入实现，但多文件/架构变更仍需 proposal。

### P0-2 FailureLedger + LoopBreaker

**改动点：**

- 工具调用失败后记录 `ToolFailureEvent`。
- 权限类同 signature 失败 2 次且无 meaningful progress，暂停 Agent 并发 `LeaderHelpRequest`。
- 命令类同 signature 失败 3 次且无 progress，暂停 Agent。

**建议接入：**

- `src/agents/runtime/ReasoningLoopDriver.ts`
- `src/agents/runtime/ToolLoopDetector.ts`
- `src/agents/BaseAgentRuntime.ts`
- `src/agents/pool/FaultRecovery.ts`

**验收：**

- Agent 因 write scope denied 连续失败 2 次后停止，不再重复调用同一写工具。
- Leader 收到结构化 help request，包含失败原因、尝试动作、需要的决策。
- Agent 不把 blocked 状态伪装成完成。

### P0-3 基础 Prompt Policy Card 单激活

**改动点：**

- 在系统提示中只注入一个当前 `autonomy_policy`。
- policy 带 `id`、`generation`、`revoked older policies` 文案。
- 暂未实现完整 PromptSegment 前，至少避免多份互斥 autonomy prompt 同时存在。

**建议接入：**

- `src/agents/prompts/i18n/leader_system_prompt.ts`
- Agent prompt compose 或 Context Manifest 注入层

**验收：**

- 切换模式后，下一轮 Leader prompt 中只有一个 autonomy policy。
- 不出现同时“全自动修复”和“修复前必须确认”的互斥段落。

### P0-4 Task Dispatch Gate

**改动点：**

- `create_task` 可以创建 research/plan/contract 节点。
- `dispatch_agent` / `dispatch_batch` 派发 implement/repair/coding/backend/frontend/fullstack 前检查 grant。
- Solo ephemeral worker 与 Team worker 都遵守同一 gate。

**验收：**

- `review_first` 下不能直接派发 implement worker。
- `balanced` 下用户明确要求小修时可以派发小范围 implement。
- `autonomous` 下可派发 implement，但 destructive / permission expansion 仍被拦截。

## P1：模式产品化与双端同步

目标：让 Web UI / TUI 都能切换，并且 Leader、Agent、UI 状态一致。

### P1-1 SessionAutonomyState

**改动点：**

- `SessionStateKeys` 新增：
  - `AUTONOMY_MODE`
  - `LIFECYCLE_PHASE`
  - `AUTONOMY_MODE_GENERATION`
- `SessionManagerRuntime` 增加：
  - `setAutonomyMode(sessionId, mode, reason, updatedBy)`
  - `getAutonomyState(sessionId)`
  - publish runtime state

**验收：**

- 新 session 默认 `balanced + bootstrap`。
- 切换后 generation 单调递增。
- runtime snapshot 包含 autonomy state。

### P1-2 Event / SSE / ACP

**改动点：**

- 新增事件：`session:autonomy_mode_changed`。
- `EventAdapter` 定义 payload。
- `SseBridge` 转发到前端。
- `AcpHandler` 增加 `session/set_autonomy_mode`。

**验收：**

- Web 调 ACP 切换后 TUI 能收到事件。
- TUI 切换后 Web store 能更新。
- generation conflict 被正确拒绝或覆盖策略清晰。

### P1-3 Web UI

**改动点：**

- 在 mode 控件区域加入 autonomy mode：
  - `Review First`
  - `Balanced`
  - `Full Auto`
- 展示 effective behavior，而不只显示模式名。
- 若当前 phase 是 `bootstrap` 或 `recovery`，显示收紧说明。

**建议接入：**

- `web/src/components/chat/ModeSplitControls.tsx`
- `web/src/components/chat/ControlModeToggle.tsx`
- `web/src/stores/sessionStore.ts`
- `web/src/stores/sessionStoreTypes.ts`
- `web/src/api/AcpClient.ts`

**验收：**

- Web 切换模式后 UI 立即显示新状态。
- 收到 SSE 后 store 与控件同步。
- Full Auto 首次选择时展示简短风险说明。

### P1-4 TUI

**改动点：**

- 增加 `/autonomy review_first|balanced|autonomous` 命令。
- 状态栏显示：
  - `autonomy`
  - `phase`
  - `control_mode`
  - `collaboration_mode`
- 订阅 `session:autonomy_mode_changed`。

**建议接入：**

- `src/tui/LingXiaoTUI.tsx`
- `src/tui/runtime/useTuiEventBridge.ts`

**验收：**

- TUI 命令切换后 Web 同步。
- Web 切换后 TUI 状态栏同步。
- 无 session 时命令给出清晰错误。

### P1-5 Running Agent Policy Update

**改动点：**

- 模式切换时向 running agent 发送 policy update。
- 下一次 LLM 调用前重组 prompt policy。
- 旧 generation grant 需要重新检查或续签。

**验收：**

- Agent 正在运行时，从 `autonomous` 切到 `review_first`，下一次写工具调用前被 gate 拦截。
- Agent 收到 policy update 后不继续从 research 自动实现。

## P2：完整 Grant 与 Lifecycle

目标：从粗 gate 进化到完整可审计授权边界。

### P2-1 GrantManager

**改动点：**

- 存储 active grants。
- grant 包含 basis、scope、policy_generation、expires_when。
- 支持 revoke、expire、renew。

**验收：**

- 没有 write grant 时写入被拒绝。
- 没有 dispatch grant 时 implement/repair dispatch 被拒绝。
- 模式切换后旧 generation grant 需要重新评估。

### P2-2 LifecyclePhase 自动判断

**信号建议：**

| 信号 | phase |
|---|---|
| 新 session / 未读项目 / 无 smoke test | `bootstrap` |
| 目标明确 / plan 已确认 / smoke test 跑通 | `active` |
| loop breaker / 权限失败 / 构建连续失败 | `recovery` |
| 多轮验证稳定 / 项目链路清楚 | `stable` |

**验收：**

- 新会话默认 bootstrap。
- 发生 Agent loop 自动进入 recovery。
- recovery 下 autonomous 也临时收紧。

### P2-3 PromptSegment Composer

**改动点：**

- 引入 segment category、generation、replaces、ttl。
- `autonomy_policy` category 单激活。
- ephemeral segment 到期清理。

**验收：**

- 同 category 不出现多个互斥段落。
- 切模式后旧 policy 不再参与 prompt compose。
- Debug 日志能看到 active policy hash。

### P2-4 Capability Tree UI

**改动点：**

- 将工具按 capability tier 展示。
- 用户能理解 read / compute / write 对应哪些工具和风险。

**验收：**

- UI 展示 L0-L6 capability tree。
- Agent tool scope 和系统 permission mode 关系清楚。

## P3：计费与体验增强

### P3-1 Cache-aware Billing

**改动点：**

- `CostBreakdown` 加：
  - `cached_input_tokens`
  - `cache_hit_ratio`
  - `cached_input_rate`
  - `rate_source`
  - `confidence`
- UI 明确标注 estimated cost，不代表 provider 实际账单。

**建议接入：**

- `web/src/utils/costCalculator.ts`
- `web/src/components/sidebar/UsageCard.tsx`
- `web/src/components/stats/StatsView.tsx`

**验收：**

- 有 cached token 数据时费用估算使用缓存费率。
- 无费率时显示 low confidence。
- 用户能看到 input / cached input / output 分项。

### P3-2 Decision Trace

**改动点：**

- 在开发者视图展示本轮 AutonomyDecision。
- 不默认暴露给普通用户，避免 UI 噪音。

**验收：**

- 能审计某次“为什么没有直接修”。
- 能审计某次“为什么允许小范围修复”。

## 6. Hard Gate 清单

以下动作无论模式如何都必须显式确认或更高层授权：

1. 删除大量文件 / destructive cleanup；
2. `git push` / merge / release / deploy；
3. 生产数据库 / 用户真实数据变更；
4. 权限提升到 `networked` / `yolo`；
5. 写入用户未授权目录；
6. 大规模自动重构；
7. API / schema / 数据迁移影响兼容性；
8. 高成本长任务超过预算；
9. 外部付费 API / 外部系统调用；
10. credentials / auth / permissions 等安全敏感文件修改；
11. 插件、MCP、hook、script 等扩大系统能力面的文件修改。

## 7. 风险与防护

| 风险 | 防护 |
|---|---|
| Prompt 冲突 | PromptSegment 单激活、generation、replaces、policy_hash |
| Web/TUI 同步竞态 | `mode_generation` 乐观锁，事件只接受更高 generation |
| 旧任务继续写 | pending task 重新 gate，running agent policy update，旧 grant 重新评估 |
| 已发生写入无法回滚 | 标记 diff `needs_review`，不继续扩大修改 |
| autonomous 被误用 | UI 解释、bootstrap 收紧、hard gate 永远存在 |
| review_first 过度打扰 | 只读和低风险验证不问，只在 write/dispatch/permission/destructive 前停 |
| Agent help request 泛滥 | request signature 去重，拒绝后必须换策略或停止 |
| 成本数字误导 | cache-aware breakdown + confidence + estimated 标注 |

## 8. 测试矩阵

### 8.1 单元测试

| 模块 | 用例 |
|---|---|
| Autonomy decision helper | research 请求不产生 write grant |
| Runtime gate | 无 write grant 拒绝写入 |
| Dispatch gate | review_first 拒绝 implement dispatch |
| LoopBreaker | write_scope_denied 两次无进展触发 help |
| Prompt composer | 切换 policy 后只保留新 generation |
| Cost calculator | cached input token 正确折算 |

### 8.2 集成测试

1. Web 切模式 → SSE → TUI 状态同步。
2. TUI 命令切模式 → Web store 更新。
3. Running Agent 收到 policy update 后停止高风险动作。
4. 用户只要求梳理 workflow，系统不派发 implement worker。
5. autonomous 下 destructive 操作仍被 gate。

### 8.3 回归测试

1. control_mode manual/eternal 原语义不变。
2. collaboration_mode solo/team 原语义不变。
3. permission mode strict/dev/networked/yolo 原语义不变。
4. 已有 create_task / dispatch_agent 在明确授权实现时不被误拦截。
5. 只读 explore / research worker 不被过度拦截。

## 9. 发布策略

### 9.1 Feature flag

建议加入开关：

```text
features.autonomyGovernor = true
features.autonomyPromptSegments = false initially
features.autonomyGrantManager = false initially
```

先上线 P0 gate 和 loop breaker，再逐步打开完整 GrantManager / PromptSegment。

### 9.2 默认值

```text
autonomy_mode = balanced
lifecycle_phase = bootstrap
```

### 9.3 向后兼容

- 未设置 autonomy state 的旧 session 自动迁移到 `balanced + bootstrap + generation=1`。
- 没有完整 GrantManager 时，使用粗粒度 gate：task kind、agent role、write_scope、permission mode。
- 旧 UI 不展示 autonomy mode 时，不影响底层默认 gate。

## 10. 最小可交付切片

第一版建议只做：

1. 新增 `autonomy_mode` 状态，默认 `balanced`。
2. Web/TUI 可切换并同步。
3. research → implement gate。
4. implement/repair dispatch gate。
5. Agent 权限类失败 2 次暂停。
6. 所有 P0/P1 gate、pause、mode 切换至少在一个 Web 视图和一个 TUI 视图可见。
7. Prompt 中只注入一个当前 Policy Card。
8. README / docs 更新。

这能先解决 80% 真实用户痛点。

## 11. 完成定义

本路线图视为完成时，应满足：

1. 用户要求“梳理/分析/看看”时，系统不会擅自修改或派发实现。
2. 用户明确要求“修/实现”时，系统能在授权范围内高效完成，不退化成频繁确认。
3. Web UI 和 TUI 对 autonomy mode 的展示和切换完全同步。
4. Web UI 和 TUI 都能展示 effective policy、gate blocked reason、Agent help request 和 policy generation。
5. Running Agent 能收到 policy update，并在下一次高风险动作前重新 gate。
6. Agent 权限循环被 LoopBreaker 拦截。
7. Prompt 不出现多份互斥 autonomy policy。
8. autonomous 仍受 hard gate、permission mode、budget 和 loop breaker 约束。
9. 计费展示能说明 cache 命中和估算置信度。
