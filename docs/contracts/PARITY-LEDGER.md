# LingXiao Rust Core 鈥?Production Parity Ledger

**Date:** 2026-07-03
**Branch:** latest-lingxiao-update
**Baseline:** TS Core ~8.4MB, 731 files; Rust Core ~14,800 lines across 9 crates
**Target:** Full TS Core runtime parity in Rust Core (production-grade, headless, no TS fallback)

---

## Ledger Structure

Each TS Core capability is classified into one of:

- **MIGRATED** 鈥?Rust implementation exists, production-shaped, tested
- **PARTIAL** 鈥?Rust implementation exists but simplified/incomplete vs TS behavior
- **STUB** 鈥?Placeholder types only, no real logic
- **MISSING** 鈥?Not yet started
- **BREAKING** 鈥?Intentionally removed or redesigned in Rust Core

---

## Executive Summary

### What Works (Production-Ready)

| Area | Status | Evidence |
|------|--------|----------|
| Persistence (SQLite schema, single-writer, WAL) | MIGRATED | 36 tables, DbOwner mutex, BEGIN IMMEDIATE, all tests pass |
| Event Log (ordered, seq/generation, replay) | MIGRATED | GS-020/021 pass; gap detection, idempotent append |
| Projection (snapshot/delta reconnect) | MIGRATED | GS-022/023 pass; cursor-based delta |
| Permission (request/resolve/mode-change) | MIGRATED | GS-011/012/013 pass; revocation, crash resume |
| Session lifecycle (create/input/interrupt/delete) | MIGRATED | GS-001/002/003/004 pass |
| Task state machine (create/assign/complete/redispatch) | MIGRATED | GS-005/006/007 pass; generation bumps |
| Sidecar tool spawning (stdio, timeout, cancel) | MIGRATED | GS-018 pass; real process, resource tracking |
| LLM streaming (text/thinking/tool deltas, usage) | MIGRATED | GS-019 pass; realtime/durable separation |
| Credential handling (AuthContext, redaction) | MIGRATED | No env reads in tests; stderr scrubbing verified; event_log payloads redact obvious API-key/token strings before SQLite persistence |
| Tool-call audit persistence | MIGRATED | `tool_calls` stores execution status plus sanitized args/result metadata only; command responses retain live tool observations without persisting full payloads |
| Retention / high-growth table pruning | MIGRATED | `retention.sweep` prunes event_log/tool_calls/traces/token_usage/llm_gateway_requests/conversation/team tables by per-session retention, updates compacted_seq, and daemon ticker schedules it |
| Command idempotency (dedupe table) | MIGRATED | Composite PK (key, method) |

### Critical Gaps (Blocking Production Parity)

| # | Gap | Impact | TS Source | Rust Status |
|---|-----|--------|-----------|-------------|
| **G-1** | **Live agent execution loop migrated** | AgentLoop/AgentPool/leader.run execute through Rust LlmRouter, native tools, durable events, DB state, and daemon/headless command paths | `BaseAgentRuntime.ts`, `AgentRoundExecutor.ts` | agent.rs + command.rs supervised execution tests pass |
| **G-2** | **Workflow node executors migrated** | DAG ordering, supervised synchronous Rust-canonical traversal, tool/LLM/agent/data node execution, durable node state, skip completed nodes, and retry policy exist | `WorkflowEngine.ts` | workflow_node_state + handle_workflow_execute |
| **G-3** | **LLM resilience migrated** | Retry/circuit/fallback plus cost/capability/context-window metadata routing exist and daemon config wires provider metadata | `LlmGuard.ts`, `RetryEngine.ts`, `CircuitBreaker.ts` | RetryConfig, CircuitBreaker, fallback chain, metadata preflight tested |
| **G-4** | **Native tools migrated** | Core native file/list/glob/search/shell/git/attempt_completion/send_message plus terminal/REPL/MCP/document boundaries exist with permission gates | `src/tools/implementations/*.ts` | Rust-canonical exact patch and supervised terminal/session boundaries documented |
| **G-5** | **Task generation gate migrated** | Stale agent completions rejected | `TaskBoard.ts` `_shouldRejectLateResult` | P1-1 test passes |
| **G-6** | **Agent heartbeat/cleanup migrated** | AgentPool records heartbeats, cancels stale handles, removes terminal agents, and command router bridges pool events into durable DB state | `WorkerProcessRunner.ts` heartbeat | agent.rs + daemon/headless tests pass |
| **G-7** | **Context compaction migrated** | Active context projection is reduced while original conversation rows are retained; AgentLoop uses active-window projection; runtime.compact can use LLM summarization with deterministic fallback | `ContextManager.ts` `compact()` | runtime.compact, token-budget compaction, replay/snapshot, and daemon-persisted agent replay tested |
| **G-8** | **Automatic recovery sweep migrated** | Daemon boot pauses stuck workflows and stops stuck agents with structured counts | `SessionManagerRuntime.ts` recovery | DbOwner::recover_orphans runs during serve bootstrap |
| **G-9** | **MessageBus wired to CommandRouter** | In-memory priority queue exposed through headless `bus.publish`, `bus.pop`, and `bus.dead_letters` commands | `MessageBus.ts` | GS-031/032 cover standalone and router dispatch paths |
| **G-10** | **Memory FTS/embedding migrated** | SQLite FTS5 search and pure-Rust cosine embedding search exist with durable upsert/search commands | `MemoryFTS.ts` | memory.upsert/search and embedding.upsert/search tested |

---

## Detailed Capability Matrix

### F-001 Session Lifecycle

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| create / input / interrupt / resume / complete / fail / delete | SessionRuntime.ts | command.rs handlers | **MIGRATED** | GS-001..004 pass |
| active/focused tracking | SessionManagerRuntime.ts | session.rs + sessions table | **MIGRATED** | `status` TEXT column |
| session.list w/ snapshots | SessionManagerRuntime.ts | handle_session_list | **MIGRATED** | |
| crash recovery via event replay | SessionManagerRuntime.ts recovery | session.list rebuilds from DB | **MIGRATED** | Boot sweep plus daemon GS-026 restart test |
| generation bump on interrupt | StateSemantics.ts | handle_session_transition, event_log_meta | **MIGRATED** | |
| session.connect cursor delta | SseBridge reconnect | handle_session_connect + ProjectionService | **MIGRATED** | GS-022/023 pass |

### F-002 Runtime State / Projection

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Snapshot at seq=N | SessionRuntimeState | ProjectionService::snapshot | **MIGRATED** | GS-022/023 pass |
| Delta reconnect | EternalRuntimeProjection | ProjectionService::connect | **MIGRATED** | |
| Gap > 100 鈫?snapshot_required | ModeRuntimeProjection | check_gap() | **MIGRATED** | MAX_REPLAY_EVENTS = 100 |
| Full UI projection fold | EternalRuntimeProjection | projection.rs::snapshot | **MIGRATED** | Rust Core canonical boundary is headless projection, not TUI/Web/Electron view state. Snapshot now folds session status, permissions, active context, tasks, agents, workflows, and sanitized tool-call summaries for adapters |

### F-003 State Semantics

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| SessionStatus enum + is_terminal | StateSemantics.ts | session.rs:SessionStatus | **MIGRATED** | Active/Interrupted/Completed/Failed/Deleted |
| TaskStatus enum + transitions | TaskBoard.ts | task.rs + command.rs | **MIGRATED** | dispatchable/running/terminal with run_generation |
| AgentStatus enum | AgentExecutionResult.ts | agent.rs + command.rs | **MIGRATED** | AgentLoop/AgentPool status transitions are bridged into durable `agent_state` and event log rows; legacy explicit lifecycle commands remain supported |
| WorkflowExecutionStatus + can_transition_to | workflow/types.ts | workflow.rs | **MIGRATED** | Guard in handle_workflow_transition |
| PermissionMode enum | PermissionSystem.ts | permission.rs | **MIGRATED** | Strict/Dev/Networked/Yolo |

### F-004 Task Board

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| task.create / assign / complete / fail / list | TaskBoard.ts | command.rs | **MIGRATED** | GS-005..007 pass |
| redispatch (blocked 鈫?dispatchable) | TaskBoard.ts | handle_task_redispatch | **MIGRATED** | run_generation + 1 |
| reopen terminal | TaskBoard.ts | handle_task_reopen | **MIGRATED** | |
| blocked_by dependencies | TaskPriorityEngine.ts | task.create params.blocked_by + handle_task_terminal | **MIGRATED** | `blocked_by` is stored as a durable JSON dependency list; successful terminal completion clears dependents, emits `task.unblocked`, and makes tasks dispatchable when dependencies are empty |
| late result rejection (generation gate) | TaskBoard._shouldRejectLateResult | handle_task_terminal | **MIGRATED** | Rejects stale caller run_generation; P1-1 test passes |

### F-005 Leader Orchestration

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| leader.plan 鈫?task batch | LeaderAgent.ts | handle_leader_plan + LeaderOrchestrator | **MIGRATED** | Rust canonical split: `leader.plan` is deterministic task-batch planning/normalization, while live LLM think/act/observe production execution is `leader.run`; both command paths are tested |
| think/act/observe loop | LeaderAgent think() | handle_leader_run | **MIGRATED** | Rust LlmRouter loop streams LLM output with injected native tool definitions, executes tools, appends observations, and finalizes; daemon stdio route tested |
| tool call dispatch from leader | LeaderTools.ts | handle_leader_run + tool.call | **MIGRATED** | Safe native tool calls route through CommandRouter/tool registry and durable tool events; daemon stdio leader.run file_read E2E passes |
| completion detection | LeaderAgent isComplete() | handle_leader_run | **MIGRATED** | Stops on `attempt_completion`, final no-tool LLM text, or max-round exhaustion |
| leader permission management | LeaderPermissionManager.ts | handle_leader_run + permission handlers | **MIGRATED** | High-risk leader tool calls create pending permission requests before side effects |

### F-006 Agent Runtime

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| agent.spawn / start / complete / crash / respawn | AgentPoolRuntime.ts | command.rs | **MIGRATED** | GS-008/009 pass (state machine only) |
| Rust-native task execution | BaseAgentRuntime.ts | agent.rs::AgentLoop + command.rs `agent.spawn run:true` | **MIGRATED** | Threaded loop executes from workflow agent nodes and daemon/headless `agent.spawn` supervised mode, with persisted context replay and terminal task result updates |
| Tool call loop (act/observe) | AgentRoundExecutor.ts | agent.rs::AgentLoop | **MIGRATED** | Native tool calls execute and feed observations back into conversation; E2E file_read observe/final test |
| LLM round executor | AgentRoundExecutor.ts | agent.rs::AgentLoop | **MIGRATED** | Uses LlmRouter, active context projection, injected native tool definitions, SQLite-backed replay, and daemon/headless AgentPool lifecycle wiring |
| Context window management per agent | ContextManager.ts | context.rs + agent.rs + agent_conversation | **MIGRATED** | AgentLoop loads persisted agent turns after router restart, persists new turns, and writes body-free active-window diagnostics |
| Result posting back to task | AgentRoundExecutor.ts | command.rs workflow agent node | **MIGRATED** | Agent-node completion updates existing task status/result deterministically |

### F-007 Agent Pool / Scheduling

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Agent pool spawn management | AgentPoolRuntime.ts | agent.rs::AgentPool + command.rs | **MIGRATED** | Spawn/cancel/supervise/terminal cleanup and max-parallel rejection tested; `CommandRouter` owns AgentPool and `agent.spawn run:true` bridges pool events to durable DB state |
| Max parallel agents | UnifiedScheduler.ts | agent.rs::AgentPool + RuntimeManager + CommandRouter | **MIGRATED** | In-process AgentPool enforces configured max; durable `agent.spawn` reserves RuntimeManager workers and rejects before `agent_state` side effects; terminal agent transitions release workers |
| Agent type routing | UnifiedScheduler.ts | task.assign route_agent_for_task_type | **MIGRATED** | `task.assign` derives a stable agent id from `agent_type` when no explicit assignee is supplied, while `preferred_agent_name` and explicit `agent_id` override routing |
| Heartbeat polling | WorkerProcessRunner.ts | agent.rs::HeartbeatMonitor | **MIGRATED** | AgentPool monitor polls heartbeat ages, cancels stale handles, emits crashed event, and removes stale agents |

### F-008 Worker / Process Lifecycle

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Sidecar process spawn/kill | WorkerProcessRunner.ts | sidecar.rs::SidecarScheduler | **MIGRATED** | GS-018 pass; real child process |
| Timeout enforcement | WorkerProcessRunner.ts | sidecar.rs:134-143 | **MIGRATED** | Polls child.try_wait() |
| Cancel signal propagation | WorkerProcessRunner.ts | sidecar.rs cancel_flag | **MIGRATED** | AtomicBool checked in loop |
| Resource usage tracking | ResourceBudgetService.ts | RuntimeManager + CommandRouter | **MIGRATED** | RuntimeManager gates sidecars, workers, native tool slots, global/per-agent token counters, and file-write bytes; command handlers reject over-budget work before external/provider side effects |
| Orphan process cleanup | PidRegistry.ts | process.rs + daemon bootstrap | **MIGRATED** | Durable `owned_processes` registry tracks owned child PIDs, sidecars/terminal/REPL register rows, daemon boot sweeps active owned PIDs, and unknown/external PIDs are safe no-ops/failures |

### F-009 Message Bus

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Priority queue (4 levels) | MessageBus.ts | bus.rs | **MIGRATED** | GS-031/032 pass |
| Backpressure on capacity | MessageBus.ts | bus.rs::publish | **MIGRATED** | Capacity check, dead-letter if full |
| Dead-letter tracking | MessageBus.ts | bus.rs::dead_letters | **MIGRATED** | |
| Integration with CommandRouter | MessageBus.ts | command.rs bus.* handlers | **MIGRATED** | `bus.publish`, `bus.pop`, and `bus.dead_letters` expose priority delivery/backpressure |

### F-010 Event Log

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Ordered append (seq monotonic) | EventEmitter.ts | event_log.rs::append | **MIGRATED** | GS-020 pass |
| Generation gate | 鈥?| event_log.rs::append_with_generation_check | **MIGRATED** | GS-021 pass |
| event_id idempotency | 鈥?| event_log.rs UNIQUE(event_id) | **MIGRATED** | |
| Replay with pagination | EventEmitter.ts | event_log.rs::replay | **MIGRATED** | GS-020 variants pass |
| Compaction / truncation | 鈥?| event_log.compact + event_log_meta.compacted_seq | **MIGRATED** | `event.compact` prunes old rows, persists compacted low-water mark, emits `event_log.compacted`, and reconnect returns `snapshot_required` for cursors before compacted_seq |

### F-011 Persistence Schema

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| 27 TS-parity tables | Database.ts | persistence.rs | **MIGRATED** | All present |
| 7 Rust-specific tables | 鈥?| persistence.rs | **MIGRATED** | event_log, command_dedupe, tool_calls, permission_* |
| SCHEMA_VERSION = 18 | Database.ts | persistence.rs:8 | **MIGRATED** | user_version = 18 |
| WAL mode | Database.ts | persistence.rs:8 PRAGMA | **MIGRATED** | |
| Single-writer enforcement | 鈥?| DbOwner::with_transaction | **MIGRATED** | BEGIN IMMEDIATE; test at persistence.rs:915 |

### F-012 Permission System

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| request / resolve lifecycle | PermissionSystem.ts | command.rs handle_permission_* | **MIGRATED** | GS-011 pass |
| Mode change + grant revocation | PermissionStore.ts | handle_permission_set_mode | **MIGRATED** | GS-012 pass; DELETEs grants, bumps generation |
| Pending preservation across crash | 鈥?| permission_requests table | **MIGRATED** | GS-013 pass; snapshot includes pending |
| Permission lease validation | PermissionStore.ts | sidecar.rs + command.rs preflight | **MIGRATED** | Sidecar lease scope/expiry validation exists, and router preflight enforces scoped grants before native/direct process side effects |
| Tool scope filtering | PermissionStore.ts | command.rs permission_grants.scope | **MIGRATED** | Permission requests can carry path/cwd/scope, grants persist scope, and router rejects out-of-scope native tools, terminal/REPL/MCP, and worktree operations before execution |

### F-013 LLM Abstraction / Routing

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| LlmProvider trait | ContentGenerator.ts | llm.rs:18-21 | **MIGRATED** | generate / generate_stream |
| Streaming normalization | ContentGenerationPipeline.ts | StreamEvent enum | **MIGRATED** | ThinkingDelta/TextDelta/ToolCallDelta/Usage/Finished |
| Provider registry | Client.ts LLMClientManager | llm.rs::ProviderRegistry + daemon RuntimeConfig | **MIGRATED** | Deterministic registration-order resolution plus daemon-configured external Rust providers with model cost/capability/context-window metadata |
| Model routing | ModelGateway.ts | LlmRouter::route_stream | **MIGRATED** | Retry/circuit/fallback chain plus provider/model metadata for cost and tool-capability routing; unsupported models and non-retryable stops tested |
| Retry engine | RetryEngine.ts | llm.rs::RetryConfig | **MIGRATED** | Full-jitter retry, max_retries_hint consumed |
| Circuit breaker | CircuitBreaker.ts | llm.rs::CircuitBreaker | **MIGRATED** | Threshold/reset/manual reset tested |
| Fallback chain | LlmGuard.ts fallback models | LlmRouter::with_fallback_chain | **MIGRATED** | Retry-exhausted and circuit-open fallback tested |
| Usage extraction | usageExtractor.ts | StreamEvent::Usage | **MIGRATED** | Normalized TokenUsage written to DB |

### F-014 Tool Registry

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Tool definition schema | Tool.ts | tool.rs::ToolDefinition | **MIGRATED** | name, description, parameters, is_native |
| Registry add/get | Registry.ts | tool.rs::ToolRegistry | **MIGRATED** | Native registry connected to CommandRouter tool.call |
| Tool permission check | 鈥?| command.rs + tool.rs | **MIGRATED** | High-risk native calls and scoped grants are denied before side effects; scoped rejection regression tested |

### F-015 Native Tools

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| file_read | FileRead.ts | tool.rs | **MIGRATED** | Offset/limit and traversal guard tested |
| file_create / file_write | FileCreate.ts | tool.rs + command.rs | **MIGRATED** | Permission-gated before side effects |
| structured_patch | StructuredPatchTool.ts | tool.rs + command.rs | **MIGRATED** | Rust-canonical breaking decision: exact replace and line_replace only. Ambiguous fuzzy patching is rejected instead of guessed; traversal/ambiguity guards and `file_write` grant before side effects are tested |
| list_dir | 鈥?| tool.rs | **MIGRATED** | Recursive bounded listing tested |
| glob | GlobTool.ts | tool.rs | **MIGRATED** | Native glob matcher tested |
| code_search | CodeSearchTool.ts | tool.rs | **MIGRATED** | Recursive text search tested |
| git_* | GitTool.ts | tool.rs + command.rs | **MIGRATED** | Native allowlisted git read/write operations cover status/diff/log/show/branch/tag/remote/rev-parse plus add/commit/checkout/switch/push/restore/reset; write ops require scoped `git_write` grant and unsupported subcommands are rejected before spawn |
| shell | Shell.ts | tool.rs + command.rs | **MIGRATED** | Native one-shot shell execution with timeout, permission grant, budget gate, and sanitized events; persistent interactive use is exposed through `terminal.*` |
| attempt_completion | 鈥?| tool.rs / agent.rs | **MIGRATED** | AgentLoop completion path tested |
| send_message | 鈥?| tool.rs + command.rs | **MIGRATED** | Rust-native structured message tool is registered, safe, and command-routed through `tool.call` |

### F-016 Sidecar Tools

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| browser_action / visual_verify | browser tools | sidecar.rs + tool-host-protocol | **MIGRATED** | Protocol solid; no built-in browser sidecar binary |
| OCR | ocr tools | document_tools.rs + command.rs `ocr.extract_text` | **MIGRATED** | Rust Core invokes local `tesseract` with timeout, owned PID registry, typed missing-dependency errors, sanitized events, and text/layout metadata when output is available |
| Office/PDF tools | parse_file, office tools | document_tools.rs + command.rs `parse_file` | **MIGRATED** | Rust-canonical production boundary: native UTF-8 extraction is built in; PDF uses local `pdftotext`; Office conversion is an explicit `soffice` dependency boundary with typed errors. Successful parses include text/layout metadata |
| MCP server host | mcp tools | mcp_bridge.rs + command.rs `mcp.*` | **MIGRATED** | Rust Core stdio JSON bridge plus lifecycle commands `mcp.server_start/list_tools/call_tool/server_stop` initialize configured local MCP commands, cache tool registry in durable session state, enforce `mcp` grants, and sanitize events |
| Node/Python REPL | repl tools | repl.rs + command.rs `repl.*` | **MIGRATED** | Rust Core supports one-shot `repl.eval` and persistent `repl.create/send/read/kill` sessions using local Node/Python runtimes, `repl` permission grants, scoped cwd checks, typed missing-runtime errors, supervised process cleanup, and sanitized events |
| Terminal PTY | terminal tools | terminal.rs + command.rs `terminal.*` | **MIGRATED** | Rust-headless canonical boundary is supervised process sessions, not a cross-platform PTY emulator. `terminal.create/send/read/kill` enforce `terminal` grants, durable `terminal_sessions`, owned PID registry, timeout/cancel, and sanitized input events; resize/control-sequence UI parity is intentionally adapter-side |
| Tool-host protocol (JSONL stdin/stdout) | 鈥?| tool-host-protocol crate | **MIGRATED** | SidecarRequest/Response types |

### F-017 Workflow Engine

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| workflow.execute | WorkflowEngine.ts execute() | handle_workflow_execute | **MIGRATED** | Rust canonical executor is deterministic supervised synchronous DAG traversal around durable `workflow_node_state`; production safety comes from per-node idempotent state, pause/resume/cancel/recovery commands, and schedule/daemon command dispatch rather than an implicit async traversal thread |
| DAG traversal | WorkflowEngine.ts topology | workflow.rs::plan_dag_execution | **MIGRATED** | Topological ordering and cycle rejection tested |
| Node executors (tool/llm/agent/data) | WorkflowEngine.ts node handlers | command.rs execute_workflow_node | **MIGRATED** | Tool nodes use native registry + permission preflight; LLM nodes use Rust router; agent nodes run AgentLoop |
| pause / resume / cancel | WorkflowEngine.ts | handle_workflow_transition | **MIGRATED** | State machine correct; GS-015 pass |
| Per-node durable progress | 鈥?| workflow_node_state + workflow_execution_logs | **MIGRATED** | Persists status/output/error/attempt/generation/timestamps; workflow.list exposes node_states |
| Recovery: running鈫抪aused on boot | 鈥?| DbOwner::recover_orphans + handle_workflow_list recover_running | **MIGRATED** | Boot sweep returns structured counts; GS-016/028 and daemon GS-026 pass |
| Retry on node failure | WorkflowEngine.ts retry config | command.rs workflow retry policy | **MIGRATED** | Retryable LLM node errors retry up to max_attempts; non-retryable nodes stop deterministically |

### F-018 Context / Checkpoint / Compression

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Token budget → compaction | ContextManager.ts | handle_llm_call budget check | **MIGRATED** | Rust canonical preflight uses metadata/context-window limits plus deterministic prompt estimates before provider calls, commits measured global/per-agent usage to RuntimeManager, and emits compaction events/reduces active_context_projection after measured overage |
| runtime.compact | checkpoint/compression | handle_runtime_compact | **MIGRATED** | Writes active context projection, retained message IDs, and deterministic summary while retaining original rows; replay/snapshot/no-fact-loss tests pass |
| In-memory context window | ContextManager.ts | context.rs + agent.rs | **MIGRATED** | ContextManager stores replay history, exposes reduced active projection, and AgentLoop persists/reloads active windows across router restart |
| Conversation persistence | 鈥?| leader_conversation table | **MIGRATED** | GS-027/033 pass |
| Compaction preserves originals | compress/ | 鈥?| **MIGRATED** | GS-034: originals retained |

### F-019 Blackboard / Graph

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| blackboard.intent lifecycle | LeaderBlackboard.ts | handle_blackboard_intent_* | **MIGRATED** | GS-035 pass; create/claim/resolve |
| graph_nodes / graph_edges tables | blackboard/ | persistence.rs | **MIGRATED** | Schema present |
| Assumption tracking | assumptions tools | command.rs assumption.* + assumptions table | **MIGRATED** | create/list/verify/falsify persist lifecycle state and evidence |
| Graph query tools | graph tools | command.rs graph.query | **MIGRATED** | Lists nodes/edges with kind/status filters |

### F-020 Scheduled Tasks

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| Cron scheduling | ScheduledTaskManager.ts | schedule.rs + command.rs schedule.* + daemon ScheduleTicker | **MIGRATED** | Durable schedule create/list plus due-fire into dispatchable tasks; daemon-owned background ticker dispatches through router. Rust canonical parser supports aliases, `every` durations, and standard 5-field numeric cron with wildcards/lists/ranges/steps |
| Manual fire | ScheduledTaskManager.ts | handle_schedule_fire | **MIGRATED** | `schedule.fire` creates normal scheduled tasks and updates last run state |
| System task auto-run | 鈥?| lingxiao-core-daemon ScheduleTicker | **MIGRATED** | Optional `background_schedule_tick_ms` runtime config starts a daemon thread that dispatches `schedule.fire_due`; shutdown handle tested |
| schedule.rs module | 鈥?| schedule.rs | **MIGRATED** | Deterministic next-run calculation for aliases, `every` durations, and 5-field numeric cron. Named months/weekdays and Quartz extensions are intentionally outside the Rust Core canonical contract |

### F-021 Team / Collaboration

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| team.send / mark_read | TeamMailbox.ts | handle_team_* | **MIGRATED** | GS-036 pass |
| teams / team_members tables | TeamProtocol.ts | persistence.rs | **MIGRATED** | |
| Team message routing | TeamMailbox.ts | command.rs `team.send`/`team.poll`/`team.mark_read` | **MIGRATED** | Durable team/member routing with urgency ordering, unread polling, optional atomic mark-read, and read receipts |

### F-022 Memory / FTS

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| memory_entry / memory_fts tables | MemoryFTS.ts | persistence.rs | **MIGRATED** | memory_fts FTS5 virtual table; upsert keeps table in sync |
| FTS search | MemoryFTS.ts search() | command.rs memory.search | **MIGRATED** | MATCH query with scope/scope_id/type filters; tested |
| Embedding upsert | memory tools | command.rs embedding.upsert | **MIGRATED** | Deterministic little-endian f32 BLOB storage; tested |
| Similarity query | memory tools | command.rs embedding.search | **MIGRATED** | Pure-Rust cosine similarity; skips dimension mismatches; tested |

### F-023 Tracing / Metrics / Usage

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| token_usage tracking | TokenTracker.ts | handle_llm_call writes | **MIGRATED** | Every LLM call writes row |
| llm_gateway_requests log | 鈥?| handle_llm_call | **MIGRATED** | Provider, model, tokens, latency |
| agent_logs | 鈥?| insert_agent_log | **MIGRATED** | Audit trail per agent event |
| traces / execution_trace_events | Tracing.ts | command.rs + persistence.rs | **MIGRATED** | CommandRouter writes sanitized spans and command execution trace events; `trace.timeline` queries both without params/secrets |
| runtime.debug_dump | diagnostics/debug tools | command.rs | **MIGRATED** | Sanitized counts for sessions/tasks/workflows/agents/tool calls; pending permissions preserved without args/secrets |
| Metrics aggregation | Metrics*.ts | command.rs metrics.query | **MIGRATED** | Aggregates sanitized runtime/tool/LLM/agent/task/workflow counters from traces and durable tables with optional session filter; omits params, results, auth, and message bodies |

### F-024 Worktree / Workspace

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| worktrees table | WorktreeService.ts | persistence.rs | **MIGRATED** | Schema present |
| Worktree create/delete | WorktreeManager.ts | command.rs worktree.* | **MIGRATED** | `worktree.create/list/delete` persist rows and invoke Rust-owned `git worktree` commands; create/delete require `git_write` grant before filesystem side effects |
| Git isolation | WorktreeService.ts | command.rs + worktrees table | **MIGRATED** | Worktree operations are bound to repo_root/path rows, reject repo_root reuse, and delete only active persisted worktrees |

### F-025 Runtime Guards / Resource Budget

| Item | TS Source | Rust Module | Status | Notes |
|------|-----------|-------------|--------|-------|
| RuntimeManager struct | ResourceBudgetService.ts | runtime.rs + command.rs | **MIGRATED** | Wired for sidecar/worker admission/release, native tool slot release, file-write rejection before side effects, cumulative token accounting, and optional per-agent token quotas |
| Token budget enforcement | — | handle_llm_call budget check + RuntimeManager | **MIGRATED** | Deterministic prompt estimate and provider/model context-window metadata reject before provider calls; measured usage is committed to global and per-agent counters |
| Sidecar budget enforcement | 鈥?| handle_tool_call + RuntimeManager | **MIGRATED** | Per-call budget plus RuntimeManager sidecar reservation gate |
| Max parallel sidecars | RuntimeGuards.ts | RuntimeManager | **MIGRATED** | CommandRouter rejects before sidecar start when RuntimeManager capacity is exhausted |
| Resource release | 鈥?| RuntimeManager::release_sidecar | **MIGRATED** | Decrements counters |

---

## Golden Scenario Coverage

| GS | Domain | Phase | Test Exists | Status |
|----|--------|-------|-------------|--------|
| GS-001..004 | Session lifecycle | 2 | 鉁?command.rs | PASSING |
| GS-005..007 | TaskBoard | 2 | 鉁?command.rs | PASSING |
| GS-008..010 | Agent/Leader | 3 | 鉁?command.rs + agent.rs | PASSING (state machine plus workflow-backed AgentLoop execution paths) |
| GS-011..013 | Permission | 2 | 鉁?command.rs | PASSING |
| GS-014..016 | Workflow | 3/4 | 鉁?command.rs + workflow.rs | PASSING (DAG ordering plus real tool/LLM/agent/data node executors) |
| GS-017 | Rust-native tool | 3 | 鉁?command.rs + tool.rs | PASSING |
| GS-018 | Sidecar timeout/cancel | 3 | 鉁?command.rs + sidecar.rs | PASSING |
| GS-019 | LLM stream text/thinking/tool | 3 | 鉁?command.rs | PASSING |
| GS-020..021 | Event log replay | 1 | 鉁?event_log.rs | PASSING |
| GS-022..023 | Snapshot/delta reconnect | 1 | 鉁?projection.rs | PASSING |
| GS-024 | SQLite schema parity | 1 | implicit (DDL) | PASSING |
| GS-025 | Core unique SQLite writer | 1 | 鉁?persistence.rs | PASSING |
| GS-026 | Kill鈫抮estart鈫抮esume | 4 | 鉁?lingxiao-core-daemon stdio_test.rs | PASSING |
| GS-027 | Context not lost on crash | 4 | 鉁?command.rs (conv_list) | PASSING |
| GS-028 | Workflow not stuck running | 4 | 鉁?command.rs (recover_running) | PASSING |
| GS-029..030 | Resource budget | 4 | 鉁?runtime.rs + command.rs | PASSING (sidecar, native tool slot, file-write budget, token preflight/accounting, token compaction events, scheduled retention sweep) |
| GS-031..032 | MessageBus backpressure | 3 | 鉁?bus.rs + command.rs | PASSING (standalone and router-integrated) |
| GS-033..034 | Context/conversation | 3/4 | 鉁?command.rs | PASSING |
| GS-035..036 | Blackboard/team | 5 | 鉁?command.rs | PASSING |
| GS-037 | Memory FTS/embedding | 5 | 鉁?command.rs | PASSING |
| GS-038..039 | Workflow node execution | 5 | 鉁?command.rs | PASSING |
| GS-040 | Recovery diagnostics | 5 | 鉁?command.rs + persistence.rs | PASSING |
| GS-041 | RuntimeManager sidecar gate | 5 | 鉁?command.rs | PASSING |
| GS-042 | Blackboard graph query | 5 | 鉁?command.rs | PASSING |
| GS-043..044 | Workflow durable node state/retry | 5 | 鉁?command.rs | PASSING |
| GS-045 | AgentLoop tool observe/final E2E | 5 | 鉁?agent.rs | PASSING |
| GS-046 | Headless user鈫抪lan鈫抋gent tool鈫抐inal E2E | 5 | 鉁?command.rs + daemon stdio_test.rs | PASSING (includes daemon `leader.run` native tool dispatch) |

**Passing: 45/46 | Gaps: GS-024/025 (implicit)**

---

## Implementation Work Plan

### Packet 1: Correctness Fixes (unblock production)

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P1-1 | Add generation gate to `handle_task_terminal` | G-5 | DONE |
| P1-2 | Add boot-time orphan recovery sweep to daemon startup | G-8 | DONE |
| P1-3 | Wire `RuntimeManager` into `CommandRouter` | G-25 | DONE |
| P1-4 | Add `GS-026` kill鈫抮estart鈫抮esume daemon test | GS-026 | DONE |

### Packet 2: LLM Resilience

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P2-1 | Implement `RetryEngine` with full-jitter backoff | G-3 | DONE |
| P2-2 | Implement `CircuitBreaker` per-provider | G-3 | DONE |
| P2-3 | Wire retry + circuit breaker into `route_llm_stream` | G-3 | DONE |
| P2-4 | Add fallback chain to `LlmRouter` | G-3 | DONE |
| P2-5 | Consume `RequestOptions.max_retries_hint` | G-3 | DONE |

### Packet 3: Native Tool Ecosystem

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P3-1 | `file_read` native tool executor | G-4 | DONE |
| P3-2 | `file_create` / `file_write` native tool executor | G-4 | DONE |
| P3-3 | `list_dir` / `glob` native tool executor | G-4 | DONE |
| P3-4 | `code_search` native tool executor | G-4 | DONE |
| P3-5 | `shell` native tool with permission gate + timeout | G-4 | DONE |
| P3-6 | `git_*` native tool executor | G-4 | DONE |
| P3-7 | `attempt_completion` / `send_message` tools | G-4 | DONE |
| P3-8 | Wire ToolRegistry into CommandRouter; replace caller-supplied result path | G-4 | DONE |

### Packet 4: Agent Execution Loop

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P4-1 | `AgentLoop`: supervised task, tool-call/observe/act cycle | G-1 | DONE (Rust canonical uses supervised OS threads; native tool schemas injected) |
| P4-2 | `AgentPool`: spawn / heartbeat / auto-crash detection | G-1, G-6 | DONE |
| P4-3 | `LeaderOrchestrator`: think/plan/dispatch loop with LLM | G-1 | DONE |
| P4-4 | Wire agent pool into daemon startup and command handlers | G-1 | DONE (`agent.spawn run:true` headless/daemon route) |

### Packet 5: Workflow Node Executors

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P5-1 | `WorkflowExecutor`: supervised DAG traversal | G-2 | DONE (Rust canonical synchronous traversal with durable per-node state/recovery) |
| P5-2 | Tool node executor | G-2 | DONE |
| P5-3 | LLM node executor | G-2 | DONE |
| P5-4 | Agent node executor | G-2 | DONE |
| P5-5 | Per-node durable progress (`workflow_node_state` table) | G-2 | DONE |
| P5-6 | Retry on node failure | G-2 | DONE |

### Packet 6: Context Compaction

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P6-1 | Real context window reduction in `handle_runtime_compact` | G-7 | DONE |
| P6-2 | Token-budget compaction with actual message summarization | G-7 | DONE (LLM-backed summarization when router configured, deterministic fact-preserving fallback otherwise) |
| P6-3 | Wire `context.rs::ContextManager` into agent LLM calls | G-7 | DONE |
| P6-4 | Persist/replay agent context across router/daemon restart | G-7 | DONE |

### Packet 7: Memory / FTS / Embedding (P2)

| # | Task | Gap | Complexity |
|---|------|-----|------------|
| P7-1 | `CREATE VIRTUAL TABLE memory_fts USING fts5(...)` migration | G-10 | DONE |
| P7-2 | `memory.upsert` / `memory.search` command handlers | G-10 | DONE |
| P7-3 | Embedding storage + similarity search (sqlite-vec or pure-Rust) | G-10 | DONE |

---

## Breaking Change Register

| BC | Description | TS Path Affected | Decision |
|----|-------------|-----------------|----------|
| BC-001 | Core protocol replaces ACP/REST/SSE event names | All TS clients | INTENTIONAL 鈥?adapter layer responsibility |
| BC-002 | No Node worker IPC; agents are Rust async tasks | WorkerProcessEntry.ts | INTENTIONAL |
| BC-003 | No multi-process SQLite writes | DB.ts concurrent access | INTENTIONAL |
| BC-004 | in-process EventEmitter replaced by durable event_log | EventEmitter.ts | INTENTIONAL |
| BC-005 | `session_state` / `agent_state` runtime snapshot tables not populated as TS did | DB.ts | INTENTIONAL 鈥?projection rebuilds from event log |

---

## Ledger Maintenance

This ledger is updated as implementation packets complete. After each packet:
1. Update relevant rows from STUB/MISSING 鈫?PARTIAL/MIGRATED.
2. Record new test name in the GS table.
3. Run `cargo test --workspace` and confirm green.
