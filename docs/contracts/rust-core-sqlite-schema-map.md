# Rust Core SQLite Schema 盘点与映射策略

> Phase 0 交付物。对应任务 P0-2：SQLite schema 盘点与 Rust 映射策略。
>
> 来源：`src/core/Database.ts` + `src/memory/MemoryFTS.ts` + `src/core/Tracing.ts` (`SqliteSpanSink`)。
>
> 原则：不迁移现有本地 SQLite 数据；Rust Core 的 SQLite schema 与当前 TS 版本保持一致或可机械映射；Rust Core 是唯一 writer。

---

## 1. 当前 Schema Version

| 来源 | 常量 | 值 |
|---|---|---|
| `Database.ts:733` | `DatabaseManager.SCHEMA_VERSION` | **15** |
| 存储方式 | `PRAGMA user_version` | integer `15` |

Rust Core 验收：对比 `PRAGMA user_version` 即可验证版本号一致。

---

## 2. Schema 功能域分组

### 2.1 核心运行时 (Core Runtime)

#### `sessions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | session UUID |
| `created_at` | REAL | | epoch seconds |
| `workspace` | TEXT | | workspace path |
| `user_request` | TEXT | | JSON string or plain text |
| `status` | TEXT | DEFAULT 'active' | e.g. active, deleted |
| `summary` | TEXT | nullable | auto-generated summary |
| `name` | TEXT | nullable | user-editable name |

无显式索引（PK 自带）。

#### `tasks`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK, composite | task UUID |
| `session_id` | TEXT | PK, composite | FK to sessions |
| `subject` | TEXT | | |
| `description` | TEXT | | JSON or plain text |
| `context` | TEXT | nullable | |
| `status` | TEXT | | |
| `exit_reason` | TEXT | nullable | |
| `run_generation` | INTEGER | NOT NULL DEFAULT 0 | |
| `agent_type` | TEXT | | |
| `blocked_by` | TEXT | | JSON array of strings |
| `blocks` | TEXT | | JSON array of strings |
| `assigned_agent` | TEXT | | |
| `preferred_agent_name` | TEXT | nullable | |
| `working_directory` | TEXT | | 应用层写入时 `|| ''` 兜底为空串（`Database.ts:1243/1279`），DDL 无 DEFAULT |
| `write_scope` | TEXT | | JSON array of strings |
| `result` | TEXT | nullable | JSON or plain text |
| `blocked_reason` | TEXT | nullable | |
| `orchestration` | TEXT | nullable | JSON object |
| `origin` | TEXT | nullable | |
| `goal` | TEXT | nullable | |
| `task_type` | TEXT | nullable | |
| `created_at` | REAL | | |
| `updated_at` | REAL | | |

索引：
- `idx_tasks_session` ON `tasks(session_id)`

PK: `(id, session_id)`

#### `messages`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | | |
| `sender` | TEXT | | |
| `recipient` | TEXT | | |
| `content` | TEXT | | JSON or plain text |
| `timestamp` | REAL | | |

索引：
- `idx_messages_session` ON `messages(session_id)`

#### `agent_logs`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | | |
| `agent_id` | TEXT | | |
| `agent_name` | TEXT | | |
| `agent_role` | TEXT | | |
| `task_id` | TEXT | | |
| `event_type` | TEXT | | |
| `content` | TEXT | | |
| `token_usage` | TEXT | nullable | JSON object |
| `action` | TEXT | nullable | |
| `details` | TEXT | nullable | |
| `timestamp` | REAL | | |

索引：
- `idx_agent_logs_session` ON `agent_logs(session_id)`

---

### 2.2 运行时残留 (Runtime Residue) — 不应作为 Rust Canonical State

#### `agent_state`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `session_id` | TEXT | UNIQUE(composite) | |
| `agent_id` | TEXT | UNIQUE(composite) | |
| `agent_name` | TEXT | | |
| `agent_role` | TEXT | | |
| `task_id` | TEXT | | |
| `status` | TEXT | | |
| `stopped` | INTEGER | | |
| `iteration` | INTEGER | | |
| `timestamp` | REAL | | |

索引：
- `idx_agent_state_session` ON `agent_state(session_id)`

UNIQUE: `(session_id, agent_id)`

**策略：legacy residue**。旧运行时快照，Rust Core 不应视其为 canonical state。其语义应迁移到 Rust 的 `agent::AgentState` 枚举 + event log。

#### `session_state`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `session_id` | TEXT | UNIQUE(composite) | |
| `key` | TEXT | UNIQUE(composite) | |
| `value` | TEXT | | JSON |
| `timestamp` | REAL | | |

UNIQUE: `(session_id, key)`

**策略：legacy residue**。旧运行时 KV 快照。Rust Core 应将活跃的 session state 纳入 `session::SessionState` canonical model；冷门 key 暂留作 runtime residue。

---

### 2.3 对话/会话 (Conversation)

#### `leader_conversation`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | | |
| `role` | TEXT | | system/user/assistant/tool |
| `content` | TEXT | | JSON or plain text |
| `tool_calls` | TEXT | nullable | JSON array |
| `tool_call_id` | TEXT | nullable | |
| `thinking_blocks` | TEXT | nullable | JSON array |
| `timestamp` | REAL | | |

索引：
- `idx_leader_conv_session` ON `leader_conversation(session_id)`

#### `agent_conversation`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | | |
| `agent_id` | TEXT | | |
| `agent_name` | TEXT | | |
| `role` | TEXT | | |
| `content` | TEXT | | |
| `tool_calls` | TEXT | nullable | |
| `tool_call_id` | TEXT | nullable | |
| `thinking_blocks` | TEXT | nullable | |
| `timestamp` | REAL | | |

索引：
- `idx_agent_conv_session` ON `agent_conversation(session_id, agent_id)`

**策略（两表一致）**：canonical state + event log。conversation 是事实源，Rust Core 将其纳入 ordered event log + snapshot projection。compaction 只产生 projection，不覆盖原始事实。

---

### 2.4 用量/追踪 (Usage & Tracing)

#### `token_usage`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | | |
| `agent_id` | TEXT | | |
| `agent_name` | TEXT | | |
| `model_name` | TEXT | | |
| `prompt_tokens` | INTEGER | | |
| `completion_tokens` | INTEGER | | |
| `total_tokens` | INTEGER | | |
| `cache_read_tokens` | INTEGER | DEFAULT 0 | |
| `cache_creation_tokens` | INTEGER | DEFAULT 0 | |
| `timestamp` | REAL | | |

索引：
- `idx_token_usage_session` ON `token_usage(session_id)`

#### `llm_gateway_requests`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `trace_id` | TEXT | NOT NULL | |
| `session_id` | TEXT | nullable | |
| `agent_id` | TEXT | nullable | |
| `agent_name` | TEXT | nullable | |
| `key_id` | TEXT | nullable | |
| `key_label` | TEXT | nullable | |
| `profile` | TEXT | nullable | |
| `requested_model` | TEXT | nullable | |
| `selected_model` | TEXT | nullable | |
| `final_model` | TEXT | nullable | |
| `provider` | TEXT | nullable | |
| `status` | TEXT | NOT NULL | success/failed/rate_limited/auth_failed |
| `prompt_tokens` | INTEGER | DEFAULT 0 | |
| `completion_tokens` | INTEGER | DEFAULT 0 | |
| `total_tokens` | INTEGER | DEFAULT 0 | |
| `cache_read_tokens` | INTEGER | DEFAULT 0 | |
| `cache_creation_tokens` | INTEGER | DEFAULT 0 | |
| `latency_ms` | INTEGER | DEFAULT 0 | |
| `attempts_json` | TEXT | nullable | |
| `error_kind` | TEXT | nullable | |
| `error_message` | TEXT | nullable | |
| `created_at` | REAL | NOT NULL | |

索引：
- `idx_llm_gateway_trace` ON `llm_gateway_requests(trace_id)`
- `idx_llm_gateway_session` ON `llm_gateway_requests(session_id, created_at)`
- `idx_llm_gateway_key` ON `llm_gateway_requests(key_id, created_at)`

#### `traces`（定义于 `Database.ts` 和 `Tracing.ts`，结构相同）

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `trace_id` | TEXT | NOT NULL | |
| `span_id` | TEXT | PK | |
| `parent_span_id` | TEXT | nullable | |
| `operation` | TEXT | NOT NULL | |
| `start_ts` | INTEGER | NOT NULL | |
| `end_ts` | INTEGER | nullable | |
| `status` | TEXT | DEFAULT 'ok' | |
| `attributes` | TEXT | nullable | JSON |
| `session_id` | TEXT | nullable | |
| `agent_id` | TEXT | nullable | |

索引：
- `idx_traces_trace` ON `traces(trace_id)`
- `idx_traces_session` ON `traces(session_id, start_ts)`

> 注意：`traces` 表在两个地方定义（`Database.ts:414-425` 和 `Tracing.ts:197-208`），结构完全一致。Rust Core 只需要一份定义。

#### `execution_trace_events`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `project_root` | TEXT | NOT NULL | |
| `session_id` | TEXT | nullable | |
| `task_id` | TEXT | nullable | |
| `agent_id` | TEXT | nullable | |
| `agent_name` | TEXT | nullable | |
| `agent_role` | TEXT | nullable | |
| `task_type` | TEXT | nullable | |
| `status` | TEXT | NOT NULL | |
| `duration_ms` | INTEGER | NOT NULL DEFAULT 0 | |
| `files_changed` | TEXT | NOT NULL DEFAULT '[]' | JSON array |
| `error_signature` | TEXT | nullable | |
| `fix_pattern` | TEXT | nullable | |
| `verification` | TEXT | nullable | |
| `metadata` | TEXT | nullable | JSON |
| `created_at` | REAL | NOT NULL | |

索引：
- `idx_execution_trace_project` ON `execution_trace_events(project_root, created_at)`
- `idx_execution_trace_task` ON `execution_trace_events(session_id, task_id)`
- `idx_execution_trace_status` ON `execution_trace_events(project_root, status)`

#### `execution_project_models`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `project_root` | TEXT | PK | |
| `model_json` | TEXT | NOT NULL | JSON |
| `rebuilt_at` | REAL | NOT NULL | |

**策略（本组所有表）**：audit log。保留 token usage、cache usage、latency、attempts、trace/span 语义。Rust Core 统一 provider usage normalization。`execution_project_models` 是 projection cache。

---

### 2.5 工作流/调度 (Workflow & Schedule)

#### `workflows`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `name` | TEXT | NOT NULL | |
| `description` | TEXT | nullable | |
| `workspace` | TEXT | nullable | |
| `nodes` | TEXT | nullable | JSON |
| `edges` | TEXT | nullable | JSON |
| `version` | TEXT | DEFAULT '1.0.0' | |
| `config` | TEXT | nullable | JSON |
| `tags` | TEXT | nullable | JSON |
| `created_at` | REAL | nullable | |
| `updated_at` | REAL | nullable | |
| `created_by` | TEXT | nullable | |

#### `workflow_executions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `workflow_id` | TEXT | NOT NULL | FK -> workflows(id) ON DELETE CASCADE |
| `session_id` | TEXT | NOT NULL | |
| `status` | TEXT | NOT NULL | |
| `start_time` | INTEGER | NOT NULL | |
| `end_time` | INTEGER | nullable | |
| `context` | TEXT | nullable | JSON |
| `error` | TEXT | nullable | |
| `created_at` | INTEGER | NOT NULL | |

索引：
- `idx_workflow_executions_workflow` ON `workflow_executions(workflow_id)`
- `idx_workflow_executions_session` ON `workflow_executions(session_id)`

#### `workflow_execution_logs`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `execution_id` | TEXT | NOT NULL | FK -> workflow_executions(id) ON DELETE CASCADE |
| `timestamp` | INTEGER | NOT NULL | |
| `level` | TEXT | NOT NULL | |
| `node_id` | TEXT | nullable | |
| `message` | TEXT | NOT NULL | |
| `data` | TEXT | nullable | JSON |

索引：
- `idx_workflow_logs_execution` ON `workflow_execution_logs(execution_id)`
- `idx_workflow_logs_timestamp` ON `workflow_execution_logs(timestamp)`

#### `scheduled_tasks`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `session_id` | TEXT | NOT NULL | |
| `cron` | TEXT | NOT NULL | |
| `prompt` | TEXT | NOT NULL | |
| `task_type` | TEXT | NOT NULL DEFAULT 'prompt' | prompt/workflow |
| `intensity` | TEXT | NOT NULL DEFAULT 'normal' | gentle/normal/aggressive/critical |
| `audience` | TEXT | NOT NULL DEFAULT 'personal' | personal/team/ops/customer |
| `workflow_id` | TEXT | nullable | |
| `workflow_input` | TEXT | nullable | JSON |
| `last_execution_id` | TEXT | nullable | |
| `last_error` | TEXT | nullable | |
| `source_type` | TEXT | nullable | workflow_trigger |
| `source_id` | TEXT | nullable | |
| `source_node_id` | TEXT | nullable | |
| `recurring` | INTEGER | NOT NULL DEFAULT 1 | boolean |
| `durable` | INTEGER | NOT NULL DEFAULT 0 | boolean |
| `enabled` | INTEGER | NOT NULL DEFAULT 1 | boolean |
| `last_run_at` | REAL | nullable | |
| `next_run_at` | REAL | nullable | |
| `created_at` | REAL | NOT NULL | |

索引：
- `idx_scheduled_tasks_session` ON `scheduled_tasks(session_id)`
- `idx_scheduled_tasks_next` ON `scheduled_tasks(next_run_at)`
- `idx_scheduled_tasks_workflow` ON `scheduled_tasks(workflow_id)` WHERE `workflow_id IS NOT NULL` (partial index)
- `idx_scheduled_tasks_source` ON `scheduled_tasks(source_type, source_id, source_node_id)` WHERE `source_type IS NOT NULL` (partial index)

#### `health_reports`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | NOT NULL | |
| `timestamp` | REAL | NOT NULL | |
| `source` | TEXT | NOT NULL | |
| `has_critical` | INTEGER | NOT NULL DEFAULT 0 | boolean |
| `decisions` | TEXT | NOT NULL | JSON |

索引：
- `idx_hr_session_ts` ON `health_reports(session_id, timestamp)`

**策略（本组所有表）**：canonical state (workflows, scheduled_tasks) + event log (workflow_executions, workflow_execution_logs) + audit log (health_reports)。`workflow_executions` 需要补 per-node durable progress，避免 crash 后永久 `running`。`scheduled_tasks` 由 Rust scheduler 持有触发权威。

---

### 2.6 工作树 (Worktree)

#### `worktrees`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `name` | TEXT | NOT NULL | |
| `repo_root` | TEXT | NOT NULL | |
| `path` | TEXT | NOT NULL UNIQUE | |
| `branch` | TEXT | NOT NULL | |
| `base_branch` | TEXT | NOT NULL | |
| `session_id` | TEXT | nullable | |
| `task_id` | TEXT | nullable | |
| `status` | TEXT | NOT NULL DEFAULT 'active' | |
| `created_at` | REAL | NOT NULL | |
| `updated_at` | REAL | NOT NULL | |
| `last_error` | TEXT | nullable | |

索引：
- `idx_worktrees_session` ON `worktrees(session_id)`
- `idx_worktrees_repo` ON `worktrees(repo_root)`
- `idx_worktrees_status` ON `worktrees(status)`

**策略**：canonical state。但 P2 迁移，Rust Core 初期可暂留 `worktrees` 作为 projection cache 直到 `lingxiao-core::workspace` 模块就绪。

---

### 2.7 黑板/图谱 (Blackboard & Graph)

#### `graph_nodes`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK, composite | |
| `session_id` | TEXT | PK, composite | |
| `kind` | TEXT | NOT NULL | |
| `title` | TEXT | NOT NULL | |
| `content` | TEXT | NOT NULL | |
| `tags` | TEXT | NOT NULL DEFAULT '[]' | JSON array |
| `created_by` | TEXT | NOT NULL | |
| `created_at` | REAL | NOT NULL | |
| `superseded_by` | TEXT | nullable | |
| `confidence` | TEXT | nullable | |
| `intent_status` | TEXT | nullable | |
| `priority` | INTEGER | nullable | |
| `evidence` | TEXT | nullable | |
| `intent_from` | TEXT | nullable | |
| `intent_to` | TEXT | nullable | |
| `contract_allowed_scope` | TEXT | nullable | |

索引：
- `idx_graph_nodes_session` ON `graph_nodes(session_id)`
- `idx_graph_nodes_kind` ON `graph_nodes(session_id, kind)`
- `idx_graph_nodes_status` ON `graph_nodes(session_id, intent_status)`

PK: `(id, session_id)`

#### `graph_edges`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK, composite | |
| `session_id` | TEXT | PK, composite | |
| `from_node_id` | TEXT | NOT NULL | |
| `to_node_id` | TEXT | NOT NULL | |
| `edge_type` | TEXT | NOT NULL | |
| `created_at` | REAL | NOT NULL | |
| `created_by` | TEXT | NOT NULL | |
| `metadata` | TEXT | nullable | JSON |

索引：
- `idx_graph_edges_from` ON `graph_edges(session_id, from_node_id)`
- `idx_graph_edges_to` ON `graph_edges(session_id, to_node_id)`
- `idx_graph_edges_type` ON `graph_edges(session_id, edge_type)`

PK: `(id, session_id)`

#### `assumptions`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `title` | TEXT | NOT NULL | |
| `content` | TEXT | nullable | |
| `status` | TEXT | NOT NULL DEFAULT 'unverified' | |
| `verification_type` | TEXT | NOT NULL | |
| `verification_target` | TEXT | NOT NULL | |
| `verification_expected` | TEXT | NOT NULL | |
| `verification_actual` | TEXT | nullable | |
| `dependents` | TEXT | NOT NULL DEFAULT '[]' | JSON array |
| `created_by` | TEXT | nullable | |
| `created_at` | REAL | NOT NULL | |
| `verified_at` | REAL | nullable | |
| `falsified_at` | REAL | nullable | |
| `evidence` | TEXT | nullable | |
| `session_id` | TEXT | nullable | |

索引：
- `idx_assumptions_status` ON `assumptions(status, session_id)`
- `idx_assumptions_target` ON `assumptions(session_id, verification_target)`

**策略（本组所有表）**：canonical state。迁移为 Rust blackboard canonical model。graph 状态变化应生成 event。

---

### 2.8 工具注册表 (Tool Registry)

#### `tool_registrations`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | INTEGER | PK AUTOINCREMENT | |
| `session_id` | TEXT | NOT NULL | |
| `tool_name` | TEXT | NOT NULL | |
| `tool_type` | TEXT | NOT NULL DEFAULT 'class' | |
| `tool_description` | TEXT | NOT NULL DEFAULT '' | |
| `tool_schema` | TEXT | NOT NULL DEFAULT '{}' | JSON |
| `registered_at` | REAL | NOT NULL | |

索引：
- `idx_tool_registrations_session` ON `tool_registrations(session_id)`
- `idx_tool_registrations_name` ON `tool_registrations(tool_name)`

UNIQUE: `(session_id, tool_name)`

**策略**：sidecar registry / canonical state。tool registration 语义迁入 Rust registry 或 sidecar registry。P1 迁移。

---

### 2.9 团队协作 (Team/Collaboration)

#### `teams`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `name` | TEXT | PK, composite | |
| `description` | TEXT | nullable | |
| `leader_name` | TEXT | NOT NULL | |
| `members_json` | TEXT | NOT NULL DEFAULT '[]' | JSON array |
| `workspace` | TEXT | NOT NULL | |
| `session_id` | TEXT | PK, composite | |
| `created_at` | REAL | NOT NULL | |
| `active` | INTEGER | NOT NULL DEFAULT 1 | boolean |

索引：
- `idx_teams_session` ON `teams(session_id)`

PK: `(session_id, name)`

#### `team_members`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `name` | TEXT | PK, composite | |
| `team` | TEXT | NOT NULL | |
| `role` | TEXT | NOT NULL | |
| `workspace` | TEXT | NOT NULL | |
| `session_id` | TEXT | PK, composite | |
| `registered_at` | REAL | NOT NULL | |

索引：
- `idx_team_members_team` ON `team_members(team)`
- `idx_team_members_session` ON `team_members(session_id)`

PK: `(session_id, name)`

#### `team_messages`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | |
| `from_team` | TEXT | NOT NULL | |
| `from_member` | TEXT | nullable | |
| `to_team` | TEXT | NOT NULL | |
| `to_member` | TEXT | nullable | |
| `content` | TEXT | NOT NULL | |
| `urgency` | TEXT | NOT NULL DEFAULT 'normal' | |
| `kind` | TEXT | NOT NULL DEFAULT 'normal' | |
| `request_id` | TEXT | nullable | |
| `session_id` | TEXT | NOT NULL | |
| `timestamp` | REAL | NOT NULL | |
| `read_by` | TEXT | NOT NULL DEFAULT '[]' | JSON array |
| `metadata` | TEXT | nullable | JSON |

索引：
- `idx_team_messages_to_team` ON `team_messages(to_team)`
- `idx_team_messages_to_member` ON `team_messages(to_member)`
- `idx_team_messages_session` ON `team_messages(session_id)`

**策略（本组所有表）**：canonical state。P2 迁移，作为核心协作能力迁移。team mailbox 语义迁入 `lingxiao-core::team`。

---

### 2.10 Memory/FTS（来自 `src/memory/MemoryFTS.ts`）— 独立数据库文件

注意：MemoryFTS 使用**独立的数据库文件**，并非主 `Database.ts` 所管理的 core DB 内的表。来源 `src/memory/MemoryService.ts:57-60`：

- 默认路径：`<workspace>/.lingxiao/memory_fts.sqlite` 或 `<memoryRoot>/memory_fts.sqlite`
- 独立连接：`MemoryFTS` 构造函数自行 `new DatabaseSync(dbPath)`，与 `DatabaseManager` 无关
- 独立 PRAGMA：仅设置 `journal_mode = WAL` + `busy_timeout = 5000`（见 6.2 节）
- 独立 schema version：不共享 `DatabaseManager.SCHEMA_VERSION`（本身也没有 version 机制）

因此 Rust Core 的策略是：**可以选择保留独立 memory DB 或合并到 core DB**。若保留独立 DB，则需单独初始化、单独设置 PRAGMA、不要用主 DB 的 `SCHEMA_VERSION` 推断它。

#### `memory_entry`

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| `id` | TEXT | PK | UUID |
| `path` | TEXT | NOT NULL UNIQUE | virtual path |
| `scope` | TEXT | NOT NULL | global/project/session |
| `scope_id` | TEXT | NOT NULL | |
| `type` | TEXT | NOT NULL | memory/checkpoint/progress/notes/free |
| `body` | TEXT | NOT NULL | |
| `fingerprint` | TEXT | NOT NULL | hash for dedup |
| `last_indexed_at` | INTEGER | NOT NULL | epoch ms |

索引：
- `memory_scope_idx` ON `memory_entry(scope, scope_id)`
- `memory_type_idx` ON `memory_entry(type)`

#### `memory_fts`（FTS5 虚拟表）

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
  body,
  content='memory_entry',
  content_rowid='rowid'
);
```

同步触发器：
- `memory_fts_ai` AFTER INSERT
- `memory_fts_ad` AFTER DELETE
- `memory_fts_au` AFTER UPDATE

#### `memory_embedding`（注意！无 CREATE TABLE 语句）

| 字段 | 类型 | 推断说明 |
|---|---|---|
| `path` | TEXT | PK? (INSERT OR REPLACE) |
| `embedding` | BLOB | Float32Array binary |
| `model` | TEXT | embedding model name |
| `dimensions` | INTEGER | |
| `created_at` | INTEGER | epoch ms |

**发现**：`storeEmbedding()` 在 `MemoryFTS.ts:152-157` 使用 `INSERT OR REPLACE INTO memory_embedding` 但代码中**无 `CREATE TABLE IF NOT EXISTS memory_embedding` 语句**。这意味着运行时该表可能由外部创建、或存在 bug（INSERT 会在表不存在时失败）。Rust Core 需要明确定义此表。

**策略**：P2 迁移。可保持 SQLite FTS5 + embedding 表，也可做可机械映射的 Rust memory schema。`memory_embedding` 需要在 Rust schema 中显式定义。

---

## 3. Rust 归属模块总表

| # | 表名 | 域 | Rust 模块 | 迁移优先级 | 策略 |
|---|---|---|---|---|---|
| 1 | `sessions` | Core Runtime | `lingxiao-core::session` (persistence sub) | P0 | canonical state |
| 2 | `tasks` | Core Runtime | `lingxiao-core::task` | P0 | canonical state |
| 3 | `messages` | Core Runtime | `lingxiao-core::session` | P0 | canonical state |
| 4 | `agent_logs` | Core Runtime | `lingxiao-core::persistence` | P1 | audit log |
| 5 | `agent_state` | Runtime Residue | `lingxiao-core::agent` (schema only) | P1 | **legacy residue** → Rust canonical enum + event log |
| 6 | `session_state` | Runtime Residue | `lingxiao-core::session` (schema only) | P1 | **legacy residue** → Rust canonical model subset |
| 7 | `leader_conversation` | Conversation | `lingxiao-core::event_log` / `lingxiao-core::session` | P0 | canonical state + event log |
| 8 | `agent_conversation` | Conversation | `lingxiao-core::event_log` / `lingxiao-core::agent` | P1 | canonical state + event log |
| 9 | `token_usage` | Usage | `lingxiao-core::llm` / `lingxiao-core::telemetry` | P1 | audit log |
| 10 | `llm_gateway_requests` | Usage | `lingxiao-core::llm` / `lingxiao-core::telemetry` | P1 | audit log |
| 11 | `traces` | Tracing | `lingxiao-core::telemetry` | P1 | audit log |
| 12 | `execution_trace_events` | Tracing | `lingxiao-core::telemetry` | P1 | audit log |
| 13 | `execution_project_models` | Tracing | `lingxiao-core::telemetry` | P2 | projection cache |
| 14 | `workflows` | Workflow | `lingxiao-core::workflow` | P1 | canonical state |
| 15 | `workflow_executions` | Workflow | `lingxiao-core::workflow` | P1 | event log (加 per-node progress) |
| 16 | `workflow_execution_logs` | Workflow | `lingxiao-core::workflow` | P1 | event log |
| 17 | `scheduled_tasks` | Schedule | `lingxiao-core::schedule` | P2 | canonical state |
| 18 | `health_reports` | Schedule | `lingxiao-core::schedule` | P2 | audit log |
| 19 | `worktrees` | Workspace | `lingxiao-core::workspace` | P2 | canonical state (初期可 projection cache) |
| 20 | `graph_nodes` | Blackboard | `lingxiao-core::blackboard` | P1 | canonical state |
| 21 | `graph_edges` | Blackboard | `lingxiao-core::blackboard` | P1 | canonical state |
| 22 | `assumptions` | Blackboard | `lingxiao-core::blackboard` | P1 | canonical state |
| 23 | `tool_registrations` | Tool | `lingxiao-core::tool` or `lingxiao-tool-host-protocol` | P1 | sidecar registry / canonical state |
| 24 | `teams` | Team | `lingxiao-core::team` | P2 | canonical state |
| 25 | `team_members` | Team | `lingxiao-core::team` | P2 | canonical state |
| 26 | `team_messages` | Team | `lingxiao-core::team` | P2 | canonical state |
| 27 | `memory_entry` | Memory | `lingxiao-core::memory` | P2 | canonical state |
| 28 | `memory_fts` | Memory | `lingxiao-core::memory` | P2 | canonical state (FTS5 或机械映射) |
| 29 | `memory_embedding` | Memory | `lingxiao-core::memory` | P2 | canonical state (需补充 CREATE TABLE) |

---

## 4. 索引清单

### 4.1 索引计数口径

索引在三文件中定义，部分索引名重复（`traces` 在 Database.ts 与 Tracing.ts 各定义一次）。必须区分三种口径：

| 口径 | 说明 | 计数 |
|---|---|---|
| **raw DDL statements** | `rg "CREATE INDEX IF NOT EXISTS"` 在三个文件中的总匹配数 | **47** |
| ├─ `Database.ts` raw | `src/core/Database.ts` 中的 CREATE INDEX 语句 | **43** |
| ├─ `MemoryFTS.ts` raw | `src/memory/MemoryFTS.ts` 中的 CREATE INDEX 语句 | **2** |
| └─ `Tracing.ts` raw | `src/core/Tracing.ts` 中的 CREATE INDEX 语句（`traces` 重复） | **2** |
| **unique index names** | 去重后的索引名数量（`idx_traces_trace` 和 `idx_traces_session` 重复） | **45** |
| **Rust target** | Rust Core 实际需要创建的索引数（traces 只需一份） | **45** |

43 (Database.ts) + 2 (MemoryFTS.ts) + 2 (Tracing.ts) = 47 raw DDL statements。
去重后唯一索引名 = 45（`idx_traces_trace` 和 `idx_traces_session` 在 Database.ts 与 Tracing.ts 重复）。
Rust Core 目标索引数 = 45（traces 索引定义一次即可）。

### 4.2 索引明细

| 索引名 | 表 | 列 | 类型 | 备注 |
|---|---|---|---|---|
| `idx_leader_conv_session` | `leader_conversation` | `session_id` | B-tree | |
| `idx_agent_conv_session` | `agent_conversation` | `session_id, agent_id` | B-tree | |
| `idx_agent_state_session` | `agent_state` | `session_id` | B-tree | |
| `idx_token_usage_session` | `token_usage` | `session_id` | B-tree | |
| `idx_llm_gateway_trace` | `llm_gateway_requests` | `trace_id` | B-tree | |
| `idx_llm_gateway_session` | `llm_gateway_requests` | `session_id, created_at` | B-tree | |
| `idx_llm_gateway_key` | `llm_gateway_requests` | `key_id, created_at` | B-tree | |
| `idx_traces_trace` | `traces` | `trace_id` | B-tree | |
| `idx_traces_session` | `traces` | `session_id, start_ts` | B-tree | |
| `idx_agent_logs_session` | `agent_logs` | `session_id` | B-tree | |
| `idx_messages_session` | `messages` | `session_id` | B-tree | |
| `idx_tasks_session` | `tasks` | `session_id` | B-tree | |
| `idx_graph_nodes_session` | `graph_nodes` | `session_id` | B-tree | |
| `idx_graph_nodes_kind` | `graph_nodes` | `session_id, kind` | B-tree | |
| `idx_graph_nodes_status` | `graph_nodes` | `session_id, intent_status` | B-tree | |
| `idx_graph_edges_from` | `graph_edges` | `session_id, from_node_id` | B-tree | |
| `idx_graph_edges_to` | `graph_edges` | `session_id, to_node_id` | B-tree | |
| `idx_graph_edges_type` | `graph_edges` | `session_id, edge_type` | B-tree | |
| `idx_scheduled_tasks_session` | `scheduled_tasks` | `session_id` | B-tree | |
| `idx_scheduled_tasks_next` | `scheduled_tasks` | `next_run_at` | B-tree | |
| `idx_scheduled_tasks_workflow` | `scheduled_tasks` | `workflow_id` | **partial** | WHERE `workflow_id IS NOT NULL` |
| `idx_scheduled_tasks_source` | `scheduled_tasks` | `source_type, source_id, source_node_id` | **partial** | WHERE `source_type IS NOT NULL` |
| `idx_worktrees_session` | `worktrees` | `session_id` | B-tree | |
| `idx_worktrees_repo` | `worktrees` | `repo_root` | B-tree | |
| `idx_worktrees_status` | `worktrees` | `status` | B-tree | |
| `idx_hr_session_ts` | `health_reports` | `session_id, timestamp` | B-tree | |
| `idx_execution_trace_project` | `execution_trace_events` | `project_root, created_at` | B-tree | |
| `idx_execution_trace_task` | `execution_trace_events` | `session_id, task_id` | B-tree | |
| `idx_execution_trace_status` | `execution_trace_events` | `project_root, status` | B-tree | |
| `idx_assumptions_status` | `assumptions` | `status, session_id` | B-tree | |
| `idx_assumptions_target` | `assumptions` | `session_id, verification_target` | B-tree | |
| `idx_workflow_executions_workflow` | `workflow_executions` | `workflow_id` | B-tree | |
| `idx_workflow_executions_session` | `workflow_executions` | `session_id` | B-tree | |
| `idx_workflow_logs_execution` | `workflow_execution_logs` | `execution_id` | B-tree | |
| `idx_workflow_logs_timestamp` | `workflow_execution_logs` | `timestamp` | B-tree | |
| `idx_tool_registrations_session` | `tool_registrations` | `session_id` | B-tree | |
| `idx_tool_registrations_name` | `tool_registrations` | `tool_name` | B-tree | |
| `idx_tool_calls_session` | `tool_calls` | `session_id` | B-tree | Rust Core owner addition |
| `idx_tool_calls_status` | `tool_calls` | `session_id, status` | B-tree | Rust Core owner addition |
| `idx_teams_session` | `teams` | `session_id` | B-tree | |
| `idx_team_members_team` | `team_members` | `team` | B-tree | |
| `idx_team_members_session` | `team_members` | `session_id` | B-tree | |
| `idx_team_messages_to_team` | `team_messages` | `to_team` | B-tree | |
| `idx_team_messages_to_member` | `team_messages` | `to_member` | B-tree | |
| `idx_team_messages_session` | `team_messages` | `session_id` | B-tree | |
| `memory_scope_idx` | `memory_entry` | `scope, scope_id` | B-tree | MemoryFTS |
| `memory_type_idx` | `memory_entry` | `type` | B-tree | MemoryFTS |

MemoryFTS 还有 **3 个 FTS5 同步触发器**（`memory_fts_ai`, `memory_fts_ad`, `memory_fts_au`），Rust 中需等价实现。

---

## 5. 旧数据迁移策略

### 不需要迁移旧数据，只需要 schema parity

**所有表都不迁移旧数据**。Rust Core 从头创建 schema 实例。用户旧 SQLite 文件只保留（重命名为 `.replaced-*`）作为冷备份。

### 不应成为 Rust canonical state 的表

| 表 | 原因 | Rust 替代方案 |
|---|---|---|
| `agent_state` | 旧运行时快照，多进程写入产物 | Rust `agent::AgentState` enum + event log replay |
| `session_state` | 旧 KV 快照，向前兼容残留 | Rust `session::SessionState` canonical model；冷门 key 暂留 runtime residue |

---

## 6. PRAGMA 配置

TS Core 使用两个独立的 SQLite 连接（DatabaseManager 和 MemoryFTS），它们的 PRAGMA 配置不同。

### 6.1 DatabaseManager（Core DB）

来源 `Database.ts:893-904`：

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 30000;
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;
PRAGMA wal_autocheckpoint = 1000;
PRAGMA temp_store = MEMORY;
```

### 6.2 MemoryFTS（独立数据库文件 + 独立连接）

来源 `MemoryFTS.ts:45-46`，连接指向 `<workspace>/.lingxiao/memory_fts.sqlite`（`MemoryService.ts:59-60`），与主 DB 完全无关：

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
```

MemoryFTS 不设置 foreign_keys、synchronous、wal_autocheckpoint、temp_store，使用 SQLite 默认值。
该文件没有 schema version 机制，不共享 `DatabaseManager.SCHEMA_VERSION`。

### 6.3 Rust Core 目标策略

- **Core DB 连接**：完整复现 Database.ts 的 PRAGMA 组（含 foreign_keys ON）。
- **Memory FTS 数据库文件**：TS 端使用独立文件 `memory_fts.sqlite`（`MemoryService.ts:57-60`）。Rust Core 可选择：
  - 保留独立文件 + 独立连接，此时只需 WAL + busy_timeout 5000，单独初始化、不共享 `SCHEMA_VERSION`。
  - 合并到 core DB 作为一张（或若干张）表，此时自动继承 Core PRAGMA。
- **时间单位**：busy_timeout 单位为毫秒（ms），与 TS 一致。

---

## 7. Schema Parity 验收方法

### 7.1 `rg` 核对命令

```bash
# 核对所有 CREATE TABLE
rg "CREATE TABLE IF NOT EXISTS" src/core/Database.ts src/memory/MemoryFTS.ts src/core/Tracing.ts

# 核对所有 CREATE INDEX
rg "CREATE INDEX IF NOT EXISTS" src/core/Database.ts src/memory/MemoryFTS.ts src/core/Tracing.ts

# 核对 SCHEMA_VERSION
rg "SCHEMA_VERSION" src/core/Database.ts

# 核对 memory_embedding 表定义缺失
rg "memory_embedding" src/memory/
```

### 7.2 Schema diff 验证步骤

1. **启动 Rust Core 初始化**并运行其 migration/init。
2. **使用 SQLite 命令行或 PRAGMA** 提取 Rust schema：
   ```sql
   SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
   SELECT name, sql FROM sqlite_master WHERE type='index' ORDER BY name;
   SELECT * FROM pragma_table_info('<table_name>');
   PRAGMA user_version;
   ```
3. **对比 TS schema 快照**（本文档）与 Rust schema。对比要点：
   - 表名集合：三种口径必须分别核对（见下表）。
   - 每表列名、类型、NOT NULL / DEFAULT 约束一致。
   - PK 定义一致。
   - UNIQUE 约束一致。
   - 索引名、列、partial WHERE 一致（Rust target = 45 个 B-tree 索引）。
   - FTS5 触发器等效实现。
   - `PRAGMA user_version` 为 15。
   - 两套连接各自的 PRAGMA 配置一致。

| 口径 | 说明 | 计数 |
|---|---|---|
| **raw `CREATE TABLE IF NOT EXISTS`** | rg 三文件匹配数 | **28**（Database.ts=26, MemoryFTS.ts=1 memory_entry, Tracing.ts=1 traces 重复） |
| **unique current TS physical tables** | 去重后当前 TS 物理表 | **27**（Database.ts 26 + MemoryFTS `memory_entry` 1；Tracing `traces` 重复） |
| **current TS virtual tables** | 当前 TS FTS5 虚拟表 | **1**（`memory_fts`） |
| **unique current TS schema objects** | 去重后物理表 + 虚拟表 | **28**（27 物理 + 1 FTS5 virtual `memory_fts`） |
| **Rust TS-parity target schema objects** | unique current TS schema objects + 补充 `memory_embedding` | **29** |
| **P1 Rust Core owner additions** | `event_log` + `event_log_meta` + `command_dedupe` + permission state tables | **6** |
| **P1 Rust owner schema objects** | TS-parity target 29 + P1 Rust Core owner additions 6 | **35** |
| **unique current TS CREATE INDEX** | 去重索引名数 | **45** |
| **Rust target indexes** | Rust 实际需创建（traces 索引不重复） | **45** |

### 7.3 机械映射豁免项

以下差异允许不做 exact match，但需在 `rust-core-sqlite-schema-map.md` 中记录：

| 表/列 | 允许差异 | 原因 |
|---|---|---|
| `memory_fts` | FTS5 → Rust alternative | Rust 端如果无法使用 FTS5，可用等价全文搜索替代 |
| `memory_embedding` | 需补充 CREATE TABLE | TS 端缺失表定义，Rust 端需显式创建 |
| `event_log` | Rust Core-only 新增表 | P1 ordered durable event store，用于 per-session `seq`、generation gate、replay/recovery |
| `event_log_meta` | Rust Core-only 新增表 | P1 event log metadata，用于 `last_seq` 与 `current_generation` |
| `command_dedupe` | Rust Core-only 新增表 | P1 command router idempotency cache，主键为 `(idempotency_key, method)` |
| `permission_modes` | Rust Core-only 新增表 | P2 permission mode canonical state，支持 mode change 后 generation bump |
| `permission_requests` | Rust Core-only 新增表 | P2 permission request/resume 持久化，支撑 crash 后 pending request 恢复 |
| `permission_grants` | Rust Core-only 新增表 | P2 active grant cache，mode change 时统一撤销并落 `permission.grant_revoked` |
| TEXT/REAL 的精度 | 允许 | SQLite 无严格类型；Rust 端可用 TEXT for epoch ms |
| 索引命名 | 允许 | Rust 端可按 `idx_<table>_<column>` 惯例重命名 |

---

## 8. Phase 1 Rust SQLite Owner 最小任务清单

当 Phase 1 搭建 Rust Core skeleton 时，`lingxiao-core::persistence` 模块需完成以下最小任务：

### T-001：定义 Schema 常量

- 在 `lingxiao-core::persistence` 或单独 `schema.rs` 中定义 `SCHEMA_VERSION = 15`。
- 实现初始化时设置 `PRAGMA user_version = 15`。

### T-002：实现 DDL 初始化

- 编写所有 P0 表的 `CREATE TABLE IF NOT EXISTS` 语句（Rust 字符串或迁移文件）。
- 编写对应索引的 `CREATE INDEX IF NOT EXISTS` 语句。
- 注意 partial index 的 `WHERE` 子句。
- 注意 `memory_embedding` 表需要补充定义。
- 处理 FTS5 虚拟表（可延迟到 Phase 2）。

### T-003：实现 Schema 版本检查

- 在初始化时读取 `PRAGMA user_version`。
- 如果不匹配则备份旧库 + 重建（兼容旧 `.replaced-*` 行为）。
- Rust Core 初期不需要迁移逻辑，只做兼容替换。

### T-004：实现 PRAGMA 配置

```rust
conn.execute_batch("PRAGMA journal_mode = WAL")?;
conn.execute_batch("PRAGMA busy_timeout = 30000")?;
conn.execute_batch("PRAGMA foreign_keys = ON")?;
conn.execute_batch("PRAGMA synchronous = NORMAL")?;
conn.execute_batch("PRAGMA wal_autocheckpoint = 1000")?;
conn.execute_batch("PRAGMA temp_store = MEMORY")?;
```

### T-005：实现 Connection Singleton + Transaction API

- 单一 SQLite 连接（`rusqlite::Connection` 或 `sqlx::Pool` with max_size=1）。
- `begin_immediate` + retry on `SQLITE_BUSY`。
- 对外暴露 `transaction<T>(fn)` API。

### T-006：验证 schema parity

- 创建一个空数据库，运行 Rust 初始化。
- 用 SQLite shell 或 `PRAGMA table_info` / `PRAGMA index_list` 逐表验证。
- 与本文档第 2 节核对。

### T-007：P0 表对应的基本 CRUD

为以下 P0 表实现基本 INSERT/GET/SELECT：

| 表 | 方法 |
|---|---|
| `sessions` | insert, get, list, updateStatus |
| `tasks` | insert, get, update, delete, listBySession |
| `messages` | insert, getBySession |
| `leader_conversation` | insert, batchInsert, getBySession, deleteBySession |

其他表的 CRUD 在 Phase 2/3/4 按需补充。

---

## 9. 发现与风险

### 9.1 发现

1. **`memory_embedding` 缺少 CREATE TABLE**：`MemoryFTS.ts` 中使用了 `INSERT OR REPLACE INTO memory_embedding` 但无 DDL。TS 端可能依赖外部脚本或手动创建。Rust 端必须显式定义此表。

2. **`traces` 表定义重复**：`Database.ts:414` 和 `Tracing.ts:197` 有结构完全相同的 `traces` 表。Rust 端统一为一份定义。

3. **`execution_project_models` 单行投影缓存**：只有 3 列（`project_root` PK, `model_json`, `rebuilt_at`），本质是 project-level 的 LLM 模型配置缓存，非事实源。Rust Core 初期可按 projection cache 处理。

4. **Composite PK 表较多**：`tasks`、`graph_nodes`、`graph_edges`、`teams`、`team_members` 使用 `(id, session_id)` 或 `(session_id, name)` 作为复合主键。Rust 端的 ORM/类型映射需正确处理复合 PK。

5. **Partial index**：`idx_scheduled_tasks_workflow` 和 `idx_scheduled_tasks_source` 是 SQLite 条件索引（`WHERE ... IS NOT NULL`）。Rust migration 需完整保留。

### 9.2 风险

- **FTS5 在 Rust 中的可用性**：rusqlite 需要 `feature = "vtab"` 来启用 FTS5。如果目标平台不支持 FTS5，需要 fallback 策略（与当前 TS 一致）。
- **WAL 模式排他写入**：Rust Core 是唯一 writer，但仍然需要使用 `BEGIN IMMEDIATE` 防止并发内部冲突。
- **PRAGMA foreign_keys 启用**：`workflow_executions` 和 `workflow_execution_logs` 依赖 ON DELETE CASCADE。必须在每个连接上设置 `PRAGMA foreign_keys = ON`（SQLite 默认关闭）。

---

## 10. 核对命令执行记录

```powershell
# 1. 核对 CREATE TABLE —— raw count = 28（Database.ts=26, MemoryFTS.ts=1, Tracing.ts=1 重复）
rg -n "CREATE TABLE IF NOT EXISTS" src/core/Database.ts src/memory/MemoryFTS.ts src/core/Tracing.ts
rg -n "CREATE TABLE IF NOT EXISTS" src/core/Database.ts | Measure-Object | % { $_.Count }  # → 26
rg -n "CREATE TABLE IF NOT EXISTS" src/memory/MemoryFTS.ts | Measure-Object | % { $_.Count }  # → 1
rg -n "CREATE TABLE IF NOT EXISTS" src/core/Tracing.ts | Measure-Object | % { $_.Count }     # → 1

# 2. 核对 CREATE VIRTUAL TABLE
rg -n "CREATE VIRTUAL TABLE" src/memory/MemoryFTS.ts  # → 1 (memory_fts)

# 3. 核对 CREATE INDEX —— raw count = 47
rg -n "CREATE INDEX IF NOT EXISTS" src/core/Database.ts | Measure-Object | % { $_.Count }  # → 43
rg -n "CREATE INDEX IF NOT EXISTS" src/memory/MemoryFTS.ts | Measure-Object | % { $_.Count }  # → 2
rg -n "CREATE INDEX IF NOT EXISTS" src/core/Tracing.ts | Measure-Object | % { $_.Count }     # → 2

# 4. 核对 CREATE TRIGGER
rg -n "CREATE TRIGGER" src/memory/MemoryFTS.ts  # → 3 (FTS5 同步触发器)

# 5. 核对 SCHEMA_VERSION
rg -n "SCHEMA_VERSION" src/core/Database.ts  # → 15

# 6. 核对 PRAGMA 配置
rg -n "exec\('PRAGMA" src/core/Database.ts  # → 6 条（WAL, busy_timeout, foreign_keys, synchronous, wal_autocheckpoint, temp_store）
rg -n "PRAGMA" src/memory/MemoryFTS.ts  # → 2 条（WAL, busy_timeout）

# 7. 核对 memory_embedding 表定义缺失
rg -n "memory_embedding" src/memory/  # → 仅 INSERT/SELECT，无 CREATE TABLE
```

表计数结论：

| 口径 | 计数 |
|---|---|
| raw `CREATE TABLE IF NOT EXISTS` | **28**（Database.ts=26，MemoryFTS.ts=1 `memory_entry`，Tracing.ts=1 `traces` 重复） |
| 去重 current TS physical tables | **27**（Database.ts 26 + MemoryFTS `memory_entry` 1；Tracing `traces` 重复） |
| current TS virtual tables | **1**（`memory_fts`） |
| unique current TS schema objects | **28**（27 physical + 1 virtual） |
| Rust TS-parity target schema objects | **29**（unique current 28 + inferred missing `memory_embedding`） |
| P1/P2 Rust Core owner additions | **6**（`event_log`, `event_log_meta`, `command_dedupe`, `permission_modes`, `permission_requests`, `permission_grants`） |
| Rust owner schema objects | **35**（TS-parity target 29 + Rust Core owner additions 6） |

### P1 owner-schema addendum: `tool_calls`

P1 command router now owns a Rust-only `tool_calls` table for canonical tool/sidecar call state and resource accounting.

Updated Rust-owner counts:

| Scope | Count |
|---|---:|
| Rust Core owner additions | **7** (`event_log`, `event_log_meta`, `command_dedupe`, `tool_calls`, `permission_modes`, `permission_requests`, `permission_grants`) |
| Rust owner schema objects | **36** (TS-parity target 29 + Rust Core owner additions 7) |
| Rust owner indexes | **47** (TS-parity target 45 + `idx_tool_calls_session`, `idx_tool_calls_status`) |

`tool_calls` is written only by Rust Core. Sidecar executors return protocol responses; they do not write this table or any other core DB table.

注：所有核对均在 `src/` 目录下执行，未修改业务代码。
