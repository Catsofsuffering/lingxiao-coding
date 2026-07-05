# Rust Core Sidecar 边界契约

> Phase 0 交付物 — P0-6 A1。
>
> 本文定义 Rust Core 与 sidecar 进程之间的架构边界、通信信封、生命周期契约、资源预算、权限规则及 DB 禁写规则。Rust-native agents 运行于 in-process supervised async tasks；heavy/untrusted/legacy capabilities 由 sidecar/tool-host 承载。

## 目录

- [1. 背景决策](#1-背景决策)
- [2. 能力分类](#2-能力分类)
- [3. Sidecar Envelope](#3-sidecar-envelope)
- [4. 生命周期契约：取消 / 超时 / Heartbeat](#4-生命周期契约取消--超时--heartbeat)
- [5. 资源预算 (Resource Budget)](#5-资源预算-resource-budget)
- [6. 权限边界](#6-权限边界)
- [7. Tool-Host 进程模型](#7-tool-host-进程模型)
- [8. 现有工具分类矩阵](#8-现有工具分类矩阵)
- [9. 错误分类法 (Error Taxonomy)](#9-错误分类法-error-taxonomy)
- [10. 相关 Event 定义](#10-相关-event-定义)
- [11. Phase 1/2/3 任务定义](#11-phase-123-任务定义)
- [12. QA Checklist](#12-qa-checklist)
- [13. 验收核对命令](#13-验收核对命令)

---

## 1. 背景决策

### 1.1 Rust Core-first

Rust Core 是 Lingxiao 未来的核心运行时。所有新开发优先在 Rust 中实现；仅当能力本质上不适合 Rust（依赖外部运行时、重型原生库、不可信第三方代码）时才走 sidecar。

### 1.2 Rust-native agents = in-process supervised async tasks

Agent 运行在 Rust Core 进程内，通过 `tokio::spawn` + `JoinSet` 或类似 supervision 机制管理。不启动 OS 子进程。通信通过 Rust 内部 channel（`tokio::mpsc`/`oneshot`/`broadcast`），不经过 IPC。

### 1.3 Heavy/untrusted/legacy → sidecar/tool-host

以下情况必须使用 sidecar：

- **Heavy**：依赖运行时体积大、启动慢、内存占用高的能力（浏览器/Playwright、Office 文档引擎、Sharp 图像处理）。
- **Untrusted**：执行用户或第三方提供的代码（Python exec、Node REPL、MCP server 调用）。
- **Legacy**：当前 TS 中已完成但迁移成本高、不值得 Rust-native 重写的工具（Office 生成工具链、旧 parse_file）。

### 1.4 短期不考虑旧 TUI/Web/Electron 兼容

Rust Core 的 sidecar 协议不保证与旧 TS 版的 IPC 格式、SSE 事件名或 ACP method 兼容。旧消费者通过 adapter/bridge 适配。

---

## 2. 能力分类

### 2.1 Rust-native（in-process async task）

这些能力运行在 Rust Core 进程内，不经过 sidecar：

| 能力 | 说明 |
|------|------|
| File read/create/glob/list_dir | 文件 I/O，直接使用 `tokio::fs` |
| structured_patch | 纯文本操作，无外部依赖 |
| code_search / ast_query | AST 搜索（tree-sitter）可 Rust-native |
| git | `git2` crate 或 CLI 封装 |
| shell | supervision/permission/cancellation 层 Rust-native；子进程仍 spawn |
| attempt_completion | pure logic |
| send_message / session_info | 内部通信 |
| work_note / blackboard / write_fact | 内存状态操作 |
| workflow engine | 核心编排逻辑 |
| tool_preflight / find_tools | 元数据查询 |
| leader meta tools (create_task, dispatch_agent 等) | 纯核心逻辑 |

### 2.2 Sidecar required（必须走 sidecar）

这些能力依赖外部运行时或重型原生库，不能或不应嵌入 Rust Core 进程：

| 能力 | 原因 |
|------|------|
| **browser_action** | Playwright -> Chromium 进程，重型、不稳定、Crash-prone |
| **browser_visual_verify** | 同 browser，依赖 Playwright + Chromium |
| **screenshot** | 同 browser |
| **visual_contact_sheet** | 同 browser + Sharp 图像合成 |
| **ocr** | tesseract.js WASM / 或 tesseract C++ 库；依赖外部 tessdata |
| **python_exec** | 不可信代码执行，必须进程隔离 |
| **node_repl** | 不可信代码执行，必须进程隔离 |
| **mcp** | MCP server 本质是外部进程协议；MCP client 可 Rust-native，server 调用走 sidecar |
| **terminal (PTY/control/output)** | node-pty / xterm headless 绑定；需完整 PTY 进程树隔离 |
| **Office 工具链** | 依赖 docx/exceljs/pptxgenjs/pdfkit/mammoth 等 JS 库；Office 生成/编辑/审查/渲染全链路 |
| **parse_file** | 依赖多种文件解析库（mammoth/pdf-parse 等） |

### 2.3 可后续迁 Rust-native

这些能力目前因成本或优先级走 sidecar，但理论上可未来迁入 Rust Core：

| 能力 | 迁移条件 |
|------|----------|
| OCR | Rust OCR crate（如 `leptess`/`tesseract-rs`）成熟且 tessdata 可嵌入 |
| MCP client | 纯 Rust MCP SDK 可用时，client 侧可不依赖 sidecar；server 调用仍走 sidecar |
| PDF 生成 | Rust `printpdf`/`genpdf` 库覆盖需求时 |
| HTTP/network 工具 | 已有 `reqwest`，可 native；但 LLM provider 调用走 core 内部 |
| web_fetch / web_search | 已有 Rust HTTP client 库，可 native |
| terminal PTY | `tokio-pty`/`portable-pty` 等 Rust PTY crate 成熟时 |

### 2.4 Legacy-only（不进入 Rust Core）

以下能力的 Rust Core 等价功能必须重新定义，不迁移旧实现：

| 能力 | 原因 |
|------|------|
| 旧 TS worker IPC（`WorkerProcessRunner`） | Rust-native agents 不启动子进程 |
| 旧 ACP/SSE event names | Rust Core 重新设计 canonical event |
| 旧 `SessionRuntimeState` UI 字段形状 | Rust Core 重新设计 projection |
| 旧 TUI/Web/Electron 状态拼装 | adapter layer 单独处理 |

---

## 3. Sidecar Envelope

### 3.1 Request Envelope

```rust
/// lingxiao-tool-host-protocol crate
struct SidecarRequest {
    // === Routing ===
    request_id: String,       // 全局唯一，用于去重和能力溯源
    session_id: String,       // 所属会话
    task_id: Option<String>,  // 所属任务（若有）
    agent_id: String,         // 发起调用的 agent ID

    // === Tool invocation ===
    tool_name: String,        // 工具名 (如 "browser_action", "python_exec")
    args: Vec<u8>,            // 序列化参数 (JSON/protobuf/msgpack)

    // === Capability negotiation ===
    capabilities: Vec<Capability>,  // 声明所需能力列表（如 ["browser", "network", "filesystem:temp"]）
    // sidecar 启动时可用 capabilities 声明校验，不匹配则拒绝启动

    // === Lifecycle ===
    deadline: Timestamp,      // 硬截止时间，超过后 Rust Core 不再等待 response
    // 达到 deadline 时，Core 发出 cancel token，不再消费 sidecar 的输出
    // sidecar 可主动检查 deadline 并自行终止，避免浪费资源

    // === Resource budget ===
    resource_budget: ResourceBudget,  // CPU/memory/time/network 配额

    // === Cancellation ===
    cancel_token: CancelToken,  // Core 通过此 token 通知 sidecar 取消
}

struct ResourceBudget {
    max_runtime_ms: u64,        // 最大执行时间 (ms)
    max_memory_mb: u64,         // 最大内存 (MB)
    max_cpu_ms: u64,            // 最大 CPU 时间 (ms) —— wall clock 超时之外限制实际 CPU 用量
    max_network_bytes: u64,     // 最大网络流量 (bytes)，仅 network 类工具生效
    max_file_write_bytes: u64,  // 最大文件写入量 (bytes)，仅 filesystem 类工具生效
}

struct CancelToken {
    /// 当此 channel closed 时 sidecar 必须立即终止当前操作并返回 cancelled 响应。
    /// 实现方式：传递一个 CancellationToken（C# 风格）或 AbortSignal（JS 风格）。
    /// 取消是**必须服从**的契约，不是建议。
}
```

### 3.2 Response / Stream / Progress

```rust
enum SidecarResponse {
    /// 同步完成响应
    Completed(SidecarCompleted),
    /// 流式响应（适用于长时间运行的工具，如 browser action 的逐步输出）
    Stream(SidecarStreamChunk),
    /// 进度更新
    Progress(SidecarProgress),
    /// 错误终止
    Error(SidecarError),
}

struct SidecarCompleted {
    request_id: String,
    /// 序列化结果 (JSON/protobuf/msgpack)
    result: Vec<u8>,
    /// 结果形状声明，帮助 Core 做 schema 校验
    result_shape: ResultShape,  // 'text' | 'json' | 'file' | 'browser_snapshot' | 'task_handle'
    /// 执行耗时
    duration_ms: u64,
    /// 实际资源消耗
    usage: ResourceUsage,
}

struct SidecarStreamChunk {
    request_id: String,
    sequence: u64,           // 单调递增序号
    data: Vec<u8>,           // 序列化 chunk
    is_final: bool,          // 是否为最后一个 chunk
    usage: Option<ResourceUsage>,  // 最终 chunk 附带累计用量
}

struct SidecarProgress {
    request_id: String,
    percent: Option<f64>,    // 0.0 ~ 1.0
    message: Option<String>, // 可读进度描述
    usage: ResourceUsage,    // 当前累计用量
}
```

### 3.3 Usage / Resource Accounting

```rust
struct ResourceUsage {
    runtime_ms: u64,
    cpu_ms: u64,
    memory_mb_peak: u64,
    network_bytes: u64,
    file_write_bytes: u64,
}
```

### 3.4 错误响应

```rust
struct SidecarError {
    request_id: String,
    code: SidecarErrorCode,
    message: String,
    details: Option<Vec<u8>>,  // 额外诊断信息
    usage: ResourceUsage,       // 失败前的累计用量
}

enum SidecarErrorCode {
    // === 调用方错误 ===
    InvalidArgs,         // 参数校验失败
    UnsupportedTool,     // sidecar 不支持该工具
    CapabilityMismatch,  // 声明的 capabilities 不匹配

    // === 执行错误 ===
    RuntimeError,        // 通用运行时错误
    ToolExecutionFailed, // 工具内部执行失败
    ResourceExhausted,   // 超预算

    // === 生命周期 ===
    Cancelled,           // 用户/系统取消
    Timeout,             // deadline 超时
    HeartbeatLost,       // core 检测到 heartbeat 丢失
    Crash,               // sidecar 进程崩溃

    // === 权限 ===
    PermissionDenied,    // sidecar 内部权限拒绝
    FileWriteDenied,     // 写文件未获 lease
    DbWriteDetected,     // 检测到 DB 写入企图

    // === 通信 ===
    ProtocolError,       // 信封格式错误
    TransportError,      // 底层 IPC 传输错误
}
```

---

## 4. 生命周期契约：取消 / 超时 / Heartbeat

### 4.1 取消契约

```
Rust Core                          Sidecar
    |                                 |
    |------- SidecarRequest -------->|  (含 cancel_token)
    |                                 |
    |           ... 执行中 ...         |
    |                                 |
    |--- [用户/系统决定取消]            |
    |                                 |
    |--- close(cancel_token) ------->|  (Core 关闭 cancel channel)
    |                                 |
    |                                 |-- 检测到 cancel token 已关闭
    |                                 |-- 立即终止当前操作
    |                                 |-- 返回 SidecarError { code: Cancelled }
    |<---- SidecarError(code=Cancelled) -|
    |                                 |
    |--- SIGTERM (5s grace) -------->|  (如果 sidecar 未在合理时间内响应取消)
    |--- SIGKILL (超时后) ---------->|
```

**关键规则**：
- Cancel token 是双边契约：Core **必须** token closed 后依然消费 sidecar 返回的消息 500ms 窗口，防竞争。
- Sidecar **必须**定期检查 cancel token（至少每 100ms 或在每个操作步骤前）。
- Sidecar 收到取消后 **不应**继续执行，但 **可以**返回已产生的中间结果。
- 如果 sidecar 在 cancel token 关闭后 5s 内未响应 cancelled 错误，Core 发送 SIGTERM（进程树）。
- SIGTERM 后 5s 仍未退出 → SIGKILL。

### 4.2 超时期约

```
Rust Core                          Sidecar
    |                                 |
    |--- SidecarRequest(deadline) -->|
    |                                 |
    |           ... 执行中 ...         |
    |                                 |
    |                            deadline 到达
    |                                 |-- (sidecar 自主检测 deadline 可自行终止)
    |--- close(cancel_token) ------->|  (Core 在 deadline 后关闭 cancel channel)
    |<---- SidecarError(code=Timeout) -|
    |                                 |
    |--- SIGTERM/SIGKILL 同上 ---------->|
```

**关键规则**：
- `deadline` 在 request 中指定，由 Core 根据工具 metadata 的默认 timeout + 用户配置计算。
- Sidecar **可以**自主检测 deadline（推荐），但 Core **总是**在 deadline 后关闭 cancel token 作为兜底。
- 超时原因必须在 `SidecarError.message` 中明确（`"deadline exceeded"` / `"tool-specific timeout"`）。

### 4.3 Heartbeat

```
Rust Core                          Sidecar
    |                                 |
    |   (sidecar 定期发送 heartbeat)   |
    |<--- SidecarHeartbeat -----------|
    |   (每 15s 至少一次)              |
    |                                 |
    |   ... 15s 无 heartbeat ...       |
    |   ... 30s 无 heartbeat ...       |
    |                                 |
    |--- close(cancel_token) -------->|  (Core 关闭 cancel channel)
    |--- SIGTERM/SIGKILL ------------>|
```

```rust
struct SidecarHeartbeat {
    request_id: String,
    /// 当前累计资源用量
    usage: ResourceUsage,
    /// sidecar 自评健康状态
    health: HeartbeatHealth,
}

enum HeartbeatHealth {
    Healthy,
    Warning { reason: String },   // 内存接近上限、网络延迟等
    Critical { reason: String },  // 即将 OOM、无法继续执行
}
```

**关键规则**：
- Sidecar 每 15s 至少发送一次 heartbeat。Core 在 45s 无 heartbeat 后推断 sidecar 失联（3 倍间隔）。
- 失联后 Core 关闭 cancel token + SIGTERM 优雅终止 → SIGKILL 强制终止。
- Heartbeat 必须附带 `usage` 字段，Core 据此做 real-time resource budget enforcement。
- Heartbeat 的 `health: Critical` 是 sidecar 请求 Core 终止自己的信号（"我快不行了"）。

---

## 5. 资源预算 (Resource Budget)

### 5.1 默认配额

| 资源 | 默认值 | 覆盖方式 |
|------|--------|----------|
| `max_runtime_ms` | 120000 (2 min) | 工具级别 override，如 browser_action 默认 300000 |
| `max_memory_mb` | 512 | 配置 `SIDECAR_MAX_MEMORY_MB` |
| `max_cpu_ms` | 同 `max_runtime_ms` | 暂不单独暴露 |
| `max_network_bytes` | 10 MB | 仅 network/browser 类工具生效 |
| `max_file_write_bytes` | 50 MB | 仅 filesystem 类工具生效 |

### 5.2 预算执行策略

- **Runtime 超时**：`deadline` 和 cancel token 兜底。
- **Memory 超限**：通过 heartbeat `usage.memory_mb_peak` 监控；heartbeat 中 `health: Critical` 时 Core 主动终止。RSS 超限由宿主进程（tool-host）负责 kill 违规 sidecar，避免宿主 OOM。
- **CPU 超限**：暂不强制（wall clock deadline 已覆盖大部分场景）。后续可集成 `cgroups` / `job objects`。
- **Network 超限**：sidecar 自行追踪，Core 不做 wire-level 计量。超限时 sidecar 返回 `ResourceExhausted`。
- **File write 超限**：sidecar 自行追踪，Core 不做 wire-level 计量。超限时 sidecar 返回 `ResourceExhausted`。

### 5.3 预算继承与覆盖

```
ResourceBudget 从三层级继承：
  1. 全局默认 (config/defaults.ts 等价)
  2. 工具级 metadata (如 browser_action 的较长 deadline)
  3. 调用时 agent 指定的 context budget (由 AutonomyGovernor 计算)
```

---

## 6. 权限边界

### 6.1 Sidecar DB 禁写规则

```
┌─────────────────────────────────────────────────────────┐
│                     RULE: SIDECAR NEVER WRITES DB       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Sidecar 进程绝对不能直接写入 Core SQLite 数据库。        │
│                                                         │
│  禁止操作包括但不限于：                                   │
│    - 打开 core.db SQLite 文件并执行 INSERT/UPDATE/DELETE │
│    - 调用任何 Core DB 的 `PRAGMA` 或 schema 修改         │
│    - 通过共享内存 / WAL / SHM 文件间接修改 Core DB       │
│    - 使用 Core 的内部 Repository 类或 DatabaseManager    │
│                                                         │
│  允许的操作：                                             │
│    - 读取 Core 通过 sidecar envelope 透传的只读状态快照   │
│    - 写入自己的 sidecar 工作目录下的临时文件               │
│    - 通过 response/stream/progress 返回结果和事件         │
│                                                         │
│  执行方式：                                               │
│    - Core 在启动 sidecar 时注入只读文件描述符（如果有）   │
│    - Core 不传递 DB 路径或连接字符串给 sidecar           │
│    - 审计：sidecar 的进程文件访问可通过平台监控检测        │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 6.2 Canonical State 禁写规则

```
┌─────────────────────────────────────────────────────────┐
│               RULE: SIDECAR NEVER MODIFIES              │
│                    CANONICAL STATE                      │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Sidecar 进程绝对不能直接修改 Rust Core 的 canonical     │
│  state（内存中的 session/task/agent/workflow 状态）。    │
│                                                         │
│  Canonical state 只由 Rust Core 在收到 sidecar          │
│  response 后，经过验证和语义转换，再更新。               │
│                                                         │
│  Sidecar 返回的结果不是事实，必须由 Core 接受后落事件    │
│  和状态。（参考 `rust-core-function-inventory.md` 第     │
│  422 行）                                               │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 6.3 文件系统写入必须通过 Rust Permission/Policy Lease

```
┌─────────────────────────────────────────────────────────┐
│       RULE: FILESYSTEM WRITES REQUIRE PERMISSION LEASE  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Sidecar 在以下条件下可以写入文件系统：                   │
│                                                         │
│  1. 写入 sidecar 专属临时目录（$TMPDIR/lingxiao/         │
│     sidecar/<request_id>/）—— 无需额外权限                │
│                                                         │
│  2. 写入工作区文件（workspace/）—— 必须获得 Rust Core    │
│     Permission System 签发的 write lease：               │
│     - Lease 包含 scope (path prefix)、generation、       │
│       expiry timestamp。                                 │
│     - Sidecar 必须在 write lease 有效期内且 scope 内     │
│       写入。                                             │
│     - Core 在发起 sidecar request 时通过 envelope 附带   │
│       lease token。                                      │
│     - Sidecar 可以在响应中声明已写入路径，Core 做        │
│       后验校验收 lease 是否覆盖。                        │
│                                                         │
│  3. 跨租户/敏感路径（如 /etc/, ~/.ssh/）—— 永远禁止。   │
│                                                         │
│  违反后果:                                                │
│    - Core 检测到 lease 外写入 → 标记 sidecar 为违规 →    │
│      终止 sidecar → 不信任该次结果 → audit log           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 7. Tool-Host 进程模型

### 7.1 默认策略：Per-call Spawn

**默认情况下，每个 sidecar 工具调用启动一个新的宿主进程。**

```
Request: browser_action(navigate, "https://example.com")
  → Start tool-host process (Node.js)
    → Load required module (Playwright browser manager)
    → Execute action
    → Stream results back via stdout/stderr/pipe
    → Exit

Next request: browser_action(click, "#button")
  → Start NEW tool-host process
    → Same as above
```

**理由**：
- 隔离性最佳：崩溃不污染后续调用。
- 内存释放彻底：进程退出后所有资源回收。
- 无状态泄漏：每次调用从干净状态开始。

### 7.2 例外：Long-lived Daemon

**以下场景使用长驻 daemon 进程，而非 per-call spawn：**

| 场景 | 理由 | 生存周期 |
|------|------|----------|
| **Terminal PTY daemon** | PTY 进程有状态（cwd, env, session 历史），不能每次调用重新创建 | 绑定 session 生命周期 |
| **Browser daemon** | Chromium 启动慢（~2-5s），频繁 create/destroy 开销大。浏览器页面状态跨调用复用时需要长驻 | 绑定 session 生命周期或显式 close |
| **MCP server daemon** | MCP 协议本质是长连接，每个 server 一个进程 | 绑定 session 生命周期 |
| **Python REPL daemon** | Python 解释器启动慢，交互式 REPL 需要保持状态 | 绑定 session 生命周期或显式 quit |

**Daemon 管理契约**：
- Daemon 由 `ToolHostDaemonRegistry` 管理，keyed by `(session_id, daemon_type)`。
- Daemon 在 session 结束时自动 SIGTERM → SIGKILL。
- Daemon 必须在 30s 内响应 shutdown 请求，否则强制终止。
- Daemon 同样适用 DB 禁写、canonical state 禁写、cancel token 等规则。
- Daemon 的 resource budget 为累计值，由 `ToolHostDaemonRegistry` 监控。

### 7.3 跨语言 sidecar

| Sidecar 类型 | 运行时 | 通信协议 |
|-------------|--------|----------|
| Browser (Playwright) | Node.js | stdin/stdout JSON-lines |
| OCR (tesseract) | Node.js (tesseract.js) / Rust (leptess) | stdin/stdout JSON-lines |
| Office 生成 | Node.js | stdin/stdout JSON-lines |
| MCP server | 任意 (Node/Python/Rust) | 原生 MCP stdio transport |
| Terminal PTY | Node.js (node-pty) | stdin/stdout JSON-lines |
| Python exec | Python | stdin/stdout JSON-lines |
| Node REPL | Node.js | stdin/stdout JSON-lines |

**通信协议**：
- **推荐**：stdin/stdout JSON-lines（每个消息一行 JSON，以 `\n` 分隔）。
- **备选**：Unix domain socket / named pipe（适用于需要双向流且 JSON-lines 不够的场景）。
- **不使用**：HTTP localhost（端口冲突、认证复杂、无进程级绑定）。
- **框架**：sidecar 两侧（Core 端 + tool-host 端）共用 `lingxiao-tool-host-protocol` crate/package 中的序列化定义。

---

## 8. 现有工具分类矩阵

来源：`src/tools/ToolMetadata.ts`，`src/tools/index.ts`，`src/tools/implementations/`。

| 工具名 | 当前分类 | Sidecar 类别 | 备注 |
|--------|----------|-------------|------|
| `file_read` | 'file' | Rust-native | |
| `list_dir` | 'file' | Rust-native | |
| `glob` | 'search' | Rust-native | |
| `code_search` | 'search' | Rust-native | |
| `ast_query` | 'search' | Rust-native | |
| `structured_patch` | 'file' | Rust-native | |
| `file_create` | 'file' | Rust-native | |
| `parallel_read_batch` | 'session' | Rust-native | |
| `send_message` | 'communication' | Rust-native | |
| `attempt_completion` | 'completion' | Rust-native | |
| `declare_assumption` | 'communication' | Rust-native | |
| `session_artifacts` | 'session' | Rust-native | |
| `session_info` | — | Rust-native | |
| `write_work_note` / `read_work_notes` | 'communication' | Rust-native | |
| `blackboard` | 'blackboard' | Rust-native | |
| `write_fact` | — | Rust-native | |
| `declare_intent` | — | Rust-native | |
| `read_graph` | — | Rust-native | |
| `workflow` | 'workflow' | Rust-native | |
| `tool_preflight` | 'session' | Rust-native | |
| `find_tools` | 'session' | Rust-native | |
| `shell` | 'execution' | Rust-native supervision layer | 子进程 spawn 是 OS 操作，但 supervision/cancel/permission 在 Rust Core 内 |
| `git` | 'git' | Rust-native | `git2` crate 或 CLI 封装 |
| `http_request` | 'network' | 可后续迁 Rust-native | 已有 `reqwest` |
| `web_fetch` | 'network' | 可后续迁 Rust-native | 已有 `reqwest` |
| `web_search` | 'network' | 可后续迁 Rust-native | 已有 `reqwest`，但 browser-backed fallback 走 sidecar |
| `memory` / `memory_read` / `memory_write` | 'memory' | Rust-native (P2) | P2 迁移 |
| team tools | 'team' | Rust-native (P2) | P2 迁移 |
| leader meta tools | — | Rust-native | 纯核心逻辑 |
| **`browser_action`** | 'browser' | **Sidecar required** | Playwright + Chromium |
| **`browser_visual_verify`** | 'browser' | **Sidecar required** | Playwright + Chromium |
| **`screenshot`** | 'browser' | **Sidecar required** | Playwright |
| **`visual_contact_sheet`** | 'browser' | **Sidecar required** | Playwright + Sharp |
| **`ocr`** | 'browser' | **Sidecar required** | tesseract.js |
| **`python_exec`** | 'execution' | **Sidecar required** | 不可信代码执行 |
| **`node_repl`** | 'execution' | **Sidecar required** | 不可信代码执行 |
| **`mcp`** | 'network' | **Sidecar required** | MCP server 调用 |
| **`terminal_control`** | 'execution' | **Sidecar required** | PTY 进程管理 |
| **`get_terminal_output`** | 'execution' | **Sidecar required** | PTY 输出读取 |
| **`parse_file`** | 'office' | **Sidecar required** | 多格式文件解析 |
| **Office 工具链** | 'office' | **Sidecar required** | 生成/编辑/审查/渲染 |
| `design_asset` | 'session' | Rust-native | |
| `bughunt_full_scan` | 'security' | 可 sidecar | 安全扫描依赖网络和外部服务 |
| `generate_canvas` | — | Legacy-only | 旧 TUI canvas 功能，Rust Core 不实现 |
| `generate_slidev` | — | Legacy-only (Office 类) | Slidev CLI 调用，P2 阶段评估 |

---

## 9. 错误分类法 (Error Taxonomy)

### 9.1 按重试策略

| 错误码 | 可重试？ | 重试策略 |
|--------|---------|----------|
| `InvalidArgs` | 否 | 调用方修复参数 |
| `UnsupportedTool` | 否 | 调用方更换 tool |
| `CapabilityMismatch` | 否 | 调用方调整 capabilities |
| `RuntimeError` | 可 | 最多 3 次指数退避 |
| `ToolExecutionFailed` | 可 | 最多 2 次，同参数 |
| `ResourceExhausted` | 否 | 需调整 resource_budget 后重试 |
| `Cancelled` | 否 | 用户意图，不重试 |
| `Timeout` | 可 | 最多 1 次，调大 deadline 后重试 |
| `HeartbeatLost` | 可 | 最多 1 次 |
| `Crash` | 可 | 最多 2 次指数退避 |
| `PermissionDenied` | 否 | 需用户批准 |
| `FileWriteDenied` | 否 | 需用户批准 |
| `DbWriteDetected` | 否 | 安全事件，上报 |
| `ProtocolError` | 否 | 实现 bug 需修复 |
| `TransportError` | 可 | 最多 3 次 |

### 9.2 按严重级别

| 级别 | 错误码 | Core 反应 |
|------|--------|----------|
| FATAL | `Crash`, `DbWriteDetected` | 记录审计日志，不信任该 sidecar 实例，关闭 daemon |
| ERROR | `Timeout`, `ResourceExhausted`, `HeartbeatLost`, `PermissionDenied`, `FileWriteDenied` | 正常错误返回，标记 sidecar event |
| WARN | `RuntimeError`, `ToolExecutionFailed`, `TransportError` | 可重试，重试次数超限后升为 ERROR |
| IGNORE | `Cancelled` | 仅是终止通知，不记错误 |

---

## 10. 相关 Event 定义

参考 `rust-core-event-inventory.md`。Sidecar 相关 canonical events：

| Event type | 触发时机 | Payload 要点 |
|-----------|----------|-------------|
| `resource.sidecar_started` | Core 发起 sidecar request | `request_id`, `tool_name`, `session_id`, `agent_id`, `resource_budget` |
| `resource.sidecar_completed` | Sidecar 返回成功 | `request_id`, `duration_ms`, `usage` |
| `resource.sidecar_failed` | Sidecar 返回错误 | `request_id`, `error_code`, `message`, `usage` |
| `resource.sidecar_cancelled` | Cancel token 关闭后 sidecar 确认 | `request_id`, `reason` (user/system/timeout) |
| `resource.sidecar_timeout` | Deadline 超时 | `request_id`, `deadline`, `actual_duration` |
| `resource.budget_exceeded` | Sidecar 超出 resource budget | `request_id`, `budget`, `actual_usage` |
| `sidecar.output_received` | Stream/progress 中间输出 | `request_id`, `sequence`, `data_summary` |
| `sidecar.error` | Sidecar 内部错误（非工具执行） | `request_id`, `code`, `message` |
| `sidecar.lease_expired` | Write lease 到期 | `request_id`, `lease_scope` |
| `tool.call_initiated` | 工具开始执行 | `request_id`, `tool_name`, `args_summary` |
| `tool.call_completed` | 工具执行完成 | `request_id`, `tool_name`, `result_shape`, `duration_ms` |
| `tool.call_failed` | 工具执行失败 | `request_id`, `tool_name`, `error_code`, `message` |
| `tool.call_timeout` | 工具执行超时 | `request_id`, `tool_name`, `deadline` |

---

## 11. Phase 1/2/3 任务定义

### Phase 1：核心基础设施

| ID | 任务 | 交付物 |
|----|------|--------|
| S-001 | 定义 `lingxiao-tool-host-protocol` crate，包含 `SidecarRequest`、`SidecarResponse`、`SidecarError`、`ResourceBudget`、`CancelToken` 等类型 | Rust crate + 序列化测试 |
| S-002 | 实现 Core 侧 sidecar 调度器：request 序列化 → process spawn → response 反序列化 → timeout/cancel → event emission | `lingxiao-core::sidecar::Scheduler` |
| S-003 | 实现 sidecar 侧 `ToolHost` 框架：request 反序列化 → dispatch → response/stream/progress → heartbeat | `lingxiao-tool-host::ToolHost` 参考实现 (Node.js) |
| S-004 | 实现 CancelToken 通道：Core 侧 `CancellationTokenSource` + 跨进程 signal | `lingxiao-core::sidecar::CancelToken` |
| S-005 | 实现 ResourceBudget 追踪：deadline 超时 + memory 监控 + usage 聚合 | `lingxiao-core::sidecar::ResourceTracker` |
| S-006 | 实现 PermissionLease 机制：write lease 签发 → 传递 → 后验校验 | `lingxiao-core::permission::Lease` |
| S-007 | Golden scenario G-007 sidecar tool contract e2e 验收 | e2e test |

### Phase 2：Sidecar 工具迁移

| ID | 任务 | 涉及 |
|----|------|------|
| S-008 | 实现 Browser sidecar tool-host（Playwright） | browser_action, screenshot, visual_contact_sheet, browser_visual_verify |
| S-009 | 实现 OCR sidecar tool-host（tesseract.js） | ocr |
| S-010 | 实现 Terminal PTY sidecar daemon | terminal_control, get_terminal_output |
| S-011 | 实现 MCP sidecar daemon | mcp (MCP server 调用) |
| S-012 | 实现 Python exec sidecar | python_exec |
| S-013 | 实现 Node REPL sidecar | node_repl |
| S-014 | 实现 Parse file sidecar | parse_file |

### Phase 3：Office 及剩余工具

| ID | 任务 | 涉及 |
|----|------|------|
| S-015 | 实现 Office sidecar tool-host | 文档生成/编辑/审查/渲染全链路 |
| S-016 | 实现 web_search browser-backed fallback sidecar | 非必要，仅 fallback |
| S-017 | 清理 legacy 工具注册和旧 TS sidecar 代码 | 移除 `WorkerProcessRunner` 依赖项 |
| S-018 | 性能基准：对比 per-call spawn vs daemon 模式资源开销 | 基准测试报告 |

---

## 12. QA Checklist

### 12.1 每个 Sidecar 类别验收检查

| # | 检查项 | 涉及角色 |
|---|--------|---------|
| 1 | Request envelope 包含 `request_id`/`session_id`/`task_id`/`agent_id`/`tool_name`/`args` | Sidecar + Core |
| 2 | Request envelope 包含 `capabilities` 声明列表 | Sidecar |
| 3 | Request envelope 包含 `deadline` (Timestamp) | Core |
| 4 | Request envelope 包含 `resource_budget` (ResourceBudget) | Core |
| 5 | Request envelope 包含 `cancel_token` (CancelToken) | Core |
| 6 | Response 类型覆盖 `Completed`/`Stream`/`Progress`/`Error` | Sidecar |
| 7 | `Completed` response 包含 `result`/`result_shape`/`duration_ms`/`usage` | Sidecar |
| 8 | Stream chunk 包含 `sequence`/`data`/`is_final` | Sidecar |
| 9 | Progress 包含 `percent`/`message`/`usage` | Sidecar |
| 10 | Error 包含 `request_id`/`code`/`message`/`usage` | Sidecar |
| 11 | Cancel 测试：关闭 cancel token 后 sidecar 在 1s 内响应 `Cancelled` | Sidecar + Core |
| 12 | Timeout 测试：超过 deadline 后 Core 关闭 cancel token，sidecar 响应 `Timeout` | Core |
| 13 | Heartbeat 测试：45s 无 heartbeat → Core kill sidecar | Core |
| 14 | Resource budget 测试：超过 `max_memory_mb` → Core kill sidecar | Core |
| 15 | Sidecar 不能打开 Core SQLite DB 文件 | Sidecar (禁写规则) |
| 16 | Sidecar 不能修改 canonical state（验证：Core state 在 sidecar 执行前后不变） | Sidecar (禁写规则) |
| 17 | 文件写入测试：有 lease 时可写 workspace，无 lease 时被拒绝 | Core + Sidecar |
| 18 | Per-call spawn 模式下每次调用产生独立进程 | Core |
| 19 | Daemon 模式下进程在 session 结束后被清理 | Core |
| 20 | 所有 canonical events (`resource.sidecar_started` 等) 在正确时机发射 | Core |

### 12.2 全局验收

| # | 检查项 |
|---|--------|
| G1 | 每个 sidecar 类别（browser/ocr/terminal/mcp/python/node_repl/office/parse_file）都有对应 envelope 定义 |
| G2 | 每个 sidecar 类别都实现了取消/超时机制 |
| G3 | 每个 sidecar 类别都声明了 resource budget |
| G4 | 每个 sidecar 类别都遵守 DB 禁写规则 |
| G5 | 每个 sidecar 类别都遵守 canonical state 禁写规则 |
| G6 | 每个 sidecar 类别都通过了 G-007 golden scenario |
| G7 | Rust Core 不接受 sidecar 结果作为事实，必须经过验证和语义转换后落事件和状态 |
| G8 | Phase 1 交付物有自动化测试覆盖 |
| G9 | Phase 2 交付物有 e2e golden scenario 测试 |
| G10 | 文档中所有 event type 与 `rust-core-event-inventory.md` 一致 |

---

## 13. 验收核对命令

```powershell
# 1. 确认文件已创建
Test-Path -LiteralPath "docs/contracts/rust-core-sidecar-boundary.md"

# 2. 检查文档无 markdown 语法错误（使用 Node.js 工具）
npx markdownlint-cli2 docs/contracts/rust-core-sidecar-boundary.md --fix

# 3. 验证所有交叉引用链接有效
# 需要手动核查以下引用点：
#   - 第 6.2 节: rust-core-function-inventory.md line 422
#   - 第 10 节: rust-core-event-inventory.md section 3.1

# 4. 核对 sidecar 工具清单与 src/tools/ToolMetadata.ts 一致
#    预期 sidecar 工具数 >= 12:
#    browser_action, browser_visual_verify, screenshot, visual_contact_sheet,
#    ocr, python_exec, node_repl, mcp, terminal_control, get_terminal_output,
#    parse_file, office_ops (+ Office generate/edit/inspect tools)
gci -Name "src/tools/implementations/*.ts" | Select-String "(BrowserAction|Screenshot|VisualContactSheet|BrowserVisualVerify|OCR|PythonExec|NodeRepl|Mcp|TerminalControl|GetTerminalOutput|ParseFile|Office|EditDocx|EditPptx|EditXlsx|GenerateDocx|GeneratePptx|GenerateXlsx|GeneratePdf)"

# 5. 对比 rust-core-function-inventory.md 中的 sidecar 清单
Select-String -Pattern "Sidecar 工具" -Context 0,20 "docs/contracts/rust-core-function-inventory.md"

# 6. 确认无业务代码被修改
git diff --name-only
# 应该只输出:
# docs/contracts/rust-core-sidecar-boundary.md
```
