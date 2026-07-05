# Rust Core Production Acceptance Gate

This gate is the shared stop condition for the Rust Core production migration loop.
Rust Core is not production-ready until every item below is proven against the current
worktree with source inspection, tests, and real-provider smoke where applicable.

## Required Evidence

- `cargo fmt --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace --quiet`
- OpenAI live E2E using `.env`, without printing API keys.
- Anthropic live smoke using `.env` when the upstream gateway is available. If the
  gateway is unavailable, record the retryable upstream error without printing keys.
- Targeted provider contract tests for tool-call request/response bodies.
- A final multi-model audit result with GPT, Opus, and CodeBuddy custom models.

## P0 Functional Gates

### LLM Tool Requests Are Not Fake Tool Completions

Failure condition:

- `llm.call` inserts model-requested `StreamEvent::ToolCall` into `tool_calls` as
  `status = 'completed'`.
- `llm.call` emits `tool.call_completed` for a tool that Core did not execute.
- `result_json` is synthesized as success, for example `{"ok": true}`, without a
  canonical tool execution.

Required behavior:

- `llm.call` may record model tool requests only as pending/requested metadata.
- Canonical completion may only come from `tool.call` or AgentLoop tool execution
  after permission, resource, and runtime preflight.
- Durable event names must distinguish model tool request from actual tool
  initiation/completion.

Required tests:

- A provider stream that returns `ToolCall` through `llm.call` must not create a
  completed `tool_calls` row.
- The same stream must not emit `tool.call_completed`.

### AgentLoop Handles ToolCallDelta

Failure condition:

- AgentLoop ignores `StreamEvent::ToolCallDelta`.
- A provider that streams only deltas plus `Finished(ToolCalls)` loses the tool call.

Required behavior:

- AgentLoop uses `ToolCallAccumulator` or equivalent defensive accumulation.
- Providers should emit final `ToolCall` when possible, but consumers must not rely
  solely on that normalization.

Required tests:

- A mock provider that emits only `ToolCallDelta` chunks causes AgentLoop to execute
  the requested tool.

### OpenAI Chat Tool Protocol Is Complete

Failure condition:

- Chat Completions request omits `tools` when `GenerateRequest.tools` is non-empty.
- Assistant `tool_calls` history is dropped.
- `tool` role history is mapped to a normal user message.
- Non-streaming tool calls are not extracted.
- Streaming tool-call deltas do not produce a final accumulated `ToolCall`.

Required behavior:

- Chat request contract preserves tools, assistant tool calls, and tool results.
- Streaming and non-streaming paths both normalize to protocol `ToolCall` events.

Required tests:

- OpenAI Chat request-body contract test with tools and tool history.
- OpenAI Chat streaming tool-call test covering delta accumulation.

### Gemini And Bedrock Tool Protocols

Failure condition:

- Provider request bodies do not declare tools from `GenerateRequest.tools`.
- Assistant tool-use and tool-result history are omitted or mapped incorrectly.
- Provider claims streaming support without stream endpoint parsing tests.

Required behavior:

- Request-body contract tests cover tools and tool history.
- Response parsing tests cover tool-call extraction and final `ToolCall` emission.
- Any missing real streaming support is explicitly tracked and must not be claimed
  as production parity.

## P1 Safety And Audit Gates

### Canonical Tool Ledger

Failure condition:

- AgentLoop tool execution emits only lifecycle events and does not write
  `tool_calls`.
- Projection/debug reads `tool_calls` but AgentLoop executions are absent.

Required behavior:

- Command `tool.call` and AgentLoop share begin/finish ledger semantics.
- `tool_calls` records actual execution status, sanitized args/result metadata, and
  terminal error state.

### Sensitive Data Persistence

Failure condition:

- `workflow_executions.context` stores raw `cmd.params`.
- `auth_context`, API keys, bearer tokens, or obvious secret strings are persisted in
  workflow context, traces, events, or debug projections.
- Full tool outputs containing likely secrets are stored without redaction or bounded
  metadata.

Required behavior:

- Persist only necessary metadata, size, hash/summary, and status for sensitive paths.
- Debug/projection redaction is not the only defense; raw SQLite tables must be safe.

Required tests:

- Negative tests search SQLite rows after commands containing fake keys such as
  `sk-test-secret` and assert they are not persisted in sensitive tables.

### Workspace Permission Boundaries

Failure condition:

- Read-only tools can read arbitrary absolute paths outside the session workspace.
- Scope checks depend on string prefixes rather than canonical path boundaries.
- Symlink, non-existent path parent, or Windows path normalization bypasses scope.

Required behavior:

- Read and write tools enforce session/workspace root policy.
- Scope grants are canonicalized at grant/write time where possible and rechecked at
  execution time.

Required tests:

- Absolute outside-workspace read denied.
- Symlink escaping workspace denied where platform supports symlink tests.
- Prefix sibling such as `C:\tmp` vs `C:\tmp_safe` denied.

### Sidecar Output Lifecycle

Failure condition:

- Sidecar child stdout/stderr pipes are not drained while the process is running.
- Large sidecar output can block the child before exit.

Required behavior:

- Drain stdout/stderr concurrently or asynchronously.
- Preserve bounded ring-buffer output for diagnostics.
- Timeout, cancel, and orphan cleanup still work.

Required tests:

- Sidecar command that emits output larger than typical OS pipe buffers completes or
  times out deterministically without hanging.

## P2/P3 Reliability Gates

- Provider request timeouts are enforced at the provider layer and supervisor/round
  layer.
- Idempotency for expensive real provider calls claims the key before side effects or
  provides an equivalent in-flight dedupe guarantee.
- Token budget commit checks actual usage, handles over-reservation, and records
  exceeded budget events.
- Retention/compaction is automatic or scheduled for high-growth tables:
  `event_log`, `tool_calls`, `traces`, `token_usage`, `llm_gateway_requests`,
  `agent_conversation`, and team/message tables.
- Process polling is bounded and justified; one-shot external commands use timeout
  waits or async process APIs.

## Production Branch Exit Criteria

The branch may be considered production-ready only when:

- All P0 gates pass.
- All P1 safety/audit gates pass.
- P2/P3 reliability gates have either landed or are explicitly documented as
  non-blocking with user approval.
- Multi-model QA returns no blocking findings.
- Real OpenAI E2E proves: user task input -> real provider -> tool call -> canonical
  tool execution -> final answer -> durable event replay.
- The worktree can be pushed as a single production branch without relying on
  untracked, undocumented local state.
