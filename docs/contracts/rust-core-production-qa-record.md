# Rust Core Production QA Record

Date: 2026-07-06

Baseline HEAD: `7371024 feat: add rust core production parity`

Worktree: `C:\Users\peony\.paseo\worktrees\30nx4bm8\rust-core-prod-loop-gpt-55`

## Source Review Scope

- Read `docs/contracts/rust-core-production-acceptance-gate.md`.
- Read `docs/contracts/PARITY-LEDGER.md`.
- Reviewed Rust Core/provider crates touched by the QA blockers:
  `lingxiao-core`, `lingxiao-core-daemon`, `lingxiao-llm-openai-provider`,
  `lingxiao-llm-anthropic-provider`, `lingxiao-llm-gemini-provider`,
  and `lingxiao-llm-bedrock-provider`.
- Scanned TS parity paths `src/core`, `src/llm`, and `src/agents`, including
  `AgentRoundExecutor.ts`, `StreamingToolCallParser.ts`,
  `ContentGenerationPipeline.ts`, and workspace/session paths.

## QA Inputs Addressed

- OpenCode Qwen FAIL on Gemini tool schema, Gemini/Bedrock streaming contracts,
  external provider stderr deadlock, Anthropic double finish, OpenAI assistant
  content with tool calls, AgentLoop ledger gap, and Gemini tool-call ID collision.
- GPT 5.5 blocker for `session.run_task` completing after model tool request
  without observation/final answer.
- OpenCode GLM evidence gate for real OpenAI Responses tool-call E2E and a
  durable QA record.
- OpenCode Kimi security blockers for persisted tool-output secrets, MCP
  lifecycle, workspace roots, MCP cwd/program boundary, and MCP timeout process
  registry state.
- DeepSeek reliability items reviewed for retention, command dedupe cleanup,
  orphan cleanup, total retry/fallback deadline, and streaming sink latency.

## Fix Summary

- Gemini provider now maps `ToolDefinition.input_schema` into Gemini function
  parameters, supports `streamGenerateContent?alt=sse` when `request.stream` is
  true, and generates unique fallback tool-call IDs.
- Bedrock provider no longer silently ignores `stream=true`; it emits an explicit
  unsupported streaming capability error.
- External process LLM and native shell paths drain stdout/stderr concurrently
  with bounded buffers, preventing pipe-buffer deadlocks.
- Anthropic streaming suppresses duplicate `Finished` events so `MessageDelta`
  finish reasons are not overwritten by `MessageStop`.
- OpenAI provider has contract coverage for preserving assistant content when
  tool calls are present.
- `RouterAgentLlmExecutor`, `llm.call`, and `session.run_task` now accumulate
  `ToolCallDelta`, persist model tool requests as `status='model_tool_request'`,
  emit `llm.model_tool_request`, and avoid synthetic `tool.call_completed`.
- `session.run_task` no longer completes task/session with an empty answer when
  the model finishes with `toolcalls`; it returns `waiting_for_tool` and leaves
  durable state non-terminal until observation/final answer.
- Conversation persistence redacts likely secrets and bounds persisted tool
  output content before writing `agent_conversation`, `leader_conversation`,
  events, and sensitive SQLite projections.
- MCP server lifecycle now keeps a persistent stdio process for
  `mcp.server_start` -> `tools/list` -> `mcp.call_tool` -> `mcp.server_stop`,
  enforces session workspace-scoped cwd, and marks owned process rows failed on
  timeout/error.
- `session.create` and `session.run_task` canonicalize/validate workspace roots
  and reject filesystem roots or invalid workspaces.
- Real OpenAI live E2E now proves real provider -> model tool request ->
  canonical `file_read` execution -> final answer -> `tool_calls` row ->
  durable replay.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace --quiet`: passed.
  The workspace run included `lingxiao-core` 286 tests, provider contract tests,
  daemon tests, and all crate test suites reported by Cargo.
- Targeted live OpenAI Responses tool-call E2E:
  `cargo test -p lingxiao-core-daemon test_stdio_real_openai_responses_tool_call_executes_canonical_tool_when_key_is_present -- --nocapture`
  passed with `.env` loaded from the sibling worktree. The test asserts
  `tool.call_initiated`, `tool.call_completed`, one completed `tool_calls` row,
  final answer `OPENAI_TOOL_E2E_FINAL canonical-file-read-ok`, and durable
  replay containing the tool events.
- Anthropic live provider checks used `.env` without printing secrets.
  Both the streaming tool-use request and minimal non-streaming smoke reached
  the provider and returned structured provider `Error` events with
  `code=ServerError`. This is recorded as upstream/gateway failure, not local
  request construction or event parsing failure.
- `npm run test:ci`: partially passed.
  `npm run test:scripts` passed 6/6 and `npm run build:server` completed.
  The full TS runner then failed because the current Node runtime rejects
  `--test-isolation=process`; the package requires Node `>=24`, while this
  environment is Node 22.

## Remaining Risks

- Bedrock streaming is explicitly unsupported in this branch rather than fully
  implemented. The production contract now prevents silent streaming parity
  claims, but true Bedrock streaming parity remains future work.
- Anthropic live validation is blocked by an upstream/gateway `ServerError`.
  Local contract tests cover thinking/tool-use event mapping and duplicate
  finish suppression; a clean upstream response is still needed for final live
  Anthropic evidence.
- `route_stream_with_sink` still buffers a successful provider attempt before
  flushing to the caller sink so retry/fallback can avoid exposing partial failed
  attempts. This preserves correctness but leaves realtime latency as a P3
  design item.
- Retry/fallback total wall-clock deadline remains bounded by per-attempt
  provider timeouts and retry counts, not by a single global deadline across the
  whole fallback chain.
- Periodic orphan cleanup exists at daemon boot via `ProcessRegistry`, and
  retention has a daemon ticker. A long-running periodic process-orphan sweep is
  not separately scheduled in this patch.
- MCP command execution is now permission-gated and workspace-cwd scoped, but
  executable program allowlisting is still policy-driven by the `mcp` grant
  rather than a separate static allowlist.
