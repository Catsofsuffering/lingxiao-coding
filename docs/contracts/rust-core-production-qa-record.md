# Rust Core Production QA Record

Date: 2026-07-06

Baseline HEAD: `4687fa7 Fix Rust core provider tool loop parity`

Worktree: `C:\Users\peony\.paseo\worktrees\30nx4bm8\rude-rat`

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
- Final Fable/GPT 5.5 re-audit findings on commit `a5c0c86`: `Bearer` token
  persistence through `session.input_received`, non-green Windows shell drain
  test, raw Anthropic SSE duplicate `Finished`, one-shot `mcp.bridge` stderr
  drain coverage, and Anthropic SDK response decode incompatibility with the
  live gateway.

## Fix Summary

- Gemini provider now maps `ToolDefinition.input_schema` into Gemini function
  parameters, supports `streamGenerateContent?alt=sse` when `request.stream` is
  true, and generates unique fallback tool-call IDs.
- Bedrock provider now implements true streaming parity: `stream=true` calls
  `InvokeModelWithResponseStream` and parses the Anthropic-on-Bedrock event
  stream (each AWS event-stream `chunk`'s `PayloadPart.bytes` blob is one
  complete Anthropic Messages SSE event JSON, parsed by the new pure
  `bedrock_stream_chunk_to_events` mirroring the Anthropic provider's
  `raw_anthropic_sse_value_to_events`), instead of the prior explicit
  `UnsupportedModel` short-circuit. Non-streaming behavior and multimodal are
  unchanged. 14 new tests cover the parser, routing, and a genuine
  `aws-smithy-eventstream` framing round-trip (no real AWS calls).
- External process LLM and native shell paths drain stdout/stderr concurrently
  with bounded buffers, preventing pipe-buffer deadlocks.
- Anthropic streaming suppresses duplicate `Finished` events so `MessageDelta`
  finish reasons are not overwritten by `MessageStop`; both SDK streaming and
  raw SSE extended-thinking paths have regression coverage.
- Anthropic non-streaming requests use raw HTTP response parsing to avoid
  SDK/gateway response decode incompatibilities while preserving Anthropic
  status-code mapping and auth headers.
- OpenAI provider has contract coverage for preserving assistant content when
  tool calls are present.
- `RouterAgentLlmExecutor` and `llm.call` accumulate `ToolCallDelta`, persist
  model tool requests as `status='model_tool_request'`, emit
  `llm.model_tool_request`, and avoid synthetic `tool.call_completed`.
- `session.run_task` accumulates `ToolCallDelta`, dispatches provider-requested
  native tools through the canonical tool executor, persists real
  `tool.call_initiated` / `tool.call_completed` events and `tool_calls` rows,
  then sends the tool observations back to the provider before completing the
  task/session with a final answer.
- Conversation persistence redacts likely secrets and bounds persisted tool
  output content before writing `agent_conversation`, `leader_conversation`,
  events, and sensitive SQLite projections.
- Event-log redaction now covers `Bearer` tokens and `sk-ant-` style keys, and
  `session.input_received` persists a redacted payload while returning the
  original in the command response.
- MCP server lifecycle now keeps a persistent stdio process for
  `mcp.server_start` -> `tools/list` -> `mcp.call_tool` -> `mcp.server_stop`,
  enforces session workspace-scoped cwd, and marks owned process rows failed on
  timeout/error.
- One-shot `mcp.bridge` drains stdout/stderr concurrently with bounded buffers
  and has regression coverage for large stderr plus timeout -> failed process
  registry state.
- `session.create` and `session.run_task` canonicalize/validate workspace roots
  and reject filesystem roots or invalid workspaces.
- Real OpenAI live E2E now proves real provider -> model tool request ->
  canonical `file_read` execution -> final answer -> `tool_calls` row ->
  durable replay.

## Verification Evidence

- `cargo fmt --check`: passed.
- `cargo clippy --workspace --all-targets -- -D warnings`: passed.
- `cargo test --workspace --quiet`: passed.
  The workspace run included `lingxiao-core` 287 tests, provider contract
  tests, daemon tests, and all crate test suites reported by Cargo.
- `npx tsx --test src/llm/customModelName.test.ts`: passed.
  The equivalent `node node_modules/tsx/dist/cli.mjs --test
  src/llm/customModelName.test.ts` command also passed.
- Targeted live OpenAI Responses tool-call E2E:
  `cargo test -p lingxiao-core-daemon test_stdio_real_openai_responses_tool_call_executes_canonical_tool_when_key_is_present -- --nocapture`
  passed with `.env` loaded from the current worktree. The test asserts
  `tool.call_initiated`, `tool.call_completed`, one completed `tool_calls` row,
  final answer `OPENAI_TOOL_E2E_FINAL canonical-file-read-ok`, and durable
  replay containing the tool events.
- Anthropic live smoke used `.env` without printing secrets and passed through
  the provider binary with event summary `TextDelta,Usage,Finished` and
  expected text `ANTHROPIC_SMOKE_OK`.
- Targeted regressions passed for:
  `test_conversation_tables_redact_secret_content`,
  `test_shell_large_stdout_drain_does_not_deadlock`,
  `test_mcp_bridge_large_stderr_does_not_deadlock`,
  `test_mcp_bridge_timeout_marks_process_failed`,
  `test_raw_anthropic_sse_does_not_emit_double_finished`,
  `test_execute_messages_against_mock_http_server`, and
  `test_auth_error_maps_to_provider_error`.

## Remaining Risks

- Bedrock streaming is now implemented (R-9): `stream=true` calls
  `InvokeModelWithResponseStream` and parses the Anthropic-on-Bedrock event
  stream into host `StreamEvent`s (text/thinking/tool-use deltas, usage,
  finish, duplicate-`Finished` suppression, safe error handling). The prior
  explicit `UnsupportedModel` short-circuit is removed. A real-provider Bedrock
  streaming smoke against a live `anthropic.claude-*` model remains
  operator-credential-gated future work (not a code gap); parser/framing
  coverage is unit-tested without real AWS calls.
- Anthropic live smoke passed for a minimal request. Tool-use and thinking
  variants should still be rechecked when the upstream gateway supports those
  response shapes consistently.
- `route_stream_with_sink` still buffers a successful provider attempt before
  flushing to the caller sink so retry/fallback can avoid exposing partial failed
  attempts. This preserves correctness but leaves realtime latency as a P3
  design item.
- Retry/fallback total wall-clock deadline remains bounded by per-attempt
  provider timeouts and retry counts, not by a single global deadline across the
  whole fallback chain. **Verified (2026-07-08, R-8 scoping) not a TS-parity
  gap:** TS `LlmGuard.call()` (src/agents/LlmGuard.ts:247 `while(true)` loop)
  likewise imposes no total/global deadline over the retry+fallback chain — it
  bounds each attempt independently (SDK `request_timeout` 180s + hang watchdog
  + first-token timeout, all per-attempt) and exits on retry-count exhaustion
  (`maxRetries` default 3) / circuit-open (8 failures) / caller abort. The only
  time-based outer bound is the Leader per-round 600s abort, which wraps the
  whole `llmGuard.call()` as an outer safety net, not a deadline within the
  chain. Both sides are per-attempt-bounded; this is a shared P3 design item,
  not a Rust-vs-TS divergence.
- Periodic orphan cleanup exists at daemon boot via `ProcessRegistry`, and
  retention has a daemon ticker. **A long-running periodic process-orphan
  reconcile sweep is now scheduled** (2026-07-08, R-8): a default-off
  `ProcessReconcileTicker` spawned when `background_process_reconcile_ms` is
  `Some(n>0)` calls the new non-killing `ProcessRegistry::reconcile_orphans`
  each tick, which probes each `active` `owned_processes` row for liveness via
  `pid_is_alive` and flips dead-PID rows to `reconciled_dead` without ever
  killing a live managed process (the boot-time `cleanup_orphans` kills every
  active row and remains boot-only — safe at boot, unsafe periodically). This
  closes the registry-leak where a row left `active` by a mid-session process
  death stayed active forever.
- MCP command execution is now permission-gated and workspace-cwd scoped, but
  executable program allowlisting is still policy-driven by the `mcp` grant
  rather than a separate static allowlist.
