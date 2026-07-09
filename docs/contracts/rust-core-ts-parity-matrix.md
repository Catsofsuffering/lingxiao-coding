# Rust Core ⇄ TS Core Parity Matrix

**Date:** 2026-07-08 (R-9 Bedrock streaming parity slice; R-7 live permission-mode mutation slice; R-7 deferred LeaderPermissionManager auto-response decision-record slice; R-7 follow-up Leader-bus escalation signal slice; R-7 agent tool-failure circuit-breaker in-process slice; R-6 agent tool-loop probe `ToolLoopDetector` slice; R-4 models.dev capability-registry auto-derive slice; R-5 blob rehydration retain-rounds slice; R-3 Bedrock multimodal slice; R-2 Gemini multimodal slice; R-1 Anthropic multimodal slice; R-4 vision-gating slice; base audit 2026-07-07)
**Branch:** `latest-lingxiao-update` (uncommitted working tree)
**Owner:** Rust Core parity main executor
**Scope:** Cross-cutting parity audit of the uncommitted working-tree changes
plus the existing migrated surface, mapped against TS Core source. This
document is the live gap register; each row cites file/line evidence on both
sides and an acceptance condition.

---

## How to read this matrix

- **PARITY** — Rust matches TS behavior for production use; cited tests pass.
- **PARTIAL** — Rust ships the surface but a behavior gap remains; the gap
  row names it and links the acceptance condition.
- **GAP** — TS has the capability; Rust does not yet wire it.
- **NON-TARGET** — Intentionally out of Rust Core scope (adapter/frontend
  responsibility), recorded with the product acceptance condition.

Status legend per area: ✅ PARITY · 🟡 PARTIAL · ❌ GAP · ⛔ NON-TARGET.

---

## 1. LLM typed content / multimodal / thinking / usage

| Item | TS source | Rust source | Status | Evidence / gap |
|------|-----------|-------------|--------|----------------|
| `MessageContentPart` enum (text/thinking/redacted_thinking/image_url/image_blob_ref) | `src/contracts/types/Message.ts:5-76` | `crates/lingxiao-llm-host-protocol/src/provider.rs:25-46` | ✅ PARITY | Same wire shape, `#[serde(tag="type", rename_all="snake_case")]`. Round-trip + legacy-string-content deserialize tested (`provider.rs` tests). |
| `contentToPlainText` / `plain_text_content` | `Message.ts:83-114` | `provider.rs:66-73`, `Message::plain_text_content` | ✅ PARITY | Identical `[image]` / `[image: mime, NKB stored as blob:shortid]` placeholders. |
| Image blob rehydration (disk → `data:` URI) | `src/llm/image_blob_store.ts:65-72` `rehydrateImageBlobRef` | `provider.rs` `rehydrate_image_blob_ref` + `base64_encode` | ✅ PARITY | RFC-4648 base64; `test_rehydrate_image_blob_ref_*`; missing file → `None` fallback. |
| OpenAI Chat-Completions multimodal content array | `OpenAIContentGenerator.ts:719-744` `toOpenAIContentParts` → `{type:image_url,image_url}` | `openai-provider/src/lib.rs` `to_chat_message` + `user_message_content_parts` | ✅ PARITY (this packet) | User message emits `ChatCompletionRequestUserMessageContent::Array` with `ImageUrl` parts rehydrated from blobs; `test_to_chat_message_emits_image_url_content_part_for_user`, `test_to_chat_message_rehydrates_blob_ref_into_image_url`. |
| OpenAI Responses-API multimodal input | (TS uses Chat path; Responses is Rust-canonical) | `openai-provider/src/lib.rs` `responses_input` → `openai_responses_content_parts` | ✅ PARITY (this packet) | Emits `input_text`/`input_image` content array; `test_responses_input_emits_multimodal_content_array`, `test_responses_input_rehydrates_blob_ref_to_data_uri`. |
| Recent-rounds blob rehydration window (retain N user rounds) | `image_blob_store.ts:119-166` `rehydrateRecentImageBlobRefs` (DEFAULT_RETAIN_IMAGE_ROUNDS=2) | `lingxiao-llm-host-protocol/src/provider.rs` `image_retain_cutoff`/`message_rehydrates_blob_at`/`rehydrate_image_blob_ref_if`/`retain_rounds_from_metadata`/`normalize_image_retain_rounds` (`DEFAULT_RETAIN_IMAGE_ROUNDS=2`); wired into all four providers | ✅ PARITY (this slice) | All four providers now gate `image_blob_ref` rehydration on a retain-rounds window defaulting to 2 (read from `request.options.metadata["image_history_retain_rounds"]`, the Rust analog of TS `advanced.image_history_retain_rounds`). Only the recent N user rounds rehydrate real image bytes; older blobs degrade to the safe `plain_text()` placeholder (short id, no `blob_path` leak, no panic) instead of sending full bytes — matching TS `rehydrateRecentImageBlobRefs`. `image_url`/data-URI parts are never gated (only blob refs are). Round semantics match TS exactly: scan messages from the end counting `role=="user"`, cutoff = index of the Nth-from-last user message (or 0 when fewer than N user rounds exist, so nothing is silently dropped). Shared cutoff logic lives in host-protocol; each provider loop computes the per-message `rehydrate` flag and forwards it. Tests: host-protocol `test_normalize_image_retain_rounds_mirrors_ts`, `test_image_retain_cutoff_mirrors_ts_scan`, `test_retain_rounds_from_metadata_reads_config_key`, `test_rehydrate_image_blob_ref_if_gates_on_eligibility`, `test_openai_chat_content_parts_with_rehydrate_false_degrades_blob_to_text`, `test_openai_chat_content_parts_with_rehydrate_false_keeps_image_url_parts`, `test_openai_responses_content_parts_with_rehydrate_false_degrades_blob_to_text`, `test_message_rehydrates_blob_at_applies_retain_window_over_message_list`; OpenAI `test_responses_input_retain_window_degrades_old_blob_but_rehydrates_recent`, `test_to_chat_message_retain_window_degrades_old_blob_to_text`; Anthropic `test_anthropic_retain_window_degrades_old_blob_to_text_on_wire`; Gemini `test_gemini_retain_window_degrades_old_blob_to_text_on_wire`; Bedrock `test_bedrock_retain_window_degrades_old_blob_to_text_on_wire`. |
| Anthropic thinking block decode (non-streaming raw response) | `AnthropicContentGenerator` maps `thinking` blocks | `anthropic-provider/src/lib.rs` `raw_anthropic_message_to_events` `thinking` arm | ✅ PARITY (prior packet) | `test_raw_anthropic_message_to_events_maps_thinking_and_tool_use`. |
| Anthropic multimodal image input (image_url → Anthropic image block) | `AnthropicContentGenerator` builds image content blocks | `anthropic-provider/src/lib.rs` `add_message` → `anthropic_user_message_content`/`anthropic_image_block_from_url` | ✅ PARITY (this slice) | User messages with typed `content_parts` now emit Anthropic content blocks: `text` → `ContentBlockParam::Text`, `image_url` data URI → `ContentBlockParam::Image{source:{type:"base64",media_type,data}}`, `image_blob_ref` rehydrated from `blob_path` → image block. Missing/unreadable blob degrades to the safe `[image: …]` text placeholder (short id only, no path leak) instead of panicking. Plain-text user messages stay `MessageContent::Text` (byte-for-byte legacy). Tests: `test_anthropic_user_message_emits_image_block_for_data_uri_image_url`, `test_anthropic_user_message_rehydrates_blob_ref_into_image_block`, `test_anthropic_user_message_missing_blob_falls_back_to_safe_text_placeholder`, `test_anthropic_user_message_pure_text_stays_compatible`, `test_anthropic_add_message_preserves_assistant_text_and_tool_use`, `test_anthropic_add_message_emits_image_block_in_serialized_params`. |
| Gemini multimodal image input | Gemini provider builds `inline_data` parts | `gemini-provider/src/lib.rs` `add_message` → `gemini_user_content` emits `Part::InlineData` | ✅ PARITY (this slice) | User messages with typed `content_parts` now emit Gemini image-capable parts: `text` → text part, `image_url` data URI → `Part::InlineData{inline_data:Blob{mime_type,data}}`, `image_blob_ref` rehydrated from `blob_path` → `inline_data`. Missing/unreadable blob degrades to the safe `[image: …]` text placeholder (short id only, no path leak) instead of panicking; remote non-data-URI `image_url` degrades to a `[image] <url>` text marker. Plain-text user messages stay `with_user_message(text)` (byte-for-byte legacy); assistant `function_call` and tool `function_response` paths unchanged. `text`/`thinking`/`redacted_thinking` parts map to text parts. Tests: `test_gemini_user_content_data_uri_image_url_emits_inline_data`, `test_gemini_user_content_rehydrates_blob_ref_into_inline_data`, `test_gemini_user_content_missing_blob_falls_back_to_safe_text_placeholder`, `test_gemini_user_content_remote_image_url_degrades_to_text_marker`, `test_gemini_user_content_pure_text_stays_text_only`, `test_gemini_add_message_preserves_assistant_function_call_and_tool_response`, `test_gemini_inline_data_emitted_on_wire_for_image_request` (mock-server wire assertion). |
| Bedrock multimodal image input | Bedrock `image` content blocks | `bedrock-provider/src/lib.rs` `bedrock_message` → `bedrock_user_content` + `bedrock_image_source_from_url` | ✅ PARITY (this slice) | User messages with typed `content_parts` now emit Anthropic-on-Bedrock content blocks: `text` → `{type:"text",text}`, `image_url` data URI → `{type:"image",source:{type:"base64",media_type,data}}`, `image_blob_ref` rehydrated from `blob_path` → image block. Missing/unreadable blob degrades to the safe `[image: …]` text placeholder (short id only, no path leak) instead of panicking; remote non-data-URI `image_url` degrades to a `[image] <url>` text marker. Plain-text user messages stay the legacy single text block (byte-for-byte); assistant `tool_use`, tool `tool_result`, and `system` paths unchanged. Tests: `test_bedrock_message_data_uri_image_url_emits_image_block`, `test_bedrock_message_rehydrates_blob_ref_into_image_block`, `test_bedrock_message_missing_blob_falls_back_to_safe_text_placeholder`, `test_bedrock_message_remote_image_url_degrades_to_text_marker`, `test_bedrock_message_pure_text_stays_compatible`, `test_bedrock_message_assistant_and_tool_paths_unaffected_by_multimodal`, `test_build_invoke_body_emits_image_block_on_wire_for_multimodal_request`. |
| `TokenUsage` (incl. cache/reasoning fields) | `usageExtractor.ts` | `types.rs:4-12` `TokenUsage` | ✅ PARITY | cache_creation/read + reasoning_tokens present; written to DB. |
| Model capability metadata: tools | `model_capabilities.ts` | `llm.rs:634-682` `ModelRoutingMetadata.supports_tools` | ✅ PARITY | `resolve_for_request` filters by `supports_tools`. |
| Model capability metadata: streaming | (TS streaming gating is per-provider config) | `llm.rs` `supports_streaming` + `resolve_for_request(needs_streaming)` | ✅ PARITY (this packet) | `test_model_routing_metadata_filters_stream_incompatible_provider`. |
| Model capability metadata: vision | `model_capabilities.ts` `supportsVision`/`getInputModalities` (models.dev) | `llm.rs` `ModelRoutingMetadata.supports_vision` + `vision_declared` + `resolve_for_request(needs_vision)`; `provider.rs` `GenerateRequest::needs_vision`; `ModelsDevRegistry` (embedded `models.dev` snapshot) auto-derivation | ✅ PARITY (this slice) | Routing-level vision gating landed (prior slice) and `supports_vision` now auto-derives from the embedded offline `models.dev` registry when operator metadata does not declare it — closing the R-4 PARTIAL gap. `ModelsDevRegistry` (`llm.rs`) embeds the build-time `src/llm/models-snapshot.json` via `include_str!` (no network fetch), lazily parses it into a lowercased-id → vision index (`vision = attachment || modalities.input ∋ image`, merged across providers with vision=true winning — mirroring TS `buildIndex`/`normalize`), and resolves via exact-then-longest-prefix match (mirroring TS `getModelInfo`). `ProviderRegistry::with_capability_registry` attaches it; `resolve_for_request` consults it only when `vision_declared=false` (operator did not call `with_vision_support`), so the precedence is: (1) explicit operator `supports_vision` (runtime.json) wins; (2) registry auto-derive; (3) optimistic default `true` on registry miss. Pure-text requests are unaffected. Daemon attaches `ModelsDevRegistry` by default in `configure_router`. Tests: `test_capability_registry_vision_explicit_false_overrides_registry_true`, `test_capability_registry_vision_false_filters_out_image_request`, `test_capability_registry_vision_true_routes_image_request`, `test_capability_registry_miss_keeps_optimistic_default_true`, `test_capability_registry_does_not_affect_plain_text_request`, `test_capability_registry_vision_false_does_not_affect_routing_without_registry`, `test_models_dev_registry_embeds_and_parses_snapshot`, `test_models_dev_registry_prefix_match_resolves_family_alias` (+ prior R-4 tests unchanged). **TS-side OCR fallback (`local_vision_fallback.ts`) is intentionally NON-TARGET for Rust Core** (no tesseract in Rust); the Rust gate fails fast with a clear vision-specific error instead of degrading. |
| Token counting estimate uses plain text | `ContextTokenCalculator.ts` | `llm.rs:1050` `estimate_request_tokens` → `plain_text_content()`; `command.rs:11642` `estimate_messages_tokens` | ✅ PARITY | Both call sites updated to project typed content. |

---

## 2. Provider real E2E (OpenAI / Anthropic / Gemini / Bedrock)

| Provider | Tool protocol | Streaming | Multimodal | E2E evidence | Status |
|----------|---------------|-----------|------------|--------------|--------|
| OpenAI Chat Completions | tools + assistant tool_calls + tool role preserved (`to_chat_message`, `chat_tool_call_from_protocol`) | delta accumulation → `ToolCall` (`execute_chat_completion_stream`) | ✅ image_url + rehydrated blob (this packet) | contract tests + live Responses E2E (qa-record) | ✅ PARITY |
| OpenAI Responses API | function_call/function_call_output replay (`responses_input`) | SSE delta parsing | ✅ input_image (this packet) | `test_responses_api_tool_call_is_not_dropped`, `test_responses_input_emits_multimodal_content_array` | ✅ PARITY |
| Anthropic | tool_use blocks → `ToolCall`; thinking blocks decoded | raw SSE + SDK; double-`Finished` suppressed | ✅ image block (this slice) | `test_raw_anthropic_message_to_events_maps_thinking_and_tool_use`, `test_anthropic_add_message_emits_image_block_in_serialized_params`; live smoke passes | ✅ PARITY |
| Gemini | `FunctionDeclaration` parameters via JSON round-trip; unique tool-call IDs | `streamGenerateContent?alt=sse` when `stream=true` | ✅ inline_data (this slice) | `test_gemini_inline_data_emitted_on_wire_for_image_request`, `test_gemini_tools_declared_as_function_declarations`, `test_gemini_stream_true_uses_streaming_endpoint` | ✅ PARITY |
| Bedrock | tool_use/tool_result history | `stream=true` → `InvokeModelWithResponseStream` + Anthropic-on-Bedrock chunk parsing (this slice) | ✅ image source bytes (R-3) | provider tests incl. streaming chunk parser + event-stream framing round-trip | ✅ PARITY |

---

## 3. Tool registry governance (schema validation / permissions / mode policy / timeouts / recovery hints)

| Item | TS source | Rust source | Status | Evidence |
|------|-----------|-------------|--------|----------|
| Tool definition schema | `Tool.ts` | `tool.rs::ToolDefinition` + `schema_for_tool` | ✅ PARITY | name/description/parameters/is_native. |
| `structured_patch` line_replace replacement text | `StructuredPatchTool.ts` (accepts `content`/`new`) | `tool.rs:577-590` accepts `content`/`new`/`replace`; missing → error | ✅ PARITY (this packet) | `test_p3_structured_patch_line_replace_accepts_schema_new_field`, `test_p3_structured_patch_line_replace_requires_replacement_text`. Schema declares `content`+`replace` aliases (`tool.rs:197-198`). |
| Shell control-operator injection rejection | `Shell.ts` sanitizes | `tool.rs:902-905` `unsafe_shell_control_reason` rejects `&&\|\|;\`backtick\`$(&<>` + newlines before spawn | ✅ PARITY (this packet) | `test_shell_rejects_control_operators_before_spawn`. |
| Terminal ANSI-escape input rejection | (TS terminal sanitizes control seqs) | `terminal.rs:131-135` `contains_terminal_escape` rejects ESC/CSI | ✅ PARITY (this packet) | test in `terminal.rs` tests module. |
| Permission gate before side effects (native tools) | `PermissionSystem.ts` | `command.rs` `preflight_native_tool_call` after `workspace_scoped_tool_args` | ✅ PARITY (this packet) | Reordered so preflight sees scoped args; leader.run + agent executor + tool.call paths all gate before RuntimeManager side effects. |
| Scoped permission grants (path/cwd scope) | `PermissionStore.ts` scope | `command.rs` `require_scoped_permission_grant` with `path_scope` from `required_permission_for_call` | ✅ PARITY (this packet) | `test_leader_run_requests_permission_when_existing_grant_scope_is_too_narrow`, `test_shell_omitted_cwd_uses_workspace_scope_for_grant`, `test_terminal_and_repl_omitted_cwd_use_workspace_scope_for_grant`. |
| Default tool scope when omitted (`code_search`/`glob`/`shell` → `.`) | (TS defaults to workspace) | `command.rs` `default_tool_scope` | ✅ PARITY (this packet) | Narrow-scope grant now correctly denies omitted-cwd shell/terminal/repl. |
| Timeouts (shell/sidecar/repl/terminal/mcp/document) | per-tool timeouts | `wait_timeout` + per-runner timeout_ms | ✅ PARITY | All child spawners now use `configure_command_for_process_tree` + `kill_child_tree`. |
| Recovery hints on missing dependency | typed errors | `document_tools.rs` `DocumentToolError::MissingDependency` | ✅ PARITY | typed missing-runtime/dependency errors. |
| Mode policy (Strict/Dev/Networked/Yolo) | `PermissionSystem.ts` | `permission.rs` `PermissionMode` | ✅ PARITY | GS-011..013. |

---

## 4. Workspace / path / security boundaries

| Item | TS source | Rust source | Status | Evidence |
|------|-----------|-------------|--------|----------|
| Workspace root validation (reject fs roots / control chars) | `SessionManagerRuntime.ts` | `command.rs` `validate_workspace_root` | ✅ PARITY | prior packet. |
| Canonical path boundary (not string prefix) | `PermissionSystem.ts` | `command.rs` `workspace_scope_allows`, `ensure_path_inside_session_workspace` | ✅ PARITY | prefix-sibling (`C:\tmp` vs `C:\tmp_safe`) denied. |
| Hierarchical canonicalization for non-existent paths | (TS canonicalizes parent) | `command.rs` `normalize_permission_path` walks parents | ✅ PARITY (this packet) | re-implemented to canonicalize nearest existing ancestor + append suffix. |
| Terminal/REPL cwd scoped to session workspace | (TS resolves cwd against workspace) | `command.rs` `session_scoped_cwd` + `session_workspace_root` | ✅ PARITY (this packet) | omitted/relative cwd resolved against workspace; outside-workspace denied. |
| MCP cwd scoped to session workspace | `mcp tools` | `command.rs` `mcp_cwd_for_session` | ✅ PARITY | prior packet. |
| Symlink escape denial | `PermissionSystem.ts` | `command.rs` + `create_test_dir_link` test helper | ✅ PARITY | symlink/junction escape denied where platform supports. |
| Secret redaction breadth (key prefixes) | `message_sanitizer.ts` | `command.rs` `redact_secret_text` (sk-/sk-ant-/AKIA/ASIA/ghp_/gho_/ghs_/glpat-/xoxb-/xoxp-/xoxa-/xoxs-/AIza/eyJ/Bearer) + `is_sensitive_persistence_key` | ✅ PARITY (this packet) | expanded prefix set + sensitive-key value redaction in `redact_persistence_secrets`. |
| Process-tree kill (no orphan descendants) | `PidRegistry.ts` | `process.rs` `kill_pid_tree` (Win taskkill /T /F; Unix setpgid + SIGTERM→SIGKILL) + `configure_command_for_process_tree` | ✅ PARITY (this packet) | `test_process_registry_cleanup_kills_descendant_process`; wired into sidecar/terminal/repl/mcp/document/llm external provider/shell. |

---

## 5. Workflow nodes parity

| Item | TS source | Rust source | Status | Evidence |
|------|-----------|-------------|--------|----------|
| DAG traversal + cycle rejection | `WorkflowEngine.ts` | `workflow.rs::plan_dag_execution` | ✅ PARITY | GS-014. |
| Tool/LLM/agent/data node executors | `WorkflowEngine.ts` node handlers | `command.rs` `execute_workflow_node` | ✅ PARITY | GS-038/039. |
| Per-node durable progress + retry | `WorkflowEngine.ts` retry | `workflow_node_state` + retry policy | ✅ PARITY | GS-043/044. |
| Recovery: running→paused on boot | `SessionManagerRuntime.ts` recovery | `DbOwner::recover_orphans` | ✅ PARITY | GS-016/028. |

---

## 6. Leader / agent runtime parity

| Item | TS source | Rust source | Status | Evidence |
|------|-----------|-------------|--------|----------|
| State machine (agent/leader status transitions) | `AgentExecutionResult.ts`/`LeaderAgent.ts` | `agent.rs` + `command.rs` | ✅ PARITY | GS-008..010. |
| AgentLoop ToolCallDelta accumulation | `AgentRoundExecutor.ts` | `agent.rs::AgentLoop` `ToolCallAccumulator` | ✅ PARITY | acceptance-gate P0; `agent.rs:384-400` test. |
| Tool-loop probe (`ToolLoopDetector`) | `src/agents/runtime/ToolLoopDetector.ts` + `BaseAgentRuntime.handleNativeToolCalls` / `LeaderThinkingLoop` | `agent.rs::ToolLoopDetector` + `AgentLoop::run` guard | ✅ PARITY (this slice) | Same-name + same-args consecutive-round loop probe. `stable_json` (recursive key sort) + multiset round signature (sorted, order-independent) match TS `stableJson`/`fingerprintToolCall`/`observe` exactly. Disabled by default (`LINGXIAO_TOOL_LOOP_DETECTOR` env, truthy 1/true/yes/on); pure-text round neither extends nor resets streak; default threshold 4 (`max(2, n)`). `AgentLoop::run` observes each round's tool calls before execution; on trip it injects a recovery `system` message into the context, resets, and `continue`s (skips the round) — mirroring TS `handleNativeToolCalls` guard branch. `with_tool_loop_detector` builder lets tests inject an explicitly-enabled detector without the env var. Tests: `test_tool_loop_detector_disabled_by_default_never_reports`, `test_tool_loop_detector_reports_repeated_identical_when_enabled`, `test_tool_loop_detector_different_args_resets_streak`, `test_tool_loop_detector_empty_round_neither_extends_nor_resets`, `test_tool_loop_detector_multiset_signature_is_order_independent`, `test_tool_loop_detector_stable_json_key_order_independent`, `test_tool_loop_detector_reset_clears_streak`, `test_tool_loop_detector_threshold_floored_at_two`, `test_agent_loop_tool_loop_guard_skips_looping_round_and_injects_system_prompt`, `test_agent_loop_tool_loop_guard_disabled_does_not_skip`. |
| Tool-*failure* circuit-breaker (`ToolFailureLoopGuard`) | `src/agents/runtime/ToolFailureLoopGuard.ts` + `BaseAgentRuntime.recordToolFailure` / `formatToolFailureLoopError` | `agent.rs::ToolFailureLoopGuard` + `classify_tool_failure` + `format_tool_failure_loop_error` + `AgentLoop::run` failure path + `ToolFailureLoopEscalation` + `AgentEvent::ToolFailureLoopEscalated` + `start_agent_pool_event_bridge` escalation arm + `apply_permission_mode_mutation_in_tx` | ✅ PARITY (this slice) | In-process failure circuit-breaker landed (prior slice) **plus Leader-bus escalation signal landed (prior follow-up slice) plus live permission-mode mutation landed (this slice)**. `key = {toolName}::{argsHash}::{errorKind}` (TS parity); `argsHash` is now a 16-char SHA-1 truncation of the stable-JSON args (TS `hashArgs` parity — switched from the prior full-stable-JSON-string fingerprint so the durable escalation payload cannot leak args-secrets; equal-args⇒equal-key semantics preserved); `errorKind` ∈ permission/mode/write_scope/sandbox/network/schema/precondition/execution/timeout/aborted/other via `classify_tool_failure` (case-insensitive keyword substring match over the free-text error, mirroring TS `ERROR_KIND_PATTERNS`/`classifyToolFailure` — Rust has no typed `ToolErrorEnvelope.code`, so code+message collapse to one text; WriteScope is checked before Permission because its Rust texts embed "permission"). `STATE_ERROR_KINDS` (permission/mode/write_scope/sandbox/network/schema) ⇒ `requires_escalation`; `NON_TRIPPING_ERROR_KINDS` (precondition) never trips (preserves the read-first hint). Default threshold 3 (`max(2, n)`), env-gated `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` (reuses R-6 `is_truthy_env`). `record` accrues per-key count, trips at threshold, freezes count after trip (no inflation); **`LoopGuardDecision.just_tripped`** is true only on the first record that flips a key to tripped (mirrors TS `emitTripped` firing once inside the first-trip `if (tripped)` block, not the already-tripped branch); `clear_on_success` wipes matching (toolName,argsHash) across all kinds; `reset_session` clears all; `snapshot`/`count_tripped` for observation. `AgentLoop::run` failure path records then, on trip, surfaces a `TOOL_FAILURE_LOOP_TRIPPED` recovery error (via `format_tool_failure_loop_error`, with `LLM_RECOVERY=` JSON payload) to the LLM *in place of* the raw failure — prompting a strategy change, not skipping the round. Success path calls `clear_on_success`. **Leader-bus escalation signal (this slice):** on a *state-class* trip (`just_tripped && requires_escalation`), `AgentLoop` emits an `AgentEvent::ToolFailureLoopEscalated { escalation: ToolFailureLoopEscalation }` over the agent→supervisor mpsc channel. The `start_agent_pool_event_bridge` (command.rs) handles it via `persist_agent_pool_escalation`, which writes (1) the `agent_logs` operator row (event_type `agent.tool_failure_loop_escalation`) and (2) a durable canonical `EventEnvelope` in `event_log` (same event_type, via `append_event_in_tx`, which runs `redact_persistence_secrets` over the payload as defense-in-depth). This is the minimal verifiable equivalent of the TS `agent:tool_failure_loop` emitter event + `tool_failure_loop_escalation` MessageBus message to `LeaderPermissionManager` — Rust `AgentLoop` runs in-process under the `AgentPool` (no worker bus), so the signal is **durable-and-observable** (replayable via `event_log`) rather than handled in place. The `ToolFailureLoopEscalation` payload carries session_id/agent_id/agent_name/task_id/tool_name/args_hash/error_kind/error_code/count/threshold/requires_escalation/last_error_message and **deliberately omits the raw tool `arguments`** (only the SHA-1 `args_hash`) so args-secrets cannot leak into the durable record. Non-state trips (e.g. timeout) surface the recovery error only (no escalation signal); disabled guard emits nothing (byte-for-byte legacy). **Auto-response decision record (this slice):** the bridge additionally persists a *deterministic* errorKind → action decision alongside the escalation signal, mirroring the TS `LeaderPermissionManager.handleToolFailureLoopEscalation` (src/agents/LeaderPermissionManager.ts:199-225) decision table: permission/network ⇒ `approved` (auto-escalate permission mode + approve retry; yolo stays yolo, else → networked); sandbox/mode/write_scope/schema ⇒ `rejected`; execution/timeout/aborted/other/precondition ⇒ `interactive`. The pure policy core is `escalation_auto_response(ToolFailureErrorKind) -> EscalationAutoResponse` (`agent.rs`), persisted by `persist_escalation_auto_response_decision_in_tx` (command.rs) in the *same transaction* as the escalation signal as (1) a canonical `agent.tool_failure_loop_escalation_decision` `EventEnvelope` in `event_log` (via `append_event_in_tx` ⇒ `redact_persistence_secrets` defense-in-depth) and (2) a `session_state` row keyed `tool_failure_loop_escalation_decision:{agent_id}:{tool_name}:{args_hash}` so an operator/Leader layer can read the latest decision for a trip and act on it. The record carries action/decision/reason/error_kind/session_id/agent_id/agent_name/tool_name/args_hash/count/from_mode/target_mode/`mutation`/`mutated_to`/`revoked_grants` — only the `args_hash` (never raw args), `last_error_message` truncated to 200 chars. **Live permission-mode *mutation* (this slice):** the bridge now *applies* the in-place mode change for an approved decision whose `target_mode` != `from_mode` via a new private in-tx helper `apply_permission_mode_mutation_in_tx` (command.rs) reusing `handle_permission_set_mode` semantics — upsert `permission_modes` + generation bump + revoke stale `permission_grants` (delete + `permission.grant_revoked` per grant) + emit `permission.mode_changed`. The decision record's `mutation` field graduated from `"deferred"` to `"applied"` (approved, target != from)/`"noop"` (approved, yolo→yolo, no mutation but decision recorded)/`"not_applicable"` (rejected/interactive, no mutation); `mutated_to`/`revoked_grants` carry the applied-mutation facts. Rejected/interactive decisions never mutate. Mutation event ids are deterministic per occurrence (prefix + `occurred_at`) so an intra-dispatch duplicate dedups via `try_fetch_event_by_id` rather than double-applying; the decision helper self-`ensure_meta`s so it succeeds even when invoked directly. Tests: prior-slice `ToolFailureLoopGuard`/classify/format/`AgentLoop` in-process tests unchanged; new R-7 follow-up tests `test_agent_loop_failure_guard_emits_escalation_signal_on_state_error`, `test_agent_loop_failure_guard_no_escalation_on_non_state_trip`, `test_agent_loop_failure_guard_disabled_emits_no_escalation`, `test_agent_loop_failure_guard_success_clears_no_escalation`, `test_escalation_payload_omits_raw_args_and_does_not_leak_secrets`; R-7 decision-record tests `test_escalation_auto_response_*` (7 agent-module policy tests) + `test_escalation_decision_*` (9 command-bridge persistence tests); new R-7 live-mutation tests (this slice) `test_escalation_live_mutation_*` (8: strict→networked mutates + mode_changed event; grant revocation + grant_revoked events + revoked_grants count; no-grants case; rejected no mutation + grants preserved; interactive no mutation; yolo approved noop; idempotency on duplicate intra-dispatch; event_log contains mode_changed + decision). |
| Manifest / memory / ledger / blackboard / mailbox | respective TS modules | `command.rs` handlers + persistence | ✅ PARITY | GS-035/036, F-019/021/022. |
| Leader.run scoped tool permission + scoped args | `LeaderPermissionManager.ts` | `command.rs` `handle_leader_run` (this packet) | ✅ PARITY (this packet) | `required_permission_for_call(tool_call.name, &tool_arguments)`; `workspace_scoped_tool_args` before grant check; `test_leader_run_requests_permission_when_existing_grant_scope_is_too_narrow`. |
| Recovery (heartbeat/stale cleanup) | `WorkerProcessRunner.ts` | `agent.rs::HeartbeatMonitor` + boot orphan sweep | ✅ PARITY | F-007/G-6/G-8. |
| Structured completion (attempt_completion/send_message) | `tool.ts`/`agent.rs` | native tools registered | ✅ PARITY | F-015. |

---

## 7. Process lifecycle / terminal / MCP / web / node_repl / document tools

| Item | TS source | Rust source | Status | Evidence |
|------|-----------|-------------|--------|----------|
| Sidecar spawn/timeout/cancel + concurrent drain | `WorkerProcessRunner.ts` | `sidecar.rs` + `process.rs` tree kill | ✅ PARITY (this packet) | `configure_command_for_process_tree` + `kill_child_tree` wired; large-stdout drain test. |
| Terminal sessions (create/send/read/kill) | terminal tools | `terminal.rs` | ✅ PARITY (this packet) | replace-on-recreate kills prior tree; ANSI escape rejection; workspace-scoped cwd. |
| MCP stdio bridge (one-shot + persistent) | mcp tools | `mcp_bridge.rs` | ✅ PARITY (this packet) | tree kill on stop/fail/timeout; concurrent drain; `test_mcp_bridge_*`. |
| Node/Python REPL (eval + persistent) | repl tools | `repl.rs` | ✅ PARITY (this packet) | tree kill on timeout; workspace-scoped cwd. |
| Document tools (OCR/parse_file) | document tools | `document_tools.rs` | ✅ PARITY (this packet) | tree kill on timeout; typed missing-dependency errors. |
| Owned-process registry + boot orphan sweep | `PidRegistry.ts` | `process.rs::ProcessRegistry` | ✅ PARITY (this packet) | `cleanup_orphans` kills descendant trees. |
| Web server / SSE bridge | `web-server` | ⛔ NON-TARGET | Rust Core is headless; web/SSE is adapter-side. Acceptance: projection snapshot/delta (`projection.rs`) is the canonical boundary. |

---

## 8. Gaps closed this packet (2026-07-07)

1. **OpenAI Chat-Completions multimodal wire** — user messages now emit real
   `image_url` content parts with rehydrated blob data URIs instead of
   flattened `[image]` text. (`openai-provider/src/lib.rs` `to_chat_message`,
   `user_message_content_parts`, `parse_image_detail`.)
2. **OpenAI Responses-API multimodal wire** — `responses_input` emits
   `input_text`/`input_image` content arrays. (`responses_input` +
   `Message::openai_responses_content_parts`.)
3. **Host-protocol image rehydration primitives** —
   `rehydrate_image_blob_ref`, `base64_encode`, `ResolvedImageContent`,
   `MessageContentPart::resolved_image_url`/`is_image`,
   `Message::has_image_content`/`openai_chat_content_parts`/
   `openai_responses_content_parts`. (`provider.rs`.)

### 8a. Gaps closed this slice (2026-07-08) — R-4 vision gating

4. **Vision capability gating (`supports_vision`)** — routing now refuses to
   send an image-bearing request to a provider whose metadata declares
   `supports_vision=false`. `ModelRoutingMetadata` gained a `supports_vision`
   field (default `true`, mirroring the tools/streaming optimistic default so
   unconfigured providers keep working); `resolve_for_request` takes a new
   `needs_vision` flag; `GenerateRequest::needs_vision` detects `image_url`/
   `image_blob_ref` parts via `Message::has_image_content`; and when no
   vision-capable candidate exists the router returns a vision-specific
   `ProviderError(UnsupportedModel)` instead of dropping the image on a text
   model. (`llm.rs` `ModelRoutingMetadata`/`resolve_for_request`/
   `LlmRouter::route_stream_with_sink`/`no_provider_error`; `provider.rs`
   `GenerateRequest::needs_vision`; `lingxiao-core-daemon/src/lib.rs`
   `supports_vision` runtime-config field.)

### 8b. Gaps closed this slice (2026-07-08) — R-1 Anthropic multimodal

5. **Anthropic provider native multimodal image input** — `add_message` no
   longer flattens user messages to `plain_text_content()`. User messages
   carrying typed `content_parts` are projected into Anthropic content blocks
   (`anthropic_user_message_content`): `text` parts → `ContentBlockParam::Text`,
   `image_url` data-URI parts → `ContentBlockParam::Image` with a
   `base64` source (`anthropic_image_block_from_url`), and `image_blob_ref`
   parts → rehydrated from disk via `rehydrate_image_blob_ref` into an image
   block. Missing/unreadable blob files degrade to the safe `[image: …]`
   text placeholder (short id only; never a `blob_path` leak; never a panic).
   Non-data-URI `image_url` (remote http URL) and `thinking`/`redacted_thinking`
   parts degrade to text markers, mirroring TS `toAnthropicContent`. Plain-text
   user messages stay `MessageContent::Text` (byte-for-byte legacy), and the
   assistant/tool/system branches are unchanged so tool-use replay and
   tool_result history are preserved. Both the SDK streaming path
   (`to_message_params`→`add_message`) and the non-streaming raw reqwest path
   (`anthropic_request_body`→`to_message_params`) share this single build
   point. (`anthropic-provider/src/lib.rs`; `tempfile` dev-dependency added.)
   **Surpasses TS**: the TS `toAnthropicContent` emits only a textual
   `[image stored as blob:…]` placeholder for `image_blob_ref` (it never
   rehydrates blobs for Anthropic), whereas Rust rehydrates real image bytes —
   a strict superset, no image silently lost.

### 8c. Gaps closed this slice (2026-07-08) — R-2 Gemini multimodal

6. **Gemini provider native multimodal image input** — `add_message` no longer
   flattens user messages to `plain_text_content()`. User messages carrying
   typed `content_parts` are projected into Gemini image-capable content parts
   (`gemini_user_content`): `text` parts → `Part::Text`, `image_url` data-URI
   parts → `Part::InlineData{inline_data:Blob{mime_type,data}}`
   (`gemini_inline_data_from_url` parses `data:<mime>;base64,<data>`), and
   `image_blob_ref` parts → rehydrated from disk via
   `rehydrate_image_blob_ref` into an `inline_data` part. Missing/unreadable
   blob files degrade to the safe `[image: …]` text placeholder (short id only;
   never a `blob_path` leak; never a panic); remote non-data-URI `image_url`
   (an http URL the inline-data path cannot express without fetching the bytes)
   degrades to a `[image] <url>` text marker. Non-image parts
   (`thinking`/`redacted_thinking`) map to text parts. Plain-text user messages
   (no `content_parts`) stay on `with_user_message(text)` (byte-for-byte
   legacy), and the assistant `function_call`, tool `function_response`, and
   `system` branches are unchanged so tool-use replay and tool-result history
   are preserved. The single `add_message` build point is shared by both the
   streaming (`execute_stream`) and non-streaming (`execute`) SDK paths.
   (`gemini-provider/src/lib.rs`; `tempfile` dev-dependency added.)
   **Surpasses TS**: the TS `VercelAIContentGenerator.convertUserContent` maps
   `image_url` to a Gemini image part but **skips `image_blob_ref` parts
   entirely** (not directly supported by the AI SDK URL-based image), whereas
   Rust rehydrates real image bytes from the blob store — a strict superset, no
   image silently lost.

### 8d. Gaps closed this slice (2026-07-08) — R-3 Bedrock multimodal

7. **Bedrock provider native multimodal image input** — `bedrock_message` no
   longer flattens user messages to `plain_text_content()`. User messages
   carrying typed `content_parts` are projected into Anthropic-on-Bedrock
   content blocks (`bedrock_user_content`): `text` parts →
   `{type:"text",text}`, `image_url` data-URI parts →
   `{type:"image",source:{type:"base64",media_type,data}}`
   (`bedrock_image_source_from_url` parses `data:<mime>;base64,<data>`), and
   `image_blob_ref` parts → rehydrated from disk via
   `rehydrate_image_blob_ref` into an `image` source block. Bedrock exposes
   Anthropic Claude models via the `anthropic_version: bedrock-2023-05-31`
   Messages API, whose image content block is the `image`/`source` shape above.
   Missing/unreadable blob files degrade to the safe `[image: …]` text
   placeholder (short id only; never a `blob_path` leak; never a panic); remote
   non-data-URI `image_url` (an http URL the base64 source path cannot express)
   degrades to a `[image] <url>` text marker. Non-image parts
   (`thinking`/`redacted_thinking`) map to text blocks. Plain-text user messages
   (no `content_parts`) stay the legacy single `{type:"text",text}` block
   (byte-for-byte), and the assistant `tool_use`, tool `tool_result`, and
   `system` branches are unchanged so tool-use replay and tool-result history
   are preserved. The single `bedrock_message` build point feeds
   `build_invoke_body`, the only request-shape emitter (the `bedrock_body`
   metadata escape hatch short-circuits before it, preserving passthrough).
   (`bedrock-provider/src/lib.rs`; `tempfile` dev-dependency added.)
   **Surpasses TS**: there is no direct TS Bedrock multimodal counterpart
   because the Rust Core Bedrock provider is the canonical path; the wire shape
   follows the Anthropic Messages-over-Bedrock image block spec.

### 8e. Gaps closed this slice (2026-07-08) — R-5 blob retain-rounds cutoff

8. **Blob rehydration retain-rounds cutoff** — `image_blob_ref` rehydration is
   now gated by a retain-rounds window that mirrors TS
   `rehydrateRecentImageBlobRefs` (`DEFAULT_RETAIN_IMAGE_ROUNDS=2`). New shared
   host-protocol helpers: `DEFAULT_RETAIN_IMAGE_ROUNDS`,
   `normalize_image_retain_rounds` (floors/clamps to min 1, finite-checks like
   TS `normalizeImageRetainRounds`), `image_retain_cutoff` (scans the message
   array from the end counting `role=="user"`; returns the cutoff index, or 0
   when fewer than N user rounds exist so nothing is silently dropped — exactly
   the TS initial-`cutoffIndex=0` semantics), `message_rehydrates_blob_at`
   (per-message `index >= cutoff` convenience), `retain_rounds_from_metadata`
   (reads `metadata["image_history_retain_rounds"]`, the Rust analog of TS
   `advanced.image_history_retain_rounds`), and `rehydrate_image_blob_ref_if`
   (returns `None` when not eligible so callers degrade to `plain_text()`).
   Each provider loop (`responses_input`/`to_chat_completion_request` for
   OpenAI; `add_message`→`to_message_params` for Anthropic; `add_message` for
   Gemini; `build_invoke_body`→`bedrock_message` for Bedrock) computes the
   per-message `rehydrate` flag and forwards it into the single-message
   projection (`Message::openai_chat_content_parts_with_rehydrate`/
   `openai_responses_content_parts_with_rehydrate`; `anthropic_user_message_content`;
   `gemini_user_content`; `bedrock_user_content`). Older-round blobs degrade to
   the safe `plain_text()` placeholder (short blob id, no `blob_path` leak, no
   panic) instead of sending full image bytes. `image_url`/data-URI parts are
   never gated — only `image_blob_ref` is — so R-1/R-2/R-3 wire shapes are
   untouched. The existing zero-arg OpenAI projections are preserved as
   `rehydrate=true` delegates so existing behavior/tests are byte-for-byte
   unchanged. (`lingxiao-llm-host-protocol/src/provider.rs`; all four provider
   crates.)
   **Round semantics note**: Rust has no per-message round id, so "round" is
   approximated by `role=="user"` message position from the end of the sequence
   — the **same** approximation TS uses (it too counts `role === 'user'`
   messages). This is an exact match over the same message array; the only
   structural divergence is that Rust applies the window at the provider
   projection point rather than a separate pre-projection pass (TS mutates the
   message array up front in `Client.generateContent*`). Observable behavior is
   identical. Bedrock computes the window over the **original** message array
   (system messages included) so filtering system messages out of the wire body
   does not shift the user-round count.

### 8f. Gaps closed this slice (2026-07-08) — R-4 models.dev capability-registry auto-derive

9. **`supports_vision` auto-derivation from an offline models.dev registry** —
   Rust Core now auto-derives `supports_vision` from the build-time-embedded
   `models.dev` snapshot when operator `runtime.json` metadata does not declare
   it, matching the TS `supportsVision` → `getInputModalities` → models.dev
   registry path. New surface in `crates/lingxiao-core/src/llm.rs`:
   - `CapabilityRegistry` trait (`vision_for(model_id) -> Option<bool>`) —
     the pluggable capability-source interface.
   - `ModelsDevRegistry` — embeds `src/llm/models-snapshot.json` via
     `include_str!` (5.2 MB / 5329 models, fetched by
     `scripts/fetch-models-snapshot.mjs` from `https://models.dev/api.json`),
     lazily parses it into a lowercased-id → `{vision}` index via a
     `OnceLock` (zero cost until the first image request), and resolves via
     exact-then-longest-prefix match. No network fetch at runtime.
   - `parse_models_dev_snapshot` — `vision = attachment.unwrap_or(false) ||
     modalities.input ∋ "image"`; merged across all providers with
     vision=true winning over vision=false (mirrors TS `buildIndex`/
     `normalize`/`setIfBetter`). Parse failure degrades to an empty index
     (`vision_for` → `None` → optimistic default), mirroring the TS
     `loadSnapshot` try/catch.
   - `ModelRoutingMetadata.vision_declared: bool` — carries the
     "operator did not declare vision" signal the `bool` field cannot
     represent; set `true` by `with_vision_support`, `false` by default.
   - `ProviderRegistry::with_capability_registry` builder +
     `provider_supports_vision(provider_id, model_id)` — the precedence
     resolver: (1) explicit operator declaration (`vision_declared`) wins;
     (2) registry auto-derive; (3) optimistic `true` on registry miss.
   - `resolve_for_request`'s vision filter now calls
     `provider_supports_vision` instead of reading `supports_vision` directly.
   The daemon attaches a default `ModelsDevRegistry` in `configure_router`
   (`crates/lingxiao-core-daemon/src/lib.rs`), so production routing gets
   auto-derivation out of the box while explicit `supports_vision` in
   `runtime.json` still overrides it. `supports_tools`/`supports_streaming`
   remain explicit-only (no models.dev signal for streaming; tools deferred to
   keep this slice focused) — registry auto-derive is wired for vision only.
   (`crates/lingxiao-core/src/llm.rs`; `crates/lingxiao-core-daemon/src/lib.rs`.)
   **Net effect:** R-4 moves from PARTIAL to PARITY — operators no longer have
   to set `supports_vision` per model to get correct vision gating; the
   embedded registry supplies it. TS OCR fallback remains NON-TARGET (no
   tesseract in Rust).

### 8g. Gaps closed this slice (2026-07-08) — R-6 agent tool-loop probe (`ToolLoopDetector`)

10. **Agent tool-loop probe** — `AgentLoop` now detects when it is stuck
    re-issuing the *exact same* tool name + arguments across consecutive
    rounds and short-circuits the loop, mirroring TS
    `src/agents/runtime/ToolLoopDetector.ts` + its `BaseAgentRuntime.
    handleNativeToolCalls` / `LeaderThinkingLoop` integration. New surface in
    `crates/lingxiao-core/src/agent.rs`:
    - `stable_json(value: &Value) -> String` — recursively sorts object keys so
      `{a:1,b:2}` ≡ `{b:2,a:1}` (TS `stableJson` parity). Rust
      `ToolCall.arguments` is already a parsed `serde_json::Value`, so no
      string→object normalize step is needed (TS carries a JSON string and
      `normalizeArgs`-parses it).
    - `fingerprint_tool_call(tc) -> "{name}::{stable_json(args)}"` (TS
      `fingerprintToolCall` parity).
    - `ToolLoopDetector` — `observe(&[ToolCall])` builds the round signature as
      the **sorted multiset** of per-call fingerprints joined by `|`
      (order-independent; a round only extends the streak when its whole
      fingerprint set matches the previous round); an empty round (pure text,
      no tool calls) neither extends nor resets the streak; `is_looping()` /
      `consecutive_count()` / `current_signature()` / `reset()` accessors.
      `ToolLoopDetectorOptions { enabled: Option<bool>, threshold: usize }`
      with `enabled=None` reading `LINGXIAO_TOOL_LOOP_DETECTOR` (truthy
      1/true/yes/on, TS `isToolLoopDetectorEnabled` parity) and
      `threshold.max(2)` (default 4, TS `DEFAULT_THRESHOLD` parity).
    - `AgentLoop::with_tool_loop_detector(detector)` builder — production uses
      the default env-gated detector; tests inject an explicitly-enabled one so
      behavior does not depend on the env var being set.
    - `AgentLoop::run` (now `run(mut self)`) observes each round's `tool_calls`
      after the empty-round early-return and before the tool-execution loop;
      when `is_looping()` it appends a recovery `system` message to the context
      (and `AgentContextStore` if present), `reset()`s the probe, and
      `continue`s to the next round — skipping execution of the repeated call
      so no `ToolCallInitiated`/`ToolCallCompleted` events fire and
      `tool_call_history` is not polluted. This matches the TS guard branch,
      which `return { done: false }` after pushing the recovery system message.
    Disabled by default, so production behavior is byte-for-byte unchanged
    unless an operator opts in via the env var (TS parity). The probe is purely
    additive to the round loop; provider wires, tool execution, and
    `attempt_completion` are untouched.
    (`crates/lingxiao-core/src/agent.rs`.)
    **Net effect:** R-6 closes an agent-runtime parity gap that the matrix had
    previously mislabeled PARITY (the whole §6 row block was green, but Rust
    shipped neither `ToolLoopDetector` nor `ToolFailureLoopGuard`). R-6 lands
    the *loop* probe; the *failure* circuit-breaker (`ToolFailureLoopGuard`,
    which needs MessageBus escalation to the Leader + `LeaderPermissionManager`
    handling + `agent:tool_failure_loop` event) is tracked as R-7.

### 8h. Gaps closed this slice (2026-07-08) — R-7 agent tool-failure circuit-breaker (in-process)

11. **Agent tool-*failure* circuit-breaker (in-process slice)** — `AgentLoop`
    now detects when a tool keeps failing the *same way* on the *same
    arguments* across rounds and surfaces a tripped recovery error to the LLM
    instead of the raw failure, mirroring TS
    `src/agents/runtime/ToolFailureLoopGuard.ts` +
    `BaseAgentRuntime.recordToolFailure` / `formatToolFailureLoopError` (in-
    process scope only — Leader-bus escalation is deferred). New surface in
    `crates/lingxiao-core/src/agent.rs`:
    - `ToolFailureErrorKind` enum (permission/mode/write_scope/sandbox/network/
      schema/precondition/execution/timeout/aborted/other) with `is_state_error`
      (TS `STATE_ERROR_KINDS`) and `is_non_tripping` (TS
      `NON_TRIPPING_ERROR_KINDS`) predicates — TS parity.
    - `classify_tool_failure(error_text) -> ToolFailureErrorKind` — case-
      insensitive keyword substring match mirroring TS `classifyToolFailure` /
      `ERROR_KIND_PATTERNS`. Rust `ToolResult.error` is a free-text string with
      no typed `code` field (unlike TS `ToolErrorEnvelope.code`), so the
      TS code+message combine collapses to a single text here; the keyword set
      covers both the TS underscore errorCode tokens (`permission_required`,
      `write_scope_forbidden`, …) and Rust's natural-language error phrases
      (`permission denied`, `outside permission grant`, `Missing required
      param`, `timed out`). WriteScope is checked before Permission because
      Rust WriteScope texts embed the word "permission" ("outside permission
      grant") — the more-specific phrase must win, exactly as the TS regex
      array orders write_scope before the generic permission fallback.
    - `failure_args_fingerprint(args) -> String` — reuses the R-6 `stable_json`
      helper so structurally-equal args produce the same key regardless of key
      ordering (TS `hashArgs(normalizeArgsForHash(args))` parity over equality
      semantics; Rust `ToolCall.arguments` is already a parsed `Value`, so no
      string-parse normalize step is needed). TS uses a 16-char SHA-1
      truncation of the same stable-JSON string for memory; Rust uses the full
      stable-JSON string as the fingerprint — same "equal args ⇒ equal hash"
      semantics, no new `sha1` dependency (keeps the slice additive to
      `Cargo.toml`).
    - `ToolFailureLoopGuard` + `ToolFailureLoopGuardOptions` — `key =
      {toolName}::{argsHash}::{errorKind}`; `record()` accrues per-key count,
      trips at `threshold` (default 3, `max(2, n)` — TS `Math.max(2, n)`),
      freezes count after trip so a looping LLM cannot inflate the counter
      (TS parity); `clear_on_success(toolName, args)` wipes every record
      sharing that (toolName, argsHash) across all error kinds; `reset_session`
      clears all; `snapshot()`/`count_tripped()` for test observation; capacity
      guard evicts the lowest-count key at `max_keys` (default 256). Disabled
      by default via `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` (truthy 1/true/yes/on,
      reusing the R-6 `is_truthy_env` — TS `isToolFailureLoopGuardEnabled`
      parity); an explicit `enabled` option always wins (TS constructor parity).
    - `LoopGuardDecision` / `ToolFailureSignature` — return shapes mirroring
      TS `LoopGuardDecision` / `ToolFailureSignature` (`tripped`, `count`,
      `error_kind`, `signature`, `requires_escalation`).
    - `format_tool_failure_loop_error(toolName, decision) -> String` — mirrors
      TS `formatToolFailureLoopError`: a `TOOL_FAILURE_LOOP_TRIPPED` banner
      with count/kind, a state-class escalation hint or non-state retry hint,
      and a trailing `LLM_RECOVERY={...}` JSON payload carrying the structured
      failure-loop block (`code`/`message`/`retryable: false`/`fix`/
      `failure_loop{toolName,argsHash,errorKind,errorCode,count,
      requiresEscalation}`).
    - `AgentLoop::with_tool_failure_loop_guard(guard)` builder — production
      uses the default env-gated guard; tests inject an explicitly-enabled one.
    - `AgentLoop::run` failure path: after `execute_tool` returns
      `success=false`, the guard records the failure (`tool_name`, `arguments`,
      `""`, error_text); on `decision.tripped` the surfaced `result_value`
      becomes `{"error": format_tool_failure_loop_error(...)}` — the LLM reads
      the tripped recovery error *in place of* the raw failure, prompting a
      strategy change rather than another identical retry. The round is **not**
      skipped (unlike the R-6 loop detector, which `continue`s): the tool
      result is still appended to the context so the assistant↔tool message
      pairing stays well-formed. The success path calls `clear_on_success` so a
      later different-kind failure is not merged into the old streak.
    Scope note: the TS guard is a process-global singleton keyed by sessionId;
    the Rust `AgentLoop` is a per-agent instance bound to a single session, so
    the guard lives as an `AgentLoop` field and tracks one session's keys —
    the cross-session isolation the TS singleton achieves via its sessionId
    map is structurally guaranteed here by ownership. `trippedRetentionMs`
    (TS keeps a 60s memory to suppress repeat trips) is intentionally not
    time-based here: a tripped key stays tripped until `clear_on_success` or
    `reset_session` — sufficient for the in-process loop, which has no bus to
    suppress chatter against.
    (`crates/lingxiao-core/src/agent.rs`.)
    **Net effect:** R-7's in-process scope is at PARITY. The deferred remainder
    is Leader-bus escalation — emitting `agent:tool_failure_loop` and routing
    `tool_failure_loop_escalation` over the MessageBus to
    `LeaderPermissionManager`. Rust `AgentLoop` runs in-process under the
    `AgentPool` (not a worker bus), so the in-process trip + LLM-facing error
    surfacing is the maximal verifiable slice without a cross-process bus; the
    escalation sub-slice is tracked as the R-7 follow-up.

### 8i. Gaps closed this slice (2026-07-08) — R-7 follow-up Leader-bus escalation signal

12. **Leader-bus escalation signal (durable/canonical)** — when the
    `ToolFailureLoopGuard` trips on a *state-class* error
    (`just_tripped && requires_escalation`), `AgentLoop` now emits a
    `ToolFailureLoopEscalated` event over the agent→supervisor mpsc channel,
    which the `start_agent_pool_event_bridge` (command.rs) persists as a
    durable/canonical escalation signal: an `agent_logs` operator row (event_type
    `agent.tool_failure_loop_escalation`) **and** a canonical `EventEnvelope` in
    `event_log` (same event_type, via `append_event_in_tx`). This is the minimal
    verifiable equivalent of the TS `agent:tool_failure_loop` emitter event +
    `tool_failure_loop_escalation` MessageBus message to
    `LeaderPermissionManager` — Rust has no worker bus, so the signal is
    durable-and-observable (replayable via `event_log`) rather than handled in
    place. New surface in `crates/lingxiao-core/src/agent.rs` +
    `crates/lingxiao-core/src/command.rs`:
    - `ToolFailureLoopEscalation` (`#[derive(serde::Serialize)]`) — the canonical
      payload: session_id/agent_id/agent_name/task_id/tool_name/args_hash/
      error_kind/error_code/count/threshold/requires_escalation/last_error_message.
      **Deliberately omits the raw tool `arguments`** (only the `args_hash`) so
      args-secrets cannot leak into the durable record.
      `ToolFailureLoopEscalation::from_tripped(decision, session_id, agent_id,
      agent_name, task_id, tool_name, threshold, last_error_message)` builds it
      from a tripped decision; `warrants_signal()` exposes the
      state-class gate.
    - `LoopGuardDecision.just_tripped: bool` — true only on the *first* record
      that flips a key from not-tripped to tripped (mirrors TS `emitTripped`
      firing once inside the first-trip `if (tripped)` block, not the
      already-tripped branch). Lets `AgentLoop` emit the escalation signal
      exactly once per trip, so a looping LLM cannot spam the durable
      `event_log` with duplicate escalation rows.
    - `AgentEvent::ToolFailureLoopEscalated { agent_id, escalation }` — the new
      agent→supervisor channel variant (non-terminal; the supervisor's `_ => {}`
      forward-all path already relays it to the external bridge).
    - `AgentLoop::run` failure path: on `decision.tripped`, it strips the
      `LLM_RECOVERY` trailer from the summary, and when
      `decision.just_tripped && decision.requires_escalation` it constructs the
      escalation and emits `AgentEvent::ToolFailureLoopEscalated` *before*
      surfacing the `TOOL_FAILURE_LOOP_TRIPPED` recovery error to the LLM. The
      round is still not skipped (R-7 in-process behavior unchanged) — the
      assistant↔tool message pairing stays well-formed; only an additional
      durable signal is emitted.
    - `failure_args_fingerprint` switched from the full stable-JSON string to a
      **16-char SHA-1 truncation** of it (TS `hashArgs` parity). SHA-1 is used as
      a fingerprint only (not for security), but it is one-way: the durable
      escalation payload carries the hash rather than the raw args, so an
      args-secret (e.g. a token in a shell command) cannot leak into the durable
      record. Equal-args⇒equal-key semantics are preserved (so prior R-7
      in-process tests still pass). New `sha1 = "0.10"` dependency in
      `lingxiao-core/Cargo.toml` (TS parity; required because the fingerprint is
      now persisted, where the prior full-string fingerprint was in-memory only).
    - `ToolFailureErrorKind` gained `#[derive(serde::Serialize)]` +
      `#[serde(rename_all = "snake_case")]` so the escalation payload's
      `error_kind` serializes to TS-matching lowercase tokens (`permission`,
      `write_scope`, …), consistent with the existing `as_str()`.
    - `start_agent_pool_event_bridge` (command.rs) new
      `AgentEvent::ToolFailureLoopEscalated` arm → `persist_agent_pool_escalation`
      (writes `agent_logs` + `event_log` under the active agent_state row's
      session, with an unattached fallback for unit-test harnesses that drive
      `AgentLoop` directly without a DB-backed spawn). `append_event_in_tx` runs
      `redact_persistence_secrets` over the payload as defense-in-depth, and the
      `event_id` is idempotent (one row per trip even if the bridge sees the
      event twice).
    (`crates/lingxiao-core/src/agent.rs`; `crates/lingxiao-core/src/command.rs`;
    `crates/lingxiao-core/Cargo.toml`.)
    **Net effect:** R-7 moves from "in-process PARITY, escalation deferred" to
    "in-process PARITY + durable/canonical escalation signal LANDED". The
    deferred remainder narrows to **full `LeaderPermissionManager` auto-response
    parity** — the TS handler *responds* to the escalation by
    auto-approving/rejecting/interactive-approval based on errorKind (e.g.
    permission/network ⇒ bump permission mode; mode/write_scope/schema ⇒ reject;
    sandbox ⇒ reject; others ⇒ interactive). Rust has no Leader MessageBus, so
    the signal is durable-and-observable but not auto-handled in place; an
    operator/Leader layer observing `event_log` can act on it. That auto-response
    parity is the remaining deferred sub-item.

### 8m. Gaps closed this slice (2026-07-08) — R-8 long-running periodic process-orphan reconcile sweep

15. **Long-running periodic process-orphan *reconcile* sweep** — the Rust
    daemon had only the boot-time `ProcessRegistry::cleanup_orphans`
    (`process.rs::cleanup_orphans`), which `kill_pid_tree`s **every** active
    `owned_processes` row. That is correct only at boot, where every active row
    is stale because the owning daemon is gone — but it is **unsafe to run
    periodically**, because it would kill the daemon's own *live* managed
    processes (sidecars, terminals, REPLs, MCP servers). TS `PidRegistry.listAll`
    (src/core/PidRegistry.ts:119-142) instead performs **lazy reconciliation** —
    `isSamePidEntry` → `processExists` (`kill(pid,0)`) silently drops entries
    whose PID has exited *without ever killing a live process* (TS killing is
    boot/shutdown/session-end only). Rust had no recurring or lazy reconcile, so
    a row left `active` when a managed process died mid-session (orphaned by a
    crash the boot sweep did not see, or simply a reaped child whose `complete`
    was missed) stayed active forever — a slow registry leak. This slice lands
    the safe recurring counterpart. New surface in
    `crates/lingxiao-core/src/process.rs` +
    `crates/lingxiao-core-daemon/src/lib.rs`:
    - `pid_is_alive(pid: u32) -> bool` — a **non-killing** liveness probe, the
      read-only analog of `kill_pid_tree`. Unix: `kill(pid, 0)` (existence
      check only; `ESRCH` ⇒ dead, `EPERM` ⇒ a live process we may not signal ⇒
      conservatively alive — the safe answer for a reconcile sweep that must
      never kill). Windows: `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)` +
      `GetExitCodeProcess`, treating `STILL_ACTIVE` (259) as alive and any
      `OpenProcess`/read failure as dead (so a race where the process exited
      mid-probe reconciles the row rather than leaving it active forever).
      `PROCESS_QUERY_LIMITED_INFORMATION` grants no terminate rights, so even a
      bug here cannot kill the process — and unlike the existing Windows
      `kill_pid_tree` it spawns no `taskkill` subprocess per checked PID.
    - `ProcessRegistry::reconcile_orphans(&self) -> Result<CleanupReport>` —
      the safe periodic counterpart to `cleanup_orphans`. It iterates `active`
      rows, and for each: if `pid_is_alive` ⇒ leave it `active` (it is a live
      managed process; the owning manager can still `complete`/`mark_failed` it
      through the normal path); if dead ⇒ flip to `reconciled_dead` with a
      `cleanup_attempted_at` timestamp + short `last_error` note. **Never calls
      `kill_pid_tree`** — the live-PID contract is the whole point. Reuses the
      existing `CleanupReport` (`attempted`/`cleaned`/`failed`) where `cleaned`
      = rows marked dead (a live row that stays `active` is `attempted` but not
      `cleaned`). Idempotent: a second pass over the same rows is a no-op once
      dead rows are terminal.
    - `ProcessReconcileTicker` (daemon) — a named background thread
      (`lingxiao-process-reconcile`) mirroring the existing `ScheduleTicker`
      pattern: `Arc<AtomicBool>` stop flag + `Option<JoinHandle>` + `shutdown`/
      `Drop` join. It owns a fresh `ProcessRegistry` over the shared `DbOwner`
      and calls `reconcile_orphans` each tick, logging `inspected`/`marked_dead`
      counts to stderr when it reaps anything; a per-tick error is logged and
      swallowed so a transient DB lock never tears down the sweep. Spawned in
      `serve_with_runtime_config` only when `background_process_reconcile_ms`
      is `Some(n > 0)`; `None`/`0` (the default) keeps the daemon boot-only,
      preserving prior behavior byte-for-byte.
    (`crates/lingxiao-core/src/process.rs`;
    `crates/lingxiao-core-daemon/src/lib.rs`; `crates/lingxiao-core/Cargo.toml`
    — new `windows-sys = { version = "0.52", features = ["Win32_Foundation",
    "Win32_System_Threading"] }` Windows-only direct dep, already resolved in
    the workspace lockfile so no new download.)
    **Net effect:** R-8 closes a P2 reliability gap (acceptance-gate P2/P3
    "process polling is bounded and justified"; QA-record "Remaining Risks"
    "long-running periodic process-orphan sweep not separately scheduled"). The
    sweep is safe-by-construction: it can never kill a live managed process,
    unlike a naive periodic reuse of `cleanup_orphans`. Boot-time killing is
    unchanged (still `cleanup_orphans` at boot, where killing every active row
    is correct). The retry/fallback total-deadline candidate was verified **not
    a parity gap** — TS `LlmGuard.call()` (LlmGuard.ts:247 loop) has no
    total/global wall-clock deadline either; both sides bound per-attempt only.

## 9. Remaining gaps (prioritized)

| # | Gap | Priority | Acceptance condition |
|---|-----|----------|----------------------|
| R-1 | Anthropic provider multimodal (image blocks) | P1 | `add_message` → `anthropic_user_message_content` maps `image_url`/rehydrated blob → `ContentBlockParam::Image`; missing blob degrades to safe text; pure text & tool_use preserved. 6 contract tests landed. **Landed (this slice).** |
| R-2 | Gemini provider multimodal (`inline_data`) | P1 | `add_message` → `gemini_user_content` maps `image_url`/rehydrated blob → `Part::InlineData`; missing blob degrades to safe text; remote URL degrades to marker; pure text & function_call/function_response preserved. 7 contract tests landed (incl. mock-server wire assertion). **Landed (this slice).** |
| R-3 | Bedrock provider multimodal (`image` source bytes) | P2 | `bedrock_message` maps image → `{type:"image",source:{type:"base64",media_type,data}}`; missing blob → safe text placeholder; remote URL → text marker; pure text & tool_use/tool_result preserved. 7 contract tests landed (incl. `build_invoke_body` wire assertion). **Landed (this slice).** |
| R-4 | Vision capability gating (`supports_vision`) | P1 | `ModelRoutingMetadata.supports_vision` + `vision_declared`; `resolve_for_request(needs_vision)` filters non-vision providers for image requests; `GenerateRequest::needs_vision` detects image parts; vision-specific `ProviderError` when no vision provider; `ModelsDevRegistry` auto-derives `supports_vision` from the embedded offline `models.dev` snapshot when operator metadata does not declare it (precedence: explicit > registry > optimistic `true`). Tests landed. **Landed (routing gate + registry auto-derive).** TS OCR fallback intentionally NON-TARGET for Rust Core (no tesseract). |
| R-5 | Blob rehydration retain-rounds cutoff | P2 | Rehydrate only recent N user rounds; degrade older blobs to placeholder (mirrors TS `rehydrateRecentImageBlobRefs`, DEFAULT_RETAIN_IMAGE_ROUNDS=2). **Landed (this slice).** Shared host-protocol helpers (`image_retain_cutoff`, `message_rehydrates_blob_at`, `rehydrate_image_blob_ref_if`, `retain_rounds_from_metadata`, `normalize_image_retain_rounds`, `DEFAULT_RETAIN_IMAGE_ROUNDS`) wired into all four providers; config read from `metadata["image_history_retain_rounds"]` (TS `advanced.image_history_retain_rounds` analog). Round semantics match TS exactly (count `user` messages from the end; cutoff 0 when fewer than N user rounds). |
| R-6 | Agent tool-loop probe (`ToolLoopDetector`) | P1 | Rust `AgentLoop` had no same-name+same-args consecutive-round loop detection, so a stuck agent could burn rounds/tokens re-issuing an identical call whose observation would not change. `agent.rs::ToolLoopDetector` now mirrors TS `ToolLoopDetector` (fingerprint `{name}::{stable_json(args)}`, sorted-multiset round signature, env-gated `LINGXIAO_TOOL_LOOP_DETECTOR`, default threshold 4 `max(2,n)`, empty round neutral, reset on trip); `AgentLoop::run` observes each round, injects a recovery `system` message + skips the round on trip. **Landed (this slice).** TS `ToolFailureLoopGuard` (the *failure* circuit-breaker with bus escalation to Leader) is a separate, larger surface and remains the next scoped gap — see R-7. |
| R-7 | Agent tool-*failure* circuit-breaker (`ToolFailureLoopGuard`) | P1 | TS `src/agents/runtime/ToolFailureLoopGuard.ts` counts consecutive *failures* keyed by `{toolName}::{argsHash}::{errorKind}`, trips at threshold (default 3, env-gated `LINGXIAO_TOOL_FAILURE_LOOP_GUARD`), and on trip emits `agent:tool_failure_loop` + escalates to the Leader via MessageBus `tool_failure_loop_escalation` (`LeaderPermissionManager` handles it). State-class errors (permission/mode/write_scope/sandbox/network/schema) force escalation. **In-process slice landed (prior slice):** `agent.rs::ToolFailureLoopGuard` mirrors the key/threshold/errorKind-classify semantics (`classify_tool_failure` over free-text errors, `STATE_ERROR_KINDS`/`NON_TRIPPING_ERROR_KINDS`, default threshold 3 `max(2,n)`, env-gated), and is wired into `AgentLoop::run`'s failure path — on trip it surfaces a `TOOL_FAILURE_LOOP_TRIPPED` recovery error (with `LLM_RECOVERY=` payload) to the LLM instead of the raw failure; `clear_on_success`/`reset_session` landed. **Leader-bus escalation signal landed (this follow-up slice):** on a state-class first-trip (`just_tripped && requires_escalation`), `AgentLoop` emits `AgentEvent::ToolFailureLoopEscalated`; `start_agent_pool_event_bridge` persists it as a durable canonical `EventEnvelope` in `event_log` (event_type `agent.tool_failure_loop_escalation`) + an `agent_logs` row — the minimal verifiable equivalent of the TS `agent:tool_failure_loop` event + `tool_failure_loop_escalation` MessageBus message (Rust has no worker bus, so the signal is durable-and-observable, not handled in place). `argsHash` switched to a 16-char SHA-1 truncation (TS `hashArgs` parity) so the durable payload cannot leak args-secrets; the `ToolFailureLoopEscalation` payload omits raw args. `LoopGuardDecision.just_tripped` ensures the signal fires once per trip. 5 new focused tests landed (state-error⇒signal; non-state trip⇒no signal; disabled⇒no signal; success-clear⇒no signal; payload omits args secrets). **Auto-response decision record landed (this slice):** the bridge now persists a *deterministic* errorKind → action decision alongside the escalation signal, mirroring `LeaderPermissionManager.handleToolFailureLoopEscalation` (LeaderPermissionManager.ts:199-225): permission/network ⇒ `approved` (mode escalates: yolo stays yolo, else → networked); sandbox/mode/write_scope/schema ⇒ `rejected`; execution/timeout/aborted/other/precondition ⇒ `interactive`. Pure policy core `escalation_auto_response` (`agent.rs`) is persisted by `persist_escalation_auto_response_decision_in_tx` (command.rs) in the same transaction as the escalation signal — a canonical `agent.tool_failure_loop_escalation_decision` `EventEnvelope` in `event_log` (redact_persistence_secrets defense-in-depth) + a `session_state` row keyed `tool_failure_loop_escalation_decision:{agent_id}:{tool_name}:{args_hash}`. The record carries action/decision/reason/error_kind/session_id/agent_id/agent_name/tool_name/args_hash/count/from_mode/target_mode/`mutation`/`mutated_to`/`revoked_grants` — args_hash only (no raw args), last_error_message truncated to 200 chars. 16 new focused tests landed (7 agent-module policy tests `test_escalation_auto_response_*` + 9 command-bridge persistence tests `test_escalation_decision_*` covering: permission⇒approved+networked, yolo-stays-yolo, network⇒approved, write_scope/schema/sandbox/mode⇒rejected, timeout/other/execution⇒interactive, exhaustive coverage of every error kind, decision event durable in event_log, no raw args / secrets redacted, idempotency on duplicate trip, unattached-fallback path). **Live permission-mode *mutation* landed (next slice, §8l):** the bridge now *applies* the in-place mode change for an approved decision whose `target_mode` != `from_mode` via a new private in-tx helper `apply_permission_mode_mutation_in_tx` (command.rs) reusing `handle_permission_set_mode` semantics — upsert `permission_modes` + generation bump + revoke stale grants (delete + `permission.grant_revoked` per grant) + emit `permission.mode_changed`. The decision record's `mutation` field graduated from `"deferred"` to `"applied"`/`"noop"` (yolo→yolo, no mutation but decision recorded)/`"not_applicable"` (rejected/interactive, no mutation); `mutated_to`/`revoked_grants` carry the applied-mutation facts. Rejected/interactive never mutate. Mutation event ids are deterministic per occurrence (prefix + `occurred_at`) so an intra-dispatch duplicate dedups via `try_fetch_event_by_id` rather than double-applying. The decision helper self-`ensure_meta`s so it succeeds even when invoked directly. 8 new focused tests landed (`test_escalation_live_mutation_*` covering: strict→networked mutates + mode_changed event; grant revocation (delete + grant_revoked events) + revoked_grants count; no-grants case; rejected (sandbox/mode/write_scope/schema) no mutation + grants preserved; interactive (execution/timeout/aborted/other) no mutation; yolo approved noop; idempotency on duplicate intra-dispatch (one mode_changed, one generation bump); event_log contains both mode_changed and decision). **R-7 fully LANDED** — in-process guard + durable escalation signal + auto-response decision record + live mode mutation; no deferred remainder. |
| R-8 | Long-running periodic process-orphan *reconcile* sweep | P2 | TS `PidRegistry.listAll` performs lazy reconciliation (`isSamePidEntry` → `processExists`): it silently drops `owned_processes` entries whose PID has exited *without ever killing a live process* — killing is boot/shutdown/session-end only. Rust had only the boot-time `cleanup_orphans` (which `kill_pid_tree`s **every** active row — correct only at boot, where every row is stale from a dead daemon; unsafe to run periodically because it would kill the daemon's own live sidecars/terminals/REPLs/MCP servers), and no recurring or lazy reconcile, so a `owned_processes` row left `active` when a managed process died mid-session stayed active forever. `process.rs::reconcile_orphans` now probes each active row's PID for liveness via the new non-killing `pid_is_alive` (`kill(pid,0)` on Unix — `EPERM` treated as alive; `OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION)` + `GetExitCodeProcess`/`STILL_ACTIVE` on Windows — no terminate rights, no `taskkill` subprocess per PID) and flips dead-PID rows to `reconciled_dead` with a `cleanup_attempted_at` timestamp + short `last_error` note; live PIDs are left untouched. The daemon gains a `ProcessReconcileTicker` (mirrors `ScheduleTicker`: named background thread, `AtomicBool` stop, `JoinHandle`/`Drop` shutdown) spawned when `background_process_reconcile_ms` runtime config is `Some(n>0)`; it owns a fresh `ProcessRegistry` over the shared `DbOwner` and calls `reconcile_orphans` each tick, logging `inspected`/`marked_dead` counts and swallowing per-tick errors so a transient DB lock never tears down the sweep. Default `None`/`0` preserves the prior boot-only behavior. 5 new focused tests landed (4 `lingxiao-core` `process::` unit tests + 1 daemon ticker integration test). **Landed (this slice).** |
| R-9 | Bedrock streaming parity (`InvokeModelWithResponseStream`) | P1 | The Rust Bedrock provider explicitly returned `UnsupportedModel` for `stream=true`, while TS routed Bedrock through the `@ai-sdk/amazon-bedrock` Vercel SDK (which streams natively) — so a `supports_streaming: true` Bedrock provider in `runtime.json` silently failed every streaming request. `execute_invoke_model` now branches on `stream`: the non-streaming `invoke_model` path is byte-for-byte unchanged, and `stream=true` calls `client.invoke_model_with_response_stream()` and drains the `EventReceiver` via `recv()`. Each AWS event-stream `chunk` frame's `PayloadPart.bytes` blob (already base64-decoded by the SDK) is the raw JSON of one complete Anthropic Messages SSE event (`{"type":"content_block_delta",...}`) — no SSE text framing to split, simpler than the Anthropic raw-SSE path — parsed by the new pure `bedrock_stream_chunk_to_events` (mirrors the Anthropic provider's `raw_anthropic_sse_value_to_events`: text/thinking/tool-use deltas with per-tool-block accumulation across `content_block_start`/`_delta`/`_stop`, `message_delta` usage+finish, `message_stop`, `error`, ignored `message_start`/`ping`/`signature_delta`). Duplicate-`Finished` suppression + defensive terminal `Finished` mirror the Anthropic path. Errors map through the existing `provider_error_from_bedrock` (AKIA/ASIA redaction); mid-stream errors after events surface as `StreamEvent::Error` rather than failing the whole call. Non-streaming behavior, the `bedrock_body` escape hatch, and R-3 multimodal `build_invoke_body`/`bedrock_message` are shared and unchanged. **Landed (this slice).** No real AWS calls in tests: 14 new tests cover the pure chunk parser (text/thinking/signature/tool-use lifecycle/usage/finish/error/message_start+ping/full sequence/non-tool-block-start), the streaming routing guard (`stream=true` no longer returns `UnsupportedModel`; unsupported-auth rejected pre-SDK), and a genuine `aws-smithy-eventstream` binary framing round-trip (`Message`+`write_message_to`→`read_message_from`+`parse_response_headers`+payload `{"bytes":<base64>}` decode → `bedrock_stream_chunk_to_events`) proving the parser against real Bedrock wire framing. |

## 10. Verification gate

### 10a. This slice — R-1 (2026-07-08)

- `cargo fmt --check`: PASS (exit 0, workspace)
- `cargo clippy -p lingxiao-llm-anthropic-provider -p lingxiao-llm-host-protocol --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo test -p lingxiao-llm-anthropic-provider --quiet`: PASS — 17 (was 11; +6 multimodal tests: data-URI image_url → image block; blob_ref rehydrate → image block; missing blob → safe text placeholder (no path leak); pure text stays `Text`; assistant text+tool_use preserved; serialized params carry image block)
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 31 (untouched crate, re-run for safety; 0 fail)
- R-1 is additive to the Anthropic user-message build path only (`add_message` `_`/user branch + two new pure helpers); assistant/tool/system branches, streaming SSE decode, and thinking-block output decode are unchanged. Mock-server test (`test_execute_messages_against_mock_http_server`) still green → non-streaming reqwest path (`anthropic_request_body`→`to_message_params`→`add_message`) reachability preserved.

### 10b. Prior slice — R-4 (2026-07-08)

- `cargo fmt --check`: PASS (exit 0, workspace)
- `cargo clippy -p lingxiao-llm-host-protocol -p lingxiao-core -p lingxiao-core-daemon --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 31 (was 30; +1 `test_generate_request_needs_vision_detects_image_parts`)
- `cargo test -p lingxiao-core --quiet`: PASS — 307 (was 303; +4 vision-gating tests); `vision` filter → 4/4 pass
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures
- Prior-packet gate (`cargo clippy --workspace`, `npm run test:scripts`, architecture invariants) not re-run this slice; R-4 is additive routing metadata + a new `GenerateRequest` method, with no changes to provider wires or TS, so the prior gate still holds.

### 10c. This slice — R-2 (2026-07-08)

- `cargo fmt -p lingxiao-llm-gemini-provider -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-llm-gemini-provider -p lingxiao-llm-host-protocol --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo test -p lingxiao-llm-gemini-provider --quiet`: PASS — 16 (was 9; +7 multimodal tests: data-URI image_url → `inline_data`; blob_ref rehydrate → `inline_data`; missing blob → safe text placeholder (no path leak, no panic); remote http image_url → text marker; pure text stays legacy path; assistant `function_call` + tool `function_response` preserved; `inlineData` emitted on the mock-server wire with text part alongside)
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 31 (untouched crate, re-run for safety; 0 fail)
- R-2 is additive to the Gemini user-message build path only (`add_message` user branch + new `gemini_user_content`/`gemini_inline_data_from_url`/`text_part` helpers); the assistant `function_call`, tool `function_response`, and `system` branches are unchanged. Existing mock-server tests (`test_execute_generate_content_against_mock_http_server`, `test_generate_content_maps_function_call_to_tool_call`, `test_gemini_tools_declared_as_function_declarations`, `test_gemini_stream_true_uses_streaming_endpoint`, `test_gemini_tool_history_maps_assistant_tool_calls`) still green → both streaming and non-streaming SDK paths reach `add_message` unchanged.
- Note: a one-off `cargo` invocation hit an `error launching git:` sandbox failure (intermittent Windows/git-in-sandbox launch); re-running outside the sandbox succeeded. No Windows/target-dir test failure observed — all tests green per-package as listed above.

### 10d. This slice — R-3 (2026-07-08)

- `cargo fmt -p lingxiao-llm-bedrock-provider -- --check`: PASS (exit 0); workspace `cargo fmt --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-llm-bedrock-provider -p lingxiao-llm-host-protocol --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo test -p lingxiao-llm-bedrock-provider --quiet`: PASS — 18 (was 11; +7 multimodal tests: data-URI image_url → image source block; blob_ref rehydrate → image source block; missing blob → safe text placeholder (short id only, no path leak, no panic); remote http image_url → text marker; pure text stays legacy single text block; assistant `tool_use` + tool `tool_result` preserved; `build_invoke_body` emits image block on the wire with text/system/anthropic_version envelope)
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 31 (untouched crate, re-run for safety; 0 fail)
- R-3 is additive to the Bedrock user-message build path only (`bedrock_message` user branch extracted into `bedrock_user_content` + new `bedrock_image_source_from_url` helper); the assistant `tool_use`, tool `tool_result`, and `system` branches are unchanged. Existing tests (`test_build_invoke_body_defaults_to_anthropic_bedrock_shape`, `test_build_invoke_body_accepts_explicit_bedrock_body_metadata`, `test_build_invoke_body_includes_tool_schemas`, `test_response_to_stream_events_*`, `test_bedrock_message_assistant_with_tool_calls_maps_to_tool_use_content`, `test_bedrock_message_tool_role_maps_to_user_tool_result_content`, `test_resolved_config_uses_auth_context_not_env_defaults`, `test_unsupported_auth_rejected_before_sdk_config`, `test_stream_true_returns_explicit_unsupported_error_without_invoking_model`) still green → `build_invoke_body` reachability and the `bedrock_body` metadata escape hatch (short-circuits before `bedrock_message`) preserved.

### 10e. This slice — R-5 (2026-07-08)

- `cargo fmt --check`: PASS (exit 0, workspace)
- `cargo clippy -p lingxiao-llm-host-protocol -p lingxiao-llm-openai-provider -p lingxiao-llm-anthropic-provider -p lingxiao-llm-gemini-provider -p lingxiao-llm-bedrock-provider --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive API surface only (new free functions + `Message` `_with_rehydrate` overloads; existing zero-arg methods preserved as `rehydrate=true` delegates), so no downstream breakage.
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 39 (was 31; +8 R-5 helper/projection tests)
- `cargo test -p lingxiao-llm-openai-provider --quiet`: PASS — 21 (was 19; +2 R-5 wire tests: Responses-API retain window over a 2-round sequence; Chat-Completions old-blob degrades to text)
- `cargo test -p lingxiao-llm-anthropic-provider --quiet`: PASS — 18 (was 17; +1 R-5 wire test asserting old-round blob → text block, recent-round blob → image block via `to_message_params` JSON)
- `cargo test -p lingxiao-llm-gemini-provider --quiet`: PASS — 17 (was 16; +1 R-5 mock-server wire test asserting old-round blob → text part / no `inlineData`, recent-round blob → `inlineData`)
- `cargo test -p lingxiao-llm-bedrock-provider --quiet`: PASS — 19 (was 18; +1 R-5 wire test over a system+2-user-round sequence with retain=1, asserting window is computed over the original array so the system message does not shift the user-round count)
- Note: a combined `cargo test -p … -p … -p …` run hit the intermittent Windows rustc `STATUS_STACK_BUFFER_OVERRUN` / "aws_smithy_runtime required to be available in rlib format" launch failure (same class as the R-2 note). Re-running each crate individually is green as listed above; no Windows/target-dir test failure observed.
- R-5 is additive: shared cutoff helpers in host-protocol + a per-message `rehydrate: bool` threaded into each provider's single-message projection (`openai_chat_content_parts_with_rehydrate`/`openai_responses_content_parts_with_rehydrate` on `Message`; `anthropic_user_message_content`/`gemini_user_content`/`bedrock_user_content` provider functions). The existing zero-arg OpenAI projections and existing provider tests are unchanged in behavior (they delegate to `rehydrate=true`). `image_url`/data-URI parts are never gated (only `image_blob_ref` is), so vision wire shapes from R-1/R-2/R-3/R-4 are untouched. R-4 routing (`supports_vision`) is unchanged.

### 10f. This slice — R-4 models.dev capability-registry auto-derive (2026-07-08)

- `cargo fmt -p lingxiao-core -p lingxiao-core-daemon -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core -p lingxiao-core-daemon --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive API surface (new trait + struct + `vision_declared` field + `with_capability_registry` builder + `provider_supports_vision` helper; `resolve_for_request` vision filter delegates to it). No downstream breakage.
- `cargo test -p lingxiao-core --quiet`: PASS — 315 (was 307; +8: 6 `capability`-registry precedence tests + 2 `models_dev` embedded-snapshot tests). `capability` filter → 6/6 pass; `models_dev` filter → 2/2 pass; prior R-4 `vision` tests → unchanged, still pass.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (daemon attaches `ModelsDevRegistry` by default; existing `test_runtime_config_model_metadata_controls_routing` with `supports_vision: None` still green — auto-derive is additive, no metadata → registry/optimistic path).
- Note: the first `cargo test -p lingxiao-core` invocation hit the intermittent Windows rustc `STATUS_STACK_BUFFER_OVERRUN` / memory-allocation launch failure (same class documented in the R-2/R-5 notes). Re-running with `CARGO_BUILD_JOBS=1` (outside the sandbox) is green as listed above; no Windows/target-dir test failure observed.
- R-4 auto-derive is additive: a new `CapabilityRegistry` trait + `ModelsDevRegistry` impl + `ModelRoutingMetadata.vision_declared` field (default `false`, set `true` by `with_vision_support`). The `bool supports_vision` field, its default, and all prior R-4 tests are unchanged in behavior. `ProviderRegistry` with no capability registry attached (unit tests, embedded callers) keeps the legacy optimistic default — auto-derivation is strictly opt-in via `with_capability_registry`. No provider wire shapes (R-1/R-2/R-3/R-5) are touched. The embedded `models.dev` snapshot is read-only and lazily parsed; a corrupt snapshot degrades to "registry unavailable" (`vision_for` → `None` → optimistic `true`), never a panic.

### 10g. This slice — R-6 agent tool-loop probe (2026-07-08)

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new `ToolLoopDetector` struct + `stable_json`/`fingerprint_tool_call` free fns + `AgentLoop.tool_loop_detector` field + `with_tool_loop_detector` builder; `AgentLoop::run` signature changed `run(self)` → `run(mut self)` (internal mutability for the probe only). No downstream breakage (daemon `AgentLoop` callers unchanged).
- `cargo test -p lingxiao-core --quiet`: PASS — 325 (was 315; +10: 8 `ToolLoopDetector` unit tests + 2 `AgentLoop` integration tests). `tool_loop` filter → 10/10 pass.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (daemon exercises `AgentLoop` via the stdio leader.run / agent.spawn paths; the guard is disabled by default so existing daemon E2E is byte-for-byte unchanged).
- R-6 is additive and default-off: the probe reads `LINGXIAO_TOOL_LOOP_DETECTOR` (TS `isToolLoopDetectorEnabled` parity) and is inert unless an operator opts in. The `AgentLoop::run` integration only short-circuits a round when `is_looping()` is true (probe enabled *and* streak ≥ threshold); the default-disabled path executes every round exactly as before (proven by `test_agent_loop_tool_loop_guard_disabled_does_not_skip`: 3 rounds → 3 `ToolCallInitiated`, no skips). Provider wires, `attempt_completion`, tool execution, `tool_call_history`, and `AgentContextStore` persistence are untouched on the non-tripping path. On the tripping path, only a `system` recovery message is appended (same `context.append` + optional `store.append_message` used elsewhere) and the round is skipped via `continue` — no new event types, no DB schema change, no provider-facing wire change.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure documented in the R-2/R-5/10f notes (no Windows/target-dir test failure observed; all green per-package as listed).

### 10h. This slice — R-7 agent tool-failure circuit-breaker, in-process (2026-07-08)

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new `ToolFailureLoopGuard` struct + `ToolFailureErrorKind` enum + `classify_tool_failure`/`failure_args_fingerprint`/`format_tool_failure_loop_error` free fns + `ToolFailureRecord`/`LoopGuardDecision`/`ToolFailureSignature` types + `AgentLoop.tool_failure_loop_guard` field + `with_tool_failure_loop_guard` builder. `AgentLoop::run` failure path now records + conditionally surfaces a tripped error; success path calls `clear_on_success`. No downstream breakage (daemon `AgentLoop` callers unchanged — `AgentLoop::new` constructs the default env-gated guard internally).
- `cargo test -p lingxiao-core --lib agent::`: PASS — 36 (was 20 agent-module tests pre-slice; +16: 14 `ToolFailureLoopGuard`/classify/format unit tests + 2 `AgentLoop` integration tests). `test_tool_failure_loop_guard_*` / `test_agent_loop_failure_guard_*` → 16/16 pass.
- `cargo test -p lingxiao-core --quiet`: PASS — 341 (was 325; +16 R-7 tests). 0 failures.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (daemon exercises `AgentLoop` via the stdio leader.run / agent.spawn paths; the guard is disabled by default so existing daemon E2E is byte-for-byte unchanged).
- R-7 (in-process) is additive and default-off: the guard reads `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` (TS `isToolFailureLoopGuardEnabled` parity) and is inert unless an operator opts in. The `AgentLoop::run` failure path only swaps the surfaced `result_value` for a `TOOL_FAILURE_LOOP_TRIPPED` recovery error when `decision.tripped` is true (guard enabled *and* per-key count ≥ threshold); the default-disabled path surfaces the raw `{"error": tool_result.error}` exactly as before (proven by `test_agent_loop_failure_guard_disabled_surfaces_raw_failure`: 3 rounds → 3 raw `permission denied` results, no tripped banner). On the enabled tripping path, the round is **not** skipped — the tripped error is appended as the tool result so the assistant↔tool message pairing stays well-formed (`test_agent_loop_failure_guard_surfaces_tripped_error_after_threshold`: 5 rounds → 5 `ToolCallCompleted`, the 3rd-onward carry `TOOL_FAILURE_LOOP_TRIPPED`, agent exhausts `max_rounds` — no silent completion, no infinite loop). Provider wires, `attempt_completion`, `tool_call_history`, and `AgentContextStore` persistence are untouched on the non-tripping path. The R-6 `ToolLoopDetector` and its tests are unchanged.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure documented in the prior slice notes (no Windows/target-dir test failure observed; all green per-package as listed).

### 10i. This slice — R-7 follow-up Leader-bus escalation signal (2026-07-08)

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new `ToolFailureLoopEscalation` struct + `AgentEvent::ToolFailureLoopEscalated` variant + `LoopGuardDecision.just_tripped` field + `ToolFailureErrorKind` `Serialize`/`rename_all` derives + `failure_args_fingerprint` SHA-1 switch + `start_agent_pool_event_bridge` escalation arm + `persist_agent_pool_escalation`/`persist_agent_pool_escalation_unattached` fns + new `sha1 = "0.10"` dep. No downstream breakage (daemon `AgentLoop` callers unchanged — the new event variant is non-terminal and the supervisor's forward-all `_ => {}` path already relays it).
- `cargo test -p lingxiao-core --lib agent::`: PASS — 41 (was 36; +5 R-7 follow-up tests). `test_agent_loop_failure_guard_emits_escalation_signal_on_state_error`, `test_agent_loop_failure_guard_no_escalation_on_non_state_trip`, `test_agent_loop_failure_guard_disabled_emits_no_escalation`, `test_agent_loop_failure_guard_success_clears_no_escalation`, `test_escalation_payload_omits_raw_args_and_does_not_leak_secrets` → 5/5 pass.
- `cargo test -p lingxiao-core --quiet`: PASS — 346 (was 341; +5 R-7 follow-up tests). 0 failures.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (daemon exercises `AgentLoop` via the stdio leader.run / agent.spawn paths; the guard is disabled by default so existing daemon E2E is byte-for-byte unchanged — no escalation signal is emitted on the default path).
- R-7 follow-up is additive and default-off: the escalation signal is emitted only when the guard is enabled *and* a state-class first-trip occurs (`just_tripped && requires_escalation`). The default-disabled path emits nothing (proven by `test_agent_loop_failure_guard_disabled_emits_no_escalation`: 3 rounds → 0 escalation events). The state-error path emits exactly one signal per trip (`test_agent_loop_failure_guard_emits_escalation_signal_on_state_error`: threshold=3, 5 rounds → exactly 1 `ToolFailureLoopEscalated` with `requires_escalation=true`, `error_kind=permission`, `count=3`). Non-state trips (timeout) surface the recovery error but emit no signal (`test_agent_loop_failure_guard_no_escalation_on_non_state_trip`). A sub-threshold streak cleared by success emits no signal and does not trip (`test_agent_loop_failure_guard_success_clears_no_escalation`). The durable payload omits raw args — `test_escalation_payload_omits_raw_args_and_does_not_leak_secrets` proves a fake `sk-secret-leak-12345`/`Authorization` token in the args never appears in the serialized `ToolFailureLoopEscalation`, while the `args_hash` (now a 16-char SHA-1 truncation) is carried. The round is still not skipped on trip (R-7 in-process behavior unchanged) — the assistant↔tool message pairing stays well-formed; only an additional durable signal is emitted. R-6 `ToolLoopDetector`, provider wires (R-1/R-2/R-3/R-5), and R-4 vision gating are untouched.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure documented in the prior slice notes (no Windows/target-dir test failure observed; all green per-package as listed).

### 10j. This slice — R-7 follow-up Leader-bus escalation signal: command bridge persistence coverage (2026-07-08)

The prior R-7 follow-up slice (§10i) verified only that `AgentLoop` *emits*
`AgentEvent::ToolFailureLoopEscalated` over the mpsc channel (agent-module
tests). The verifier flagged a P1 gap: the `command.rs` bridge that turns that
in-process event into a durable record — `persist_agent_pool_escalation` /
`persist_agent_pool_escalation_unattached`, which double-write `agent_logs` +
canonical `event_log` with `event_id` idempotency and `redact_persistence_secrets`
defense-in-depth — had **no live-DB regression coverage** (it runs only under
default-disabled E2E). This slice closes that gap with 4 focused bridge tests
plus a small correctness fix the tests surfaced.

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0)
- `cargo test -p lingxiao-core --lib command::`: PASS — 135 (was 131; +4 R-7
  bridge tests). `test_persist_agent_pool_escalation_double_writes_agent_logs_and_event_log`,
  `test_persist_agent_pool_escalation_payload_redacts_args_secret_in_event_log`,
  `test_persist_agent_pool_escalation_event_id_is_idempotent`,
  `test_persist_agent_pool_escalation_unattached_writes_event_log_without_agent_state`
  → 4/4 pass.
- `cargo test -p lingxiao-core --quiet`: PASS — 350 (was 346; +4). 0 failures.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows
  rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure (prior slice notes).

**Coverage landed (bridge behaviors now under regression test):**
- **Double-write** (`test_persist_agent_pool_escalation_double_writes_agent_logs_and_event_log`):
  with an active `agent_state` row, the attached path writes both an
  `agent_logs` operator row and a canonical `event_log` `EventEnvelope`, both
  with event_type `agent.tool_failure_loop_escalation`; the `event_log` payload
  carries `args_hash`/`error_kind="permission"`/`requires_escalation=true` and
  has no `arguments`/`args` field (proving the durable schema omits raw args).
- **Args-secret defense-in-depth**
  (`test_persist_agent_pool_escalation_payload_redacts_args_secret_in_event_log`):
  a fake `sk-secret-leak-12345` + `Bearer` token embedded in `last_error_message`
  never appears in the persisted `event_log` payload (`redact_persistence_secrets`
  via `append_event_in_tx`/`insert_event` strips `sk-`-prefixed and Bearer
  tokens), while the non-secret `args_hash` is still carried — so the bridge's
  secret-safety claim is now asserted, not just code-reviewed.
- **event_id idempotency** (`test_persist_agent_pool_escalation_event_id_is_idempotent`):
  two `append_event_in_tx` calls with the same `event_id` produce exactly one
  `event_log` row (the `try_fetch_event_by_id` short-circuit the bridge relies
  on so a duplicate trip cannot spam the durable log). Tested at the
  `append_event_in_tx` boundary with a fixed event_id since the bridge
  recomputes `now_ms()` per call; this is the exact dedup primitive
  `persist_agent_pool_escalation` reuses.
- **Unattached fallback**
  (`test_persist_agent_pool_escalation_unattached_writes_event_log_without_agent_state`):
  with no `agent_state` row (pool-less harness driving `AgentLoop` directly),
  the bridge falls back to the escalation's own `session_id`/`agent_id` and
  still writes both `event_log` and `agent_logs` rows under the escalation's
  own `agent_name` ("Worker"), proving the fallback path the unattached fn
  exists to serve actually works.

**Latent fix surfaced by the tests:** the bridge called
`get_current_generation` (which queries `event_log_meta`) *before*
`append_event_in_tx` (which internally calls `ensure_meta` to create the
session-meta row). In production this is harmless because the agent pool always
runs under a session created via `session.create` (which initializes the meta
row), so `get_current_generation` finds a row. But in a pool-less harness —
exactly the case the unattached fallback exists for — `get_current_generation`
hit `QueryReturnedNoRows`, the transaction rolled back, and the `let _ =`
swallowed it, silently dropping the escalation signal. Both
`persist_agent_pool_escalation` and `persist_agent_pool_escalation_unattached`
now call `ensure_meta` (idempotent `INSERT OR IGNORE`, the same call
`append_event_in_tx` makes internally) before `get_current_generation`, so the
durable write succeeds regardless of whether a session-meta row pre-exists.
This is a minimal, scoped correctness fix to the bridge itself (the subject of
this slice) — no new architecture, no unrelated-runtime change; the E2E path is
unchanged (`ensure_meta` is a no-op when the row already exists).

**Doc wording fix:** §11 previously claimed non-state trips emit no signal
"mirroring TS where the bus escalation is the state-error path only" —
imprecise, because TS `BaseAgentRuntime` actually sends the
`tool_failure_loop_escalation` bus message on *every* trip (state or
non-state). Reworded to state plainly that Rust's state-only gate is a
deliberate narrowing for durable dedup (one durable row per state trip) while
non-state trips still get the LLM-facing recovery error; full
`LeaderPermissionManager` auto-response parity (TS responds to every
escalation) remains the deferred R-7 sub-item (§9).

### 8k. Gaps closed this slice (2026-07-08) — R-7 deferred LeaderPermissionManager auto-response decision record

13. **LeaderPermissionManager auto-response *decision record* (durable)** —
    when the durable escalation signal is produced, the Rust bridge now
    additionally records a *deterministic* errorKind → action decision in the
    same transaction, mirroring the decision table TS
    `LeaderPermissionManager.handleToolFailureLoopEscalation`
    (src/agents/LeaderPermissionManager.ts:161-226) applies when it receives the
    `tool_failure_loop_escalation` bus message:
      - `permission`/`network` ⇒ `approved` (auto-escalate permission mode one
        tier + approve retry; `yolo` stays `yolo`, else → `networked` — TS
        `this.permissionContext.mode === 'yolo' ? 'yolo' : 'networked'`);
      - `sandbox` ⇒ `rejected`;
      - `mode`/`write_scope`/`schema` ⇒ `rejected` (retry is meaningless);
      - `execution`/`timeout`/`aborted`/`other`/`precondition`/default ⇒
        `interactive`.
    Rust `AgentLoop` runs in-process under the `AgentPool` (no Leader
    MessageBus), so the TS "respond by mutating live permission state + sending
    a `permission_response` to the worker" loop has no in-process analog. The
    minimal verifiable equivalent is a *durable decision record* computed from
    the same policy table and persisted so an operator/Leader layer observing
    `event_log`/`session_state` can act on it. New surface in
    `crates/lingxiao-core/src/agent.rs` + `crates/lingxiao-core/src/command.rs`:
    - `EscalationAction` enum (`Approved`/`Rejected`/`Interactive`,
      `#[serde(rename_all="snake_case")]`) — the high-level action.
    - `EscalationAutoResponse { action, decision, reason, error_kind }` — the
      decision shape; `target_mode(from_mode)` computes the TS mode-escalation
      target for `Approved` (else `None`).
    - `escalation_auto_response(ToolFailureErrorKind) -> EscalationAutoResponse`
      — the pure, testable policy core (the TS switch statement with no
      bus/db side effects). Exhaustive over every `ToolFailureErrorKind`
      variant (no silent default fall-through for a future variant).
    - `persist_escalation_auto_response_decision_in_tx` (command.rs) — runs in
      the *same transaction* as the escalation signal and writes (1) a canonical
      `agent.tool_failure_loop_escalation_decision` `EventEnvelope` in
      `event_log` (via `append_event_in_tx` ⇒ `redact_persistence_secrets`
      defense-in-depth) and (2) a `session_state` row keyed
      `tool_failure_loop_escalation_decision:{agent_id}:{tool_name}:{args_hash}`
      (upsert, so the latest decision for a trip is always readable). The
      record carries `action`/`decision`/`reason`/`error_kind`/`session_id`/
      `agent_id`/`agent_name`/`task_id`/`tool_name`/`args_hash`/`error_code`/
      `count`/`threshold`/`from_mode`/`target_mode`/`last_error_message`
      (truncated to 200 chars, mirroring TS `lastErrorMessage.slice(0, 200)`) /
      `mutation:"deferred"` (the static deferred marker as of this slice; the
      §8l live-mutation slice graduates it to `"applied"`/`"noop"`/
      `"not_applicable"`).
    - Wired into both `persist_agent_pool_escalation` (attached path) and
      `persist_agent_pool_escalation_unattached` (pool-less fallback), so the
      decision is recorded regardless of whether an `agent_state` row exists.
    (`crates/lingxiao-core/src/agent.rs`; `crates/lingxiao-core/src/command.rs`.)
    **Secret-safety:** the record carries only the `args_hash` (the 16-char
    SHA-1 fingerprint, never the raw args — the `EscalationAutoResponse` shape
    has no args field at all), and the persisted event is run through
    `redact_persistence_secrets` (strips `sk-`/`Bearer`/etc.), so a fake
    `sk-secret-leak-12345`/`Bearer` token embedded in `last_error_message`
    never leaks into the durable record (`test_escalation_decision_record_
    omits_raw_args_and_redacts_secrets`). **Idempotency:** the decision
    `event_id` is deterministic per escalation occurrence (shares `occurred_at`
    with the escalation signal, computed once per bridge dispatch), so an
    intra-dispatch duplicate write dedups to one row via
    `try_fetch_event_by_id` (`test_escalation_decision_idempotent_when_same_
    trip_processed_twice` exercises the dedup primitive at the
    `append_event_in_tx` boundary). **Deferred (remaining, narrowed):** live
    permission-mode *mutation* — actual in-place mode change is deferred because
    the only mode-change path is `CommandRouter::handle_permission_set_mode`, a
    `&self` method on the router (not reachable from the bridge's `DbOwner`
    transaction); the decision record's `from_mode`/`target_mode` fully specify
    the deferred mutation so an operator/Leader layer observing
    `event_log`/`session_state` can act on it (e.g. issue a real
    `permission.set_mode`). **Net effect:** R-7 moves from "in-process PARITY +
    durable escalation signal LANDED, auto-response deferred" to "in-process
    PARITY + durable escalation signal LANDED + durable auto-response *decision
    record* LANDED". The deferred remainder narrows from "full auto-response" to
    "live permission-mode *mutation*" only — the decision logic itself (the
    errorKind → action table) is now landed, tested, and durable. This is the
    recommended next R-7 follow-up if full in-place auto-mutation is required.

### 8l. Gaps closed this slice (2026-07-08) — R-7 live permission-mode mutation

14. **Live permission-mode *mutation* (in-transaction)** — the bridge now
    *applies* the in-place permission-mode change for an approved escalation
    decision whose `target_mode` differs from `from_mode`, closing the last
    deferred R-7 sub-item. New surface in `crates/lingxiao-core/src/command.rs`:
    - `apply_permission_mode_mutation_in_tx(tx, session_id, new_mode, occurred_at,
      mutation_event_id_prefix) -> Result<usize>` — a private in-transaction
      helper that reuses the `handle_permission_set_mode` semantics: read the
      current mode/generation inside the same tx, upsert `permission_modes`
      with `generation + 1` (ON CONFLICT → same upsert as the command path),
      collect + delete the session's `permission_grants` (a mode change
      invalidates grants scoped to the prior mode), and emit the canonical
      `permission.mode_changed` event plus one `permission.grant_revoked` event
      per revoked grant (the exact collect-then-delete-then-emit loop the full
      command path performs). Returns the revoked-grant count for the audit
      record. It does **not** do command-envelope concerns (`ensure_session_active`,
      idempotency-key cache, the `CommandResponse`) — those belong to the
      `permission.set_mode` command; the bridge is an internal in-tx caller that
      already holds a live session and shares one `occurred_at` with the
      escalation + decision events so the mutation is causally linked.
    - `persist_escalation_auto_response_decision_in_tx` now *applies* the
      mutation for an approved decision whose `target_mode` != `from_mode` by
      calling the helper, *before* writing the decision event (so the decision
      record's `from_mode` reflects the pre-mutation mode while `mutated_to`
      reflects the applied result). The decision record's `mutation` field
      graduated from the static `"deferred"` marker to an auditable outcome:
      `"applied"` (approved, target != from — mode mutated), `"noop"` (approved,
      target == from — yolo→yolo, no mutation but the decision is still
      recorded), or `"not_applicable"` (rejected/interactive — no mutation).
      New `mutated_to` (the applied mode, present only when `applied`) and
      `revoked_grants` (the count) fields carry the applied-mutation facts.
      Rejected/interactive decisions never mutate (`target_mode` is `None`).
      The decision helper now self-`ensure_meta`s (generalizing the §10j fix)
      so the mutation + decision events succeed even when invoked directly
      without the bridge's pre-existing meta row.
    - Mutation event ids are deterministic per escalation occurrence
      (`{prefix}_mode_changed_{generation}` + `{prefix}_grant_revoked_{tool}_{generation}`,
      prefix = `agent_tool_failure_loop_escalation_mutation_{session}_{agent}_{args_hash}_{occurred_at}`),
      so an intra-dispatch duplicate dispatch dedups the mode-changed /
      grant-revoked events via `try_fetch_event_by_id` rather than
      double-applying (the `permission_modes` upsert is also idempotent via
      ON CONFLICT).
    (`crates/lingxiao-core/src/command.rs`.)
    **Scope guardrails (honored):** no refactor of the whole permission system;
    denied/interactive decision logic unchanged (they never mutate); only the
    approved auto-response decision mutates, and only when `target_mode` !=
    `from_mode`. The mutation reuses the *existing* `handle_permission_set_mode`
    semantics (same upsert, same generation bump, same grant revocation, same
    event types) rather than introducing a parallel mode-change path. Secret-
    safety preserved: the decision record still carries only `args_hash` (never
    raw args; the mutation events carry only mode/tool_name/reason, no args),
    and `redact_persistence_secrets` still runs over every persisted event.
    **Net effect:** R-7 is fully LANDED — in-process guard + durable escalation
    signal + auto-response decision record + live mode mutation. The decision
    logic (errorKind → action) is landed, tested, durable, *and* now acted on
    in-transaction for approved mode escalations. No deferred R-7 remainder.

### 10k. This slice — R-7 deferred LeaderPermissionManager auto-response decision record (2026-07-08)

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new `EscalationAction`/
  `EscalationAutoResponse` types + `escalation_auto_response` pure fn (agent.rs)
  + `persist_escalation_auto_response_decision_in_tx` in-tx helper (command.rs)
  + new `escalation_auto_response` import. No downstream breakage (the new
  surface is purely additive; the bridge calls the helper inside the existing
  escalation-persist transaction — no new DB schema, no new event variants on
  the agent→supervisor channel).
- `cargo test -p lingxiao-core --lib agent::`: PASS — 48 (was 41; +7 policy
  tests). `test_escalation_auto_response_permission_approves_and_targets_networked`,
  `test_escalation_auto_response_network_approves`,
  `test_escalation_auto_response_sandbox_rejects`,
  `test_escalation_auto_response_mode_write_scope_schema_reject`,
  `test_escalation_auto_response_non_state_kinds_are_interactive`,
  `test_escalation_auto_response_covers_every_error_kind`,
  `test_escalation_auto_response_decision_record_has_no_raw_args` → 7/7 pass.
- `cargo test -p lingxiao-core --lib command::`: PASS — 144 (was 135; +9
  decision-record bridge tests). `test_escalation_decision_permission_approves_and_records_mode_target`,
  `test_escalation_decision_permission_keeps_yolo_when_already_yolo`,
  `test_escalation_decision_network_approves`,
  `test_escalation_decision_write_scope_schema_sandbox_reject`,
  `test_escalation_decision_timeout_other_interactive`,
  `test_escalation_decision_event_is_durable_in_event_log`,
  `test_escalation_decision_record_omits_raw_args_and_redacts_secrets`,
  `test_escalation_decision_idempotent_when_same_trip_processed_twice`,
  `test_escalation_decision_unattached_path_also_writes_decision_record`
  → 9/9 pass.
- `cargo test -p lingxiao-core --quiet`: PASS — 366 (was 350; +16). 0 failures.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (the
  decision record is written only on the default-disabled escalation path, so
  existing daemon E2E is byte-for-byte unchanged — no decision event is emitted
  on the default path).
- R-7 decision-record is additive and default-off-derived: the decision is
  computed and persisted *only* when the escalation signal is (guard enabled +
  state-class first-trip). The default-disabled path emits no escalation and
  therefore no decision record. The decision helper (`escalation_auto_response`)
  is a pure function over `ToolFailureErrorKind` (no I/O, no DB), so it is fully
  unit-testable in isolation; the bridge coverage asserts the durable
  persistence (event_log + session_state), secret non-leak, and idempotency
  behaviors. The R-7 in-process guard, the R-7 escalation signal, R-6
  `ToolLoopDetector`, provider wires (R-1/R-2/R-3/R-5), and R-4 vision gating
  are untouched.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows
  rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure documented in the prior
  slice notes (no Windows/target-dir test failure observed; all green
  per-package as listed above). A transient deadlock in one decision test
  (`test_escalation_decision_unattached_path_also_writes_decision_record`) was
  caught and fixed during development: it held a `db.conn()` `MutexGuard` across
  a call to a helper that re-locks `db.conn()`; rewritten to drop the guard
  before the helper call. No production code was affected (the deadlock was
  test-only — `db.conn()` is never re-locked in production bridge code).

### 10l. This slice — R-7 live permission-mode mutation (2026-07-08)

The prior R-7 decision-record slice (§10k) recorded the auto-response *decision*
but left the live mode mutation `"deferred"`. This slice closes that last
deferred sub-item: the bridge now *applies* the in-place mode change for an
approved decision via a new private in-tx helper that reuses the
`handle_permission_set_mode` semantics.

- `cargo fmt -p lingxiao-core -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new private
  `apply_permission_mode_mutation_in_tx` in-tx helper + `persist_escalation_auto_
  response_decision_in_tx` now calls it for approved target != from + new
  `mutation`/`mutated_to`/`revoked_grants` decision-payload fields + `EscalationAction`
  import. No downstream breakage (the mutation reuses the existing `permission_
  modes` upsert + `permission.mode_changed`/`permission.grant_revoked` event
  types; no new DB schema, no new agent→supervisor channel variant).
- `cargo test -p lingxiao-core --lib command::`: PASS — 152 (was 144; +8
  live-mutation tests). `test_escalation_live_mutation_strict_to_networked_
  mutates_mode_and_emits_event`, `test_escalation_live_mutation_revokes_stale_
  grants_and_emits_grant_revoked`, `test_escalation_live_mutation_no_grants_
  emits_no_grant_revoked`, `test_escalation_live_mutation_rejected_does_not_
  mutate`, `test_escalation_live_mutation_interactive_does_not_mutate`,
  `test_escalation_live_mutation_yolo_approved_is_noop`,
  `test_escalation_live_mutation_idempotent_on_duplicate_dispatch`,
  `test_escalation_live_mutation_event_log_contains_mode_changed_and_decision`
  → 8/8 pass. Existing decision-record tests updated to assert the new
  `mutation` outcomes (`applied`/`noop`/`not_applicable`) + live mode row /
  event assertions.
- `cargo test -p lingxiao-core --quiet`: PASS — 374 (was 366; +8). 0 failures.
- `cargo test -p lingxiao-core-daemon --quiet`: PASS — 8+5+11; 0 failures (the
  live mutation runs only on the default-disabled escalation path — guard
  enabled + state-class first-trip — so existing daemon E2E is byte-for-byte
  unchanged; no mutation event is emitted on the default path).
- R-7 live-mutation is additive and default-off-derived: the mutation is applied
  *only* when the escalation signal is produced (guard enabled + state-class
  first-trip) *and* the decision is `approved` *and* `target_mode` != `from_mode`.
  The default-disabled path emits no escalation and therefore no mutation
  (proven by the daemon E2E being unchanged). Rejected/interactive decisions
  never mutate (`test_escalation_live_mutation_rejected_does_not_mutate`,
  `test_escalation_live_mutation_interactive_does_not_mutate`: mode + generation
  + grants untouched, zero `mode_changed`/`grant_revoked` events). An approved
  yolo→yolo decision is a recorded `noop` with no mutation
  (`test_escalation_live_mutation_yolo_approved_is_noop`). An approved
  strict/dev→networked decision applies the mutation: `permission_modes` upserted
  with `generation + 1` and a `permission.mode_changed` event carrying old/new
  mode + generation + `source:"tool_failure_loop_escalation_auto_response"`
  (`test_escalation_live_mutation_strict_to_networked_mutates_mode_and_emits_
  event`). Stale grants are revoked — deleted from `permission_grants` with one
  `permission.grant_revoked` event per grant and `revoked_grants` recorded
  (`test_escalation_live_mutation_revokes_stale_grants_and_emits_grant_revoked`);
  a no-grants session emits no `grant_revoked` events but still emits
  `mode_changed` (`test_escalation_live_mutation_no_grants_emits_no_grant_revoked`).
  Idempotency: a duplicate *intra-dispatch* write (same `occurred_at`) dedups to
  one `mode_changed` event and one generation bump via `try_fetch_event_by_id`
  + the idempotent ON CONFLICT upsert (`test_escalation_live_mutation_idempotent_
  on_duplicate_dispatch`). The decision helper self-`ensure_meta`s (generalizing
  the §10j fix) so it succeeds when invoked directly without the bridge's
  pre-existing meta row. The R-7 in-process guard, the R-7 escalation signal, the
  R-7 decision record, R-6 `ToolLoopDetector`, provider wires (R-1/R-2/R-3/R-5),
  and R-4 vision gating are untouched.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows
  rustc `STATUS_STACK_BUFFER_OVERRUN` launch failure documented in the prior
  slice notes (no Windows/target-dir test failure observed; all green
  per-package as listed above).

### 10m. This slice — R-8 long-running periodic process-orphan reconcile sweep (2026-07-08)

- `cargo fmt -p lingxiao-core -p lingxiao-core-daemon -- --check`: PASS (exit 0)
- `cargo clippy -p lingxiao-core -p lingxiao-core-daemon --all-targets -- -D warnings`: PASS (exit 0, 0 warnings)
- `cargo check --workspace`: PASS (exit 0) — additive: new `pid_is_alive` free
  fn + `ProcessRegistry::reconcile_orphans` method (`process.rs`) + new
  `ProcessReconcileTicker` struct + `background_process_reconcile_ms`
  `RuntimeConfig` field (`lingxiao-core-daemon`) + new Windows-only
  `windows-sys` direct dep (0.52, already in the workspace lockfile). No
  downstream breakage: the boot-time `cleanup_orphans` path is unchanged; the
  new sweep is default-off (`background_process_reconcile_ms` defaults to
  `None`).
- `cargo test -p lingxiao-core --lib process::`: PASS — 9 (was 5; +4 R-8 tests).
  `test_pid_is_alive_detects_live_and_dead`,
  `test_reconcile_orphans_empty_is_noop`,
  `test_reconcile_orphans_marks_dead_pid_without_killing_live` (the
  non-killing contract: a live owned child stays `active` + still running while
  a dead-PID row flips to `reconciled_dead`; a second pass is a no-op),
  `test_reconcile_orphans_marks_reaped_real_child_as_dead` (a child that exits
  naturally — not killed — is detected dead on the next reconcile) → 4/4 pass;
  the 5 pre-existing `cleanup_orphans` tests still pass unchanged.
- `cargo test -p lingxiao-core --quiet`: PASS — 378 (was 374; +4). 0 failures.
- `cargo test -p lingxiao-core-daemon`: PASS — 9 lib + 5 main + 11 stdio
  integration; 0 failures. New
  `test_process_reconcile_ticker_marks_dead_pid_row_without_killing_live`
  drives the live `ProcessReconcileTicker` (10 ms interval) against an
  in-memory DB and asserts the dead-PID row reaches `reconciled_dead` while the
  live-managed row stays `active` and its process is still alive afterward
  (the sweep never killed it); the 11 stdio integration tests (incl. GS-026
  kill→restart→resume, leader.run native tool, session lifecycle) are
  byte-for-byte unchanged because the ticker is default-off — `serve`/stdio
  paths spawn no reconcile thread.
- R-8 is additive and default-off: the recurring sweep is spawned only when an
  operator sets `background_process_reconcile_ms` to a positive value in
  `runtime.json`. The default path (no config / `None`/`0`) runs no ticker, so
  prior daemon E2E is unchanged (proven by the 11 stdio integration tests
  passing). The sweep is safe-by-construction: `reconcile_orphans` calls
  `pid_is_alive` (never `kill_pid_tree`), so it cannot terminate a live managed
  process — the boot-time `cleanup_orphans` (which kills every active row)
  remains boot-only and is not reused periodically. The retry/fallback
  total-deadline candidate (§8m note) was verified not a parity gap against
  TS `LlmGuard`, so it is left as a shared P3 design item, not tracked here.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows
  rustc launch failure documented in the prior slice notes (no
  Windows/target-dir test failure observed; all green per-package as listed
  above).

### 10n. This slice — R-9 Bedrock streaming parity (2026-07-08)

- `cargo fmt -p lingxiao-llm-bedrock-provider -- --check`: PASS (exit 0);
  workspace `cargo fmt --check`: PASS (exit 0).
- `cargo clippy -p lingxiao-llm-bedrock-provider --all-targets -- -D warnings`:
  PASS (exit 0, 0 warnings).
- `cargo check --workspace`: PASS (exit 0) — the only production-code change is
  inside `execute_invoke_model` (new `stream` branch delegating to the new
  `execute_invoke_model_stream`) + the new pure `bedrock_stream_chunk_to_events`
  /`map_stop_reason_str`/`initial_tool_input_json` helpers + a new
  `use aws_sdk_bedrockruntime::types::ResponseStream;` import. The non-streaming
  `invoke_model` path, `build_invoke_body`, `bedrock_message`, and all R-3
  multimodal projections are byte-for-byte unchanged. No downstream breakage
  (the Bedrock provider is a leaf binary consumed over stdio; no other crate
  references its internals).
- `cargo test -p lingxiao-llm-bedrock-provider`: PASS — 32 (was 18; +14 R-9
  streaming tests). The 18 pre-existing tests (config/body-build/tool-schemas/
  non-streaming-response-parse/anthropic+titan response maps/multimodal image
  blocks/retain-window/auth-rejection) all still pass unchanged → non-streaming
  + multimodal behavior preserved.
- `cargo test -p lingxiao-llm-host-protocol --quiet`: PASS — 39 (untouched
  crate, re-run for safety; 0 fail).
- New R-9 tests (14):
  - `test_stream_true_routes_to_streaming_path_not_unsupported` — regression
    guard: `stream=true` pointed at an unreachable port surfaces a dispatch
    error, **never** `UnsupportedModel` (the old short-circuit code is gone).
  - `test_stream_true_rejects_unsupported_auth_before_sdk_call` — the streaming
    path shares `ResolvedBedrockConfig::from_request`, so unsupported auth is
    rejected before any AWS call.
  - Pure `bedrock_stream_chunk_to_events` parser tests:
    `test_stream_chunk_text_delta_emits_text_delta`,
    `test_stream_chunk_thinking_delta_emits_thinking_delta`,
    `test_stream_chunk_signature_delta_is_ignored`,
    `test_stream_chunk_tool_use_lifecycle_emits_delta_then_tool_call`
    (start→delta→delta→stop produces ToolCallDelta×3 then a finalized ToolCall
    with parsed `{"path":"README.md"}`),
    `test_stream_chunk_message_delta_emits_usage_and_finish`,
    `test_stream_chunk_message_delta_tool_use_finish_reason`,
    `test_stream_chunk_message_stop_emits_finished_stop`,
    `test_stream_chunk_error_emits_provider_error`,
    `test_stream_chunk_message_start_and_ping_are_ignored`,
    `test_stream_chunk_full_sequence_text_then_finish_no_duplicate_finished`
    (2 text deltas + usage + 2 terminal Finished events; the streaming loop
    dedups the second),
    `test_stream_chunk_with_non_tool_content_block_start_is_ignored`.
  - `test_bedrock_event_stream_frame_round_trips_chunk_payload` — genuine
    `aws-smithy-eventstream` binary framing round-trip: builds a real Bedrock
    `chunk` `Message` (headers `:message-type=event`/`:event-type=chunk`/
    `:content-type=application/json`, payload `{"bytes":<base64 of an Anthropic
    text_delta event>}`), serializes via `write_message_to`, re-reads via
    `read_message_from`, parses headers via the public `parse_response_headers`
    (the same call the SDK's `ResponseStreamUnmarshaller` uses — replicated here
    because that unmarshaller module is crate-private), base64-decodes the
    payload `bytes` blob back to the original event JSON, and feeds it through
    `bedrock_stream_chunk_to_events` → asserts the expected `TextDelta`. This
    proves the parser against real Bedrock wire framing, not just hand-rolled
    JSON.
- Note: tests run with `CARGO_BUILD_JOBS=1` to avoid the intermittent Windows
  rustc launch failure documented in the prior slice notes (no
  Windows/target-dir test failure observed; all green per-package as listed
  above). No real AWS credentials or calls are made: the only test that
  constructs an AWS `Client` points `endpoint_url` at `http://127.0.0.1:9` (a
  closed port) purely to prove the streaming branch is taken; all parsing
  coverage is against hand-crafted JSON and a locally-built event-stream frame.
- R-9 is additive to the streaming branch only: `request.stream == false`
  continues through the unchanged `invoke_model` path (proven by the 18
  unchanged tests). The `bedrock_body` metadata escape hatch short-circuits
  before `build_invoke_body` in both paths, so passthrough is preserved. R-3
  multimodal (`bedrock_user_content`/`bedrock_image_source_from_url`) and R-5
  retain-rounds are computed inside `build_invoke_body`, which the streaming
  path reuses, so a streaming multimodal request builds the same image blocks
  on the wire as a non-streaming one. Duplicate-`Finished` suppression and the
  defensive terminal `Finished` mirror the Anthropic provider's streaming path
  so a `message_delta`+`message_stop` sequence yields exactly one `Finished`.

## 11. Recommendation

Advance to verifier for the R-7 follow-up Leader-bus escalation signal slice.
Rust Core's `AgentLoop` now produces a durable/canonical escalation signal when
the `ToolFailureLoopGuard` trips on a state-class error — closing the P1 gap the
verifier flagged against the prior R-7 in-process slice (which only surfaced a
recovery error to the LLM, leaving the Leader un-escalated). On a state-class
first-trip (`just_tripped && requires_escalation`), `AgentLoop` emits an
`AgentEvent::ToolFailureLoopEscalated` carrying a `ToolFailureLoopEscalation`
payload; the `start_agent_pool_event_bridge` persists it both to `agent_logs`
(operator row) and to the durable `event_log` as a canonical `EventEnvelope`
(event_type `agent.tool_failure_loop_escalation`, via `append_event_in_tx` with
`redact_persistence_secrets` defense-in-depth). This is the minimal verifiable
equivalent of the TS `agent:tool_failure_loop` emitter event +
`tool_failure_loop_escalation` MessageBus message to `LeaderPermissionManager` —
Rust has no worker bus, so the signal is durable-and-observable (replayable via
`event_log`) rather than handled in place.

Secret-safety is verified: `LoopGuardDecision.just_tripped` ensures the signal
fires exactly once per trip (mirrors TS `emitTripped`'s first-trip-only block),
so a looping LLM cannot spam the durable `event_log`. The
`failure_args_fingerprint` was switched from the full stable-JSON string to a
16-char SHA-1 truncation (TS `hashArgs` parity) precisely so the durable
payload cannot leak args-secrets — `test_escalation_payload_omits_raw_args_and_
does_not_leak_secrets` proves a fake `sk-secret-leak-12345`/`Authorization`
token in the args never appears in the serialized escalation, while the
`args_hash` is still carried for Leader-side de-dup. Equal-args⇒equal-key
semantics are preserved (prior R-7 in-process tests still pass). The
`ToolFailureLoopEscalation` payload schema deliberately has no `args`/
`arguments` field.

Default-off and pairing-safe: the signal is emitted only when the guard is
enabled *and* a state-class first-trip occurs; the default-disabled path emits
nothing (`test_agent_loop_failure_guard_disabled_emits_no_escalation`). Non-state
trips (e.g. timeout) surface the recovery error but emit no Leader signal
(`test_agent_loop_failure_guard_no_escalation_on_non_state_trip`). This is a
deliberate narrowing of the TS behavior, not a byte-match: TS
`BaseAgentRuntime` sends the `tool_failure_loop_escalation` MessageBus message
on *every* trip (state or non-state), whereas Rust emits the durable
escalation signal only on state-class first-trips — the state-only gate keeps
the durable `event_log` dedup-set small (one durable row per state trip) while
still surfacing the LLM-facing recovery error for non-state trips. The
deferred remainder — full `LeaderPermissionManager` *auto-response* parity (TS
responds to every escalation by mode-bump/reject/interactive based on
errorKind) — is recorded in §9 R-7. A sub-threshold streak
cleared by success neither trips nor escalates
(`test_agent_loop_failure_guard_success_clears_no_escalation`). The round is
still not skipped on trip — the tripped recovery error is appended as the tool
result so the assistant↔tool message pairing stays well-formed (R-7 in-process
behavior unchanged); only an additional durable signal is emitted. The R-6
`ToolLoopDetector`, provider wires (R-1/R-2/R-3/R-5), and R-4 vision gating are
untouched. No regressions: lingxiao-core 341→346 (+5), daemon 8+5+11 unchanged,
`cargo check --workspace` green, clippy clean, `cargo fmt` clean.

**Scope boundary (deferred remainder):** this slice lands the durable/canonical
escalation *signal*. The deferred remainder is **full
`LeaderPermissionManager` auto-response parity** — the TS handler *responds* to
the escalation by auto-approving/rejecting/interactive-approval based on
errorKind (permission/network ⇒ bump permission mode; mode/write_scope/schema ⇒
reject; sandbox ⇒ reject; others ⇒ interactive). Rust has no Leader MessageBus,
so the signal is durable-and-observable but not auto-handled in place; an
operator/Leader layer observing `event_log` can act on it. That auto-response
parity is the recommended next R-7 follow-up. With R-7 in-process + the
escalation signal landed, the agent-runtime area now ships the *loop* probe
(R-6), the *failure* circuit-breaker (R-7 in-process), and the *failure*
escalation signal (R-7 follow-up) — the mislabeled-PARITY gap the matrix
previously carried is closed except for the Leader auto-response sub-item.

**Follow-up (§10j):** the verifier's P1 test-coverage note against the prior
slice is now closed — the `command.rs` bridge's durable double-write
(`agent_logs` + canonical `event_log`), `event_id` idempotency,
`redact_persistence_secrets` defense-in-depth, and unattached fallback are
under live-DB regression test (4 tests; lingxiao-core 346→350). A latent
bridge bug the tests surfaced (`get_current_generation` before `ensure_meta`
silently dropped the signal in pool-less harnesses) is fixed. Recommend
advancing this test-coverage follow-up to the verifier.

**Follow-up (§10k — this slice):** the deferred `LeaderPermissionManager`
*auto-response* sub-item is now partially closed. When the durable escalation
signal is produced, the bridge additionally records a *deterministic*
errorKind → action decision in the same transaction, mirroring the TS
`LeaderPermissionManager.handleToolFailureLoopEscalation` (src/agents/
LeaderPermissionManager.ts:161-226) decision table: permission/network ⇒
`approved` (mode escalates: yolo stays yolo, else → networked); sandbox/mode/
write_scope/schema ⇒ `rejected`; execution/timeout/aborted/other/precondition
⇒ `interactive`. The pure policy core `escalation_auto_response` (`agent.rs`)
is persisted by `persist_escalation_auto_response_decision_in_tx`
(command.rs) as (1) a canonical `agent.tool_failure_loop_escalation_decision`
`EventEnvelope` in `event_log` (`redact_persistence_secrets` defense-in-depth)
and (2) a `session_state` row keyed
`tool_failure_loop_escalation_decision:{agent_id}:{tool_name}:{args_hash}` so
an operator/Leader layer can read the latest decision for a trip and act on it.
The record carries `action`/`decision`/`reason`/`error_kind`/`session_id`/
`agent_id`/`agent_name`/`task_id`/`tool_name`/`args_hash`/`error_code`/`count`/
`threshold`/`from_mode`/`target_mode`/`last_error_message` (truncated 200)/
`mutation` (graduated from `"deferred"` to `"applied"`/`"noop"`/`"not_applicable"`
in the §10l live-mutation slice) — `args_hash` only (never raw args; the
decision shape has no args field at all). 16 new focused tests landed (7
agent-module policy tests `test_escalation_auto_response_*` + 9 command-bridge
persistence tests `test_escalation_decision_*`); lingxiao-core 350→366, daemon
8+5+11 unchanged. `cargo check --workspace` green, clippy clean, `cargo fmt`
clean.

**Scope boundary (deferred remainder, narrowed):** the decision *logic* (the
errorKind → action table) is now landed, tested, and durable. The deferred
remainder narrows to **live permission-mode *mutation*** — actual in-place
mode change is deferred because the only mode-change path is
`CommandRouter::handle_permission_set_mode`, a `&self` method on the router
(not reachable from the bridge's `DbOwner` transaction; the low-level
`current_permission_mode`/`current_permission_generation` readers exist, but no
in-tx mutation helper composes them with the `permission.mode_changed`/
`permission.grant_revoked` event emission the full command path performs). The
decision record's `from_mode`/`target_mode` fully specify the deferred
mutation, so it is actionable and auditable — an operator/Leader layer can read
the `session_state` row and issue a real `permission.set_mode` to apply it.
This is the recommended next R-7 follow-up if full in-place auto-mutation is
required. With R-7 in-process + the escalation signal + the auto-response
decision record landed, the agent-runtime area now ships the *loop* probe
(R-6), the *failure* circuit-breaker (R-7 in-process), the *failure* escalation
signal (R-7 follow-up), and the *failure* auto-response decision record
(R-7 this slice) — the mislabeled-PARITY gap the matrix previously carried is
closed except for the live-mutation sub-item. Recommend advancing this
decision-record slice to the verifier.

**Follow-up (§10l — this slice):** the last deferred R-7 sub-item — live
permission-mode *mutation* — is now LANDED. For an approved escalation decision
whose `target_mode` != `from_mode`, the bridge now *applies* the in-place mode
change in the same transaction via a new private in-tx helper
`apply_permission_mode_mutation_in_tx` (command.rs) that reuses the
`handle_permission_set_mode` semantics: upsert `permission_modes` with
`generation + 1`, revoke stale `permission_grants` (delete + one
`permission.grant_revoked` event per grant), and emit the canonical
`permission.mode_changed` event. The decision record's `mutation` field
graduated from the static `"deferred"` marker to an auditable outcome
(`"applied"`/`"noop"`/`"not_applicable"`), with new `mutated_to`/`revoked_grants`
fields carrying the applied-mutation facts. Rejected/interactive decisions never
mutate; an approved yolo→yolo decision is a recorded `noop`. Mutation event ids
are deterministic per occurrence so an intra-dispatch duplicate dedups via
`try_fetch_event_by_id` rather than double-applying; the decision helper
self-`ensure_meta`s (generalizing the §10j fix). Scope guardrails honored: no
permission-system refactor, denied/interactive logic unchanged, only the
approved auto-response mutates and only when target != from, reusing the
existing mode-change semantics (no parallel path). Secret-safety preserved
(decision record carries `args_hash` only; mutation events carry no args;
`redact_persistence_secrets` still runs). 8 new focused tests landed
(`test_escalation_live_mutation_*`: strict→networked mutates + mode_changed;
grant revocation + grant_revoked events + revoked_grants count; no-grants case;
rejected no mutation + grants preserved; interactive no mutation; yolo noop;
intra-dispatch idempotency; event_log contains mode_changed + decision);
existing decision-record tests updated for the new `mutation` outcomes;
lingxiao-core 366→374, daemon 8+5+11 unchanged. `cargo check --workspace`
green, clippy clean, `cargo fmt` clean.

**R-7 is now fully LANDED** — in-process guard + durable escalation signal +
auto-response decision record + live mode mutation. The agent-runtime area
ships the *loop* probe (R-6), the *failure* circuit-breaker (R-7 in-process),
the *failure* escalation signal (R-7 follow-up), the *failure* auto-response
decision record (R-7 §10k), and the *failure* live mode mutation (R-7 §10l) —
the mislabeled-PARITY gap the matrix previously carried is fully closed. No
deferred R-7 remainder. Recommend advancing this live-mutation slice to the
verifier.

**Follow-up (§10m — this slice):** the P2 reliability gap "long-running
periodic process-orphan sweep not separately scheduled" (QA-record Remaining
Risks; acceptance-gate P2/P3 "process polling is bounded and justified") is now
LANDED as R-8. Rust had only the boot-time `cleanup_orphans` (which
`kill_pid_tree`s every active row — correct only at boot, where every row is
stale from a dead daemon; unsafe to run periodically because it would kill the
daemon's own live sidecars/terminals/REPLs/MCP servers), and no recurring or
lazy reconcile — so a `owned_processes` row left `active` when a managed
process died mid-session stayed active forever. The new
`ProcessRegistry::reconcile_orphans` probes each active row's PID for liveness
via the non-killing `pid_is_alive` (`kill(pid,0)` on Unix; `OpenProcess` +
`GetExitCodeProcess`/`STILL_ACTIVE` on Windows — no terminate rights) and flips
dead-PID rows to `reconciled_dead` while leaving live PIDs untouched; the
daemon gains a `ProcessReconcileTicker` (mirrors `ScheduleTicker`: named thread,
`AtomicBool` stop, `JoinHandle`/`Drop` shutdown) spawned when
`background_process_reconcile_ms` is `Some(n>0)` (default `None` ⇒ boot-only,
byte-for-byte prior behavior). Safe-by-construction: the sweep can never kill a
live managed process, unlike a naive periodic reuse of `cleanup_orphans`. 5 new
focused tests landed (4 `lingxiao-core` `process::` unit tests incl. the
non-killing contract + 1 daemon ticker integration test); lingxiao-core
374→378, daemon 9+5+11 unchanged (default-off). The retry/fallback
total-deadline candidate was verified **not a parity gap** — TS `LlmGuard.call()`
(LlmGuard.ts:247 loop) has no total/global wall-clock deadline either; both
sides bound per-attempt only — so it is left as a shared P3 design item, not
tracked as R-8. `cargo check --workspace` green, clippy clean (touched
packages), `cargo fmt` clean. Recommend advancing this R-8 slice to the
verifier.

**Follow-up (§10n — this slice):** the highest user-visible remaining provider
gap the R-8 verifier identified — Bedrock streaming — is now LANDED as R-9. The
Rust Bedrock provider explicitly returned `UnsupportedModel` for `stream=true`,
so a `supports_streaming: true` Bedrock provider in `runtime.json` silently
failed every streaming request (TS routes Bedrock through the
`@ai-sdk/amazon-bedrock` Vercel SDK, which streams natively).
`execute_invoke_model` now branches on `stream`: `stream=false` stays on the
byte-for-byte-unchanged `invoke_model` path; `stream=true` calls
`client.invoke_model_with_response_stream()` and drains the `EventReceiver`
via `recv()`. Each AWS event-stream `chunk` frame's `PayloadPart.bytes` blob
(already base64-decoded by the SDK) is the raw JSON of one complete Anthropic
Messages SSE event — no SSE text framing to split, simpler than the Anthropic
raw-SSE path — parsed by the new pure `bedrock_stream_chunk_to_events`, which
mirrors the Anthropic provider's `raw_anthropic_sse_value_to_events`
(text/thinking/tool-use deltas with per-tool-block accumulation,
`message_delta` usage+finish, `message_stop`, `error`, ignored
`message_start`/`ping`/`signature_delta`), with duplicate-`Finished`
suppression and a defensive terminal `Finished`. Errors map through the
existing `provider_error_from_bedrock` (AKIA/ASIA redaction); mid-stream
errors after events surface as `StreamEvent::Error` rather than failing the
whole call. Non-streaming behavior, the `bedrock_body` escape hatch, and R-3
multimodal are shared and unchanged. 14 new tests landed (no real AWS calls):
12 pure-parser/routing tests + a genuine `aws-smithy-eventstream` binary
framing round-trip proving the parser against real Bedrock wire framing;
lingxiao-llm-bedrock-provider 18→32, host-protocol 39 unchanged. `cargo check
--workspace` green, clippy clean, `cargo fmt` clean. The Bedrock provider now
matches TS streaming parity. Recommend advancing this R-9 slice to the
verifier.

**Remaining highest-priority gap:** with R-1..R-9 landed, the previously-open
provider/runtime parity gaps are closed. The remaining items are shared P3
design items (not Rust-vs-TS divergences): (1) the streaming sink latency in
`route_stream_with_sink` — a successful provider attempt is buffered before
flushing to the caller sink so retry/fallback can avoid exposing partial failed
attempts (correctness-preserving, P3 realtime-latency design item, recorded in
the QA-record Remaining Risks); (2) the retry/fallback total wall-clock deadline
is per-attempt-bounded on both sides (verified not a parity gap vs TS
`LlmGuard`). The next verifier-visible candidate is a real-provider Bedrock
streaming smoke against a live `anthropic.claude-*` model on Bedrock (analogous
to the OpenAI live E2E / Anthropic smoke in the acceptance gate) — gated on
operator AWS credentials, not a code gap.
