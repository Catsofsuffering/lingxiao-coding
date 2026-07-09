use crate::context::ContextManager;
#[cfg(test)]
use crate::llm::LlmRouter;
use crate::llm::{
    AuthContext, FinishReason, GenerateRequest, LlmEventSink, Message, ProviderError,
    RequestOptions, StreamEvent, ToolCall, ToolCallAccumulator,
};
use crate::tool::{ToolRegistry, ToolResult};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

// ─────────────────────────────────────────────────────────────────────────────
// ToolLoopDetector — same-name + same-args tool-call loop probe
// ─────────────────────────────────────────────────────────────────────────────
//
// Mirrors TS `src/agents/runtime/ToolLoopDetector.ts`. Detects when the agent
// requests the *exact same* tool name + arguments across consecutive rounds
// (a stuck strategy) so the loop can inject a recovery system prompt and skip
// the repeated round, instead of burning rounds/tokens re-issuing a call that
// will produce the same observation.
//
// Fingerprint = `{name}::{stable_json(args)}`. `stable_json` recursively sorts
// object keys so `{a:1,b:2}` ≡ `{b:2,a:1}` (TS `stableJson` parity). When a
// round has multiple tool calls, the round signature is the sorted multiset of
// per-call fingerprints joined by `|` (order-independent): a round only extends
// the streak when its whole fingerprint set matches the previous round.
//
// Disabled by default (TS parity — `LINGXIAO_TOOL_LOOP_DETECTOR` env, truthy
// 1/true/yes/on); a pure-text round (no tool calls) neither extends nor resets
// the streak. Default threshold 4 (5th identical round trips), `max(2, n)`.

const DEFAULT_TOOL_LOOP_THRESHOLD: usize = 4;

fn is_truthy_env(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Whether the tool-loop probe is enabled. Reads `LINGXIAO_TOOL_LOOP_DETECTOR`
/// the same way TS `isToolLoopDetectorEnabled` does. An explicit `enabled`
/// option always wins over the env (matching the TS constructor).
fn tool_loop_detector_enabled_from_env() -> bool {
    is_truthy_env(std::env::var("LINGXIAO_TOOL_LOOP_DETECTOR").ok().as_deref())
}

/// Serialize a JSON value into a stable string: object keys sorted recursively,
/// arrays in order, scalars via `serde_json::to_string`. Mirrors TS `stableJson`
/// so structurally-equal args produce the same fingerprint regardless of key
/// ordering in the wire representation.
fn stable_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let body = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        stable_json(&map[k])
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("{{{body}}}")
        }
        Value::Array(items) => {
            let body = items.iter().map(stable_json).collect::<Vec<_>>().join(",");
            format!("[{body}]")
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| String::from("null")),
    }
}

/// Fingerprint a single tool call: `{name}::{stable_json(args)}`. Mirrors TS
/// `fingerprintToolCall`. Rust `ToolCall.arguments` is already a parsed
/// `serde_json::Value` (TS carries a JSON string), so no normalize/parse step is
/// needed.
fn fingerprint_tool_call(tool_call: &ToolCall) -> String {
    format!("{}::{}", tool_call.name, stable_json(&tool_call.arguments))
}

#[derive(Debug, Clone)]
pub struct ToolLoopDetectorOptions {
    pub enabled: Option<bool>,
    pub threshold: usize,
}

impl Default for ToolLoopDetectorOptions {
    fn default() -> Self {
        Self {
            enabled: None,
            threshold: DEFAULT_TOOL_LOOP_THRESHOLD,
        }
    }
}

#[derive(Debug, Default)]
pub struct ToolLoopDetector {
    enabled: bool,
    threshold: usize,
    last_signature: Option<String>,
    streak: usize,
}

impl ToolLoopDetector {
    pub fn new(options: ToolLoopDetectorOptions) -> Self {
        Self {
            enabled: options
                .enabled
                .unwrap_or_else(tool_loop_detector_enabled_from_env),
            threshold: options.threshold.max(2),
            last_signature: None,
            streak: 0,
        }
    }

    /// Observe one round's tool-call set. Empty rounds neither extend nor
    /// reset the streak (TS `observe` parity). A round only extends the streak
    /// when its full sorted fingerprint multiset matches the previous round.
    pub fn observe(&mut self, tool_calls: &[ToolCall]) {
        if !self.enabled {
            return;
        }
        if tool_calls.is_empty() {
            return;
        }
        let mut fingerprints: Vec<String> = tool_calls.iter().map(fingerprint_tool_call).collect();
        fingerprints.sort();
        let signature = fingerprints.join("|");
        if Some(&signature) == self.last_signature.as_ref() {
            self.streak += 1;
        } else {
            self.last_signature = Some(signature);
            self.streak = 1;
        }
    }

    pub fn is_looping(&self) -> bool {
        self.enabled && self.streak >= self.threshold
    }

    pub fn consecutive_count(&self) -> usize {
        self.streak
    }

    pub fn current_signature(&self) -> Option<&str> {
        self.last_signature.as_deref()
    }

    pub fn reset(&mut self) {
        self.last_signature = None;
        self.streak = 0;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ToolFailureLoopGuard — consecutive-tool-failure circuit-breaker (R-7)
// ─────────────────────────────────────────────────────────────────────────────
//
// Mirrors TS `src/agents/runtime/ToolFailureLoopGuard.ts` (in-process slice).
// Where `ToolLoopDetector` (R-6) trips on the *same successful* call re-issued
// across rounds, this guard trips on the *same failing* call re-issued across
// rounds — the "tool reports failure → LLM reads error → retries identical call
// → tool fails the same way" death-spiral that burns rounds/tokens. State-class
// errors (permission/mode/write_scope/sandbox/network/schema) keep failing no
// matter how many times retried, so reaching the threshold forces a trip.
//
// key = `{toolName}::{argsHash}::{errorKind}` (TS parity). `argsHash` reuses the
// R-6 `stable_json` helper so structurally-equal args produce the same key
// regardless of key ordering (TS uses a 16-char SHA-1 truncation of the same
// stable-JSON string for memory; Rust uses the full stable-JSON string as the
// fingerprint — same "equal args ⇒ equal hash" semantics, no new dep). The
// `errorKind` is the normalized error class derived from the failure text.
//
// Disabled by default (TS parity — `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` env,
// truthy 1/true/yes/on, reusing the R-6 `is_truthy_env` helper). Default
// threshold 3, floored at 2 (TS `Math.max(2, n)`). On trip the caller surfaces
// a `TOOL_FAILURE_LOOP_TRIPPED` recovery error to the LLM instead of the raw
// failure, prompting a strategy change; it does NOT perform Leader-bus
// escalation (that is the deferred R-7 follow-up sub-slice — Rust `AgentLoop`
// runs in-process under the `AgentPool`, not a worker bus).
//
// Scope note: the TS guard is a process-global singleton keyed by sessionId so
// failures are shared across modules. The Rust `AgentLoop` is a per-agent
// instance bound to a single session, so the guard lives as an `AgentLoop`
// field and tracks one session's keys — the cross-session isolation the TS
// singleton achieves via its sessionId map is structurally guaranteed here by
// ownership. `trippedRetentionMs` (TS keeps a 60s memory to suppress repeat
// trips) is intentionally not time-based here: a tripped key stays tripped
// until `clear_on_success` (the tool eventually succeeds with these args) or
// `reset_session` (agent terminates) — sufficient for the in-process loop,
// which has no bus to suppress chatter against.

const DEFAULT_TOOL_FAILURE_THRESHOLD: usize = 3;
const DEFAULT_MAX_FAILURE_KEYS: usize = 256;

/// Normalized error class for the third segment of the failure key. Mirrors TS
/// `ToolFailureErrorKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolFailureErrorKind {
    Permission,
    Mode,
    WriteScope,
    Sandbox,
    Network,
    Schema,
    Precondition,
    Execution,
    Timeout,
    Aborted,
    Other,
}

impl ToolFailureErrorKind {
    /// Whether this kind is a "state-class" error — one where retrying cannot
    /// change the outcome, so the guard must trip and the caller must escalate
    /// (TS `STATE_ERROR_KINDS`). precondition/execution/timeout/aborted/other
    /// are not state-class (a hint or transient jitter may yet resolve them).
    fn is_state_error(self) -> bool {
        matches!(
            self,
            Self::Permission
                | Self::Mode
                | Self::WriteScope
                | Self::Sandbox
                | Self::Network
                | Self::Schema
        )
    }

    /// Guided precondition errors (e.g. "read the file first") carry an
    /// explicit next-step hint; tripping would replace that hint with an
    /// unrecoverable error, hiding the real recovery path. TS
    /// `NON_TRIPPING_ERROR_KINDS` parity.
    fn is_non_tripping(self) -> bool {
        matches!(self, Self::Precondition)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Mode => "mode",
            Self::WriteScope => "write_scope",
            Self::Sandbox => "sandbox",
            Self::Network => "network",
            Self::Schema => "schema",
            Self::Precondition => "precondition",
            Self::Execution => "execution",
            Self::Timeout => "timeout",
            Self::Aborted => "aborted",
            Self::Other => "other",
        }
    }
}

/// Classify a tool failure into a normalized error kind. Mirrors TS
/// `classifyToolFailure` / `ERROR_KIND_PATTERNS`: case-insensitive keyword
/// substring match over the combined error text (Rust `ToolResult.error` is a
/// free-text string with no typed `code` field, unlike TS `ToolErrorEnvelope`,
/// so the code+message combine TS does collapses to a single text here). The
/// keyword set is ordered to match the TS pattern array precedence, so the
/// first matching kind wins (e.g. `WRITE_SCOPE_FORBIDDEN` is caught by
/// write_scope before the generic permission fallback).
fn classify_tool_failure(error_text: &str) -> ToolFailureErrorKind {
    let combined = error_text.trim();
    if combined.is_empty() {
        return ToolFailureErrorKind::Other;
    }
    let lower = combined.to_ascii_lowercase();
    // Order matters: more specific kinds first, mirroring the TS pattern array.
    // WriteScope is checked before Permission because its Rust error texts embed
    // the word "permission" (e.g. "outside permission grant") — the more
    // specific "outside permission …" phrases must win over the bare permission
    // tokens, exactly as the TS regex array tries write_scope before the
    // generic permission fallback.
    let matched: [(ToolFailureErrorKind, &[&str]); 9] = [
        (
            ToolFailureErrorKind::WriteScope,
            &[
                "write_scope_forbidden",
                "write_out_of_scope",
                "scope_forbidden",
                "outside permission scope",
                "outside permission grant",
                "write_scope",
                "outside the session workspace",
            ],
        ),
        (
            ToolFailureErrorKind::Permission,
            &[
                "permission_required",
                "permission_denied",
                "tool_scope_forbidden",
                "requires permission grant",
                // Bare fallback for Rust's free-text errors (e.g. "permission
                // denied"). Safe because WriteScope — whose texts also embed
                // "permission" ("outside permission grant") — is checked first.
                "permission",
            ],
        ),
        (
            ToolFailureErrorKind::Mode,
            &[
                "mode_tool_forbidden",
                "mode_forbidden",
                "tool_mode_mismatch",
                "mode",
            ],
        ),
        (
            ToolFailureErrorKind::Sandbox,
            &["sandbox_forbidden", "sandbox_blocked", "sandbox"],
        ),
        (
            ToolFailureErrorKind::Network,
            &[
                "network_forbidden",
                "network_blocked",
                "network_unreachable",
                "network",
            ],
        ),
        (
            ToolFailureErrorKind::Precondition,
            &[
                "file_must_be_read_first",
                "read_first",
                "file_must_read_first",
                "must be read first",
            ],
        ),
        (
            ToolFailureErrorKind::Schema,
            &[
                "tool_argument_parse_failed",
                "tool_argument_validation_failed",
                "tool_not_found",
                "schema_invalid",
                "argument_validation",
                "missing required param",
                "tool not found",
            ],
        ),
        (
            ToolFailureErrorKind::Timeout,
            &["tool_timeout", "timed out", "timeout"],
        ),
        (
            ToolFailureErrorKind::Aborted,
            &["tool_aborted", "aborted", "aborterror"],
        ),
    ];
    for (kind, keywords) in matched {
        for kw in keywords {
            if lower.contains(kw) {
                return kind;
            }
        }
    }
    ToolFailureErrorKind::Other
}

/// Stable args fingerprint for the failure key — reuses the R-6 `stable_json`
/// helper so `{a:1,b:2}` ≡ `{b:2,a:1}` (TS `hashArgs(normalizeArgsForHash(args))`
/// parity over equality semantics), then SHA-1-truncates to 16 hex chars (TS
/// `hashArgs` parity). Rust `ToolCall.arguments` is already a parsed `Value`,
/// so no string-parse normalize step is needed.
///
/// SHA-1 is used as a fingerprint only (not for security), but it is one-way:
/// the durable escalation payload (persisted to `event_log`) carries this hash
/// rather than the raw args, so an args-secret (e.g. a token in a shell
/// command) cannot leak into the durable record. The R-7 in-process slice used
/// the full stable-JSON string as the in-memory key; switching to the SHA-1
/// truncation preserves equal-args⇒equal-key semantics (so existing tests
/// still pass) while making the fingerprint safe to persist.
fn failure_args_fingerprint(args: &Value) -> String {
    use sha1::{Digest, Sha1};
    let raw = stable_json(args);
    let digest = Sha1::digest(raw.as_bytes());
    // 16 hex chars — TS `hashArgs` slices the SHA-1 hex to 16 chars.
    let hex = digest
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    hex[..16].to_string()
}

/// A recorded failure for one (toolName, argsHash, errorKind) key. Mirrors TS
/// `ToolFailureRecord` (minus the time-retention fields, which have no
/// time-based behavior in the in-process slice). Public so the test/observation
/// `snapshot()` accessor can return it.
#[derive(Debug, Clone)]
pub struct ToolFailureRecord {
    pub tool_name: String,
    pub args_hash: String,
    pub error_kind: ToolFailureErrorKind,
    pub error_code: String,
    pub count: usize,
    pub last_error_message: String,
    pub tripped: bool,
}

/// Normalized signature returned to the caller. Mirrors TS
/// `ToolFailureSignature`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolFailureSignature {
    pub tool_name: String,
    pub args_hash: String,
    pub error_kind: ToolFailureErrorKind,
    pub error_code: String,
}

/// Decision returned by `ToolFailureLoopGuard::record`. Mirrors TS
/// `LoopGuardDecision`.
#[derive(Debug, Clone)]
pub struct LoopGuardDecision {
    pub tripped: bool,
    pub count: usize,
    pub error_kind: ToolFailureErrorKind,
    pub signature: ToolFailureSignature,
    pub requires_escalation: bool,
    /// True only on the *first* record that flips this key from not-tripped to
    /// tripped (i.e. the count just reached the threshold this call). Subsequent
    /// records against an already-tripped key return `tripped: true` but
    /// `just_tripped: false`. Lets the caller emit a Leader escalation signal
    /// exactly once per trip (mirroring TS `emitTripped`, which fires only inside
    /// the `if (tripped)` first-trip block, not the already-tripped branch).
    pub just_tripped: bool,
}

#[derive(Debug, Clone)]
pub struct ToolFailureLoopGuardOptions {
    pub enabled: Option<bool>,
    pub threshold: usize,
    pub max_keys: usize,
}

impl Default for ToolFailureLoopGuardOptions {
    fn default() -> Self {
        Self {
            enabled: None,
            threshold: DEFAULT_TOOL_FAILURE_THRESHOLD,
            max_keys: DEFAULT_MAX_FAILURE_KEYS,
        }
    }
}

/// Reads `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` the same way TS
/// `isToolFailureLoopGuardEnabled` does. An explicit `enabled` option always
/// wins over the env (matching the TS constructor).
fn tool_failure_loop_guard_enabled_from_env() -> bool {
    is_truthy_env(
        std::env::var("LINGXIAO_TOOL_FAILURE_LOOP_GUARD")
            .ok()
            .as_deref(),
    )
}

#[derive(Debug, Default)]
pub struct ToolFailureLoopGuard {
    enabled: bool,
    threshold: usize,
    max_keys: usize,
    /// key → record. The key embeds toolName+argsHash+errorKind, so different
    /// args or different error kinds keep separate counts (TS parity).
    records: HashMap<String, ToolFailureRecord>,
}

impl ToolFailureLoopGuard {
    pub fn new(options: ToolFailureLoopGuardOptions) -> Self {
        Self {
            enabled: options
                .enabled
                .unwrap_or_else(tool_failure_loop_guard_enabled_from_env),
            threshold: options.threshold.max(2),
            max_keys: options.max_keys.max(16),
            records: HashMap::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Record one tool failure and return the decision. The caller invokes this
    /// on the failure path (`ToolResult.success == false`); when `tripped` is
    /// true it must surface a tripped recovery error to the LLM instead of the
    /// raw failure and stop retrying that toolCall locally.
    ///
    /// `error_code` is the best-effort typed code extracted from the failure
    /// text (Rust has no typed envelope, so callers may pass "" — the kind is
    /// still classified from `error_text`). `error_text` is the raw failure
    /// message used both for classification and the surfaced summary.
    pub fn record(
        &mut self,
        tool_name: &str,
        args: &Value,
        error_code: &str,
        error_text: &str,
    ) -> LoopGuardDecision {
        let error_kind = classify_tool_failure(error_text);
        let args_hash = failure_args_fingerprint(args);
        let signature = ToolFailureSignature {
            tool_name: tool_name.to_string(),
            args_hash: args_hash.clone(),
            error_kind,
            error_code: error_code.to_string(),
        };
        if !self.enabled {
            return LoopGuardDecision {
                tripped: false,
                count: 0,
                error_kind,
                signature,
                requires_escalation: false,
                just_tripped: false,
            };
        }
        let key = format!("{}::{}::{}", tool_name, args_hash, error_kind.as_str());

        // Already tripped: refresh the summary but do not accrue count, so a
        // looping LLM cannot inflate the counter after the trip (TS parity).
        if let Some(record) = self.records.get_mut(&key) {
            if record.tripped {
                record.last_error_message = if error_text.is_empty() {
                    record.last_error_message.clone()
                } else {
                    error_text.to_string()
                };
                return LoopGuardDecision {
                    tripped: true,
                    count: record.count,
                    error_kind,
                    signature,
                    requires_escalation: error_kind.is_state_error(),
                    just_tripped: false,
                };
            }
        }

        // Capacity guard: evict the oldest-seen key when at capacity (TS
        // `evictOldest` parity, approximated by last-write-order via a re-scan
        // since HashMap has no insertion order).
        if self.records.len() >= self.max_keys && !self.records.contains_key(&key) {
            self.evict_oldest();
        }

        // Snapshot the pre-record tripped state so `just_tripped` distinguishes
        // the first trip from repeat records against an already-tripped key.
        // (Already-tripped keys are handled by the early-return above, so this is
        // always false on the path that reaches here — but capturing it explicitly
        // keeps the invariant self-evident and robust to future reordering.)
        let was_tripped_before = self.records.get(&key).map(|r| r.tripped).unwrap_or(false);

        let record = self
            .records
            .entry(key.clone())
            .and_modify(|r| {
                r.count += 1;
                if !error_text.is_empty() {
                    r.last_error_message = error_text.to_string();
                }
            })
            .or_insert_with(|| ToolFailureRecord {
                tool_name: tool_name.to_string(),
                args_hash: args_hash.clone(),
                error_kind,
                error_code: error_code.to_string(),
                count: 1,
                last_error_message: error_text.to_string(),
                tripped: false,
            });

        let can_trip = !error_kind.is_non_tripping();
        let tripped = can_trip && record.count >= self.threshold;
        if tripped {
            record.tripped = true;
        }
        // `just_tripped` is true only on the record that flips the key to tripped
        // (mirrors TS `emitTripped` firing once, inside the first-trip `if (tripped)`
        // block — not on repeat records against the already-tripped key).
        let just_tripped = tripped && !was_tripped_before;

        LoopGuardDecision {
            tripped,
            count: record.count,
            error_kind,
            signature,
            requires_escalation: error_kind.is_state_error(),
            just_tripped,
        }
    }

    /// On a successful tool call, clear every failure record sharing this
    /// (toolName, argsHash) — across all error kinds — so a later different-
    /// kind failure is not wrongly merged into the old streak (TS
    /// `clearOnSuccess` parity).
    pub fn clear_on_success(&mut self, tool_name: &str, args: &Value) {
        let args_hash = failure_args_fingerprint(args);
        self.records
            .retain(|_, r| !(r.tool_name == tool_name && r.args_hash == args_hash));
    }

    /// Clear all failure records (agent termination / session end). TS
    /// `resetSession` parity.
    pub fn reset_session(&mut self) {
        self.records.clear();
    }

    /// Test/observation: number of tripped keys.
    pub fn count_tripped(&self) -> usize {
        self.records.values().filter(|r| r.tripped).count()
    }

    /// Test/observation: snapshot of current records.
    pub fn snapshot(&self) -> Vec<ToolFailureRecord> {
        self.records.values().cloned().collect()
    }

    fn evict_oldest(&mut self) {
        // HashMap has no insertion order; evict the record with the smallest
        // count (least evidence of a real loop) as a deterministic, low-risk
        // approximation of TS "evict oldest-seen".
        if let Some(key_to_evict) = self
            .records
            .iter()
            .min_by_key(|(_, r)| r.count)
            .map(|(k, _)| k.clone())
        {
            self.records.remove(&key_to_evict);
        }
    }
}

/// Format the tripped decision into the recovery error text surfaced to the
/// LLM. Mirrors TS `formatToolFailureLoopError`: a `TOOL_FAILURE_LOOP_TRIPPED`
/// banner with the count/kind, a state-class escalation hint or a non-state
/// retry hint, and an `LLM_RECOVERY` JSON block carrying the structured
/// failure-loop payload. The LLM reads this in place of the raw tool failure.
pub fn format_tool_failure_loop_error(tool_name: &str, decision: &LoopGuardDecision) -> String {
    let mut lines: Vec<String> = vec![format!(
        "TOOL_FAILURE_LOOP_TRIPPED: tool \"{}\" failed {} times in a row with the same args \
         + error kind ({}); the failure loop guard has tripped.",
        tool_name,
        decision.count,
        decision.error_kind.as_str()
    )];
    if decision.requires_escalation {
        lines.push(
            "This is a state-class error (permission / mode / write_scope / sandbox / network / \
             schema) — retrying will not change the outcome."
                .into(),
        );
        lines.push(
            "Next step: escalate to the leader / request a permission update; do not re-issue the \
             same call."
                .into(),
        );
    } else {
        lines.push(
            "This is not a state-class error, but the guard threshold was reached — do not keep \
             burning rounds on this tripped key."
                .into(),
        );
        lines.push("Next step: adjust the arguments and retry, or escalate to the leader.".into());
    }
    let banner = lines.join("\n");
    let payload = serde_json::json!({
        "code": "TOOL_FAILURE_LOOP_TRIPPED",
        "message": lines.join(" "),
        "retryable": false,
        "fix": "Do not retry the same toolName+args+errorKind combination. Escalate to leader or change strategy.",
        "failure_loop": {
            "toolName": decision.signature.tool_name,
            "argsHash": decision.signature.args_hash,
            "errorKind": decision.error_kind.as_str(),
            "errorCode": decision.signature.error_code,
            "count": decision.count,
            "requiresEscalation": decision.requires_escalation,
        }
    });
    format!("{banner}\n\nLLM_RECOVERY={payload}")
}

// ─────────────────────────────────────────────────────────────────────────────
// ToolFailureLoopEscalation — durable/canonical escalation signal (R-7 follow-up)
// ─────────────────────────────────────────────────────────────────────────────
//
// Mirrors the *signal* the TS `ToolFailureLoopGuard` produces on trip —
// `emitter.emit('agent:tool_failure_loop', event)` + a `tool_failure_loop_escalation`
// MessageBus message to the Leader (`LeaderPermissionManager.handleToolFailureLoopEscalation`).
// Rust `AgentLoop` runs in-process under the `AgentPool` (no worker bus), so the
// minimal verifiable equivalent is a canonical escalation *signal* the session /
// Leader layer can observe (via the durable `event_log` replay) or act on later —
// rather than only surfacing the recovery error to the LLM.
//
// The signal is emitted as an `AgentEvent::ToolFailureLoopEscalated` over the
// agent→supervisor mpsc channel; the `start_agent_pool_event_bridge` (command.rs)
// then writes it both to the `agent_logs` table (event_type
// `agent.tool_failure_loop_escalation`) and to the durable `event_log` as a
// canonical `EventEnvelope` (same event_type). That double-write reuses the
// existing persistence/emit paths — no new DB schema.
//
// Secret-safety: the payload carries the `argsHash` (the stable fingerprint, not
// the raw args) and the failure `errorKind`/`errorCode`/`lastErrorMessage`. It
// deliberately omits the raw tool `arguments` so an args-secret (e.g. a token in
// a shell command) can never leak into the durable escalation record. The bridge
// also runs `redact_persistence_secrets` over the payload before writing
// `event_log` as a defense-in-depth, exactly as it does for every other event.
//
// Scope (this slice): only a *state-class* trip (`decision.requires_escalation`
// true — permission/mode/write_scope/sandbox/network/schema) emits the
// escalation signal, matching the TS `STATE_ERROR_KINDS` ⇒ `requiresEscalation`
// contract that forces Leader handling. A non-state-class trip (e.g. timeout)
// still surfaces the `TOOL_FAILURE_LOOP_TRIPPED` recovery error to the LLM (R-7
// in-process behavior, unchanged) but does NOT emit a Leader escalation signal —
// mirroring TS, where the bus escalation is the state-error path and non-state
// trips are an LLM-facing hint only. A disabled guard emits nothing (byte-for-byte
// legacy behavior). Full `LeaderPermissionManager` parity (auto-approve/reject/
// interactive mode escalation *response*) is deferred: Rust has no Leader
// MessageBus, so the signal is durable-and-observable rather than
// handled-in-place; the matrix records this exact scope.

/// Canonical escalation payload emitted when the `ToolFailureLoopGuard` trips on
/// a state-class error. Serialized into the durable `event_log` payload; the raw
/// tool `arguments` are intentionally absent (only the stable `args_hash` is
/// carried) so args-secrets cannot leak into the durable record.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ToolFailureLoopEscalation {
    pub session_id: String,
    pub agent_id: String,
    pub agent_name: String,
    pub task_id: String,
    pub tool_name: String,
    pub args_hash: String,
    pub error_kind: ToolFailureErrorKind,
    pub error_code: String,
    pub count: usize,
    pub threshold: usize,
    pub requires_escalation: bool,
    pub last_error_message: String,
}

impl ToolFailureLoopEscalation {
    /// Build the escalation payload from a tripped decision. `last_error_message`
    /// is the raw failure text (pre-`LLM_RECOVERY` split, mirroring TS
    /// `recordToolFailure`); the bridge redacts it before persistence. Only call
    /// this when `decision.tripped && decision.requires_escalation`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_tripped(
        decision: &LoopGuardDecision,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        task_id: &str,
        tool_name: &str,
        threshold: usize,
        last_error_message: &str,
    ) -> Self {
        Self {
            session_id: session_id.to_string(),
            agent_id: agent_id.to_string(),
            agent_name: agent_name.to_string(),
            task_id: task_id.to_string(),
            tool_name: tool_name.to_string(),
            args_hash: decision.signature.args_hash.clone(),
            error_kind: decision.error_kind,
            error_code: decision.signature.error_code.clone(),
            count: decision.count,
            threshold,
            requires_escalation: decision.requires_escalation,
            last_error_message: last_error_message.to_string(),
        }
    }

    /// Whether this escalation warrants a Leader signal. Always true when built
    /// via `from_tripped` on a state-class trip; exposed for the caller's guard.
    pub fn warrants_signal(&self) -> bool {
        self.requires_escalation
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Escalation auto-response policy (R-7 deferred LeaderPermissionManager parity)
// ─────────────────────────────────────────────────────────────────────────────
//
// Mirrors the errorKind → action decision table the TS
// `LeaderPermissionManager.handleToolFailureLoopEscalation`
// (src/agents/LeaderPermissionManager.ts:199-225) applies when it receives the
// `tool_failure_loop_escalation` bus message:
//
//   permission | network  → 'approved'  (auto-escalate permission mode one tier,
//                                        then approve so the worker retries
//                                        under the widened mode; yolo stays yolo)
//   sandbox                → 'rejected'  (environment problem; Leader should take
//                                        a different path rather than auto-fix)
//   mode | write_scope | schema
//                          → 'rejected'  (retry is meaningless — the call needs a
//                                        mode change or different args, not a
//                                        re-issue)
//   execution | timeout | aborted | other | (default)
//                          → 'interactive' (non-state/transient; surface for human
//                                        or Leader-LLM judgement rather than
//                                        auto-acting)
//
// Rust `AgentLoop` runs in-process under the `AgentPool` (no Leader MessageBus),
// so the TS "respond by mutating live permission state and sending a
// permission_response to the worker" loop has no in-process analog here. The
// minimal verifiable equivalent is a *deterministic* decision record computed
// from the same policy table and persisted alongside the escalation signal — so
// an operator / Leader layer observing `event_log` can act on it, and the
// decision is auditable rather than implicit. This function is the pure,
// testable core of that record; `command.rs` persists its output as a durable
// canonical event + session_state row (actual live mode mutation is deferred —
// the mode-change command path is a `&self CommandRouter` method, not reachable
// from the bridge's `DbOwner` transaction; see the matrix R-7 row).

/// The auto-response action the TS `LeaderPermissionManager` would take for a
/// given escalation errorKind. Serialized into the durable decision record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EscalationAction {
    /// `permission`/`network`: auto-escalate the permission mode one tier and
    /// approve the retry (TS returns `'approved'` and bumps the mode to
    /// `networked`, or keeps `yolo`).
    Approved,
    /// `sandbox`/`mode`/`write_scope`/`schema`: reject — retrying cannot help
    /// (TS returns `'rejected'`).
    Rejected,
    /// `execution`/`timeout`/`aborted`/`other`/unknown: surface for human or
    /// Leader-LLM judgement (TS returns `'interactive'`).
    Interactive,
}

impl EscalationAction {
    fn as_decision_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Interactive => "interactive",
        }
    }
}

/// The deterministic auto-response decision for an escalation. Pure projection
/// of `error_kind` through the TS policy table — no DB, no I/O. Persisted by
/// the bridge as a durable canonical record.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct EscalationAutoResponse {
    /// The high-level action (approve/reject/interactive).
    pub action: EscalationAction,
    /// The TS return-value token: `'approved'` | `'rejected'` | `'interactive'`.
    pub decision: &'static str,
    /// Human-readable reason naming the policy branch that fired.
    pub reason: &'static str,
    /// The errorKind this decision was computed from (mirrors the escalation's
    /// `error_kind`).
    pub error_kind: ToolFailureErrorKind,
}

impl EscalationAutoResponse {
    /// The permission mode the TS handler would move *to* for an `approved`
    /// escalation. Mirrors
    /// `LeaderPermissionManager.handleToolFailureLoopEscalation`:
    /// `this.permissionContext.mode === 'yolo' ? 'yolo' : 'networked'`.
    /// Returns `None` for non-approve actions (no mode change).
    pub fn target_mode(&self, from_mode: &str) -> Option<String> {
        match self.action {
            EscalationAction::Approved => {
                if from_mode.eq_ignore_ascii_case("yolo") {
                    Some("yolo".to_string())
                } else {
                    Some("networked".to_string())
                }
            }
            EscalationAction::Rejected | EscalationAction::Interactive => None,
        }
    }
}

/// Compute the deterministic auto-response decision for an escalation errorKind.
/// Pure function — the TS `handleToolFailureLoopEscalation` switch statement
/// (LeaderPermissionManager.ts:199-225) with no bus/db side effects. The bridge
/// persists the result as a durable canonical record (actual live mode mutation
/// is deferred; see the matrix R-7 row).
pub fn escalation_auto_response(error_kind: ToolFailureErrorKind) -> EscalationAutoResponse {
    match error_kind {
        // permission/network: auto-escalate mode + approve the retry.
        ToolFailureErrorKind::Permission | ToolFailureErrorKind::Network => {
            EscalationAutoResponse {
                action: EscalationAction::Approved,
                decision: EscalationAction::Approved.as_decision_str(),
                reason:
                    "permission/network failure: auto-escalate permission mode and approve retry",
                error_kind,
            }
        }
        // sandbox: environment problem; reject so the Leader takes a new path.
        ToolFailureErrorKind::Sandbox => EscalationAutoResponse {
            action: EscalationAction::Rejected,
            decision: EscalationAction::Rejected.as_decision_str(),
            reason: "sandbox failure: reject; environment issue, retry cannot fix",
            error_kind,
        },
        // mode/write_scope/schema: retry is meaningless (needs mode change or
        // different args), so reject.
        ToolFailureErrorKind::Mode
        | ToolFailureErrorKind::WriteScope
        | ToolFailureErrorKind::Schema => EscalationAutoResponse {
            action: EscalationAction::Rejected,
            decision: EscalationAction::Rejected.as_decision_str(),
            reason:
                "mode/write_scope/schema failure: reject; retry needs mode change or different args",
            error_kind,
        },
        // execution/timeout/aborted/other/precondition (non-state, transient, or
        // guided): interactive — surface for human / Leader-LLM judgement.
        ToolFailureErrorKind::Execution
        | ToolFailureErrorKind::Timeout
        | ToolFailureErrorKind::Aborted
        | ToolFailureErrorKind::Precondition
        | ToolFailureErrorKind::Other => EscalationAutoResponse {
            action: EscalationAction::Interactive,
            decision: EscalationAction::Interactive.as_decision_str(),
            reason:
                "non-state/transient failure: interactive; surface for human or Leader judgement",
            error_kind,
        },
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AgentStatus state machine
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentStatus {
    Starting,
    Running,
    Stopped,
}

impl AgentStatus {
    pub fn is_terminal(self) -> bool {
        self == Self::Stopped
    }

    pub fn can_transition_to(self, to: Self) -> bool {
        use AgentStatus::*;
        matches!(
            (self, to),
            (Starting, Running | Stopped) | (Running, Stopped) | (Stopped, Starting)
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Messages the agent loop sends back to the supervisor
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum AgentEvent {
    Started {
        agent_id: String,
    },
    ToolCallInitiated {
        agent_id: String,
        tool_call: ToolCall,
    },
    ToolCallCompleted {
        agent_id: String,
        tool_call_id: String,
        result: Value,
    },
    LlmRoundCompleted {
        agent_id: String,
        assistant_message: String,
    },
    /// R-7 follow-up: the `ToolFailureLoopGuard` tripped on a *state-class*
    /// error. The bridge writes this as a durable/canonical escalation signal
    /// (agent_logs + event_log) so the session / Leader layer can observe and
    /// later act on it, instead of only surfacing the recovery error to the LLM.
    /// Emitted only when `decision.tripped && decision.requires_escalation`.
    ToolFailureLoopEscalated {
        agent_id: String,
        escalation: ToolFailureLoopEscalation,
    },
    Completed {
        agent_id: String,
        result: Value,
    },
    Crashed {
        agent_id: String,
        error: String,
    },
    Heartbeat {
        agent_id: String,
        at_ms: u64,
    },
}

/// Instructions sent into a running agent loop.
#[derive(Debug)]
pub enum AgentCommand {
    Cancel,
    Interrupt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContextMessage {
    pub role: String,
    pub content: String,
    pub tool_call_id: Option<String>,
}

pub trait AgentContextStore: Send + Sync {
    fn load_messages(&self, session_id: &str, agent_id: &str) -> Vec<AgentContextMessage>;

    fn append_message(
        &self,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        message: &AgentContextMessage,
    );

    fn save_active_projection(
        &self,
        session_id: &str,
        agent_id: &str,
        original_message_count: usize,
        active_message_count: usize,
    );
}

pub trait AgentToolExecutor: Send + Sync {
    fn execute_tool(&self, session_id: &str, agent_id: &str, tool_call: &ToolCall) -> ToolResult;
}

pub trait AgentLlmExecutor: Send + Sync {
    fn stream_llm(
        &self,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError>;

    fn execute_llm(
        &self,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        request: GenerateRequest,
    ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
        let mut events = Vec::new();
        self.stream_llm(session_id, agent_id, agent_name, request, &mut |event| {
            events.push(event);
            Ok(())
        })?;
        Ok(events)
    }
}

#[cfg(test)]
pub struct DirectAgentToolExecutor {
    registry: Arc<ToolRegistry>,
}

#[cfg(test)]
impl DirectAgentToolExecutor {
    pub fn new(registry: Arc<ToolRegistry>) -> Self {
        Self { registry }
    }
}

#[cfg(test)]
impl AgentToolExecutor for DirectAgentToolExecutor {
    fn execute_tool(&self, _session_id: &str, _agent_id: &str, tool_call: &ToolCall) -> ToolResult {
        self.registry.execute(&tool_call.name, &tool_call.arguments)
    }
}

#[cfg(test)]
pub struct DirectAgentLlmExecutor {
    router: Arc<LlmRouter>,
}

#[cfg(test)]
impl DirectAgentLlmExecutor {
    pub fn new(router: Arc<LlmRouter>) -> Self {
        Self { router }
    }
}

#[cfg(test)]
impl AgentLlmExecutor for DirectAgentLlmExecutor {
    fn stream_llm(
        &self,
        _session_id: &str,
        _agent_id: &str,
        _agent_name: &str,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError> {
        self.router.route_stream_with_sink(request, sink)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AgentLoop — the inner LLM-round-executor
// ─────────────────────────────────────────────────────────────────────────────

/// Configuration for one agent execution.
pub struct AgentConfig {
    pub agent_id: String,
    pub session_id: String,
    pub task_id: String,
    pub task_content: String,
    pub model: String,
    pub auth_context: AuthContext,
    pub max_rounds: u32,
    pub round_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub context_retain_last: usize,
    pub agent_name: String,
    pub context_store: Option<Arc<dyn AgentContextStore>>,
    pub request_options: RequestOptions,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            session_id: String::new(),
            task_id: String::new(),
            task_content: String::new(),
            model: "mock/model".into(),
            auth_context: AuthContext::None,
            max_rounds: 20,
            round_timeout_ms: 60_000,
            heartbeat_interval_ms: 5_000,
            context_retain_last: 20,
            agent_name: String::new(),
            context_store: None,
            request_options: RequestOptions::default(),
        }
    }
}

/// Runs the agent's LLM loop in a dedicated thread.
pub struct AgentLoop {
    config: AgentConfig,
    tool_registry: Arc<ToolRegistry>,
    llm_executor: Arc<dyn AgentLlmExecutor>,
    tool_executor: Arc<dyn AgentToolExecutor>,
    event_tx: Sender<AgentEvent>,
    cmd_rx: Receiver<AgentCommand>,
    tool_loop_detector: ToolLoopDetector,
    tool_failure_loop_guard: ToolFailureLoopGuard,
}

impl AgentLoop {
    pub fn new(
        config: AgentConfig,
        tool_registry: Arc<ToolRegistry>,
        llm_executor: Arc<dyn AgentLlmExecutor>,
        tool_executor: Arc<dyn AgentToolExecutor>,
        event_tx: Sender<AgentEvent>,
        cmd_rx: Receiver<AgentCommand>,
    ) -> Self {
        Self {
            config,
            tool_registry,
            llm_executor,
            tool_executor,
            event_tx,
            cmd_rx,
            tool_loop_detector: ToolLoopDetector::new(ToolLoopDetectorOptions::default()),
            tool_failure_loop_guard: ToolFailureLoopGuard::new(
                ToolFailureLoopGuardOptions::default(),
            ),
        }
    }

    /// Replace the tool-loop probe. Production uses the default (env-gated)
    /// detector; tests inject an explicitly-enabled one so behavior does not
    /// depend on `LINGXIAO_TOOL_LOOP_DETECTOR` being set in the environment.
    pub fn with_tool_loop_detector(mut self, detector: ToolLoopDetector) -> Self {
        self.tool_loop_detector = detector;
        self
    }

    /// Replace the tool-failure loop guard. Production uses the default
    /// (env-gated) guard; tests inject an explicitly-enabled one so behavior
    /// does not depend on `LINGXIAO_TOOL_FAILURE_LOOP_GUARD` being set.
    pub fn with_tool_failure_loop_guard(mut self, guard: ToolFailureLoopGuard) -> Self {
        self.tool_failure_loop_guard = guard;
        self
    }

    /// Run the agent until completion, cancellation, or max_rounds.
    pub fn run(mut self) {
        let _ = self.event_tx.send(AgentEvent::Started {
            agent_id: self.config.agent_id.clone(),
        });

        let mut context = ContextManager::new();
        let agent_name = if self.config.agent_name.is_empty() {
            self.config.agent_id.clone()
        } else {
            self.config.agent_name.clone()
        };
        if let Some(store) = &self.config.context_store {
            for message in store.load_messages(&self.config.session_id, &self.config.agent_id) {
                context.append(message.role, message.content, message.tool_call_id);
            }
        }
        if context.replay().is_empty() {
            let message = AgentContextMessage {
                role: "user".into(),
                content: self.config.task_content.clone(),
                tool_call_id: None,
            };
            context.append(
                message.role.clone(),
                message.content.clone(),
                message.tool_call_id.clone(),
            );
            if let Some(store) = &self.config.context_store {
                store.append_message(
                    &self.config.session_id,
                    &self.config.agent_id,
                    &agent_name,
                    &message,
                );
            }
        }

        let mut tool_call_history: Vec<ToolCall> = Vec::new();
        let mut last_heartbeat = Instant::now();
        let heartbeat_interval = Duration::from_millis(self.config.heartbeat_interval_ms);

        for round in 0..self.config.max_rounds {
            // Check for incoming commands (cancel/interrupt)
            match self.cmd_rx.try_recv() {
                Ok(AgentCommand::Cancel) | Ok(AgentCommand::Interrupt) => {
                    let _ = self.event_tx.send(AgentEvent::Crashed {
                        agent_id: self.config.agent_id.clone(),
                        error: "Cancelled by command".into(),
                    });
                    return;
                }
                Err(_) => {}
            }

            // Heartbeat
            if last_heartbeat.elapsed() >= heartbeat_interval {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let _ = self.event_tx.send(AgentEvent::Heartbeat {
                    agent_id: self.config.agent_id.clone(),
                    at_ms: now_ms,
                });
                last_heartbeat = Instant::now();
            }

            // Build LLM request
            let mut messages = Vec::new();
            for message in context.active_projection(self.config.context_retain_last) {
                if message.role == "tool" {
                    if let Some(tool_call_id) = &message.tool_call_id {
                        if let Some(call) = tool_call_history
                            .iter()
                            .find(|call| &call.id == tool_call_id)
                        {
                            messages.push(Message {
                                role: "assistant".into(),
                                content: String::new(),
                                tool_calls: vec![call.clone()],
                                ..Default::default()
                            });
                        }
                    }
                }
                messages.push(Message {
                    role: message.role,
                    content: message.content,
                    tool_call_id: message.tool_call_id.clone(),
                    name: message.tool_call_id.as_ref().and_then(|tool_call_id| {
                        tool_call_history
                            .iter()
                            .find(|call| &call.id == tool_call_id)
                            .map(|call| call.name.clone())
                    }),
                    ..Default::default()
                });
            }
            if let Some(store) = &self.config.context_store {
                store.save_active_projection(
                    &self.config.session_id,
                    &self.config.agent_id,
                    context.replay().len(),
                    messages.len(),
                );
            }
            let request = GenerateRequest {
                model: self.config.model.clone(),
                messages,
                tools: self.tool_registry.llm_tool_definitions(),
                stream: true,
                auth_context: self.config.auth_context.clone(),
                options: {
                    let mut options = self.config.request_options.clone();
                    if options.timeout_ms_hint.is_none() {
                        options.timeout_ms_hint = Some(self.config.round_timeout_ms);
                    }
                    options
                },
            };

            // Call LLM and process stream events as they arrive.
            let mut assistant_text = String::new();
            let mut tool_calls: Vec<ToolCall> = Vec::new();
            let mut tool_call_accumulator = ToolCallAccumulator::new();
            let mut completed = false;

            let stream_result = self.llm_executor.stream_llm(
                &self.config.session_id,
                &self.config.agent_id,
                &agent_name,
                request,
                &mut |event| {
                    match event {
                        Ok(StreamEvent::TextDelta(text)) => assistant_text.push_str(&text),
                        Ok(StreamEvent::ToolCall(tc)) => tool_calls.push(tc),
                        Ok(StreamEvent::ToolCallDelta(delta)) => {
                            tool_call_accumulator.append(delta);
                        }
                        Ok(StreamEvent::Finished(reason)) => {
                            if matches!(reason, FinishReason::ToolCalls) {
                                tool_calls.extend(tool_call_accumulator.finalize());
                            }
                        }
                        Ok(StreamEvent::ThinkingDelta(_)) | Ok(StreamEvent::Usage(_)) => {}
                        Ok(StreamEvent::Error(e)) => return Err(e),
                        Err(e) => {
                            return Err(e);
                        }
                    }
                    Ok(())
                },
            );
            if let Err(e) = stream_result {
                let _ = self.event_tx.send(AgentEvent::Crashed {
                    agent_id: self.config.agent_id.clone(),
                    error: format!("LLM stream error on round {round}: {}", e.message),
                });
                return;
            }
            tool_calls.extend(tool_call_accumulator.finalize());
            dedupe_tool_calls(&mut tool_calls);

            let _ = self.event_tx.send(AgentEvent::LlmRoundCompleted {
                agent_id: self.config.agent_id.clone(),
                assistant_message: assistant_text.clone(),
            });

            // Add assistant message to conversation
            context.append("assistant", assistant_text.clone(), None);
            if let Some(store) = &self.config.context_store {
                store.append_message(
                    &self.config.session_id,
                    &self.config.agent_id,
                    &agent_name,
                    &AgentContextMessage {
                        role: "assistant".into(),
                        content: assistant_text.clone(),
                        tool_call_id: None,
                    },
                );
            }

            // Execute tool calls
            if tool_calls.is_empty() {
                // No tool calls — agent is done (attempt_completion path)
                let _ = self.event_tx.send(AgentEvent::Completed {
                    agent_id: self.config.agent_id.clone(),
                    result: json!({
                        "answer": assistant_text,
                        "rounds": round + 1,
                    }),
                });
                return;
            }

            // ── Tool-loop probe (TS `ToolLoopDetector` parity) ──
            // Observe this round's tool-call set; if the agent is stuck
            // re-issuing the *same* name+args across consecutive rounds, inject
            // a recovery system prompt and skip the round instead of executing
            // a call that will yield the same observation again. Disabled by
            // default; gated on `LINGXIAO_TOOL_LOOP_DETECTOR`.
            self.tool_loop_detector.observe(&tool_calls);
            if self.tool_loop_detector.is_looping() {
                let streak = self.tool_loop_detector.consecutive_count();
                let sig = self
                    .tool_loop_detector
                    .current_signature()
                    .unwrap_or("<unknown>")
                    .to_string();
                let tool_name = sig.split("::").next().unwrap_or("<unknown>");
                let recovery = format!(
                    "⚠️ [tool-loop guard] You have called `{tool_name}` with the exact same \
                     arguments {streak} times in a row, which usually means the strategy is \
                     stuck. Try: 1) change the arguments (different path/pattern/query), \
                     2) switch to a different tool, or 3) conclude with what you already know. \
                     Next call must use new arguments, a different tool, or produce a final answer.",
                );
                context.append("system", recovery.clone(), None);
                if let Some(store) = &self.config.context_store {
                    store.append_message(
                        &self.config.session_id,
                        &self.config.agent_id,
                        &agent_name,
                        &AgentContextMessage {
                            role: "system".into(),
                            content: recovery,
                            tool_call_id: None,
                        },
                    );
                }
                self.tool_loop_detector.reset();
                continue;
            }

            for tc in &tool_calls {
                tool_call_history.push(tc.clone());
                let _ = self.event_tx.send(AgentEvent::ToolCallInitiated {
                    agent_id: self.config.agent_id.clone(),
                    tool_call: tc.clone(),
                });

                // Check for attempt_completion
                if tc.name == "attempt_completion" {
                    completed = true;
                    let result = tc
                        .arguments
                        .get("result")
                        .cloned()
                        .unwrap_or_else(|| json!(assistant_text));
                    let _ = self.event_tx.send(AgentEvent::Completed {
                        agent_id: self.config.agent_id.clone(),
                        result,
                    });
                    break;
                }

                let tool_result = self.tool_executor.execute_tool(
                    &self.config.session_id,
                    &self.config.agent_id,
                    tc,
                );
                let result_value = if tool_result.success {
                    // ── Tool-failure loop guard: success clears the streak for
                    // this (toolName, argsHash) so a later different-kind
                    // failure is not merged into the old one (TS
                    // `clearOnSuccess` parity). Disabled guard is a no-op.
                    self.tool_failure_loop_guard
                        .clear_on_success(&tc.name, &tc.arguments);
                    tool_result.output
                } else {
                    // ── Tool-failure loop guard (TS `ToolFailureLoopGuard`
                    // parity, in-process slice). Record the failure; if the
                    // same toolName+args+errorKind has now failed `threshold`
                    // times in a row, surface a `TOOL_FAILURE_LOOP_TRIPPED`
                    // recovery error to the LLM instead of the raw failure —
                    // prompting a strategy change rather than another identical
                    // retry that will fail the same way. Disabled guard records
                    // nothing and never trips, so the raw failure is surfaced
                    // unchanged (byte-for-byte legacy behavior).
                    //
                    // R-7 follow-up — Leader-bus escalation: when the trip is a
                    // *state-class* error (`requires_escalation`), additionally
                    // emit a durable/canonical escalation signal so the session
                    // / Leader layer can observe it (the bridge writes it to
                    // `agent_logs` + the durable `event_log`). Rust `AgentLoop`
                    // runs in-process under the `AgentPool` (no worker bus), so
                    // this signal is the minimal verifiable equivalent of the
                    // TS `agent:tool_failure_loop` event +
                    // `tool_failure_loop_escalation` MessageBus message — durable
                    // and observable rather than handled in place. Non-state
                    // trips (e.g. timeout) surface the recovery error only.
                    let error_text = tool_result.error.clone().unwrap_or_default();
                    let decision = self.tool_failure_loop_guard.record(
                        &tc.name,
                        &tc.arguments,
                        "",
                        &error_text,
                    );
                    if decision.tripped {
                        // Strip the LLM_RECOVERY trailer from the summary that
                        // goes into the durable record (mirrors TS
                        // `recordToolFailure` splitting on `LLM_RECOVERY=`).
                        let last_error_message =
                            error_text.split("\n\nLLM_RECOVERY=").next().unwrap_or("");
                        // Emit the escalation signal only on the *first* trip for
                        // this key (`just_tripped`), so a looping LLM cannot
                        // spam the durable event_log with duplicate escalation
                        // rows — mirroring TS `emitTripped`, which fires once
                        // inside the first-trip block (not the already-tripped
                        // branch). Only state-class trips escalate.
                        if decision.just_tripped && decision.requires_escalation {
                            let escalation = ToolFailureLoopEscalation::from_tripped(
                                &decision,
                                &self.config.session_id,
                                &self.config.agent_id,
                                &agent_name,
                                &self.config.task_id,
                                &tc.name,
                                self.tool_failure_loop_guard.threshold(),
                                last_error_message,
                            );
                            let _ = self.event_tx.send(AgentEvent::ToolFailureLoopEscalated {
                                agent_id: self.config.agent_id.clone(),
                                escalation,
                            });
                        }
                        json!({
                            "error": format_tool_failure_loop_error(&tc.name, &decision)
                        })
                    } else {
                        json!({"error": tool_result.error})
                    }
                };

                let _ = self.event_tx.send(AgentEvent::ToolCallCompleted {
                    agent_id: self.config.agent_id.clone(),
                    tool_call_id: tc.id.clone(),
                    result: result_value.clone(),
                });

                // Add tool result to conversation
                context.append("tool", result_value.to_string(), Some(tc.id.clone()));
                if let Some(store) = &self.config.context_store {
                    store.append_message(
                        &self.config.session_id,
                        &self.config.agent_id,
                        &agent_name,
                        &AgentContextMessage {
                            role: "tool".into(),
                            content: result_value.to_string(),
                            tool_call_id: Some(tc.id.clone()),
                        },
                    );
                }
            }

            if completed {
                return;
            }
        }

        // Max rounds exhausted without completion
        let _ = self.event_tx.send(AgentEvent::Completed {
            agent_id: self.config.agent_id.clone(),
            result: json!({"error": "max_rounds_exceeded"}),
        });
    }
}

fn dedupe_tool_calls(tool_calls: &mut Vec<ToolCall>) {
    let mut seen = std::collections::HashSet::new();
    tool_calls.retain(|call| seen.insert(call.id.clone()));
}

// ─────────────────────────────────────────────────────────────────────────────
// AgentPool — manages spawned agent threads
// ─────────────────────────────────────────────────────────────────────────────

struct AgentHandle {
    cmd_tx: Sender<AgentCommand>,
    last_heartbeat_ms: u64,
}

pub struct AgentPool {
    agents: Arc<Mutex<HashMap<String, AgentHandle>>>,
    event_tx: Sender<AgentEvent>,
    max_parallel: usize,
}

pub struct HeartbeatMonitor {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HeartbeatMonitor {
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for HeartbeatMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl AgentPool {
    pub fn new(event_tx: Sender<AgentEvent>) -> Self {
        Self::with_max_parallel(event_tx, usize::MAX)
    }

    pub fn with_max_parallel(event_tx: Sender<AgentEvent>, max_parallel: usize) -> Self {
        Self {
            agents: Arc::new(Mutex::new(HashMap::new())),
            event_tx,
            max_parallel,
        }
    }

    /// Spawn a new agent thread. Returns error if agent_id already active.
    pub fn spawn(
        &self,
        config: AgentConfig,
        tool_registry: Arc<ToolRegistry>,
        llm_executor: Arc<dyn AgentLlmExecutor>,
        tool_executor: Arc<dyn AgentToolExecutor>,
    ) -> Result<(), String> {
        let mut agents = self.agents.lock().unwrap();
        if agents.contains_key(&config.agent_id) {
            return Err(format!("Agent already active: {}", config.agent_id));
        }
        if agents.len() >= self.max_parallel {
            return Err(format!(
                "Max parallel agents exceeded: requested {}, available {}",
                agents.len() + 1,
                self.max_parallel
            ));
        }

        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let (loop_event_tx, loop_event_rx) = std::sync::mpsc::channel();
        let external_event_tx = self.event_tx.clone();
        let agents_for_supervisor = Arc::clone(&self.agents);
        let agent_id = config.agent_id.clone();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        agents.insert(
            agent_id.clone(),
            AgentHandle {
                cmd_tx,
                last_heartbeat_ms: now_ms,
            },
        );
        drop(agents);

        std::thread::Builder::new()
            .name(format!("agent-supervisor-{agent_id}"))
            .spawn(move || {
                for event in loop_event_rx {
                    match &event {
                        AgentEvent::Heartbeat { agent_id, at_ms } => {
                            if let Some(handle) =
                                agents_for_supervisor.lock().unwrap().get_mut(agent_id)
                            {
                                handle.last_heartbeat_ms = *at_ms;
                            }
                        }
                        AgentEvent::Completed { agent_id, .. }
                        | AgentEvent::Crashed { agent_id, .. } => {
                            agents_for_supervisor.lock().unwrap().remove(agent_id);
                        }
                        _ => {}
                    }
                    let is_terminal = matches!(
                        event,
                        AgentEvent::Completed { .. } | AgentEvent::Crashed { .. }
                    );
                    let _ = external_event_tx.send(event);
                    if is_terminal {
                        break;
                    }
                }
            })
            .map_err(|e| format!("Failed to spawn agent supervisor thread: {e}"))?;

        // Spawn OS thread for agent loop
        std::thread::Builder::new()
            .name(format!("agent-{agent_id}"))
            .spawn(move || {
                let agent_loop = AgentLoop::new(
                    config,
                    tool_registry,
                    llm_executor,
                    tool_executor,
                    loop_event_tx,
                    cmd_rx,
                );
                agent_loop.run();
            })
            .map_err(|e| format!("Failed to spawn agent thread: {e}"))?;

        Ok(())
    }

    /// Send a cancel command to a running agent.
    pub fn cancel(&self, agent_id: &str) -> bool {
        let agents = self.agents.lock().unwrap();
        if let Some(handle) = agents.get(agent_id) {
            let _ = handle.cmd_tx.send(AgentCommand::Cancel);
            true
        } else {
            false
        }
    }

    /// Remove a finished agent from the pool.
    pub fn remove(&self, agent_id: &str) {
        self.agents.lock().unwrap().remove(agent_id);
    }

    /// Update heartbeat timestamp for a running agent.
    pub fn record_heartbeat(&self, agent_id: &str, at_ms: u64) {
        if let Some(handle) = self.agents.lock().unwrap().get_mut(agent_id) {
            handle.last_heartbeat_ms = at_ms;
        }
    }

    /// Return agent_ids whose last heartbeat is older than `timeout_ms`.
    pub fn stale_agents(&self, timeout_ms: u64) -> Vec<String> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.agents
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, h)| now_ms.saturating_sub(h.last_heartbeat_ms) > timeout_ms)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn active_count(&self) -> usize {
        self.agents.lock().unwrap().len()
    }

    pub fn start_heartbeat_monitor(&self, timeout_ms: u64, interval_ms: u64) -> HeartbeatMonitor {
        let agents = Arc::clone(&self.agents);
        let event_tx = self.event_tx.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("agent-heartbeat-monitor".into())
            .spawn(move || {
                while !stop_thread.load(Ordering::SeqCst) {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let stale = {
                        let mut guard = agents.lock().unwrap();
                        let stale_ids: Vec<String> = guard
                            .iter()
                            .filter(|(_, handle)| {
                                now_ms.saturating_sub(handle.last_heartbeat_ms) > timeout_ms
                            })
                            .map(|(id, _)| id.clone())
                            .collect();
                        stale_ids
                            .into_iter()
                            .filter_map(|id| guard.remove(&id).map(|handle| (id, handle.cmd_tx)))
                            .collect::<Vec<_>>()
                    };
                    for (agent_id, cmd_tx) in stale {
                        let _ = cmd_tx.send(AgentCommand::Cancel);
                        let _ = event_tx.send(AgentEvent::Crashed {
                            agent_id,
                            error: format!("heartbeat stale for more than {timeout_ms}ms"),
                        });
                    }
                    std::thread::sleep(Duration::from_millis(interval_ms.max(1)));
                }
            })
            .expect("failed to spawn agent heartbeat monitor");
        HeartbeatMonitor {
            stop,
            handle: Some(handle),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{
        FinishReason, GenerateResponse, LlmProvider, MockLlmProvider, ProviderError,
        ProviderRegistry, TokenUsage, ToolDefinition,
    };
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_router_with_mock() -> Arc<LlmRouter> {
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(MockLlmProvider::new("mock")));
        Arc::new(LlmRouter::new(registry))
    }

    #[derive(Debug)]
    struct ToolThenFinalProvider {
        path: String,
        calls: AtomicUsize,
    }

    impl LlmProvider for ToolThenFinalProvider {
        fn provider_id(&self) -> &'static str {
            "tool-then-final"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "tool/model"
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "final answer".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    reasoning_tokens: None,
                },
            })
        }

        fn generate_stream(
            &self,
            _request: GenerateRequest,
        ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Ok(vec![
                    Ok(StreamEvent::ToolCall(ToolCall {
                        id: "read-call".into(),
                        name: "file_read".into(),
                        arguments: json!({"path": self.path}),
                    })),
                    Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
                ]);
            }
            Ok(vec![
                Ok(StreamEvent::TextDelta("verified file content".into())),
                Ok(StreamEvent::Finished(FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct CapturingProvider {
        calls: AtomicUsize,
        seen: Mutex<Vec<Vec<Message>>>,
        seen_tools: Mutex<Vec<Vec<ToolDefinition>>>,
    }

    impl LlmProvider for CapturingProvider {
        fn provider_id(&self) -> &'static str {
            "capturing"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "capturing/model"
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "done".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    reasoning_tokens: None,
                },
            })
        }

        fn generate_stream(
            &self,
            request: GenerateRequest,
        ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
            self.seen.lock().unwrap().push(request.messages);
            self.seen_tools.lock().unwrap().push(request.tools);
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Ok(vec![
                    Ok(StreamEvent::TextDelta("need observation".into())),
                    Ok(StreamEvent::ToolCall(ToolCall {
                        id: "observe-call".into(),
                        name: "list_dir".into(),
                        arguments: json!({"path": std::env::temp_dir().to_string_lossy()}),
                    })),
                    Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
                ]);
            }
            Ok(vec![
                Ok(StreamEvent::TextDelta("done".into())),
                Ok(StreamEvent::Finished(FinishReason::Stop)),
            ])
        }
    }

    #[test]
    fn test_agent_transitions() {
        assert!(AgentStatus::Starting.can_transition_to(AgentStatus::Running));
        assert!(AgentStatus::Running.can_transition_to(AgentStatus::Stopped));
        assert!(AgentStatus::Stopped.can_transition_to(AgentStatus::Starting));
        assert!(!AgentStatus::Running.can_transition_to(AgentStatus::Starting));
    }

    #[test]
    fn test_p4_agent_loop_completes() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx; // keep sender alive

        let config = AgentConfig {
            agent_id: "agent-1".into(),
            session_id: "sess-1".into(),
            task_id: "task-1".into(),
            task_content: "Write a hello world".into(),
            model: "mock/model".into(),
            ..Default::default()
        };

        let llm = make_router_with_mock();
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));
        let agent = AgentLoop::new(config, tools, llm_executor, executor, event_tx, cmd_rx);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::Started { .. })),
            "Expected Started event"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::Completed { .. })),
            "Expected Completed event"
        );
    }

    #[test]
    fn test_p4_agent_loop_tool_observe_final_e2e() {
        let path = std::env::temp_dir().join("lingxiao_agent_loop_e2e.txt");
        fs::write(&path, "agent observed this").unwrap();
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolThenFinalProvider {
            path: path.to_string_lossy().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-e2e".into(),
                session_id: "sess-e2e".into(),
                task_id: "task-e2e".into(),
                task_content: "Read the file and report.".into(),
                model: "tool/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        );
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolCallInitiated { tool_call, .. } if tool_call.name == "file_read"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolCallCompleted { result, .. }
                if result["content"] == "agent observed this"
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::Completed { result, .. }
                if result["answer"] == "verified file content"
        )));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_p4_agent_loop_uses_active_context_projection() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let provider = Arc::new(CapturingProvider {
            calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
            seen_tools: Mutex::new(Vec::new()),
        });
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-context".into(),
                session_id: "sess-context".into(),
                task_id: "task-context".into(),
                task_content: "original user fact".into(),
                model: "capturing/model".into(),
                max_rounds: 2,
                context_retain_last: 1,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        );
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        assert!(events
            .iter()
            .any(|event| matches!(event, AgentEvent::Completed { .. })));
        let seen = provider.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].len(), 1);
        assert_eq!(seen[0][0].content, "original user fact");
        assert_eq!(seen[1].len(), 3);
        assert_eq!(seen[1][0].role, "system");
        assert!(seen[1][0].content.contains("original user fact"));
        assert!(seen[1][0].content.contains("need observation"));
        assert_eq!(seen[1][1].role, "assistant");
        assert_eq!(seen[1][1].tool_calls[0].name, "list_dir");
        assert_eq!(seen[1][2].role, "tool");
        drop(seen);

        let seen_tools = provider.seen_tools.lock().unwrap();
        assert_eq!(seen_tools.len(), 2);
        let tool_names = seen_tools[0]
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>();
        assert!(tool_names.contains(&"file_read"));
        assert!(tool_names.contains(&"attempt_completion"));
    }

    #[test]
    fn test_p4_agent_pool_spawn_and_cancel() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let pool = AgentPool::new(event_tx);

        let llm = make_router_with_mock();
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));

        let config = AgentConfig {
            agent_id: "pool-agent-1".into(),
            session_id: "sess-1".into(),
            task_id: "task-1".into(),
            task_content: "Do something".into(),
            model: "mock/model".into(),
            ..Default::default()
        };

        pool.spawn(config, tools, llm_executor, executor).unwrap();
        assert_eq!(pool.active_count(), 1);

        // Duplicate spawn should fail
        let config2 = AgentConfig {
            agent_id: "pool-agent-1".into(),
            ..Default::default()
        };
        let tools2 = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor2 = Arc::new(DirectAgentLlmExecutor::new(make_router_with_mock()));
        let result = pool.spawn(
            config2,
            Arc::clone(&tools2),
            llm_executor2,
            Arc::new(DirectAgentToolExecutor::new(tools2)),
        );
        assert!(result.is_err());

        // Give it a moment to run
        std::thread::sleep(Duration::from_millis(200));

        // Check events arrived
        let events: Vec<_> = event_rx.try_iter().collect();
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::Started { .. })),
            "Expected Started event from pool"
        );
    }

    #[test]
    fn test_p4_agent_pool_removes_completed_agents() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let pool = AgentPool::new(event_tx);

        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(make_router_with_mock()));
        pool.spawn(
            AgentConfig {
                agent_id: "pool-agent-complete".into(),
                session_id: "sess-1".into(),
                task_id: "task-1".into(),
                task_content: "Complete".into(),
                model: "mock/model".into(),
                ..Default::default()
            },
            Arc::clone(&tools),
            llm_executor,
            Arc::new(DirectAgentToolExecutor::new(tools)),
        )
        .unwrap();

        let mut saw_completed = false;
        for _ in 0..20 {
            if event_rx
                .try_iter()
                .any(|e| matches!(e, AgentEvent::Completed { .. }))
            {
                saw_completed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        assert!(saw_completed, "Expected completed event from agent pool");
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_p4_agent_pool_rejects_over_max_parallel() {
        let (event_tx, _event_rx) = std::sync::mpsc::channel();
        let pool = AgentPool::with_max_parallel(event_tx, 0);

        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(make_router_with_mock()));
        let result = pool.spawn(
            AgentConfig {
                agent_id: "pool-agent-over-capacity".into(),
                session_id: "sess-1".into(),
                task_id: "task-1".into(),
                task_content: "Do something".into(),
                model: "mock/model".into(),
                ..Default::default()
            },
            Arc::clone(&tools),
            llm_executor,
            Arc::new(DirectAgentToolExecutor::new(tools)),
        );

        assert!(result.is_err());
        assert_eq!(pool.active_count(), 0);
    }

    #[test]
    fn test_p4_agent_heartbeat_staleness() {
        let (event_tx, _event_rx) = std::sync::mpsc::channel();
        let pool = AgentPool::new(event_tx);

        // Manually insert a stale agent
        {
            let (cmd_tx, _cmd_rx) = std::sync::mpsc::channel();
            pool.agents.lock().unwrap().insert(
                "stale-agent".into(),
                AgentHandle {
                    cmd_tx,
                    last_heartbeat_ms: 0, // epoch — very stale
                },
            );
        }

        let stale = pool.stale_agents(5_000);
        assert_eq!(stale, vec!["stale-agent".to_string()]);
    }

    #[test]
    fn test_p4_agent_heartbeat_monitor_crashes_and_removes_stale_agent() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let pool = AgentPool::new(event_tx);
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        pool.agents.lock().unwrap().insert(
            "stale-agent".into(),
            AgentHandle {
                cmd_tx,
                last_heartbeat_ms: 0,
            },
        );

        let monitor = pool.start_heartbeat_monitor(5, 1);
        let mut saw_crash = false;
        for _ in 0..50 {
            if event_rx
                .try_iter()
                .any(|event| matches!(event, AgentEvent::Crashed { agent_id, .. } if agent_id == "stale-agent"))
            {
                saw_crash = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        monitor.shutdown();

        assert!(saw_crash, "expected stale agent crash event");
        assert_eq!(pool.active_count(), 0);
        assert!(matches!(cmd_rx.try_recv(), Ok(AgentCommand::Cancel)));
    }

    // -----------------------------------------------------------------------
    // P0: AgentLoop must handle a delta-only stream (no final ToolCall event
    //     before Finished) and still execute the requested tool.
    // -----------------------------------------------------------------------

    /// Provider that emits ONLY ToolCallDelta chunks followed by Finished(ToolCalls),
    /// with no synthetic final ToolCall.  AgentLoop must accumulate the deltas and
    /// build the ToolCall itself via ToolCallAccumulator.
    #[derive(Debug)]
    struct DeltaOnlyProvider {
        path: String,
        calls: AtomicUsize,
    }

    impl LlmProvider for DeltaOnlyProvider {
        fn provider_id(&self) -> &'static str {
            "delta-only"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "delta-only/model"
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "delta-only ok".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    reasoning_tokens: None,
                },
            })
        }

        fn generate_stream(
            &self,
            _request: GenerateRequest,
        ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                // Round 1: stream only ToolCallDelta chunks; no final ToolCall event.
                let args = format!(r#"{{"path":"{}"}}"#, self.path);
                return Ok(vec![
                    Ok(StreamEvent::ToolCallDelta(crate::llm::ToolCallDelta {
                        index: 0,
                        id: Some("delta-call-1".into()),
                        name: Some("file_read".into()),
                        partial_json: None,
                    })),
                    Ok(StreamEvent::ToolCallDelta(crate::llm::ToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        partial_json: Some(args),
                    })),
                    // Deliberately omit StreamEvent::ToolCall — AgentLoop must reconstruct it.
                    Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
                ]);
            }
            // Round 2: final answer after observation.
            Ok(vec![
                Ok(StreamEvent::TextDelta("delta-only verified".into())),
                Ok(StreamEvent::Finished(FinishReason::Stop)),
            ])
        }
    }

    #[test]
    fn test_p4_agent_loop_delta_only_stream_executes_tool() {
        let path = std::env::temp_dir().join("lingxiao_delta_only_e2e.txt");
        fs::write(&path, "delta read content").unwrap();

        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(DeltaOnlyProvider {
            path: path.to_string_lossy().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));

        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-delta-only".into(),
                session_id: "sess-delta-only".into(),
                task_id: "task-delta-only".into(),
                task_content: "Read the file using only delta events.".into(),
                model: "delta-only/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        );
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        // The tool must have been initiated, proving delta accumulation produced a ToolCall.
        assert!(
            events.iter().any(|e| matches!(
                e,
                AgentEvent::ToolCallInitiated { tool_call, .. }
                    if tool_call.name == "file_read"
            )),
            "expected ToolCallInitiated(file_read) from delta-only stream; got: {events:?}"
        );
        // The tool must have produced a result.
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::ToolCallCompleted { .. })),
            "expected ToolCallCompleted after delta-only tool execution"
        );
        let _ = fs::remove_file(path);
    }

    // ── ToolLoopDetector unit tests (TS `ToolLoopDetector.test.ts` parity) ──

    fn loop_call(name: &str, args: Value) -> ToolCall {
        ToolCall {
            id: format!("{name}-1"),
            name: name.into(),
            arguments: args,
        }
    }

    #[test]
    fn test_tool_loop_detector_disabled_by_default_never_reports() {
        // No `enabled` option and no env var set in this process → disabled.
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(false),
            threshold: 2,
        });
        let call = loop_call("file_read", json!({"path": "a.ts"}));
        detector.observe(&[call.clone()]);
        detector.observe(&[call.clone()]);
        detector.observe(&[call.clone()]);
        assert_eq!(detector.consecutive_count(), 0);
        assert!(!detector.is_looping());
    }

    #[test]
    fn test_tool_loop_detector_reports_repeated_identical_when_enabled() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 2,
        });
        let call = loop_call("file_read", json!({"path": "a.ts"}));
        detector.observe(&[call.clone()]);
        assert!(!detector.is_looping());
        detector.observe(&[call.clone()]);
        assert!(detector.is_looping());
        assert_eq!(detector.consecutive_count(), 2);
        assert_eq!(
            detector.current_signature(),
            Some("file_read::{\"path\":\"a.ts\"}")
        );
    }

    #[test]
    fn test_tool_loop_detector_different_args_resets_streak() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 3,
        });
        detector.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        detector.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        assert_eq!(detector.consecutive_count(), 2);
        // Different arguments → streak resets to 1, not extended.
        detector.observe(&[loop_call("file_read", json!({"path": "b.ts"}))]);
        assert_eq!(detector.consecutive_count(), 1);
        assert!(!detector.is_looping());
    }

    #[test]
    fn test_tool_loop_detector_empty_round_neither_extends_nor_resets() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 3,
        });
        detector.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        assert_eq!(detector.consecutive_count(), 1);
        // A pure-text round (no tool calls) must not reset the streak.
        detector.observe(&[]);
        assert_eq!(detector.consecutive_count(), 1);
        // Resuming the same call extends the prior streak.
        detector.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        assert_eq!(detector.consecutive_count(), 2);
    }

    #[test]
    fn test_tool_loop_detector_multiset_signature_is_order_independent() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 2,
        });
        // Same two calls in different order across rounds → same multiset → loops.
        detector.observe(&[
            loop_call("file_read", json!({"path": "a.ts"})),
            loop_call("list_dir", json!({"path": "."})),
        ]);
        assert_eq!(detector.consecutive_count(), 1);
        detector.observe(&[
            loop_call("list_dir", json!({"path": "."})),
            loop_call("file_read", json!({"path": "a.ts"})),
        ]);
        assert_eq!(detector.consecutive_count(), 2);
        assert!(detector.is_looping());
    }

    #[test]
    fn test_tool_loop_detector_stable_json_key_order_independent() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 2,
        });
        // {path, limit} vs {limit, path} — same object, different key order.
        detector.observe(&[loop_call("file_read", json!({"path": "a.ts", "limit": 10}))]);
        detector.observe(&[loop_call("file_read", json!({"limit": 10, "path": "a.ts"}))]);
        assert_eq!(detector.consecutive_count(), 2);
        assert!(detector.is_looping());
    }

    #[test]
    fn test_tool_loop_detector_reset_clears_streak() {
        let mut detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 2,
        });
        let call = loop_call("file_read", json!({"path": "a.ts"}));
        detector.observe(&[call.clone()]);
        detector.observe(&[call.clone()]);
        assert!(detector.is_looping());
        detector.reset();
        assert_eq!(detector.consecutive_count(), 0);
        assert!(!detector.is_looping());
        // After reset the same call starts a fresh streak at 1.
        detector.observe(&[call.clone()]);
        assert_eq!(detector.consecutive_count(), 1);
        assert!(!detector.is_looping());
    }

    #[test]
    fn test_tool_loop_detector_threshold_floored_at_two() {
        let detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 1,
        });
        // threshold is clamped to >= 2 so a single observation cannot trip.
        let mut d = detector;
        d.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        assert!(!d.is_looping());
        d.observe(&[loop_call("file_read", json!({"path": "a.ts"}))]);
        assert!(d.is_looping());
    }

    // ── Integration: AgentLoop skips a looping round and injects a system prompt ──

    /// A provider that always returns the *same* `file_read` tool call every
    /// round, never producing a final answer. Used to drive the loop guard.
    #[derive(Debug)]
    struct LoopingToolProvider {
        path: String,
        calls: Arc<AtomicUsize>,
    }

    impl LlmProvider for LoopingToolProvider {
        fn provider_id(&self) -> &'static str {
            "looping-tool"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "looping/model"
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "final".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    reasoning_tokens: None,
                },
            })
        }

        fn generate_stream(
            &self,
            _request: GenerateRequest,
        ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![
                Ok(StreamEvent::TextDelta("reading again".into())),
                Ok(StreamEvent::ToolCall(ToolCall {
                    id: "loop-read".into(),
                    name: "file_read".into(),
                    arguments: json!({"path": self.path}),
                })),
                Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
            ])
        }
    }

    #[test]
    fn test_agent_loop_tool_loop_guard_skips_looping_round_and_injects_system_prompt() {
        let path = std::env::temp_dir().join("lingxiao_tool_loop_guard.txt");
        fs::write(&path, "content").unwrap();

        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(LoopingToolProvider {
            path: path.to_string_lossy().to_string(),
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));

        let detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(true),
            threshold: 2,
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-loop-guard".into(),
                session_id: "sess-loop-guard".into(),
                task_id: "task-loop-guard".into(),
                task_content: "Read the file repeatedly.".into(),
                model: "looping/model".into(),
                max_rounds: 4,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_loop_detector(detector);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let llm_calls = calls.load(Ordering::SeqCst);
        let initiated = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::ToolCallInitiated { .. }))
            .count();

        // The provider is invoked once per round (4 rounds → 4 LLM calls).
        assert_eq!(
            llm_calls, 4,
            "expected one LLM call per round; got {llm_calls}"
        );
        // With threshold=2 the pattern is execute, skip, execute, skip:
        // round1 streak=1 (executes), round2 streak=2 → guard trips (skip),
        // round3 streak=1 (executes, after reset), round4 streak=2 → guard
        // trips (skip). So only rounds 1 and 3 actually execute the tool.
        assert_eq!(
            initiated, 2,
            "expected the loop guard to skip 2 of 4 looping rounds; got {initiated} (events: {events:?})"
        );
        // The agent exhausts max_rounds without a final answer (provider never
        // stops), proving the guard did not silently complete the task.
        assert!(
            events.iter().any(|e| matches!(
                e,
                AgentEvent::Completed { result, .. }
                    if result.get("error").and_then(|v| v.as_str()) == Some("max_rounds_exceeded")
            )),
            "expected max_rounds_exceeded completion; got: {events:?}"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_agent_loop_tool_loop_guard_disabled_does_not_skip() {
        // With the guard disabled (default), a looping provider executes the
        // tool every round — no skips. This proves the guard is opt-in and the
        // wiring above only acts when enabled (TS env-gate parity).
        let path = std::env::temp_dir().join("lingxiao_tool_loop_disabled.txt");
        fs::write(&path, "content").unwrap();

        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(LoopingToolProvider {
            path: path.to_string_lossy().to_string(),
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(DirectAgentToolExecutor::new(Arc::clone(&tools)));

        let detector = ToolLoopDetector::new(ToolLoopDetectorOptions {
            enabled: Some(false),
            threshold: 2,
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-loop-disabled".into(),
                session_id: "sess-loop-disabled".into(),
                task_id: "task-loop-disabled".into(),
                task_content: "Read the file repeatedly.".into(),
                model: "looping/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_loop_detector(detector);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let initiated = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::ToolCallInitiated { .. }))
            .count();
        // Guard disabled → every round executes the tool (3 rounds → 3 calls).
        assert_eq!(
            initiated, 3,
            "disabled guard must not skip any round; got {initiated}"
        );
        let _ = fs::remove_file(path);
    }

    // ── ToolFailureLoopGuard unit tests (TS `ToolFailureLoopGuard.test.ts` parity) ──

    #[test]
    fn test_tool_failure_loop_guard_disabled_by_default_never_trips() {
        // No `enabled` option and no env var → disabled; record is a no-op.
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(false),
            threshold: 2,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        let d1 = guard.record("shell", &args, "PERMISSION_REQUIRED", "permission denied");
        let d2 = guard.record("shell", &args, "PERMISSION_REQUIRED", "permission denied");
        let d3 = guard.record("shell", &args, "PERMISSION_REQUIRED", "permission denied");
        assert!(!d1.tripped);
        assert!(!d2.tripped);
        assert!(!d3.tripped);
        assert_eq!(d3.count, 0);
        assert_eq!(guard.count_tripped(), 0);
        assert_eq!(guard.snapshot().len(), 0);
    }

    #[test]
    fn test_tool_failure_loop_guard_classifies_error_kind() {
        // Mirrors TS `classifyToolFailure` over the keyword set. Rust has no
        // typed error code, so the kind is derived from the free-text message.
        assert_eq!(
            classify_tool_failure("PERMISSION_REQUIRED: need dev permission"),
            ToolFailureErrorKind::Permission
        );
        assert_eq!(
            classify_tool_failure("MODE_TOOL_FORBIDDEN: office mode"),
            ToolFailureErrorKind::Mode
        );
        assert_eq!(
            classify_tool_failure("requested scope is outside permission grant 'shell'"),
            ToolFailureErrorKind::WriteScope
        );
        assert_eq!(
            classify_tool_failure("SANDBOX_BLOCKED: sandbox denied"),
            ToolFailureErrorKind::Sandbox
        );
        assert_eq!(
            classify_tool_failure("NETWORK_FORBIDDEN: network blocked"),
            ToolFailureErrorKind::Network
        );
        assert_eq!(
            classify_tool_failure("Missing required param: path"),
            ToolFailureErrorKind::Schema
        );
        assert_eq!(
            classify_tool_failure("FILE_MUST_BE_READ_FIRST: read first"),
            ToolFailureErrorKind::Precondition
        );
        assert_eq!(
            classify_tool_failure("shell command timed out after 30000ms"),
            ToolFailureErrorKind::Timeout
        );
        assert_eq!(
            classify_tool_failure("some unrelated runtime error"),
            ToolFailureErrorKind::Other
        );
        assert_eq!(classify_tool_failure(""), ToolFailureErrorKind::Other);
    }

    #[test]
    fn test_tool_failure_state_error_kinds_require_escalation() {
        // State-class errors must escalate; the transient/guided kinds must not.
        assert!(ToolFailureErrorKind::Permission.is_state_error());
        assert!(ToolFailureErrorKind::Mode.is_state_error());
        assert!(ToolFailureErrorKind::WriteScope.is_state_error());
        assert!(ToolFailureErrorKind::Sandbox.is_state_error());
        assert!(ToolFailureErrorKind::Network.is_state_error());
        assert!(ToolFailureErrorKind::Schema.is_state_error());
        assert!(!ToolFailureErrorKind::Execution.is_state_error());
        assert!(!ToolFailureErrorKind::Precondition.is_state_error());
        assert!(!ToolFailureErrorKind::Timeout.is_state_error());
        assert!(!ToolFailureErrorKind::Other.is_state_error());
    }

    #[test]
    fn test_tool_failure_loop_guard_trips_after_threshold_on_same_key() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 3,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        let d1 = guard.record("shell", &args, "", "permission denied");
        let d2 = guard.record("shell", &args, "", "permission denied");
        let d3 = guard.record("shell", &args, "", "permission denied");
        assert!(!d1.tripped);
        assert_eq!(d1.count, 1);
        assert!(d1.requires_escalation); // permission is state-class
        assert!(!d2.tripped);
        assert_eq!(d2.count, 2);
        assert!(d3.tripped);
        assert_eq!(d3.count, 3);
        assert!(d3.requires_escalation);
        assert_eq!(guard.count_tripped(), 1);
    }

    #[test]
    fn test_tool_failure_loop_guard_different_args_keeps_counts_separate() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let d1 = guard.record("shell", &json!({"cmd": "ls"}), "", "permission denied");
        let d2 = guard.record("shell", &json!({"cmd": "ls"}), "", "permission denied");
        let d3 = guard.record("shell", &json!({"cmd": "pwd"}), "", "permission denied");
        assert_eq!(d1.count, 1);
        assert!(d2.tripped);
        assert_eq!(d2.count, 2);
        // Different args → different key → fresh count, not tripped.
        assert_eq!(d3.count, 1);
        assert!(!d3.tripped);
    }

    #[test]
    fn test_tool_failure_loop_guard_different_error_kind_keeps_counts_separate() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        let d1 = guard.record("shell", &args, "", "permission denied");
        let d2 = guard.record("shell", &args, "", "permission denied");
        let d3 = guard.record("shell", &args, "", "shell command timed out after 1ms");
        assert_eq!(d1.count, 1);
        assert!(d2.tripped);
        // Different errorKind (timeout vs permission) → different key → fresh.
        assert_eq!(d3.count, 1);
        assert!(!d3.tripped);
        assert_eq!(d3.error_kind, ToolFailureErrorKind::Timeout);
    }

    #[test]
    fn test_tool_failure_loop_guard_precondition_never_trips() {
        // Guided precondition errors carry a next-step hint; tripping would
        // hide that hint. Count accrues but `tripped` stays false (TS
        // `NON_TRIPPING_ERROR_KINDS` parity).
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 3,
            ..Default::default()
        });
        let args = json!({"path": "src/example.ts"});
        guard.record("structured_patch", &args, "", "FILE_MUST_BE_READ_FIRST");
        guard.record("structured_patch", &args, "", "FILE_MUST_BE_READ_FIRST");
        guard.record("structured_patch", &args, "", "FILE_MUST_BE_READ_FIRST");
        let last = guard.record("structured_patch", &args, "", "FILE_MUST_BE_READ_FIRST");
        assert_eq!(last.error_kind, ToolFailureErrorKind::Precondition);
        assert_eq!(last.count, 4);
        assert!(!last.tripped);
        assert!(!last.requires_escalation);
        assert_eq!(guard.count_tripped(), 0);
    }

    #[test]
    fn test_tool_failure_loop_guard_already_tripped_does_not_inflate_count() {
        // After a trip, further identical records must not accrue count.
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        guard.record("shell", &args, "", "permission denied");
        let tripped = guard.record("shell", &args, "", "permission denied");
        assert!(tripped.tripped);
        assert_eq!(tripped.count, 2);
        let again = guard.record("shell", &args, "", "permission denied");
        assert!(again.tripped);
        assert_eq!(again.count, 2); // still 2, not 3
    }

    #[test]
    fn test_tool_failure_loop_guard_clear_on_success_wipes_matching_records() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        guard.record("shell", &args, "", "permission denied");
        guard.record("shell", &args, "", "permission denied");
        assert_eq!(guard.count_tripped(), 1);
        // A later success with the same args clears the streak.
        guard.clear_on_success("shell", &args);
        assert_eq!(guard.count_tripped(), 0);
        assert_eq!(guard.snapshot().len(), 0);
        // The next failure starts a fresh count at 1.
        let d = guard.record("shell", &args, "", "permission denied");
        assert_eq!(d.count, 1);
        assert!(!d.tripped);
    }

    #[test]
    fn test_tool_failure_loop_guard_clear_on_success_preserves_other_keys() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 5,
            ..Default::default()
        });
        guard.record("shell", &json!({"cmd": "ls"}), "", "permission denied");
        guard.record(
            "file_read",
            &json!({"path": "a.ts"}),
            "",
            "permission denied",
        );
        assert_eq!(guard.snapshot().len(), 2);
        // Clearing the shell record must not touch the file_read record.
        guard.clear_on_success("shell", &json!({"cmd": "ls"}));
        assert_eq!(guard.snapshot().len(), 1);
        assert_eq!(guard.snapshot()[0].tool_name, "file_read");
    }

    #[test]
    fn test_tool_failure_loop_guard_reset_session_clears_all() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        guard.record("shell", &json!({"cmd": "ls"}), "", "permission denied");
        guard.record("shell", &json!({"cmd": "ls"}), "", "permission denied");
        assert_eq!(guard.count_tripped(), 1);
        guard.reset_session();
        assert_eq!(guard.count_tripped(), 0);
        assert_eq!(guard.snapshot().len(), 0);
    }

    #[test]
    fn test_tool_failure_loop_guard_threshold_floored_at_two() {
        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 1,
            ..Default::default()
        });
        // threshold is clamped to >= 2 so a single failure cannot trip.
        assert_eq!(guard.threshold(), 2);
        let mut g = guard;
        let args = json!({"cmd": "ls"});
        let d1 = g.record("shell", &args, "", "permission denied");
        assert!(!d1.tripped);
        let d2 = g.record("shell", &args, "", "permission denied");
        assert!(d2.tripped);
    }

    #[test]
    fn test_tool_failure_loop_guard_stable_json_key_order_independent() {
        // {cmd, env} vs {env, cmd} → same argsHash → same key → accrues.
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let d1 = guard.record(
            "shell",
            &json!({"cmd": "ls", "env": {}}),
            "",
            "permission denied",
        );
        let d2 = guard.record(
            "shell",
            &json!({"env": {}, "cmd": "ls"}),
            "",
            "permission denied",
        );
        assert_eq!(d1.signature.args_hash, d2.signature.args_hash);
        assert_eq!(d2.count, 2);
        assert!(d2.tripped);
    }

    #[test]
    fn test_format_tool_failure_loop_error_carries_payload_and_kind() {
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let args = json!({"cmd": "ls"});
        guard.record("shell", &args, "", "permission denied");
        let decision = guard.record("shell", &args, "", "permission denied");
        let text = format_tool_failure_loop_error("shell", &decision);
        assert!(text.contains("TOOL_FAILURE_LOOP_TRIPPED"));
        assert!(text.contains("permission"));
        assert!(text.contains("LLM_RECOVERY="));
        // State-class error → escalation hint present.
        assert!(text.contains("state-class error"));
        // Non-state-class path:
        let mut guard2 = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let targs = json!({"cmd": "ls"});
        guard2.record("shell", &targs, "", "shell command timed out after 1ms");
        let dec2 = guard2.record("shell", &targs, "", "shell command timed out after 1ms");
        let text2 = format_tool_failure_loop_error("shell", &dec2);
        assert!(text2.contains("not a state-class error"));
    }

    // ── Integration: AgentLoop surfaces a tripped failure-loop error ──

    /// A tool executor that fails every non-`attempt_completion` call with a
    /// permission-class error, so the failure-loop guard can trip against it.
    #[derive(Debug)]
    struct AlwaysFailingToolExecutor;

    impl AgentToolExecutor for AlwaysFailingToolExecutor {
        fn execute_tool(
            &self,
            _session_id: &str,
            _agent_id: &str,
            tool_call: &ToolCall,
        ) -> ToolResult {
            if tool_call.name == "attempt_completion" {
                return ToolResult::ok(json!({"result": "done"}));
            }
            ToolResult::err("permission denied: requires permission grant 'shell'")
        }
    }

    /// A provider that re-issues the *same* failing `shell` call every round,
    /// never producing a final answer, so the failure loop repeats.
    #[derive(Debug)]
    struct FailingLoopProvider {
        calls: Arc<AtomicUsize>,
    }

    impl LlmProvider for FailingLoopProvider {
        fn provider_id(&self) -> &'static str {
            "failing-loop"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "failing/model"
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "final".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                    cache_creation_input_tokens: None,
                    cache_read_input_tokens: None,
                    reasoning_tokens: None,
                },
            })
        }

        fn generate_stream(
            &self,
            _request: GenerateRequest,
        ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![
                Ok(StreamEvent::TextDelta("trying shell".into())),
                Ok(StreamEvent::ToolCall(ToolCall {
                    id: "fail-call".into(),
                    name: "shell".into(),
                    arguments: json!({"command": "ls"}),
                })),
                Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
            ])
        }
    }

    #[test]
    fn test_agent_loop_failure_guard_surfaces_tripped_error_after_threshold() {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(AlwaysFailingToolExecutor);

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 3,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-failure-guard".into(),
                session_id: "sess-failure-guard".into(),
                task_id: "task-failure-guard".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 5,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let completed_results: Vec<Value> = events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::ToolCallCompleted { result, .. } => Some(result.clone()),
                _ => None,
            })
            .collect();

        // Every round executes the tool (the failure guard surfaces an error
        // *result* rather than skipping the round, unlike the loop detector),
        // so 5 rounds → 5 ToolCallCompleted events.
        assert_eq!(
            completed_results.len(),
            5,
            "failure guard surfaces a result each round; got: {events:?}"
        );
        // The first two results are the raw failure; from the 3rd on the guard
        // has tripped and surfaces a TOOL_FAILURE_LOOP_TRIPPED recovery error.
        let tripped_results: Vec<&Value> = completed_results
            .iter()
            .filter(|r| {
                r.get("error")
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains("TOOL_FAILURE_LOOP_TRIPPED"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !tripped_results.is_empty(),
            "expected at least one tripped recovery result; got: {completed_results:?}"
        );
        // The agent exhausts max_rounds (provider never stops) — the guard did
        // not silently complete the task.
        assert!(
            events.iter().any(|e| matches!(
                e,
                AgentEvent::Completed { result, .. }
                    if result.get("error").and_then(|v| v.as_str()) == Some("max_rounds_exceeded")
            )),
            "expected max_rounds_exceeded; got: {events:?}"
        );
    }

    #[test]
    fn test_agent_loop_failure_guard_disabled_surfaces_raw_failure() {
        // With the guard disabled (default), the raw failure is surfaced every
        // round unchanged — no TOOL_FAILURE_LOOP_TRIPPED banner. This proves the
        // wiring is opt-in (TS env-gate parity).
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(AlwaysFailingToolExecutor);

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(false),
            threshold: 2,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-failure-disabled".into(),
                session_id: "sess-failure-disabled".into(),
                task_id: "task-failure-disabled".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let completed_results: Vec<Value> = events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::ToolCallCompleted { result, .. } => Some(result.clone()),
                _ => None,
            })
            .collect();
        // 3 rounds → 3 raw failures, none tripped.
        assert_eq!(completed_results.len(), 3);
        for r in &completed_results {
            let err = r.get("error").and_then(|v| v.as_str()).unwrap_or("");
            assert!(
                !err.contains("TOOL_FAILURE_LOOP_TRIPPED"),
                "disabled guard must not surface a tripped banner; got: {err}"
            );
            assert!(
                err.contains("permission denied"),
                "disabled guard must surface the raw failure; got: {err}"
            );
        }
    }

    // ── R-7 follow-up: Leader-bus escalation signal (durable/canonical) ──

    /// A tool executor that fails every non-`attempt_completion` call with a
    /// *timeout* error (a non-state-class kind): the guard trips but
    /// `requires_escalation` is false, so no escalation signal is emitted.
    #[derive(Debug)]
    struct TimeoutFailingToolExecutor;

    impl AgentToolExecutor for TimeoutFailingToolExecutor {
        fn execute_tool(
            &self,
            _session_id: &str,
            _agent_id: &str,
            tool_call: &ToolCall,
        ) -> ToolResult {
            if tool_call.name == "attempt_completion" {
                return ToolResult::ok(json!({"result": "done"}));
            }
            ToolResult::err("shell command timed out after 30000ms")
        }
    }

    /// A tool executor that fails the first `fail_n` non-`attempt_completion`
    /// calls with a permission error, then succeeds. Used to exercise the
    /// `clear_on_success` path: a sub-threshold failure streak wiped by a
    /// later success must not trip and must not emit an escalation signal.
    #[derive(Debug)]
    struct FlakyThenSuccessToolExecutor {
        fail_n: usize,
        calls: AtomicUsize,
    }

    impl AgentToolExecutor for FlakyThenSuccessToolExecutor {
        fn execute_tool(
            &self,
            _session_id: &str,
            _agent_id: &str,
            tool_call: &ToolCall,
        ) -> ToolResult {
            if tool_call.name == "attempt_completion" {
                return ToolResult::ok(json!({"result": "done"}));
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_n {
                return ToolResult::err("permission denied: requires permission grant 'shell'");
            }
            ToolResult::ok(json!({"content": "ok"}))
        }
    }

    /// Collect any `ToolFailureLoopEscalated` events from the channel.
    fn collect_escalations(events: &[AgentEvent]) -> Vec<ToolFailureLoopEscalation> {
        events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::ToolFailureLoopEscalated { escalation, .. } => Some(escalation.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_agent_loop_failure_guard_emits_escalation_signal_on_state_error() {
        // A state-class (permission) trip must emit a durable escalation signal
        // carrying the failure signature — the Rust equivalent of the TS
        // `agent:tool_failure_loop` event + `tool_failure_loop_escalation`
        // MessageBus message. The signal is observable on the agent→supervisor
        // event channel (the bridge writes it to agent_logs + event_log).
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(AlwaysFailingToolExecutor);

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 3,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-escalation".into(),
                session_id: "sess-escalation".into(),
                task_id: "task-escalation".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 5,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let escalations = collect_escalations(&events);
        // Exactly one escalation signal — emitted on the 3rd (threshold) failure.
        assert_eq!(
            escalations.len(),
            1,
            "expected exactly one escalation signal on a state-class trip; got: {escalations:?}"
        );
        let esc = &escalations[0];
        assert_eq!(esc.agent_id, "agent-escalation");
        assert_eq!(esc.session_id, "sess-escalation");
        assert_eq!(esc.tool_name, "shell");
        assert_eq!(esc.error_kind, ToolFailureErrorKind::Permission);
        assert!(esc.requires_escalation);
        assert_eq!(esc.count, 3);
        assert_eq!(esc.threshold, 3);
        // The summary carries the raw failure text (pre-LLM_RECOVERY split).
        assert!(esc.last_error_message.contains("permission denied"));
    }

    #[test]
    fn test_agent_loop_failure_guard_no_escalation_on_non_state_trip() {
        // A non-state-class trip (timeout) still surfaces the
        // TOOL_FAILURE_LOOP_TRIPPED recovery error to the LLM (R-7 in-process),
        // but must NOT emit a Leader escalation signal — mirroring TS, where the
        // bus escalation is the state-error path only.
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(TimeoutFailingToolExecutor);

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-timeout-trip".into(),
                session_id: "sess-timeout-trip".into(),
                task_id: "task-timeout-trip".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        // The tripped recovery error is still surfaced to the LLM...
        let tripped_results: Vec<&Value> = events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::ToolCallCompleted { result, .. } => Some(result),
                _ => None,
            })
            .filter(|r| {
                r.get("error")
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains("TOOL_FAILURE_LOOP_TRIPPED"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !tripped_results.is_empty(),
            "non-state trip must still surface the tripped recovery error; got: {events:?}"
        );
        // ...but no escalation signal is emitted.
        let escalations = collect_escalations(&events);
        assert!(
            escalations.is_empty(),
            "non-state trip must not emit a Leader escalation signal; got: {escalations:?}"
        );
    }

    #[test]
    fn test_agent_loop_failure_guard_disabled_emits_no_escalation() {
        // Disabled guard: raw failures surface every round, no trip, no
        // escalation signal. Byte-for-byte legacy behavior (TS env-gate parity).
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        let executor = Arc::new(AlwaysFailingToolExecutor);

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(false),
            threshold: 2,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-disabled-esc".into(),
                session_id: "sess-disabled-esc".into(),
                task_id: "task-disabled-esc".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 3,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let escalations = collect_escalations(&events);
        assert!(
            escalations.is_empty(),
            "disabled guard must not emit any escalation signal; got: {escalations:?}"
        );
    }

    #[test]
    fn test_agent_loop_failure_guard_success_clears_no_escalation() {
        // A sub-threshold failure streak wiped by a later success must not trip
        // and must not emit an escalation signal. Exercises `clear_on_success`.
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let _ = cmd_tx;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(FailingLoopProvider {
            calls: Arc::clone(&calls),
        }));
        let llm = Arc::new(LlmRouter::new(registry));
        let tools = Arc::new(crate::tool::ToolRegistry::with_native_tools());
        let llm_executor = Arc::new(DirectAgentLlmExecutor::new(llm));
        // Fail the first call (count=1, below threshold=3), then succeed.
        let executor = Arc::new(FlakyThenSuccessToolExecutor {
            fail_n: 1,
            calls: AtomicUsize::new(0),
        });

        let guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 3,
            ..Default::default()
        });
        let agent = AgentLoop::new(
            AgentConfig {
                agent_id: "agent-success-clear".into(),
                session_id: "sess-success-clear".into(),
                task_id: "task-success-clear".into(),
                task_content: "Run the same shell call.".into(),
                model: "failing/model".into(),
                max_rounds: 5,
                ..Default::default()
            },
            tools,
            llm_executor,
            executor,
            event_tx,
            cmd_rx,
        )
        .with_tool_failure_loop_guard(guard);
        agent.run();

        let events: Vec<_> = event_rx.try_iter().collect();
        let escalations = collect_escalations(&events);
        assert!(
            escalations.is_empty(),
            "sub-threshold streak cleared by success must not emit an escalation signal; got: {escalations:?}"
        );
        // The agent eventually completes (the provider keeps calling shell, but
        // once the executor succeeds the LLM gets a real observation). At minimum
        // no tripped banner should appear.
        let tripped = events.iter().any(|e| {
            matches!(
                e,
                AgentEvent::ToolCallCompleted { result, .. }
                    if result.get("error").and_then(|v| v.as_str())
                        .map(|s| s.contains("TOOL_FAILURE_LOOP_TRIPPED"))
                        .unwrap_or(false)
            )
        });
        assert!(
            !tripped,
            "sub-threshold streak cleared by success must not trip; got: {events:?}"
        );
    }

    #[test]
    fn test_escalation_payload_omits_raw_args_and_does_not_leak_secrets() {
        // The durable escalation payload carries only the stable `args_hash`,
        // never the raw tool `arguments` — so an args-secret (e.g. a token in a
        // shell command) cannot leak into the durable record. Verify by
        // serializing the payload and asserting the secret is absent while the
        // hash is present.
        let mut guard = ToolFailureLoopGuard::new(ToolFailureLoopGuardOptions {
            enabled: Some(true),
            threshold: 2,
            ..Default::default()
        });
        // Args contain a fake secret. The guard must NOT embed the raw args in
        // the escalation; only the stable hash is carried.
        let secret_args = json!({"command": "curl -H 'Authorization: Bearer sk-secret-leak-12345' https://example.com"});
        guard.record("shell", &secret_args, "", "permission denied");
        let decision = guard.record("shell", &secret_args, "", "permission denied");
        assert!(decision.tripped);
        assert!(decision.requires_escalation);

        let escalation = ToolFailureLoopEscalation::from_tripped(
            &decision,
            "sess-secret",
            "agent-secret",
            "worker",
            "task-secret",
            "shell",
            2,
            "permission denied",
        );
        let payload = serde_json::to_value(&escalation).unwrap();
        let payload_str = payload.to_string();
        // The secret token must never appear in the durable payload.
        assert!(
            !payload_str.contains("sk-secret-leak-12345"),
            "escalation payload must not leak raw args secrets; got: {payload_str}"
        );
        assert!(
            !payload_str.contains("Authorization"),
            "escalation payload must not embed raw args; got: {payload_str}"
        );
        // The stable args_hash IS carried (so the Leader can de-dup trips).
        assert!(
            payload.get("args_hash").and_then(|v| v.as_str()).is_some(),
            "escalation payload must carry the args_hash; got: {payload_str}"
        );
        assert!(!payload
            .get("args_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .is_empty());
        // No top-level `args` / `arguments` field exists in the payload schema.
        assert!(
            payload.get("args").is_none() && payload.get("arguments").is_none(),
            "escalation payload must not have an args/arguments field; got: {payload_str}"
        );
        // error_kind serializes to TS snake_case ("permission").
        assert_eq!(
            payload.get("error_kind").and_then(|v| v.as_str()),
            Some("permission")
        );
        assert_eq!(
            payload.get("requires_escalation").and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    // ───────────────────────────────────────────────────────────────────────
    // R-7 deferred LeaderPermissionManager auto-response parity — the pure
    // `escalation_auto_response` policy table that mirrors TS
    // `LeaderPermissionManager.handleToolFailureLoopEscalation` (decision only;
    // actual live mode mutation is deferred, see the matrix R-7 row).
    // ───────────────────────────────────────────────────────────────────────

    #[test]
    fn test_escalation_auto_response_permission_approves_and_targets_networked() {
        // TS: permission ⇒ 'approved', mode → networked (unless already yolo).
        let resp = escalation_auto_response(ToolFailureErrorKind::Permission);
        assert_eq!(resp.action, EscalationAction::Approved);
        assert_eq!(resp.decision, "approved");
        assert_eq!(resp.error_kind, ToolFailureErrorKind::Permission);
        // From a non-yolo mode, the target is networked (TS parity).
        assert_eq!(resp.target_mode("strict").as_deref(), Some("networked"));
        assert_eq!(resp.target_mode("dev").as_deref(), Some("networked"));
        // yolo stays yolo.
        assert_eq!(resp.target_mode("yolo").as_deref(), Some("yolo"));
        // Case-insensitive yolo match (TS uses `=== 'yolo'` on a canonicalized
        // mode; Rust's free-text mode string may differ in case).
        assert_eq!(resp.target_mode("YOLO").as_deref(), Some("yolo"));
    }

    #[test]
    fn test_escalation_auto_response_network_approves() {
        // TS: network ⇒ 'approved' (same branch as permission).
        let resp = escalation_auto_response(ToolFailureErrorKind::Network);
        assert_eq!(resp.action, EscalationAction::Approved);
        assert_eq!(resp.decision, "approved");
        assert_eq!(resp.target_mode("dev").as_deref(), Some("networked"));
    }

    #[test]
    fn test_escalation_auto_response_sandbox_rejects() {
        // TS: sandbox ⇒ 'rejected'.
        let resp = escalation_auto_response(ToolFailureErrorKind::Sandbox);
        assert_eq!(resp.action, EscalationAction::Rejected);
        assert_eq!(resp.decision, "rejected");
        // Reject never proposes a mode change.
        assert!(resp.target_mode("dev").is_none());
    }

    #[test]
    fn test_escalation_auto_response_mode_write_scope_schema_reject() {
        // TS: mode/write_scope/schema ⇒ 'rejected' (retry is meaningless).
        for kind in [
            ToolFailureErrorKind::Mode,
            ToolFailureErrorKind::WriteScope,
            ToolFailureErrorKind::Schema,
        ] {
            let resp = escalation_auto_response(kind);
            assert_eq!(resp.action, EscalationAction::Rejected, "kind {kind:?}");
            assert_eq!(resp.decision, "rejected", "kind {kind:?}");
            assert!(resp.target_mode("dev").is_none(), "kind {kind:?}");
            assert_eq!(resp.error_kind, kind);
        }
    }

    #[test]
    fn test_escalation_auto_response_non_state_kinds_are_interactive() {
        // TS: execution/timeout/aborted/other/default ⇒ 'interactive'.
        for kind in [
            ToolFailureErrorKind::Execution,
            ToolFailureErrorKind::Timeout,
            ToolFailureErrorKind::Aborted,
            ToolFailureErrorKind::Precondition,
            ToolFailureErrorKind::Other,
        ] {
            let resp = escalation_auto_response(kind);
            assert_eq!(resp.action, EscalationAction::Interactive, "kind {kind:?}");
            assert_eq!(resp.decision, "interactive", "kind {kind:?}");
            assert!(resp.target_mode("dev").is_none(), "kind {kind:?}");
            assert_eq!(resp.error_kind, kind);
        }
    }

    #[test]
    fn test_escalation_auto_response_covers_every_error_kind() {
        // Exhaustive: every ToolFailureErrorKind variant maps to exactly one
        // action, so the policy table cannot silently fall through to a default
        // for a new variant added later.
        let all = [
            ToolFailureErrorKind::Permission,
            ToolFailureErrorKind::Mode,
            ToolFailureErrorKind::WriteScope,
            ToolFailureErrorKind::Sandbox,
            ToolFailureErrorKind::Network,
            ToolFailureErrorKind::Schema,
            ToolFailureErrorKind::Precondition,
            ToolFailureErrorKind::Execution,
            ToolFailureErrorKind::Timeout,
            ToolFailureErrorKind::Aborted,
            ToolFailureErrorKind::Other,
        ];
        let mut approved = 0;
        let mut rejected = 0;
        let mut interactive = 0;
        for kind in all {
            match escalation_auto_response(kind).action {
                EscalationAction::Approved => approved += 1,
                EscalationAction::Rejected => rejected += 1,
                EscalationAction::Interactive => interactive += 1,
            }
        }
        // permission + network.
        assert_eq!(approved, 2);
        // sandbox + mode + write_scope + schema.
        assert_eq!(rejected, 4);
        // precondition + execution + timeout + aborted + other.
        assert_eq!(interactive, 5);
    }

    #[test]
    fn test_escalation_auto_response_decision_record_has_no_raw_args() {
        // The decision record is derived purely from error_kind + from_mode; it
        // never touches the tool arguments, so it cannot leak an args-secret
        // even before the bridge's redaction defense-in-depth runs.
        let resp = escalation_auto_response(ToolFailureErrorKind::Permission);
        let serialized = serde_json::to_string(&resp).unwrap();
        // No args-bearing field exists on the decision shape.
        assert!(!serialized.contains("args"));
        assert!(!serialized.contains("arguments"));
        assert!(!serialized.contains("sk-"));
        assert!(!serialized.contains("Bearer"));
        // The decision shape carries the action/decision/reason/error_kind only.
        let value: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(value["action"], "approved");
        assert_eq!(value["decision"], "approved");
        assert_eq!(value["error_kind"], "permission");
        assert!(value["reason"].as_str().unwrap().contains("permission"));
    }
}
