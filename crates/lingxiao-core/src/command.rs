use crate::agent::{AgentConfig, AgentEvent, AgentLlmExecutor, AgentPool, AgentToolExecutor};
use crate::bus::{Message as BusMessage, MessageBus, Priority};
use crate::document_tools::{
    document_error_message, document_error_payload, DocumentToolError, DocumentToolRunner,
};
use crate::event_log::{
    allocate_seq, ensure_meta, get_current_generation, insert_event, try_fetch_event_by_id,
    update_meta_seq, EventLog,
};
use crate::leader::LeaderOrchestrator;
use crate::llm::{
    AuthContext, GenerateRequest, LlmEventSink, LlmRouter, Message, MockLlmProvider, ProviderError,
    ProviderErrorCode, ProviderRegistry, RequestOptions, StreamEvent, ToolCall,
    ToolCallAccumulator,
};
use crate::mcp_bridge::{
    mcp_error_message, mcp_error_payload, McpBridgeError, McpBridgeRequest, McpBridgeRunner,
};
use crate::persistence::DbOwner;
use crate::process::ProcessRegistry;
use crate::projection::{ConnectResult, ProjectionService};
use crate::repl::{ReplEvalError, ReplEvalRequest, ReplRunner};
use crate::runtime::RuntimeManager;
use crate::schedule::Scheduler;
use crate::session::SessionStatus;
use crate::sidecar::{SidecarCommand, SidecarInvocation, SidecarScheduler};
use crate::terminal::{terminal_read_json, TerminalCreateOptions, TerminalManager};
use crate::tool::{ToolPermission, ToolRegistry, ToolResult};
use crate::workflow::plan_dag_execution;
use lingxiao_core_protocol::actor::{Actor, ActorKind};
use lingxiao_core_protocol::command::{CommandEnvelope, CommandResponse};
use lingxiao_core_protocol::error::{CoreError, ErrorCode};
use lingxiao_core_protocol::event::EventEnvelope;
use lingxiao_core_protocol::types::*;
use lingxiao_tool_host_protocol::{
    CancelToken, PermissionLease, ResourceBudget, SidecarErrorCode, SidecarRequest, SidecarResponse,
};
use rusqlite::{params, OptionalExtension, Result, Transaction};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const SESSION_CREATE: &str = "session.create";
const SESSION_INPUT: &str = "session.input";
const SESSION_INTERRUPT: &str = "session.interrupt";
const SESSION_RESUME: &str = "session.resume";
const SESSION_COMPLETE: &str = "session.complete";
const SESSION_FAIL: &str = "session.fail";
const SESSION_DELETE: &str = "session.delete";
const SESSION_LIST: &str = "session.list";
const SESSION_RUN_TASK: &str = "session.run_task";
const SESSION_SNAPSHOT: &str = "session.snapshot";
const SESSION_CONNECT: &str = "session.connect";
const CONV_LIST: &str = "conv.list";
const EVENT_REPLAY: &str = "event.replay";
const EVENT_COMPACT: &str = "event.compact";
const RETENTION_SWEEP: &str = "retention.sweep";
const TASK_CREATE: &str = "task.create";
const TASK_ASSIGN: &str = "task.assign";
const TASK_COMPLETE: &str = "task.complete";
const TASK_FAIL: &str = "task.fail";
const TASK_REDISPATCH: &str = "task.redispatch";
const TASK_REOPEN: &str = "task.reopen";
const TASK_LIST: &str = "task.list";
const PERMISSION_REQUEST: &str = "permission.request";
const PERMISSION_RESOLVE: &str = "permission.resolve";
const PERMISSION_SET_MODE: &str = "permission.set_mode";
const AGENT_SPAWN: &str = "agent.spawn";
const AGENT_START: &str = "agent.start";
const AGENT_COMPLETE: &str = "agent.complete";
const AGENT_CRASH: &str = "agent.crash";
const AGENT_RESPAWN: &str = "agent.respawn";
const LEADER_PLAN: &str = "leader.plan";
const LEADER_RUN: &str = "leader.run";
const WORKFLOW_EXECUTE: &str = "workflow.execute";
const WORKFLOW_PAUSE: &str = "workflow.pause";
const WORKFLOW_RESUME: &str = "workflow.resume";
const WORKFLOW_CANCEL: &str = "workflow.cancel";
const WORKFLOW_LIST: &str = "workflow.list";
const TOOL_CALL: &str = "tool.call";
const TOOL_CANCEL: &str = "tool.cancel";
const LLM_CALL: &str = "llm.call";
const RUNTIME_COMPACT: &str = "runtime.compact";
const RUNTIME_DEBUG_DUMP: &str = "runtime.debug_dump";
const BLACKBOARD_INTENT_CREATE: &str = "blackboard.intent.create";
const BLACKBOARD_INTENT_CLAIM: &str = "blackboard.intent.claim";
const BLACKBOARD_INTENT_RESOLVE: &str = "blackboard.intent.resolve";
const GRAPH_QUERY: &str = "graph.query";
const TEAM_SEND: &str = "team.send";
const TEAM_POLL: &str = "team.poll";
const TEAM_MARK_READ: &str = "team.mark_read";
const MEMORY_UPSERT: &str = "memory.upsert";
const MEMORY_SEARCH: &str = "memory.search";
const EMBEDDING_UPSERT: &str = "embedding.upsert";
const EMBEDDING_SEARCH: &str = "embedding.search";
const BUS_PUBLISH: &str = "bus.publish";
const BUS_POP: &str = "bus.pop";
const BUS_DEAD_LETTERS: &str = "bus.dead_letters";
const COMMAND_DEDUPE_TTL_MS: i64 = 24 * 60 * 60 * 1000;
const DEFAULT_RETENTION_RETAIN_PER_SESSION: i64 = 1_000;
const SCHEDULE_CREATE: &str = "schedule.create";
const SCHEDULE_LIST: &str = "schedule.list";
const SCHEDULE_FIRE: &str = "schedule.fire";
const SCHEDULE_FIRE_DUE: &str = "schedule.fire_due";
const ASSUMPTION_CREATE: &str = "assumption.create";
const ASSUMPTION_LIST: &str = "assumption.list";
const ASSUMPTION_VERIFY: &str = "assumption.verify";
const ASSUMPTION_FALSIFY: &str = "assumption.falsify";
const TRACE_TIMELINE: &str = "trace.timeline";
const METRICS_QUERY: &str = "metrics.query";
const WORKTREE_CREATE: &str = "worktree.create";
const WORKTREE_DELETE: &str = "worktree.delete";
const WORKTREE_LIST: &str = "worktree.list";
const TERMINAL_CREATE: &str = "terminal.create";
const TERMINAL_SEND: &str = "terminal.send";
const TERMINAL_READ: &str = "terminal.read";
const TERMINAL_KILL: &str = "terminal.kill";
const REPL_EVAL: &str = "repl.eval";
const REPL_CREATE: &str = "repl.create";
const REPL_SEND: &str = "repl.send";
const REPL_READ: &str = "repl.read";
const REPL_KILL: &str = "repl.kill";
const PARSE_FILE: &str = "parse_file";
const OCR_EXTRACT_TEXT: &str = "ocr.extract_text";
const MCP_BRIDGE: &str = "mcp.bridge";
const MCP_SERVER_START: &str = "mcp.server_start";
const MCP_LIST_TOOLS: &str = "mcp.list_tools";
const MCP_CALL_TOOL: &str = "mcp.call_tool";
const MCP_SERVER_STOP: &str = "mcp.server_stop";

pub struct CommandRouter {
    db: DbOwner,
    event_log: EventLog,
    projection: ProjectionService,
    llm_router: Option<Arc<LlmRouter>>,
    sidecar_scheduler: Arc<SidecarScheduler>,
    sidecar_commands: HashMap<String, SidecarCommand>,
    tool_registry: Arc<crate::tool::ToolRegistry>,
    runtime_manager: Arc<Mutex<RuntimeManager>>,
    message_bus: Arc<Mutex<MessageBus>>,
    terminal_manager: Arc<TerminalManager>,
    repl_runner: Arc<ReplRunner>,
    document_tools: Arc<DocumentToolRunner>,
    mcp_bridge: Arc<McpBridgeRunner>,
    agent_pool: Arc<AgentPool>,
    allow_mock_llm_fallback: bool,
}

#[derive(Debug, Clone)]
struct WorkflowNodeExecution {
    node_id: String,
    node_type: String,
    status: String,
    success: bool,
    output: Value,
    error: Option<String>,
    attempt: i64,
    retryable: bool,
}

impl WorkflowNodeExecution {
    fn as_json(&self) -> Value {
        json!({
            "node_id": self.node_id,
            "node_type": self.node_type,
            "status": self.status,
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "attempt": self.attempt,
            "retryable": self.retryable,
        })
    }
}

#[derive(Clone)]
struct SqliteAgentContextStore {
    db: DbOwner,
}

impl SqliteAgentContextStore {
    fn new(db: DbOwner) -> Self {
        Self { db }
    }
}

impl crate::agent::AgentContextStore for SqliteAgentContextStore {
    fn load_messages(
        &self,
        session_id: &str,
        agent_id: &str,
    ) -> Vec<crate::agent::AgentContextMessage> {
        let conn = self.db.conn();
        let Ok(mut stmt) = conn.prepare(
            "SELECT role, content, tool_call_id FROM agent_conversation \
             WHERE session_id = ?1 AND agent_id = ?2 ORDER BY id",
        ) else {
            return Vec::new();
        };
        let Ok(rows) = stmt.query_map(params![session_id, agent_id], |row| {
            Ok(crate::agent::AgentContextMessage {
                role: row.get(0)?,
                content: row.get(1)?,
                tool_call_id: row.get(2)?,
            })
        }) else {
            return Vec::new();
        };
        rows.filter_map(Result::ok).collect()
    }

    fn append_message(
        &self,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        message: &crate::agent::AgentContextMessage,
    ) {
        let conn = self.db.conn();
        let _ = conn.execute(
            "INSERT INTO agent_conversation \
             (session_id, agent_id, agent_name, role, content, tool_call_id, timestamp) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_id,
                agent_id,
                agent_name,
                &message.role,
                &message.content,
                &message.tool_call_id,
                now_ms() as f64 / 1000.0,
            ],
        );
    }

    fn save_active_projection(
        &self,
        session_id: &str,
        agent_id: &str,
        original_message_count: usize,
        active_message_count: usize,
    ) {
        let conn = self.db.conn();
        let key = format!("agent_active_context_projection:{agent_id}");
        let value = json!({
            "agent_id": agent_id,
            "original_message_count": original_message_count,
            "active_message_count": active_message_count,
            "message_bodies": "omitted",
        });
        let _ = conn.execute(
            "INSERT INTO session_state (session_id, key, value, timestamp) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(session_id, key) DO UPDATE SET \
             value = excluded.value, timestamp = excluded.timestamp",
            params![session_id, key, value.to_string(), now_ms() as f64 / 1000.0],
        );
    }
}

struct RouterAgentToolExecutor {
    db: DbOwner,
    tool_registry: Arc<ToolRegistry>,
    runtime_manager: Arc<Mutex<RuntimeManager>>,
}

impl RouterAgentToolExecutor {
    fn new(
        db: DbOwner,
        tool_registry: Arc<ToolRegistry>,
        runtime_manager: Arc<Mutex<RuntimeManager>>,
    ) -> Self {
        Self {
            db,
            tool_registry,
            runtime_manager,
        }
    }
}

impl AgentToolExecutor for RouterAgentToolExecutor {
    fn execute_tool(&self, session_id: &str, agent_id: &str, tool_call: &ToolCall) -> ToolResult {
        if !self.tool_registry.is_registered(&tool_call.name) {
            return ToolResult::err(format!("Tool not found: {}", tool_call.name));
        }
        if let Err(error) = preflight_native_tool_call(
            &self.db,
            self.tool_registry.as_ref(),
            session_id,
            &tool_call.name,
            &tool_call.arguments,
        ) {
            return ToolResult::err(error.message);
        }
        let execution_args = match workspace_scoped_tool_args(
            &self.db,
            session_id,
            &tool_call.name,
            &tool_call.arguments,
        ) {
            Ok(args) => args,
            Err(error) => return ToolResult::err(error.message),
        };

        let slot_id = format!("{session_id}:{agent_id}:{}", tool_call.id);
        let estimated_file_write_bytes =
            estimated_native_file_write_bytes(&tool_call.name, &tool_call.arguments);
        let mut runtime = self.runtime_manager.lock().unwrap();
        let slot = match runtime.try_acquire_tool(slot_id) {
            Ok(slot) => slot,
            Err(error) => {
                return ToolResult::err(format!(
                    "Tool runtime budget exceeded: requested {}, available {}",
                    error.requested, error.available
                ));
            }
        };
        let file_write_reservation = if let Some(bytes) = estimated_file_write_bytes {
            match runtime.try_reserve_file_write(format!("{slot}:file_write"), bytes) {
                Ok(reservation) => Some(reservation),
                Err(error) => {
                    runtime.release_tool(&slot);
                    return ToolResult::err(format!(
                        "File write budget exceeded: requested {}, available {}",
                        error.requested, error.available
                    ));
                }
            }
        } else {
            None
        };
        drop(runtime);

        let started_at = now_ms();
        if let Err(error) = begin_agent_tool_call_ledger(
            &self.db,
            session_id,
            &tool_call.id,
            &tool_call.name,
            &tool_call.arguments,
            agent_id,
            estimated_file_write_bytes,
            started_at,
        ) {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime.release_tool(&slot);
            if let Some(reservation) = file_write_reservation {
                runtime.release_file_write_reservation(&reservation);
            }
            return ToolResult::err(format!("Tool ledger persistence failed: {error}"));
        }

        if let Err(error) = append_agent_tool_lifecycle_event(
            &self.db,
            session_id,
            agent_id,
            tool_call,
            "tool.call_initiated",
            json!({
                "tool_call_id": tool_call.id,
                "tool_name": tool_call.name,
                "args": sanitized_persistence_value(&tool_call.arguments),
            }),
        ) {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime.release_tool(&slot);
            if let Some(reservation) = file_write_reservation {
                runtime.release_file_write_reservation(&reservation);
            }
            return ToolResult::err(format!("Tool event persistence failed: {error}"));
        }

        let result = self.tool_registry.execute(&tool_call.name, &execution_args);
        let mut runtime = self.runtime_manager.lock().unwrap();
        runtime.release_tool(&slot);
        if let Some(reservation) = file_write_reservation {
            if result.success {
                runtime.commit_file_write_reservation(&reservation);
            } else {
                runtime.release_file_write_reservation(&reservation);
            }
        }
        drop(runtime);

        let terminal_status = if result.success {
            "completed"
        } else {
            "failed"
        };
        let terminal_result = result.success.then_some(&result.output);
        let terminal_error = result.error.as_deref();
        if let Err(error) = finish_agent_tool_call_ledger(
            &self.db,
            session_id,
            &tool_call.id,
            terminal_status,
            terminal_result,
            terminal_error,
            now_ms(),
        ) {
            return ToolResult::err(format!("Tool ledger persistence failed: {error}"));
        }

        let event_type = if result.success {
            "tool.call_completed"
        } else {
            "tool.call_failed"
        };
        let payload = if result.success {
            json!({
                "tool_call_id": tool_call.id,
                "tool_name": tool_call.name,
                "result": sanitized_persistence_value(&result.output),
            })
        } else {
            json!({
                "tool_call_id": tool_call.id,
                "tool_name": tool_call.name,
                "error": result.error.clone(),
            })
        };
        if let Err(error) = append_agent_tool_lifecycle_event(
            &self.db, session_id, agent_id, tool_call, event_type, payload,
        ) {
            return ToolResult::err(format!("Tool event persistence failed: {error}"));
        }
        result
    }
}

#[allow(clippy::too_many_arguments)]
fn begin_agent_tool_call_ledger(
    db: &DbOwner,
    session_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    args: &Value,
    agent_id: &str,
    estimated_file_write_bytes: Option<u64>,
    occurred_at: Timestamp,
) -> std::result::Result<(), rusqlite::Error> {
    let request_id: RequestId = format!("req_agent_tool_{}_{}", agent_id, tool_call_id);
    db.with_transaction(|tx| {
        ensure_session_active(tx, session_id, &request_id)?;
        begin_tool_call_in_tx(
            tx,
            session_id,
            tool_call_id,
            tool_name,
            "native",
            args,
            occurred_at,
            json!({
                "agent_id": agent_id,
                "estimated_file_write_bytes": estimated_file_write_bytes,
            }),
        )?;
        Ok(())
    })
}

fn finish_agent_tool_call_ledger(
    db: &DbOwner,
    session_id: &str,
    tool_call_id: &str,
    status: &str,
    result: Option<&Value>,
    error: Option<&str>,
    occurred_at: Timestamp,
) -> std::result::Result<(), rusqlite::Error> {
    let request_id: RequestId = format!("req_agent_tool_finish_{tool_call_id}");
    db.with_transaction(|tx| {
        ensure_session_active(tx, session_id, &request_id)?;
        set_tool_call_terminal(
            tx,
            session_id,
            tool_call_id,
            status,
            result,
            error,
            occurred_at,
        )
    })
}

fn append_agent_tool_lifecycle_event(
    db: &DbOwner,
    session_id: &str,
    agent_id: &str,
    tool_call: &ToolCall,
    event_type: &str,
    payload: Value,
) -> std::result::Result<(), rusqlite::Error> {
    let request_id: RequestId = format!("req_agent_tool_{}_{}", agent_id, tool_call.id);
    let actor = Actor {
        kind: ActorKind::Agent,
        id: Some(agent_id.to_string()),
    };
    let occurred_at = now_ms();
    db.with_transaction(|tx| {
        ensure_session_active(tx, session_id, &request_id)?;
        let generation = get_current_generation(tx, &Some(session_id.to_string()))?;
        simple_event(
            tx,
            session_id,
            generation,
            event_type,
            actor,
            payload,
            occurred_at,
            &request_id,
        )?;
        Ok(())
    })
}

struct RouterAgentLlmExecutor {
    db: DbOwner,
    llm_router: Arc<LlmRouter>,
    runtime_manager: Arc<Mutex<RuntimeManager>>,
}

fn provider_error_json(error: &ProviderError) -> Value {
    json!({
        "provider_error": {
            "code": error.code.to_string(),
            "message": error.message,
            "retryable": error.retryable,
        }
    })
}

impl RouterAgentLlmExecutor {
    fn new(
        db: DbOwner,
        llm_router: Arc<LlmRouter>,
        runtime_manager: Arc<Mutex<RuntimeManager>>,
    ) -> Self {
        Self {
            db,
            llm_router,
            runtime_manager,
        }
    }
}

impl AgentLlmExecutor for RouterAgentLlmExecutor {
    fn stream_llm(
        &self,
        session_id: &str,
        agent_id: &str,
        agent_name: &str,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> std::result::Result<(), ProviderError> {
        if let Err(error) = ensure_session_active_outside(&self.db, session_id) {
            return Err(ProviderError::new(
                ProviderErrorCode::BadRequest,
                error.message,
            ));
        }

        let estimated_prompt_tokens = estimate_messages_tokens(&request.messages);
        let token_reservation = format!("llm_agent_tokens:{agent_id}:{}", now_ms());
        let agent_token_reservation = format!("llm_agent_tokens_by_agent:{agent_id}:{}", now_ms());
        {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime
                .try_reserve_tokens(token_reservation.clone(), estimated_prompt_tokens)
                .map_err(|error| {
                    ProviderError::new(
                        ProviderErrorCode::ContextOverflow,
                        format!(
                            "Runtime token budget exceeded before agent provider call: requested {}, available {}",
                            error.requested, error.available
                        ),
                    )
                })?;
            runtime
                .try_reserve_agent_tokens(
                    agent_token_reservation.clone(),
                    agent_id,
                    estimated_prompt_tokens,
                )
                .map_err(|error| {
                    runtime.release_token_reservation(&token_reservation);
                    ProviderError::new(
                        ProviderErrorCode::ContextOverflow,
                        format!(
                            "Agent token budget exceeded before provider call: requested {}, available {}",
                            error.requested, error.available
                        ),
                    )
                })?;
        }

        let occurred_at = now_ms();
        let llm_call_id = format!("llm_agent_{agent_id}_{occurred_at}");
        let request_id: RequestId = format!("req_{llm_call_id}");
        let actor = Actor {
            kind: ActorKind::Agent,
            id: Some(agent_id.to_string()),
        };
        let model = request.model.clone();

        let mut stream = Vec::new();
        let route_result =
            self.llm_router
                .route_stream_with_sink(request, &mut |event: std::result::Result<
                    StreamEvent,
                    ProviderError,
                >| {
                    stream.push(event.clone());
                    sink.emit(event)
                });
        match route_result {
            Ok(()) => {
                let _ = persist_provider_health_snapshots(&self.db, &self.llm_router);
            }
            Err(error) => {
                let _ = persist_provider_health_snapshots(&self.db, &self.llm_router);
                let mut runtime = self.runtime_manager.lock().unwrap();
                runtime.release_token_reservation(&token_reservation);
                runtime.release_agent_token_reservation(&agent_token_reservation);
                return Err(error);
            }
        };

        let mut usage = json!({"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0});
        let mut finish_reason = "unknown".to_string();
        let mut tool_calls = Vec::new();
        for item in &stream {
            match item {
                Ok(StreamEvent::ToolCall(call)) => {
                    tool_calls.push(json!({
                        "tool_call_id": call.id,
                        "name": call.name,
                        "arguments": call.arguments,
                    }));
                }
                Ok(StreamEvent::Usage(token_usage)) => {
                    usage = json!({
                        "prompt_tokens": token_usage.prompt_tokens,
                        "completion_tokens": token_usage.completion_tokens,
                        "total_tokens": token_usage.total_tokens,
                        "reasoning_tokens": token_usage.reasoning_tokens,
                    });
                }
                Ok(StreamEvent::Finished(reason)) => {
                    finish_reason = format!("{reason:?}").to_lowercase();
                }
                Ok(_) => {}
                Err(error) => {
                    finish_reason = "error".into();
                    usage = json!({
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0,
                        "error": error.message,
                    });
                }
            }
        }

        let persist_result: std::result::Result<(), rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.to_string()))?;
                let mut events = Vec::new();
                events.push(simple_event(
                    tx,
                    session_id,
                    generation,
                    "llm.call_started",
                    actor.clone(),
                    json!({"llm_call_id": llm_call_id, "model": model, "agent_id": agent_id}),
                    occurred_at,
                    &request_id,
                )?);
                events.push(simple_event(
                    tx,
                    session_id,
                    generation,
                    "llm.call_finished",
                    actor.clone(),
                    json!({
                        "llm_call_id": llm_call_id,
                        "model": model,
                        "agent_id": agent_id,
                        "finish_reason": finish_reason,
                        "usage": usage,
                        "tool_calls": tool_calls,
                    }),
                    occurred_at,
                    &request_id,
                )?);

                let total_tokens = usage
                    .get("total_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if total_tokens > 0 {
                    tx.execute(
                        "INSERT INTO token_usage \
                         (session_id, agent_id, agent_name, model_name, prompt_tokens, completion_tokens, total_tokens, cache_read_tokens, cache_creation_tokens, timestamp) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0, 0, ?8)",
                        params![
                            session_id,
                            agent_id,
                            agent_name,
                            model,
                            usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0),
                            usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0),
                            total_tokens,
                            occurred_at as f64 / 1000.0
                        ],
                    )?;
                }
                Ok(())
            });

        if let Err(error) = persist_result {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime.release_token_reservation(&token_reservation);
            runtime.release_agent_token_reservation(&agent_token_reservation);
            return Err(ProviderError::new(
                ProviderErrorCode::Unknown,
                format!("agent LLM accounting persistence failed: {error}"),
            ));
        }

        let total_tokens = usage
            .get("total_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if total_tokens > 0 {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime.commit_token_reservation_actual(&token_reservation, total_tokens);
            runtime.commit_agent_token_reservation_actual(&agent_token_reservation, total_tokens);
        } else {
            let mut runtime = self.runtime_manager.lock().unwrap();
            runtime.release_token_reservation(&token_reservation);
            runtime.release_agent_token_reservation(&agent_token_reservation);
        }

        Ok(())
    }
}

fn persist_provider_health_snapshots(db: &DbOwner, router: &LlmRouter) -> Result<()> {
    let snapshots = router.provider_health_snapshots();
    if snapshots.is_empty() {
        return Ok(());
    }
    let updated_at = now_ms();
    db.with_transaction(|tx| {
        for snapshot in &snapshots {
            tx.execute(
                "INSERT INTO provider_health \
                 (provider_id, failure_count, last_failure_ms, circuit_open, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(provider_id) DO UPDATE SET \
                   failure_count = excluded.failure_count, \
                   last_failure_ms = excluded.last_failure_ms, \
                   circuit_open = excluded.circuit_open, \
                   updated_at = excluded.updated_at",
                params![
                    snapshot.provider_id,
                    snapshot.failure_count,
                    snapshot.last_failure_ms,
                    if snapshot.circuit_open { 1 } else { 0 },
                    updated_at
                ],
            )?;
        }
        Ok(())
    })
}

impl CommandRouter {
    pub fn new(db: DbOwner) -> Self {
        let event_log = EventLog::new(db.clone());
        let projection = ProjectionService::new(event_log.clone());
        let process_registry = ProcessRegistry::new(db.clone());
        let terminal_manager = TerminalManager::new(process_registry.clone());
        let repl_runner = ReplRunner::new(process_registry.clone());
        let document_tools = DocumentToolRunner::new(process_registry.clone());
        let mcp_bridge = McpBridgeRunner::new(process_registry.clone());
        let runtime_manager = Arc::new(Mutex::new(RuntimeManager::new()));
        let (agent_event_tx, agent_event_rx) = mpsc::channel();
        start_agent_pool_event_bridge(db.clone(), Arc::clone(&runtime_manager), agent_event_rx);
        Self {
            db,
            event_log,
            projection,
            llm_router: None,
            sidecar_scheduler: Arc::new(SidecarScheduler::with_process_registry(process_registry)),
            sidecar_commands: HashMap::new(),
            tool_registry: Arc::new(crate::tool::ToolRegistry::with_native_tools()),
            runtime_manager,
            message_bus: Arc::new(Mutex::new(MessageBus::new())),
            terminal_manager: Arc::new(terminal_manager),
            repl_runner: Arc::new(repl_runner),
            document_tools: Arc::new(document_tools),
            mcp_bridge: Arc::new(mcp_bridge),
            agent_pool: Arc::new(AgentPool::new(agent_event_tx)),
            allow_mock_llm_fallback: cfg!(test),
        }
    }

    pub fn with_llm_router(mut self, llm_router: LlmRouter) -> Self {
        self.llm_router = Some(Arc::new(llm_router));
        self
    }

    pub fn with_sidecar_command(
        mut self,
        tool_name: impl Into<String>,
        command: SidecarCommand,
    ) -> Self {
        self.sidecar_commands.insert(tool_name.into(), command);
        self
    }

    pub fn with_runtime_manager(self, runtime_manager: RuntimeManager) -> Self {
        *self.runtime_manager.lock().unwrap() = runtime_manager;
        self
    }

    pub fn with_message_bus_capacity(mut self, capacity: usize) -> Self {
        self.message_bus = Arc::new(Mutex::new(MessageBus::with_capacity(capacity)));
        self
    }

    pub fn with_test_mock_llm(mut self) -> Self {
        self.allow_mock_llm_fallback = true;
        self
    }

    pub fn without_mock_llm_fallback(mut self) -> Self {
        self.allow_mock_llm_fallback = false;
        self
    }

    fn route_llm_stream(
        &self,
        request: GenerateRequest,
    ) -> std::result::Result<
        Vec<std::result::Result<StreamEvent, crate::llm::ProviderError>>,
        crate::llm::ProviderError,
    > {
        if let Some(router) = &self.llm_router {
            let result = router.route_stream(request);
            let _ = persist_provider_health_snapshots(&self.db, router);
            return result;
        }

        if !self.allow_mock_llm_fallback {
            return Err(crate::llm::ProviderError::new(
                crate::llm::ProviderErrorCode::UnsupportedModel,
                format!(
                    "No LLM router configured for model '{}'; production mock fallback is disabled",
                    request.model
                ),
            ));
        }
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(
            MockLlmProvider::new("mock").with_models(vec![request.model.clone()]),
        ));
        LlmRouter::new(registry).route_stream(request)
    }

    fn llm_router_arc(&self, model: &str) -> std::result::Result<Arc<LlmRouter>, CoreError> {
        if let Some(router) = &self.llm_router {
            return Ok(Arc::clone(router));
        }
        if !self.allow_mock_llm_fallback {
            return Err(CoreError::with_details(
                ErrorCode::InvalidTransition,
                "No LLM router configured; production mock fallback is disabled",
                json!({"model": model}),
                false,
            ));
        }
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(
            MockLlmProvider::new("mock").with_models(vec![model.to_string()]),
        ));
        Ok(Arc::new(LlmRouter::new(registry)))
    }

    fn summarize_compacted_context(
        &self,
        session_id: &str,
        retain_last: usize,
        model: &str,
    ) -> Option<String> {
        self.llm_router.as_ref()?;
        let compacted = compacted_leader_context_text(&self.db, session_id, retain_last).ok()?;
        if compacted.trim().is_empty() {
            return None;
        }
        let request = GenerateRequest {
            model: model.to_string(),
            messages: vec![Message {
                role: "user".into(),
                content: format!(
                    "Summarize these conversation turns for future agent context. Preserve concrete facts, IDs, file paths, decisions, and unresolved tasks.\n\n{compacted}"
                ),
                ..Default::default()
            }],
            tools: Default::default(),
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };
        let mut summary = String::new();
        let stream = self.route_llm_stream(request).ok()?;
        for event in stream {
            match event.ok()? {
                StreamEvent::TextDelta(text) => summary.push_str(&text),
                StreamEvent::Finished(_) => break,
                StreamEvent::ThinkingDelta(_)
                | StreamEvent::ToolCallDelta(_)
                | StreamEvent::ToolCall(_)
                | StreamEvent::Usage(_) => {}
                StreamEvent::Error(_) => return None,
            }
        }
        let summary = summary.trim();
        (!summary.is_empty()).then(|| summary.to_string())
    }

    pub fn dispatch(&self, cmd: CommandEnvelope) -> CommandResponse {
        let method = cmd.method.clone();
        let request_id = cmd.request_id.clone();
        let trace_session_id = cmd.session_id.clone();
        let idempotency_key = cmd.idempotency_key.clone();
        let trace_start = now_ms();

        // Pre-transaction dedupe check (read-only; miss is safe).
        if let Some(ref key) = idempotency_key {
            if let Some(cached) = lookup_idempotent_outside_tx(&self.db, key, &method) {
                self.record_command_trace(
                    &request_id,
                    &method,
                    trace_session_id.as_deref(),
                    trace_start,
                    &cached,
                    true,
                );
                return cached;
            }
        }

        let response = match method.as_str() {
            SESSION_CREATE => self.handle_session_create(cmd),
            SESSION_INPUT => self.handle_session_input(cmd),
            SESSION_INTERRUPT => self.handle_session_transition(
                cmd,
                SESSION_INTERRUPT,
                SessionStatus::Interrupted,
                "session.interrupted",
            ),
            SESSION_RESUME => self.handle_session_transition(
                cmd,
                SESSION_RESUME,
                SessionStatus::Active,
                "session.resumed",
            ),
            SESSION_COMPLETE => self.handle_session_transition(
                cmd,
                SESSION_COMPLETE,
                SessionStatus::Completed,
                "session.completed",
            ),
            SESSION_FAIL => self.handle_session_transition(
                cmd,
                SESSION_FAIL,
                SessionStatus::Failed,
                "session.failed",
            ),
            SESSION_DELETE => self.handle_session_transition(
                cmd,
                SESSION_DELETE,
                SessionStatus::Deleted,
                "session.deleted",
            ),
            SESSION_LIST => self.handle_session_list(cmd),
            SESSION_RUN_TASK => self.handle_session_run_task(cmd),
            SESSION_SNAPSHOT => self.handle_session_snapshot(cmd),
            SESSION_CONNECT => self.handle_session_connect(cmd),
            CONV_LIST => self.handle_conv_list(cmd),
            EVENT_REPLAY => self.handle_event_replay(cmd),
            EVENT_COMPACT => self.handle_event_compact(cmd),
            RETENTION_SWEEP => self.handle_retention_sweep(cmd),
            TASK_CREATE => self.handle_task_create(cmd),
            TASK_ASSIGN => self.handle_task_assign(cmd),
            TASK_COMPLETE => {
                self.handle_task_terminal(cmd, TASK_COMPLETE, "completed", "task.completed")
            }
            TASK_FAIL => self.handle_task_terminal(cmd, TASK_FAIL, "failed", "task.failed"),
            TASK_REDISPATCH => self.handle_task_redispatch(cmd),
            TASK_REOPEN => self.handle_task_reopen(cmd),
            TASK_LIST => self.handle_task_list(cmd),
            PERMISSION_REQUEST => self.handle_permission_request(cmd),
            PERMISSION_RESOLVE => self.handle_permission_resolve(cmd),
            PERMISSION_SET_MODE => self.handle_permission_set_mode(cmd),
            AGENT_SPAWN => self.handle_agent_spawn(cmd, AGENT_SPAWN, "agent.spawned"),
            AGENT_START => {
                self.handle_agent_transition(cmd, AGENT_START, "running", "agent.started")
            }
            AGENT_COMPLETE => {
                self.handle_agent_transition(cmd, AGENT_COMPLETE, "stopped", "agent.completed")
            }
            AGENT_CRASH => {
                self.handle_agent_transition(cmd, AGENT_CRASH, "stopped", "agent.crashed")
            }
            AGENT_RESPAWN => self.handle_agent_spawn(cmd, AGENT_RESPAWN, "agent.spawned"),
            LEADER_PLAN => self.handle_leader_plan(cmd),
            LEADER_RUN => self.handle_leader_run(cmd),
            WORKFLOW_EXECUTE => self.handle_workflow_execute(cmd),
            WORKFLOW_PAUSE => self.handle_workflow_transition(
                cmd,
                WORKFLOW_PAUSE,
                "paused",
                "workflow.execution_paused",
            ),
            WORKFLOW_RESUME => self.handle_workflow_transition(
                cmd,
                WORKFLOW_RESUME,
                "running",
                "workflow.execution_resumed",
            ),
            WORKFLOW_CANCEL => self.handle_workflow_transition(
                cmd,
                WORKFLOW_CANCEL,
                "cancelled",
                "workflow.execution_cancelled",
            ),
            WORKFLOW_LIST => self.handle_workflow_list(cmd),
            TOOL_CALL => self.handle_tool_call(cmd),
            TOOL_CANCEL => self.handle_tool_cancel(cmd),
            LLM_CALL => self.handle_llm_call(cmd),
            RUNTIME_COMPACT => self.handle_runtime_compact(cmd),
            RUNTIME_DEBUG_DUMP => self.handle_runtime_debug_dump(cmd),
            BLACKBOARD_INTENT_CREATE => self.handle_blackboard_intent_create(cmd),
            BLACKBOARD_INTENT_CLAIM => self.handle_blackboard_intent_transition(
                cmd,
                BLACKBOARD_INTENT_CLAIM,
                "claimed",
                "blackboard.intent_claimed",
            ),
            BLACKBOARD_INTENT_RESOLVE => self.handle_blackboard_intent_transition(
                cmd,
                BLACKBOARD_INTENT_RESOLVE,
                "resolved",
                "blackboard.intent_resolved",
            ),
            GRAPH_QUERY => self.handle_graph_query(cmd),
            TEAM_SEND => self.handle_team_send(cmd),
            TEAM_POLL => self.handle_team_poll(cmd),
            TEAM_MARK_READ => self.handle_team_mark_read(cmd),
            MEMORY_UPSERT => self.handle_memory_upsert(cmd),
            MEMORY_SEARCH => self.handle_memory_search(cmd),
            EMBEDDING_UPSERT => self.handle_embedding_upsert(cmd),
            EMBEDDING_SEARCH => self.handle_embedding_search(cmd),
            BUS_PUBLISH => self.handle_bus_publish(cmd),
            BUS_POP => self.handle_bus_pop(cmd),
            BUS_DEAD_LETTERS => self.handle_bus_dead_letters(cmd),
            SCHEDULE_CREATE => self.handle_schedule_create(cmd),
            SCHEDULE_LIST => self.handle_schedule_list(cmd),
            SCHEDULE_FIRE => self.handle_schedule_fire(cmd, false),
            SCHEDULE_FIRE_DUE => self.handle_schedule_fire(cmd, true),
            ASSUMPTION_CREATE => self.handle_assumption_create(cmd),
            ASSUMPTION_LIST => self.handle_assumption_list(cmd),
            ASSUMPTION_VERIFY => self.handle_assumption_resolve(cmd, true),
            ASSUMPTION_FALSIFY => self.handle_assumption_resolve(cmd, false),
            TRACE_TIMELINE => self.handle_trace_timeline(cmd),
            METRICS_QUERY => self.handle_metrics_query(cmd),
            WORKTREE_CREATE => self.handle_worktree_create(cmd),
            WORKTREE_DELETE => self.handle_worktree_delete(cmd),
            WORKTREE_LIST => self.handle_worktree_list(cmd),
            TERMINAL_CREATE => self.handle_terminal_create(cmd),
            TERMINAL_SEND => self.handle_terminal_send(cmd),
            TERMINAL_READ => self.handle_terminal_read(cmd),
            TERMINAL_KILL => self.handle_terminal_kill(cmd),
            REPL_EVAL => self.handle_repl_eval(cmd),
            REPL_CREATE => self.handle_repl_create(cmd),
            REPL_SEND => self.handle_repl_send(cmd),
            REPL_READ => self.handle_repl_read(cmd),
            REPL_KILL => self.handle_repl_kill(cmd),
            PARSE_FILE => self.handle_parse_file(cmd),
            OCR_EXTRACT_TEXT => self.handle_ocr_extract_text(cmd),
            MCP_BRIDGE => self.handle_mcp_bridge(cmd),
            MCP_SERVER_START => self.handle_mcp_server_start(cmd),
            MCP_LIST_TOOLS => self.handle_mcp_list_tools(cmd),
            MCP_CALL_TOOL => self.handle_mcp_call_tool(cmd),
            MCP_SERVER_STOP => self.handle_mcp_server_stop(cmd),
            _ => CommandResponse::err(
                cmd.request_id,
                CoreError::new(
                    ErrorCode::InvalidTransition,
                    format!("Unknown method: {method}"),
                ),
            ),
        };
        self.record_command_trace(
            &request_id,
            &method,
            trace_session_id.as_deref(),
            trace_start,
            &response,
            false,
        );
        response
    }

    fn record_command_trace(
        &self,
        request_id: &str,
        method: &str,
        session_id: Option<&str>,
        start_ms: Timestamp,
        response: &CommandResponse,
        idempotent_cache_hit: bool,
    ) {
        let end_ms = now_ms();
        let status = if response.success { "ok" } else { "error" };
        let attrs = json!({
            "request_id": request_id,
            "method": method,
            "success": response.success,
            "event_count": response.events.len(),
            "has_result": response.result.is_some(),
            "error_code": response.error.as_ref().map(|error| error.code.to_string()),
            "idempotent_cache_hit": idempotent_cache_hit,
        });
        let conn = self.db.conn();
        let project_root = session_id
            .and_then(|sid| {
                conn.query_row(
                    "SELECT workspace FROM sessions WHERE id = ?1",
                    params![sid],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .ok()
                .flatten()
            })
            .unwrap_or_else(|| "unknown".into());
        let _ = conn.execute(
            "INSERT OR REPLACE INTO traces \
             (trace_id, span_id, parent_span_id, operation, start_ts, end_ts, status, attributes, session_id, agent_id) \
             VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, NULL)",
            params![
                request_id,
                format!("cmd_{request_id}"),
                method,
                start_ms,
                end_ms,
                status,
                attrs.to_string(),
                session_id,
            ],
        );
        let _ = conn.execute(
            "INSERT OR REPLACE INTO execution_trace_events \
             (id, project_root, session_id, task_id, agent_id, agent_name, agent_role, task_type, status, duration_ms, files_changed, error_signature, fix_pattern, verification, metadata, created_at) \
             VALUES (?1, ?2, ?3, NULL, NULL, NULL, NULL, 'command', ?4, ?5, '[]', ?6, NULL, NULL, ?7, ?8)",
            params![
                format!("exec_{request_id}"),
                project_root,
                session_id,
                status,
                end_ms.saturating_sub(start_ms),
                response.error.as_ref().map(|error| error.code.to_string()),
                attrs.to_string(),
                end_ms as f64 / 1000.0,
            ],
        );
    }

    // -----------------------------------------------------------------------
    // Session create  (atomic: session row + event + dedupe cache)
    // -----------------------------------------------------------------------

    fn handle_session_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let workspace = cmd
            .params
            .get("workspace")
            .and_then(|v| v.as_str())
            .unwrap_or("/default")
            .to_string();
        let session_id = cmd
            .params
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("sess_{}", now_ms()));
        let occurred_at = now_ms();
        let generation: Generation = 1;
        let idempotency_key = cmd.idempotency_key.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                // In-transaction dedupe check (guards against missed pre-check)
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, SESSION_CREATE)? {
                        return Ok(cached);
                    }
                }

                // Existing session idempotency
                let existing: Option<String> = tx
                    .query_row(
                        "SELECT id FROM sessions WHERE id = ?1",
                        params![session_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if existing.is_some() {
                    let latest_seq: Seq = tx
                        .query_row(
                            "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
                            params![session_id],
                            |row| row.get(0),
                        )
                        .optional()?
                        .unwrap_or(0);
                    return Ok(CommandResponse::ok(
                        request_id.clone(),
                        Some(json!({
                            "session_id": session_id,
                            "latest_seq": latest_seq,
                            "generation": generation,
                            "status": "active",
                        })),
                        Some(latest_seq),
                    ));
                }

                // Insert session row
                let insert_ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "INSERT INTO sessions (id, created_at, workspace, status) \
                     VALUES (?1, ?2, ?3, 'active')",
                    params![session_id, insert_ts, workspace],
                )?;

                // Append canonical event
                let event_id = format!("session_created_{session_id}");
                if let Some(dup) = try_fetch_event_by_id(tx, &event_id)? {
                    let dup_seq = dup.seq;
                    return Ok(CommandResponse::with_event(
                        request_id.clone(),
                        dup,
                        Some(json!({
                            "session_id": session_id,
                            "latest_seq": dup_seq,
                            "generation": generation,
                            "status": "active",
                        })),
                    ));
                }

                ensure_meta(tx, &Some(session_id.clone()))?;
                let next_seq = allocate_seq(tx, &Some(session_id.clone()))?;

                let mut event = EventEnvelope {
                    event_id,
                    session_id: Some(session_id.clone()),
                    seq: 0,
                    generation,
                    event_type: "session.created".into(),
                    source: cmd.actor,
                    payload: json!({
                        "session_id": session_id,
                        "workspace": workspace,
                    }),
                    occurred_at,
                    causation_id: Some(cmd.request_id.clone()),
                    correlation_id: Some(cmd.request_id),
                };
                event.seq = next_seq;
                insert_event(tx, &event)?;
                update_meta_seq(tx, &Some(session_id.clone()), next_seq)?;

                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "latest_seq": next_seq,
                        "generation": generation,
                        "status": "active",
                    })),
                );

                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, SESSION_CREATE, &response)?;
                }

                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("session.create failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Session input  (atomic: validation + event + conversation + dedupe)
    // -----------------------------------------------------------------------

    fn handle_session_input(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let content = match cmd.params.get("content").and_then(|v| v.as_str()) {
            Some(c) => c.to_string(),
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing content"),
                );
            }
        };
        let occurred_at = now_ms();
        let idempotency_key = cmd.idempotency_key.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                // In-transaction dedupe check
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, SESSION_INPUT)? {
                        return Ok(cached);
                    }
                }

                // Validate session exists and is not terminal
                let status: Option<String> = tx
                    .query_row(
                        "SELECT status FROM sessions WHERE id = ?1",
                        params![session_id],
                        |row| row.get(0),
                    )
                    .optional()?;

                let _status_s = match status {
                    None => {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::session_not_found(&session_id),
                        ));
                    }
                    Some(s) if session_status_from_str(&s).is_terminal() => {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::session_already_terminal(&session_id),
                        ));
                    }
                    Some(s) => s,
                };

                let current_gen = get_current_generation(tx, &Some(session_id.clone()))?;

                // Append canonical event
                let event_id = match &idempotency_key {
                    Some(key) => format!("cmd_{key}"),
                    None => format!("session_input_{session_id}_{occurred_at}_{request_id}"),
                };
                if let Some(dup) = try_fetch_event_by_id(tx, &event_id)? {
                    let dup_seq = dup.seq;
                    let dup_gen = dup.generation;
                    return Ok(CommandResponse::with_event(
                        request_id.clone(),
                        dup,
                        Some(json!({
                            "session_id": session_id,
                            "latest_seq": dup_seq,
                            "generation": dup_gen,
                        })),
                    ));
                }

                ensure_meta(tx, &Some(session_id.clone()))?;
                let next_seq = allocate_seq(tx, &Some(session_id.clone()))?;

                let mut event = EventEnvelope {
                    event_id,
                    session_id: Some(session_id.clone()),
                    seq: 0,
                    generation: current_gen,
                    event_type: "session.input_received".into(),
                    source: cmd.actor,
                    payload: json!({
                        "session_id": session_id,
                        "content": content,
                    }),
                    occurred_at,
                    causation_id: Some(cmd.request_id.clone()),
                    correlation_id: Some(cmd.request_id),
                };
                event.seq = next_seq;
                insert_event(tx, &event)?;
                update_meta_seq(tx, &Some(session_id.clone()), next_seq)?;

                // Write conversation record (same transaction)
                let insert_ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "INSERT INTO leader_conversation (session_id, role, content, timestamp) \
                     VALUES (?1, 'user', ?2, ?3)",
                    params![session_id, content, insert_ts],
                )?;

                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "latest_seq": next_seq,
                        "generation": current_gen,
                    })),
                );

                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, SESSION_INPUT, &response)?;
                }

                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("session.input failed: {e}")),
            )
        })
    }

    fn handle_session_run_task(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = session_id_from_cmd(&cmd).unwrap_or_else(|| format!("sess_{}", now_ms()));
        let task_id =
            string_param(&cmd, &["task_id", "id"]).unwrap_or_else(|| format!("task_{}", now_ms()));
        let content = string_param(&cmd, &["content", "task", "prompt"]).unwrap_or_default();
        if content.trim().is_empty() {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing task content"),
            );
        }
        let workspace = string_param(&cmd, &["workspace"]).unwrap_or_else(|| "/default".into());
        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "mock/model".into());
        let provider_id = string_param(&cmd, &["provider", "provider_id"]).unwrap_or_else(|| {
            if self.llm_router.is_some() {
                "external".into()
            } else {
                "mock".into()
            }
        });
        let idempotency_key = cmd.idempotency_key.clone();
        let actor = cmd.actor.clone();
        let occurred_at = now_ms();

        if let Some(ref key) = idempotency_key {
            if let Some(cached) = lookup_idempotent_outside_tx(&self.db, key, SESSION_RUN_TASK) {
                return cached;
            }
        }

        let auth_context = auth_context_from_cmd(&cmd).unwrap_or(AuthContext::None);
        let request_options = request_options_from_cmd(&cmd);
        let mut response_events = Vec::new();

        let started: std::result::Result<Option<CommandResponse>, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, SESSION_RUN_TASK)? {
                        return Ok(Some(cached));
                    }
                }

                let existing_session: Option<String> = tx
                    .query_row(
                        "SELECT status FROM sessions WHERE id = ?1",
                        params![session_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if matches!(
                    existing_session.as_deref().map(session_status_from_str),
                    Some(status) if status.is_terminal()
                ) {
                    return Ok(Some(CommandResponse::err(
                        request_id.clone(),
                        CoreError::session_already_terminal(&session_id),
                    )));
                }
                if existing_session.is_none() {
                    tx.execute(
                        "INSERT INTO sessions (id, created_at, workspace, status) \
                         VALUES (?1, ?2, ?3, 'active')",
                        params![session_id, occurred_at as f64 / 1000.0, workspace],
                    )?;
                }

                ensure_meta(tx, &Some(session_id.clone()))?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?.max(1);
                if existing_session.is_none() {
                    response_events.push(append_event_in_tx(
                        tx,
                        Some(session_id.clone()),
                        generation,
                        "session.created",
                        actor.clone(),
                        json!({"session_id": session_id, "workspace": workspace}),
                        occurred_at,
                        Some(request_id.clone()),
                        Some(request_id.clone()),
                        format!("session_run_task_created_{session_id}_{request_id}"),
                    )?);
                }
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "session.input_received",
                    actor.clone(),
                    json!({"session_id": session_id, "content": content}),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("session_run_task_input_{session_id}_{request_id}"),
                )?);
                tx.execute(
                    "INSERT INTO leader_conversation (session_id, role, content, timestamp) \
                     VALUES (?1, 'user', ?2, ?3)",
                    params![session_id, content, occurred_at as f64 / 1000.0],
                )?;
                tx.execute(
                    "INSERT INTO tasks \
                     (id, session_id, subject, description, status, run_generation, agent_type, \
                      assigned_agent, result, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, '', 'running', 1, 'core', 'core-runtime', NULL, ?4, ?4)",
                    params![
                        task_id,
                        session_id,
                        content,
                        occurred_at as f64 / 1000.0
                    ],
                )?;
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "task.created",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "subject": content,
                        "status": "dispatchable",
                    }),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                        format!("session_run_task_task_created_{session_id}_{task_id}"),
                    )?);
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "task.assigned",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "assigned_agent": "core-runtime",
                        "run_generation": 1,
                        "status": "running",
                    }),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                        format!("session_run_task_task_assigned_{session_id}_{task_id}"),
                    )?);
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "llm.call_started",
                    actor.clone(),
                    json!({"session_id": session_id, "llm_call_id": format!("llm_{task_id}"), "model": model}),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("session_run_task_llm_started_{session_id}_{task_id}"),
                    )?);
                Ok(None)
            });

        match started {
            Ok(Some(response)) => return response,
            Ok(None) => {}
            Err(e) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("session.run_task start failed: {e}")),
                );
            }
        }

        let mut messages = vec![Message {
            role: "user".into(),
            content: content.clone(),
            ..Default::default()
        }];
        let max_rounds = cmd
            .params
            .get("max_rounds")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(4);
        let tool_executor = RouterAgentToolExecutor::new(
            self.db.clone(),
            self.tool_registry.clone(),
            self.runtime_manager.clone(),
        );
        let mut answer = String::new();
        let mut realtime = Vec::new();
        let mut usage = json!({"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0});
        let mut finish_reason = "unknown".to_string();

        for _round in 0..max_rounds {
            let llm_request = GenerateRequest {
                model: model.clone(),
                messages: messages.clone(),
                tools: self.tool_registry.llm_tool_definitions(),
                stream: true,
                auth_context: auth_context.clone(),
                options: request_options.clone(),
            };
            let stream = match self.route_llm_stream(llm_request) {
                Ok(stream) => stream,
                Err(err) => {
                    return CommandResponse::err(
                        request_id,
                        CoreError::with_details(
                            ErrorCode::Internal,
                            "LLM provider routing failed",
                            provider_error_json(&err),
                            err.retryable,
                        ),
                    );
                }
            };

            let mut assistant_text = String::new();
            let mut tool_calls = Vec::new();
            let mut tool_call_accumulator = ToolCallAccumulator::new();
            for item in stream {
                match item {
                    Ok(StreamEvent::ThinkingDelta(text)) => realtime.push(json!({
                        "event_type": "realtime.llm.thinking_delta",
                        "payload": {"text": text},
                    })),
                    Ok(StreamEvent::TextDelta(text)) => {
                        assistant_text.push_str(&text);
                        realtime.push(json!({
                            "event_type": "realtime.llm.text_delta",
                            "payload": {"text": text},
                        }));
                    }
                    Ok(StreamEvent::ToolCallDelta(delta)) => {
                        realtime.push(json!({
                            "event_type": "realtime.llm.tool_call_delta",
                            "payload": {
                                "index": delta.index,
                                "tool_call_id": delta.id,
                                "name": delta.name,
                                "args_delta": delta.partial_json,
                            },
                        }));
                        tool_call_accumulator.append(delta);
                    }
                    Ok(StreamEvent::ToolCall(call)) => tool_calls.push(call),
                    Ok(StreamEvent::Usage(token_usage)) => {
                        usage = json!({
                            "prompt_tokens": token_usage.prompt_tokens,
                            "completion_tokens": token_usage.completion_tokens,
                            "total_tokens": token_usage.total_tokens,
                            "reasoning_tokens": token_usage.reasoning_tokens,
                        });
                    }
                    Ok(StreamEvent::Finished(reason)) => {
                        finish_reason = format!("{:?}", reason).to_lowercase();
                        if matches!(reason, crate::llm::FinishReason::ToolCalls) {
                            tool_calls.extend(tool_call_accumulator.finalize());
                        }
                    }
                    Ok(StreamEvent::Error(err)) | Err(err) => {
                        return CommandResponse::err(
                            request_id,
                            CoreError::with_details(
                                ErrorCode::Internal,
                                "LLM provider stream failed",
                                provider_error_json(&err),
                                err.retryable,
                            ),
                        );
                    }
                }
            }
            tool_calls.extend(tool_call_accumulator.finalize());
            dedupe_tool_calls_by_id(&mut tool_calls);
            messages.push(Message {
                role: "assistant".into(),
                content: assistant_text.clone(),
                tool_calls: tool_calls.clone(),
                ..Default::default()
            });

            if tool_calls.is_empty() {
                answer = assistant_text;
                break;
            }

            let mut completed_by_tool = false;
            for tool_call in tool_calls {
                if tool_call.name == "attempt_completion" {
                    answer = tool_call
                        .arguments
                        .get("result")
                        .and_then(Value::as_str)
                        .unwrap_or(&assistant_text)
                        .to_string();
                    completed_by_tool = true;
                    break;
                }
                let tool_result =
                    tool_executor.execute_tool(&session_id, "core-runtime", &tool_call);
                let tool_observation = if tool_result.success {
                    tool_result.output
                } else {
                    json!({"error": tool_result.error})
                };
                messages.push(Message {
                    role: "tool".into(),
                    content: tool_observation.to_string(),
                    tool_call_id: Some(tool_call.id),
                    name: Some(tool_call.name),
                    ..Default::default()
                });
            }
            if completed_by_tool {
                break;
            }
        }

        if answer.is_empty() && finish_reason == "toolcalls" {
            answer = "max tool-call rounds exhausted".into();
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                let generation = get_current_generation(tx, &Some(session_id.clone()))?.max(1);
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "llm.call_finished",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "llm_call_id": format!("llm_{task_id}"),
                        "model": model,
                        "finish_reason": finish_reason,
                        "usage": usage,
                    }),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("session_run_task_llm_finished_{session_id}_{task_id}"),
                )?);
                tx.execute(
                    "UPDATE tasks SET status = 'terminal', result = ?1, updated_at = ?2 \
                     WHERE id = ?3 AND session_id = ?4",
                    params![
                        json!({"answer": answer, "finish_reason": finish_reason}).to_string(),
                        now_ms() as f64 / 1000.0,
                        task_id,
                        session_id
                    ],
                )?;
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "task.completed",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "run_generation": 1,
                        "status": "terminal",
                        "exit_reason": "completed",
                        "result": {"answer": answer},
                    }),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                        format!("session_run_task_task_completed_{session_id}_{task_id}"),
                    )?);
                tx.execute(
                    "UPDATE sessions SET status = 'completed', summary = ?1 WHERE id = ?2",
                    params![answer, session_id],
                )?;
                response_events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "session.completed",
                    actor.clone(),
                    json!({"session_id": session_id, "status": "completed"}),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("session_run_task_session_completed_{session_id}_{request_id}"),
                )?);
                tx.execute(
                    "INSERT INTO llm_gateway_requests \
                     (trace_id, session_id, agent_id, agent_name, requested_model, selected_model, final_model, provider, status, prompt_tokens, completion_tokens, total_tokens, created_at) \
                     VALUES (?1, ?2, '', 'core-runtime', ?3, ?3, ?3, ?4, 'completed', ?5, ?6, ?7, ?8)",
                    params![
                        format!("llm_{task_id}"),
                        session_id,
                        model,
                        provider_id,
                        usage.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
                        usage.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
                        usage.get("total_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
                        occurred_at as f64 / 1000.0,
                    ],
                )?;

                let response = response_with_events(
                    request_id.clone(),
                    response_events,
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "status": "completed",
                        "answer": answer,
                        "finish_reason": finish_reason,
                        "usage": usage,
                        "realtime_events": realtime,
                    }),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, SESSION_RUN_TASK, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("session.run_task failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Session lifecycle transitions
    // -----------------------------------------------------------------------

    fn handle_session_transition(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        target: SessionStatus,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let occurred_at = now_ms();
        let idempotency_key = cmd.idempotency_key.clone();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }

                let status: Option<String> = tx
                    .query_row(
                        "SELECT status FROM sessions WHERE id = ?1",
                        params![session_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let current_raw = match status {
                    Some(s) => s,
                    None => {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::session_not_found(&session_id),
                        ));
                    }
                };
                let current = session_status_from_str(&current_raw);
                if !current.can_transition_to(target) {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(
                            current_raw,
                            session_status_to_str(target).to_string(),
                        ),
                    ));
                }

                tx.execute(
                    "UPDATE sessions SET status = ?1 WHERE id = ?2",
                    params![session_status_to_str(target), session_id],
                )?;

                let current_gen = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    current_gen,
                    event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "status": session_status_to_str(target),
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!(
                                "{}_{}_{}_{}",
                                event_type.replace('.', "_"),
                                session_id,
                                occurred_at,
                                request_id
                            )
                        }),
                )?;

                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "latest_seq": event_seq,
                        "generation": current_gen,
                        "status": session_status_to_str(target),
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Task lifecycle
    // -----------------------------------------------------------------------

    fn handle_task_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let task_id = cmd
            .params
            .get("task_id")
            .or_else(|| cmd.params.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("task_{}", now_ms()));
        let subject = cmd
            .params
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("Untitled task")
            .to_string();
        let description = cmd
            .params
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let agent_type = cmd
            .params
            .get("agent_type")
            .and_then(|v| v.as_str())
            .unwrap_or("general")
            .to_string();
        let blocked_by = normalize_task_dependencies(cmd.params.get("blocked_by"));
        let blocked_by_json = if blocked_by.is_empty() {
            None
        } else {
            Some(json!(blocked_by).to_string())
        };
        let initial_status = if blocked_by_json.is_some() {
            "blocked"
        } else {
            "dispatchable"
        };
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TASK_CREATE)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;

                let exists: Option<String> = tx
                    .query_row(
                        "SELECT id FROM tasks WHERE id = ?1 AND session_id = ?2",
                        params![task_id, session_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if exists.is_some() {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Task already exists: {task_id}"),
                        ),
                    ));
                }

                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "INSERT INTO tasks \
                     (id, session_id, subject, description, status, run_generation, agent_type, \
                      blocked_by, blocked_reason, assigned_agent, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8, '', ?9, ?9)",
                    params![
                        task_id,
                        session_id,
                        subject,
                        description,
                        initial_status,
                        agent_type,
                        blocked_by_json,
                        blocked_by_json.as_ref().map(|_| "blocked_by dependency"),
                        ts
                    ],
                )?;

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "task.created",
                    actor,
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "subject": subject,
                        "description": description,
                        "status": initial_status,
                        "blocked_by": blocked_by_json
                            .as_deref()
                            .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| format!("task_created_{session_id}_{task_id}")),
                )?;

                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "latest_seq": event_seq,
                        "status": initial_status,
                        "blocked_by": blocked_by_json
                            .as_deref()
                            .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
                        "run_generation": 0,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TASK_CREATE, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("task.create failed: {e}")),
            )
        })
    }

    fn handle_task_assign(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, task_id) = match task_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or task_id",
                    ),
                );
            }
        };
        let assigned_agent_param = cmd
            .params
            .get("agent_id")
            .or_else(|| cmd.params.get("assigned_agent"))
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TASK_ASSIGN)? {
                        return Ok(cached);
                    }
                }
                let (status, run_generation) =
                    load_task_status_generation(tx, &session_id, &task_id, &request_id)?;
                if status != "dispatchable" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(status, "running"),
                    ));
                }
                let assigned_agent = match assigned_agent_param.clone() {
                    Some(agent) => agent,
                    None => route_agent_for_task_type(tx, &session_id, &task_id)?,
                };
                let next_generation = run_generation + 1;
                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "UPDATE tasks SET status = 'running', assigned_agent = ?1, \
                     run_generation = ?2, updated_at = ?3 \
                     WHERE id = ?4 AND session_id = ?5",
                    params![assigned_agent, next_generation, ts, task_id, session_id],
                )?;

                let session_generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    session_generation,
                    "task.assigned",
                    actor,
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "assigned_agent": assigned_agent,
                        "run_generation": next_generation,
                        "status": "running",
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!("task_assigned_{session_id}_{task_id}_{next_generation}")
                        }),
                )?;
                let event_seq = event.seq;

                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "latest_seq": event_seq,
                        "status": "running",
                        "run_generation": next_generation,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TASK_ASSIGN, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("task.assign failed: {e}")),
            )
        })
    }

    fn handle_task_terminal(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        exit_reason: &'static str,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, task_id) = match task_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or task_id",
                    ),
                );
            }
        };
        let result = cmd.params.get("result").cloned();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                let (status, run_generation) =
                    load_task_status_generation(tx, &session_id, &task_id, &request_id)?;
                if status == "terminal" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::task_already_terminal(&task_id),
                    ));
                }
                if status != "running" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(status, "terminal"),
                    ));
                }

                // Generation gate: reject late results from stale agents
                if let Some(caller_generation) = cmd.params.get("run_generation").and_then(|v| v.as_i64()) {
                    if caller_generation < run_generation {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::with_details(
                                ErrorCode::InvalidTransition,
                                "Task result rejected: stale generation",
                                json!({
                                    "task_id": task_id,
                                    "caller_generation": caller_generation,
                                    "current_generation": run_generation,
                                    "reason": "task was redispatched; this result is from an older run"
                                }),
                                false,
                            ),
                        ));
                    }
                }

                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "UPDATE tasks SET status = 'terminal', exit_reason = ?1, result = ?2, \
                     updated_at = ?3 WHERE id = ?4 AND session_id = ?5",
                    params![
                        exit_reason,
                        result.as_ref().map(|v| v.to_string()),
                        ts,
                        task_id,
                        session_id
                    ],
                )?;

                let session_generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    session_generation,
                    event_type,
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "run_generation": run_generation,
                        "status": "terminal",
                        "exit_reason": exit_reason,
                        "result": result,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!("{event_type}_{session_id}_{task_id}_{run_generation}")
                                .replace('.', "_")
                        }),
                )?;
                let mut events = vec![event];
                if exit_reason == "completed" {
                    let unblocked = unblock_dependent_tasks(
                        tx,
                        &session_id,
                        &task_id,
                        session_generation,
                        actor.clone(),
                        occurred_at,
                        &request_id,
                    )?;
                    events.extend(unblocked);
                }
                let event_seq = events
                    .last()
                    .map(|event| event.seq)
                    .unwrap_or(session_generation);

                let response = response_with_events(
                    request_id.clone(),
                    events,
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "latest_seq": event_seq,
                        "status": "terminal",
                        "exit_reason": exit_reason,
                        "run_generation": run_generation,
                    }),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        })
    }

    fn handle_task_redispatch(&self, cmd: CommandEnvelope) -> CommandResponse {
        self.handle_task_reset(cmd, TASK_REDISPATCH, "task.redispatched", false)
    }

    fn handle_task_reopen(&self, cmd: CommandEnvelope) -> CommandResponse {
        self.handle_task_reset(cmd, TASK_REOPEN, "task.reopened", true)
    }

    fn handle_task_reset(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        event_type: &'static str,
        require_terminal: bool,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, task_id) = match task_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or task_id",
                    ),
                );
            }
        };
        let reason = cmd
            .params
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or(if require_terminal {
                "reopen"
            } else {
                "redispatch"
            })
            .to_string();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                let (status, run_generation) =
                    load_task_status_generation(tx, &session_id, &task_id, &request_id)?;
                if require_terminal && status != "terminal" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(status, "dispatchable"),
                    ));
                }
                if !require_terminal && status != "running" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(status, "dispatchable"),
                    ));
                }
                let next_generation = run_generation + 1;
                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "UPDATE tasks SET status = 'dispatchable', exit_reason = NULL, result = NULL, \
                     assigned_agent = '', run_generation = ?1, updated_at = ?2 \
                     WHERE id = ?3 AND session_id = ?4",
                    params![next_generation, ts, task_id, session_id],
                )?;

                let session_generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    session_generation,
                    event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "reason": reason,
                        "old_run_generation": run_generation,
                        "run_generation": next_generation,
                        "status": "dispatchable",
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!(
                                "{}_{}_{}_{}",
                                event_type.replace('.', "_"),
                                session_id,
                                task_id,
                                next_generation
                            )
                        }),
                )?;
                let event_seq = event.seq;

                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "task_id": task_id,
                        "latest_seq": event_seq,
                        "status": "dispatchable",
                        "run_generation": next_generation,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Permission gate
    // -----------------------------------------------------------------------

    fn handle_permission_request(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let permission_request_id = cmd
            .params
            .get("permission_request_id")
            .or_else(|| cmd.params.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("perm_{}", now_ms()));
        let tool_name = cmd
            .params
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown_tool")
            .to_string();
        let args = cmd.params.get("args").cloned().unwrap_or_else(|| json!({}));
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, PERMISSION_REQUEST)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let mode =
                    current_permission_mode(tx, &session_id)?.unwrap_or_else(|| "strict".into());

                tx.execute(
                    "INSERT INTO permission_requests \
                     (id, session_id, tool_name, args_json, mode, status, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
                    params![
                        permission_request_id,
                        session_id,
                        tool_name,
                        args.to_string(),
                        mode,
                        occurred_at
                    ],
                )?;

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "permission.request_created",
                    actor,
                    json!({
                        "session_id": session_id,
                        "permission_request_id": permission_request_id,
                        "tool_name": tool_name,
                        "args": args,
                        "mode": mode,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!(
                                "permission_request_created_{session_id}_{permission_request_id}"
                            )
                        }),
                )?;
                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "permission_request_id": permission_request_id,
                        "latest_seq": event_seq,
                        "status": "pending",
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, PERMISSION_REQUEST, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("permission.request failed: {e}")),
            )
        })
    }

    fn handle_permission_resolve(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let permission_request_id = match cmd
            .params
            .get("permission_request_id")
            .or_else(|| cmd.params.get("id"))
            .and_then(|v| v.as_str())
        {
            Some(id) => id.to_string(),
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing permission_request_id",
                    ),
                );
            }
        };
        let decision = cmd
            .params
            .get("decision")
            .and_then(|v| v.as_str())
            .unwrap_or("deny")
            .to_string();
        let reason = cmd
            .params
            .get("reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, PERMISSION_RESOLVE)? {
                        return Ok(cached);
                    }
                }
                let row: Option<(String, String, String, String, String)> = tx
                    .query_row(
                        "SELECT session_id, tool_name, mode, status, args_json FROM permission_requests WHERE id = ?1",
                        params![permission_request_id],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get(4)?,
                            ))
                        },
                    )
                    .optional()?;
                let (session_id, tool_name, mode, status, args_json) = match row {
                    Some(row) => row,
                    None => {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::permission_denied(format!(
                                "Permission request not found: {permission_request_id}"
                            )),
                        ));
                    }
                };
                if status != "pending" {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Permission request already resolved: {permission_request_id}"),
                        ),
                    ));
                }
                tx.execute(
                    "UPDATE permission_requests SET status = 'resolved', decision = ?1, reason = ?2, \
                     resolved_at = ?3 WHERE id = ?4",
                    params![decision, reason, occurred_at, permission_request_id],
                )?;
                if decision == "allow" {
                    let scope = permission_scope_from_args_json(&args_json);
                    tx.execute(
                        "INSERT OR REPLACE INTO permission_grants \
                         (session_id, tool_name, mode, scope, granted_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![session_id, tool_name, mode, scope, occurred_at],
                    )?;
                }

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "permission.request_resolved",
                    actor,
                    json!({
                        "session_id": session_id,
                        "permission_request_id": permission_request_id,
                        "tool_name": tool_name,
                        "decision": decision,
                        "reason": reason,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!("permission_request_resolved_{permission_request_id}")
                        }),
                )?;
                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "permission_request_id": permission_request_id,
                        "latest_seq": event_seq,
                        "decision": decision,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, PERMISSION_RESOLVE, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("permission.resolve failed: {e}")),
            )
        })
    }

    fn handle_permission_set_mode(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let new_mode = cmd
            .params
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("strict")
            .to_string();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, PERMISSION_SET_MODE)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let old_mode = current_permission_mode(tx, &session_id)?.unwrap_or_else(|| "dev".into());
                let mode_generation = current_permission_generation(tx, &session_id)? + 1;
                tx.execute(
                    "INSERT INTO permission_modes (session_id, mode, generation, updated_at) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(session_id) DO UPDATE SET \
                     mode = excluded.mode, generation = excluded.generation, updated_at = excluded.updated_at",
                    params![session_id, new_mode, mode_generation, occurred_at],
                )?;

                let grants: Vec<String> = {
                    let mut stmt = tx.prepare(
                        "SELECT tool_name FROM permission_grants WHERE session_id = ?1 ORDER BY tool_name",
                    )?;
                    let rows = stmt.query_map(params![session_id], |row| row.get(0))?;
                    rows.collect::<Result<Vec<String>>>()?
                };
                tx.execute(
                    "DELETE FROM permission_grants WHERE session_id = ?1",
                    params![session_id],
                )?;

                let session_generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let mode_event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    session_generation,
                    "permission.mode_changed",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "old_mode": old_mode,
                        "new_mode": new_mode,
                        "generation": mode_generation,
                    }),
                    occurred_at,
                    Some(causation_id.clone()),
                    Some(correlation_id.clone()),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!("permission_mode_changed_{session_id}_{mode_generation}")
                        }),
                )?;
                let mut events = vec![mode_event];
                for tool_name in grants {
                    let event = append_event_in_tx(
                        tx,
                        Some(session_id.clone()),
                        session_generation,
                        "permission.grant_revoked",
                        actor.clone(),
                        json!({
                            "session_id": session_id,
                            "tool_name": tool_name,
                            "reason": "mode_changed",
                        }),
                        occurred_at,
                        Some(causation_id.clone()),
                        Some(correlation_id.clone()),
                        format!(
                            "permission_grant_revoked_{}_{}_{}",
                            session_id, tool_name, mode_generation
                        ),
                    )?;
                    events.push(event);
                }
                let latest_seq = events.last().map(|event| event.seq);
                let response = CommandResponse {
                    request_id: request_id.clone(),
                    success: true,
                    result: Some(json!({
                        "session_id": session_id,
                        "old_mode": old_mode,
                        "new_mode": new_mode,
                        "generation": mode_generation,
                        "revoked_grants": events.len().saturating_sub(1),
                    })),
                    error: None,
                    events,
                    latest_seq,
                };
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, PERMISSION_SET_MODE, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("permission.set_mode failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Agent / leader orchestration substrate
    // -----------------------------------------------------------------------

    fn handle_agent_spawn(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, agent_id) = match agent_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or agent_id",
                    ),
                );
            }
        };
        let task_id = cmd
            .params
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let agent_name = cmd
            .params
            .get("agent_name")
            .and_then(|v| v.as_str())
            .unwrap_or(&agent_id)
            .to_string();
        let agent_role = cmd
            .params
            .get("agent_role")
            .and_then(|v| v.as_str())
            .unwrap_or("worker")
            .to_string();
        let run_supervised = cmd
            .params
            .get("run")
            .or_else(|| cmd.params.get("supervised"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "mock/model".into());
        let task_content = string_param(&cmd, &["task_content", "prompt", "objective"])
            .or_else(|| load_task_content_outside(&self.db, &session_id, &task_id))
            .unwrap_or_default();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();
        let worker_token = agent_worker_token(&session_id, &agent_id);

        if let Some(ref key) = idempotency_key {
            if let Some(cached) = lookup_idempotent_outside_tx(&self.db, key, method) {
                return cached;
            }
        }
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        if let Err(error) = self
            .runtime_manager
            .lock()
            .unwrap()
            .try_acquire_worker(worker_token.clone())
        {
            return CommandResponse::err(
                request_id,
                CoreError::with_details(
                    ErrorCode::InvalidTransition,
                    "Max parallel agents exceeded",
                    json!({
                        "resource": "worker",
                        "requested": error.requested,
                        "available": error.available,
                    }),
                    false,
                ),
            );
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "INSERT INTO agent_state \
                     (session_id, agent_id, agent_name, agent_role, task_id, status, stopped, iteration, timestamp) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 'starting', 0, 0, ?6) \
                     ON CONFLICT(session_id, agent_id) DO UPDATE SET \
                     agent_name = excluded.agent_name, agent_role = excluded.agent_role, \
                     task_id = excluded.task_id, status = 'starting', stopped = 0, \
                     iteration = agent_state.iteration + 1, timestamp = excluded.timestamp",
                    params![session_id, agent_id, agent_name, agent_role, task_id, ts],
                )?;
                insert_agent_log(tx, &session_id, &agent_id, &agent_name, &agent_role, &task_id, event_type, "", occurred_at)?;

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "agent_name": agent_name,
                        "agent_role": agent_role,
                        "task_id": task_id,
                        "status": "starting",
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| format!("{}_{}_{}", event_type.replace('.', "_"), session_id, agent_id)),
                )?;
                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "task_id": task_id,
                        "status": "starting",
                        "latest_seq": event_seq,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        let response = outcome.unwrap_or_else(|e| {
            self.runtime_manager
                .lock()
                .unwrap()
                .release_worker(&worker_token);
            CommandResponse::err(
                request_id.clone(),
                CoreError::internal(format!("{method} failed: {e}")),
            )
        });
        if !response.success {
            self.runtime_manager
                .lock()
                .unwrap()
                .release_worker(&worker_token);
        } else if run_supervised {
            let llm_router = match self.llm_router_arc(&model) {
                Ok(router) => router,
                Err(error) => {
                    self.runtime_manager
                        .lock()
                        .unwrap()
                        .release_worker(&worker_token);
                    persist_agent_pool_terminal_event(
                        &self.db,
                        &self.runtime_manager,
                        AgentPoolTerminalUpdate {
                            explicit_session_id: Some(&session_id),
                            agent_id: &agent_id,
                            event_type: "agent.crashed",
                            target_status: "stopped",
                            exit_reason: &error.message,
                            result: None,
                        },
                    );
                    return CommandResponse::err(request_id, error);
                }
            };
            let tool_executor = Arc::new(RouterAgentToolExecutor::new(
                self.db.clone(),
                Arc::clone(&self.tool_registry),
                Arc::clone(&self.runtime_manager),
            ));
            let llm_executor = Arc::new(RouterAgentLlmExecutor::new(
                self.db.clone(),
                Arc::clone(&llm_router),
                Arc::clone(&self.runtime_manager),
            ));
            let config = AgentConfig {
                agent_id: agent_id.clone(),
                session_id: session_id.clone(),
                task_id: task_id.clone(),
                task_content,
                model: model.clone(),
                auth_context: auth_context_from_cmd(&cmd).unwrap_or(AuthContext::None),
                max_rounds: cmd
                    .params
                    .get("max_rounds")
                    .and_then(Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                    .unwrap_or(8),
                round_timeout_ms: cmd
                    .params
                    .get("round_timeout_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(60_000),
                heartbeat_interval_ms: cmd
                    .params
                    .get("heartbeat_interval_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or(5_000),
                context_retain_last: cmd
                    .params
                    .get("context_retain_last")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or(20),
                agent_name: agent_name.clone(),
                context_store: Some(Arc::new(SqliteAgentContextStore::new(self.db.clone()))),
                request_options: request_options_from_cmd(&cmd),
            };
            if let Err(error) = self.agent_pool.spawn(
                config,
                Arc::clone(&self.tool_registry),
                llm_executor,
                tool_executor,
            ) {
                self.runtime_manager
                    .lock()
                    .unwrap()
                    .release_worker(&worker_token);
                persist_agent_pool_terminal_event(
                    &self.db,
                    &self.runtime_manager,
                    AgentPoolTerminalUpdate {
                        explicit_session_id: Some(&session_id),
                        agent_id: &agent_id,
                        event_type: "agent.crashed",
                        target_status: "stopped",
                        exit_reason: &error,
                        result: None,
                    },
                );
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("agent pool spawn failed: {error}")),
                );
            }
        }
        response
    }

    fn handle_agent_transition(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        target_status: &'static str,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, agent_id) = match agent_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or agent_id",
                    ),
                );
            }
        };
        let exit_reason = cmd
            .params
            .get("exit_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                let (agent_name, agent_role, task_id, current_status): (
                    String,
                    String,
                    String,
                    String,
                ) = tx.query_row(
                    "SELECT agent_name, agent_role, task_id, status FROM agent_state \
                         WHERE session_id = ?1 AND agent_id = ?2",
                    params![session_id, agent_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
                let stopped = i64::from(target_status == "stopped");
                let ts = occurred_at as f64 / 1000.0;
                tx.execute(
                    "UPDATE agent_state SET status = ?1, stopped = ?2, timestamp = ?3 \
                     WHERE session_id = ?4 AND agent_id = ?5",
                    params![target_status, stopped, ts, session_id, agent_id],
                )?;
                insert_agent_log(
                    tx,
                    &session_id,
                    &agent_id,
                    &agent_name,
                    &agent_role,
                    &task_id,
                    event_type,
                    exit_reason,
                    occurred_at,
                )?;

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "task_id": task_id,
                        "from_status": current_status,
                        "status": target_status,
                        "exit_reason": exit_reason,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!(
                                "{}_{}_{}",
                                event_type.replace('.', "_"),
                                session_id,
                                agent_id
                            )
                        }),
                )?;
                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "agent_id": agent_id,
                        "task_id": task_id,
                        "status": target_status,
                        "latest_seq": event_seq,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        let response = outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        });
        if response.success && target_status == "stopped" {
            self.runtime_manager
                .lock()
                .unwrap()
                .release_worker(&agent_worker_token(&session_id, &agent_id));
        }
        response
    }

    fn handle_leader_plan(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let leader = LeaderOrchestrator::new();
        let tasks = match cmd.params.get("tasks").and_then(|v| v.as_array()) {
            Some(values) => match leader.normalize_task_values(values) {
                Ok(plan) => plan
                    .tasks
                    .into_iter()
                    .map(|task| task.to_value())
                    .collect::<Vec<_>>(),
                Err(err) => {
                    return CommandResponse::err(
                        request_id,
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Invalid leader tasks: {err:?}"),
                        ),
                    );
                }
            },
            None => {
                let objective = cmd
                    .params
                    .get("objective")
                    .or_else(|| cmd.params.get("prompt"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                match leader.plan_from_objective(objective) {
                    Ok(plan) => plan
                        .tasks
                        .into_iter()
                        .map(|task| task.to_value())
                        .collect::<Vec<_>>(),
                    Err(_) => Vec::new(),
                }
            }
        };
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, LEADER_PLAN)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let mut events = Vec::new();
                for (idx, task) in tasks.iter().enumerate() {
                    let task_id = task
                        .get("task_id")
                        .or_else(|| task.get("id"))
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("leader_task_{}_{}", occurred_at, idx));
                    let subject = task
                        .get("subject")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Leader task")
                        .to_string();
                    let description = task
                        .get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let ts = occurred_at as f64 / 1000.0;
                    tx.execute(
                        "INSERT INTO tasks \
                         (id, session_id, subject, description, status, run_generation, agent_type, \
                          assigned_agent, created_at, updated_at) \
                         VALUES (?1, ?2, ?3, ?4, 'dispatchable', 0, 'general', '', ?5, ?5)",
                        params![task_id, session_id, subject, description, ts],
                    )?;
                    let event = append_event_in_tx(
                        tx,
                        Some(session_id.clone()),
                        generation,
                        "task.created",
                        actor.clone(),
                        json!({
                            "session_id": session_id,
                            "task_id": task_id,
                            "subject": subject,
                            "description": description,
                            "source": "leader.plan",
                            "status": "dispatchable",
                        }),
                        occurred_at,
                        Some(causation_id.clone()),
                        Some(correlation_id.clone()),
                        format!("leader_plan_task_created_{}_{}", session_id, task_id),
                    )?;
                    events.push(event);
                }
                let latest_seq = events.last().map(|event| event.seq);
                let response = CommandResponse {
                    request_id: request_id.clone(),
                    success: true,
                    result: Some(json!({
                        "session_id": session_id,
                        "created_tasks": events.len(),
                    })),
                    error: None,
                    events,
                    latest_seq,
                };
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, LEADER_PLAN, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("leader.plan failed: {e}")),
            )
        })
    }

    fn handle_leader_run(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }

        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "mock/model".into());
        let objective = string_param(&cmd, &["objective", "prompt"]).unwrap_or_default();
        let max_rounds = cmd
            .params
            .get("max_rounds")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .unwrap_or(8);
        let mut messages = vec![Message {
            role: "user".into(),
            content: objective.clone(),
            ..Default::default()
        }];
        append_leader_conversation_outside(&self.db, &session_id, "user", &objective, None);

        let mut all_events = Vec::new();
        let mut observations = Vec::new();
        let mut final_answer = None;

        for round in 0..max_rounds {
            let request = GenerateRequest {
                model: model.clone(),
                messages: messages.clone(),
                tools: self.tool_registry.llm_tool_definitions(),
                stream: true,
                auth_context: auth_context_from_cmd(&cmd).unwrap_or(AuthContext::None),
                options: request_options_from_cmd(&cmd),
            };
            let stream = match self.route_llm_stream(request) {
                Ok(stream) => stream,
                Err(error) => {
                    return CommandResponse::err(
                        request_id,
                        CoreError::with_details(
                            ErrorCode::Internal,
                            "Leader LLM routing failed",
                            provider_error_json(&error),
                            error.retryable,
                        ),
                    );
                }
            };

            let mut assistant_text = String::new();
            let mut tool_calls = Vec::new();
            let mut tool_call_accumulator = ToolCallAccumulator::new();
            for event in stream {
                match event {
                    Ok(StreamEvent::TextDelta(text)) => assistant_text.push_str(&text),
                    Ok(StreamEvent::ToolCall(tool_call)) => tool_calls.push(tool_call),
                    Ok(StreamEvent::ToolCallDelta(delta)) => tool_call_accumulator.append(delta),
                    Ok(StreamEvent::Finished(reason)) => {
                        if matches!(reason, crate::llm::FinishReason::ToolCalls) {
                            tool_calls.extend(tool_call_accumulator.finalize());
                        }
                    }
                    Ok(StreamEvent::ThinkingDelta(_)) | Ok(StreamEvent::Usage(_)) => {}
                    Ok(StreamEvent::Error(error)) | Err(error) => {
                        return CommandResponse::err(
                            request_id,
                            CoreError::with_details(
                                ErrorCode::Internal,
                                "Leader stream failed",
                                provider_error_json(&error),
                                error.retryable,
                            ),
                        );
                    }
                }
            }
            tool_calls.extend(tool_call_accumulator.finalize());
            dedupe_tool_calls_by_id(&mut tool_calls);
            append_leader_conversation_outside(
                &self.db,
                &session_id,
                "assistant",
                &assistant_text,
                None,
            );
            messages.push(Message {
                role: "assistant".into(),
                content: assistant_text.clone(),
                tool_calls: tool_calls.clone(),
                ..Default::default()
            });

            if tool_calls.is_empty() {
                final_answer = Some(assistant_text);
                break;
            }

            let mut blocked_permission = None;
            for tool_call in tool_calls {
                if tool_call.name == "attempt_completion" {
                    final_answer = Some(
                        tool_call
                            .arguments
                            .get("result")
                            .and_then(Value::as_str)
                            .unwrap_or(&assistant_text)
                            .to_string(),
                    );
                    break;
                }

                if let Some(ToolPermission::RequiresGrant { tool_name, .. }) = self
                    .tool_registry
                    .required_permission_for_call(&tool_call.name, &tool_call.arguments)
                {
                    if !has_permission_grant(&self.db, &session_id, &tool_name) {
                        let permission_request_id =
                            format!("leader_perm_{}_{}", session_id, tool_call.id);
                        let permission_resp = self.dispatch(CommandEnvelope {
                            request_id: format!("{request_id}_permission_{}", tool_call.id),
                            method: PERMISSION_REQUEST.into(),
                            params: json!({
                                "permission_request_id": permission_request_id,
                                "tool_name": tool_name,
                                "mode": "leader_tool",
                                "args": tool_call.arguments,
                            }),
                            actor: cmd.actor.clone(),
                            session_id: Some(session_id.clone()),
                            idempotency_key: None,
                            submitted_at: now_ms(),
                        });
                        all_events.extend(permission_resp.events);
                        blocked_permission = Some(json!({
                            "permission_request_id": permission_request_id,
                            "tool_name": tool_name,
                            "tool_call_id": tool_call.id,
                        }));
                        break;
                    }
                }

                let tool_resp = self.dispatch(CommandEnvelope {
                    request_id: format!("{request_id}_tool_{}_{}", round, tool_call.id),
                    method: TOOL_CALL.into(),
                    params: json!({
                        "tool_call_id": tool_call.id,
                        "tool_name": tool_call.name,
                        "tool_type": "native",
                        "args": tool_call.arguments,
                    }),
                    actor: cmd.actor.clone(),
                    session_id: Some(session_id.clone()),
                    idempotency_key: None,
                    submitted_at: now_ms(),
                });
                let observation = leader_tool_observation(&tool_call, &tool_resp);
                all_events.extend(tool_resp.events);
                observations.push(observation.clone());
                append_leader_conversation_outside(
                    &self.db,
                    &session_id,
                    "tool",
                    &observation.to_string(),
                    Some(&tool_call.id),
                );
                messages.push(Message {
                    role: "tool".into(),
                    content: observation.to_string(),
                    tool_call_id: Some(tool_call.id.clone()),
                    name: Some(tool_call.name.clone()),
                    ..Default::default()
                });
            }

            if let Some(permission) = blocked_permission {
                return CommandResponse {
                    request_id,
                    success: true,
                    result: Some(json!({
                        "session_id": session_id,
                        "status": "blocked",
                        "blocked_by": "permission",
                        "permission": permission,
                        "observations": observations,
                        "rounds": round + 1,
                    })),
                    error: None,
                    latest_seq: all_events.last().map(|event| event.seq),
                    events: all_events,
                };
            }

            if final_answer.is_some() {
                break;
            }
        }

        let status = if final_answer.is_some() {
            "completed"
        } else {
            "max_rounds_exceeded"
        };
        CommandResponse {
            request_id,
            success: true,
            result: Some(json!({
                "session_id": session_id,
                "status": status,
                "answer": final_answer.unwrap_or_default(),
                "observations": observations,
            })),
            error: None,
            latest_seq: all_events.last().map(|event| event.seq),
            events: all_events,
        }
    }

    // -----------------------------------------------------------------------
    // Workflow execution substrate
    // -----------------------------------------------------------------------

    fn handle_workflow_execute(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let workflow_id = cmd
            .params
            .get("workflow_id")
            .and_then(|v| v.as_str())
            .unwrap_or("workflow-default")
            .to_string();
        let execution_id = cmd
            .params
            .get("execution_id")
            .or_else(|| cmd.params.get("id"))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| format!("wfexec_{}", now_ms()));
        let nodes = cmd
            .params
            .get("nodes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_else(|| vec![json!({"id": "A"})]);
        let edges = cmd
            .params
            .get("edges")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let node_plan = match plan_dag_execution(&nodes, &edges) {
            Ok(plan) => plan,
            Err(err) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        format!("Invalid workflow DAG: {err:?}"),
                    ),
                );
            }
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();
        let node_by_id = nodes
            .iter()
            .filter_map(|node| workflow_node_id(node).map(|id| (id, node.clone())))
            .collect::<HashMap<_, _>>();

        let bootstrap: std::result::Result<(), rusqlite::Error> = self.db.with_transaction(|tx| {
            ensure_session_active(tx, &session_id, &request_id)?;
            let ts = occurred_at as f64 / 1000.0;
            tx.execute(
                "INSERT OR IGNORE INTO workflows \
                 (id, name, description, workspace, nodes, edges, created_at, updated_at) \
                 VALUES (?1, ?1, '', '', ?2, ?3, ?4, ?4)",
                params![
                    workflow_id,
                    serde_json::Value::Array(nodes.clone()).to_string(),
                    serde_json::Value::Array(edges.clone()).to_string(),
                    ts
                ],
            )?;
            tx.execute(
                "INSERT INTO workflow_executions \
                 (id, workflow_id, session_id, status, start_time, context, created_at) \
                 VALUES (?1, ?2, ?3, 'running', ?4, ?5, ?4) \
                 ON CONFLICT(id) DO UPDATE SET status = 'running', context = excluded.context",
                params![
                    execution_id,
                    workflow_id,
                    session_id,
                    occurred_at,
                    workflow_execution_context(&cmd.params).to_string()
                ],
            )?;
            Ok(())
        });
        if let Err(e) = bootstrap {
            return CommandResponse::err(
                request_id,
                CoreError::internal(format!("workflow.execute bootstrap failed: {e}")),
            );
        }

        let mut node_results = Vec::new();
        for node in &node_plan {
            match load_workflow_node_state(&self.db, &execution_id, &node.id) {
                Ok(Some(existing)) if existing.success => {
                    node_results.push(existing);
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    return CommandResponse::err(
                        request_id,
                        CoreError::internal(format!("workflow node state lookup failed: {e}")),
                    );
                }
            }

            let node_def = node_by_id
                .get(&node.id)
                .cloned()
                .unwrap_or_else(|| json!({"id": node.id}));
            let max_attempts = workflow_node_max_attempts(&node_def);
            let mut attempt =
                next_workflow_node_attempt(&self.db, &execution_id, &node.id).unwrap_or(0) + 1;
            let mut final_result = None;
            while attempt <= max_attempts {
                let mut result = match self.execute_workflow_node(&cmd, &session_id, &node_def) {
                    Ok(result) => result,
                    Err(error) => return CommandResponse::err(request_id, error),
                };
                result.attempt = attempt;
                if workflow_node_forces_retryable(&node_def) && !result.success {
                    result.retryable = true;
                }
                if let Err(e) =
                    persist_workflow_node_state(&self.db, &execution_id, &result, occurred_at)
                {
                    return CommandResponse::err(
                        request_id,
                        CoreError::internal(format!("workflow node state persist failed: {e}")),
                    );
                }
                let should_retry = !result.success && result.retryable && attempt < max_attempts;
                final_result = Some(result);
                if !should_retry {
                    break;
                }
                workflow_node_retry_sleep(&node_def, attempt);
                attempt += 1;
            }
            if let Some(result) = final_result {
                node_results.push(result);
            }
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, WORKFLOW_EXECUTE)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let mut events = Vec::new();
                events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "workflow.execution_started",
                    actor.clone(),
                    json!({
                        "session_id": session_id,
                        "workflow_id": workflow_id,
                        "execution_id": execution_id,
                        "status": "running",
                    }),
                    occurred_at,
                    Some(causation_id.clone()),
                    Some(correlation_id.clone()),
                    format!("workflow_execution_started_{session_id}_{execution_id}"),
                )?);

                let mut workflow_failed = false;
                for node_result in &node_results {
                    let node_id = node_result.node_id.as_str();
                    events.push(append_event_in_tx(
                        tx,
                        Some(session_id.clone()),
                        generation,
                        "workflow.node_started",
                        actor.clone(),
                        json!({
                            "session_id": session_id,
                            "workflow_id": workflow_id,
                            "execution_id": execution_id,
                            "node_id": node_id,
                            "node_type": node_result.node_type,
                        }),
                        occurred_at,
                        Some(causation_id.clone()),
                        Some(correlation_id.clone()),
                        format!("workflow_node_started_{execution_id}_{node_id}"),
                    )?);
                    insert_workflow_log(
                        tx,
                        &execution_id,
                        "info",
                        Some(node_id),
                        "node started",
                        occurred_at,
                    )?;
                    let completion_event_type = if node_result.success {
                        "workflow.node_completed"
                    } else {
                        workflow_failed = true;
                        "workflow.node_failed"
                    };
                    events.push(append_event_in_tx(
                        tx,
                        Some(session_id.clone()),
                        generation,
                        completion_event_type,
                        actor.clone(),
                        json!({
                            "session_id": session_id,
                            "workflow_id": workflow_id,
                            "execution_id": execution_id,
                            "node_id": node_id,
                            "node_type": node_result.node_type,
                            "status": node_result.status,
                            "output": node_result.output,
                        }),
                        occurred_at,
                        Some(causation_id.clone()),
                        Some(correlation_id.clone()),
                        format!("{completion_event_type}_{execution_id}_{node_id}"),
                    )?);
                    insert_workflow_log(
                        tx,
                        &execution_id,
                        if node_result.success { "info" } else { "error" },
                        Some(node_id),
                        &format!(
                            "{}: {}",
                            node_result.status,
                            serde_json::to_string(&node_result.output)
                                .unwrap_or_else(|_| "null".into())
                        ),
                        occurred_at,
                    )?;
                }

                let final_status = if workflow_failed {
                    "failed"
                } else {
                    "completed"
                };
                let final_event_type = if workflow_failed {
                    "workflow.execution_failed"
                } else {
                    "workflow.execution_completed"
                };
                tx.execute(
                    "UPDATE workflow_executions SET status = ?1, end_time = ?2 \
                     WHERE id = ?3",
                    params![final_status, occurred_at, execution_id],
                )?;
                events.push(append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    final_event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "workflow_id": workflow_id,
                        "execution_id": execution_id,
                        "status": final_status,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    format!("{final_event_type}_{session_id}_{execution_id}"),
                )?);

                let latest_seq = events.last().map(|event| event.seq);
                let response = CommandResponse {
                    request_id: request_id.clone(),
                    success: true,
                    result: Some(json!({
                        "session_id": session_id,
                        "workflow_id": workflow_id,
                        "execution_id": execution_id,
                        "status": final_status,
                        "nodes": nodes.len(),
                        "node_results": node_results.iter().map(WorkflowNodeExecution::as_json).collect::<Vec<_>>(),
                    })),
                    error: None,
                    events,
                    latest_seq,
                };
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, WORKFLOW_EXECUTE, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("workflow.execute failed: {e}")),
            )
        })
    }

    fn handle_workflow_transition(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        target_status: &'static str,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let (session_id, execution_id) = match workflow_ref_from_cmd(&cmd) {
            Some(v) => v,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(
                        ErrorCode::InvalidTransition,
                        "Missing session_id or execution_id",
                    ),
                );
            }
        };
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let causation_id = cmd.request_id.clone();
        let correlation_id = cmd.request_id.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                let current: Option<String> = tx
                    .query_row(
                        "SELECT status FROM workflow_executions WHERE id = ?1 AND session_id = ?2",
                        params![execution_id, session_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let current = match current {
                    Some(status) => status,
                    None => {
                        return Ok(CommandResponse::err(
                            request_id.clone(),
                            CoreError::new(
                                ErrorCode::InvalidTransition,
                                format!("Workflow execution not found: {execution_id}"),
                            ),
                        ));
                    }
                };
                if matches!(current.as_str(), "completed" | "failed" | "cancelled") {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::invalid_transition(current, target_status),
                    ));
                }
                let end_time: Option<Timestamp> = if matches!(target_status, "cancelled") {
                    Some(occurred_at)
                } else {
                    None
                };
                tx.execute(
                    "UPDATE workflow_executions SET status = ?1, end_time = COALESCE(?2, end_time) \
                     WHERE id = ?3 AND session_id = ?4",
                    params![target_status, end_time, execution_id, session_id],
                )?;
                insert_workflow_log(tx, &execution_id, "info", None, event_type, occurred_at)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    event_type,
                    actor,
                    json!({
                        "session_id": session_id,
                        "execution_id": execution_id,
                        "status": target_status,
                    }),
                    occurred_at,
                    Some(causation_id),
                    Some(correlation_id),
                    idempotency_key
                        .as_ref()
                        .map(|key| format!("cmd_{key}"))
                        .unwrap_or_else(|| {
                            format!(
                                "{}_{}_{}",
                                event_type.replace('.', "_"),
                                session_id,
                                execution_id
                            )
                        }),
                )?;
                let event_seq = event.seq;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "execution_id": execution_id,
                        "status": target_status,
                        "latest_seq": event_seq,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        })
    }

    fn handle_workflow_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let recover_running = cmd
            .params
            .get("recover_running")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if recover_running {
                    tx.execute(
                        "UPDATE workflow_executions SET status = 'paused' \
                         WHERE session_id = ?1 AND status = 'running'",
                        params![session_id],
                    )?;
                }
                let executions = {
                    let mut stmt = tx.prepare(
                        "SELECT id, workflow_id, status FROM workflow_executions \
                         WHERE session_id = ?1 ORDER BY created_at, id",
                    )?;
                    let rows = stmt.query_map(params![session_id], |row| {
                        let execution_id: String = row.get(0)?;
                        Ok(json!({
                            "execution_id": execution_id,
                            "workflow_id": row.get::<_, String>(1)?,
                            "status": row.get::<_, String>(2)?,
                        }))
                    })?;
                    let mut executions = rows.collect::<Result<Vec<Value>>>()?;
                    for execution in &mut executions {
                        let execution_id = execution["execution_id"].as_str().unwrap_or_default();
                        let node_states = load_workflow_node_states_in_tx(tx, execution_id)?;
                        if let Some(obj) = execution.as_object_mut() {
                            obj.insert("node_states".into(), Value::Array(node_states));
                        }
                    }
                    executions
                };
                Ok(CommandResponse::ok(
                    request_id.clone(),
                    Some(json!({
                        "session_id": session_id,
                        "executions": executions,
                    })),
                    None,
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("workflow.list failed: {e}")),
            )
        })
    }

    fn execute_workflow_node(
        &self,
        cmd: &CommandEnvelope,
        session_id: &str,
        node: &Value,
    ) -> std::result::Result<WorkflowNodeExecution, CoreError> {
        let node_id = workflow_node_id(node).unwrap_or_else(|| "node".into());
        let node_type = node
            .get("type")
            .or_else(|| node.get("kind"))
            .and_then(|value| value.as_str())
            .unwrap_or("data")
            .to_string();
        match node_type.as_str() {
            "tool" => self.execute_workflow_tool_node(session_id, node, &node_id, &node_type),
            "llm" => self.execute_workflow_llm_node(cmd, node, &node_id, &node_type),
            "agent" => {
                self.execute_workflow_agent_node(cmd, session_id, node, &node_id, &node_type)
            }
            "data" | "noop" => Ok(WorkflowNodeExecution {
                node_id,
                node_type,
                status: "completed".into(),
                success: true,
                output: node.get("output").cloned().unwrap_or_else(|| json!({})),
                error: None,
                attempt: 0,
                retryable: false,
            }),
            other => Ok(WorkflowNodeExecution {
                node_id,
                node_type: other.to_string(),
                status: "failed".into(),
                success: false,
                output: json!({"error": format!("Unsupported workflow node type: {other}")}),
                error: Some(format!("Unsupported workflow node type: {other}")),
                attempt: 0,
                retryable: false,
            }),
        }
    }

    fn execute_workflow_tool_node(
        &self,
        session_id: &str,
        node: &Value,
        node_id: &str,
        node_type: &str,
    ) -> std::result::Result<WorkflowNodeExecution, CoreError> {
        let tool_name = node
            .get("tool_name")
            .or_else(|| node.get("name"))
            .and_then(|value| value.as_str())
            .ok_or_else(|| CoreError::new(ErrorCode::InvalidTransition, "Missing tool_name"))?;
        let args = node.get("args").cloned().unwrap_or_else(|| json!({}));
        preflight_native_tool_call(
            &self.db,
            self.tool_registry.as_ref(),
            session_id,
            tool_name,
            &args,
        )?;
        let executor = RouterAgentToolExecutor::new(
            self.db.clone(),
            Arc::clone(&self.tool_registry),
            Arc::clone(&self.runtime_manager),
        );
        let result = executor.execute_tool(
            session_id,
            "workflow",
            &ToolCall {
                id: format!("workflow_{node_id}"),
                name: tool_name.to_string(),
                arguments: args,
            },
        );
        let error = if result.success {
            None
        } else {
            result.error.clone()
        };
        Ok(WorkflowNodeExecution {
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            status: if result.success {
                "completed"
            } else {
                "failed"
            }
            .into(),
            success: result.success,
            output: if result.success {
                result.output
            } else {
                json!({"error": result.error.unwrap_or_else(|| "Tool execution failed".into())})
            },
            error,
            attempt: 0,
            retryable: false,
        })
    }

    fn execute_workflow_llm_node(
        &self,
        cmd: &CommandEnvelope,
        node: &Value,
        node_id: &str,
        node_type: &str,
    ) -> std::result::Result<WorkflowNodeExecution, CoreError> {
        let model = node
            .get("model")
            .or_else(|| cmd.params.get("model"))
            .and_then(|value| value.as_str())
            .unwrap_or("mock/model")
            .to_string();
        let messages = node
            .get("messages")
            .and_then(|value| serde_json::from_value::<Vec<Message>>(value.clone()).ok())
            .unwrap_or_else(|| {
                vec![Message {
                    role: "user".into(),
                    content: node
                        .get("prompt")
                        .and_then(|value| value.as_str())
                        .unwrap_or("")
                        .to_string(),
                    ..Default::default()
                }]
            });
        let request = GenerateRequest {
            model,
            messages,
            tools: self.tool_registry.llm_tool_definitions(),
            stream: true,
            auth_context: node
                .get("auth_context")
                .and_then(|value| serde_json::from_value(value.clone()).ok())
                .or_else(|| auth_context_from_cmd(cmd))
                .unwrap_or(AuthContext::None),
            options: request_options_from_cmd(cmd),
        };
        let stream = match self.route_llm_stream(request) {
            Ok(stream) => stream,
            Err(error) => {
                let retryable = error.retryable;
                let message = error.message;
                return Ok(WorkflowNodeExecution {
                    node_id: node_id.to_string(),
                    node_type: node_type.to_string(),
                    status: "failed".into(),
                    success: false,
                    output: json!({"error": message}),
                    error: Some(message),
                    attempt: 0,
                    retryable,
                });
            }
        };
        let mut text = String::new();
        let mut usage = Value::Null;
        for event in stream {
            match event {
                Ok(StreamEvent::TextDelta(delta)) => text.push_str(&delta),
                Ok(StreamEvent::Usage(value)) => usage = json!(value),
                Ok(StreamEvent::Finished(_))
                | Ok(StreamEvent::ThinkingDelta(_))
                | Ok(StreamEvent::ToolCall(_))
                | Ok(StreamEvent::ToolCallDelta(_)) => {}
                Ok(StreamEvent::Error(error)) => {
                    let retryable = error.retryable;
                    let message = error.message;
                    return Ok(WorkflowNodeExecution {
                        node_id: node_id.to_string(),
                        node_type: node_type.to_string(),
                        status: "failed".into(),
                        success: false,
                        output: json!({"error": message}),
                        error: Some(message),
                        attempt: 0,
                        retryable,
                    });
                }
                Err(error) => {
                    let message = error.message;
                    let retryable = error.retryable;
                    return Ok(WorkflowNodeExecution {
                        node_id: node_id.to_string(),
                        node_type: node_type.to_string(),
                        status: "failed".into(),
                        success: false,
                        output: json!({"error": message}),
                        error: Some(message),
                        attempt: 0,
                        retryable,
                    });
                }
            }
        }
        Ok(WorkflowNodeExecution {
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            status: "completed".into(),
            success: true,
            output: json!({"text": text, "usage": usage}),
            error: None,
            attempt: 0,
            retryable: false,
        })
    }

    fn execute_workflow_agent_node(
        &self,
        cmd: &CommandEnvelope,
        session_id: &str,
        node: &Value,
        node_id: &str,
        node_type: &str,
    ) -> std::result::Result<WorkflowNodeExecution, CoreError> {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (_cmd_tx, cmd_rx) = std::sync::mpsc::channel();
        let model = node
            .get("model")
            .or_else(|| cmd.params.get("model"))
            .and_then(|value| value.as_str())
            .unwrap_or("mock/model")
            .to_string();
        let llm_router = self.llm_router_arc(&model)?;
        let config = crate::agent::AgentConfig {
            agent_id: node
                .get("agent_id")
                .and_then(|value| value.as_str())
                .unwrap_or(node_id)
                .to_string(),
            session_id: session_id.to_string(),
            task_id: node
                .get("task_id")
                .and_then(|value| value.as_str())
                .unwrap_or(node_id)
                .to_string(),
            task_content: node
                .get("task")
                .or_else(|| node.get("prompt"))
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string(),
            model,
            auth_context: node
                .get("auth_context")
                .and_then(|value| serde_json::from_value(value.clone()).ok())
                .or_else(|| auth_context_from_cmd(cmd))
                .unwrap_or(AuthContext::None),
            max_rounds: node
                .get("max_rounds")
                .and_then(|value| value.as_u64())
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(3),
            agent_name: node
                .get("agent_name")
                .and_then(|value| value.as_str())
                .unwrap_or(node_id)
                .to_string(),
            context_store: Some(Arc::new(SqliteAgentContextStore::new(self.db.clone()))),
            request_options: node
                .get("options")
                .and_then(|value| serde_json::from_value(value.clone()).ok())
                .unwrap_or_else(|| request_options_from_cmd(cmd)),
            ..Default::default()
        };
        let task_id_for_update = config.task_id.clone();
        crate::agent::AgentLoop::new(
            config,
            Arc::clone(&self.tool_registry),
            Arc::new(RouterAgentLlmExecutor::new(
                self.db.clone(),
                llm_router,
                Arc::clone(&self.runtime_manager),
            )),
            Arc::new(RouterAgentToolExecutor::new(
                self.db.clone(),
                Arc::clone(&self.tool_registry),
                Arc::clone(&self.runtime_manager),
            )),
            event_tx,
            cmd_rx,
        )
        .run();
        let mut terminal = json!({"error": "agent did not produce terminal event"});
        let mut success = false;
        let mut error_message = Some("agent did not produce terminal event".to_string());
        for event in event_rx.try_iter() {
            match event {
                crate::agent::AgentEvent::Completed { result, .. } => {
                    terminal = result;
                    success = true;
                    error_message = None;
                }
                crate::agent::AgentEvent::Crashed { error, .. } => {
                    terminal = json!({"error": error});
                    error_message = Some(error);
                    success = false;
                }
                _ => {}
            }
        }
        persist_agent_task_result(
            &self.db,
            session_id,
            &task_id_for_update,
            success,
            &terminal,
            error_message.as_deref(),
        )
        .map_err(|error| {
            CoreError::internal(format!("agent task result persist failed: {error}"))
        })?;
        Ok(WorkflowNodeExecution {
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
            status: if success { "completed" } else { "failed" }.into(),
            success,
            output: terminal,
            error: error_message,
            attempt: 0,
            retryable: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_registered_sidecar(
        &self,
        command: SidecarCommand,
        cmd: &CommandEnvelope,
        session_id: &str,
        tool_call_id: &str,
        tool_name: &str,
        args: &Value,
        requested_units: i64,
    ) -> std::result::Result<Value, SidecarErrorCode> {
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(30_000);
        let request = SidecarRequest {
            request_id: cmd.request_id.clone(),
            session_id: session_id.to_string(),
            task_id: string_param(cmd, &["task_id"]),
            agent_id: string_param(cmd, &["agent_id"]).unwrap_or_else(|| "core".into()),
            tool_call_id: tool_call_id.to_string(),
            tool_name: tool_name.to_string(),
            args: serde_json::to_vec(args).map_err(|_| SidecarErrorCode::ProtocolError)?,
            capabilities: vec![tool_name.to_string()],
            deadline: now_ms() + timeout_ms as i64,
            resource_budget: ResourceBudget {
                max_runtime_ms: timeout_ms,
                max_memory_mb: cmd
                    .params
                    .get("max_memory_mb")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(512),
                max_cpu_ms: cmd
                    .params
                    .get("max_cpu_ms")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(timeout_ms),
                max_network_bytes: cmd
                    .params
                    .get("max_network_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0),
                max_file_write_bytes: cmd
                    .params
                    .get("max_file_write_bytes")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(requested_units.max(0) as u64),
            },
            cancel_token: CancelToken {
                token_id: string_param(cmd, &["cancel_token"])
                    .unwrap_or_else(|| format!("cancel_{tool_call_id}")),
            },
            permission_lease: permission_lease_from_cmd(cmd),
        };

        let execution = self
            .sidecar_scheduler
            .execute(SidecarInvocation {
                command,
                request,
                timeout_ms,
            })
            .map_err(|err| err.code)?;

        match execution.response {
            SidecarResponse::Completed(done) if done.result_shape == "json" => {
                serde_json::from_slice(&done.result).map_err(|_| SidecarErrorCode::ProtocolError)
            }
            SidecarResponse::Completed(done) => Ok(json!({
                "result_shape": done.result_shape,
                "bytes": done.result.len(),
            })),
            SidecarResponse::Error(err) => Err(err.code),
            SidecarResponse::Stream(_) | SidecarResponse::Progress(_) => {
                Err(SidecarErrorCode::ProtocolError)
            }
        }
    }

    fn handle_tool_call(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let tool_call_id = string_param(&cmd, &["tool_call_id", "id"])
            .unwrap_or_else(|| format!("tool_call_{}", now_ms()));
        let tool_name =
            string_param(&cmd, &["tool_name", "name"]).unwrap_or_else(|| "mock_tool".into());
        let tool_type = string_param(&cmd, &["tool_type"]).unwrap_or_else(|| "native".into());
        let mut outcome_kind =
            string_param(&cmd, &["scenario", "outcome"]).unwrap_or_else(|| "success".into());
        let args = cmd.params.get("args").cloned().unwrap_or_else(|| json!({}));
        let mut result = Value::Null;
        let requested_units = cmd
            .params
            .get("resource_units")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);
        let budget_limit = cmd
            .params
            .get("budget_limit")
            .and_then(|v| v.as_i64())
            .unwrap_or(8);
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let sidecar = matches!(tool_type.as_str(), "sidecar" | "browser");
        if !sidecar && !self.tool_registry.is_registered(&tool_name) {
            return CommandResponse::err(request_id, CoreError::tool_not_found(tool_name));
        }
        if sidecar && !self.sidecar_commands.contains_key(&tool_name) {
            return CommandResponse::err(request_id, CoreError::tool_not_found(tool_name));
        }
        let sidecar_memory_mb = cmd
            .params
            .get("max_memory_mb")
            .and_then(|v| v.as_u64())
            .unwrap_or(64);
        let mut runtime_budget_denied = false;
        let mut native_runtime_denied = false;
        let estimated_file_write_bytes = estimated_native_file_write_bytes(&tool_name, &args);

        if let Some(ref key) = idempotency_key {
            if let Some(cached) = lookup_idempotent_outside_tx(&self.db, key, TOOL_CALL) {
                return cached;
            }
        }

        let execution_args = if !sidecar && self.tool_registry.is_registered(&tool_name) {
            if let Err(error) = preflight_native_tool_call(
                &self.db,
                self.tool_registry.as_ref(),
                &session_id,
                &tool_name,
                &args,
            ) {
                return CommandResponse::err(request_id, error);
            }
            match workspace_scoped_tool_args(&self.db, &session_id, &tool_name, &args) {
                Ok(args) => args,
                Err(error) => return CommandResponse::err(request_id, error),
            }
        } else {
            args.clone()
        };

        // Native tool dispatch: if the tool is registered and not sidecar, execute it.
        if !sidecar && outcome_kind == "success" && self.tool_registry.is_registered(&tool_name) {
            let mut runtime = self.runtime_manager.lock().unwrap();
            let tool_slot = runtime.try_acquire_tool(tool_call_id.clone());
            let file_write_reservation = if tool_slot.is_ok() {
                estimated_file_write_bytes.and_then(|bytes| {
                    runtime
                        .try_reserve_file_write(format!("{tool_call_id}:file_write"), bytes)
                        .ok()
                })
            } else {
                None
            };
            let file_write_allowed =
                estimated_file_write_bytes.is_none() || file_write_reservation.is_some();
            drop(runtime);

            if tool_slot.is_err() || !file_write_allowed {
                if let Ok(tool_slot) = tool_slot {
                    let mut runtime = self.runtime_manager.lock().unwrap();
                    runtime.release_tool(&tool_slot);
                    if let Some(reservation) = &file_write_reservation {
                        runtime.release_file_write_reservation(reservation);
                    }
                }
                native_runtime_denied = true;
            } else {
                let tool_slot = tool_slot.unwrap();
                let tool_result = self.tool_registry.execute(&tool_name, &execution_args);
                let mut runtime = self.runtime_manager.lock().unwrap();
                runtime.release_tool(&tool_slot);
                if tool_result.success {
                    if let Some(reservation) = &file_write_reservation {
                        runtime.commit_file_write_reservation(reservation);
                    }
                    result = tool_result.output;
                } else {
                    if let Some(reservation) = &file_write_reservation {
                        runtime.release_file_write_reservation(reservation);
                    }
                    outcome_kind = "failed".into();
                    result = json!({
                        "error": tool_result.error.unwrap_or_else(|| "Tool execution failed".into())
                    });
                }
            }
        }

        if sidecar && outcome_kind == "success" && requested_units <= budget_limit {
            let reservation = self
                .runtime_manager
                .lock()
                .unwrap()
                .try_acquire_sidecar(tool_call_id.clone(), sidecar_memory_mb);
            match reservation {
                Ok(reservation) => {
                    let command = self
                        .sidecar_commands
                        .get(&tool_name)
                        .expect("sidecar registration checked before reservation");
                    match self.execute_registered_sidecar(
                        command.clone(),
                        &cmd,
                        &session_id,
                        &tool_call_id,
                        &tool_name,
                        &args,
                        requested_units,
                    ) {
                        Ok(value) => {
                            result = value;
                        }
                        Err(SidecarErrorCode::Timeout) => {
                            outcome_kind = "timeout".into();
                        }
                        Err(_) => {
                            outcome_kind = "failed".into();
                        }
                    }
                    self.runtime_manager
                        .lock()
                        .unwrap()
                        .release_sidecar(&reservation.token);
                }
                Err(_) => {
                    runtime_budget_denied = true;
                }
            }
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TOOL_CALL)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let budget_exceeded =
                    requested_units > budget_limit || runtime_budget_denied || native_runtime_denied;
                let final_status = if budget_exceeded {
                    "rejected"
                } else if outcome_kind == "timeout" {
                    "timeout"
                } else if outcome_kind == "failed" {
                    "failed"
                } else {
                    "completed"
                };

                begin_tool_call_in_tx(
                    tx,
                    &session_id,
                    &tool_call_id,
                    &tool_name,
                    &tool_type,
                    &args,
                    occurred_at,
                    json!({
                        "requested_units": requested_units,
                        "budget_limit": budget_limit,
                        "estimated_file_write_bytes": estimated_file_write_bytes,
                    }),
                )?;

                let initiated_payload = json!({
                    "session_id": session_id,
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                    "tool_type": tool_type,
                    "args": args,
                });
                let persisted_initiated_payload = json!({
                    "session_id": session_id,
                    "tool_call_id": tool_call_id,
                    "tool_name": tool_name,
                    "tool_type": tool_type,
                    "args": sanitized_persistence_value(&args),
                });
                let mut events = vec![append_event_with_persisted_payload_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "tool.call_initiated",
                    actor.clone(),
                    initiated_payload,
                    persisted_initiated_payload,
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("tool_call_initiated_{session_id}_{tool_call_id}"),
                )?];

                if budget_exceeded {
                    set_tool_call_terminal(
                        tx,
                        &session_id,
                        &tool_call_id,
                        "rejected",
                        None,
                        Some("Resource budget exceeded"),
                        occurred_at,
                    )?;
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "resource.budget_exceeded",
                        actor.clone(),
                        json!({
                            "tool_call_id": tool_call_id,
                            "requested_units": requested_units,
                            "budget_limit": budget_limit,
                            "estimated_file_write_bytes": estimated_file_write_bytes,
                        }),
                        occurred_at,
                        &request_id,
                    )?);
                } else if sidecar {
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "resource.sidecar_started",
                        actor.clone(),
                        json!({"tool_call_id": tool_call_id, "tool_name": tool_name}),
                        occurred_at,
                        &request_id,
                    )?);
                    match outcome_kind.as_str() {
                        "timeout" => {
                            set_tool_call_terminal(
                                tx,
                                &session_id,
                                &tool_call_id,
                                "timeout",
                                None,
                                Some("timeout"),
                                occurred_at,
                            )?;
                            events.push(simple_event(
                                tx,
                                &session_id,
                                generation,
                                "tool.call_timeout",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "tool_name": tool_name}),
                                occurred_at,
                                &request_id,
                            )?);
                            events.push(simple_event(
                                tx,
                                &session_id,
                                generation,
                                "resource.sidecar_cancelled",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "reason": "timeout"}),
                                occurred_at,
                                &request_id,
                            )?);
                        }
                        "failed" => {
                            set_tool_call_terminal(
                                tx,
                                &session_id,
                                &tool_call_id,
                                "failed",
                                None,
                                Some("sidecar failed"),
                                occurred_at,
                            )?;
                            events.push(simple_event(
                                tx,
                                &session_id,
                                generation,
                                "resource.sidecar_failed",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "reason": "sidecar failed"}),
                                occurred_at,
                                &request_id,
                            )?);
                            events.push(simple_event(
                                tx,
                                &session_id,
                                generation,
                                "tool.call_failed",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "error": "sidecar failed"}),
                                occurred_at,
                                &request_id,
                            )?);
                        }
                        _ => {
                            set_tool_call_terminal(
                                tx,
                                &session_id,
                                &tool_call_id,
                                "completed",
                                Some(&result),
                                None,
                                occurred_at,
                            )?;
                            events.push(simple_event(
                                tx,
                                &session_id,
                                generation,
                                "resource.sidecar_completed",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "resource_units": requested_units}),
                                occurred_at,
                                &request_id,
                            )?);
                            events.push(append_event_with_persisted_payload_in_tx(
                                tx,
                                Some(session_id.clone()),
                                generation,
                                "tool.call_completed",
                                actor.clone(),
                                json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "result": result}),
                                json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "result": sanitized_persistence_value(&result)}),
                                occurred_at,
                                Some(request_id.clone()),
                                Some(request_id.clone()),
                                format!("tool_call_completed_{session_id}_{tool_call_id}"),
                            )?);
                        }
                    }
                } else if outcome_kind == "failed" {
                    set_tool_call_terminal(
                        tx,
                        &session_id,
                        &tool_call_id,
                        "failed",
                        None,
                        Some("tool failed"),
                        occurred_at,
                    )?;
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "tool.call_failed",
                        actor.clone(),
                        json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "error": "tool failed"}),
                        occurred_at,
                        &request_id,
                    )?);
                } else {
                    set_tool_call_terminal(
                        tx,
                        &session_id,
                        &tool_call_id,
                        "completed",
                        Some(&result),
                        None,
                        occurred_at,
                    )?;
                    events.push(append_event_with_persisted_payload_in_tx(
                        tx,
                        Some(session_id.clone()),
                        generation,
                        "tool.call_completed",
                        actor.clone(),
                        json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "result": result}),
                        json!({"tool_call_id": tool_call_id, "tool_name": tool_name, "result": sanitized_persistence_value(&result)}),
                        occurred_at,
                        Some(request_id.clone()),
                        Some(request_id.clone()),
                        format!("tool_call_completed_{session_id}_{tool_call_id}"),
                    )?);
                }

                let response = response_with_events(
                    request_id.clone(),
                    events,
                    json!({
                        "session_id": session_id,
                        "tool_call_id": tool_call_id,
                        "tool_name": tool_name,
                        "status": final_status,
                    }),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TOOL_CALL, &response)?;
                }
                Ok(response)
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("tool.call failed: {e}")),
            )
        })
    }

    fn handle_tool_cancel(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some((session_id, tool_call_id)) = tool_call_ref_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(
                    ErrorCode::InvalidTransition,
                    "Missing session_id or tool_call_id",
                ),
            );
        };
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TOOL_CANCEL)? {
                        return Ok(cached);
                    }
                }
                let tool_type: String = tx
                    .query_row(
                        "SELECT tool_type FROM tool_calls WHERE session_id = ?1 AND id = ?2",
                        params![session_id, tool_call_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .unwrap_or_else(|| "sidecar".into());
                tx.execute(
                    "INSERT INTO tool_calls \
                     (id, session_id, tool_name, tool_type, status, started_at, cancelled_at) \
                     VALUES (?1, ?2, 'unknown', ?3, 'cancelled', ?4, ?4) \
                     ON CONFLICT(session_id, id) DO UPDATE SET \
                     status = 'cancelled', cancelled_at = excluded.cancelled_at",
                    params![tool_call_id, session_id, tool_type, occurred_at],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let mut events = vec![simple_event(
                    tx,
                    &session_id,
                    generation,
                    "tool.call_cancelled",
                    actor.clone(),
                    json!({"tool_call_id": tool_call_id}),
                    occurred_at,
                    &request_id,
                )?];
                if matches!(tool_type.as_str(), "sidecar" | "browser") {
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "resource.sidecar_cancelled",
                        actor.clone(),
                        json!({"tool_call_id": tool_call_id, "reason": "cancelled"}),
                        occurred_at,
                        &request_id,
                    )?);
                }
                let response = response_with_events(
                    request_id.clone(),
                    events,
                    json!({"session_id": session_id, "tool_call_id": tool_call_id, "status": "cancelled"}),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TOOL_CANCEL, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("tool.cancel failed: {e}")),
            )
        })
    }

    fn handle_llm_call(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "mock/model".into());
        let provider_id = string_param(&cmd, &["provider", "provider_id"]).unwrap_or_else(|| {
            if self.llm_router.is_some() {
                "external".into()
            } else {
                "mock".into()
            }
        });
        let llm_call_id = string_param(&cmd, &["llm_call_id", "id"])
            .unwrap_or_else(|| format!("llm_{}", now_ms()));
        let agent_id = string_param(&cmd, &["agent_id"]).unwrap_or_default();
        let agent_name = string_param(&cmd, &["agent_name"]).unwrap_or_else(|| {
            if agent_id.is_empty() {
                "core".into()
            } else {
                agent_id.clone()
            }
        });
        let token_budget = cmd.params.get("token_budget").and_then(|v| v.as_u64());
        let agent_token_budget = cmd
            .params
            .get("agent_token_budget")
            .and_then(|v| v.as_u64());
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let prompt = string_param(&cmd, &["prompt"]).unwrap_or_default();

        if let Some(ref key) = idempotency_key {
            if let Some(cached) = lookup_idempotent_outside_tx(&self.db, key, LLM_CALL) {
                return cached;
            }
        }
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }

        let estimated_prompt_tokens = estimate_text_tokens(&prompt);
        if let Some(budget) = token_budget {
            if estimated_prompt_tokens > budget {
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::InvalidTransition,
                        "LLM prompt exceeds token budget before provider call",
                        json!({
                            "llm_call_id": llm_call_id,
                            "estimated_prompt_tokens": estimated_prompt_tokens,
                            "budget": budget,
                        }),
                        false,
                    ),
                );
            }
        }
        let token_reservation = format!("llm_tokens:{llm_call_id}");
        let agent_token_reservation = format!("llm_agent_tokens:{llm_call_id}");
        if let Err(err) = self
            .runtime_manager
            .lock()
            .unwrap()
            .try_reserve_tokens(token_reservation.clone(), estimated_prompt_tokens)
        {
            return CommandResponse::err(
                request_id,
                CoreError::with_details(
                    ErrorCode::InvalidTransition,
                    "Runtime token budget exceeded before provider call",
                    json!({
                        "llm_call_id": llm_call_id,
                        "estimated_prompt_tokens": estimated_prompt_tokens,
                        "requested": err.requested,
                        "available": err.available,
                    }),
                    false,
                ),
            );
        }
        if !agent_id.is_empty() {
            let agent_budget_check = {
                let mut runtime = self.runtime_manager.lock().unwrap();
                if let Some(budget) = agent_token_budget {
                    runtime.set_agent_token_budget(agent_id.clone(), budget);
                }
                runtime.try_reserve_agent_tokens(
                    agent_token_reservation.clone(),
                    &agent_id,
                    estimated_prompt_tokens,
                )
            };
            if let Err(err) = agent_budget_check {
                self.runtime_manager
                    .lock()
                    .unwrap()
                    .release_token_reservation(&token_reservation);
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::InvalidTransition,
                        "Agent token budget exceeded before provider call",
                        json!({
                            "llm_call_id": llm_call_id,
                            "agent_id": agent_id,
                            "estimated_prompt_tokens": estimated_prompt_tokens,
                            "requested": err.requested,
                            "available": err.available,
                        }),
                        false,
                    ),
                );
            }
        }

        let request = GenerateRequest {
            model: model.clone(),
            messages: vec![Message {
                role: "user".into(),
                content: prompt,
                ..Default::default()
            }],
            tools: self.tool_registry.llm_tool_definitions(),
            stream: true,
            auth_context: auth_context_from_cmd(&cmd).unwrap_or(AuthContext::None),
            options: request_options_from_cmd(&cmd),
        };
        let stream = match self.route_llm_stream(request) {
            Ok(stream) => stream,
            Err(err) => {
                let mut runtime = self.runtime_manager.lock().unwrap();
                runtime.release_token_reservation(&token_reservation);
                runtime.release_agent_token_reservation(&agent_token_reservation);
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::Internal,
                        "LLM provider routing failed",
                        provider_error_json(&err),
                        err.retryable,
                    ),
                );
            }
        };
        let summary_override = if token_budget.is_some() {
            self.summarize_compacted_context(&session_id, 20, &model)
        } else {
            None
        };

        let mut observed_total_tokens = 0_u64;
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, LLM_CALL)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let mut events = vec![simple_event(
                    tx,
                    &session_id,
                    generation,
                    "llm.call_started",
                    actor.clone(),
                    json!({"llm_call_id": llm_call_id, "model": model}),
                    occurred_at,
                    &request_id,
                )?];
                let mut realtime = Vec::new();
                let mut model_tool_requests: Vec<ToolCall> = Vec::new();
                let mut tool_accumulator = ToolCallAccumulator::new();
                let mut usage = json!({"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0});
                let mut finish_reason = "unknown".to_string();

                for item in stream {
                    match item {
                        Ok(StreamEvent::ThinkingDelta(text)) => realtime.push(json!({
                            "event_type": "realtime.llm.thinking_delta",
                            "payload": {"text": text},
                        })),
                        Ok(StreamEvent::TextDelta(text)) => realtime.push(json!({
                            "event_type": "realtime.llm.text_delta",
                            "payload": {"text": text},
                        })),
                        Ok(StreamEvent::ToolCallDelta(delta)) => {
                            realtime.push(json!({
                                "event_type": "realtime.llm.tool_call_delta",
                                "payload": {
                                    "index": delta.index,
                                    "tool_call_id": delta.id,
                                    "name": delta.name,
                                    "args_delta": delta.partial_json,
                                },
                            }));
                            tool_accumulator.append(delta);
                        }
                        Ok(StreamEvent::ToolCall(call)) => {
                            if !model_tool_requests.iter().any(|existing| existing.id == call.id) {
                                model_tool_requests.push(call);
                            }
                        }
                        Ok(StreamEvent::Usage(token_usage)) => {
                            usage = json!({
                                "prompt_tokens": token_usage.prompt_tokens,
                                "completion_tokens": token_usage.completion_tokens,
                                "total_tokens": token_usage.total_tokens,
                                "reasoning_tokens": token_usage.reasoning_tokens,
                            });
                            tx.execute(
                                "INSERT INTO token_usage \
                                 (session_id, agent_id, agent_name, model_name, prompt_tokens, completion_tokens, total_tokens, cache_read_tokens, cache_creation_tokens, timestamp) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    session_id,
                                    agent_id,
                                    agent_name,
                                    model,
                                    token_usage.prompt_tokens,
                                    token_usage.completion_tokens,
                                    token_usage.total_tokens,
                                    token_usage.cache_read_input_tokens.unwrap_or(0),
                                    token_usage.cache_creation_input_tokens.unwrap_or(0),
                                    occurred_at as f64 / 1000.0
                                ],
                            )?;
                        }
                        Ok(StreamEvent::Finished(reason)) => {
                            finish_reason = format!("{:?}", reason).to_lowercase();
                        }
                        Ok(StreamEvent::Error(err)) | Err(err) => {
                            finish_reason = "error".into();
                            realtime.push(json!({
                                "event_type": "realtime.llm.error",
                                "payload": {"code": err.code.to_string(), "message": err.message},
                            }));
                        }
                    }
                }
                for call in tool_accumulator.finalize() {
                    if call.id.is_empty() && call.name.is_empty() {
                        continue;
                    }
                    if !model_tool_requests.iter().any(|existing| existing.id == call.id) {
                        model_tool_requests.push(call);
                    }
                }
                let mut model_tool_request_payloads = Vec::new();
                let mut response_model_tool_requests = Vec::new();
                for call in &model_tool_requests {
                    tx.execute(
                        "INSERT OR IGNORE INTO tool_calls \
                         (id, session_id, tool_name, tool_type, status, args_json, started_at, resource_usage_json) \
                         VALUES (?1, ?2, ?3, 'model_tool_request', 'model_tool_request', ?4, ?5, ?6)",
                        params![
                            call.id,
                            session_id,
                            call.name,
                            sanitized_persistence_value(&call.arguments).to_string(),
                            occurred_at,
                            json!({"llm_call_id": llm_call_id}).to_string()
                        ],
                    )?;
                    let payload = json!({
                        "llm_call_id": llm_call_id,
                        "tool_call_id": call.id,
                        "tool_name": call.name,
                        "args": sanitized_persistence_value(&call.arguments),
                        "status": "model_tool_request",
                    });
                    response_model_tool_requests.push(json!({
                        "llm_call_id": llm_call_id,
                        "tool_call_id": call.id,
                        "tool_name": call.name,
                        "args": call.arguments,
                        "status": "model_tool_request",
                    }));
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "llm.model_tool_request",
                        actor.clone(),
                        payload.clone(),
                        occurred_at,
                        &request_id,
                    )?);
                    model_tool_request_payloads.push(payload);
                }

                events.push(simple_event(
                    tx,
                    &session_id,
                    generation,
                    "llm.call_finished",
                    actor.clone(),
                    json!({
                        "llm_call_id": llm_call_id,
                        "model": model,
                        "finish_reason": finish_reason,
                        "usage": usage,
                        "model_tool_requests": model_tool_request_payloads,
                    }),
                    occurred_at,
                    &request_id,
                )?);

                let total_tokens = usage
                    .get("total_tokens")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                observed_total_tokens = total_tokens;
                if token_budget.is_some_and(|budget| total_tokens > budget) {
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "resource.budget_exceeded",
                        actor.clone(),
                        json!({"llm_call_id": llm_call_id, "total_tokens": total_tokens, "budget": token_budget}),
                        occurred_at,
                        &request_id,
                    )?);
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "persistence.compaction_started",
                        actor.clone(),
                        json!({"reason": "token_budget_exceeded"}),
                        occurred_at,
                        &request_id,
                    )?);
                    let active_context =
                        compact_leader_context_in_tx(
                            tx,
                            &session_id,
                            20,
                            Some(&llm_call_id),
                            summary_override.as_deref(),
                        )?;
                    events.push(simple_event(
                        tx,
                        &session_id,
                        generation,
                        "persistence.compaction_completed",
                        actor.clone(),
                        json!({
                            "reason": "token_budget_exceeded",
                            "retained_event_log": true,
                            "active_context": active_context,
                        }),
                        occurred_at,
                        &request_id,
                    )?);
                }

                tx.execute(
                    "INSERT INTO llm_gateway_requests \
                     (trace_id, session_id, agent_id, agent_name, requested_model, selected_model, final_model, provider, status, prompt_tokens, completion_tokens, total_tokens, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?5, ?6, 'completed', ?7, ?8, ?9, ?10)",
                    params![
                        llm_call_id,
                        session_id,
                        agent_id,
                        agent_name,
                        model,
                        provider_id,
                        usage.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
                        usage.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
                        total_tokens,
                        occurred_at as f64 / 1000.0,
                    ],
                )?;

                let response = response_with_events(
                    request_id.clone(),
                    events,
                    json!({
                        "session_id": session_id,
                        "llm_call_id": llm_call_id,
                        "finish_reason": finish_reason,
                        "usage": usage,
                        "realtime_events": realtime,
                        "model_tool_requests": response_model_tool_requests,
                    }),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, LLM_CALL, &response)?;
                }
                Ok(response)
            });

        match outcome {
            Ok(response) => {
                let mut runtime = self.runtime_manager.lock().unwrap();
                if observed_total_tokens > 0 {
                    runtime
                        .commit_token_reservation_actual(&token_reservation, observed_total_tokens);
                    runtime.commit_agent_token_reservation_actual(
                        &agent_token_reservation,
                        observed_total_tokens,
                    );
                } else {
                    runtime.release_token_reservation(&token_reservation);
                    runtime.release_agent_token_reservation(&agent_token_reservation);
                }
                response
            }
            Err(e) => {
                let mut runtime = self.runtime_manager.lock().unwrap();
                runtime.release_token_reservation(&token_reservation);
                runtime.release_agent_token_reservation(&agent_token_reservation);
                CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("llm.call failed: {e}")),
                )
            }
        }
    }

    fn handle_runtime_compact(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let retain_last = cmd
            .params
            .get("retain_last")
            .and_then(|value| value.as_u64())
            .unwrap_or(20) as usize;
        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "mock/model".into());
        let summary_override = self.summarize_compacted_context(&session_id, retain_last, &model);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, RUNTIME_COMPACT)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let active_context = compact_leader_context_in_tx(
                    tx,
                    &session_id,
                    retain_last,
                    None,
                    summary_override.as_deref(),
                )?;
                let message_count = active_context["original_message_count"]
                    .as_i64()
                    .unwrap_or_default();
                let events = vec![
                    simple_event(
                        tx,
                        &session_id,
                        generation,
                        "persistence.compaction_started",
                        actor.clone(),
                        json!({"message_count": message_count}),
                        occurred_at,
                        &request_id,
                    )?,
                    {
                        simple_event(
                            tx,
                            &session_id,
                            generation,
                            "persistence.compaction_completed",
                            actor.clone(),
                            json!({
                                "message_count": message_count,
                                "original_rows_retained": true,
                                "active_context": active_context,
                            }),
                            occurred_at,
                            &request_id,
                        )?
                    },
                ];
                let response = response_with_events(
                    request_id.clone(),
                    events,
                    json!({
                        "session_id": session_id,
                        "message_count": message_count,
                        "original_rows_retained": true,
                        "active_context": active_context,
                    }),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, RUNTIME_COMPACT, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("runtime.compact failed: {e}")),
            )
        })
    }

    fn handle_runtime_debug_dump(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let sessions_by_status = count_by_column(&conn, "sessions", "status")?;
            let tasks_by_status = count_by_column(&conn, "tasks", "status")?;
            let workflows_by_status = count_by_column(&conn, "workflow_executions", "status")?;
            let agents_by_status = count_by_column(&conn, "agent_state", "status")?;
            let tool_calls_by_status = count_by_column(&conn, "tool_calls", "status")?;
            let pending_permissions = {
                let mut stmt = conn.prepare(
                    "SELECT id, session_id, tool_name, mode, created_at \
                     FROM permission_requests \
                     WHERE status = 'pending' \
                     ORDER BY created_at, id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(json!({
                        "permission_request_id": row.get::<_, String>(0)?,
                        "session_id": row.get::<_, String>(1)?,
                        "tool_name": row.get::<_, String>(2)?,
                        "mode": row.get::<_, String>(3)?,
                        "created_at": row.get::<_, Timestamp>(4)?,
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            let running_workflows = {
                let mut stmt = conn.prepare(
                    "SELECT id, workflow_id, session_id, start_time \
                     FROM workflow_executions \
                     WHERE status = 'running' \
                     ORDER BY start_time, id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(json!({
                        "execution_id": row.get::<_, String>(0)?,
                        "workflow_id": row.get::<_, String>(1)?,
                        "session_id": row.get::<_, String>(2)?,
                        "start_time": row.get::<_, Timestamp>(3)?,
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            let running_agents = {
                let mut stmt = conn.prepare(
                    "SELECT session_id, agent_id, agent_name, agent_role, task_id, timestamp \
                     FROM agent_state \
                     WHERE status = 'running' \
                     ORDER BY timestamp, agent_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    Ok(json!({
                        "session_id": row.get::<_, String>(0)?,
                        "agent_id": row.get::<_, String>(1)?,
                        "agent_name": row.get::<_, String>(2)?,
                        "agent_role": row.get::<_, String>(3)?,
                        "task_id": row.get::<_, String>(4)?,
                        "timestamp": row.get::<_, f64>(5)?,
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            let session_context_windows = {
                let mut stmt = conn.prepare(
                    "SELECT s.id, \
                            (SELECT COUNT(*) FROM leader_conversation lc WHERE lc.session_id = s.id), \
                            ss.value \
                     FROM sessions s \
                     LEFT JOIN session_state ss \
                       ON ss.session_id = s.id AND ss.key = 'active_context_projection' \
                     ORDER BY s.created_at, s.id",
                )?;
                let rows = stmt.query_map([], |row| {
                    let projection_raw: Option<String> = row.get(2)?;
                    let projection = projection_raw
                        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                        .unwrap_or(Value::Null);
                    Ok(json!({
                        "session_id": row.get::<_, String>(0)?,
                        "original_message_count": row.get::<_, i64>(1)?,
                        "active_message_count": projection
                            .get("active_message_count")
                            .and_then(|value| value.as_i64()),
                        "has_active_context_projection": !projection.is_null(),
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            let agent_context_windows = {
                let mut stmt = conn.prepare(
                    "SELECT ac.session_id, ac.agent_id, COUNT(*), ss.value \
                     FROM agent_conversation ac \
                     LEFT JOIN session_state ss \
                       ON ss.session_id = ac.session_id \
                      AND ss.key = ('agent_active_context_projection:' || ac.agent_id) \
                     GROUP BY ac.session_id, ac.agent_id, ss.value \
                     ORDER BY ac.session_id, ac.agent_id",
                )?;
                let rows = stmt.query_map([], |row| {
                    let projection_raw: Option<String> = row.get(3)?;
                    let projection = projection_raw
                        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                        .unwrap_or(Value::Null);
                    Ok(json!({
                        "session_id": row.get::<_, String>(0)?,
                        "agent_id": row.get::<_, String>(1)?,
                        "original_message_count": row.get::<_, i64>(2)?,
                        "active_message_count": projection
                            .get("active_message_count")
                            .and_then(|value| value.as_i64()),
                        "has_active_context_projection": !projection.is_null(),
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({
                    "schema_version": crate::persistence::SCHEMA_VERSION,
                    "sessions_by_status": sessions_by_status,
                    "tasks_by_status": tasks_by_status,
                    "workflows_by_status": workflows_by_status,
                    "agents_by_status": agents_by_status,
                    "tool_calls_by_status": tool_calls_by_status,
                    "pending_permissions": pending_permissions,
                    "running_workflows": running_workflows,
                    "running_agents": running_agents,
                    "session_context_windows": session_context_windows,
                    "agent_context_windows": agent_context_windows,
                    "redaction": {
                        "message_bodies": "omitted",
                        "auth_context": "omitted",
                        "tool_args": "omitted",
                        "tool_results": "omitted"
                    }
                })),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("runtime.debug_dump failed: {e}")),
            )
        })
    }

    fn handle_trace_timeline(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_filter = session_id_from_cmd(&cmd);
        let operation_filter = string_param(&cmd, &["operation", "method"]);
        let limit = cmd
            .params
            .get("limit")
            .and_then(Value::as_u64)
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or(100)
            .clamp(1, 1000);

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let spans = select_trace_spans(
                &conn,
                session_filter.as_deref(),
                operation_filter.as_deref(),
                limit,
            )?;
            let execution_events =
                select_execution_trace_events(&conn, session_filter.as_deref(), limit)?;
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({
                    "spans": spans,
                    "execution_events": execution_events,
                    "redaction": {
                        "command_params": "omitted",
                        "command_results": "omitted",
                        "message_bodies": "omitted",
                        "auth_context": "omitted"
                    }
                })),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("trace.timeline failed: {e}")),
            )
        })
    }

    fn handle_metrics_query(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_filter = session_id_from_cmd(&cmd);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let metrics = collect_metrics(&conn, session_filter.as_deref())?;
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({
                    "metrics": metrics,
                    "scope": {
                        "session_id": session_filter,
                    },
                    "redaction": {
                        "command_params": "omitted",
                        "command_results": "omitted",
                        "tool_args": "omitted",
                        "tool_results": "omitted",
                        "auth_context": "omitted",
                        "message_bodies": "omitted"
                    }
                })),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("metrics.query failed: {e}")),
            )
        })
    }

    fn handle_worktree_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(repo_root) = string_param(&cmd, &["repo_root", "repository"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing repo_root"),
            );
        };
        let Some(path) = string_param(&cmd, &["path", "worktree_path"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing path"),
            );
        };
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "git_write", Some(&repo_root))
                .or_else(|_| {
                    require_scoped_permission_grant(&self.db, &session_id, "git_write", Some(&path))
                })
        {
            return CommandResponse::err(request_id, error);
        }
        let Some(branch) = string_param(&cmd, &["branch", "branch_name"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing branch"),
            );
        };
        let base_branch =
            string_param(&cmd, &["base_branch", "base"]).unwrap_or_else(|| "HEAD".into());
        let worktree_id = string_param(&cmd, &["worktree_id", "id"])
            .unwrap_or_else(|| format!("worktree_{}", now_ms()));
        let name = string_param(&cmd, &["name"]).unwrap_or_else(|| branch.clone());
        let task_id = string_param(&cmd, &["task_id"]);
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();

        if let Err(error) = validate_worktree_paths(&repo_root, &path, false) {
            return CommandResponse::err(request_id, error);
        }
        if let Err(error) = run_git_worktree(
            &repo_root,
            &["worktree", "add", "-b", &branch, &path, &base_branch],
        ) {
            return CommandResponse::err(request_id, error);
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO worktrees \
                     (id, name, repo_root, path, branch, base_branch, session_id, task_id, status, created_at, updated_at, last_error) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'active', ?9, ?9, NULL) \
                     ON CONFLICT(id) DO UPDATE SET \
                     name = excluded.name, repo_root = excluded.repo_root, path = excluded.path, \
                     branch = excluded.branch, base_branch = excluded.base_branch, session_id = excluded.session_id, \
                     task_id = excluded.task_id, status = 'active', updated_at = excluded.updated_at, last_error = NULL",
                    params![
                        worktree_id,
                        name,
                        repo_root,
                        path,
                        branch,
                        base_branch,
                        session_id,
                        task_id,
                        occurred_at as f64 / 1000.0
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "worktree.created",
                    actor,
                    json!({
                        "worktree_id": worktree_id,
                        "repo_root": repo_root,
                        "path": path,
                        "branch": branch,
                        "base_branch": base_branch,
                        "status": "active",
                    }),
                    occurred_at,
                    &request_id,
                )?;
                let event_seq = event.seq;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "worktree_id": worktree_id,
                        "session_id": session_id,
                        "path": path,
                        "branch": branch,
                        "status": "active",
                        "latest_seq": event_seq,
                    })),
                ))
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("worktree.create failed: {e}")),
            )
        })
    }

    fn handle_worktree_delete(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let worktree_id = string_param(&cmd, &["worktree_id", "id"]);
        let path_param = string_param(&cmd, &["path", "worktree_path"]);
        if worktree_id.is_none() && path_param.is_none() {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing worktree_id or path"),
            );
        }
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();

        let lookup = {
            let conn = self.db.conn();
            if let Some(ref id) = worktree_id {
                conn.query_row(
                    "SELECT id, repo_root, path FROM worktrees WHERE session_id = ?1 AND id = ?2 AND status = 'active'",
                    params![session_id, id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
                )
                .optional()
            } else {
                conn.query_row(
                    "SELECT id, repo_root, path FROM worktrees WHERE session_id = ?1 AND path = ?2 AND status = 'active'",
                    params![session_id, path_param.as_deref().unwrap_or_default()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
                )
                .optional()
            }
        };
        let (worktree_id, repo_root, path) = match lookup {
            Ok(Some(value)) => value,
            Ok(None) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Active worktree not found"),
                )
            }
            Err(e) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("worktree lookup failed: {e}")),
                )
            }
        };
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "git_write", Some(&repo_root))
                .or_else(|_| {
                    require_scoped_permission_grant(&self.db, &session_id, "git_write", Some(&path))
                })
        {
            return CommandResponse::err(request_id, error);
        }
        if let Err(error) = validate_worktree_paths(&repo_root, &path, true) {
            return CommandResponse::err(request_id, error);
        }
        if let Err(error) = run_git_worktree(&repo_root, &["worktree", "remove", "--force", &path])
        {
            return CommandResponse::err(request_id, error);
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "UPDATE worktrees SET status = 'deleted', updated_at = ?1, last_error = NULL \
                     WHERE session_id = ?2 AND id = ?3",
                    params![occurred_at as f64 / 1000.0, session_id, worktree_id],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "worktree.deleted",
                    actor,
                    json!({
                        "worktree_id": worktree_id,
                        "path": path,
                        "status": "deleted",
                    }),
                    occurred_at,
                    &request_id,
                )?;
                let event_seq = event.seq;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "worktree_id": worktree_id,
                        "session_id": session_id,
                        "path": path,
                        "status": "deleted",
                        "latest_seq": event_seq,
                    })),
                ))
            });

        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("worktree.delete failed: {e}")),
            )
        })
    }

    fn handle_worktree_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_filter = session_id_from_cmd(&cmd);
        let include_deleted = cmd
            .params
            .get("include_deleted")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let worktrees = select_worktrees(&conn, session_filter.as_deref(), include_deleted)?;
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({"worktrees": worktrees})),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("worktree.list failed: {e}")),
            )
        })
    }

    fn handle_terminal_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let terminal_id = string_param(&cmd, &["terminal_id", "id"])
            .unwrap_or_else(|| format!("terminal_{}", now_ms()));
        let shell = string_param(&cmd, &["shell", "program"]);
        let args = string_array_param(&cmd, "args");
        let cwd = string_param(&cmd, &["cwd"]).map(std::path::PathBuf::from);
        let cwd_scope = cwd.as_ref().map(|path| path.to_string_lossy().to_string());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "terminal", cwd_scope.as_deref())
        {
            return CommandResponse::err(request_id, error);
        }
        let created = match self.terminal_manager.create(TerminalCreateOptions {
            terminal_id: terminal_id.clone(),
            shell: shell.clone(),
            args,
            cwd: cwd.clone(),
        }) {
            Ok(created) => created,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("terminal.create failed: {error}")),
                )
            }
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                tx.execute(
                    "INSERT INTO terminal_sessions \
                     (id, session_id, pid, shell, cwd, status, created_at, last_activity_at, completed_at, exit_code, last_error) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 'running', ?6, ?6, NULL, NULL, NULL) \
                     ON CONFLICT(id) DO UPDATE SET \
                     session_id = excluded.session_id, pid = excluded.pid, shell = excluded.shell, \
                     cwd = excluded.cwd, status = 'running', created_at = excluded.created_at, \
                     last_activity_at = excluded.last_activity_at, completed_at = NULL, exit_code = NULL, last_error = NULL",
                    params![
                        terminal_id,
                        session_id,
                        created.pid,
                        created.shell,
                        created.cwd,
                        occurred_at as f64 / 1000.0
                    ],
                )?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "terminal.created",
                    actor,
                    json!({
                        "terminal_id": terminal_id,
                        "pid": created.pid,
                        "shell": created.shell,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "terminal_id": terminal_id,
                        "pid": created.pid,
                        "shell": created.shell,
                        "status": "running",
                    })),
                ))
            });

        outcome.unwrap_or_else(|error| {
            let _ = self.terminal_manager.kill(&terminal_id);
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("terminal.create persist failed: {error}")),
            )
        })
    }

    fn handle_terminal_send(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let Some(terminal_id) = string_param(&cmd, &["terminal_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing terminal_id"),
            );
        };
        let terminal_scope = terminal_scope_from_db(&self.db, &session_id, &terminal_id);
        if let Err(error) = require_scoped_permission_grant(
            &self.db,
            &session_id,
            "terminal",
            terminal_scope.as_deref(),
        ) {
            return CommandResponse::err(request_id, error);
        }
        let Some(input) = string_param(&cmd, &["input", "text"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing input"),
            );
        };
        let bytes = match self.terminal_manager.send(&terminal_id, &input) {
            Ok(bytes) => bytes,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("terminal.send failed: {error}")),
                )
            }
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                tx.execute(
                    "UPDATE terminal_sessions SET last_activity_at = ?1 WHERE id = ?2 AND session_id = ?3",
                    params![occurred_at as f64 / 1000.0, terminal_id, session_id],
                )?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "terminal.input_sent",
                    actor,
                    json!({
                        "terminal_id": terminal_id,
                        "bytes": bytes,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"terminal_id": terminal_id, "bytes": bytes})),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("terminal.send persist failed: {error}")),
            )
        })
    }

    fn handle_terminal_read(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(terminal_id) = string_param(&cmd, &["terminal_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing terminal_id"),
            );
        };
        let max_bytes = cmd
            .params
            .get("max_bytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(65_536);
        let read = match self.terminal_manager.read(&terminal_id, max_bytes) {
            Ok(read) => read,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("terminal.read failed: {error}")),
                )
            }
        };
        if !read.running {
            let _ = self.db.conn().execute(
                "UPDATE terminal_sessions SET status = 'completed', completed_at = ?1, exit_code = ?2 \
                 WHERE id = ?3",
                params![now_ms() as f64 / 1000.0, read.exit_code, terminal_id],
            );
        }
        CommandResponse::ok(request_id, Some(terminal_read_json(read)), None)
    }

    fn handle_terminal_kill(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let Some(terminal_id) = string_param(&cmd, &["terminal_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing terminal_id"),
            );
        };
        let terminal_scope = terminal_scope_from_db(&self.db, &session_id, &terminal_id);
        if let Err(error) = require_scoped_permission_grant(
            &self.db,
            &session_id,
            "terminal",
            terminal_scope.as_deref(),
        ) {
            return CommandResponse::err(request_id, error);
        }
        let exit_code = match self.terminal_manager.kill(&terminal_id) {
            Ok(exit_code) => exit_code,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("terminal.kill failed: {error}")),
                )
            }
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                tx.execute(
                    "UPDATE terminal_sessions SET status = 'killed', completed_at = ?1, exit_code = ?2, last_activity_at = ?1 \
                     WHERE id = ?3 AND session_id = ?4",
                    params![occurred_at as f64 / 1000.0, exit_code, terminal_id, session_id],
                )?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "terminal.killed",
                    actor,
                    json!({
                        "terminal_id": terminal_id,
                        "exit_code": exit_code,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "terminal_id": terminal_id,
                        "status": "killed",
                        "exit_code": exit_code,
                    })),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("terminal.kill persist failed: {error}")),
            )
        })
    }

    fn handle_repl_eval(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let eval_id =
            string_param(&cmd, &["eval_id", "id"]).unwrap_or_else(|| format!("repl_{}", now_ms()));
        let language =
            string_param(&cmd, &["language", "runtime"]).unwrap_or_else(|| "python".into());
        let Some(code) = string_param(&cmd, &["code", "input"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing code"),
            );
        };
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(5_000);
        let cwd = string_param(&cmd, &["cwd"]).map(std::path::PathBuf::from);
        let cwd_scope = cwd.as_ref().map(|path| path.to_string_lossy().to_string());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "repl", cwd_scope.as_deref())
        {
            return CommandResponse::err(request_id, error);
        }

        let result = match self.repl_runner.eval(ReplEvalRequest {
            eval_id: eval_id.clone(),
            language: language.clone(),
            code,
            cwd,
            timeout_ms,
        }) {
            Ok(result) => result,
            Err(ReplEvalError::MissingRuntime { language }) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::InvalidTransition,
                        format!("REPL runtime missing: {language}"),
                        json!({"kind": "missing_runtime", "language": language}),
                        false,
                    ),
                )
            }
            Err(ReplEvalError::Timeout) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::InvalidTransition,
                        "REPL evaluation timed out",
                        json!({"kind": "timeout", "eval_id": eval_id}),
                        true,
                    ),
                )
            }
            Err(ReplEvalError::SpawnFailed(error) | ReplEvalError::WaitFailed(error)) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("repl.eval failed: {error}")),
                )
            }
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "repl.evaluated",
                    actor,
                    json!({
                        "eval_id": eval_id,
                        "language": language,
                        "runtime": result["runtime"],
                        "pid": result["pid"],
                        "exit_code": result["exit_code"],
                        "success": result["success"],
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(result),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("repl.eval persist failed: {error}")),
            )
        })
    }

    fn handle_repl_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let repl_id =
            string_param(&cmd, &["repl_id", "id"]).unwrap_or_else(|| format!("repl_{}", now_ms()));
        let language =
            string_param(&cmd, &["language", "runtime"]).unwrap_or_else(|| "python".into());
        let cwd = string_param(&cmd, &["cwd"]).map(std::path::PathBuf::from);
        let cwd_scope = cwd.as_ref().map(|path| path.to_string_lossy().to_string());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "repl", cwd_scope.as_deref())
        {
            return CommandResponse::err(request_id, error);
        }
        let (program, args) = match repl_session_command(&language) {
            Some(command) => command,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::with_details(
                        ErrorCode::InvalidTransition,
                        format!("REPL runtime missing: {language}"),
                        json!({"kind": "missing_runtime", "language": language}),
                        false,
                    ),
                )
            }
        };
        let created = match self.terminal_manager.create(TerminalCreateOptions {
            terminal_id: repl_id.clone(),
            shell: Some(program.clone()),
            args,
            cwd: cwd.clone(),
        }) {
            Ok(created) => created,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("repl.create failed: {error}")),
                )
            }
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                tx.execute(
                    "INSERT INTO terminal_sessions \
                     (id, session_id, pid, shell, cwd, status, created_at, last_activity_at, completed_at, exit_code, last_error) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 'running', ?6, ?6, NULL, NULL, NULL) \
                     ON CONFLICT(id) DO UPDATE SET \
                     session_id = excluded.session_id, pid = excluded.pid, shell = excluded.shell, \
                     cwd = excluded.cwd, status = 'running', created_at = excluded.created_at, \
                     last_activity_at = excluded.last_activity_at, completed_at = NULL, exit_code = NULL, last_error = NULL",
                    params![
                        repl_id,
                        session_id,
                        created.pid,
                        format!("repl:{language}:{program}"),
                        created.cwd,
                        occurred_at as f64 / 1000.0
                    ],
                )?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "repl.created",
                    actor,
                    json!({
                        "repl_id": repl_id,
                        "language": language,
                        "pid": created.pid,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "repl_id": repl_id,
                        "language": language,
                        "pid": created.pid,
                        "status": "running",
                    })),
                ))
            });
        outcome.unwrap_or_else(|error| {
            let _ = self.terminal_manager.kill(&repl_id);
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("repl.create persist failed: {error}")),
            )
        })
    }

    fn handle_repl_send(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(repl_id) = string_param(&cmd, &["repl_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing repl_id"),
            );
        };
        let scope = terminal_scope_from_db(&self.db, &session_id, &repl_id);
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "repl", scope.as_deref())
        {
            return CommandResponse::err(request_id, error);
        }
        let Some(input) = string_param(&cmd, &["input", "code"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing input"),
            );
        };
        let bytes = match self.terminal_manager.send(&repl_id, &input) {
            Ok(bytes) => bytes,
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("repl.send failed: {error}")),
                )
            }
        };
        CommandResponse::ok(
            request_id,
            Some(json!({"repl_id": repl_id, "bytes": bytes, "redaction": {"input": "omitted"}})),
            None,
        )
    }

    fn handle_repl_read(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(repl_id) = string_param(&cmd, &["repl_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing repl_id"),
            );
        };
        let max_bytes = cmd
            .params
            .get("max_bytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(65_536);
        match self.terminal_manager.read(&repl_id, max_bytes) {
            Ok(read) => CommandResponse::ok(request_id, Some(terminal_read_json(read)), None),
            Err(error) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("repl.read failed: {error}")),
            ),
        }
    }

    fn handle_repl_kill(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(repl_id) = string_param(&cmd, &["repl_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing repl_id"),
            );
        };
        let scope = terminal_scope_from_db(&self.db, &session_id, &repl_id);
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "repl", scope.as_deref())
        {
            return CommandResponse::err(request_id, error);
        }
        match self.terminal_manager.kill(&repl_id) {
            Ok(exit_code) => CommandResponse::ok(
                request_id,
                Some(json!({"repl_id": repl_id, "status": "killed", "exit_code": exit_code})),
                None,
            ),
            Err(error) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("repl.kill failed: {error}")),
            ),
        }
    }

    fn handle_parse_file(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let Some(path) = string_param(&cmd, &["path", "file"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing path"),
            );
        };
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000);
        if let Err(error) = ensure_path_inside_session_workspace(&self.db, &session_id, &path) {
            return CommandResponse::err(request_id, error);
        }
        let result = match self.document_tools.parse_file(
            &request_id,
            std::path::Path::new(&path),
            timeout_ms,
        ) {
            Ok(result) => result,
            Err(error) => {
                return document_tool_error_response(request_id, "parse_file", error);
            }
        };
        self.document_tool_success_response(cmd, session_id, "document.parsed", result)
    }

    fn handle_ocr_extract_text(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let Some(path) = string_param(&cmd, &["path", "image"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing path"),
            );
        };
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(20_000);
        if let Err(error) = ensure_path_inside_session_workspace(&self.db, &session_id, &path) {
            return CommandResponse::err(request_id, error);
        }
        let result = match self.document_tools.ocr_image(
            &request_id,
            std::path::Path::new(&path),
            timeout_ms,
        ) {
            Ok(result) => result,
            Err(error) => {
                return document_tool_error_response(request_id, "ocr.extract_text", error);
            }
        };
        self.document_tool_success_response(cmd, session_id, "ocr.text_extracted", result)
    }

    fn document_tool_success_response(
        &self,
        cmd: CommandEnvelope,
        session_id: String,
        event_type: &str,
        result: Value,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    event_type,
                    actor,
                    json!({
                        "kind": result["kind"],
                        "dependency": result.get("dependency").cloned().unwrap_or(Value::Null),
                        "exit_code": result.get("exit_code").cloned(),
                        "text_bytes": result["text"].as_str().map(str::len).unwrap_or(0),
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(result),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("document tool persist failed: {error}")),
            )
        })
    }

    fn handle_mcp_bridge(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let bridge_id =
            string_param(&cmd, &["bridge_id", "id"]).unwrap_or_else(|| format!("mcp_{}", now_ms()));
        let Some(program) = string_param(&cmd, &["program", "command"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing program"),
            );
        };
        let payload = cmd
            .params
            .get("payload")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let args = string_array_param(&cmd, "args");
        let cwd = string_param(&cmd, &["cwd"]).map(std::path::PathBuf::from);
        let scope_string = cwd
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| program.clone());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "mcp", Some(&scope_string))
        {
            return CommandResponse::err(request_id, error);
        }
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000);
        let method = payload
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let result = match self.mcp_bridge.invoke(McpBridgeRequest {
            bridge_id: bridge_id.clone(),
            program: std::path::PathBuf::from(program),
            args,
            cwd,
            payload,
            timeout_ms,
        }) {
            Ok(result) => result,
            Err(error) => return mcp_bridge_error_response(request_id, error),
        };

        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "mcp.bridge_invoked",
                    actor,
                    json!({
                        "bridge_id": bridge_id,
                        "method": method,
                        "exit_code": result["exit_code"],
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(result),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("mcp.bridge persist failed: {error}")),
            )
        })
    }

    fn handle_mcp_server_start(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        if let Err(error) = ensure_session_active_outside(&self.db, &session_id) {
            return CommandResponse::err(request_id, error);
        }
        let server_id = string_param(&cmd, &["server_id", "bridge_id", "id"])
            .unwrap_or_else(|| format!("mcp_server_{}", now_ms()));
        let Some(program) = string_param(&cmd, &["program", "command"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing program"),
            );
        };
        let args = string_array_param(&cmd, "args");
        let cwd = string_param(&cmd, &["cwd"]).map(std::path::PathBuf::from);
        let scope_string = cwd
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| program.clone());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "mcp", Some(&scope_string))
        {
            return CommandResponse::err(request_id, error);
        }
        let timeout_ms = cmd
            .params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(10_000);
        let init_payload = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}});
        let init_result = match self.mcp_bridge.invoke(McpBridgeRequest {
            bridge_id: format!("{server_id}:initialize"),
            program: std::path::PathBuf::from(&program),
            args: args.clone(),
            cwd: cwd.clone(),
            payload: init_payload,
            timeout_ms,
        }) {
            Ok(result) => result,
            Err(error) => return mcp_bridge_error_response(request_id, error),
        };
        let tools_result = match self.mcp_bridge.invoke(McpBridgeRequest {
            bridge_id: format!("{server_id}:tools"),
            program: std::path::PathBuf::from(&program),
            args: args.clone(),
            cwd: cwd.clone(),
            payload: json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            timeout_ms,
        }) {
            Ok(result) => result,
            Err(error) => return mcp_bridge_error_response(request_id, error),
        };
        let tools = tools_result["response"]["result"]["tools"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let state = json!({
            "server_id": server_id,
            "program": program,
            "args": args,
            "cwd": cwd.as_ref().map(|path| path.to_string_lossy().to_string()),
            "tools": tools,
            "initialized": init_result["response"].clone(),
            "timeout_ms": timeout_ms,
        });
        self.persist_mcp_server_state(
            request_id,
            session_id,
            server_id,
            "mcp.server_started",
            state,
        )
    }

    fn handle_mcp_list_tools(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let server_id = string_param(&cmd, &["server_id", "bridge_id", "id"])
            .unwrap_or_else(|| "default".into());
        match load_mcp_server_state(&self.db, &session_id, &server_id) {
            Ok(Some(state)) => CommandResponse::ok(
                request_id,
                Some(json!({
                    "server_id": server_id,
                    "tools": state["tools"].clone(),
                })),
                None,
            ),
            Ok(None) => CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "MCP server is not started"),
            ),
            Err(error) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("mcp.list_tools failed: {error}")),
            ),
        }
    }

    fn handle_mcp_call_tool(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let server_id = string_param(&cmd, &["server_id", "bridge_id", "id"])
            .unwrap_or_else(|| "default".into());
        let Some(tool_name) = string_param(&cmd, &["tool_name", "name"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing tool_name"),
            );
        };
        let state = match load_mcp_server_state(&self.db, &session_id, &server_id) {
            Ok(Some(state)) => state,
            Ok(None) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "MCP server is not started"),
                )
            }
            Err(error) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("mcp.call_tool failed: {error}")),
                )
            }
        };
        let program = state["program"].as_str().unwrap_or("").to_string();
        let args = state["args"]
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let cwd = state["cwd"]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from);
        let scope_string = cwd
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| program.clone());
        if let Err(error) =
            require_scoped_permission_grant(&self.db, &session_id, "mcp", Some(&scope_string))
        {
            return CommandResponse::err(request_id, error);
        }
        let arguments = cmd
            .params
            .get("arguments")
            .or_else(|| cmd.params.get("args_json"))
            .cloned()
            .unwrap_or_else(|| json!({}));
        let timeout_ms = state["timeout_ms"].as_u64().unwrap_or(10_000);
        let result = match self.mcp_bridge.invoke(McpBridgeRequest {
            bridge_id: format!("{server_id}:call:{tool_name}"),
            program: std::path::PathBuf::from(program),
            args,
            cwd,
            payload: json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments}
            }),
            timeout_ms,
        }) {
            Ok(result) => result,
            Err(error) => return mcp_bridge_error_response(request_id, error),
        };
        self.mcp_lifecycle_success_response(
            request_id,
            session_id,
            server_id.clone(),
            "mcp.tool_called",
            json!({
                "server_id": server_id,
                "tool_name": tool_name,
                "response": result["response"].clone(),
            }),
        )
    }

    fn handle_mcp_server_stop(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let server_id = string_param(&cmd, &["server_id", "bridge_id", "id"])
            .unwrap_or_else(|| "default".into());
        let key = format!("mcp_server:{server_id}");
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "DELETE FROM session_state WHERE session_id = ?1 AND key = ?2",
                    params![session_id, key],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "mcp.server_stopped",
                    actor,
                    json!({"server_id": server_id}),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"server_id": server_id, "status": "stopped"})),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("mcp.server_stop failed: {error}")),
            )
        })
    }

    fn persist_mcp_server_state(
        &self,
        request_id: String,
        session_id: String,
        server_id: String,
        event_type: &str,
        state: Value,
    ) -> CommandResponse {
        let key = format!("mcp_server:{server_id}");
        let occurred_at = now_ms();
        let actor = Actor::with_id(ActorKind::Runtime, "mcp");
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO session_state (session_id, key, value, timestamp) \
                     VALUES (?1, ?2, ?3, ?4) \
                     ON CONFLICT(session_id, key) DO UPDATE SET value = excluded.value, timestamp = excluded.timestamp",
                    params![session_id, key, state.to_string(), occurred_at as f64 / 1000.0],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    event_type,
                    actor,
                    json!({
                        "server_id": server_id,
                        "tool_count": state["tools"].as_array().map(Vec::len).unwrap_or(0),
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "server_id": server_id,
                        "tools": state["tools"].clone(),
                        "status": "running",
                    })),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("mcp.server_start persist failed: {error}")),
            )
        })
    }

    fn mcp_lifecycle_success_response(
        &self,
        request_id: String,
        session_id: String,
        server_id: String,
        event_type: &str,
        result: Value,
    ) -> CommandResponse {
        let occurred_at = now_ms();
        let actor = Actor::with_id(ActorKind::Runtime, "mcp");
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    event_type,
                    actor,
                    json!({"server_id": server_id, "tool_name": result.get("tool_name").cloned()}),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(result),
                ))
            });
        outcome.unwrap_or_else(|error| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{event_type} persist failed: {error}")),
            )
        })
    }

    fn handle_blackboard_intent_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let intent_id = string_param(&cmd, &["intent_id", "id"])
            .unwrap_or_else(|| format!("intent_{}", now_ms()));
        let title = string_param(&cmd, &["title"]).unwrap_or_else(|| "Intent".into());
        let content = string_param(&cmd, &["content"]).unwrap_or_default();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) =
                        lookup_idempotent_in_tx(tx, key, BLACKBOARD_INTENT_CREATE)?
                    {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO graph_nodes \
                     (id, session_id, kind, title, content, created_by, created_at, intent_status, priority) \
                     VALUES (?1, ?2, 'intent', ?3, ?4, ?5, ?6, 'open', ?7)",
                    params![
                        intent_id,
                        session_id,
                        title,
                        content,
                        actor.id.clone().unwrap_or_else(|| "user".into()),
                        occurred_at as f64 / 1000.0,
                        cmd.params.get("priority").and_then(|v| v.as_i64()).unwrap_or(0)
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "blackboard.intent_created",
                    actor,
                    json!({"intent_id": intent_id, "status": "open", "title": title}),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"session_id": session_id, "intent_id": intent_id, "status": "open"})),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, BLACKBOARD_INTENT_CREATE, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("blackboard.intent.create failed: {e}")),
            )
        })
    }

    fn handle_blackboard_intent_transition(
        &self,
        cmd: CommandEnvelope,
        method: &'static str,
        target_status: &'static str,
        event_type: &'static str,
    ) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(intent_id) = string_param(&cmd, &["intent_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing intent_id"),
            );
        };
        let assignee = string_param(&cmd, &["agent", "agent_id"]).unwrap_or_default();
        let result = cmd.params.get("result").cloned();
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, method)? {
                        return Ok(cached);
                    }
                }
                let changed = tx.execute(
                    "UPDATE graph_nodes SET intent_status = ?1, intent_to = COALESCE(NULLIF(?2, ''), intent_to), evidence = COALESCE(?3, evidence) \
                     WHERE session_id = ?4 AND id = ?5 AND kind = 'intent'",
                    params![
                        target_status,
                        assignee,
                        result.as_ref().map(|v| v.to_string()),
                        session_id,
                        intent_id
                    ],
                )?;
                if changed == 0 {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Intent not found: {intent_id}"),
                        ),
                    ));
                }
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    event_type,
                    actor,
                    json!({"intent_id": intent_id, "status": target_status, "agent": assignee, "result": result}),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"session_id": session_id, "intent_id": intent_id, "status": target_status})),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, method, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{method} failed: {e}")),
            )
        })
    }

    fn handle_graph_query(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let kind = string_param(&cmd, &["kind"]);
        let intent_status = string_param(&cmd, &["intent_status", "status"]);
        let include_edges = cmd
            .params
            .get("include_edges")
            .and_then(|value| value.as_bool())
            .unwrap_or(true);
        let limit = cmd
            .params
            .get("limit")
            .and_then(|value| value.as_u64())
            .unwrap_or(100)
            .clamp(1, 500) as i64;
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let nodes = {
                let mut stmt = conn.prepare(
                    "SELECT id, kind, title, content, tags, created_by, created_at, \
                            superseded_by, confidence, intent_status, priority, evidence, \
                            intent_from, intent_to, contract_allowed_scope \
                     FROM graph_nodes \
                     WHERE session_id = ?1 \
                       AND (?2 IS NULL OR kind = ?2) \
                       AND (?3 IS NULL OR intent_status = ?3) \
                     ORDER BY created_at, id \
                     LIMIT ?4",
                )?;
                let rows = stmt.query_map(params![session_id, kind, intent_status, limit], |row| {
                    Ok(json!({
                        "id": row.get::<_, String>(0)?,
                        "kind": row.get::<_, String>(1)?,
                        "title": row.get::<_, String>(2)?,
                        "content": row.get::<_, String>(3)?,
                        "tags": serde_json::from_str::<Value>(&row.get::<_, String>(4)?).unwrap_or_else(|_| json!([])),
                        "created_by": row.get::<_, String>(5)?,
                        "created_at": row.get::<_, f64>(6)?,
                        "superseded_by": row.get::<_, Option<String>>(7)?,
                        "confidence": row.get::<_, Option<String>>(8)?,
                        "intent_status": row.get::<_, Option<String>>(9)?,
                        "priority": row.get::<_, Option<i64>>(10)?,
                        "evidence": row.get::<_, Option<String>>(11)?,
                        "intent_from": row.get::<_, Option<String>>(12)?,
                        "intent_to": row.get::<_, Option<String>>(13)?,
                        "contract_allowed_scope": row.get::<_, Option<String>>(14)?,
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            };
            let edges = if include_edges {
                let mut stmt = conn.prepare(
                    "SELECT id, from_node_id, to_node_id, edge_type, created_at, created_by, metadata \
                     FROM graph_edges \
                     WHERE session_id = ?1 \
                     ORDER BY created_at, id \
                     LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![session_id, limit], |row| {
                    let metadata: Option<String> = row.get(6)?;
                    Ok(json!({
                        "id": row.get::<_, String>(0)?,
                        "from_node_id": row.get::<_, String>(1)?,
                        "to_node_id": row.get::<_, String>(2)?,
                        "edge_type": row.get::<_, String>(3)?,
                        "created_at": row.get::<_, f64>(4)?,
                        "created_by": row.get::<_, String>(5)?,
                        "metadata": metadata.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()).unwrap_or_else(|| json!({})),
                    }))
                })?;
                rows.collect::<Result<Vec<Value>>>()?
            } else {
                Vec::new()
            };
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({
                    "session_id": session_id,
                    "nodes": nodes,
                    "edges": edges,
                })),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("graph.query failed: {e}")),
            )
        })
    }

    fn handle_assumption_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let assumption_id = string_param(&cmd, &["assumption_id", "id"])
            .unwrap_or_else(|| format!("assumption_{}", now_ms()));
        let title = string_param(&cmd, &["title"]).unwrap_or_else(|| "Untitled assumption".into());
        let content = string_param(&cmd, &["content"]);
        let verification_type =
            string_param(&cmd, &["verification_type"]).unwrap_or_else(|| "manual".into());
        let verification_target =
            string_param(&cmd, &["verification_target", "target"]).unwrap_or_else(|| title.clone());
        let verification_expected = string_param(&cmd, &["verification_expected", "expected"])
            .unwrap_or_else(|| "true".into());
        let dependents = cmd
            .params
            .get("dependents")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let evidence = cmd.params.get("evidence").cloned();
        let now = now_ms();
        let actor = cmd.actor.clone();
        let actor_label = actor.to_string();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO assumptions \
                     (id, title, content, status, verification_type, verification_target, verification_expected, dependents, created_by, created_at, evidence, session_id) \
                     VALUES (?1, ?2, ?3, 'unverified', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        assumption_id,
                        title,
                        content,
                        verification_type,
                        verification_target,
                        verification_expected,
                        dependents.to_string(),
                        actor_label,
                        now as f64 / 1000.0,
                        evidence.map(|value| value.to_string()),
                        session_id,
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "assumption.created",
                    actor,
                    json!({"assumption_id": assumption_id, "title": title, "status": "unverified"}),
                    now,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "assumption_id": assumption_id,
                        "session_id": session_id,
                        "status": "unverified",
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("assumption.create failed: {e}")),
            )
        })
    }

    fn handle_assumption_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let status_filter = string_param(&cmd, &["status"]);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let mut assumptions = Vec::new();
            if let Some(status) = status_filter {
                let mut stmt = conn.prepare(
                    "SELECT id, title, content, status, verification_type, verification_target, verification_expected, verification_actual, dependents, created_by, created_at, verified_at, falsified_at, evidence \
                     FROM assumptions WHERE session_id = ?1 AND status = ?2 ORDER BY created_at, id",
                )?;
                let rows = stmt.query_map(params![session_id, status], assumption_row_json)?;
                for row in rows {
                    assumptions.push(row?);
                }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT id, title, content, status, verification_type, verification_target, verification_expected, verification_actual, dependents, created_by, created_at, verified_at, falsified_at, evidence \
                     FROM assumptions WHERE session_id = ?1 ORDER BY created_at, id",
                )?;
                let rows = stmt.query_map(params![session_id], assumption_row_json)?;
                for row in rows {
                    assumptions.push(row?);
                }
            }
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({"assumptions": assumptions})),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("assumption.list failed: {e}")),
            )
        })
    }

    fn handle_assumption_resolve(&self, cmd: CommandEnvelope, verified: bool) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(assumption_id) = string_param(&cmd, &["assumption_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing assumption_id"),
            );
        };
        let actual = string_param(&cmd, &["verification_actual", "actual"]);
        let evidence = cmd.params.get("evidence").cloned();
        let status = if verified { "verified" } else { "falsified" };
        let event_type = if verified {
            "assumption.verified"
        } else {
            "assumption.falsified"
        };
        let now = now_ms();
        let actor = cmd.actor.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let updated = if verified {
                    tx.execute(
                        "UPDATE assumptions SET status = 'verified', verification_actual = ?1, verified_at = ?2, evidence = ?3 \
                         WHERE id = ?4 AND session_id = ?5",
                        params![
                            actual,
                            now as f64 / 1000.0,
                            evidence.map(|value| value.to_string()),
                            assumption_id,
                            session_id
                        ],
                    )?
                } else {
                    tx.execute(
                        "UPDATE assumptions SET status = 'falsified', verification_actual = ?1, falsified_at = ?2, evidence = ?3 \
                         WHERE id = ?4 AND session_id = ?5",
                        params![
                            actual,
                            now as f64 / 1000.0,
                            evidence.map(|value| value.to_string()),
                            assumption_id,
                            session_id
                        ],
                    )?
                };
                if updated == 0 {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Assumption not found: {assumption_id}"),
                        ),
                    ));
                }
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    event_type,
                    actor,
                    json!({"assumption_id": assumption_id, "status": status}),
                    now,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "assumption_id": assumption_id,
                        "session_id": session_id,
                        "status": status,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("{event_type} failed: {e}")),
            )
        })
    }

    fn handle_team_send(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let message_id = string_param(&cmd, &["message_id", "id"])
            .unwrap_or_else(|| format!("team_msg_{}", now_ms()));
        let from_team = string_param(&cmd, &["from_team", "from"]).unwrap_or_else(|| "core".into());
        let to_team = string_param(&cmd, &["to_team"]).unwrap_or_else(|| "default".into());
        let content = string_param(&cmd, &["content"]).unwrap_or_default();
        let urgency = string_param(&cmd, &["urgency"]).unwrap_or_else(|| "normal".into());
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TEAM_SEND)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO team_messages \
                     (id, from_team, from_member, to_team, to_member, content, urgency, kind, request_id, session_id, timestamp, metadata) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'normal', ?8, ?9, ?10, ?11)",
                    params![
                        message_id,
                        from_team,
                        cmd.params.get("from_member").and_then(|v| v.as_str()),
                        to_team,
                        cmd.params.get("to_member").and_then(|v| v.as_str()),
                        content,
                        urgency,
                        request_id,
                        session_id,
                        occurred_at as f64 / 1000.0,
                        json!({"status": "delivered"}).to_string()
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "team.message_sent",
                    actor,
                    json!({"message_id": message_id, "from_team": from_team, "to_team": to_team, "status": "delivered", "urgency": urgency}),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"session_id": session_id, "message_id": message_id, "status": "delivered"})),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TEAM_SEND, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("team.send failed: {e}")),
            )
        })
    }

    fn handle_team_poll(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let to_team = string_param(&cmd, &["to_team", "team"]).unwrap_or_else(|| "default".into());
        let member = string_param(&cmd, &["to_member", "member", "agent_id"])
            .unwrap_or_else(|| to_team.clone());
        let limit = cmd
            .params
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(32)
            .clamp(1, 256) as usize;
        let mark_read = cmd
            .params
            .get("mark_read")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let mut stmt = tx.prepare(
                    "SELECT id, from_team, from_member, to_team, to_member, content, urgency, kind, timestamp, read_by \
                     FROM team_messages \
                     WHERE session_id = ?1 AND to_team = ?2 AND (to_member IS NULL OR to_member = ?3) \
                     ORDER BY \
                        CASE urgency WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END, \
                        timestamp, id",
                )?;
                let rows = stmt.query_map(params![session_id, to_team, member], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, f64>(8)?,
                        row.get::<_, String>(9)?,
                    ))
                })?;
                let candidates = rows.collect::<Result<Vec<_>>>()?;
                drop(stmt);

                let mut messages = Vec::new();
                let mut delivered_ids = Vec::new();
                for (
                    id,
                    from_team,
                    from_member,
                    routed_team,
                    routed_member,
                    content,
                    urgency,
                    kind,
                    timestamp,
                    read_by_raw,
                ) in candidates
                {
                    let mut read_by: Vec<String> =
                        serde_json::from_str(&read_by_raw).unwrap_or_default();
                    if read_by.iter().any(|reader| reader == &member) {
                        continue;
                    }
                    messages.push(json!({
                        "message_id": id,
                        "from_team": from_team,
                        "from_member": from_member,
                        "to_team": routed_team,
                        "to_member": routed_member,
                        "content": content,
                        "urgency": urgency,
                        "kind": kind,
                        "timestamp": timestamp,
                    }));
                    if mark_read {
                        read_by.push(member.clone());
                        tx.execute(
                            "UPDATE team_messages SET read_by = ?1, metadata = ?2 WHERE session_id = ?3 AND id = ?4",
                            params![
                                serde_json::to_string(&read_by).unwrap_or_else(|_| "[]".into()),
                                json!({"status": "read", "delivered_to": member}).to_string(),
                                session_id,
                                id
                            ],
                        )?;
                    }
                    delivered_ids.push(id);
                    if messages.len() >= limit {
                        break;
                    }
                }

                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "team.mailbox_polled",
                    actor,
                    json!({
                        "to_team": to_team,
                        "member": member,
                        "count": messages.len(),
                        "message_ids": delivered_ids,
                        "mark_read": mark_read,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "to_team": to_team,
                        "member": member,
                        "messages": messages,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("team.poll failed: {e}")),
            )
        })
    }

    fn handle_team_mark_read(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(message_id) = string_param(&cmd, &["message_id", "id"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing message_id"),
            );
        };
        let reader = string_param(&cmd, &["reader", "member"]).unwrap_or_else(|| "reader".into());
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, TEAM_MARK_READ)? {
                        return Ok(cached);
                    }
                }
                let read_by_json: Option<String> = tx
                    .query_row(
                        "SELECT read_by FROM team_messages WHERE session_id = ?1 AND id = ?2",
                        params![session_id, message_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(read_by_json) = read_by_json else {
                    return Ok(CommandResponse::err(
                        request_id.clone(),
                        CoreError::new(
                            ErrorCode::InvalidTransition,
                            format!("Team message not found: {message_id}"),
                        ),
                    ));
                };
                let mut read_by: Vec<String> =
                    serde_json::from_str(&read_by_json).unwrap_or_default();
                if !read_by.iter().any(|value| value == &reader) {
                    read_by.push(reader.clone());
                }
                tx.execute(
                    "UPDATE team_messages SET read_by = ?1, metadata = ?2 WHERE session_id = ?3 AND id = ?4",
                    params![
                        serde_json::to_string(&read_by).unwrap_or_else(|_| "[]".into()),
                        json!({"status": "read"}).to_string(),
                        session_id,
                        message_id
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "team.message_read",
                    actor,
                    json!({"message_id": message_id, "reader": reader, "status": "read"}),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({"session_id": session_id, "message_id": message_id, "status": "read", "read_by": read_by})),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, TEAM_MARK_READ, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("team.mark_read failed: {e}")),
            )
        })
    }

    fn handle_bus_publish(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let from = string_param(&cmd, &["from"]).unwrap_or_else(|| "core".into());
        let to = string_param(&cmd, &["to"]).unwrap_or_else(|| "default".into());
        let priority = match string_param(&cmd, &["priority"])
            .as_deref()
            .map(parse_bus_priority)
            .transpose()
        {
            Ok(priority) => priority.unwrap_or(Priority::P2),
            Err(message) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, message),
                );
            }
        };
        let content = match cmd.params.get("content") {
            Some(Value::String(content)) => content.as_bytes().to_vec(),
            Some(value) => value.to_string().into_bytes(),
            None => Vec::new(),
        };
        let message = BusMessage {
            from,
            to,
            priority,
            content,
        };
        let mut bus = self.message_bus.lock().unwrap();
        match bus.publish(message) {
            Ok(()) => CommandResponse::ok(
                request_id,
                Some(json!({"status": "queued", "queued_count": bus.len()})),
                None,
            ),
            Err(dead) => CommandResponse::ok(
                request_id,
                Some(json!({
                    "status": "dead_lettered",
                    "reason": dead.reason,
                    "dead_letter_count": bus.dead_letters().len(),
                })),
                None,
            ),
        }
    }

    fn handle_bus_pop(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let mut bus = self.message_bus.lock().unwrap();
        let message = bus.pop_next().map(bus_message_json);
        CommandResponse::ok(
            request_id,
            Some(json!({
                "message": message,
                "queued_count": bus.len(),
            })),
            None,
        )
    }

    fn handle_bus_dead_letters(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let bus = self.message_bus.lock().unwrap();
        let dead_letters = bus
            .dead_letters()
            .iter()
            .map(|dead| {
                json!({
                    "reason": dead.reason,
                    "message": bus_message_json(dead.message.clone()),
                })
            })
            .collect::<Vec<_>>();
        CommandResponse::ok(
            request_id,
            Some(json!({
                "dead_letters": dead_letters,
                "dead_letter_count": bus.dead_letters().len(),
            })),
            None,
        )
    }

    fn handle_schedule_create(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let schedule_id = string_param(&cmd, &["schedule_id", "id"])
            .unwrap_or_else(|| format!("schedule_{}", now_ms()));
        let cron = string_param(&cmd, &["cron"]).unwrap_or_else(|| "@daily".into());
        let prompt = string_param(&cmd, &["prompt"]).unwrap_or_default();
        let task_type = string_param(&cmd, &["task_type"]).unwrap_or_else(|| "prompt".into());
        let intensity = string_param(&cmd, &["intensity"]).unwrap_or_else(|| "normal".into());
        let audience = string_param(&cmd, &["audience"]).unwrap_or_else(|| "personal".into());
        let workflow_id = string_param(&cmd, &["workflow_id"]);
        let workflow_input = cmd.params.get("workflow_input").map(Value::to_string);
        let recurring = cmd
            .params
            .get("recurring")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let durable = cmd
            .params
            .get("durable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let now = now_ms();
        let now_u64 = u64::try_from(now).unwrap_or_default();
        let next_run_at = cmd
            .params
            .get("next_run_at")
            .and_then(Value::as_f64)
            .or_else(|| Scheduler::next_run_after(&cron, now_u64).map(|ts| ts as f64 / 1000.0));
        let actor = cmd.actor.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO scheduled_tasks \
                     (id, session_id, cron, prompt, task_type, intensity, audience, workflow_id, workflow_input, recurring, durable, enabled, next_run_at, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 1, ?12, ?13)",
                    params![
                        schedule_id,
                        session_id,
                        cron,
                        prompt,
                        task_type,
                        intensity,
                        audience,
                        workflow_id,
                        workflow_input,
                        if recurring { 1 } else { 0 },
                        if durable { 1 } else { 0 },
                        next_run_at,
                        now as f64 / 1000.0,
                    ],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "schedule.created",
                    actor,
                    json!({
                        "schedule_id": schedule_id,
                        "cron": cron,
                        "task_type": task_type,
                        "next_run_at": next_run_at,
                    }),
                    now,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "schedule_id": schedule_id,
                        "session_id": session_id,
                        "cron": cron,
                        "next_run_at": next_run_at,
                        "enabled": true,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("schedule.create failed: {e}")),
            )
        })
    }

    fn handle_schedule_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_filter = session_id_from_cmd(&cmd);
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let mut schedules = Vec::new();
            if let Some(session_id) = session_filter {
                let mut stmt = conn.prepare(
                    "SELECT id, session_id, cron, prompt, task_type, intensity, audience, workflow_id, workflow_input, last_execution_id, last_error, recurring, durable, enabled, last_run_at, next_run_at, created_at \
                     FROM scheduled_tasks WHERE session_id = ?1 ORDER BY created_at, id",
                )?;
                let rows = stmt.query_map(params![session_id], schedule_row_json)?;
                for row in rows {
                    schedules.push(row?);
                }
            } else {
                let mut stmt = conn.prepare(
                    "SELECT id, session_id, cron, prompt, task_type, intensity, audience, workflow_id, workflow_input, last_execution_id, last_error, recurring, durable, enabled, last_run_at, next_run_at, created_at \
                     FROM scheduled_tasks ORDER BY created_at, id",
                )?;
                let rows = stmt.query_map([], schedule_row_json)?;
                for row in rows {
                    schedules.push(row?);
                }
            }
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({"schedules": schedules})),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("schedule.list failed: {e}")),
            )
        })
    }

    fn handle_schedule_fire(&self, cmd: CommandEnvelope, due_only: bool) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let schedule_id = string_param(&cmd, &["schedule_id", "id"]);
        if !due_only && schedule_id.is_none() {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing schedule_id"),
            );
        }
        let session_filter = session_id_from_cmd(&cmd);
        let now = now_ms();
        let now_u64 = u64::try_from(now).unwrap_or_default();
        let actor = cmd.actor.clone();

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                let schedules = select_schedules_to_fire(
                    tx,
                    schedule_id.as_deref(),
                    session_filter.as_deref(),
                    due_only,
                    now as f64 / 1000.0,
                )?;
                let mut fired = Vec::new();
                let mut events = Vec::new();
                for (index, schedule) in schedules.into_iter().enumerate() {
                    ensure_session_active(tx, &schedule.session_id, &request_id)?;
                    let task_id = format!("schedule_{}_{}_{}", schedule.id, now, index);
                    tx.execute(
                        "INSERT INTO tasks \
                         (id, session_id, subject, description, status, run_generation, agent_type, assigned_agent, origin, task_type, created_at, updated_at) \
                         VALUES (?1, ?2, ?3, ?4, 'dispatchable', 0, 'scheduled', '', ?5, 'scheduled', ?6, ?6)",
                        params![
                            task_id,
                            schedule.session_id,
                            format!("Scheduled: {}", schedule.prompt),
                            schedule.prompt,
                            format!("schedule:{}", schedule.id),
                            now as f64 / 1000.0,
                        ],
                    )?;
                    let next_run_at = if schedule.recurring {
                        Scheduler::next_run_after(&schedule.cron, now_u64)
                            .map(|ts| ts as f64 / 1000.0)
                    } else {
                        None
                    };
                    tx.execute(
                        "UPDATE scheduled_tasks SET last_run_at = ?1, next_run_at = ?2, last_execution_id = ?3, last_error = NULL, enabled = ?4 WHERE id = ?5",
                        params![
                            now as f64 / 1000.0,
                            next_run_at,
                            task_id,
                            if schedule.recurring || next_run_at.is_some() { 1 } else { 0 },
                            schedule.id,
                        ],
                    )?;
                    let generation = get_current_generation(tx, &Some(schedule.session_id.clone()))?;
                    events.push(simple_event(
                        tx,
                        &schedule.session_id,
                        generation,
                        "schedule.fired",
                        actor.clone(),
                        json!({
                            "schedule_id": schedule.id,
                            "task_id": task_id,
                            "next_run_at": next_run_at,
                        }),
                        now,
                        &request_id,
                    )?);
                    fired.push(json!({
                        "schedule_id": schedule.id,
                        "session_id": schedule.session_id,
                        "task_id": task_id,
                        "next_run_at": next_run_at,
                    }));
                }
                Ok(response_with_events(
                    request_id.clone(),
                    events,
                    json!({"fired": fired, "fired_count": fired.len()}),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("schedule.fire failed: {e}")),
            )
        })
    }

    fn handle_memory_upsert(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(path) = string_param(&cmd, &["path"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing path"),
            );
        };
        let Some(body) = string_param(&cmd, &["body", "content"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing body"),
            );
        };
        let memory_id =
            string_param(&cmd, &["memory_id", "id"]).unwrap_or_else(|| format!("mem_{}", now_ms()));
        let scope = string_param(&cmd, &["scope"]).unwrap_or_else(|| "workspace".into());
        let scope_id = string_param(&cmd, &["scope_id"]).unwrap_or_else(|| session_id.clone());
        let memory_type = string_param(&cmd, &["type", "kind"]).unwrap_or_else(|| "note".into());
        let fingerprint =
            string_param(&cmd, &["fingerprint"]).unwrap_or_else(|| stable_fingerprint(&body));
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, MEMORY_UPSERT)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO memory_entry \
                     (id, path, scope, scope_id, type, body, fingerprint, last_indexed_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
                     ON CONFLICT(path) DO UPDATE SET \
                     id = excluded.id, scope = excluded.scope, scope_id = excluded.scope_id, \
                     type = excluded.type, body = excluded.body, fingerprint = excluded.fingerprint, \
                     last_indexed_at = excluded.last_indexed_at",
                    params![
                        memory_id,
                        path,
                        scope,
                        scope_id,
                        memory_type,
                        body,
                        fingerprint,
                        occurred_at
                    ],
                )?;
                tx.execute("DELETE FROM memory_fts WHERE path = ?1", params![path])?;
                tx.execute(
                    "INSERT INTO memory_fts (path, scope, scope_id, type, body) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![path, scope, scope_id, memory_type, body],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "memory.entry_upserted",
                    actor,
                    json!({
                        "memory_id": memory_id,
                        "path": path,
                        "scope": scope,
                        "scope_id": scope_id,
                        "type": memory_type,
                        "fingerprint": fingerprint,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "memory_id": memory_id,
                        "path": path,
                        "scope": scope,
                        "scope_id": scope_id,
                        "type": memory_type,
                        "fingerprint": fingerprint,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, MEMORY_UPSERT, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("memory.upsert failed: {e}")),
            )
        })
    }

    fn handle_memory_search(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(query) = string_param(&cmd, &["query", "q"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing query"),
            );
        };
        let scope = string_param(&cmd, &["scope"]);
        let scope_id = string_param(&cmd, &["scope_id"]);
        let memory_type = string_param(&cmd, &["type", "kind"]);
        let limit = cmd
            .params
            .get("limit")
            .and_then(|value| value.as_u64())
            .unwrap_or(20)
            .clamp(1, 100) as i64;
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let fts_query = fts_query_from_text(&query);
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.path, e.scope, e.scope_id, e.type, e.body, e.fingerprint, \
                            e.last_indexed_at, bm25(memory_fts) AS rank \
                     FROM memory_fts \
                     JOIN memory_entry e ON e.path = memory_fts.path \
                     WHERE memory_fts MATCH ?1 \
                       AND (?2 IS NULL OR e.scope = ?2) \
                       AND (?3 IS NULL OR e.scope_id = ?3) \
                       AND (?4 IS NULL OR e.type = ?4) \
                     ORDER BY rank ASC, e.last_indexed_at DESC \
                     LIMIT ?5",
                )?;
                let rows = stmt.query_map(
                    params![fts_query, scope, scope_id, memory_type, limit],
                    |row| {
                        Ok(json!({
                            "memory_id": row.get::<_, String>(0)?,
                            "path": row.get::<_, String>(1)?,
                            "scope": row.get::<_, String>(2)?,
                            "scope_id": row.get::<_, String>(3)?,
                            "type": row.get::<_, String>(4)?,
                            "body": row.get::<_, String>(5)?,
                            "fingerprint": row.get::<_, String>(6)?,
                            "last_indexed_at": row.get::<_, i64>(7)?,
                            "rank": row.get::<_, f64>(8)?,
                        }))
                    },
                )?;
                let results = rows.collect::<Result<Vec<Value>>>()?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "memory.search_completed",
                    actor,
                    json!({
                        "query_terms": fts_query,
                        "result_count": results.len(),
                        "scope": scope,
                        "scope_id": scope_id,
                        "type": memory_type,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "query": query,
                        "results": results,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("memory.search failed: {e}")),
            )
        })
    }

    fn handle_embedding_upsert(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let Some(path) = string_param(&cmd, &["path"]) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing path"),
            );
        };
        let model = string_param(&cmd, &["model"]).unwrap_or_else(|| "default".into());
        let Some(values) = embedding_param(&cmd, "embedding") else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing embedding"),
            );
        };
        if values.is_empty() {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Embedding must not be empty"),
            );
        }
        let dimensions = values.len() as i64;
        let encoded = encode_embedding_f32(&values);
        let idempotency_key = cmd.idempotency_key.clone();
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                if let Some(ref key) = idempotency_key {
                    if let Some(cached) = lookup_idempotent_in_tx(tx, key, EMBEDDING_UPSERT)? {
                        return Ok(cached);
                    }
                }
                ensure_session_active(tx, &session_id, &request_id)?;
                tx.execute(
                    "INSERT INTO memory_embedding (path, embedding, model, dimensions, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(path) DO UPDATE SET \
                     embedding = excluded.embedding, model = excluded.model, \
                     dimensions = excluded.dimensions, created_at = excluded.created_at",
                    params![path, encoded, model, dimensions, occurred_at],
                )?;
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "memory.embedding_upserted",
                    actor,
                    json!({
                        "path": path,
                        "model": model,
                        "dimensions": dimensions,
                    }),
                    occurred_at,
                    &request_id,
                )?;
                let response = CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "path": path,
                        "model": model,
                        "dimensions": dimensions,
                    })),
                );
                if let Some(ref key) = idempotency_key {
                    cache_idempotent_in_tx(tx, key, EMBEDDING_UPSERT, &response)?;
                }
                Ok(response)
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("embedding.upsert failed: {e}")),
            )
        })
    }

    fn handle_embedding_search(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let model = string_param(&cmd, &["model"]);
        let Some(query_embedding) = embedding_param(&cmd, "embedding") else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing embedding"),
            );
        };
        if query_embedding.is_empty() {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Embedding must not be empty"),
            );
        }
        let limit = cmd
            .params
            .get("limit")
            .and_then(|value| value.as_u64())
            .unwrap_or(20)
            .clamp(1, 100) as usize;
        let occurred_at = now_ms();
        let actor = cmd.actor.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                ensure_session_active(tx, &session_id, &request_id)?;
                let mut stmt = tx.prepare(
                    "SELECT path, embedding, model, dimensions, created_at \
                     FROM memory_embedding \
                     WHERE (?1 IS NULL OR model = ?1)",
                )?;
                let rows = stmt.query_map(params![model], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                })?;
                let mut scored = Vec::new();
                for row in rows {
                    let (path, blob, row_model, dimensions, created_at) = row?;
                    let stored = decode_embedding_f32(&blob);
                    if stored.len() != query_embedding.len() {
                        continue;
                    }
                    if let Some(score) = cosine_similarity(&query_embedding, &stored) {
                        scored.push(json!({
                            "path": path,
                            "model": row_model,
                            "dimensions": dimensions,
                            "created_at": created_at,
                            "score": score,
                        }));
                    }
                }
                scored.sort_by(|left, right| {
                    right["score"]
                        .as_f64()
                        .partial_cmp(&left["score"].as_f64())
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                scored.truncate(limit);
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = simple_event(
                    tx,
                    &session_id,
                    generation,
                    "memory.embedding_search_completed",
                    actor,
                    json!({
                        "model": model,
                        "dimensions": query_embedding.len(),
                        "result_count": scored.len(),
                    }),
                    occurred_at,
                    &request_id,
                )?;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "model": model,
                        "dimensions": query_embedding.len(),
                        "results": scored,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("embedding.search failed: {e}")),
            )
        })
    }

    fn handle_session_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let mut stmt = conn.prepare(
                "SELECT s.id, s.status, COALESCE(m.current_generation, 1), COALESCE(m.last_seq, 0), s.workspace \
                 FROM sessions s \
                 LEFT JOIN event_log_meta m ON m.session_id = s.id \
                 ORDER BY s.created_at, s.id",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(json!({
                    "session_id": row.get::<_, String>(0)?,
                    "status": row.get::<_, String>(1)?,
                    "generation": row.get::<_, i64>(2)?,
                    "last_seq": row.get::<_, i64>(3)?,
                    "workspace": row.get::<_, String>(4)?,
                }))
            })?;
            let sessions = rows.collect::<Result<Vec<Value>>>()?;
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({"sessions": sessions})),
                None,
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("session.list failed: {e}")),
            )
        })
    }

    fn handle_task_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let mut stmt = conn.prepare(
                "SELECT id, subject, status, exit_reason, run_generation, assigned_agent, blocked_by \
                 FROM tasks WHERE session_id = ?1 ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map(params![session_id], |row| {
                let blocked_by_raw: Option<String> = row.get(6)?;
                Ok(json!({
                    "task_id": row.get::<_, String>(0)?,
                    "subject": row.get::<_, String>(1)?,
                    "status": row.get::<_, String>(2)?,
                    "exit_reason": row.get::<_, Option<String>>(3)?,
                    "run_generation": row.get::<_, i64>(4)?,
                    "assigned_agent": row.get::<_, String>(5)?,
                    "blocked_by": blocked_by_raw
                        .as_deref()
                        .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
                }))
            })?;
            let tasks = rows.collect::<Result<Vec<Value>>>()?;
            let latest_seq: Seq = conn
                .query_row(
                    "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
                    params![session_id],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({"session_id": session_id, "tasks": tasks})),
                Some(latest_seq),
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("task.list failed: {e}")),
            )
        })
    }

    fn handle_conv_list(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let Some(session_id) = session_id_from_cmd(&cmd) else {
            return CommandResponse::err(
                request_id,
                CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
            );
        };
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> = (|| {
            let conn = self.db.conn();
            let mut stmt = conn.prepare(
                "SELECT id, role, content, tool_calls, tool_call_id, timestamp \
                 FROM leader_conversation WHERE session_id = ?1 ORDER BY id",
            )?;
            let rows = stmt.query_map(params![session_id], |row| {
                let tool_calls: Option<String> = row.get(3)?;
                Ok(json!({
                    "id": row.get::<_, i64>(0)?,
                    "role": row.get::<_, String>(1)?,
                    "content": row.get::<_, String>(2)?,
                    "tool_calls": tool_calls
                        .as_deref()
                        .and_then(|value| serde_json::from_str::<Value>(value).ok()),
                    "tool_call_id": row.get::<_, Option<String>>(4)?,
                    "timestamp": row.get::<_, f64>(5)?,
                }))
            })?;
            let messages = rows.collect::<Result<Vec<Value>>>()?;
            let active_context = conn
                .query_row(
                    "SELECT value FROM session_state \
                     WHERE session_id = ?1 AND key = 'active_context_projection'",
                    params![session_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .and_then(|value| serde_json::from_str::<Value>(&value).ok());
            let latest_seq: Seq = conn
                .query_row(
                    "SELECT last_seq FROM event_log_meta WHERE session_id = ?1",
                    params![session_id],
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or(0);
            Ok(CommandResponse::ok(
                request_id.clone(),
                Some(json!({
                    "session_id": session_id,
                    "messages": messages,
                    "active_context": active_context,
                })),
                Some(latest_seq),
            ))
        })();
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("conv.list failed: {e}")),
            )
        })
    }

    // -----------------------------------------------------------------------
    // Session snapshot
    // -----------------------------------------------------------------------

    fn handle_session_snapshot(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };

        match self.projection.snapshot(&session_id) {
            Ok(snap) => CommandResponse::ok(
                request_id,
                Some(serde_json::to_value(&snap).unwrap_or_default()),
                Some(snap.last_seq),
            ),
            Err(e) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("snapshot failed: {e}")),
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Session connect
    // -----------------------------------------------------------------------

    fn handle_session_connect(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let last_known_seq: Seq = cmd
            .params
            .get("cursor")
            .and_then(|c| c.get("last_known_seq"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let cursor = ReplayCursor {
            session_id,
            last_known_seq,
            generation: None,
        };

        match self.projection.connect(&cursor) {
            Ok(ConnectResult::Delta { events, latest_seq }) => {
                let latest_seq_val = events.last().map(|e| e.seq).unwrap_or(latest_seq);
                CommandResponse {
                    request_id,
                    success: true,
                    result: Some(json!({ "delta_type": "events", "latest_seq": latest_seq })),
                    error: None,
                    events,
                    latest_seq: Some(latest_seq_val),
                }
            }
            Ok(ConnectResult::SnapshotRequired {
                message,
                latest_seq,
            }) => CommandResponse {
                request_id,
                success: true,
                result: Some(json!({
                    "delta_type": "snapshot_required",
                    "message": message,
                    "latest_seq": latest_seq,
                })),
                error: None,
                events: Vec::new(),
                latest_seq: Some(latest_seq),
            },
            Err(e) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("connect failed: {e}")),
            ),
        }
    }

    // -----------------------------------------------------------------------
    // Event replay
    // -----------------------------------------------------------------------

    fn handle_event_replay(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let from_seq: Seq = cmd
            .params
            .get("from_seq")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let limit: u64 = cmd
            .params
            .get("limit")
            .and_then(|v| v.as_u64())
            .unwrap_or(100);

        match self.event_log.replay(&session_id, from_seq, limit) {
            Ok(batch) => CommandResponse {
                request_id,
                success: true,
                result: Some(json!({ "has_more": batch.has_more, "count": batch.events.len() })),
                error: None,
                events: batch.events,
                latest_seq: None,
            },
            Err(e) => CommandResponse::err(
                request_id,
                CoreError::internal(format!("replay failed: {e}")),
            ),
        }
    }

    fn handle_event_compact(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let session_id = match session_id_from_cmd(&cmd) {
            Some(s) => s,
            None => {
                return CommandResponse::err(
                    request_id,
                    CoreError::new(ErrorCode::InvalidTransition, "Missing session_id"),
                );
            }
        };
        let latest_seq = match self.event_log.latest_seq(&session_id) {
            Ok(seq) => seq,
            Err(e) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("event.compact latest_seq failed: {e}")),
                )
            }
        };
        let compact_through_seq = cmd
            .params
            .get("compact_through_seq")
            .or_else(|| cmd.params.get("through_seq"))
            .and_then(Value::as_u64)
            .or_else(|| {
                cmd.params
                    .get("retain_last")
                    .and_then(Value::as_u64)
                    .map(|retain| latest_seq.saturating_sub(retain))
            })
            .unwrap_or(0);
        if compact_through_seq == 0 {
            return CommandResponse::err(
                request_id,
                CoreError::new(
                    ErrorCode::InvalidTransition,
                    "Missing compact_through_seq or retain_last",
                ),
            );
        }
        let deleted = match self.event_log.compact(&session_id, compact_through_seq) {
            Ok(deleted) => deleted,
            Err(e) => {
                return CommandResponse::err(
                    request_id,
                    CoreError::internal(format!("event.compact failed: {e}")),
                )
            }
        };
        let compacted_seq = self.event_log.compacted_seq(&session_id).unwrap_or(0);
        let occurred_at = now_ms();
        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                let generation = get_current_generation(tx, &Some(session_id.clone()))?;
                let event = append_event_in_tx(
                    tx,
                    Some(session_id.clone()),
                    generation,
                    "event_log.compacted",
                    cmd.actor.clone(),
                    json!({
                        "session_id": session_id,
                        "compacted_seq": compacted_seq,
                        "deleted_events": deleted,
                        "snapshot_required_before_seq": compacted_seq,
                    }),
                    occurred_at,
                    Some(request_id.clone()),
                    Some(request_id.clone()),
                    format!("event_log_compacted_{session_id}_{compacted_seq}_{occurred_at}"),
                )?;
                let event_seq = event.seq;
                Ok(CommandResponse::with_event(
                    request_id.clone(),
                    event,
                    Some(json!({
                        "session_id": session_id,
                        "compacted_seq": compacted_seq,
                        "deleted_events": deleted,
                        "snapshot_required_before_seq": compacted_seq,
                        "latest_seq": event_seq,
                    })),
                ))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("event.compact event write failed: {e}")),
            )
        })
    }

    fn handle_retention_sweep(&self, cmd: CommandEnvelope) -> CommandResponse {
        let request_id = cmd.request_id.clone();
        let retain_per_session = cmd
            .params
            .get("retain_per_session")
            .and_then(Value::as_i64)
            .unwrap_or(DEFAULT_RETENTION_RETAIN_PER_SESSION);
        if retain_per_session < 1 {
            return CommandResponse::err(
                request_id,
                CoreError::new(
                    ErrorCode::InvalidTransition,
                    "retain_per_session must be >= 1",
                ),
            );
        }

        let outcome: std::result::Result<CommandResponse, rusqlite::Error> =
            self.db.with_transaction(|tx| {
                let summary = retention_sweep_in_tx(tx, retain_per_session)?;
                Ok(CommandResponse::ok(request_id.clone(), Some(summary), None))
            });
        outcome.unwrap_or_else(|e| {
            CommandResponse::err(
                request_id,
                CoreError::internal(format!("retention.sweep failed: {e}")),
            )
        })
    }

    pub fn event_log(&self) -> &EventLog {
        &self.event_log
    }

    pub fn projection(&self) -> &ProjectionService {
        &self.projection
    }
}

// ---------------------------------------------------------------------------
// In-transaction dedupe helpers
// ---------------------------------------------------------------------------

fn lookup_idempotent_in_tx(
    tx: &Transaction,
    key: &str,
    method: &str,
) -> Result<Option<CommandResponse>> {
    let json_str: Option<String> = tx
        .query_row(
            "SELECT response_json FROM command_dedupe \
             WHERE idempotency_key = ?1 AND method = ?2",
            params![key, method],
            |row| row.get(0),
        )
        .optional()?;

    match json_str {
        Some(s) => serde_json::from_str(&s)
            .map(Some)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into())),
        None => Ok(None),
    }
}

fn cache_idempotent_in_tx(
    tx: &Transaction,
    key: &str,
    method: &str,
    response: &CommandResponse,
) -> Result<()> {
    prune_command_dedupe_in_tx(tx, now_ms() - COMMAND_DEDUPE_TTL_MS)?;
    let json_str = serde_json::to_string(response)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(e.into()))?;
    tx.execute(
        "INSERT OR REPLACE INTO command_dedupe \
         (idempotency_key, method, response_json, created_at) \
         VALUES (?1, ?2, ?3, ?4)",
        params![key, method, json_str, now_ms()],
    )?;
    Ok(())
}

fn prune_command_dedupe_in_tx(tx: &Transaction, older_than_ms: i64) -> Result<usize> {
    tx.execute(
        "DELETE FROM command_dedupe WHERE created_at < ?1",
        params![older_than_ms],
    )
}

fn retention_sweep_in_tx(tx: &Transaction, retain_per_session: i64) -> Result<Value> {
    let event_log = compact_event_log_by_retention_in_tx(tx, retain_per_session)?;
    let tool_calls = prune_partitioned_table_in_tx(
        tx,
        "tool_calls",
        "session_id",
        "started_at",
        retain_per_session,
    )?;
    let token_usage = prune_partitioned_table_in_tx(
        tx,
        "token_usage",
        "session_id",
        "timestamp",
        retain_per_session,
    )?;
    let llm_gateway_requests = prune_partitioned_table_in_tx(
        tx,
        "llm_gateway_requests",
        "COALESCE(session_id, '')",
        "created_at",
        retain_per_session,
    )?;
    let agent_conversation = prune_partitioned_table_in_tx(
        tx,
        "agent_conversation",
        "session_id",
        "timestamp",
        retain_per_session,
    )?;
    let leader_conversation = prune_partitioned_table_in_tx(
        tx,
        "leader_conversation",
        "session_id",
        "timestamp",
        retain_per_session,
    )?;
    let traces = prune_partitioned_table_in_tx(
        tx,
        "traces",
        "COALESCE(session_id, '')",
        "start_ts",
        retain_per_session,
    )?;
    let execution_trace_events = prune_partitioned_table_in_tx(
        tx,
        "execution_trace_events",
        "COALESCE(session_id, '')",
        "created_at",
        retain_per_session,
    )?;
    let team_messages = prune_partitioned_table_in_tx(
        tx,
        "team_messages",
        "session_id",
        "timestamp",
        retain_per_session,
    )?;
    Ok(json!({
        "retain_per_session": retain_per_session,
        "deleted": {
            "event_log": event_log,
            "tool_calls": tool_calls,
            "token_usage": token_usage,
            "llm_gateway_requests": llm_gateway_requests,
            "agent_conversation": agent_conversation,
            "leader_conversation": leader_conversation,
            "traces": traces,
            "execution_trace_events": execution_trace_events,
            "team_messages": team_messages,
        }
    }))
}

fn compact_event_log_by_retention_in_tx(
    tx: &Transaction,
    retain_per_session: i64,
) -> Result<usize> {
    let sessions: Vec<(String, i64)> = {
        let mut stmt =
            tx.prepare("SELECT session_id, MAX(seq) FROM event_log GROUP BY session_id")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect::<Result<Vec<_>>>()?
    };
    let mut deleted = 0;
    for (session_id, latest_seq) in sessions {
        let compact_through_seq = latest_seq.saturating_sub(retain_per_session);
        if compact_through_seq <= 0 {
            continue;
        }
        deleted += tx.execute(
            "DELETE FROM event_log WHERE session_id = ?1 AND seq <= ?2",
            params![session_id, compact_through_seq],
        )?;
        tx.execute(
            "INSERT INTO event_log_meta (session_id, last_seq, current_generation, compacted_seq) \
             VALUES (?1, ?2, 1, ?3) \
             ON CONFLICT(session_id) DO UPDATE SET \
             compacted_seq = MAX(event_log_meta.compacted_seq, excluded.compacted_seq), \
             last_seq = MAX(event_log_meta.last_seq, excluded.last_seq)",
            params![session_id, latest_seq, compact_through_seq],
        )?;
    }
    Ok(deleted)
}

fn prune_partitioned_table_in_tx(
    tx: &Transaction,
    table: &'static str,
    partition_expr: &'static str,
    order_expr: &'static str,
    retain_per_partition: i64,
) -> Result<usize> {
    let sql = format!(
        "DELETE FROM {table} WHERE rowid IN (\
         SELECT rowid FROM (\
           SELECT rowid, ROW_NUMBER() OVER (PARTITION BY {partition_expr} \
             ORDER BY {order_expr} DESC, rowid DESC) AS rn FROM {table}\
         ) WHERE rn > ?1\
       )"
    );
    tx.execute(&sql, params![retain_per_partition])
}

// ---------------------------------------------------------------------------
// Pre-transaction dedupe lookup (read-only; errors treated as miss)
// ---------------------------------------------------------------------------

fn lookup_idempotent_outside_tx(db: &DbOwner, key: &str, method: &str) -> Option<CommandResponse> {
    let conn = db.conn();
    let json_str: Option<String> = conn
        .query_row(
            "SELECT response_json FROM command_dedupe \
             WHERE idempotency_key = ?1 AND method = ?2",
            params![key, method],
            |row| row.get(0),
        )
        .ok();
    json_str.and_then(|s| serde_json::from_str(&s).ok())
}

fn compact_leader_context_in_tx(
    tx: &Transaction,
    session_id: &str,
    retain_last: usize,
    reason_id: Option<&str>,
    summary_override: Option<&str>,
) -> Result<Value> {
    let mut stmt = tx.prepare(
        "SELECT id, role, content FROM leader_conversation \
         WHERE session_id = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map(params![session_id], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let messages = rows.collect::<Result<Vec<_>>>()?;
    let split_at = messages.len().saturating_sub(retain_last);
    let (compacted, retained) = messages.split_at(split_at);
    let summary = compacted
        .iter()
        .map(|(_, role, content)| format!("{role}: {content}"))
        .collect::<Vec<_>>()
        .join("\n");
    let retained_message_ids: Vec<i64> = retained.iter().map(|(id, _, _)| *id).collect();
    let active_messages = retained
        .iter()
        .map(|(id, role, content)| {
            json!({
                "id": id,
                "role": role,
                "content": content,
            })
        })
        .collect::<Vec<_>>();
    let base_summary = summary_override
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&summary);
    let persisted_summary = match reason_id {
        Some(id) if base_summary.is_empty() => format!("compacted after {id}"),
        Some(id) => format!("{base_summary}\ncompacted after {id}"),
        None => base_summary.to_string(),
    };
    let projection = json!({
        "summary": persisted_summary,
        "retained_message_ids": retained_message_ids,
        "active_messages": active_messages,
        "original_message_count": messages.len(),
        "active_message_count": retained.len(),
        "original_rows_retained": true,
    });
    tx.execute(
        "UPDATE sessions SET summary = ?1 WHERE id = ?2",
        params![projection["summary"].as_str().unwrap_or(""), session_id],
    )?;
    tx.execute(
        "INSERT INTO session_state (session_id, key, value, timestamp) \
         VALUES (?1, 'active_context_projection', ?2, ?3) \
         ON CONFLICT(session_id, key) DO UPDATE SET \
         value = excluded.value, timestamp = excluded.timestamp",
        params![session_id, projection.to_string(), now_ms() as f64 / 1000.0],
    )?;
    Ok(projection)
}

fn compacted_leader_context_text(
    db: &DbOwner,
    session_id: &str,
    retain_last: usize,
) -> Result<String> {
    let conn = db.conn();
    let mut stmt = conn.prepare(
        "SELECT role, content FROM leader_conversation \
         WHERE session_id = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map(params![session_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let messages = rows.collect::<Result<Vec<_>>>()?;
    let split_at = messages.len().saturating_sub(retain_last);
    Ok(messages[..split_at]
        .iter()
        .map(|(role, content)| format!("{role}: {content}"))
        .collect::<Vec<_>>()
        .join("\n"))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_ms() -> Timestamp {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as Timestamp
}

fn preflight_native_tool_call(
    db: &DbOwner,
    registry: &crate::tool::ToolRegistry,
    session_id: &str,
    tool_name: &str,
    args: &Value,
) -> std::result::Result<(), CoreError> {
    let status: Option<String> = {
        let conn = db.conn();
        conn.query_row(
            "SELECT status FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| CoreError::internal(format!("tool preflight failed: {e}")))?
    };
    match status.as_deref().map(session_status_from_str) {
        Some(status) if !status.is_terminal() => {}
        Some(_) => {
            return Err(CoreError::new(
                ErrorCode::InvalidTransition,
                format!("Session already terminal: {session_id}"),
            ));
        }
        None => return Err(CoreError::session_not_found(session_id)),
    }
    enforce_workspace_read_boundary(db, session_id, tool_name, args)?;

    let Some(ToolPermission::RequiresGrant {
        tool_name: required_tool,
        path_scope,
    }) = registry.required_permission_for_call(tool_name, args)
    else {
        return Ok(());
    };

    let grant_scope_row: Option<Option<String>> = {
        let conn = db.conn();
        conn.query_row(
            "SELECT scope FROM permission_grants WHERE session_id = ?1 AND tool_name = ?2",
            params![session_id, required_tool],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| CoreError::internal(format!("permission grant check failed: {e}")))?
    };
    let Some(grant_scope) = grant_scope_row else {
        return Err(CoreError::permission_denied(format!(
            "Tool '{tool_name}' requires permission grant '{required_tool}'"
        )));
    };
    if let Some(grant_scope) = grant_scope {
        if !permission_scope_allows(&grant_scope, path_scope.as_deref()) {
            return Err(CoreError::permission_denied(format!(
                "Tool '{tool_name}' is outside permission scope for grant '{required_tool}'"
            )));
        }
    }
    Ok(())
}

fn enforce_workspace_read_boundary(
    db: &DbOwner,
    session_id: &str,
    tool_name: &str,
    args: &Value,
) -> std::result::Result<(), CoreError> {
    let Some(requested_scope) = read_tool_requested_scope(tool_name, args) else {
        return Ok(());
    };
    let workspace: String = db
        .conn()
        .query_row(
            "SELECT workspace FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(|error| CoreError::internal(format!("workspace lookup failed: {error}")))?;
    if workspace.trim().is_empty() {
        return Err(CoreError::permission_denied(
            "Read tools require a session workspace scope",
        ));
    }
    if !workspace_scope_allows(&workspace, &requested_scope) {
        return Err(CoreError::permission_denied(format!(
            "Tool '{tool_name}' read path is outside the session workspace"
        )));
    }
    Ok(())
}

fn read_tool_requested_scope(tool_name: &str, args: &Value) -> Option<String> {
    let key = read_tool_scope_key(tool_name)?;
    args.get(key).and_then(Value::as_str).map(str::to_string)
}

fn read_tool_scope_key(tool_name: &str) -> Option<&'static str> {
    Some(match tool_name {
        "file_read" | "list_dir" | "code_search" => "path",
        "glob" => "base_dir",
        _ => return None,
    })
}

fn workspace_scope_allows(workspace: &str, requested_scope: &str) -> bool {
    let workspace_path = std::path::Path::new(workspace);
    let requested_path = std::path::Path::new(requested_scope);
    let root = normalize_permission_path(workspace_path);
    let requested = if requested_path.is_absolute() {
        normalize_permission_path(requested_path)
    } else {
        normalize_permission_path(root.join(requested_path))
    };
    requested == root || requested.strip_prefix(root).is_ok()
}

fn workspace_scoped_tool_args(
    db: &DbOwner,
    session_id: &str,
    tool_name: &str,
    args: &Value,
) -> std::result::Result<Value, CoreError> {
    let Some(key) = read_tool_scope_key(tool_name) else {
        return Ok(args.clone());
    };
    let Some(raw_scope) = args.get(key).and_then(Value::as_str) else {
        return Ok(args.clone());
    };
    if raw_scope.chars().any(char::is_control) {
        return Err(CoreError::permission_denied(format!(
            "Tool '{tool_name}' path contains control characters"
        )));
    }
    let workspace: String = db
        .conn()
        .query_row(
            "SELECT workspace FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .map_err(|error| CoreError::internal(format!("workspace lookup failed: {error}")))?;
    if workspace.trim().is_empty() {
        return Err(CoreError::permission_denied(
            "Read tools require a session workspace scope",
        ));
    }
    let workspace_root = normalize_permission_path(&workspace);
    let requested_path = std::path::Path::new(raw_scope);
    let requested = if requested_path.is_absolute() {
        normalize_permission_path(requested_path)
    } else {
        normalize_permission_path(workspace_root.join(requested_path))
    };
    if requested != workspace_root && requested.strip_prefix(&workspace_root).is_err() {
        return Err(CoreError::permission_denied(format!(
            "Tool '{tool_name}' read path is outside the session workspace"
        )));
    }

    let mut scoped_args = args.clone();
    if let Some(object) = scoped_args.as_object_mut() {
        object.insert(
            key.to_string(),
            Value::String(requested.display().to_string()),
        );
    }
    Ok(scoped_args)
}

fn permission_scope_from_args_json(args_json: &str) -> Option<String> {
    serde_json::from_str::<Value>(args_json)
        .ok()
        .and_then(|args| {
            args.get("scope")
                .or_else(|| args.get("path"))
                .or_else(|| args.get("cwd"))
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

fn permission_scope_allows(grant_scope: &str, requested_scope: Option<&str>) -> bool {
    let Some(requested_scope) = requested_scope else {
        return true;
    };
    let grant = normalize_permission_path(grant_scope);
    let requested = normalize_permission_path(requested_scope);
    requested.strip_prefix(&grant).is_ok()
}

fn ensure_path_inside_session_workspace(
    db: &DbOwner,
    session_id: &str,
    requested_path: &str,
) -> std::result::Result<(), CoreError> {
    let workspace = db
        .conn()
        .query_row(
            "SELECT workspace FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| CoreError::internal(format!("workspace lookup failed: {error}")))?
        .ok_or_else(|| CoreError::session_not_found(session_id.to_string()))?;
    let workspace_root = normalize_permission_path(&workspace);
    let requested = normalize_permission_path(requested_path);
    if requested == workspace_root || requested.strip_prefix(&workspace_root).is_ok() {
        Ok(())
    } else {
        Err(CoreError::with_details(
            ErrorCode::PermissionDenied,
            "Path is outside the session workspace",
            json!({
                "session_id": session_id,
                "path": requested.display().to_string(),
                "workspace": workspace_root.display().to_string(),
            }),
            false,
        ))
    }
}

fn normalize_permission_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    let raw = path.as_ref();
    if let Ok(canonical) = std::fs::canonicalize(raw) {
        return canonical;
    }
    if let (Some(parent), Some(file_name)) = (raw.parent(), raw.file_name()) {
        if let Ok(canonical_parent) = std::fs::canonicalize(parent) {
            return lexical_normalize_path(canonical_parent.join(file_name));
        }
    }
    lexical_normalize_path(raw)
}

fn lexical_normalize_path(path: impl AsRef<std::path::Path>) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.as_ref().components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

fn workflow_execution_context(params: &Value) -> Value {
    let node_count = params
        .get("nodes")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let edge_count = params
        .get("edges")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    json!({
        "node_count": node_count,
        "edge_count": edge_count,
        "has_input": params.get("input").is_some(),
        "redaction": {
            "command_params": "omitted",
            "auth_context": "omitted"
        }
    })
}

fn estimated_native_file_write_bytes(tool_name: &str, args: &Value) -> Option<u64> {
    match tool_name {
        "file_write" | "file_create" => args
            .get("content")
            .and_then(Value::as_str)
            .map(|content| content.len() as u64),
        "structured_patch" => args
            .get("operations")
            .and_then(Value::as_array)
            .map(|operations| {
                operations
                    .iter()
                    .filter_map(|operation| {
                        operation
                            .get("new")
                            .or_else(|| operation.get("content"))
                            .and_then(Value::as_str)
                            .map(|content| content.len() as u64)
                    })
                    .sum()
            }),
        _ => None,
    }
}

fn has_permission_grant(db: &DbOwner, session_id: &str, tool_name: &str) -> bool {
    let conn = db.conn();
    conn.query_row(
        "SELECT COUNT(*) FROM permission_grants WHERE session_id = ?1 AND tool_name = ?2",
        params![session_id, tool_name],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .unwrap_or(false)
}

fn require_scoped_permission_grant(
    db: &DbOwner,
    session_id: &str,
    tool_name: &str,
    requested_scope: Option<&str>,
) -> std::result::Result<(), CoreError> {
    let conn = db.conn();
    let grant_scope_row: Option<Option<String>> = conn
        .query_row(
            "SELECT scope FROM permission_grants WHERE session_id = ?1 AND tool_name = ?2",
            params![session_id, tool_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| CoreError::internal(format!("permission grant check failed: {error}")))?;
    let Some(grant_scope) = grant_scope_row else {
        return Err(CoreError::permission_denied(format!(
            "requires permission grant '{tool_name}'"
        )));
    };
    if let Some(grant_scope) = grant_scope {
        if !permission_scope_allows(&grant_scope, requested_scope) {
            return Err(CoreError::permission_denied(format!(
                "requested scope is outside permission grant '{tool_name}'"
            )));
        }
    }
    Ok(())
}

fn terminal_scope_from_db(db: &DbOwner, session_id: &str, terminal_id: &str) -> Option<String> {
    db.conn()
        .query_row(
            "SELECT cwd FROM terminal_sessions WHERE session_id = ?1 AND id = ?2",
            params![session_id, terminal_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .ok()
        .flatten()
        .flatten()
}

fn repl_session_command(language: &str) -> Option<(String, Vec<String>)> {
    match language.to_ascii_lowercase().as_str() {
        "python" | "py" => runtime_command_available("python").then(|| {
            (
                "python".to_string(),
                vec!["-i".to_string(), "-q".to_string()],
            )
        }),
        "node" | "javascript" | "js" => {
            runtime_command_available("node").then(|| ("node".to_string(), vec!["-i".to_string()]))
        }
        _ => None,
    }
}

fn runtime_command_available(program: &str) -> bool {
    std::process::Command::new(program)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok()
}

fn start_agent_pool_event_bridge(
    db: DbOwner,
    runtime_manager: Arc<Mutex<RuntimeManager>>,
    rx: mpsc::Receiver<AgentEvent>,
) {
    let _ = std::thread::Builder::new()
        .name("lingxiao-agent-pool-event-bridge".into())
        .spawn(move || {
            for event in rx {
                match event {
                    AgentEvent::Started { agent_id } => {
                        persist_agent_pool_started_event(&db, &agent_id);
                    }
                    AgentEvent::Completed { agent_id, result } => {
                        persist_agent_pool_terminal_event(
                            &db,
                            &runtime_manager,
                            AgentPoolTerminalUpdate {
                                explicit_session_id: None,
                                agent_id: &agent_id,
                                event_type: "agent.completed",
                                target_status: "stopped",
                                exit_reason: "completed",
                                result: Some(result),
                            },
                        );
                    }
                    AgentEvent::Crashed { agent_id, error } => {
                        persist_agent_pool_terminal_event(
                            &db,
                            &runtime_manager,
                            AgentPoolTerminalUpdate {
                                explicit_session_id: None,
                                agent_id: &agent_id,
                                event_type: "agent.crashed",
                                target_status: "stopped",
                                exit_reason: &error,
                                result: None,
                            },
                        );
                    }
                    AgentEvent::Heartbeat { agent_id, at_ms } => {
                        persist_agent_pool_heartbeat(&db, &agent_id, at_ms);
                    }
                    AgentEvent::ToolCallInitiated {
                        agent_id,
                        tool_call,
                    } => {
                        persist_agent_pool_log(
                            &db,
                            &agent_id,
                            "agent.tool_call_initiated",
                            &json!({
                                "tool_call_id": tool_call.id,
                                "tool_name": tool_call.name,
                            })
                            .to_string(),
                        );
                    }
                    AgentEvent::ToolCallCompleted {
                        agent_id,
                        tool_call_id,
                        result,
                    } => {
                        persist_agent_pool_log(
                            &db,
                            &agent_id,
                            "agent.tool_call_completed",
                            &json!({
                                "tool_call_id": tool_call_id,
                                "result": result,
                            })
                            .to_string(),
                        );
                    }
                    AgentEvent::LlmRoundCompleted {
                        agent_id,
                        assistant_message,
                    } => {
                        persist_agent_pool_log(
                            &db,
                            &agent_id,
                            "agent.llm_round_completed",
                            &assistant_message,
                        );
                    }
                }
            }
        });
}

struct ActiveAgentRow {
    session_id: String,
    agent_name: String,
    agent_role: String,
    task_id: String,
    status: String,
}

struct AgentPoolTerminalUpdate<'a> {
    explicit_session_id: Option<&'a str>,
    agent_id: &'a str,
    event_type: &'a str,
    target_status: &'a str,
    exit_reason: &'a str,
    result: Option<Value>,
}

fn load_active_agent_row(db: &DbOwner, agent_id: &str) -> Result<Option<ActiveAgentRow>> {
    let row = {
        let conn = db.conn();
        conn.query_row(
            "SELECT session_id, agent_name, agent_role, task_id, status \
             FROM agent_state WHERE agent_id = ?1 AND stopped = 0 \
             ORDER BY timestamp DESC LIMIT 1",
            params![agent_id],
            |row| {
                Ok(ActiveAgentRow {
                    session_id: row.get(0)?,
                    agent_name: row.get(1)?,
                    agent_role: row.get(2)?,
                    task_id: row.get(3)?,
                    status: row.get(4)?,
                })
            },
        )
        .optional()?
    };
    Ok(row)
}

fn persist_agent_pool_started_event(db: &DbOwner, agent_id: &str) {
    let Ok(Some(row)) = load_active_agent_row(db, agent_id) else {
        return;
    };
    let _ = db.with_transaction(|tx| {
        let occurred_at = now_ms();
        let ts = occurred_at as f64 / 1000.0;
        tx.execute(
            "UPDATE agent_state SET status = 'running', stopped = 0, timestamp = ?1 \
             WHERE session_id = ?2 AND agent_id = ?3",
            params![ts, row.session_id, agent_id],
        )?;
        insert_agent_log(
            tx,
            &row.session_id,
            agent_id,
            &row.agent_name,
            &row.agent_role,
            &row.task_id,
            "agent.started",
            "",
            occurred_at,
        )?;
        let generation = get_current_generation(tx, &Some(row.session_id.clone()))?;
        append_event_in_tx(
            tx,
            Some(row.session_id.clone()),
            generation,
            "agent.started",
            Actor::with_id(ActorKind::Runtime, "agent-pool"),
            json!({
                "session_id": row.session_id,
                "agent_id": agent_id,
                "task_id": row.task_id,
                "from_status": row.status,
                "status": "running",
            }),
            occurred_at,
            None,
            None,
            format!(
                "agent_pool_started_{}_{}_{occurred_at}",
                row.session_id, agent_id
            ),
        )?;
        Ok(())
    });
}

fn persist_agent_pool_terminal_event(
    db: &DbOwner,
    runtime_manager: &Arc<Mutex<RuntimeManager>>,
    update: AgentPoolTerminalUpdate<'_>,
) {
    let row = if let Some(session_id) = update.explicit_session_id {
        db.conn()
            .query_row(
                "SELECT session_id, agent_name, agent_role, task_id, status \
                 FROM agent_state WHERE session_id = ?1 AND agent_id = ?2",
                params![session_id, update.agent_id],
                |row| {
                    Ok(ActiveAgentRow {
                        session_id: row.get(0)?,
                        agent_name: row.get(1)?,
                        agent_role: row.get(2)?,
                        task_id: row.get(3)?,
                        status: row.get(4)?,
                    })
                },
            )
            .optional()
            .ok()
            .flatten()
    } else {
        load_active_agent_row(db, update.agent_id).ok().flatten()
    };
    let Some(row) = row else {
        return;
    };
    let success = update.event_type == "agent.completed";
    let _ = db.with_transaction(|tx| {
        let occurred_at = now_ms();
        let ts = occurred_at as f64 / 1000.0;
        tx.execute(
            "UPDATE agent_state SET status = ?1, stopped = 1, timestamp = ?2 \
             WHERE session_id = ?3 AND agent_id = ?4",
            params![update.target_status, ts, row.session_id, update.agent_id],
        )?;
        insert_agent_log(
            tx,
            &row.session_id,
            update.agent_id,
            &row.agent_name,
            &row.agent_role,
            &row.task_id,
            update.event_type,
            update.exit_reason,
            occurred_at,
        )?;
        if !row.task_id.is_empty() {
            tx.execute(
                "UPDATE tasks SET status = ?1, result = ?2, exit_reason = ?3, updated_at = ?4 \
                 WHERE session_id = ?5 AND id = ?6",
                params![
                    if success { "completed" } else { "failed" },
                    update.result.as_ref().map(Value::to_string),
                    update.exit_reason,
                    ts,
                    row.session_id,
                    row.task_id
                ],
            )?;
        }
        let generation = get_current_generation(tx, &Some(row.session_id.clone()))?;
        append_event_in_tx(
            tx,
            Some(row.session_id.clone()),
            generation,
            update.event_type,
            Actor::with_id(ActorKind::Runtime, "agent-pool"),
            json!({
                "session_id": row.session_id,
                "agent_id": update.agent_id,
                "task_id": row.task_id,
                "from_status": row.status,
                "status": update.target_status,
                "exit_reason": update.exit_reason,
                "result": update.result,
            }),
            occurred_at,
            None,
            None,
            format!(
                "agent_pool_{}_{}_{}_{}",
                update.event_type.replace('.', "_"),
                row.session_id,
                update.agent_id,
                occurred_at
            ),
        )?;
        Ok(())
    });
    runtime_manager
        .lock()
        .unwrap()
        .release_worker(&agent_worker_token(&row.session_id, update.agent_id));
}

fn persist_agent_pool_heartbeat(db: &DbOwner, agent_id: &str, at_ms: u64) {
    let _ = db.conn().execute(
        "UPDATE agent_state SET timestamp = ?1 WHERE agent_id = ?2 AND stopped = 0",
        params![at_ms as f64 / 1000.0, agent_id],
    );
}

fn persist_agent_pool_log(db: &DbOwner, agent_id: &str, event_type: &str, content: &str) {
    let Ok(Some(row)) = load_active_agent_row(db, agent_id) else {
        return;
    };
    let _ = db.with_transaction(|tx| {
        insert_agent_log(
            tx,
            &row.session_id,
            agent_id,
            &row.agent_name,
            &row.agent_role,
            &row.task_id,
            event_type,
            content,
            now_ms(),
        )?;
        Ok(())
    });
}

fn load_task_content_outside(db: &DbOwner, session_id: &str, task_id: &str) -> Option<String> {
    if task_id.is_empty() {
        return None;
    }
    db.conn()
        .query_row(
            "SELECT subject || CASE WHEN description = '' THEN '' ELSE '\n' || description END \
             FROM tasks WHERE session_id = ?1 AND id = ?2",
            params![session_id, task_id],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten()
}

fn load_mcp_server_state(db: &DbOwner, session_id: &str, server_id: &str) -> Result<Option<Value>> {
    let key = format!("mcp_server:{server_id}");
    let raw: Option<String> = db
        .conn()
        .query_row(
            "SELECT value FROM session_state WHERE session_id = ?1 AND key = ?2",
            params![session_id, key],
            |row| row.get(0),
        )
        .optional()?;
    Ok(raw.and_then(|value| serde_json::from_str::<Value>(&value).ok()))
}

fn append_leader_conversation_outside(
    db: &DbOwner,
    session_id: &str,
    role: &str,
    content: &str,
    tool_call_id: Option<&str>,
) {
    let conn = db.conn();
    let _ = conn.execute(
        "INSERT INTO leader_conversation (session_id, role, content, tool_call_id, timestamp) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            session_id,
            role,
            content,
            tool_call_id,
            now_ms() as f64 / 1000.0,
        ],
    );
}

fn leader_tool_observation(tool_call: &ToolCall, response: &CommandResponse) -> Value {
    let result = response
        .events
        .iter()
        .rev()
        .find(|event| event.event_type == "tool.call_completed")
        .and_then(|event| event.payload.get("result"))
        .cloned()
        .or_else(|| {
            response
                .events
                .iter()
                .rev()
                .find(|event| event.event_type == "tool.call_failed")
                .and_then(|event| event.payload.get("error"))
                .cloned()
        })
        .unwrap_or_else(|| json!({"status": response.result.as_ref().and_then(|value| value.get("status")).cloned()}));
    json!({
        "tool_call_id": tool_call.id,
        "tool_name": tool_call.name,
        "success": response.success,
        "result": result,
    })
}

fn dedupe_tool_calls_by_id(tool_calls: &mut Vec<ToolCall>) {
    let mut seen = std::collections::HashSet::new();
    tool_calls.retain(|call| seen.insert(call.id.clone()));
}

fn session_status_from_str(s: &str) -> SessionStatus {
    match s {
        "created" => SessionStatus::Created,
        "active" => SessionStatus::Active,
        "interrupted" => SessionStatus::Interrupted,
        "completed" => SessionStatus::Completed,
        "failed" => SessionStatus::Failed,
        "deleted" => SessionStatus::Deleted,
        _ => SessionStatus::Failed,
    }
}

fn session_status_to_str(status: SessionStatus) -> &'static str {
    match status {
        SessionStatus::Created => "created",
        SessionStatus::Active => "active",
        SessionStatus::Interrupted => "interrupted",
        SessionStatus::Completed => "completed",
        SessionStatus::Failed => "failed",
        SessionStatus::Deleted => "deleted",
    }
}

fn session_id_from_cmd(cmd: &CommandEnvelope) -> Option<String> {
    cmd.session_id.clone().or_else(|| {
        cmd.params
            .get("session_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    })
}

fn task_ref_from_cmd(cmd: &CommandEnvelope) -> Option<(String, String)> {
    let session_id = session_id_from_cmd(cmd)?;
    let task_id = cmd
        .params
        .get("task_id")
        .or_else(|| cmd.params.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    Some((session_id, task_id))
}

fn route_agent_for_task_type(
    tx: &Transaction<'_>,
    session_id: &str,
    task_id: &str,
) -> Result<String> {
    let (agent_type, preferred): (String, Option<String>) = tx.query_row(
        "SELECT agent_type, preferred_agent_name FROM tasks WHERE session_id = ?1 AND id = ?2",
        params![session_id, task_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if let Some(preferred) = preferred.filter(|value| !value.trim().is_empty()) {
        return Ok(preferred);
    }
    let mut slug = agent_type
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while slug.contains("--") {
        slug = slug.replace("--", "-");
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() || slug == "general" {
        Ok("agent-default".into())
    } else {
        Ok(format!("{slug}-agent"))
    }
}

fn normalize_task_dependencies(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str())
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect(),
        Some(Value::String(item)) => {
            let item = item.trim();
            if item.is_empty() {
                Vec::new()
            } else {
                vec![item.to_string()]
            }
        }
        _ => Vec::new(),
    }
}

fn parse_blocked_by(raw: &str) -> Vec<String> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .map(|value| normalize_task_dependencies(Some(&value)))
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| normalize_task_dependencies(Some(&Value::String(raw.to_string()))))
}

fn unblock_dependent_tasks(
    tx: &Transaction<'_>,
    session_id: &str,
    completed_task_id: &str,
    generation: Generation,
    actor: Actor,
    occurred_at: Timestamp,
    request_id: &str,
) -> Result<Vec<EventEnvelope>> {
    let blocked: Vec<(String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT id, blocked_by FROM tasks \
             WHERE session_id = ?1 AND status = 'blocked' AND blocked_by IS NOT NULL",
        )?;
        let rows = stmt.query_map(params![session_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<Result<Vec<_>>>()?
    };

    let mut events = Vec::new();
    let ts = occurred_at as f64 / 1000.0;
    for (task_id, blocked_by_raw) in blocked {
        let mut dependencies = parse_blocked_by(&blocked_by_raw);
        let before_len = dependencies.len();
        dependencies.retain(|id| id != completed_task_id);
        if dependencies.len() == before_len {
            continue;
        }

        if dependencies.is_empty() {
            tx.execute(
                "UPDATE tasks SET status = 'dispatchable', blocked_by = NULL, blocked_reason = NULL, \
                 updated_at = ?1 WHERE id = ?2 AND session_id = ?3 AND status = 'blocked'",
                params![ts, task_id, session_id],
            )?;
            events.push(simple_event(
                tx,
                session_id,
                generation,
                "task.unblocked",
                actor.clone(),
                json!({
                    "session_id": session_id,
                    "task_id": task_id,
                    "unblocked_by": completed_task_id,
                    "status": "dispatchable",
                }),
                occurred_at,
                request_id,
            )?);
        } else {
            tx.execute(
                "UPDATE tasks SET blocked_by = ?1, updated_at = ?2 \
                 WHERE id = ?3 AND session_id = ?4 AND status = 'blocked'",
                params![json!(dependencies).to_string(), ts, task_id, session_id],
            )?;
        }
    }

    Ok(events)
}

fn tool_call_ref_from_cmd(cmd: &CommandEnvelope) -> Option<(String, String)> {
    let session_id = session_id_from_cmd(cmd)?;
    let tool_call_id = cmd
        .params
        .get("tool_call_id")
        .or_else(|| cmd.params.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    Some((session_id, tool_call_id))
}

fn agent_ref_from_cmd(cmd: &CommandEnvelope) -> Option<(String, String)> {
    let session_id = session_id_from_cmd(cmd)?;
    let agent_id = cmd
        .params
        .get("agent_id")
        .or_else(|| cmd.params.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    Some((session_id, agent_id))
}

fn workflow_ref_from_cmd(cmd: &CommandEnvelope) -> Option<(String, String)> {
    let session_id = session_id_from_cmd(cmd)?;
    let execution_id = cmd
        .params
        .get("execution_id")
        .or_else(|| cmd.params.get("id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)?;
    Some((session_id, execution_id))
}

fn workflow_node_id(node: &Value) -> Option<String> {
    node.get("id")
        .or_else(|| node.get("node_id"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}

fn workflow_node_max_attempts(node: &Value) -> i64 {
    node.get("retry")
        .and_then(|retry| retry.get("max_attempts").or_else(|| retry.get("attempts")))
        .or_else(|| node.get("max_attempts"))
        .or_else(|| node.get("attempts"))
        .and_then(|value| value.as_i64())
        .unwrap_or(1)
        .clamp(1, 10)
}

fn workflow_node_forces_retryable(node: &Value) -> bool {
    node.get("retry")
        .and_then(|retry| retry.get("retryable"))
        .or_else(|| node.get("retryable"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn workflow_node_retry_sleep(node: &Value, attempt: i64) {
    let base_ms = node
        .get("retry")
        .and_then(|retry| retry.get("backoff_ms"))
        .or_else(|| node.get("backoff_ms"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0)
        .min(250);
    if base_ms == 0 {
        return;
    }
    let multiplier = 2_u64.saturating_pow(attempt.saturating_sub(1) as u32);
    std::thread::sleep(std::time::Duration::from_millis(
        base_ms.saturating_mul(multiplier).min(1_000),
    ));
}

fn load_workflow_node_state(
    db: &DbOwner,
    execution_id: &str,
    node_id: &str,
) -> Result<Option<WorkflowNodeExecution>> {
    let conn = db.conn();
    conn.query_row(
        "SELECT node_type, status, output_json, error, attempt \
         FROM workflow_node_state WHERE execution_id = ?1 AND node_id = ?2",
        params![execution_id, node_id],
        |row| {
            let status: String = row.get(1)?;
            let output_raw: Option<String> = row.get(2)?;
            let output = output_raw
                .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
                .unwrap_or(Value::Null);
            Ok(WorkflowNodeExecution {
                node_id: node_id.to_string(),
                node_type: row.get(0)?,
                success: status == "completed",
                status,
                output,
                error: row.get(3)?,
                attempt: row.get(4)?,
                retryable: false,
            })
        },
    )
    .optional()
}

fn next_workflow_node_attempt(db: &DbOwner, execution_id: &str, node_id: &str) -> Result<i64> {
    let conn = db.conn();
    Ok(conn
        .query_row(
            "SELECT attempt FROM workflow_node_state WHERE execution_id = ?1 AND node_id = ?2",
            params![execution_id, node_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

fn persist_workflow_node_state(
    db: &DbOwner,
    execution_id: &str,
    result: &WorkflowNodeExecution,
    occurred_at: Timestamp,
) -> Result<()> {
    db.with_transaction(|tx| {
        tx.execute(
            "INSERT INTO workflow_node_state \
             (execution_id, node_id, node_type, status, output_json, error, attempt, generation, started_at, completed_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?10) \
             ON CONFLICT(execution_id, node_id) DO UPDATE SET \
             node_type = excluded.node_type, status = excluded.status, \
             output_json = excluded.output_json, error = excluded.error, \
             attempt = excluded.attempt, generation = workflow_node_state.generation + 1, \
             started_at = COALESCE(workflow_node_state.started_at, excluded.started_at), \
             completed_at = excluded.completed_at, updated_at = excluded.updated_at",
            params![
                execution_id,
                result.node_id,
                result.node_type,
                result.status,
                result.output.to_string(),
                result.error,
                result.attempt,
                occurred_at,
                if result.success { Some(occurred_at) } else { None },
                occurred_at
            ],
        )?;
        Ok(())
    })
}

fn persist_agent_task_result(
    db: &DbOwner,
    session_id: &str,
    task_id: &str,
    success: bool,
    output: &Value,
    error: Option<&str>,
) -> Result<()> {
    if task_id.is_empty() {
        return Ok(());
    }
    let conn = db.conn();
    conn.execute(
        "UPDATE tasks SET status = ?1, result = ?2, exit_reason = ?3, updated_at = ?4 \
         WHERE session_id = ?5 AND id = ?6",
        params![
            if success { "completed" } else { "failed" },
            output.to_string(),
            error,
            now_ms() as f64 / 1000.0,
            session_id,
            task_id,
        ],
    )?;
    Ok(())
}

fn load_workflow_node_states_in_tx(tx: &Transaction, execution_id: &str) -> Result<Vec<Value>> {
    let mut stmt = tx.prepare(
        "SELECT node_id, node_type, status, output_json, error, attempt, generation, started_at, completed_at, updated_at \
         FROM workflow_node_state WHERE execution_id = ?1 ORDER BY started_at, node_id",
    )?;
    let rows = stmt.query_map(params![execution_id], |row| {
        let output_raw: Option<String> = row.get(3)?;
        Ok(json!({
            "node_id": row.get::<_, String>(0)?,
            "node_type": row.get::<_, String>(1)?,
            "status": row.get::<_, String>(2)?,
            "output": output_raw.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()).unwrap_or(Value::Null),
            "error": row.get::<_, Option<String>>(4)?,
            "attempt": row.get::<_, i64>(5)?,
            "generation": row.get::<_, i64>(6)?,
            "started_at": row.get::<_, Option<Timestamp>>(7)?,
            "completed_at": row.get::<_, Option<Timestamp>>(8)?,
            "updated_at": row.get::<_, Timestamp>(9)?,
        }))
    })?;
    rows.collect::<Result<Vec<Value>>>()
}

fn count_by_column(conn: &rusqlite::Connection, table: &str, column: &str) -> Result<Value> {
    let sql = format!("SELECT {column}, COUNT(*) FROM {table} GROUP BY {column} ORDER BY {column}");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut counts = serde_json::Map::new();
    for row in rows {
        let (key, count) = row?;
        counts.insert(key, json!(count));
    }
    Ok(Value::Object(counts))
}

fn collect_metrics(conn: &rusqlite::Connection, session_id: Option<&str>) -> Result<Value> {
    let command_count = count_rows(conn, "traces", session_id)?;
    let command_errors = count_trace_status(conn, "error", session_id)?;
    let avg_command_duration_ms = avg_trace_duration_ms(conn, session_id)?;
    let operation_counts = count_trace_operations(conn, session_id)?;
    let execution_events_by_status =
        count_grouped_optional_session(conn, "execution_trace_events", "status", session_id)?;
    let execution_events_by_type =
        count_grouped_optional_session(conn, "execution_trace_events", "task_type", session_id)?;
    let tool_calls_by_status =
        count_grouped_optional_session(conn, "tool_calls", "status", session_id)?;
    let tool_calls_by_name =
        count_grouped_optional_session(conn, "tool_calls", "tool_name", session_id)?;
    let task_status = count_grouped_optional_session(conn, "tasks", "status", session_id)?;
    let agent_status = count_grouped_optional_session(conn, "agent_state", "status", session_id)?;
    let workflow_status =
        count_grouped_optional_session(conn, "workflow_executions", "status", session_id)?;
    let workflow_node_status = count_workflow_node_status(conn, session_id)?;
    let llm_status =
        count_grouped_optional_session(conn, "llm_gateway_requests", "status", session_id)?;
    let llm_tokens = sum_llm_tokens(conn, session_id)?;

    Ok(json!({
        "runtime": {
            "command_count": command_count,
            "command_errors": command_errors,
            "avg_command_duration_ms": avg_command_duration_ms,
            "operations": operation_counts,
            "execution_events_by_status": execution_events_by_status,
            "execution_events_by_type": execution_events_by_type,
        },
        "tools": {
            "by_status": tool_calls_by_status,
            "by_name": tool_calls_by_name,
        },
        "llm": {
            "by_status": llm_status,
            "total_tokens": llm_tokens,
        },
        "agents": {
            "by_status": agent_status,
        },
        "tasks": {
            "by_status": task_status,
        },
        "workflows": {
            "executions_by_status": workflow_status,
            "nodes_by_status": workflow_node_status,
        },
    }))
}

fn count_rows(conn: &rusqlite::Connection, table: &str, session_id: Option<&str>) -> Result<i64> {
    if let Some(session) = session_id {
        let sql = format!("SELECT COUNT(*) FROM {table} WHERE session_id = ?1");
        conn.query_row(&sql, params![session], |row| row.get(0))
    } else {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        conn.query_row(&sql, [], |row| row.get(0))
    }
}

fn count_trace_status(
    conn: &rusqlite::Connection,
    status: &str,
    session_id: Option<&str>,
) -> Result<i64> {
    if let Some(session) = session_id {
        conn.query_row(
            "SELECT COUNT(*) FROM traces WHERE status = ?1 AND session_id = ?2",
            params![status, session],
            |row| row.get(0),
        )
    } else {
        conn.query_row(
            "SELECT COUNT(*) FROM traces WHERE status = ?1",
            params![status],
            |row| row.get(0),
        )
    }
}

fn avg_trace_duration_ms(conn: &rusqlite::Connection, session_id: Option<&str>) -> Result<f64> {
    let value: Option<f64> = if let Some(session) = session_id {
        conn.query_row(
            "SELECT AVG(COALESCE(end_ts, start_ts) - start_ts) FROM traces WHERE session_id = ?1",
            params![session],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT AVG(COALESCE(end_ts, start_ts) - start_ts) FROM traces",
            [],
            |row| row.get(0),
        )?
    };
    Ok(value.unwrap_or(0.0))
}

fn count_trace_operations(conn: &rusqlite::Connection, session_id: Option<&str>) -> Result<Value> {
    if let Some(session) = session_id {
        count_grouped_query(
            conn,
            "SELECT operation, COUNT(*) FROM traces WHERE session_id = ?1 GROUP BY operation ORDER BY operation",
            params![session],
        )
    } else {
        count_grouped_query(
            conn,
            "SELECT operation, COUNT(*) FROM traces GROUP BY operation ORDER BY operation",
            [],
        )
    }
}

fn count_grouped_optional_session(
    conn: &rusqlite::Connection,
    table: &str,
    column: &str,
    session_id: Option<&str>,
) -> Result<Value> {
    if let Some(session) = session_id {
        let sql = format!(
            "SELECT COALESCE({column}, ''), COUNT(*) FROM {table} WHERE session_id = ?1 GROUP BY {column} ORDER BY {column}"
        );
        count_grouped_query(conn, &sql, params![session])
    } else {
        let sql = format!(
            "SELECT COALESCE({column}, ''), COUNT(*) FROM {table} GROUP BY {column} ORDER BY {column}"
        );
        count_grouped_query(conn, &sql, [])
    }
}

fn count_workflow_node_status(
    conn: &rusqlite::Connection,
    session_id: Option<&str>,
) -> Result<Value> {
    if let Some(session) = session_id {
        count_grouped_query(
            conn,
            "SELECT wns.status, COUNT(*) \
             FROM workflow_node_state wns \
             JOIN workflow_executions we ON we.id = wns.execution_id \
             WHERE we.session_id = ?1 \
             GROUP BY wns.status ORDER BY wns.status",
            params![session],
        )
    } else {
        count_grouped_query(
            conn,
            "SELECT status, COUNT(*) FROM workflow_node_state GROUP BY status ORDER BY status",
            [],
        )
    }
}

fn sum_llm_tokens(conn: &rusqlite::Connection, session_id: Option<&str>) -> Result<i64> {
    let value: Option<i64> = if let Some(session) = session_id {
        conn.query_row(
            "SELECT SUM(total_tokens) FROM llm_gateway_requests WHERE session_id = ?1",
            params![session],
            |row| row.get(0),
        )?
    } else {
        conn.query_row(
            "SELECT SUM(total_tokens) FROM llm_gateway_requests",
            [],
            |row| row.get(0),
        )?
    };
    Ok(value.unwrap_or(0))
}

fn count_grouped_query<P>(conn: &rusqlite::Connection, sql: &str, params: P) -> Result<Value>
where
    P: rusqlite::Params,
{
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params, |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut counts = serde_json::Map::new();
    for row in rows {
        let (key, count) = row?;
        counts.insert(key, json!(count));
    }
    Ok(Value::Object(counts))
}

fn select_trace_spans(
    conn: &rusqlite::Connection,
    session_id: Option<&str>,
    operation: Option<&str>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut spans = Vec::new();
    match (session_id, operation) {
        (Some(session), Some(op)) => {
            let mut stmt = conn.prepare(
                "SELECT trace_id, span_id, operation, start_ts, end_ts, status, attributes, session_id, agent_id \
                 FROM traces WHERE session_id = ?1 AND operation = ?2 ORDER BY start_ts, span_id LIMIT ?3",
            )?;
            let rows = stmt.query_map(params![session, op, limit], trace_span_row_json)?;
            for row in rows {
                spans.push(row?);
            }
        }
        (Some(session), None) => {
            let mut stmt = conn.prepare(
                "SELECT trace_id, span_id, operation, start_ts, end_ts, status, attributes, session_id, agent_id \
                 FROM traces WHERE session_id = ?1 ORDER BY start_ts, span_id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![session, limit], trace_span_row_json)?;
            for row in rows {
                spans.push(row?);
            }
        }
        (None, Some(op)) => {
            let mut stmt = conn.prepare(
                "SELECT trace_id, span_id, operation, start_ts, end_ts, status, attributes, session_id, agent_id \
                 FROM traces WHERE operation = ?1 ORDER BY start_ts, span_id LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![op, limit], trace_span_row_json)?;
            for row in rows {
                spans.push(row?);
            }
        }
        (None, None) => {
            let mut stmt = conn.prepare(
                "SELECT trace_id, span_id, operation, start_ts, end_ts, status, attributes, session_id, agent_id \
                 FROM traces ORDER BY start_ts, span_id LIMIT ?1",
            )?;
            let rows = stmt.query_map(params![limit], trace_span_row_json)?;
            for row in rows {
                spans.push(row?);
            }
        }
    }
    Ok(spans)
}

fn trace_span_row_json(row: &rusqlite::Row<'_>) -> Result<Value> {
    let attributes: Option<String> = row.get(6)?;
    Ok(json!({
        "trace_id": row.get::<_, String>(0)?,
        "span_id": row.get::<_, String>(1)?,
        "operation": row.get::<_, String>(2)?,
        "start_ts": row.get::<_, Timestamp>(3)?,
        "end_ts": row.get::<_, Option<Timestamp>>(4)?,
        "status": row.get::<_, Option<String>>(5)?,
        "attributes": attributes
            .as_deref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .unwrap_or_else(|| json!({})),
        "session_id": row.get::<_, Option<String>>(7)?,
        "agent_id": row.get::<_, Option<String>>(8)?,
    }))
}

fn select_execution_trace_events(
    conn: &rusqlite::Connection,
    session_id: Option<&str>,
    limit: i64,
) -> Result<Vec<Value>> {
    let mut events = Vec::new();
    if let Some(session) = session_id {
        let mut stmt = conn.prepare(
            "SELECT id, project_root, session_id, task_id, agent_id, agent_name, agent_role, task_type, status, duration_ms, files_changed, error_signature, fix_pattern, verification, metadata, created_at \
             FROM execution_trace_events WHERE session_id = ?1 ORDER BY created_at, id LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![session, limit], execution_trace_event_row_json)?;
        for row in rows {
            events.push(row?);
        }
    } else {
        let mut stmt = conn.prepare(
            "SELECT id, project_root, session_id, task_id, agent_id, agent_name, agent_role, task_type, status, duration_ms, files_changed, error_signature, fix_pattern, verification, metadata, created_at \
             FROM execution_trace_events ORDER BY created_at, id LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], execution_trace_event_row_json)?;
        for row in rows {
            events.push(row?);
        }
    }
    Ok(events)
}

fn execution_trace_event_row_json(row: &rusqlite::Row<'_>) -> Result<Value> {
    let files_changed: String = row.get(10)?;
    let metadata: Option<String> = row.get(14)?;
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "project_root": row.get::<_, String>(1)?,
        "session_id": row.get::<_, Option<String>>(2)?,
        "task_id": row.get::<_, Option<String>>(3)?,
        "agent_id": row.get::<_, Option<String>>(4)?,
        "agent_name": row.get::<_, Option<String>>(5)?,
        "agent_role": row.get::<_, Option<String>>(6)?,
        "task_type": row.get::<_, Option<String>>(7)?,
        "status": row.get::<_, String>(8)?,
        "duration_ms": row.get::<_, i64>(9)?,
        "files_changed": serde_json::from_str::<Value>(&files_changed).unwrap_or_else(|_| json!([])),
        "error_signature": row.get::<_, Option<String>>(11)?,
        "fix_pattern": row.get::<_, Option<String>>(12)?,
        "verification": row.get::<_, Option<String>>(13)?,
        "metadata": metadata
            .as_deref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .unwrap_or_else(|| json!({})),
        "created_at": row.get::<_, f64>(15)?,
    }))
}

fn validate_worktree_paths(
    repo_root: &str,
    worktree_path: &str,
    worktree_must_exist: bool,
) -> std::result::Result<(), CoreError> {
    let repo = std::path::Path::new(repo_root);
    if !repo.exists() {
        return Err(CoreError::new(
            ErrorCode::InvalidTransition,
            "repo_root does not exist",
        ));
    }
    let repo_canonical = repo
        .canonicalize()
        .map_err(|e| CoreError::internal(format!("repo_root canonicalize failed: {e}")))?;
    let path = std::path::Path::new(worktree_path);
    if path == repo_canonical {
        return Err(CoreError::new(
            ErrorCode::InvalidTransition,
            "worktree path must differ from repo_root",
        ));
    }
    if worktree_must_exist {
        if !path.exists() {
            return Err(CoreError::new(
                ErrorCode::InvalidTransition,
                "worktree path does not exist",
            ));
        }
        return Ok(());
    }
    if path.exists() {
        return Err(CoreError::new(
            ErrorCode::InvalidTransition,
            "worktree path already exists",
        ));
    }
    let Some(parent) = path.parent() else {
        return Err(CoreError::new(
            ErrorCode::InvalidTransition,
            "worktree path has no parent",
        ));
    };
    if !parent.exists() {
        return Err(CoreError::new(
            ErrorCode::InvalidTransition,
            "worktree parent does not exist",
        ));
    }
    Ok(())
}

fn run_git_worktree(repo_root: &str, args: &[&str]) -> std::result::Result<(), CoreError> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| CoreError::internal(format!("git spawn failed: {e}")))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let sanitized = stderr
        .lines()
        .next()
        .unwrap_or("git worktree failed")
        .trim();
    Err(CoreError::with_details(
        ErrorCode::InvalidTransition,
        "git worktree command failed",
        json!({
            "exit_code": output.status.code(),
            "stderr": sanitized,
        }),
        false,
    ))
}

fn select_worktrees(
    conn: &rusqlite::Connection,
    session_id: Option<&str>,
    include_deleted: bool,
) -> Result<Vec<Value>> {
    let mut worktrees = Vec::new();
    match (session_id, include_deleted) {
        (Some(session), true) => {
            let mut stmt = conn.prepare(
                "SELECT id, name, repo_root, path, branch, base_branch, session_id, task_id, status, created_at, updated_at, last_error \
                 FROM worktrees WHERE session_id = ?1 ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map(params![session], worktree_row_json)?;
            for row in rows {
                worktrees.push(row?);
            }
        }
        (Some(session), false) => {
            let mut stmt = conn.prepare(
                "SELECT id, name, repo_root, path, branch, base_branch, session_id, task_id, status, created_at, updated_at, last_error \
                 FROM worktrees WHERE session_id = ?1 AND status != 'deleted' ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map(params![session], worktree_row_json)?;
            for row in rows {
                worktrees.push(row?);
            }
        }
        (None, true) => {
            let mut stmt = conn.prepare(
                "SELECT id, name, repo_root, path, branch, base_branch, session_id, task_id, status, created_at, updated_at, last_error \
                 FROM worktrees ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map([], worktree_row_json)?;
            for row in rows {
                worktrees.push(row?);
            }
        }
        (None, false) => {
            let mut stmt = conn.prepare(
                "SELECT id, name, repo_root, path, branch, base_branch, session_id, task_id, status, created_at, updated_at, last_error \
                 FROM worktrees WHERE status != 'deleted' ORDER BY created_at, id",
            )?;
            let rows = stmt.query_map([], worktree_row_json)?;
            for row in rows {
                worktrees.push(row?);
            }
        }
    }
    Ok(worktrees)
}

fn worktree_row_json(row: &rusqlite::Row<'_>) -> Result<Value> {
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "name": row.get::<_, String>(1)?,
        "repo_root": row.get::<_, String>(2)?,
        "path": row.get::<_, String>(3)?,
        "branch": row.get::<_, String>(4)?,
        "base_branch": row.get::<_, String>(5)?,
        "session_id": row.get::<_, Option<String>>(6)?,
        "task_id": row.get::<_, Option<String>>(7)?,
        "status": row.get::<_, String>(8)?,
        "created_at": row.get::<_, f64>(9)?,
        "updated_at": row.get::<_, f64>(10)?,
        "last_error": row.get::<_, Option<String>>(11)?,
    }))
}

fn agent_worker_token(session_id: &str, agent_id: &str) -> String {
    format!("agent:{session_id}:{agent_id}")
}

fn ensure_session_active_outside(
    db: &DbOwner,
    session_id: &str,
) -> std::result::Result<(), CoreError> {
    let conn = db.conn();
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| CoreError::internal(format!("session lookup failed: {e}")))?;
    match status.as_deref().map(session_status_from_str) {
        Some(status) if !status.is_terminal() => Ok(()),
        Some(_) => Err(CoreError::session_already_terminal(session_id)),
        None => Err(CoreError::session_not_found(session_id)),
    }
}

fn ensure_session_active(tx: &Transaction, session_id: &str, _request_id: &str) -> Result<()> {
    let status: Option<String> = tx
        .query_row(
            "SELECT status FROM sessions WHERE id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()?;
    match status.as_deref().map(session_status_from_str) {
        Some(status) if !status.is_terminal() => Ok(()),
        Some(_) => Err(rusqlite::Error::InvalidQuery),
        None => Err(rusqlite::Error::QueryReturnedNoRows),
    }
}

fn load_task_status_generation(
    tx: &Transaction,
    session_id: &str,
    task_id: &str,
    _request_id: &str,
) -> Result<(String, i64)> {
    tx.query_row(
        "SELECT status, run_generation FROM tasks WHERE id = ?1 AND session_id = ?2",
        params![task_id, session_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
}

fn current_permission_mode(tx: &Transaction, session_id: &str) -> Result<Option<String>> {
    tx.query_row(
        "SELECT mode FROM permission_modes WHERE session_id = ?1",
        params![session_id],
        |row| row.get(0),
    )
    .optional()
}

fn current_permission_generation(tx: &Transaction, session_id: &str) -> Result<i64> {
    Ok(tx
        .query_row(
            "SELECT generation FROM permission_modes WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

#[allow(clippy::too_many_arguments)]
fn insert_agent_log(
    tx: &Transaction,
    session_id: &str,
    agent_id: &str,
    agent_name: &str,
    agent_role: &str,
    task_id: &str,
    event_type: &str,
    content: &str,
    occurred_at: Timestamp,
) -> Result<()> {
    tx.execute(
        "INSERT INTO agent_logs \
         (session_id, agent_id, agent_name, agent_role, task_id, event_type, content, timestamp) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            session_id,
            agent_id,
            agent_name,
            agent_role,
            task_id,
            event_type,
            content,
            occurred_at as f64 / 1000.0
        ],
    )?;
    Ok(())
}

fn insert_workflow_log(
    tx: &Transaction,
    execution_id: &str,
    level: &str,
    node_id: Option<&str>,
    message: &str,
    occurred_at: Timestamp,
) -> Result<()> {
    tx.execute(
        "INSERT INTO workflow_execution_logs \
         (execution_id, timestamp, level, node_id, message) \
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![execution_id, occurred_at, level, node_id, message],
    )?;
    Ok(())
}

fn string_param(cmd: &CommandEnvelope, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        cmd.params
            .get(*key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    })
}

fn string_array_param(cmd: &CommandEnvelope, key: &str) -> Vec<String> {
    cmd.params
        .get(key)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_bus_priority(priority: &str) -> std::result::Result<Priority, String> {
    match priority.to_ascii_lowercase().as_str() {
        "p0" | "0" => Ok(Priority::P0),
        "p1" | "1" => Ok(Priority::P1),
        "p2" | "2" => Ok(Priority::P2),
        "p3" | "3" => Ok(Priority::P3),
        other => Err(format!("Invalid bus priority: {other}")),
    }
}

fn bus_priority_label(priority: Priority) -> &'static str {
    match priority {
        Priority::P0 => "p0",
        Priority::P1 => "p1",
        Priority::P2 => "p2",
        Priority::P3 => "p3",
    }
}

fn bus_message_json(message: BusMessage) -> Value {
    let content = String::from_utf8(message.content.clone()).ok();
    json!({
        "from": message.from,
        "to": message.to,
        "priority": bus_priority_label(message.priority),
        "content": content,
        "content_bytes": message.content.len(),
    })
}

#[derive(Debug)]
struct ScheduleToFire {
    id: String,
    session_id: String,
    cron: String,
    prompt: String,
    recurring: bool,
}

fn schedule_row_json(row: &rusqlite::Row<'_>) -> Result<Value> {
    let workflow_input: Option<String> = row.get(8)?;
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "session_id": row.get::<_, String>(1)?,
        "cron": row.get::<_, String>(2)?,
        "prompt": row.get::<_, String>(3)?,
        "task_type": row.get::<_, String>(4)?,
        "intensity": row.get::<_, String>(5)?,
        "audience": row.get::<_, String>(6)?,
        "workflow_id": row.get::<_, Option<String>>(7)?,
        "workflow_input": workflow_input
            .as_deref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok()),
        "last_execution_id": row.get::<_, Option<String>>(9)?,
        "last_error": row.get::<_, Option<String>>(10)?,
        "recurring": row.get::<_, i64>(11)? != 0,
        "durable": row.get::<_, i64>(12)? != 0,
        "enabled": row.get::<_, i64>(13)? != 0,
        "last_run_at": row.get::<_, Option<f64>>(14)?,
        "next_run_at": row.get::<_, Option<f64>>(15)?,
        "created_at": row.get::<_, f64>(16)?,
    }))
}

fn select_schedules_to_fire(
    tx: &Transaction,
    schedule_id: Option<&str>,
    session_id: Option<&str>,
    due_only: bool,
    now_s: f64,
) -> Result<Vec<ScheduleToFire>> {
    let map_row = |row: &rusqlite::Row<'_>| {
        Ok(ScheduleToFire {
            id: row.get(0)?,
            session_id: row.get(1)?,
            cron: row.get(2)?,
            prompt: row.get(3)?,
            recurring: row.get::<_, i64>(4)? != 0,
        })
    };
    let mut out = Vec::new();
    match (schedule_id, session_id, due_only) {
        (Some(id), Some(session), false) => {
            let mut stmt = tx.prepare(
                "SELECT id, session_id, cron, prompt, recurring FROM scheduled_tasks \
                 WHERE id = ?1 AND session_id = ?2 AND enabled = 1",
            )?;
            let rows = stmt.query_map(params![id, session], map_row)?;
            for row in rows {
                out.push(row?);
            }
        }
        (Some(id), None, false) => {
            let mut stmt = tx.prepare(
                "SELECT id, session_id, cron, prompt, recurring FROM scheduled_tasks \
                 WHERE id = ?1 AND enabled = 1",
            )?;
            let rows = stmt.query_map(params![id], map_row)?;
            for row in rows {
                out.push(row?);
            }
        }
        (_, Some(session), true) => {
            let mut stmt = tx.prepare(
                "SELECT id, session_id, cron, prompt, recurring FROM scheduled_tasks \
                 WHERE session_id = ?1 AND enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?2 \
                 ORDER BY next_run_at, id",
            )?;
            let rows = stmt.query_map(params![session, now_s], map_row)?;
            for row in rows {
                out.push(row?);
            }
        }
        (_, None, true) => {
            let mut stmt = tx.prepare(
                "SELECT id, session_id, cron, prompt, recurring FROM scheduled_tasks \
                 WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?1 \
                 ORDER BY next_run_at, id",
            )?;
            let rows = stmt.query_map(params![now_s], map_row)?;
            for row in rows {
                out.push(row?);
            }
        }
        (None, _, false) => {}
    }
    Ok(out)
}

fn assumption_row_json(row: &rusqlite::Row<'_>) -> Result<Value> {
    let dependents: String = row.get(8)?;
    let evidence: Option<String> = row.get(13)?;
    Ok(json!({
        "id": row.get::<_, String>(0)?,
        "title": row.get::<_, String>(1)?,
        "content": row.get::<_, Option<String>>(2)?,
        "status": row.get::<_, String>(3)?,
        "verification_type": row.get::<_, String>(4)?,
        "verification_target": row.get::<_, String>(5)?,
        "verification_expected": row.get::<_, String>(6)?,
        "verification_actual": row.get::<_, Option<String>>(7)?,
        "dependents": serde_json::from_str::<Value>(&dependents).unwrap_or_else(|_| json!([])),
        "created_by": row.get::<_, Option<String>>(9)?,
        "created_at": row.get::<_, f64>(10)?,
        "verified_at": row.get::<_, Option<f64>>(11)?,
        "falsified_at": row.get::<_, Option<f64>>(12)?,
        "evidence": evidence
            .as_deref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok()),
    }))
}

fn embedding_param(cmd: &CommandEnvelope, key: &str) -> Option<Vec<f32>> {
    let values = cmd.params.get(key)?.as_array()?;
    values
        .iter()
        .map(|value| value.as_f64().map(|number| number as f32))
        .collect()
}

fn encode_embedding_f32(values: &[f32]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        encoded.extend_from_slice(&value.to_le_bytes());
    }
    encoded
}

fn decode_embedding_f32(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(std::mem::size_of::<f32>())
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> Option<f64> {
    if left.len() != right.len() || left.is_empty() {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut left_norm = 0.0_f64;
    let mut right_norm = 0.0_f64;
    for (left_value, right_value) in left.iter().zip(right.iter()) {
        let l = f64::from(*left_value);
        let r = f64::from(*right_value);
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm == 0.0 || right_norm == 0.0 {
        return None;
    }
    Some(dot / (left_norm.sqrt() * right_norm.sqrt()))
}

fn fts_query_from_text(query: &str) -> String {
    let terms = query
        .split_whitespace()
        .map(|term| {
            let escaped = term.replace('"', "\"\"");
            format!("\"{escaped}\"")
        })
        .collect::<Vec<_>>();
    if terms.is_empty() {
        "\"\"".into()
    } else {
        terms.join(" AND ")
    }
}

fn stable_fingerprint(body: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in body.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("fnv1a64:{hash:016x}")
}

fn permission_lease_from_cmd(cmd: &CommandEnvelope) -> Option<PermissionLease> {
    let lease = cmd.params.get("permission_lease")?;
    Some(PermissionLease {
        scope: lease.get("scope")?.as_str()?.to_string(),
        generation: lease
            .get("generation")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        expires_at: lease.get("expires_at")?.as_i64()?,
    })
}

fn auth_context_from_cmd(cmd: &CommandEnvelope) -> Option<AuthContext> {
    cmd.params
        .get("auth_context")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

fn request_options_from_cmd(cmd: &CommandEnvelope) -> RequestOptions {
    let mut options: RequestOptions = cmd
        .params
        .get("options")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    if options.timeout_ms_hint.is_none() {
        options.timeout_ms_hint = cmd.params.get("timeout_ms").and_then(|v| v.as_u64());
    }
    if options.max_tokens.is_none() {
        options.max_tokens = cmd
            .params
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
    }
    options
}

fn estimate_text_tokens(text: &str) -> u64 {
    let chars = text.chars().count() as u64;
    let words = text.split_whitespace().count() as u64;
    chars.div_ceil(4).max(words).max(1)
}

fn estimate_messages_tokens(messages: &[Message]) -> u64 {
    messages
        .iter()
        .map(|message| estimate_text_tokens(&message.content))
        .sum::<u64>()
        .max(1)
}

fn response_with_events(
    request_id: RequestId,
    events: Vec<EventEnvelope>,
    result: Value,
) -> CommandResponse {
    let latest_seq = events.last().map(|event| event.seq);
    CommandResponse {
        request_id,
        success: true,
        result: Some(result),
        error: None,
        events,
        latest_seq,
    }
}

fn document_tool_error_response(
    request_id: RequestId,
    operation: &str,
    error: DocumentToolError,
) -> CommandResponse {
    let retryable = matches!(error, DocumentToolError::Timeout { .. });
    CommandResponse::err(
        request_id,
        CoreError::with_details(
            ErrorCode::InvalidTransition,
            format!("{operation} failed: {}", document_error_message(&error)),
            document_error_payload(&error),
            retryable,
        ),
    )
}

fn mcp_bridge_error_response(request_id: RequestId, error: McpBridgeError) -> CommandResponse {
    let retryable = matches!(error, McpBridgeError::Timeout);
    CommandResponse::err(
        request_id,
        CoreError::with_details(
            ErrorCode::InvalidTransition,
            mcp_error_message(&error),
            mcp_error_payload(&error),
            retryable,
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn simple_event(
    tx: &Transaction,
    session_id: &str,
    generation: Generation,
    event_type: &str,
    source: Actor,
    mut payload: Value,
    occurred_at: Timestamp,
    request_id: &str,
) -> Result<EventEnvelope> {
    if let Some(obj) = payload.as_object_mut() {
        obj.entry("session_id")
            .or_insert_with(|| Value::String(session_id.to_string()));
    }
    append_event_in_tx(
        tx,
        Some(session_id.to_string()),
        generation,
        event_type,
        source,
        payload,
        occurred_at,
        Some(request_id.to_string()),
        Some(request_id.to_string()),
        format!(
            "{}_{}_{}_{}",
            event_type.replace('.', "_"),
            session_id,
            request_id,
            occurred_at
        ),
    )
}

#[allow(clippy::too_many_arguments)]
fn begin_tool_call_in_tx(
    tx: &Transaction,
    session_id: &str,
    tool_call_id: &str,
    tool_name: &str,
    tool_type: &str,
    args: &Value,
    occurred_at: Timestamp,
    resource_usage: Value,
) -> Result<()> {
    let persisted_args = sanitized_persistence_value(args);
    tx.execute(
        "INSERT INTO tool_calls \
         (id, session_id, tool_name, tool_type, status, args_json, started_at, resource_usage_json) \
         VALUES (?1, ?2, ?3, ?4, 'running', ?5, ?6, ?7) \
         ON CONFLICT(session_id, id) DO UPDATE SET \
         tool_name = excluded.tool_name, tool_type = excluded.tool_type, \
         status = 'running', args_json = excluded.args_json, started_at = excluded.started_at, \
         resource_usage_json = excluded.resource_usage_json",
        params![
            tool_call_id,
            session_id,
            tool_name,
            tool_type,
            persisted_args.to_string(),
            occurred_at,
            resource_usage.to_string()
        ],
    )?;
    Ok(())
}

fn set_tool_call_terminal(
    tx: &Transaction,
    session_id: &str,
    tool_call_id: &str,
    status: &str,
    result: Option<&Value>,
    error: Option<&str>,
    occurred_at: Timestamp,
) -> Result<()> {
    let persisted_result = result.map(sanitized_persistence_value);
    tx.execute(
        "UPDATE tool_calls SET status = ?1, result_json = ?2, error = ?3, completed_at = ?4 \
         WHERE session_id = ?5 AND id = ?6",
        params![
            status,
            persisted_result.map(|value| value.to_string()),
            error,
            occurred_at,
            session_id,
            tool_call_id
        ],
    )?;
    Ok(())
}

fn sanitized_persistence_value(value: &Value) -> Value {
    let serialized_len = value.to_string().len();
    let shape = match value {
        Value::Null => json!({"kind": "null"}),
        Value::Bool(_) => json!({"kind": "bool"}),
        Value::Number(_) => json!({"kind": "number"}),
        Value::String(text) => json!({
            "kind": "string",
            "chars": text.chars().count(),
            "bytes": text.len(),
        }),
        Value::Array(items) => json!({
            "kind": "array",
            "len": items.len(),
        }),
        Value::Object(object) => json!({
            "kind": "object",
            "fields": object.len(),
        }),
    };
    json!({
        "redaction": "omitted",
        "shape": shape,
        "serialized_bytes": serialized_len,
    })
}

#[allow(clippy::too_many_arguments)]
fn append_event_in_tx(
    tx: &Transaction,
    session_id: Option<String>,
    generation: Generation,
    event_type: &str,
    source: Actor,
    payload: Value,
    occurred_at: Timestamp,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    event_id: String,
) -> Result<EventEnvelope> {
    if let Some(existing) = try_fetch_event_by_id(tx, &event_id)? {
        return Ok(existing);
    }

    ensure_meta(tx, &session_id)?;
    let seq = allocate_seq(tx, &session_id)?;
    let event = EventEnvelope {
        event_id,
        session_id,
        seq,
        generation,
        event_type: event_type.to_string(),
        source,
        payload,
        occurred_at,
        causation_id,
        correlation_id,
    };
    let persisted_event = EventEnvelope {
        payload: redact_persistence_secrets(&event.payload),
        ..event.clone()
    };
    insert_event(tx, &persisted_event)?;
    update_meta_seq(tx, &event.session_id, seq)?;
    Ok(event)
}

#[allow(clippy::too_many_arguments)]
fn append_event_with_persisted_payload_in_tx(
    tx: &Transaction,
    session_id: Option<String>,
    generation: Generation,
    event_type: &str,
    source: Actor,
    response_payload: Value,
    persisted_payload: Value,
    occurred_at: Timestamp,
    causation_id: Option<String>,
    correlation_id: Option<String>,
    event_id: String,
) -> Result<EventEnvelope> {
    if let Some(existing) = try_fetch_event_by_id(tx, &event_id)? {
        return Ok(existing);
    }

    ensure_meta(tx, &session_id)?;
    let seq = allocate_seq(tx, &session_id)?;
    let persisted_event = EventEnvelope {
        event_id: event_id.clone(),
        session_id: session_id.clone(),
        seq,
        generation,
        event_type: event_type.to_string(),
        source: source.clone(),
        payload: redact_persistence_secrets(&persisted_payload),
        occurred_at,
        causation_id: causation_id.clone(),
        correlation_id: correlation_id.clone(),
    };
    insert_event(tx, &persisted_event)?;
    update_meta_seq(tx, &persisted_event.session_id, seq)?;
    Ok(EventEnvelope {
        event_id,
        session_id,
        seq,
        generation,
        event_type: event_type.to_string(),
        source,
        payload: response_payload,
        occurred_at,
        causation_id,
        correlation_id,
    })
}

fn redact_persistence_secrets(value: &Value) -> Value {
    match value {
        Value::String(text) => Value::String(redact_secret_text(text)),
        Value::Array(items) => Value::Array(items.iter().map(redact_persistence_secrets).collect()),
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), redact_persistence_secrets(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn redact_secret_text(text: &str) -> String {
    let mut redacted = redact_prefixed_secret(text, "sk-");
    redacted = redact_prefixed_secret(&redacted, "AKIA");
    redact_prefixed_secret(&redacted, "ASIA")
}

fn redact_prefixed_secret(text: &str, prefix: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(relative_start) = text[cursor..].find(prefix) {
        let start = cursor + relative_start;
        output.push_str(&text[cursor..start]);
        let end = text[start..]
            .find(|ch: char| {
                ch.is_whitespace()
                    || matches!(ch, '"' | '\'' | ',' | ';' | ')' | ']' | '}' | '<' | '>')
            })
            .map(|relative_end| start + relative_end)
            .unwrap_or(text.len());
        output.push_str("[redacted]");
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{
        FinishReason, GenerateResponse, LlmProvider, ProviderError, ProviderErrorCode, TokenUsage,
    };
    use crate::persistence::DbOwner;
    use lingxiao_core_protocol::actor::Actor;
    use lingxiao_core_protocol::actor::ActorKind;
    use lingxiao_core_protocol::snapshot::SnapshotEnvelope;
    use serde_json::Value;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    static TEST_REQ_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn setup_router() -> CommandRouter {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        CommandRouter::new(db)
    }

    fn setup_router_with_browser_sidecar() -> CommandRouter {
        setup_router().with_sidecar_command(
            "browser.open",
            SidecarCommand {
                program: powershell(),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-File".into(),
                    write_sidecar_provider().to_string_lossy().to_string(),
                ],
                cwd: None,
            },
        )
    }

    fn setup_router_with_llm_provider(script: PathBuf) -> CommandRouter {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(
            crate::llm::ExternalProcessProvider::new("external", powershell()).with_args(vec![
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                &script.to_string_lossy(),
            ]),
        ));
        CommandRouter::new(db).with_llm_router(LlmRouter::new(registry))
    }

    fn setup_router_with_mock_stream(
        stream: Vec<std::result::Result<StreamEvent, crate::llm::ProviderError>>,
    ) -> CommandRouter {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(
            MockLlmProvider::new("mock")
                .with_models(vec!["mock/model"])
                .with_stream(stream),
        ));
        CommandRouter::new(db).with_llm_router(LlmRouter::new(registry))
    }

    #[derive(Debug)]
    struct ExhaustRouterRetryThenOkProvider {
        calls: AtomicUsize,
    }

    impl LlmProvider for ExhaustRouterRetryThenOkProvider {
        fn provider_id(&self) -> &'static str {
            "transient"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "transient/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "retry ok".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if call < 4 {
                return Err(ProviderError::new(
                    ProviderErrorCode::ServerError,
                    "temporary workflow provider failure",
                ));
            }
            Ok(vec![
                Ok(StreamEvent::TextDelta("retry ok".into())),
                Ok(StreamEvent::Finished(FinishReason::Stop)),
            ])
        }
    }

    fn setup_router_with_transient_llm_provider() -> CommandRouter {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ExhaustRouterRetryThenOkProvider {
            calls: AtomicUsize::new(0),
        }));
        CommandRouter::new(db).with_llm_router(LlmRouter::new(registry))
    }

    #[derive(Debug)]
    struct TaskToolRoundProvider {
        target_path: String,
        calls: AtomicUsize,
    }

    impl LlmProvider for TaskToolRoundProvider {
        fn provider_id(&self) -> &'static str {
            "task_tool"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "task-tool/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "unused".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            let saw_tool_observation = request.messages.iter().any(|message| {
                message.role == "tool" && message.tool_call_id.as_deref() == Some("task-read")
            });
            if saw_tool_observation {
                return Ok(vec![
                    Ok(StreamEvent::TextDelta(
                        "TASK_TOOL_DONE after canonical observation".into(),
                    )),
                    Ok(StreamEvent::Usage(TokenUsage {
                        prompt_tokens: 10,
                        completion_tokens: 5,
                        total_tokens: 15,
                        cache_creation_input_tokens: None,
                        cache_read_input_tokens: None,
                        reasoning_tokens: None,
                    })),
                    Ok(StreamEvent::Finished(FinishReason::Stop)),
                ]);
            }
            Ok(vec![
                Ok(StreamEvent::ToolCall(ToolCall {
                    id: "task-read".into(),
                    name: "file_read".into(),
                    arguments: json!({"path": self.target_path}),
                })),
                Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
            ])
        }
    }

    fn setup_router_with_task_tool_provider(target_path: String) -> CommandRouter {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(TaskToolRoundProvider {
            target_path,
            calls: AtomicUsize::new(0),
        }));
        CommandRouter::new(db).with_llm_router(LlmRouter::new(registry))
    }

    #[derive(Debug)]
    struct CapturingReplayProvider {
        calls: AtomicUsize,
        seen: Arc<Mutex<Vec<Vec<Message>>>>,
    }

    impl LlmProvider for CapturingReplayProvider {
        fn provider_id(&self) -> &'static str {
            "capturing-replay"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "replay/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "ok".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            self.seen.lock().unwrap().push(request.messages);
            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(vec![
                Ok(StreamEvent::TextDelta(format!("answer-{call}"))),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
    }

    impl LlmProvider for CountingProvider {
        fn provider_id(&self) -> &'static str {
            "counting"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "counting/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(GenerateResponse {
                content: "ok".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(vec![
                Ok(StreamEvent::TextDelta("ok".into())),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct ToolSchemaCapturingProvider {
        seen_tools: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl LlmProvider for ToolSchemaCapturingProvider {
        fn provider_id(&self) -> &'static str {
            "tool-schema-capturing"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "tool-schema/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "ok".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            self.seen_tools
                .lock()
                .unwrap()
                .push(request.tools.iter().map(|tool| tool.name.clone()).collect());
            Ok(vec![
                Ok(StreamEvent::TextDelta("ok".into())),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct ToolReadThenFinalProvider {
        path: String,
        calls: AtomicUsize,
    }

    impl LlmProvider for ToolReadThenFinalProvider {
        fn provider_id(&self) -> &'static str {
            "tool-read-final"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "tool-read/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "verified".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if call == 0 {
                return Ok(vec![
                    Ok(StreamEvent::ToolCall(crate::llm::ToolCall {
                        id: "read-verification-file".into(),
                        name: "file_read".into(),
                        arguments: json!({"path": self.path}),
                    })),
                    Ok(StreamEvent::Finished(crate::llm::FinishReason::ToolCalls)),
                ]);
            }
            Ok(vec![
                Ok(StreamEvent::TextDelta(
                    "final answer verified from file".into(),
                )),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct DeltaToolReadThenFinalProvider {
        path: String,
        calls: AtomicUsize,
    }

    impl LlmProvider for DeltaToolReadThenFinalProvider {
        fn provider_id(&self) -> &'static str {
            "delta-tool-read-final"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "delta-tool-read/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "verified".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            let call = self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if call == 0 {
                return Ok(vec![
                    Ok(StreamEvent::ToolCallDelta(crate::llm::ToolCallDelta {
                        index: 0,
                        id: Some("delta-read-verification-file".into()),
                        name: Some("file_read".into()),
                        partial_json: Some(format!(
                            r#"{{"path":{}"#,
                            serde_json::to_string(&self.path).unwrap()
                        )),
                    })),
                    Ok(StreamEvent::ToolCallDelta(crate::llm::ToolCallDelta {
                        index: 0,
                        id: None,
                        name: None,
                        partial_json: Some("}".into()),
                    })),
                    Ok(StreamEvent::Finished(crate::llm::FinishReason::ToolCalls)),
                ]);
            }
            Ok(vec![
                Ok(StreamEvent::TextDelta(
                    "final answer verified from delta tool".into(),
                )),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
            ])
        }
    }

    #[derive(Debug)]
    struct WriteThenFinalProvider {
        path: String,
    }

    impl LlmProvider for WriteThenFinalProvider {
        fn provider_id(&self) -> &'static str {
            "write-then-final"
        }

        fn supports_model(&self, model_id: &str) -> bool {
            model_id == "write/model"
        }

        fn generate(
            &self,
            _request: GenerateRequest,
        ) -> std::result::Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "write".into(),
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
        ) -> std::result::Result<Vec<std::result::Result<StreamEvent, ProviderError>>, ProviderError>
        {
            Ok(vec![
                Ok(StreamEvent::ToolCall(crate::llm::ToolCall {
                    id: "write-denied".into(),
                    name: "file_write".into(),
                    arguments: json!({"path": self.path, "content": "must not write"}),
                })),
                Ok(StreamEvent::Finished(crate::llm::FinishReason::ToolCalls)),
            ])
        }
    }

    fn make_cmd(
        method: &str,
        session_id: Option<&str>,
        params: Value,
        idempotency_key: Option<&str>,
    ) -> CommandEnvelope {
        let ctr = TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            request_id: format!("req-{ctr}"),
            method: method.into(),
            params,
            actor: Actor::new(ActorKind::User),
            session_id: session_id.map(|s| s.into()),
            idempotency_key: idempotency_key.map(|s| s.into()),
            submitted_at: now_ms(),
        }
    }

    fn assert_success(resp: &CommandResponse) {
        assert!(
            resp.success,
            "Expected success, got error: {:?}",
            resp.error
        );
    }

    fn assert_error_code(resp: &CommandResponse, code: ErrorCode) {
        assert!(!resp.success, "Expected error, got success");
        assert_eq!(
            resp.error.as_ref().unwrap().code,
            code,
            "Expected error code {code:?}, got {:?}",
            resp.error.as_ref().unwrap()
        );
    }

    fn grant_tool(router: &CommandRouter, session_id: &str, tool_name: &str, request_id: &str) {
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(session_id),
            json!({"permission_request_id": request_id, "tool_name": tool_name}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some(session_id),
            json!({"permission_request_id": request_id, "decision": "allow"}),
            None,
        )));
    }

    fn runtime_available(program: &str) -> bool {
        std::process::Command::new(program)
            .arg("--version")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .is_ok()
    }

    fn write_llm_stream_provider() -> PathBuf {
        write_script(
            "llm_stream_provider.ps1",
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
if ($req.auth_context.key -ne 'sk-router-secret') { exit 2 }
@{ TextDelta = 'external text' } | ConvertTo-Json -Compress
@{ Usage = @{
    prompt_tokens = 1
    completion_tokens = 2
    total_tokens = 3
    cache_creation_input_tokens = $null
    cache_read_input_tokens = $null
    reasoning_tokens = $null
  }
} | ConvertTo-Json -Depth 8 -Compress
@{ Finished = 'Stop' } | ConvertTo-Json -Compress
"#,
        )
    }

    fn write_sidecar_provider() -> PathBuf {
        write_script(
            "sidecar_provider.ps1",
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
$args = [System.Text.Encoding]::UTF8.GetString([byte[]]$req.args) | ConvertFrom-Json
$payload = "{""external"":true,""value"":$($args.value)}"
$response = @{
  Completed = @{
    request_id = $req.request_id
    result = [System.Text.Encoding]::UTF8.GetBytes($payload)
    result_shape = 'json'
    duration_ms = 1
    usage = @{
      runtime_ms = 1
      cpu_ms = 1
      memory_mb_peak = 1
      network_bytes = 0
      file_write_bytes = 0
    }
  }
}
$response | ConvertTo-Json -Depth 8 -Compress
"#,
        )
    }

    fn write_mcp_bridge_script() -> PathBuf {
        write_script(
            "mcp_bridge.ps1",
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
if ($req.method -eq 'initialize') {
  $response = @{
    jsonrpc = '2.0'
    id = $req.id
    result = @{
      protocolVersion = '2024-11-05'
      serverInfo = @{ name = 'fake-mcp'; version = '1.0.0' }
      capabilities = @{ tools = @{} }
    }
  }
  $response | ConvertTo-Json -Depth 8 -Compress
  exit 0
}
if ($req.method -eq 'tools/call') {
  $response = @{
    jsonrpc = '2.0'
    id = $req.id
    result = @{
      content = @(@{ type = 'text'; text = "echo:$($req.params.arguments.value)" })
      isError = $false
    }
  }
  $response | ConvertTo-Json -Depth 8 -Compress
  exit 0
}
$response = @{
  jsonrpc = '2.0'
  id = $req.id
  result = @{
    tools = @(@{
      name = 'echo'
      description = 'test tool'
      inputSchema = @{ type = 'object' }
    })
  }
}
$response | ConvertTo-Json -Depth 8 -Compress
"#,
        )
    }

    fn write_script(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lingxiao_router_{}", now_ms()));
        fs::create_dir_all(&dir).unwrap();
        let script = dir.join(name);
        fs::write(&script, body).unwrap();
        script
    }

    fn powershell() -> PathBuf {
        // Rust's `Command` on Windows does not do PATHEXT resolution, so a bare
        // `"powershell"` can fail to spawn. Anchor to the system root.
        for key in ["SystemRoot", "windir", "SYSTEMROOT", "WINDIR"] {
            if let Ok(root) = std::env::var(key) {
                let candidate = std::path::Path::new(&root)
                    .join("System32")
                    .join("WindowsPowerShell")
                    .join("v1.0")
                    .join("powershell.exe");
                if candidate.exists() {
                    return candidate;
                }
            }
        }
        PathBuf::from("powershell.exe")
    }

    fn run_git_for_test(cwd: &std::path::Path, args: &[&str]) -> bool {
        std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn init_git_repo_for_worktree_test(root: &std::path::Path) -> bool {
        if !run_git_for_test(root, &["init"]) {
            return false;
        }
        let _ = run_git_for_test(root, &["config", "user.email", "lingxiao@example.invalid"]);
        let _ = run_git_for_test(root, &["config", "user.name", "LingXiao Test"]);
        if std::fs::write(root.join("README.md"), "worktree test\n").is_err() {
            return false;
        }
        run_git_for_test(root, &["add", "README.md"])
            && run_git_for_test(root, &["commit", "-m", "init"])
    }

    // -----------------------------------------------------------------------
    // terminal.*
    // -----------------------------------------------------------------------

    #[test]
    fn test_terminal_create_requires_permission_before_process_spawn() {
        let router = setup_router();
        let created = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-terminal-denied", "workspace": "/tmp/ws"}),
            None,
        ));
        assert_success(&created);

        let denied = router.dispatch(make_cmd(
            "terminal.create",
            Some("sess-terminal-denied"),
            json!({"terminal_id": "term-denied"}),
            None,
        ));
        assert_error_code(&denied, ErrorCode::PermissionDenied);
        let conn = router.db.conn();
        let process_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM owned_processes WHERE owner_kind = 'terminal'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let terminal_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM terminal_sessions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(process_count, 0);
        assert_eq!(terminal_count, 0);
    }

    #[test]
    fn test_terminal_session_create_send_read_kill_and_registry() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-terminal", "workspace": "/tmp/ws"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some("sess-terminal"),
            json!({"permission_request_id": "perm-terminal", "tool_name": "terminal"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some("sess-terminal"),
            json!({"permission_request_id": "perm-terminal", "decision": "allow"}),
            None,
        )));

        let create = router.dispatch(make_cmd(
            "terminal.create",
            Some("sess-terminal"),
            json!({"terminal_id": "term-live"}),
            None,
        ));
        assert_success(&create);
        assert_eq!(create.events[0].event_type, "terminal.created");
        assert!(create.result.as_ref().unwrap()["pid"].as_u64().unwrap() > 0);

        let send = router.dispatch(make_cmd(
            "terminal.send",
            Some("sess-terminal"),
            json!({"terminal_id": "term-live", "input": "echo LX_ROUTER_TERMINAL\n"}),
            None,
        ));
        assert_success(&send);
        assert_eq!(send.events[0].event_type, "terminal.input_sent");
        assert!(send.events[0].payload.get("input").is_none());

        let mut saw_output = false;
        for _ in 0..20 {
            let read = router.dispatch(make_cmd(
                "terminal.read",
                Some("sess-terminal"),
                json!({"terminal_id": "term-live", "max_bytes": 4096}),
                None,
            ));
            assert_success(&read);
            if read.result.as_ref().unwrap()["stdout"]
                .as_str()
                .unwrap()
                .contains("LX_ROUTER_TERMINAL")
            {
                saw_output = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(saw_output);

        {
            let conn = router.db.conn();
            let status: String = conn
                .query_row(
                    "SELECT status FROM owned_processes WHERE id = 'terminal:term-live'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(status, "active");
        }

        let kill = router.dispatch(make_cmd(
            "terminal.kill",
            Some("sess-terminal"),
            json!({"terminal_id": "term-live"}),
            None,
        ));
        assert_success(&kill);
        assert_eq!(kill.events[0].event_type, "terminal.killed");
        let conn = router.db.conn();
        let terminal_status: String = conn
            .query_row(
                "SELECT status FROM terminal_sessions WHERE id = 'term-live'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let process_status: String = conn
            .query_row(
                "SELECT status FROM owned_processes WHERE id = 'terminal:term-live'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(terminal_status, "killed");
        assert_eq!(process_status, "completed");
    }

    #[test]
    fn test_repl_eval_requires_permission_before_process_spawn() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-repl-denied", "workspace": "/tmp/ws"}),
            None,
        )));
        let denied = router.dispatch(make_cmd(
            "repl.eval",
            Some("sess-repl-denied"),
            json!({"eval_id": "repl-denied", "language": "python", "code": "print('secret')"}),
            None,
        ));
        assert_error_code(&denied, ErrorCode::PermissionDenied);
        let conn = router.db.conn();
        let process_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM owned_processes WHERE owner_kind = 'repl'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(process_count, 0);
    }

    #[test]
    fn test_repl_eval_missing_runtime_is_typed_after_grant() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-repl-missing", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(&router, "sess-repl-missing", "repl", "perm-repl-missing");
        let missing = router.dispatch(make_cmd(
            "repl.eval",
            Some("sess-repl-missing"),
            json!({"eval_id": "repl-missing", "language": "definitely-not-a-runtime", "code": "1"}),
            None,
        ));
        assert_error_code(&missing, ErrorCode::InvalidTransition);
        assert_eq!(
            missing.error.as_ref().unwrap().details.as_ref().unwrap()["kind"],
            "missing_runtime"
        );
    }

    #[test]
    fn test_repl_eval_python_executes_when_available_without_code_in_event() {
        if !runtime_available("python") && !runtime_available("py") {
            return;
        }
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-repl-python", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(&router, "sess-repl-python", "repl", "perm-repl-python");
        let eval = router.dispatch(make_cmd(
            "repl.eval",
            Some("sess-repl-python"),
            json!({
                "eval_id": "repl-python",
                "language": "python",
                "code": "print('LX_REPL_OK')",
                "timeout_ms": 5_000
            }),
            None,
        ));
        assert_success(&eval);
        assert!(eval.result.as_ref().unwrap()["stdout"]
            .as_str()
            .unwrap()
            .contains("LX_REPL_OK"));
        assert_eq!(eval.events[0].event_type, "repl.evaluated");
        assert!(eval.events[0].payload.get("code").is_none());
        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM owned_processes WHERE id = 'repl:repl-python'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn test_repl_create_missing_runtime_is_typed() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-repl-create-missing", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(
            &router,
            "sess-repl-create-missing",
            "repl",
            "perm-repl-create-missing",
        );
        let created = router.dispatch(make_cmd(
            "repl.create",
            Some("sess-repl-create-missing"),
            json!({"repl_id": "repl-missing-session", "language": "definitely-not-a-runtime"}),
            None,
        ));
        assert_error_code(&created, ErrorCode::InvalidTransition);
        assert_eq!(
            created.error.as_ref().unwrap().details.as_ref().unwrap()["kind"],
            "missing_runtime"
        );
    }

    #[test]
    fn test_repl_session_python_send_read_kill_when_available() {
        if !runtime_available("python") {
            return;
        }
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-repl-session", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(&router, "sess-repl-session", "repl", "perm-repl-session");
        let created = router.dispatch(make_cmd(
            "repl.create",
            Some("sess-repl-session"),
            json!({"repl_id": "repl-live", "language": "python"}),
            None,
        ));
        assert_success(&created);
        assert_eq!(created.events[0].event_type, "repl.created");

        let sent = router.dispatch(make_cmd(
            "repl.send",
            Some("sess-repl-session"),
            json!({"repl_id": "repl-live", "input": "print('LX_REPL_SESSION_OK')\n"}),
            None,
        ));
        assert_success(&sent);
        assert_eq!(
            sent.result.as_ref().unwrap()["redaction"]["input"],
            "omitted"
        );

        let mut saw_output = false;
        for _ in 0..30 {
            let read = router.dispatch(make_cmd(
                "repl.read",
                Some("sess-repl-session"),
                json!({"repl_id": "repl-live", "max_bytes": 4096}),
                None,
            ));
            assert_success(&read);
            if read.result.as_ref().unwrap()["stdout"]
                .as_str()
                .unwrap()
                .contains("LX_REPL_SESSION_OK")
            {
                saw_output = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(saw_output);

        let killed = router.dispatch(make_cmd(
            "repl.kill",
            Some("sess-repl-session"),
            json!({"repl_id": "repl-live"}),
            None,
        ));
        assert_success(&killed);
    }

    #[test]
    fn test_parse_file_text_extracts_without_external_dependency_and_sanitizes_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("note.txt");
        std::fs::write(&path, "visible document text").unwrap();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-parse", "workspace": dir.path().display().to_string()}),
            None,
        )));
        let parsed = router.dispatch(make_cmd(
            "parse_file",
            Some("sess-parse"),
            json!({"path": path.display().to_string()}),
            None,
        ));
        assert_success(&parsed);
        assert_eq!(parsed.result.as_ref().unwrap()["kind"], "text");
        assert_eq!(
            parsed.result.as_ref().unwrap()["text"],
            "visible document text"
        );
        assert_eq!(parsed.events[0].event_type, "document.parsed");
        assert!(parsed.events[0].payload.get("text").is_none());
    }

    #[test]
    fn test_native_tool_permission_scope_rejects_out_of_scope_before_side_effect() {
        let dir = tempfile::tempdir().unwrap();
        let allowed_dir = dir.path().join("allowed");
        let denied_dir = dir.path().join("denied");
        std::fs::create_dir_all(&allowed_dir).unwrap();
        std::fs::create_dir_all(&denied_dir).unwrap();
        let allowed_file = allowed_dir.join("ok.txt");
        let denied_file = denied_dir.join("no.txt");
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-scope", "workspace": "/tmp/ws"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some("sess-scope"),
            json!({
                "permission_request_id": "perm-scope",
                "tool_name": "file_write",
                "args": {"path": allowed_dir.display().to_string()}
            }),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some("sess-scope"),
            json!({"permission_request_id": "perm-scope", "decision": "allow"}),
            None,
        )));

        let allowed = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-scope"),
            json!({
                "tool_call_id": "tc-scope-ok",
                "tool_name": "file_write",
                "tool_type": "native",
                "args": {"path": allowed_file.display().to_string(), "content": "ok"}
            }),
            None,
        ));
        assert_success(&allowed);
        assert_eq!(std::fs::read_to_string(&allowed_file).unwrap(), "ok");

        let denied = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-scope"),
            json!({
                "tool_call_id": "tc-scope-denied",
                "tool_name": "file_write",
                "tool_type": "native",
                "args": {"path": denied_file.display().to_string(), "content": "no"}
            }),
            None,
        ));
        assert_error_code(&denied, ErrorCode::PermissionDenied);
        assert!(!denied_file.exists());
    }

    #[test]
    fn test_permission_scope_uses_path_boundary_not_string_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let allowed_dir = dir.path().join("tmp");
        let prefix_collision_dir = dir.path().join("tmp_safe");
        std::fs::create_dir_all(&allowed_dir).unwrap();
        std::fs::create_dir_all(&prefix_collision_dir).unwrap();

        assert!(permission_scope_allows(
            &allowed_dir.display().to_string(),
            Some(&allowed_dir.join("ok.txt").display().to_string())
        ));
        assert!(!permission_scope_allows(
            &allowed_dir.display().to_string(),
            Some(&prefix_collision_dir.join("no.txt").display().to_string())
        ));
    }

    #[test]
    fn test_parse_file_office_reports_typed_missing_dependency() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("report.docx");
        std::fs::write(&path, "placeholder").unwrap();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-office", "workspace": dir.path().display().to_string()}),
            None,
        )));
        let parsed = router.dispatch(make_cmd(
            "parse_file",
            Some("sess-office"),
            json!({"path": path.display().to_string()}),
            None,
        ));
        assert_error_code(&parsed, ErrorCode::InvalidTransition);
        assert_eq!(
            parsed.error.as_ref().unwrap().details.as_ref().unwrap(),
            &json!({"kind": "missing_dependency", "dependency": "soffice"})
        );
    }

    #[test]
    fn test_ocr_extract_text_reports_missing_tesseract_when_absent() {
        if runtime_available("tesseract") {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("image.png");
        std::fs::write(&path, b"not really an image").unwrap();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-ocr", "workspace": dir.path().display().to_string()}),
            None,
        )));
        let ocr = router.dispatch(make_cmd(
            "ocr.extract_text",
            Some("sess-ocr"),
            json!({"path": path.display().to_string()}),
            None,
        ));
        assert_error_code(&ocr, ErrorCode::InvalidTransition);
        assert_eq!(
            ocr.error.as_ref().unwrap().details.as_ref().unwrap()["kind"],
            "missing_dependency"
        );
    }

    #[test]
    fn test_mcp_bridge_requires_permission_before_spawn() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-mcp-denied", "workspace": "/tmp/ws"}),
            None,
        )));
        let denied = router.dispatch(make_cmd(
            "mcp.bridge",
            Some("sess-mcp-denied"),
            json!({
                "bridge_id": "mcp-denied",
                "program": "definitely-not-started",
                "payload": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            }),
            None,
        ));
        assert_error_code(&denied, ErrorCode::PermissionDenied);
        let conn = router.db.conn();
        let process_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM owned_processes WHERE owner_kind = 'mcp'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(process_count, 0);
    }

    #[test]
    fn test_direct_process_commands_reject_out_of_scope_before_spawn() {
        let dir = tempfile::tempdir().unwrap();
        let allowed = dir.path().join("allowed");
        let denied = dir.path().join("denied");
        std::fs::create_dir_all(&allowed).unwrap();
        std::fs::create_dir_all(&denied).unwrap();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-direct-scope", "workspace": "/tmp/ws"}),
            None,
        )));

        for (tool, permission_id) in [
            ("terminal", "perm-terminal-scope"),
            ("repl", "perm-repl-scope"),
            ("mcp", "perm-mcp-scope"),
        ] {
            assert_success(&router.dispatch(make_cmd(
                "permission.request",
                Some("sess-direct-scope"),
                json!({
                    "permission_request_id": permission_id,
                    "tool_name": tool,
                    "args": {"scope": allowed.display().to_string()}
                }),
                None,
            )));
            assert_success(&router.dispatch(make_cmd(
                "permission.resolve",
                Some("sess-direct-scope"),
                json!({"permission_request_id": permission_id, "decision": "allow"}),
                None,
            )));
        }

        let terminal = router.dispatch(make_cmd(
            "terminal.create",
            Some("sess-direct-scope"),
            json!({"terminal_id": "term-out", "cwd": denied.display().to_string()}),
            None,
        ));
        assert_error_code(&terminal, ErrorCode::PermissionDenied);

        let repl = router.dispatch(make_cmd(
            "repl.eval",
            Some("sess-direct-scope"),
            json!({
                "eval_id": "repl-out",
                "language": "python",
                "code": "print('must not run')",
                "cwd": denied.display().to_string()
            }),
            None,
        ));
        assert_error_code(&repl, ErrorCode::PermissionDenied);

        let mcp = router.dispatch(make_cmd(
            "mcp.bridge",
            Some("sess-direct-scope"),
            json!({
                "bridge_id": "mcp-out",
                "program": powershell().display().to_string(),
                "cwd": denied.display().to_string(),
                "payload": {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
            }),
            None,
        ));
        assert_error_code(&mcp, ErrorCode::PermissionDenied);

        let conn = router.db.conn();
        let process_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM owned_processes WHERE owner_id IN ('term-out', 'repl-out', 'mcp-out')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(process_count, 0);
    }

    #[test]
    fn test_mcp_bridge_invokes_stdio_json_without_leaking_params() {
        let script = write_mcp_bridge_script();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-mcp", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(&router, "sess-mcp", "mcp", "perm-mcp");
        let response = router.dispatch(make_cmd(
            "mcp.bridge",
            Some("sess-mcp"),
            json!({
                "bridge_id": "mcp-list",
                "program": powershell().display().to_string(),
                "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script.display().to_string()],
                "payload": {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"secret": "must-not-log"}}
            }),
            None,
        ));
        assert_success(&response);
        assert_eq!(
            response.result.as_ref().unwrap()["response"]["result"]["tools"][0]["name"],
            "echo"
        );
        assert_eq!(response.events[0].event_type, "mcp.bridge_invoked");
        assert_eq!(response.events[0].payload["method"], "tools/list");
        assert!(response.events[0].payload.get("params").is_none());
        assert!(!response.events[0]
            .payload
            .to_string()
            .contains("must-not-log"));
        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM owned_processes WHERE id = 'mcp:mcp-list'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn test_mcp_server_lifecycle_lists_calls_and_stops_fake_stdio_server() {
        let script = write_mcp_bridge_script();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-mcp-life", "workspace": "/tmp/ws"}),
            None,
        )));
        grant_tool(&router, "sess-mcp-life", "mcp", "perm-mcp-life");

        let started = router.dispatch(make_cmd(
            "mcp.server_start",
            Some("sess-mcp-life"),
            json!({
                "server_id": "fake",
                "program": powershell().display().to_string(),
                "args": ["-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script.display().to_string()]
            }),
            None,
        ));
        assert_success(&started);
        assert_eq!(started.events[0].event_type, "mcp.server_started");
        assert_eq!(started.result.as_ref().unwrap()["tools"][0]["name"], "echo");

        let listed = router.dispatch(make_cmd(
            "mcp.list_tools",
            Some("sess-mcp-life"),
            json!({"server_id": "fake"}),
            None,
        ));
        assert_success(&listed);
        assert_eq!(listed.result.as_ref().unwrap()["tools"][0]["name"], "echo");

        let called = router.dispatch(make_cmd(
            "mcp.call_tool",
            Some("sess-mcp-life"),
            json!({
                "server_id": "fake",
                "tool_name": "echo",
                "arguments": {"value": "hello"}
            }),
            None,
        ));
        assert_success(&called);
        assert_eq!(called.events[0].event_type, "mcp.tool_called");
        assert_eq!(
            called.result.as_ref().unwrap()["response"]["result"]["content"][0]["text"],
            "echo:hello"
        );
        assert!(!called.events[0].payload.to_string().contains("hello"));

        let stopped = router.dispatch(make_cmd(
            "mcp.server_stop",
            Some("sess-mcp-life"),
            json!({"server_id": "fake"}),
            None,
        ));
        assert_success(&stopped);

        let listed_after_stop = router.dispatch(make_cmd(
            "mcp.list_tools",
            Some("sess-mcp-life"),
            json!({"server_id": "fake"}),
            None,
        ));
        assert_error_code(&listed_after_stop, ErrorCode::InvalidTransition);
    }

    // -----------------------------------------------------------------------
    // session.create
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_create_generates_sessions_row_and_event() {
        let router = setup_router();
        let cmd = make_cmd(
            "session.create",
            None,
            json!({"workspace": "/tmp/test-ws"}),
            None,
        );

        let resp = router.dispatch(cmd);
        assert_success(&resp);
        assert_eq!(resp.events.len(), 1);

        let event = &resp.events[0];
        assert_eq!(event.event_type, "session.created");
        assert_eq!(event.seq, 1);
        assert_eq!(event.generation, 1);
        assert!(event.payload["session_id"]
            .as_str()
            .unwrap()
            .starts_with("sess_"));
        assert_eq!(event.payload["workspace"], "/tmp/test-ws");

        let sid = event.payload["session_id"].as_str().unwrap().to_string();
        drop(resp);

        let conn = router.db.conn();
        let db_status: String = conn
            .query_row(
                "SELECT status FROM sessions WHERE id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        drop(conn);
        assert_eq!(db_status, "active");

        let latest = router.event_log.latest_seq(&sid).unwrap();
        assert_eq!(latest, 1);
    }

    #[test]
    fn test_command_dispatch_writes_sanitized_trace_span() {
        let router = setup_router();
        let resp = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "trace-session", "workspace": "ws", "secret": "sk-trace-secret"}),
            None,
        ));
        assert_success(&resp);

        let conn = router.db.conn();
        let (operation, status, attributes): (String, String, String) = conn
            .query_row(
                "SELECT operation, status, attributes FROM traces WHERE trace_id = ?1",
                params![resp.request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(operation, "session.create");
        assert_eq!(status, "ok");
        assert!(attributes.contains("\"method\":\"session.create\""));
        assert!(!attributes.contains("sk-trace-secret"));
        assert!(!attributes.contains("\"workspace\""));
    }

    #[test]
    fn test_trace_timeline_returns_sanitized_spans_and_execution_events() {
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "trace-timeline-session", "workspace": "ws", "api_key": "sk-timeline-secret"}),
            None,
        )));

        let timeline = router.dispatch(make_cmd(
            "trace.timeline",
            None,
            json!({"operation": "session.create"}),
            None,
        ));
        assert_success(&timeline);
        let result = timeline.result.unwrap();
        assert!(result["spans"]
            .as_array()
            .unwrap()
            .iter()
            .any(|span| span["operation"] == "session.create"
                && span["attributes"]["method"] == "session.create"));
        assert!(result["execution_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["task_type"] == "command"
                && event["metadata"]["method"] == "session.create"));
        let serialized = result.to_string();
        assert!(!serialized.contains("sk-timeline-secret"));
        assert!(!serialized.contains("api_key"));
        assert_eq!(result["redaction"]["command_params"], "omitted");
    }

    #[test]
    fn test_metrics_query_aggregates_without_leaking_params() {
        let router = setup_router();
        let dir = tempfile::tempdir().unwrap();
        let sid = create_session_with_workspace(&router, dir.path());
        let secret = "sk-metrics-secret";
        let path = dir.path().join(format!("metrics_{}.txt", now_ms()));
        std::fs::write(&path, "metrics body").unwrap();

        let tool_resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "metrics-read",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {
                    "path": path.display().to_string(),
                    "api_key": secret
                },
            }),
            None,
        ));
        assert_success(&tool_resp);

        let metrics = router.dispatch(make_cmd("metrics.query", Some(&sid), json!({}), None));
        assert_success(&metrics);
        let result = metrics.result.unwrap();
        assert_eq!(
            result["metrics"]["runtime"]["command_count"].as_i64(),
            Some(1)
        );
        assert_eq!(
            result["metrics"]["tools"]["by_status"]["completed"].as_i64(),
            Some(1)
        );
        assert_eq!(
            result["metrics"]["tools"]["by_name"]["file_read"].as_i64(),
            Some(1)
        );
        assert_eq!(
            result["metrics"]["runtime"]["operations"]["tool.call"].as_i64(),
            Some(1)
        );
        assert!(!serde_json::to_string(&result).unwrap().contains(secret));
        assert_eq!(result["redaction"]["tool_args"], "omitted");
    }

    #[test]
    fn test_worktree_create_requires_git_write_before_side_effect() {
        let router = setup_router();
        let sid = create_session(&router);
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let worktree = dir.path().join("wt-denied");
        std::fs::create_dir_all(&repo).unwrap();

        let denied = router.dispatch(make_cmd(
            "worktree.create",
            Some(&sid),
            json!({
                "worktree_id": "wt-denied",
                "repo_root": repo.display().to_string(),
                "path": worktree.display().to_string(),
                "branch": "lx-denied",
            }),
            None,
        ));

        assert_error_code(&denied, ErrorCode::PermissionDenied);
        assert!(!worktree.exists());
        let conn = router.db.conn();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM worktrees", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_worktree_create_list_delete_git_isolated() {
        let router = setup_router();
        let sid = create_session(&router);
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        let worktree = dir.path().join("wt-active");
        std::fs::create_dir_all(&repo).unwrap();
        if !init_git_repo_for_worktree_test(&repo) {
            eprintln!(
                "skipping worktree git integration test: git unavailable or repo init failed"
            );
            return;
        }

        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({"permission_request_id": "perm-worktree", "tool_name": "git_write"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some(&sid),
            json!({"permission_request_id": "perm-worktree", "decision": "allow"}),
            None,
        )));

        let created = router.dispatch(make_cmd(
            "worktree.create",
            Some(&sid),
            json!({
                "worktree_id": "wt-1",
                "name": "Worktree One",
                "repo_root": repo.display().to_string(),
                "path": worktree.display().to_string(),
                "branch": "lx-worktree-test",
                "base_branch": "HEAD",
            }),
            None,
        ));
        assert_success(&created);
        assert_eq!(created.events[0].event_type, "worktree.created");
        assert!(worktree.join("README.md").exists());

        let listed = router.dispatch(make_cmd("worktree.list", Some(&sid), json!({}), None));
        assert_success(&listed);
        let worktrees = listed.result.unwrap()["worktrees"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(worktrees.len(), 1);
        assert_eq!(worktrees[0]["id"], "wt-1");
        assert_eq!(worktrees[0]["status"], "active");

        let deleted = router.dispatch(make_cmd(
            "worktree.delete",
            Some(&sid),
            json!({"worktree_id": "wt-1"}),
            None,
        ));
        assert_success(&deleted);
        assert_eq!(deleted.events[0].event_type, "worktree.deleted");
        assert!(!worktree.exists());

        let listed_deleted = router.dispatch(make_cmd(
            "worktree.list",
            Some(&sid),
            json!({"include_deleted": true}),
            None,
        ));
        assert_success(&listed_deleted);
        let rows = listed_deleted.result.unwrap()["worktrees"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["status"], "deleted");
    }

    #[test]
    fn test_session_create_duplicate_idempotency_key_no_second_event() {
        let router = setup_router();
        let resp1 = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"workspace": "/tmp/test-ws"}),
            Some("idem-sess-create-001"),
        ));
        assert_success(&resp1);
        assert_eq!(resp1.events.len(), 1);
        assert_eq!(resp1.events[0].seq, 1);

        let sid = resp1.events[0].payload["session_id"]
            .as_str()
            .unwrap()
            .to_string();
        drop(resp1);

        // Second request with same key → cached, no new event
        let resp2 = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"workspace": "/tmp/test-ws"}),
            Some("idem-sess-create-001"),
        ));
        assert_success(&resp2);
        let latest = router.event_log.latest_seq(&sid).unwrap();
        assert_eq!(latest, 1, "No new event in log");
        assert_eq!(
            resp2
                .result
                .as_ref()
                .and_then(|v| v.get("latest_seq"))
                .and_then(|v| v.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn test_dedupe_composite_key_different_methods_not_conflict() {
        let router = setup_router();

        // session.create with key "k1"
        let sid = {
            let r = router.dispatch(make_cmd(
                "session.create",
                None,
                json!({"workspace": "/tmp/a"}),
                Some("k1"),
            ));
            r.events[0].payload["session_id"]
                .as_str()
                .unwrap()
                .to_string()
        };

        // Different method with same key → NOT a dedupe hit. Since "session.input"
        // as a method is invalid without a valid session_id from params,
        // it should fail with Missing session_id / SessionNotFound.
        let r2 = router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "hello"}),
            Some("k1"),
        ));
        assert_success(&r2);
        assert_eq!(r2.events.len(), 1, "Different method must not be deduped");
        assert_eq!(r2.events[0].seq, 2);
    }

    #[test]
    fn test_command_dedupe_prunes_expired_entries_on_write() {
        let router = setup_router();
        router
            .db
            .with_transaction(|tx| {
                tx.execute(
                    "INSERT INTO command_dedupe \
                     (idempotency_key, method, response_json, created_at) \
                     VALUES ('old', 'session.create', '{}', ?1)",
                    params![now_ms() - COMMAND_DEDUPE_TTL_MS - 1],
                )?;
                let response = CommandResponse::ok("req".into(), Some(json!({"ok": true})), None);
                cache_idempotent_in_tx(tx, "fresh", "session.create", &response)?;
                Ok(())
            })
            .unwrap();

        let conn = router.db.conn();
        let old_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM command_dedupe WHERE idempotency_key = 'old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let fresh_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM command_dedupe WHERE idempotency_key = 'fresh'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_count, 0);
        assert_eq!(fresh_count, 1);
    }

    // -----------------------------------------------------------------------
    // session.input
    // -----------------------------------------------------------------------

    fn create_session(router: &CommandRouter) -> String {
        let cmd = make_cmd(
            "session.create",
            None,
            json!({"workspace": "/tmp/test"}),
            None,
        );
        let resp = router.dispatch(cmd);
        resp.events[0].payload["session_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn create_session_with_workspace(
        router: &CommandRouter,
        workspace: &std::path::Path,
    ) -> String {
        let cmd = make_cmd(
            "session.create",
            None,
            json!({"workspace": workspace.display().to_string()}),
            None,
        );
        let resp = router.dispatch(cmd);
        assert_success(&resp);
        resp.events[0].payload["session_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_session_input_appends_event_and_message() {
        let router = setup_router();
        let sid = create_session(&router);

        let cmd = make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "完成调研报告"}),
            None,
        );
        let resp = router.dispatch(cmd);
        assert_success(&resp);
        assert_eq!(resp.events.len(), 1);

        let event = &resp.events[0];
        assert_eq!(event.event_type, "session.input_received");
        assert_eq!(event.seq, 2);
        assert_eq!(event.generation, 1);
        assert_eq!(event.payload["session_id"], sid);
        assert_eq!(event.payload["content"], "完成调研报告");

        let conn = router.db.conn();
        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM leader_conversation WHERE session_id = ?1 AND role = 'user'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_session_input_rejects_terminal_session() {
        let router = setup_router();
        let sid = create_session(&router);

        {
            let conn = router.db.conn();
            conn.execute(
                "UPDATE sessions SET status = 'completed' WHERE id = ?1",
                params![sid],
            )
            .unwrap();
        }

        let cmd = make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "should fail"}),
            None,
        );
        let resp = router.dispatch(cmd);
        assert_error_code(&resp, ErrorCode::SessionAlreadyTerminal);
    }

    #[test]
    fn test_session_input_unknown_session() {
        let router = setup_router();
        let cmd = make_cmd(
            "session.input",
            Some("sess-nonexistent"),
            json!({"content": "hello"}),
            None,
        );
        let resp = router.dispatch(cmd);
        assert_error_code(&resp, ErrorCode::SessionNotFound);
    }

    // -----------------------------------------------------------------------
    // session.snapshot
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_snapshot_returns_correct_state() {
        let router = setup_router();
        let sid = create_session(&router);

        router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "hello"}),
            None,
        ));

        let snap_cmd = make_cmd("session.snapshot", Some(&sid), json!({}), None);
        let resp = router.dispatch(snap_cmd);
        assert_success(&resp);

        let snap: SnapshotEnvelope = serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(snap.session_id, sid);
        assert_eq!(snap.last_seq, 2);
        assert_eq!(snap.generation, 1);
        assert_eq!(snap.status, "active");
    }

    #[test]
    fn test_gs001_session_complete_lifecycle() {
        let router = setup_router();
        let sid = create_session(&router);

        router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "build core"}),
            None,
        ));
        let resp = router.dispatch(make_cmd("session.complete", Some(&sid), json!({}), None));
        assert_success(&resp);
        assert_eq!(resp.events[0].event_type, "session.completed");
        assert_eq!(resp.events[0].seq, 3);

        let snap: SnapshotEnvelope = serde_json::from_value(
            router
                .dispatch(make_cmd("session.snapshot", Some(&sid), json!({}), None))
                .result
                .unwrap(),
        )
        .unwrap();
        assert_eq!(snap.status, "completed");

        let rejected = router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "late input"}),
            None,
        ));
        assert_error_code(&rejected, ErrorCode::SessionAlreadyTerminal);
        assert_eq!(router.event_log.latest_seq(&sid).unwrap(), 3);
    }

    #[test]
    fn test_gs002_session_interrupt_resume() {
        let router = setup_router();
        let sid = create_session(&router);

        let interrupted =
            router.dispatch(make_cmd("session.interrupt", Some(&sid), json!({}), None));
        assert_success(&interrupted);
        assert_eq!(interrupted.events[0].event_type, "session.interrupted");

        let resumed = router.dispatch(make_cmd("session.resume", Some(&sid), json!({}), None));
        assert_success(&resumed);
        assert_eq!(resumed.events[0].event_type, "session.resumed");

        let snap: SnapshotEnvelope = serde_json::from_value(
            router
                .dispatch(make_cmd("session.snapshot", Some(&sid), json!({}), None))
                .result
                .unwrap(),
        )
        .unwrap();
        assert_eq!(snap.status, "active");
    }

    #[test]
    fn test_gs003_session_delete_after_terminal() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd("session.complete", Some(&sid), json!({}), None)));

        let deleted = router.dispatch(make_cmd("session.delete", Some(&sid), json!({}), None));
        assert_success(&deleted);
        assert_eq!(deleted.events[0].event_type, "session.deleted");

        let snap: SnapshotEnvelope = serde_json::from_value(
            router
                .dispatch(make_cmd("session.snapshot", Some(&sid), json!({}), None))
                .result
                .unwrap(),
        )
        .unwrap();
        assert_eq!(snap.status, "deleted");
    }

    #[test]
    fn test_gs004_session_list_recovers_after_reopen() {
        let db_path = std::env::temp_dir().join(format!(
            "lingxiao_gs004_{}.db",
            TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_file(&db_path);

        let sid = {
            let db = DbOwner::open(&db_path).unwrap();
            db.initialize().unwrap();
            let router = CommandRouter::new(db);
            let sid = create_session(&router);
            assert_success(&router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": "recover me"}),
                None,
            )));
            assert_success(&router.dispatch(make_cmd(
                "task.create",
                Some(&sid),
                json!({"task_id": "recover-task", "subject": "Recover"}),
                None,
            )));
            assert_success(&router.dispatch(make_cmd(
                "task.assign",
                Some(&sid),
                json!({"task_id": "recover-task", "agent_id": "agent-r"}),
                None,
            )));
            assert_eq!(router.event_log.latest_seq(&sid).unwrap(), 4);
            sid
        };

        let db = DbOwner::open(&db_path).unwrap();
        db.initialize().unwrap();
        let recovered = CommandRouter::new(db);
        let listed = recovered.dispatch(make_cmd("session.list", None, json!({}), None));
        assert_success(&listed);
        let sessions = listed.result.unwrap()["sessions"]
            .as_array()
            .unwrap()
            .clone();
        let session = sessions
            .iter()
            .find(|item| item["session_id"] == sid)
            .expect("recovered session should be listed");
        assert_eq!(session["status"], "active");
        assert_eq!(session["generation"], 1);
        assert_eq!(session["last_seq"], 4);

        let tasks = recovered.dispatch(make_cmd("task.list", Some(&sid), json!({}), None));
        assert_success(&tasks);
        assert_eq!(tasks.result.unwrap()["tasks"].as_array().unwrap().len(), 1);
        drop(recovered);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
    }

    #[test]
    fn test_gs005_task_lifecycle_create_assign_complete() {
        let router = setup_router();
        let sid = create_session(&router);

        let created = router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-1", "subject": "Implement", "description": "Do it"}),
            None,
        ));
        assert_success(&created);
        assert_eq!(created.events[0].event_type, "task.created");
        assert_eq!(created.events[0].payload["status"], "dispatchable");

        let assigned = router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-1", "agent_id": "agent-a"}),
            None,
        ));
        assert_success(&assigned);
        assert_eq!(assigned.events[0].event_type, "task.assigned");
        assert_eq!(assigned.events[0].payload["run_generation"], 1);

        let completed = router.dispatch(make_cmd(
            "task.complete",
            Some(&sid),
            json!({"task_id": "task-1", "result": {"ok": true}}),
            None,
        ));
        assert_success(&completed);
        assert_eq!(completed.events[0].event_type, "task.completed");

        let conn = router.db.conn();
        let (status, exit_reason, generation): (String, String, i64) = conn
            .query_row(
                "SELECT status, exit_reason, run_generation FROM tasks \
                 WHERE id = 'task-1' AND session_id = ?1",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "terminal");
        assert_eq!(exit_reason, "completed");
        assert_eq!(generation, 1);
    }

    #[test]
    fn test_task_complete_unblocks_blocked_dependents() {
        let router = setup_router();
        let sid = create_session(&router);

        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-root", "subject": "Root"}),
            None,
        )));
        let dependent = router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({
                "task_id": "task-dependent",
                "subject": "Dependent",
                "blocked_by": ["task-root"]
            }),
            None,
        ));
        assert_success(&dependent);
        assert_eq!(dependent.result.as_ref().unwrap()["status"], "blocked");

        let blocked_assign = router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-dependent", "agent_id": "agent-b"}),
            None,
        ));
        assert_error_code(&blocked_assign, ErrorCode::InvalidTransition);

        assert_success(&router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-root", "agent_id": "agent-a"}),
            None,
        )));
        let completed = router.dispatch(make_cmd(
            "task.complete",
            Some(&sid),
            json!({"task_id": "task-root", "result": {"ok": true}}),
            None,
        ));
        assert_success(&completed);
        assert_eq!(completed.events[0].event_type, "task.completed");
        assert_eq!(completed.events[1].event_type, "task.unblocked");
        assert_eq!(completed.events[1].payload["task_id"], "task-dependent");

        let conn = router.db.conn();
        let (status, blocked_by): (String, Option<String>) = conn
            .query_row(
                "SELECT status, blocked_by FROM tasks WHERE session_id = ?1 AND id = 'task-dependent'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "dispatchable");
        assert_eq!(blocked_by, None);
    }

    #[test]
    fn test_gs006_task_redispatch_bumps_generation() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-redispatch", "subject": "Retry"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-redispatch", "agent_id": "agent-a"}),
            None,
        )));

        let redispatched = router.dispatch(make_cmd(
            "task.redispatch",
            Some(&sid),
            json!({"task_id": "task-redispatch", "reason": "agent_crash"}),
            None,
        ));
        assert_success(&redispatched);
        assert_eq!(redispatched.events[0].event_type, "task.redispatched");
        assert_eq!(redispatched.events[0].payload["old_run_generation"], 1);
        assert_eq!(redispatched.events[0].payload["run_generation"], 2);

        let conn = router.db.conn();
        let (status, generation): (String, i64) = conn
            .query_row(
                "SELECT status, run_generation FROM tasks WHERE id = 'task-redispatch' AND session_id = ?1",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "dispatchable");
        assert_eq!(generation, 2);
    }

    #[test]
    fn test_task_assign_routes_by_agent_type_when_agent_missing() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({
                "task_id": "task-routing",
                "subject": "Route this",
                "agent_type": "Code Reviewer"
            }),
            None,
        )));

        let assigned = router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-routing"}),
            None,
        ));
        assert_success(&assigned);
        assert_eq!(
            assigned.events[0].payload["assigned_agent"],
            "code-reviewer-agent"
        );

        let stored: String = router
            .db
            .conn()
            .query_row(
                "SELECT assigned_agent FROM tasks WHERE session_id = ?1 AND id = 'task-routing'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, "code-reviewer-agent");
    }

    #[test]
    fn test_task_assign_prefers_explicit_preferred_agent_name() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({
                "task_id": "task-preferred-agent",
                "subject": "Route preferred",
                "agent_type": "implementation"
            }),
            None,
        )));
        router
            .db
            .conn()
            .execute(
                "UPDATE tasks SET preferred_agent_name = 'agent-specialist' \
                 WHERE session_id = ?1 AND id = 'task-preferred-agent'",
                params![sid],
            )
            .unwrap();

        let assigned = router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-preferred-agent"}),
            None,
        ));
        assert_success(&assigned);
        assert_eq!(
            assigned.events[0].payload["assigned_agent"],
            "agent-specialist"
        );
    }

    #[test]
    fn test_gs007_task_reopen_terminal_bumps_generation() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-reopen", "subject": "Fix"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-reopen"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "task.fail",
            Some(&sid),
            json!({"task_id": "task-reopen", "result": "failed"}),
            None,
        )));

        let reopened = router.dispatch(make_cmd(
            "task.reopen",
            Some(&sid),
            json!({"task_id": "task-reopen", "reason": "user_retry"}),
            None,
        ));
        assert_success(&reopened);
        assert_eq!(reopened.events[0].event_type, "task.reopened");
        assert_eq!(reopened.events[0].payload["run_generation"], 2);

        let conn = router.db.conn();
        let (status, generation, exit_reason): (String, i64, Option<String>) = conn
            .query_row(
                "SELECT status, run_generation, exit_reason FROM tasks WHERE id = 'task-reopen' AND session_id = ?1",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "dispatchable");
        assert_eq!(generation, 2);
        assert_eq!(exit_reason, None);
    }

    #[test]
    fn test_p1_1_task_complete_rejects_stale_generation() {
        let router = setup_router();
        let sid = create_session(&router);

        // Create and assign task
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-gen", "subject": "Test generation gate"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-gen", "assigned_agent": "agent-old"}),
            None,
        )));

        // Redispatch (generation bumps from 1 to 2)
        assert_success(&router.dispatch(make_cmd(
            "task.redispatch",
            Some(&sid),
            json!({"task_id": "task-gen"}),
            None,
        )));

        // Reassign to new agent
        assert_success(&router.dispatch(make_cmd(
            "task.assign",
            Some(&sid),
            json!({"task_id": "task-gen", "assigned_agent": "agent-new"}),
            None,
        )));

        // Old agent tries to complete with stale generation=2 (from first assign)
        let stale_resp = router.dispatch(make_cmd(
            "task.complete",
            Some(&sid),
            json!({
                "task_id": "task-gen",
                "run_generation": 2,
                "result": {"from": "agent-old"}
            }),
            None,
        ));
        assert!(!stale_resp.success);
        assert!(stale_resp.error.is_some());
        let err = stale_resp.error.unwrap();
        assert_eq!(
            err.code,
            lingxiao_core_protocol::error::ErrorCode::InvalidTransition
        );
        assert!(err.message.contains("stale generation"));

        // New agent completes with current generation=3 (second assign)
        let current_resp = router.dispatch(make_cmd(
            "task.complete",
            Some(&sid),
            json!({
                "task_id": "task-gen",
                "run_generation": 3,
                "result": {"from": "agent-new"}
            }),
            None,
        ));
        assert_success(&current_resp);

        // Verify correct result was stored
        let conn = router.db.conn();
        let result: String = conn
            .query_row(
                "SELECT result FROM tasks WHERE id = 'task-gen' AND session_id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        let result_json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result_json["from"], "agent-new");
    }

    #[test]
    fn test_gs011_permission_request_resolve_grants_tool() {
        let router = setup_router();
        let sid = create_session(&router);

        let requested = router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({
                "permission_request_id": "perm-1",
                "tool_name": "file_write",
                "args": {"path": "README.md"}
            }),
            None,
        ));
        assert_success(&requested);
        assert_eq!(requested.events[0].event_type, "permission.request_created");
        assert_eq!(requested.events[0].payload["mode"], "strict");

        let resolved = router.dispatch(make_cmd(
            "permission.resolve",
            Some(&sid),
            json!({"permission_request_id": "perm-1", "decision": "allow"}),
            None,
        ));
        assert_success(&resolved);
        assert_eq!(resolved.events[0].event_type, "permission.request_resolved");

        let conn = router.db.conn();
        let grant_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM permission_grants WHERE session_id = ?1 AND tool_name = 'file_write'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(grant_count, 1);
    }

    #[test]
    fn test_gs012_permission_mode_change_revokes_grants() {
        let router = setup_router();
        let sid = create_session(&router);

        assert_success(&router.dispatch(make_cmd(
            "permission.set_mode",
            Some(&sid),
            json!({"mode": "dev"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({"permission_request_id": "perm-dev", "tool_name": "file_write"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some(&sid),
            json!({"permission_request_id": "perm-dev", "decision": "allow"}),
            None,
        )));

        let strict = router.dispatch(make_cmd(
            "permission.set_mode",
            Some(&sid),
            json!({"mode": "strict"}),
            None,
        ));
        assert_success(&strict);
        assert_eq!(strict.events[0].event_type, "permission.mode_changed");
        assert_eq!(strict.events[1].event_type, "permission.grant_revoked");
        assert_eq!(strict.events[1].payload["tool_name"], "file_write");

        let conn = router.db.conn();
        let grant_count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM permission_grants WHERE session_id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(grant_count, 0);
    }

    #[test]
    fn test_gs013_snapshot_contains_pending_permission_request() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({"permission_request_id": "perm-pending", "tool_name": "shell"}),
            None,
        )));

        let snap: SnapshotEnvelope = serde_json::from_value(
            router
                .dispatch(make_cmd("session.snapshot", Some(&sid), json!({}), None))
                .result
                .unwrap(),
        )
        .unwrap();
        assert_eq!(snap.payload["permission_mode"], "strict");
        let pending = snap.payload["pending_permissions"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0]["permission_request_id"], "perm-pending");
        assert_eq!(pending[0]["tool_name"], "shell");
    }

    #[test]
    fn test_runtime_debug_dump_reports_recovery_state_without_secrets() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "secret conversation fact"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "second secret fact"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "runtime.compact",
            Some(&sid),
            json!({"retain_last": 1}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({
                "permission_request_id": "perm-debug",
                "tool_name": "shell",
                "args": {"command": "secret-token"}
            }),
            None,
        )));
        {
            let conn = router.db.conn();
            conn.execute(
                "INSERT INTO workflows (id, name) VALUES ('wf-debug', 'wf-debug')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workflow_executions \
                 (id, workflow_id, session_id, status, start_time, created_at) \
                 VALUES ('we-debug', 'wf-debug', ?1, 'running', 10, 10)",
                params![sid],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO agent_state \
                 (session_id, agent_id, agent_name, agent_role, task_id, status, stopped, iteration, timestamp) \
                 VALUES (?1, 'agent-debug', 'Agent Debug', 'worker', 'task-debug', 'running', 0, 1, 11)",
                params![sid],
            )
            .unwrap();
        }

        let dump = router.dispatch(make_cmd("runtime.debug_dump", None, json!({}), None));
        assert_success(&dump);
        let result = dump.result.as_ref().unwrap();
        assert_eq!(result["sessions_by_status"]["active"], 1);
        assert_eq!(result["workflows_by_status"]["running"], 1);
        assert_eq!(result["agents_by_status"]["running"], 1);
        assert_eq!(
            result["pending_permissions"][0]["permission_request_id"],
            "perm-debug"
        );
        assert_eq!(
            result["session_context_windows"][0]["original_message_count"],
            2
        );
        assert_eq!(
            result["session_context_windows"][0]["active_message_count"],
            1
        );
        assert_eq!(
            result["session_context_windows"][0]["has_active_context_projection"],
            true
        );
        let serialized = serde_json::to_string(result).unwrap();
        assert!(!serialized.contains("secret conversation fact"));
        assert!(!serialized.contains("second secret fact"));
        assert!(!serialized.contains("secret-token"));
        assert_eq!(result["redaction"]["tool_args"], "omitted");
    }

    #[test]
    fn test_gs008_agent_spawn_start_complete() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "agent-task", "subject": "Run agent"}),
            None,
        )));

        let spawned = router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({
                "agent_id": "agent-1",
                "agent_name": "Builder",
                "agent_role": "impl",
                "task_id": "agent-task"
            }),
            None,
        ));
        assert_success(&spawned);
        assert_eq!(spawned.events[0].event_type, "agent.spawned");

        let started = router.dispatch(make_cmd(
            "agent.start",
            Some(&sid),
            json!({"agent_id": "agent-1"}),
            None,
        ));
        assert_success(&started);
        assert_eq!(started.events[0].event_type, "agent.started");

        let completed = router.dispatch(make_cmd(
            "agent.complete",
            Some(&sid),
            json!({"agent_id": "agent-1", "exit_reason": "completed"}),
            None,
        ));
        assert_success(&completed);
        assert_eq!(completed.events[0].event_type, "agent.completed");

        let conn = router.db.conn();
        let (status, stopped): (String, i64) = conn
            .query_row(
                "SELECT status, stopped FROM agent_state WHERE session_id = ?1 AND agent_id = 'agent-1'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "stopped");
        assert_eq!(stopped, 1);
        let log_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_logs WHERE session_id = ?1 AND agent_id = 'agent-1'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(log_count, 3);
    }

    #[test]
    fn test_agent_spawn_supervised_runs_pool_and_persists_terminal_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("supervised-agent-input.txt");
        fs::write(&path, "daemon supervised agent content").unwrap();

        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolReadThenFinalProvider {
            path: path.display().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));
        let create = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"workspace": dir.path().display().to_string()}),
            None,
        ));
        assert_success(&create);
        let sid = create.events[0].payload["session_id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({
                "task_id": "task-supervised-agent",
                "subject": "Read file through supervised agent",
            }),
            None,
        )));

        let spawned = router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({
                "agent_id": "agent-supervised",
                "agent_name": "Supervised Agent",
                "task_id": "task-supervised-agent",
                "run": true,
                "model": "tool-read/model",
                "task_content": "Read the verification file and report the result",
                "max_rounds": 3,
            }),
            None,
        ));
        assert_success(&spawned);

        for _ in 0..100 {
            let status: String = {
                let conn = router.db.conn();
                conn.query_row(
                    "SELECT status FROM agent_state WHERE session_id = ?1 AND agent_id = 'agent-supervised'",
                    params![sid],
                    |row| row.get(0),
            )
                .unwrap()
            };
            if status == "stopped" {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let conn = router.db.conn();
        let (agent_status, stopped): (String, i64) = conn
            .query_row(
                "SELECT status, stopped FROM agent_state WHERE session_id = ?1 AND agent_id = 'agent-supervised'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(agent_status, "stopped");
        assert_eq!(stopped, 1);

        let (task_status, task_result): (String, Option<String>) = conn
            .query_row(
                "SELECT status, result FROM tasks WHERE session_id = ?1 AND id = 'task-supervised-agent'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(task_status, "completed");
        assert!(
            task_result
                .as_deref()
                .unwrap_or_default()
                .contains("final answer verified from file"),
            "expected persisted task result from AgentLoop"
        );

        let completed_logs: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_logs \
                 WHERE session_id = ?1 AND agent_id = 'agent-supervised' \
                   AND event_type = 'agent.completed'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(completed_logs, 1);
    }

    #[test]
    fn test_agent_spawn_rejects_over_worker_budget_before_state_write() {
        let router = setup_router().with_runtime_manager(RuntimeManager::with_budget(
            crate::runtime::RuntimeBudget {
                max_sidecars: 1,
                max_workers: 1,
                max_memory_mb: 128,
                max_tokens: 100_000,
                max_file_write_bytes: 1024,
                max_tool_concurrency: 8,
            },
        ));
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({"agent_id": "agent-cap-1", "task_id": "task-1"}),
            None,
        )));

        let rejected = router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({"agent_id": "agent-cap-2", "task_id": "task-2"}),
            None,
        ));
        assert_error_code(&rejected, ErrorCode::InvalidTransition);
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .active_workers,
            1
        );
        let conn = router.db.conn();
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_state WHERE session_id = ?1 AND agent_id = 'agent-cap-2'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_agent_complete_releases_worker_budget() {
        let router = setup_router().with_runtime_manager(RuntimeManager::with_budget(
            crate::runtime::RuntimeBudget {
                max_sidecars: 1,
                max_workers: 1,
                max_memory_mb: 128,
                max_tokens: 100_000,
                max_file_write_bytes: 1024,
                max_tool_concurrency: 8,
            },
        ));
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({"agent_id": "agent-release", "task_id": "task-1"}),
            None,
        )));
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .active_workers,
            1
        );
        assert_success(&router.dispatch(make_cmd(
            "agent.complete",
            Some(&sid),
            json!({"agent_id": "agent-release"}),
            None,
        )));
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .active_workers,
            0
        );
    }

    #[test]
    fn test_gs009_agent_crash_respawn() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({"agent_id": "agent-crash", "task_id": "task-x"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "agent.start",
            Some(&sid),
            json!({"agent_id": "agent-crash"}),
            None,
        )));
        let crashed = router.dispatch(make_cmd(
            "agent.crash",
            Some(&sid),
            json!({"agent_id": "agent-crash", "exit_reason": "process_exit"}),
            None,
        ));
        assert_success(&crashed);
        assert_eq!(crashed.events[0].event_type, "agent.crashed");

        let respawned = router.dispatch(make_cmd(
            "agent.respawn",
            Some(&sid),
            json!({"agent_id": "agent-crash", "task_id": "task-x"}),
            None,
        ));
        assert_success(&respawned);
        assert_eq!(respawned.events[0].event_type, "agent.spawned");

        let conn = router.db.conn();
        let (status, stopped, iteration): (String, i64, i64) = conn
            .query_row(
                "SELECT status, stopped, iteration FROM agent_state WHERE session_id = ?1 AND agent_id = 'agent-crash'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "starting");
        assert_eq!(stopped, 0);
        assert!(iteration >= 1);
    }

    #[test]
    fn test_gs010_leader_plan_creates_tasks() {
        let router = setup_router();
        let sid = create_session(&router);
        let planned = router.dispatch(make_cmd(
            "leader.plan",
            Some(&sid),
            json!({
                "tasks": [
                    {"task_id": "plan-1", "subject": "Research"},
                    {"task_id": "plan-2", "subject": "Implement", "description": "Build it"}
                ]
            }),
            Some("leader-plan-1"),
        ));
        assert_success(&planned);
        assert_eq!(planned.events.len(), 2);
        assert_eq!(planned.events[0].event_type, "task.created");
        assert_eq!(planned.events[1].payload["task_id"], "plan-2");

        let conn = router.db.conn();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tasks WHERE session_id = ?1 AND id IN ('plan-1', 'plan-2')",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
        drop(conn);

        let deduped = router.dispatch(make_cmd(
            "leader.plan",
            Some(&sid),
            json!({"tasks": []}),
            Some("leader-plan-1"),
        ));
        assert_success(&deduped);
        assert_eq!(deduped.events.len(), 2);
    }

    #[test]
    fn test_p4_leader_plan_objective_creates_task_without_explicit_tasks() {
        let router = setup_router();
        let sid = create_session(&router);

        let planned = router.dispatch(make_cmd(
            "leader.plan",
            Some(&sid),
            json!({"objective": "Implement the Rust workflow executor. Verify recovery."}),
            None,
        ));

        assert_success(&planned);
        assert_eq!(planned.events.len(), 1);
        assert_eq!(planned.events[0].event_type, "task.created");
        assert_eq!(
            planned.events[0].payload["subject"],
            "Implement the Rust workflow executor"
        );
    }

    #[test]
    fn test_leader_run_calls_native_tool_observes_and_finalizes() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("leader-read.txt");
        fs::write(&file_path, "leader observed evidence").unwrap();
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolReadThenFinalProvider {
            path: file_path.display().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));
        let sid = create_session_with_workspace(&router, dir.path());

        let ran = router.dispatch(make_cmd(
            "leader.run",
            Some(&sid),
            json!({
                "model": "tool-read/model",
                "objective": "Read the file and finalize.",
                "max_rounds": 3
            }),
            None,
        ));
        assert_success(&ran);
        let result = ran.result.unwrap();
        assert_eq!(result["status"], "completed");
        assert_eq!(result["answer"], "final answer verified from file");
        assert!(result["observations"][0]["result"]["content"] == "leader observed evidence");
        assert!(ran
            .events
            .iter()
            .any(|event| event.event_type == "tool.call_completed"));
    }

    #[test]
    fn test_leader_run_accumulates_delta_only_tool_call() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("leader-delta-read.txt");
        fs::write(&file_path, "leader delta observed evidence").unwrap();
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(DeltaToolReadThenFinalProvider {
            path: file_path.display().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));
        let sid = create_session_with_workspace(&router, dir.path());

        let ran = router.dispatch(make_cmd(
            "leader.run",
            Some(&sid),
            json!({
                "model": "delta-tool-read/model",
                "objective": "Read the file from delta-only tool stream and finalize.",
                "max_rounds": 3
            }),
            None,
        ));

        assert_success(&ran);
        let result = ran.result.as_ref().unwrap();
        assert_eq!(result["status"], "completed");
        assert_eq!(result["answer"], "final answer verified from delta tool");
        assert_eq!(
            result["observations"][0]["result"]["content"],
            "leader delta observed evidence"
        );
        assert!(ran.events.iter().any(|event| {
            event.event_type == "tool.call_completed"
                && event.payload["tool_call_id"] == "delta-read-verification-file"
        }));
    }

    #[test]
    fn test_leader_run_requests_permission_for_high_risk_tool_before_side_effect() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("leader-denied.txt");
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(WriteThenFinalProvider {
            path: file_path.display().to_string(),
        }));
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));
        let sid = create_session(&router);

        let ran = router.dispatch(make_cmd(
            "leader.run",
            Some(&sid),
            json!({
                "model": "write/model",
                "objective": "Write the file.",
                "max_rounds": 1
            }),
            None,
        ));
        assert_success(&ran);
        let result = ran.result.unwrap();
        assert_eq!(result["status"], "blocked");
        assert_eq!(result["blocked_by"], "permission");
        assert_eq!(result["permission"]["tool_name"], "file_write");
        assert!(!file_path.exists());
        assert!(ran
            .events
            .iter()
            .any(|event| event.event_type == "permission.request_created"));
    }

    #[test]
    fn test_gs014_workflow_execute_completes_nodes() {
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-1",
                "execution_id": "we-1",
                "nodes": [{"id": "A"}, {"id": "B"}, {"id": "C"}]
            }),
            None,
        ));
        assert_success(&resp);
        let event_types: Vec<_> = resp
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(event_types.first(), Some(&"workflow.execution_started"));
        assert_eq!(event_types.last(), Some(&"workflow.execution_completed"));
        assert_eq!(
            event_types
                .iter()
                .filter(|event_type| **event_type == "workflow.node_started")
                .count(),
            3
        );
        assert_eq!(
            event_types
                .iter()
                .filter(|event_type| **event_type == "workflow.node_completed")
                .count(),
            3
        );

        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM workflow_executions WHERE id = 'we-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn test_p5_workflow_execute_uses_topological_order() {
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-topo",
                "execution_id": "we-topo",
                "nodes": [{"id": "B"}, {"id": "A"}, {"id": "C"}],
                "edges": [{"from": "A", "to": "B"}, {"from": "B", "to": "C"}]
            }),
            None,
        ));
        assert_success(&resp);

        let started: Vec<_> = resp
            .events
            .iter()
            .filter(|event| event.event_type == "workflow.node_started")
            .map(|event| event.payload["node_id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(started, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_p5_workflow_tool_node_executes_native_tool() {
        let router = setup_router();
        let dir = tempfile::tempdir().unwrap();
        let sid = create_session_with_workspace(&router, dir.path());
        let path = dir.path().join(format!(
            "lingxiao_workflow_tool_{}.txt",
            TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::write(&path, "workflow tool node content").unwrap();

        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-tool",
                "execution_id": "we-tool",
                "nodes": [{
                    "id": "read",
                    "type": "tool",
                    "tool_name": "file_read",
                    "args": {"path": path.to_string_lossy()}
                }]
            }),
            None,
        ));
        assert_success(&resp);
        let node_results = resp.result.as_ref().unwrap()["node_results"]
            .as_array()
            .unwrap();
        assert_eq!(node_results[0]["node_type"], "tool");
        assert_eq!(node_results[0]["status"], "completed");
        assert_eq!(
            node_results[0]["output"]["content"],
            "workflow tool node content"
        );
        let completed = resp
            .events
            .iter()
            .find(|event| event.event_type == "workflow.node_completed")
            .unwrap();
        assert_eq!(
            completed.payload["output"]["content"],
            "workflow tool node content"
        );
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_p5_workflow_persists_node_state_and_skips_completed_nodes() {
        let router = setup_router();
        let dir = tempfile::tempdir().unwrap();
        let sid = create_session_with_workspace(&router, dir.path());
        let path = dir.path().join(format!(
            "lingxiao_workflow_state_{}.txt",
            TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        fs::write(&path, "first version").unwrap();

        let params = json!({
            "workflow_id": "wf-state",
            "execution_id": "we-state",
            "nodes": [{
                "id": "read",
                "type": "tool",
                "tool_name": "file_read",
                "args": {"path": path.to_string_lossy()}
            }]
        });
        let first = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            params.clone(),
            None,
        ));
        assert_success(&first);

        fs::write(&path, "second version").unwrap();
        let second = router.dispatch(make_cmd("workflow.execute", Some(&sid), params, None));
        assert_success(&second);
        assert_eq!(
            second.result.as_ref().unwrap()["node_results"][0]["attempt"],
            1
        );
        assert_eq!(
            second.result.as_ref().unwrap()["node_results"][0]["output"]["content"],
            "first version",
            "completed workflow node should be reused instead of re-executed"
        );

        let listed = router.dispatch(make_cmd(
            "workflow.list",
            Some(&sid),
            json!({"recover_running": true}),
            None,
        ));
        assert_success(&listed);
        let node_states = listed.result.as_ref().unwrap()["executions"][0]["node_states"]
            .as_array()
            .unwrap();
        assert_eq!(node_states[0]["node_id"], "read");
        assert_eq!(node_states[0]["status"], "completed");
        assert_eq!(node_states[0]["attempt"], 1);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_p5_workflow_llm_and_agent_nodes_execute() {
        let router = setup_router_with_mock_stream(vec![
            Ok(StreamEvent::TextDelta("workflow answer".into())),
            Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
        ]);
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-llm-agent",
                "execution_id": "we-llm-agent",
                "nodes": [
                    {"id": "llm", "type": "llm", "model": "mock/model", "prompt": "answer"},
                    {"id": "agent", "type": "agent", "model": "mock/model", "task": "finish", "max_rounds": 1}
                ],
                "edges": [{"from": "llm", "to": "agent"}]
            }),
            None,
        ));
        assert_success(&resp);
        assert_eq!(resp.result.as_ref().unwrap()["status"], "completed");
        let node_results = resp.result.as_ref().unwrap()["node_results"]
            .as_array()
            .unwrap();
        assert_eq!(node_results.len(), 2);
        assert_eq!(node_results[0]["node_type"], "llm");
        assert_eq!(node_results[0]["output"]["text"], "workflow answer");
        assert_eq!(node_results[1]["node_type"], "agent");
        assert_eq!(node_results[1]["output"]["answer"], "workflow answer");
    }

    #[test]
    fn test_agent_node_posts_result_back_to_task() {
        let router = setup_router_with_mock_stream(vec![
            Ok(StreamEvent::TextDelta("posted task result".into())),
            Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
        ]);
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "task-agent-result", "subject": "Agent result", "description": "post back"}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-agent-result",
                "execution_id": "we-agent-result",
                "nodes": [{
                    "id": "agent-result",
                    "type": "agent",
                    "agent_id": "agent-result",
                    "task_id": "task-agent-result",
                    "model": "mock/model",
                    "task": "complete task",
                    "max_rounds": 1
                }]
            }),
            None,
        ));
        assert_success(&resp);

        let conn = router.db.conn();
        let (status, result): (String, String) = conn
            .query_row(
                "SELECT status, result FROM tasks WHERE session_id = ?1 AND id = 'task-agent-result'",
                params![sid],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert!(result.contains("posted task result"));
    }

    #[test]
    fn test_agent_node_replays_persisted_context_after_router_restart() {
        let db_path = std::env::temp_dir().join(format!(
            "lingxiao_agent_replay_{}.sqlite",
            TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_file(&db_path);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let provider_one = Arc::new(CapturingReplayProvider {
            calls: AtomicUsize::new(0),
            seen: Arc::clone(&seen),
        });
        let db = DbOwner::open(&db_path).unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(provider_one);
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));
        let sid = create_session(&router);

        let first = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-agent-replay",
                "execution_id": "we-agent-replay-1",
                "nodes": [{
                    "id": "agent-one",
                    "type": "agent",
                    "agent_id": "agent-replay",
                    "agent_name": "Replay Agent",
                    "model": "replay/model",
                    "task": "first persisted task",
                    "max_rounds": 1
                }]
            }),
            None,
        ));
        assert_success(&first);
        drop(router);

        let provider_two = Arc::new(CapturingReplayProvider {
            calls: AtomicUsize::new(1),
            seen: Arc::clone(&seen),
        });
        let reopened = DbOwner::open(&db_path).unwrap();
        reopened.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(provider_two);
        let router = CommandRouter::new(reopened).with_llm_router(LlmRouter::new(registry));

        let second = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-agent-replay",
                "execution_id": "we-agent-replay-2",
                "nodes": [{
                    "id": "agent-two",
                    "type": "agent",
                    "agent_id": "agent-replay",
                    "agent_name": "Replay Agent",
                    "model": "replay/model",
                    "task": "should not replace replay",
                    "max_rounds": 1
                }]
            }),
            None,
        ));
        assert_success(&second);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].len(), 1);
        assert_eq!(seen[0][0].content, "first persisted task");
        assert!(
            seen[1]
                .iter()
                .any(|message| message.content == "first persisted task"),
            "restarted agent request should include persisted user turn"
        );
        assert!(
            seen[1].iter().any(|message| message.content == "answer-0"),
            "restarted agent request should include persisted assistant turn"
        );
        drop(seen);
        let debug = router.dispatch(make_cmd("runtime.debug_dump", None, json!({}), None));
        assert_success(&debug);
        let debug_result = debug.result.unwrap();
        assert_eq!(
            debug_result["agent_context_windows"][0]["agent_id"],
            "agent-replay"
        );
        assert_eq!(
            debug_result["agent_context_windows"][0]["original_message_count"],
            3
        );
        assert_eq!(
            debug_result["agent_context_windows"][0]["has_active_context_projection"],
            true
        );
        assert!(!debug_result.to_string().contains("first persisted task"));
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_headless_user_plan_agent_tool_verify_final_e2e() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("verification.txt");
        fs::write(&file_path, "headless e2e evidence").unwrap();
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolReadThenFinalProvider {
            path: file_path.display().to_string(),
            calls: AtomicUsize::new(0),
        }));
        let router = CommandRouter::new(db).with_llm_router(LlmRouter::new(registry));

        let created = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "headless-e2e", "workspace": dir.path().display().to_string()}),
            None,
        ));
        assert_success(&created);
        let sid = "headless-e2e".to_string();

        let planned = router.dispatch(make_cmd(
            "leader.plan",
            Some(&sid),
            json!({"objective": "Read the verification file and produce a checked final answer"}),
            None,
        ));
        assert_success(&planned);
        assert!(planned
            .events
            .iter()
            .any(|event| event.event_type == "task.created"));

        let executed = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-headless-e2e",
                "execution_id": "we-headless-e2e",
                "nodes": [{
                    "id": "agent-verify",
                    "type": "agent",
                    "agent_id": "agent-headless-e2e",
                    "agent_name": "Headless Agent",
                    "model": "tool-read/model",
                    "task": "Read the verification file and answer only after observing it.",
                    "max_rounds": 3
                }]
            }),
            None,
        ));
        assert_success(&executed);
        let result = executed.result.as_ref().unwrap();
        assert_eq!(result["status"], "completed");
        assert_eq!(
            result["node_results"][0]["output"]["answer"],
            "final answer verified from file"
        );

        let conn = router.db.conn();
        let tool_observation_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM agent_conversation \
                 WHERE session_id = ?1 AND agent_id = 'agent-headless-e2e' \
                   AND role = 'tool' AND content LIKE '%headless e2e evidence%'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tool_observation_count, 1);
    }

    #[test]
    fn test_p5_workflow_retries_retryable_llm_node() {
        let router = setup_router_with_transient_llm_provider();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-retry",
                "execution_id": "we-retry",
                "nodes": [{
                    "id": "llm-retry",
                    "type": "llm",
                    "model": "transient/model",
                    "prompt": "retry",
                    "retry": {"max_attempts": 2, "backoff_ms": 0}
                }]
            }),
            None,
        ));
        assert_success(&resp);
        let node_result = &resp.result.as_ref().unwrap()["node_results"][0];
        assert_eq!(node_result["status"], "completed");
        assert_eq!(node_result["attempt"], 2);
        assert_eq!(node_result["output"]["text"], "retry ok");

        let conn = router.db.conn();
        let (status, attempt): (String, i64) = conn
            .query_row(
                "SELECT status, attempt FROM workflow_node_state \
                 WHERE execution_id = 'we-retry' AND node_id = 'llm-retry'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(attempt, 2);
    }

    #[test]
    fn test_p5_workflow_non_retryable_node_stops_after_one_attempt() {
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-no-retry",
                "execution_id": "we-no-retry",
                "nodes": [{
                    "id": "bad",
                    "type": "unsupported",
                    "retry": {"max_attempts": 3, "backoff_ms": 0}
                }]
            }),
            None,
        ));
        assert_success(&resp);
        assert_eq!(resp.result.as_ref().unwrap()["status"], "failed");
        let node_result = &resp.result.as_ref().unwrap()["node_results"][0];
        assert_eq!(node_result["status"], "failed");
        assert_eq!(node_result["attempt"], 1);

        let conn = router.db.conn();
        let attempt: i64 = conn
            .query_row(
                "SELECT attempt FROM workflow_node_state \
                 WHERE execution_id = 'we-no-retry' AND node_id = 'bad'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempt, 1);
    }

    #[test]
    fn test_p5_workflow_execute_rejects_cycle_before_persisting() {
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some(&sid),
            json!({
                "workflow_id": "wf-cycle",
                "execution_id": "we-cycle",
                "nodes": [{"id": "A"}, {"id": "B"}],
                "edges": [{"from": "A", "to": "B"}, {"from": "B", "to": "A"}]
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::InvalidTransition);
        let conn = router.db.conn();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM workflow_executions WHERE id = 'we-cycle'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_gs015_workflow_pause_resume_cancel() {
        let router = setup_router();
        let sid = create_session(&router);
        {
            let conn = router.db.conn();
            conn.execute(
                "INSERT INTO workflows (id, name) VALUES ('wf-pause', 'wf-pause')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workflow_executions \
                 (id, workflow_id, session_id, status, start_time, created_at) \
                 VALUES ('we-pause', 'wf-pause', ?1, 'running', 1, 1)",
                params![sid],
            )
            .unwrap();
        }

        let paused = router.dispatch(make_cmd(
            "workflow.pause",
            Some(&sid),
            json!({"execution_id": "we-pause"}),
            None,
        ));
        assert_success(&paused);
        assert_eq!(paused.events[0].event_type, "workflow.execution_paused");

        let resumed = router.dispatch(make_cmd(
            "workflow.resume",
            Some(&sid),
            json!({"execution_id": "we-pause"}),
            None,
        ));
        assert_success(&resumed);
        assert_eq!(resumed.events[0].event_type, "workflow.execution_resumed");

        let cancelled = router.dispatch(make_cmd(
            "workflow.cancel",
            Some(&sid),
            json!({"execution_id": "we-pause"}),
            None,
        ));
        assert_success(&cancelled);
        assert_eq!(
            cancelled.events[0].event_type,
            "workflow.execution_cancelled"
        );
        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM workflow_executions WHERE id = 'we-pause'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "cancelled");
    }

    #[test]
    fn test_gs016_gs028_workflow_list_recovers_running_to_paused() {
        let router = setup_router();
        let sid = create_session(&router);
        {
            let conn = router.db.conn();
            conn.execute(
                "INSERT INTO workflows (id, name) VALUES ('wf-recover', 'wf-recover')",
                [],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO workflow_executions \
                 (id, workflow_id, session_id, status, start_time, created_at) \
                 VALUES ('we-recover', 'wf-recover', ?1, 'running', 1, 1)",
                params![sid],
            )
            .unwrap();
        }

        let listed = router.dispatch(make_cmd(
            "workflow.list",
            Some(&sid),
            json!({"recover_running": true}),
            None,
        ));
        assert_success(&listed);
        let executions = listed.result.unwrap()["executions"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(executions.len(), 1);
        assert_eq!(executions[0]["status"], "paused");
    }

    #[test]
    fn test_gs017_native_tool_call_completed() {
        // Write a temp file so file_read actually succeeds
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("gs017_test.txt");
        std::fs::write(&file_path, "hello from gs017").unwrap();

        let router = setup_router();
        let sid = create_session_with_workspace(&router, dir.path());

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-native",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {"path": file_path.display().to_string()},
            }),
            None,
        ));
        assert_success(&resp);
        let event_types: Vec<_> = resp
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            event_types,
            vec!["tool.call_initiated", "tool.call_completed"]
        );

        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM tool_calls WHERE session_id = ?1 AND id = 'tc-native'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn test_unknown_native_tool_rejects_before_fake_completion() {
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-unknown-native",
                "tool_name": "not_registered",
                "tool_type": "native",
                "result": {"forged": true},
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::ToolNotFound);
        let count: i64 = router
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE session_id = ?1 AND id = 'tc-unknown-native'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "unknown native tools must not be persisted");
    }

    #[test]
    fn test_unknown_sidecar_tool_rejects_before_resource_reservation() {
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-unknown-sidecar",
                "tool_name": "browser.missing",
                "tool_type": "sidecar",
                "result": {"forged": true},
                "resource_units": 1,
                "budget_limit": 8
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::ToolNotFound);
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .active_sidecars,
            0
        );
        let count: i64 = router
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE session_id = ?1 AND id = 'tc-unknown-sidecar'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "unknown sidecars must not be persisted");
    }

    #[test]
    fn test_send_message_native_tool_call_completed() {
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-send-message",
                "tool_name": "send_message",
                "tool_type": "native",
                "args": {
                    "recipient": "agent-b",
                    "kind": "handoff",
                    "content": "continue the workflow"
                },
            }),
            None,
        ));

        assert_success(&resp);
        let completed = resp
            .events
            .iter()
            .find(|event| event.event_type == "tool.call_completed")
            .expect("completion event");
        assert_eq!(completed.payload["result"]["sent"], true);
        assert_eq!(completed.payload["result"]["recipient"], "agent-b");
    }

    #[test]
    fn test_p3_file_write_requires_permission_before_side_effect() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("denied.txt");
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-write-denied",
                "tool_name": "file_write",
                "tool_type": "native",
                "args": {"path": file_path.display().to_string(), "content": "must not write"},
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::PermissionDenied);
        assert!(
            !file_path.exists(),
            "permission denial must happen before file_write side effects"
        );
    }

    #[test]
    fn test_p3_file_write_with_permission_grant_completes() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("allowed.txt");
        let router = setup_router();
        let sid = create_session(&router);

        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({
                "permission_request_id": "perm-write",
                "tool_name": "file_write",
                "args": {"path": file_path.display().to_string()}
            }),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some(&sid),
            json!({"permission_request_id": "perm-write", "decision": "allow"}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-write-allowed",
                "tool_name": "file_write",
                "tool_type": "native",
                "args": {"path": file_path.display().to_string(), "content": "allowed"},
            }),
            None,
        ));

        assert_success(&resp);
        assert_eq!(std::fs::read_to_string(&file_path).unwrap(), "allowed");
    }

    #[test]
    fn test_file_write_budget_rejects_before_side_effect() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("budget-denied.txt");
        let router = setup_router().with_runtime_manager(RuntimeManager::with_budget(
            crate::runtime::RuntimeBudget {
                max_sidecars: 1,
                max_workers: 8,
                max_memory_mb: 128,
                max_tokens: 100_000,
                max_file_write_bytes: 4,
                max_tool_concurrency: 8,
            },
        ));
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "permission.request",
            Some(&sid),
            json!({
                "permission_request_id": "perm-write-budget",
                "tool_name": "file_write",
                "args": {"path": file_path.display().to_string()}
            }),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "permission.resolve",
            Some(&sid),
            json!({"permission_request_id": "perm-write-budget", "decision": "allow"}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-write-budget-denied",
                "tool_name": "file_write",
                "tool_type": "native",
                "args": {"path": file_path.display().to_string(), "content": "too large"},
            }),
            None,
        ));

        assert_success(&resp);
        assert!(resp
            .events
            .iter()
            .any(|event| event.event_type == "resource.budget_exceeded"));
        assert!(!file_path.exists());
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .file_write_bytes,
            0
        );
    }

    #[test]
    fn test_native_tool_slot_released_after_failure() {
        let router = setup_router().with_runtime_manager(RuntimeManager::with_budget(
            crate::runtime::RuntimeBudget {
                max_sidecars: 1,
                max_workers: 8,
                max_memory_mb: 128,
                max_tokens: 100_000,
                max_file_write_bytes: 1024,
                max_tool_concurrency: 1,
            },
        ));
        let dir = tempfile::tempdir().unwrap();
        let sid = create_session_with_workspace(&router, dir.path());
        let missing_path = dir.path().join("lingxiao_missing_release.txt");
        let missing = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-read-missing-release",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {"path": missing_path.display().to_string()},
            }),
            None,
        ));
        assert_success(&missing);
        assert_eq!(
            router.runtime_manager.lock().unwrap().usage().active_tools,
            0
        );
    }

    #[test]
    fn test_p3_git_write_requires_git_write_permission() {
        let dir = tempfile::tempdir().unwrap();
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-git-write-denied",
                "tool_name": "git",
                "tool_type": "native",
                "args": {
                    "subcommand": "commit",
                    "args": ["-m", "should-not-run"],
                    "cwd": dir.path().display().to_string()
                },
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::PermissionDenied);
        assert!(resp.error.as_ref().unwrap().message.contains("git_write"));
    }

    #[test]
    fn test_git_read_status_runs_without_git_write_grant() {
        let dir = tempfile::tempdir().unwrap();
        if !run_git_for_test(dir.path(), &["init"]) {
            eprintln!("skipping git read test: git unavailable");
            return;
        }
        let router = setup_router();
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-git-status",
                "tool_name": "git",
                "tool_type": "native",
                "args": {
                    "subcommand": "status",
                    "args": ["--short"],
                    "cwd": dir.path().display().to_string()
                },
            }),
            None,
        ));

        assert_success(&resp);
        assert_eq!(resp.result.as_ref().unwrap()["status"], "completed");
    }

    #[test]
    fn test_git_allowed_write_runs_with_git_write_grant() {
        let dir = tempfile::tempdir().unwrap();
        if !run_git_for_test(dir.path(), &["init"]) {
            eprintln!("skipping git write test: git unavailable");
            return;
        }
        let router = setup_router();
        let sid = create_session(&router);
        grant_tool(&router, &sid, "git_write", "perm-git-write");

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-git-branch-create",
                "tool_name": "git",
                "tool_type": "native",
                "args": {
                    "subcommand": "checkout",
                    "args": ["-b", "feature/test-branch"],
                    "cwd": dir.path().display().to_string()
                },
            }),
            None,
        ));

        assert_success(&resp);
        let output = std::process::Command::new("git")
            .args(["branch", "--show-current"])
            .current_dir(dir.path())
            .output()
            .unwrap();
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "feature/test-branch"
        );
    }

    #[test]
    fn test_gs018_sidecar_timeout_and_cancel_paths() {
        let router = setup_router_with_browser_sidecar();
        let sid = create_session(&router);

        let timeout = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-sidecar-timeout",
                "tool_name": "browser.open",
                "tool_type": "sidecar",
                "scenario": "timeout"
            }),
            None,
        ));
        assert_success(&timeout);
        let timeout_events: Vec<_> = timeout
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            timeout_events,
            vec![
                "tool.call_initiated",
                "resource.sidecar_started",
                "tool.call_timeout",
                "resource.sidecar_cancelled"
            ]
        );
        assert!(
            !timeout_events.contains(&"resource.sidecar_completed"),
            "timeout path must not emit sidecar_completed"
        );

        let cancel = router.dispatch(make_cmd(
            "tool.cancel",
            Some(&sid),
            json!({"tool_call_id": "tc-sidecar-cancel"}),
            None,
        ));
        assert_success(&cancel);
        let cancel_events: Vec<_> = cancel
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            cancel_events,
            vec!["tool.call_cancelled", "resource.sidecar_cancelled"]
        );
    }

    #[test]
    fn test_tool_call_uses_registered_sidecar_process() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let router = CommandRouter::new(db).with_sidecar_command(
            "external_tool",
            SidecarCommand {
                program: powershell(),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-File".into(),
                    write_sidecar_provider().to_string_lossy().to_string(),
                ],
                cwd: None,
            },
        );
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-external",
                "tool_name": "external_tool",
                "tool_type": "sidecar",
                "args": {"value": 7},
                "resource_units": 1,
                "budget_limit": 8
            }),
            None,
        ));

        assert_success(&resp);
        assert_eq!(resp.result.as_ref().unwrap()["status"], "completed");
        assert_eq!(
            resp.events
                .iter()
                .map(|event| event.event_type.as_str())
                .collect::<Vec<_>>(),
            vec![
                "tool.call_initiated",
                "resource.sidecar_started",
                "resource.sidecar_completed",
                "tool.call_completed"
            ]
        );
        assert_eq!(
            resp.events.last().unwrap().payload["result"],
            json!({"external": true, "value": 7})
        );
        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM owned_processes WHERE id = 'sidecar:tc-external'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    #[test]
    fn test_gs019_llm_call_keeps_deltas_realtime_only() {
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({"llm_call_id": "llm-gs019", "model": "mock/model", "prompt": "hello"}),
            None,
        ));
        assert_success(&resp);
        let durable: Vec<_> = resp
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            durable,
            vec![
                "llm.call_started",
                "llm.model_tool_request",
                "llm.call_finished"
            ]
        );
        assert!(
            !durable.contains(&"tool.call_completed"),
            "llm.call must not synthesize tool completion for model-requested tools"
        );
        assert!(
            durable
                .iter()
                .all(|event_type| !event_type.contains("_delta")),
            "stream deltas must not be durable event_log events"
        );

        let realtime = resp.result.as_ref().unwrap()["realtime_events"]
            .as_array()
            .unwrap();
        assert!(realtime
            .iter()
            .any(|event| event["event_type"] == "realtime.llm.text_delta"));
        assert!(realtime
            .iter()
            .any(|event| event["event_type"] == "realtime.llm.thinking_delta"));
        assert!(realtime
            .iter()
            .any(|event| event["event_type"] == "realtime.llm.tool_call_delta"));

        let batch = router.event_log.replay(&sid, 0, 100).unwrap();
        assert!(batch
            .events
            .iter()
            .all(|event| !event.event_type.contains("_delta")));
    }

    #[test]
    fn test_llm_call_uses_injected_external_provider() {
        let router = setup_router_with_llm_provider(write_llm_stream_provider());
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({
                "llm_call_id": "llm-external",
                "model": "external/model",
                "provider": "external",
                "prompt": "hello",
                "auth_context": {
                    "type": "ApiKey",
                    "provider": "external",
                    "key": "sk-router-secret"
                }
            }),
            None,
        ));

        assert_success(&resp);
        let event_types: Vec<_> = resp
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(event_types, vec!["llm.call_started", "llm.call_finished"]);
        let result = resp.result.as_ref().unwrap();
        assert!(result["realtime_events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|event| event["payload"]["text"] == "external text"));

        let conn = router.db.conn();
        let provider: String = conn
            .query_row(
                "SELECT provider FROM llm_gateway_requests WHERE trace_id = 'llm-external'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(provider, "external");
        let health: (i64, i64) = conn
            .query_row(
                "SELECT failure_count, circuit_open FROM provider_health WHERE provider_id = 'external'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(health, (0, 0));
    }

    #[test]
    fn test_llm_call_injects_native_tool_schemas() {
        let seen_tools = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolSchemaCapturingProvider {
            seen_tools: Arc::clone(&seen_tools),
        }));
        let router = setup_router().with_llm_router(LlmRouter::new(registry));
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({
                "llm_call_id": "llm-tool-schema",
                "model": "tool-schema/model",
                "prompt": "hello"
            }),
            None,
        ));

        assert_success(&resp);
        let seen = seen_tools.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].iter().any(|name| name == "file_read"));
        assert!(seen[0].iter().any(|name| name == "send_message"));
        assert!(seen[0].iter().any(|name| name == "attempt_completion"));
    }

    #[test]
    fn test_session_run_task_injects_native_tool_schemas() {
        let seen_tools = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(ToolSchemaCapturingProvider {
            seen_tools: Arc::clone(&seen_tools),
        }));
        let router = setup_router().with_llm_router(LlmRouter::new(registry));

        let resp = router.dispatch(make_cmd(
            "session.run_task",
            None,
            json!({
                "content": "complete this task",
                "workspace": "/tmp/ws",
                "model": "tool-schema/model"
            }),
            None,
        ));

        assert_success(&resp);
        let seen = seen_tools.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].iter().any(|name| name == "file_read"));
        assert!(seen[0].iter().any(|name| name == "send_message"));
        assert!(seen[0].iter().any(|name| name == "attempt_completion"));
    }

    #[test]
    fn test_session_run_task_without_router_rejects_in_production_mode() {
        let router = setup_router().without_mock_llm_fallback();
        let resp = router.dispatch(make_cmd(
            "session.run_task",
            None,
            json!({
                "content": "must not use mock fallback",
                "workspace": "/tmp/ws",
                "model": "mock/model"
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::Internal);
        assert!(
            resp.error.as_ref().unwrap().details.as_ref().unwrap()["provider_error"]["message"]
                .as_str()
                .unwrap()
                .contains("production mock fallback is disabled")
        );
    }

    #[test]
    fn test_gs029_token_budget_exceeded_triggers_compaction_events() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "budget fact A"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "budget fact B"}),
            None,
        )));
        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({"llm_call_id": "llm-budget", "model": "mock/model", "token_budget": 10}),
            None,
        ));
        assert_success(&resp);
        let event_types: Vec<_> = resp
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(event_types.contains(&"resource.budget_exceeded"));
        assert!(event_types.contains(&"persistence.compaction_started"));
        assert!(event_types.contains(&"persistence.compaction_completed"));

        let conn = router.db.conn();
        let summary: Option<String> = conn
            .query_row(
                "SELECT summary FROM sessions WHERE id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(summary.unwrap().contains("compacted after llm-budget"));
        drop(conn);

        let listed = router.dispatch(make_cmd("conv.list", Some(&sid), json!({}), None));
        assert_success(&listed);
        let listed_result = listed.result.unwrap();
        assert_eq!(listed_result["active_context"]["original_message_count"], 2);
        assert_eq!(listed_result["active_context"]["active_message_count"], 2);
    }

    #[test]
    fn test_llm_token_preflight_rejects_before_provider_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(CountingProvider {
            calls: calls.clone(),
        }));
        let router = setup_router()
            .with_llm_router(LlmRouter::new(registry))
            .with_runtime_manager(RuntimeManager::with_budget(crate::runtime::RuntimeBudget {
                max_sidecars: 1,
                max_workers: 8,
                max_memory_mb: 128,
                max_tokens: 2,
                max_file_write_bytes: 1024,
                max_tool_concurrency: 8,
            }));
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({
                "llm_call_id": "llm-preflight-denied",
                "model": "counting/model",
                "prompt": "this prompt is definitely too large for two tokens"
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::InvalidTransition);
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        let count: i64 = router
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM llm_gateway_requests WHERE trace_id = 'llm-preflight-denied'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_llm_agent_token_preflight_rejects_before_provider_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(CountingProvider {
            calls: calls.clone(),
        }));
        let router = setup_router().with_llm_router(LlmRouter::new(registry));
        let sid = create_session(&router);

        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({
                "llm_call_id": "llm-agent-preflight-denied",
                "model": "counting/model",
                "agent_id": "agent-budgeted",
                "agent_token_budget": 2,
                "prompt": "this agent prompt is too large for two tokens"
            }),
            None,
        ));

        assert_error_code(&resp, ErrorCode::InvalidTransition);
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 0);
        let count: i64 = router
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM llm_gateway_requests WHERE trace_id = 'llm-agent-preflight-denied'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_gs030_sidecar_resource_accounting() {
        let router = setup_router_with_browser_sidecar();
        let sid = create_session(&router);

        let completed = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-budget-ok",
                "tool_name": "browser.open",
                "tool_type": "sidecar",
                "args": {"value": 1},
                "resource_units": 2,
                "budget_limit": 4
            }),
            None,
        ));
        assert_success(&completed);
        let complete_events: Vec<_> = completed
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(complete_events.contains(&"resource.sidecar_started"));
        assert!(complete_events.contains(&"resource.sidecar_completed"));

        let rejected = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-budget-reject",
                "tool_name": "browser.open",
                "tool_type": "sidecar",
                "resource_units": 9,
                "budget_limit": 4
            }),
            None,
        ));
        assert_success(&rejected);
        let rejected_events: Vec<_> = rejected
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(rejected_events.contains(&"resource.budget_exceeded"));
        assert!(
            !rejected_events.contains(&"resource.sidecar_started"),
            "budget rejection should happen before sidecar start"
        );
    }

    #[test]
    fn test_runtime_manager_rejects_sidecar_before_start() {
        let router = setup_router_with_browser_sidecar().with_runtime_manager(
            RuntimeManager::with_budget(crate::runtime::RuntimeBudget {
                max_sidecars: 0,
                max_workers: 8,
                max_memory_mb: 512,
                max_tokens: 100_000,
                max_file_write_bytes: 1024,
                max_tool_concurrency: 32,
            }),
        );
        let sid = create_session(&router);

        let rejected = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-runtime-denied",
                "tool_name": "browser.open",
                "tool_type": "sidecar",
                "resource_units": 1,
                "budget_limit": 8
            }),
            None,
        ));
        assert_success(&rejected);
        let event_types: Vec<_> = rejected
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert!(event_types.contains(&"resource.budget_exceeded"));
        assert!(!event_types.contains(&"resource.sidecar_started"));
        assert_eq!(
            router
                .runtime_manager
                .lock()
                .unwrap()
                .usage()
                .active_sidecars,
            0
        );
    }

    #[test]
    fn test_gs035_blackboard_intent_lifecycle() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "blackboard.intent.create",
            Some(&sid),
            json!({"intent_id": "intent-1", "title": "Find API", "content": "locate docs"}),
            None,
        )));
        let claimed = router.dispatch(make_cmd(
            "blackboard.intent.claim",
            Some(&sid),
            json!({"intent_id": "intent-1", "agent": "explore-1"}),
            None,
        ));
        assert_success(&claimed);
        assert_eq!(claimed.events[0].event_type, "blackboard.intent_claimed");
        let resolved = router.dispatch(make_cmd(
            "blackboard.intent.resolve",
            Some(&sid),
            json!({"intent_id": "intent-1", "result": "found 3 files"}),
            None,
        ));
        assert_success(&resolved);
        assert_eq!(resolved.events[0].event_type, "blackboard.intent_resolved");

        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT intent_status FROM graph_nodes WHERE session_id = ?1 AND id = 'intent-1'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "resolved");
    }

    #[test]
    fn test_graph_query_filters_nodes_and_returns_edges() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "blackboard.intent.create",
            Some(&sid),
            json!({"intent_id": "intent-query", "title": "Query me", "content": "graph content"}),
            None,
        )));
        {
            let conn = router.db.conn();
            conn.execute(
                "INSERT INTO graph_nodes \
                 (id, session_id, kind, title, content, created_by, created_at) \
                 VALUES ('note-query', ?1, 'note', 'Note', 'note content', 'test', 2)",
                params![sid],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO graph_edges \
                 (id, session_id, from_node_id, to_node_id, edge_type, created_at, created_by, metadata) \
                 VALUES ('edge-query', ?1, 'intent-query', 'note-query', 'supports', 3, 'test', '{\"weight\":1}')",
                params![sid],
            )
            .unwrap();
        }

        let queried = router.dispatch(make_cmd(
            "graph.query",
            Some(&sid),
            json!({"kind": "intent", "intent_status": "open"}),
            None,
        ));
        assert_success(&queried);
        let result = queried.result.as_ref().unwrap();
        assert_eq!(result["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(result["nodes"][0]["id"], "intent-query");
        assert_eq!(result["edges"].as_array().unwrap().len(), 1);
        assert_eq!(result["edges"][0]["edge_type"], "supports");
        assert_eq!(result["edges"][0]["metadata"]["weight"], 1);
    }

    #[test]
    fn test_assumption_create_list_and_verify() {
        let router = setup_router();
        let sid = create_session(&router);
        let created = router.dispatch(make_cmd(
            "assumption.create",
            Some(&sid),
            json!({
                "assumption_id": "assume-1",
                "title": "API is reachable",
                "content": "The provider endpoint accepts requests",
                "verification_type": "http",
                "verification_target": "https://example.test",
                "verification_expected": "200",
                "dependents": ["task-1"]
            }),
            None,
        ));
        assert_success(&created);
        assert_eq!(created.events[0].event_type, "assumption.created");

        let listed = router.dispatch(make_cmd(
            "assumption.list",
            Some(&sid),
            json!({"status": "unverified"}),
            None,
        ));
        assert_success(&listed);
        let assumptions = listed.result.unwrap()["assumptions"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(assumptions.len(), 1);
        assert_eq!(assumptions[0]["id"], "assume-1");
        assert_eq!(assumptions[0]["dependents"][0], "task-1");

        let verified = router.dispatch(make_cmd(
            "assumption.verify",
            Some(&sid),
            json!({
                "assumption_id": "assume-1",
                "actual": "200",
                "evidence": {"source": "mock"}
            }),
            None,
        ));
        assert_success(&verified);
        assert_eq!(verified.events[0].event_type, "assumption.verified");

        let listed = router.dispatch(make_cmd(
            "assumption.list",
            Some(&sid),
            json!({"status": "verified"}),
            None,
        ));
        assert_success(&listed);
        let assumptions = listed.result.unwrap()["assumptions"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(assumptions.len(), 1);
        assert_eq!(assumptions[0]["verification_actual"], "200");
        assert_eq!(assumptions[0]["evidence"]["source"], "mock");
    }

    #[test]
    fn test_assumption_falsify_sets_terminal_status() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "assumption.create",
            Some(&sid),
            json!({
                "assumption_id": "assume-false",
                "title": "File exists",
                "verification_target": "workspace://missing",
                "verification_expected": "exists"
            }),
            None,
        )));

        let falsified = router.dispatch(make_cmd(
            "assumption.falsify",
            Some(&sid),
            json!({"assumption_id": "assume-false", "actual": "missing"}),
            None,
        ));
        assert_success(&falsified);
        assert_eq!(falsified.events[0].event_type, "assumption.falsified");

        let conn = router.db.conn();
        let status: String = conn
            .query_row(
                "SELECT status FROM assumptions WHERE id = 'assume-false'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "falsified");
    }

    #[test]
    fn test_gs036_team_mailbox_send_and_read() {
        let router = setup_router();
        let sid = create_session(&router);
        let sent = router.dispatch(make_cmd(
            "team.send",
            Some(&sid),
            json!({
                "message_id": "team-msg-1",
                "from": "alice",
                "to_team": "research",
                "content": "please inspect",
                "urgency": "high"
            }),
            None,
        ));
        assert_success(&sent);
        assert_eq!(sent.events[0].event_type, "team.message_sent");
        assert_success(&router.dispatch(make_cmd(
            "team.send",
            Some(&sid),
            json!({
                "message_id": "team-msg-other",
                "from": "alice",
                "to_team": "implementation",
                "to_member": "carol",
                "content": "not for research",
                "urgency": "critical"
            }),
            None,
        )));

        let polled = router.dispatch(make_cmd(
            "team.poll",
            Some(&sid),
            json!({"to_team": "research", "member": "bob", "mark_read": true}),
            None,
        ));
        assert_success(&polled);
        assert_eq!(polled.events[0].event_type, "team.mailbox_polled");
        let messages = polled.result.as_ref().unwrap()["messages"]
            .as_array()
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["message_id"], "team-msg-1");

        let drained = router.dispatch(make_cmd(
            "team.poll",
            Some(&sid),
            json!({"to_team": "research", "member": "bob"}),
            None,
        ));
        assert_success(&drained);
        assert_eq!(
            drained.result.as_ref().unwrap()["messages"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let read = router.dispatch(make_cmd(
            "team.mark_read",
            Some(&sid),
            json!({"message_id": "team-msg-1", "reader": "bob"}),
            None,
        ));
        assert_success(&read);
        assert_eq!(read.events[0].event_type, "team.message_read");

        let conn = router.db.conn();
        let read_by: String = conn
            .query_row(
                "SELECT read_by FROM team_messages WHERE session_id = ?1 AND id = 'team-msg-1'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(read_by.contains("bob"));
    }

    #[test]
    fn test_gs031_command_router_bus_priority_delivery() {
        let router = setup_router();
        for (priority, content) in [("p3", "low"), ("p0", "urgent"), ("p1", "normal")] {
            let resp = router.dispatch(make_cmd(
                "bus.publish",
                None,
                json!({"from": "leader", "to": "agent", "priority": priority, "content": content}),
                None,
            ));
            assert_success(&resp);
            assert_eq!(resp.result.unwrap()["status"], "queued");
        }

        let first = router.dispatch(make_cmd("bus.pop", None, json!({}), None));
        assert_success(&first);
        assert_eq!(first.result.unwrap()["message"]["content"], "urgent");

        let second = router.dispatch(make_cmd("bus.pop", None, json!({}), None));
        assert_success(&second);
        assert_eq!(second.result.unwrap()["message"]["content"], "normal");

        let third = router.dispatch(make_cmd("bus.pop", None, json!({}), None));
        assert_success(&third);
        assert_eq!(third.result.unwrap()["message"]["content"], "low");
    }

    #[test]
    fn test_gs032_command_router_bus_backpressure_dead_letters() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let router = CommandRouter::new(db).with_message_bus_capacity(1);

        let first = router.dispatch(make_cmd(
            "bus.publish",
            None,
            json!({"from": "leader", "to": "agent", "priority": "p2", "content": "first"}),
            None,
        ));
        assert_success(&first);

        let overflow = router.dispatch(make_cmd(
            "bus.publish",
            None,
            json!({"from": "leader", "to": "agent", "priority": "p0", "content": "overflow"}),
            None,
        ));
        assert_success(&overflow);
        let overflow_result = overflow.result.unwrap();
        assert_eq!(overflow_result["status"], "dead_lettered");
        assert_eq!(overflow_result["reason"], "backpressure_capacity_exceeded");

        let dead = router.dispatch(make_cmd("bus.dead_letters", None, json!({}), None));
        assert_success(&dead);
        let dead_result = dead.result.unwrap();
        assert_eq!(dead_result["dead_letter_count"], 1);
        assert_eq!(
            dead_result["dead_letters"][0]["message"]["content"],
            "overflow"
        );
    }

    #[test]
    fn test_schedule_create_list_and_fire_due_creates_task() {
        let router = setup_router();
        let sid = create_session(&router);
        let created = router.dispatch(make_cmd(
            "schedule.create",
            Some(&sid),
            json!({
                "schedule_id": "sched-1",
                "cron": "every 5m",
                "prompt": "inspect scheduled work",
                "next_run_at": 0.0
            }),
            None,
        ));
        assert_success(&created);
        assert_eq!(created.events[0].event_type, "schedule.created");

        let listed = router.dispatch(make_cmd("schedule.list", Some(&sid), json!({}), None));
        assert_success(&listed);
        let schedules = listed.result.unwrap()["schedules"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0]["id"], "sched-1");

        let fired = router.dispatch(make_cmd("schedule.fire_due", Some(&sid), json!({}), None));
        assert_success(&fired);
        assert_eq!(fired.events[0].event_type, "schedule.fired");
        let fired_result = fired.result.unwrap();
        assert_eq!(fired_result["fired_count"], 1);
        let task_id = fired_result["fired"][0]["task_id"].as_str().unwrap();

        let conn = router.db.conn();
        let task_status: String = conn
            .query_row(
                "SELECT status FROM tasks WHERE session_id = ?1 AND id = ?2",
                params![sid, task_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(task_status, "dispatchable");
    }

    #[test]
    fn test_schedule_manual_fire_non_recurring_disables_schedule() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "schedule.create",
            Some(&sid),
            json!({
                "schedule_id": "sched-once",
                "cron": "@once",
                "prompt": "one shot",
                "recurring": false
            }),
            None,
        )));

        let fired = router.dispatch(make_cmd(
            "schedule.fire",
            Some(&sid),
            json!({"schedule_id": "sched-once"}),
            None,
        ));
        assert_success(&fired);
        assert_eq!(fired.result.unwrap()["fired_count"], 1);

        let conn = router.db.conn();
        let enabled: i64 = conn
            .query_row(
                "SELECT enabled FROM scheduled_tasks WHERE id = 'sched-once'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(enabled, 0);
    }

    #[test]
    fn test_memory_upsert_indexes_fts_and_search_filters_scope() {
        let router = setup_router();
        let sid = create_session(&router);
        let upserted = router.dispatch(make_cmd(
            "memory.upsert",
            Some(&sid),
            json!({
                "id": "mem-alpha",
                "path": "workspace://notes/alpha",
                "scope": "workspace",
                "scope_id": "ws-1",
                "type": "note",
                "body": "alpha release plan keeps durable facts"
            }),
            Some("memory-upsert-alpha"),
        ));
        assert_success(&upserted);
        assert_eq!(upserted.events[0].event_type, "memory.entry_upserted");
        assert!(
            upserted.events[0].payload.get("body").is_none(),
            "upsert event must not copy memory body into the durable audit payload"
        );

        let conn = router.db.conn();
        let indexed_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM memory_fts WHERE path = ?1 AND memory_fts MATCH ?2",
                params!["workspace://notes/alpha", "\"alpha\""],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexed_count, 1);
        drop(conn);

        let search = router.dispatch(make_cmd(
            "memory.search",
            Some(&sid),
            json!({"query": "alpha durable", "scope": "workspace", "scope_id": "ws-1", "type": "note"}),
            None,
        ));
        assert_success(&search);
        assert_eq!(search.events[0].event_type, "memory.search_completed");
        let results = search.result.as_ref().unwrap()["results"]
            .as_array()
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["path"], "workspace://notes/alpha");
        assert_eq!(results[0]["body"], "alpha release plan keeps durable facts");

        let filtered = router.dispatch(make_cmd(
            "memory.search",
            Some(&sid),
            json!({"query": "alpha", "scope": "workspace", "scope_id": "other"}),
            None,
        ));
        assert_success(&filtered);
        assert!(filtered.result.unwrap()["results"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_embedding_upsert_search_orders_by_similarity_and_skips_dimension_mismatch() {
        let router = setup_router();
        let sid = create_session(&router);
        for (path, embedding) in [
            ("workspace://mem/near", json!([1.0, 0.0, 0.0])),
            ("workspace://mem/far", json!([0.0, 1.0, 0.0])),
            ("workspace://mem/mismatch", json!([1.0, 0.0])),
        ] {
            let resp = router.dispatch(make_cmd(
                "embedding.upsert",
                Some(&sid),
                json!({"path": path, "model": "test-embed", "embedding": embedding}),
                None,
            ));
            assert_success(&resp);
            assert_eq!(resp.events[0].event_type, "memory.embedding_upserted");
        }

        let search = router.dispatch(make_cmd(
            "embedding.search",
            Some(&sid),
            json!({"model": "test-embed", "embedding": [0.9, 0.1, 0.0], "limit": 5}),
            None,
        ));
        assert_success(&search);
        assert_eq!(
            search.events[0].event_type,
            "memory.embedding_search_completed"
        );
        let results = search.result.as_ref().unwrap()["results"]
            .as_array()
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["path"], "workspace://mem/near");
        assert_eq!(results[1]["path"], "workspace://mem/far");
        assert!(
            results[0]["score"].as_f64().unwrap() > results[1]["score"].as_f64().unwrap(),
            "nearest embedding should have the highest cosine score"
        );
    }

    #[test]
    fn test_gs027_gs033_conv_list_survives_reopen() {
        let db_path = std::env::temp_dir().join(format!(
            "lingxiao_gs027_{}.db",
            TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_file(&db_path);

        let sid = {
            let db = DbOwner::open(&db_path).unwrap();
            db.initialize().unwrap();
            let router = CommandRouter::new(db);
            let sid = create_session(&router);
            assert_success(&router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": "first durable message"}),
                None,
            )));
            assert_success(&router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": "second durable message"}),
                None,
            )));
            sid
        };

        let db = DbOwner::open(&db_path).unwrap();
        db.initialize().unwrap();
        let recovered = CommandRouter::new(db);
        let listed = recovered.dispatch(make_cmd("conv.list", Some(&sid), json!({}), None));
        assert_success(&listed);
        let messages = listed.result.unwrap()["messages"]
            .as_array()
            .unwrap()
            .clone();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["content"], "first durable message");
        assert_eq!(messages[1]["content"], "second durable message");
        drop(recovered);
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(db_path.with_extension("db-wal"));
        let _ = std::fs::remove_file(db_path.with_extension("db-shm"));
    }

    #[test]
    fn test_gs034_runtime_compact_keeps_original_conversation_rows() {
        let router = setup_router();
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "fact A"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "fact B"}),
            None,
        )));

        let compacted = router.dispatch(make_cmd(
            "runtime.compact",
            Some(&sid),
            json!({"retain_last": 1}),
            None,
        ));
        assert_success(&compacted);
        let event_types: Vec<_> = compacted
            .events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect();
        assert_eq!(
            event_types,
            vec![
                "persistence.compaction_started",
                "persistence.compaction_completed"
            ]
        );

        let listed = router.dispatch(make_cmd("conv.list", Some(&sid), json!({}), None));
        assert_success(&listed);
        let listed_result = listed.result.unwrap();
        let messages = listed_result["messages"].as_array().unwrap().clone();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["content"], "fact A");
        assert_eq!(messages[1]["content"], "fact B");
        assert_eq!(listed_result["active_context"]["original_message_count"], 2);
        assert_eq!(listed_result["active_context"]["active_message_count"], 1);
        assert!(listed_result["active_context"]["summary"]
            .as_str()
            .unwrap()
            .contains("fact A"));
        assert_eq!(
            listed_result["active_context"]["active_messages"][0]["content"],
            "fact B"
        );

        let snapshot = router.dispatch(make_cmd("session.snapshot", Some(&sid), json!({}), None));
        assert_success(&snapshot);
        let snap: SnapshotEnvelope = serde_json::from_value(snapshot.result.unwrap()).unwrap();
        assert_eq!(snap.payload["active_context"]["active_message_count"], 1);
        assert_eq!(
            snap.payload["active_context"]["active_messages"][0]["content"],
            "fact B"
        );

        let replay = router.dispatch(make_cmd(
            "event.replay",
            Some(&sid),
            json!({"from_seq": 0, "limit": 100}),
            None,
        ));
        assert_success(&replay);
        let event_types: Vec<_> = replay
            .events
            .iter()
            .map(|event| event.event_type.clone())
            .collect();
        assert!(event_types.contains(&"persistence.compaction_started".to_string()));
        assert!(event_types.contains(&"persistence.compaction_completed".to_string()));
    }

    #[test]
    fn test_runtime_compact_uses_llm_summary_when_router_configured() {
        let router = setup_router_with_mock_stream(vec![
            Ok(StreamEvent::TextDelta(
                "LLM summary preserves fact A and decision B".into(),
            )),
            Ok(StreamEvent::Finished(crate::llm::FinishReason::Stop)),
        ]);
        let sid = create_session(&router);
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "fact A"}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some(&sid),
            json!({"content": "decision B"}),
            None,
        )));

        let compacted = router.dispatch(make_cmd(
            "runtime.compact",
            Some(&sid),
            json!({"retain_last": 1, "model": "mock/model"}),
            None,
        ));
        assert_success(&compacted);
        let summary = compacted.result.as_ref().unwrap()["active_context"]["summary"]
            .as_str()
            .unwrap();
        assert_eq!(summary, "LLM summary preserves fact A and decision B");

        let original_count: i64 = router
            .db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM leader_conversation WHERE session_id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(original_count, 2);
    }

    // -----------------------------------------------------------------------
    // session.connect / event.replay
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_connect_returns_delta() {
        let router = setup_router();
        let sid = create_session(&router);

        for content in &["msg1", "msg2", "msg3"] {
            router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": content}),
                None,
            ));
        }

        let connect_cmd = make_cmd(
            "session.connect",
            Some(&sid),
            json!({"cursor": {"last_known_seq": 2}}),
            None,
        );
        let resp = router.dispatch(connect_cmd);
        assert_success(&resp);

        assert_eq!(resp.result.as_ref().unwrap()["delta_type"], "events");
        assert_eq!(resp.events.len(), 2, "seq=3 and seq=4");
        assert_eq!(resp.events[0].seq, 3);
        assert_eq!(resp.events[1].seq, 4);
    }

    #[test]
    fn test_event_replay_returns_filtered_events() {
        let router = setup_router();
        let sid = create_session(&router);

        for content in &["a", "b", "c"] {
            router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": content}),
                None,
            ));
        }

        let replay_cmd = make_cmd(
            "event.replay",
            Some(&sid),
            json!({"from_seq": 1, "limit": 2}),
            None,
        );
        let resp = router.dispatch(replay_cmd);
        assert_success(&resp);

        assert_eq!(resp.events.len(), 2);
        assert_eq!(resp.events[0].seq, 2);
        assert_eq!(resp.events[1].seq, 3);
        assert_eq!(resp.result.as_ref().unwrap()["has_more"], true);
    }

    #[test]
    fn test_event_compact_prunes_rows_and_sets_snapshot_boundary() {
        let router = setup_router();
        let sid = create_session(&router);
        for content in &["first", "second"] {
            assert_success(&router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": content}),
                None,
            )));
        }

        let compacted = router.dispatch(make_cmd(
            "event.compact",
            Some(&sid),
            json!({"compact_through_seq": 2}),
            None,
        ));
        assert_success(&compacted);
        assert_eq!(compacted.events[0].event_type, "event_log.compacted");
        assert_eq!(compacted.result.as_ref().unwrap()["compacted_seq"], 2);

        {
            let conn = router.db.conn();
            let old_rows: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM event_log WHERE session_id = ?1 AND seq <= 2",
                    params![sid],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(old_rows, 0);
            let compacted_seq: i64 = conn
                .query_row(
                    "SELECT compacted_seq FROM event_log_meta WHERE session_id = ?1",
                    params![sid],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(compacted_seq, 2);
        }

        let connected = router.dispatch(make_cmd(
            "session.connect",
            Some(&sid),
            json!({"cursor": {"last_known_seq": 1}}),
            None,
        ));
        assert_success(&connected);
        assert_eq!(connected.result.unwrap()["delta_type"], "snapshot_required");
    }

    #[test]
    fn test_retention_sweep_prunes_high_growth_tables() {
        let router = setup_router();
        let sid = create_session(&router);
        for content in &["one", "two", "three"] {
            assert_success(&router.dispatch(make_cmd(
                "session.input",
                Some(&sid),
                json!({"content": content}),
                None,
            )));
        }

        {
            let conn = router.db.conn();
            for i in 0..3 {
                conn.execute(
                    "INSERT INTO tool_calls \
                     (id, session_id, tool_name, tool_type, status, args_json, started_at) \
                     VALUES (?1, ?2, 'file_read', 'native', 'completed', '{}', ?3)",
                    params![format!("tc-ret-{i}"), sid, i],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO token_usage \
                     (session_id, agent_id, agent_name, model_name, prompt_tokens, completion_tokens, total_tokens, timestamp) \
                     VALUES (?1, 'agent', 'agent', 'model', 1, 1, 2, ?2)",
                    params![sid, i as f64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO llm_gateway_requests \
                     (trace_id, session_id, status, created_at) VALUES (?1, ?2, 'completed', ?3)",
                    params![format!("trace-ret-{i}"), sid, i as f64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO agent_conversation \
                     (session_id, agent_id, agent_name, role, content, timestamp) \
                     VALUES (?1, 'agent', 'agent', 'assistant', 'body', ?2)",
                    params![sid, i as f64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO leader_conversation (session_id, role, content, timestamp) \
                     VALUES (?1, 'user', 'body', ?2)",
                    params![sid, i as f64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO traces (trace_id, span_id, operation, start_ts, session_id) \
                     VALUES (?1, ?2, 'op', ?3, ?4)",
                    params![format!("trace-{i}"), format!("span-{i}"), i, sid],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO execution_trace_events \
                     (id, project_root, session_id, status, duration_ms, files_changed, created_at) \
                     VALUES (?1, 'root', ?2, 'ok', 0, '[]', ?3)",
                    params![format!("exec-{i}"), sid, i as f64],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO team_messages \
                     (id, from_team, to_team, content, session_id, timestamp) \
                     VALUES (?1, 'from', 'to', 'body', ?2, ?3)",
                    params![format!("team-{i}"), sid, i as f64],
                )
                .unwrap();
            }
        }

        let swept = router.dispatch(make_cmd(
            "retention.sweep",
            None,
            json!({"retain_per_session": 1}),
            None,
        ));
        assert_success(&swept);

        let conn = router.db.conn();
        for table in [
            "tool_calls",
            "token_usage",
            "llm_gateway_requests",
            "agent_conversation",
            "leader_conversation",
            "traces",
            "execution_trace_events",
            "team_messages",
        ] {
            let count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {table} WHERE session_id = ?1"),
                    params![sid],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "{table} should retain only the newest row");
        }
        let event_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM event_log WHERE session_id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);
        let compacted_seq: i64 = conn
            .query_row(
                "SELECT compacted_seq FROM event_log_meta WHERE session_id = ?1",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert!(compacted_seq >= 3);
    }

    #[test]
    fn test_session_run_task_executes_provider_tool_call_before_final_answer() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("task-evidence.txt");
        fs::write(&file_path, "provider requested evidence").unwrap();
        let sid = "sess_run_task_tool_loop";
        let router = setup_router_with_task_tool_provider(file_path.display().to_string());

        let response = router.dispatch(make_cmd(
            "session.run_task",
            Some(sid),
            json!({
                "content": "read the evidence before finalizing",
                "task_id": "task-tool-loop",
                "workspace": dir.path().display().to_string(),
                "model": "task-tool/model",
                "provider": "task_tool",
            }),
            None,
        ));
        assert_success(&response);
        assert_eq!(response.result.as_ref().unwrap()["status"], "completed");
        assert_eq!(
            response.result.as_ref().unwrap()["answer"],
            "TASK_TOOL_DONE after canonical observation"
        );

        let event_types: Vec<String> = router
            .db
            .conn()
            .prepare("SELECT event_type FROM event_log WHERE session_id = ?1 ORDER BY seq")
            .unwrap()
            .query_map(params![sid], |row| row.get::<_, String>(0))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert!(
            event_types.contains(&"tool.call_initiated".to_string()),
            "session.run_task must turn provider tool requests into canonical tool starts: {event_types:?}"
        );
        assert!(
            event_types.contains(&"tool.call_completed".to_string()),
            "session.run_task must complete provider-requested tools through the ledger: {event_types:?}"
        );

        let status: String = router
            .db
            .conn()
            .query_row(
                "SELECT status FROM tool_calls WHERE session_id = ?1 AND id = 'task-read'",
                params![sid],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(status, "completed");
    }

    // -----------------------------------------------------------------------
    // Unknown method
    // -----------------------------------------------------------------------

    #[test]
    fn test_unknown_method_returns_error() {
        let router = setup_router();
        let cmd = make_cmd("session.unknown", None, json!({}), None);
        let resp = router.dispatch(cmd);
        assert!(!resp.success);
    }

    // -----------------------------------------------------------------------
    // P0 regression: llm.call must store model tool requests as pending,
    //                never as completed, and must not emit tool.call_completed.
    // -----------------------------------------------------------------------

    #[test]
    fn test_llm_call_model_tool_request_not_stored_as_completed() {
        // The default mock emits ToolCallDelta + ToolCall(get_weather) + Finished(ToolCalls).
        let router = setup_router();
        let sid = create_session(&router);
        let resp = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({"llm_call_id": "llm-tc-p0-regression", "model": "mock/model", "prompt": "test"}),
            None,
        ));
        assert_success(&resp);

        // Durable events must include llm.model_tool_request but NOT tool.call_completed.
        let durable_types: Vec<_> = resp.events.iter().map(|e| e.event_type.as_str()).collect();
        assert!(
            durable_types.contains(&"llm.model_tool_request"),
            "expected llm.model_tool_request in durable events, got: {durable_types:?}"
        );
        assert!(
            !durable_types.contains(&"tool.call_completed"),
            "llm.call must NOT emit tool.call_completed for model-requested tools, \
             got events: {durable_types:?}"
        );

        // Every tool_calls row inserted by llm.call must have status = 'model_tool_request',
        // never 'completed' or any other terminal status.
        let conn = router.db.conn();
        let rows: Vec<(String, String)> = {
            let mut stmt = conn
                .prepare("SELECT id, status FROM tool_calls WHERE session_id = ?1")
                .unwrap();
            stmt.query_map(params![sid], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect()
        };
        assert!(
            !rows.is_empty(),
            "expected at least one tool_calls row after llm.call with ToolCall stream"
        );
        for (id, status) in &rows {
            assert_eq!(
                status.as_str(),
                "model_tool_request",
                "tool_calls row '{id}' has status '{status}'; must be 'model_tool_request'"
            );
        }
    }

    // -----------------------------------------------------------------------
    // P1 safety: workflow_executions.context must not contain raw cmd.params
    //            (including any embedded secrets or auth context strings).
    // -----------------------------------------------------------------------

    #[test]
    fn test_workflow_execution_context_strips_sensitive_params() {
        let dir = tempfile::tempdir().unwrap();
        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-wf-secret", "workspace": dir.path().display().to_string()}),
            None,
        )));

        // Execute a minimal workflow whose params embed a fake secret string.
        let resp = router.dispatch(make_cmd(
            "workflow.execute",
            Some("sess-wf-secret"),
            json!({
                "execution_id": "wf-secret-test",
                "nodes": [
                    {"id": "n1", "kind": "tool", "tool_name": "attempt_completion",
                     "config": {"result": "done"}}
                ],
                "edges": [],
                "input": {
                    "auth_context": "sk-test-secret",
                    "api_key": "sk-test-secret",
                    "bearer": "Bearer sk-test-secret"
                }
            }),
            None,
        ));
        assert_success(&resp);

        // The stored context row must not contain the raw secret string at all.
        let conn = router.db.conn();
        let context: String = conn
            .query_row(
                "SELECT context FROM workflow_executions WHERE id = 'wf-secret-test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !context.contains("sk-test-secret"),
            "workflow_executions.context must not store raw params; \
             found secret string in: {context}"
        );
        // Must have the redaction marker to confirm it was deliberately sanitised.
        assert!(
            context.contains("omitted"),
            "expected 'omitted' redaction marker in context, got: {context}"
        );
    }

    // -----------------------------------------------------------------------
    // P1 safety: workspace boundary for read tools — absolute outside path
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_tool_workspace_boundary_denies_outside_absolute_path() {
        let ws = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&outside_file, "should not be readable").unwrap();

        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-ws-abs", "workspace": ws.path().display().to_string()}),
            None,
        )));

        // Workspace boundary check runs before grant check, so no permission setup needed.
        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-ws-abs"),
            json!({
                "tool_call_id": "tc-abs-outside",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {"path": outside_file.display().to_string()}
            }),
            None,
        ));
        assert_error_code(&resp, ErrorCode::PermissionDenied);
    }

    // -----------------------------------------------------------------------
    // P1 safety: workspace boundary for read tools — prefix-sibling path
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_tool_relative_path_resolves_against_session_workspace() {
        let ws = tempfile::tempdir().unwrap();
        let file = ws.path().join("agent-verification.txt");
        std::fs::write(&file, "workspace-relative-ok").unwrap();

        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-ws-relative", "workspace": ws.path().display().to_string()}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-ws-relative"),
            json!({
                "tool_call_id": "tc-relative-read",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {"path": "agent-verification.txt"}
            }),
            None,
        ));
        assert_success(&resp);
        let conn = router.db.conn();
        let result_json: String = conn
            .query_row(
                "SELECT result_json FROM tool_calls WHERE id = 'tc-relative-read'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let result: Value = serde_json::from_str(&result_json).unwrap();
        assert_eq!(result["redaction"], "omitted");
        assert_eq!(result["shape"]["kind"], "object");
        assert!(!result_json.contains("workspace-relative-ok"));
    }

    #[test]
    fn test_tool_call_raw_persistence_omits_args_and_results() {
        let ws = tempfile::tempdir().unwrap();
        let file = ws.path().join("secret.txt");
        std::fs::write(&file, "sk-test-secret").unwrap();

        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-tool-secret", "workspace": ws.path().display().to_string()}),
            None,
        )));
        assert_success(&router.dispatch(make_cmd(
            "session.input",
            Some("sess-tool-secret"),
            json!({"content": "Bearer sk-test-secret"}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-tool-secret"),
            json!({
                "tool_call_id": "tc-secret-read",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {
                    "path": "secret.txt",
                    "api_key": "sk-test-secret"
                }
            }),
            None,
        ));
        assert_success(&resp);

        let conn = router.db.conn();
        let (args_json, result_json): (String, String) = conn
            .query_row(
                "SELECT args_json, result_json FROM tool_calls WHERE id = 'tc-secret-read'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(!args_json.contains("sk-test-secret"));
        assert!(!result_json.contains("sk-test-secret"));
        assert!(args_json.contains("\"redaction\":\"omitted\""));
        assert!(result_json.contains("\"redaction\":\"omitted\""));

        let event_payloads = {
            let mut stmt = conn
                .prepare("SELECT payload FROM event_log WHERE session_id = 'sess-tool-secret'")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap()
                .join("\n")
        };
        assert!(!event_payloads.contains("sk-test-secret"));
        assert!(event_payloads.contains("\"redaction\":\"omitted\""));
    }

    #[test]
    fn test_read_tool_workspace_boundary_denies_prefix_sibling() {
        // Verify that workspace "/x/ws" does NOT allow reading from "/x/ws_sibling"
        // (string prefix would match; canonical path boundary must not).
        let base = tempfile::tempdir().unwrap();
        let ws_dir = base.path().join("ws");
        let sibling_dir = base.path().join("ws_sibling");
        std::fs::create_dir_all(&ws_dir).unwrap();
        std::fs::create_dir_all(&sibling_dir).unwrap();
        let sibling_file = sibling_dir.join("data.txt");
        std::fs::write(&sibling_file, "sibling data").unwrap();

        let router = setup_router();
        assert_success(&router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"session_id": "sess-ws-prefix", "workspace": ws_dir.display().to_string()}),
            None,
        )));

        let resp = router.dispatch(make_cmd(
            "tool.call",
            Some("sess-ws-prefix"),
            json!({
                "tool_call_id": "tc-prefix-sibling",
                "tool_name": "file_read",
                "tool_type": "native",
                "args": {"path": sibling_file.display().to_string()}
            }),
            None,
        ));
        assert_error_code(&resp, ErrorCode::PermissionDenied);
    }

    // -----------------------------------------------------------------------
    // P1 safety: sidecar large stdout must not deadlock on the OS pipe buffer.
    // -----------------------------------------------------------------------

    #[test]
    fn test_sidecar_large_stdout_drain_does_not_deadlock() {
        // Windows OS pipe buffers are ~64 KB. The sidecar must drain stdout/stderr
        // concurrently while the child runs; if it does not, the child blocks on
        // Write and the parent blocks on wait_timeout — causing a deadlock.
        // This test emits ~2.4 MB of stdout (30 × 80 KB lines).
        let large_line = "X".repeat(80_000);
        let script_body = format!(
            r#"
$data = "{large_line}"
1..30 | ForEach-Object {{ Write-Output $data }}
"#
        );
        let script_path = write_script("sidecar_large_stdout.ps1", &script_body);

        let router = setup_router().with_sidecar_command(
            "large.output",
            SidecarCommand {
                program: powershell(),
                args: vec![
                    "-NoProfile".into(),
                    "-ExecutionPolicy".into(),
                    "Bypass".into(),
                    "-File".into(),
                    script_path.to_string_lossy().to_string(),
                ],
                cwd: None,
            },
        );
        let sid = create_session(&router);

        let started = std::time::Instant::now();
        // tool.call returns regardless of whether the sidecar JSON is valid —
        // what matters is it completes rather than hanging.
        let _resp = router.dispatch(make_cmd(
            "tool.call",
            Some(&sid),
            json!({
                "tool_call_id": "tc-large-stdout",
                "tool_name": "large.output",
                "tool_type": "sidecar",
                "args": {}
            }),
            None,
        ));
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(60),
            "sidecar large-stdout test took {elapsed:?}; \
             likely deadlocked on pipe buffer (expected < 60s)"
        );
    }

    // -----------------------------------------------------------------------
    // P2 reliability: session.run_task idempotency — cached key skips provider
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_run_task_idempotency_cached_key_skips_provider_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(CountingProvider {
            calls: calls.clone(),
        }));
        let router = setup_router().with_llm_router(LlmRouter::new(registry));

        // First dispatch — provider should be called.
        let resp1 = router.dispatch(make_cmd(
            "session.run_task",
            None,
            json!({
                "content": "idempotent task content",
                "workspace": "/tmp/ws-idem",
                "model": "counting/model"
            }),
            Some("run-task-idem-key-001"),
        ));
        assert_success(&resp1);
        let calls_after_first = calls.load(AtomicOrdering::SeqCst);
        assert!(
            calls_after_first >= 1,
            "expected at least one provider call on first session.run_task"
        );

        // Second dispatch with the same idempotency key — provider must NOT be called again.
        let resp2 = router.dispatch(make_cmd(
            "session.run_task",
            None,
            json!({
                "content": "idempotent task content",
                "workspace": "/tmp/ws-idem",
                "model": "counting/model"
            }),
            Some("run-task-idem-key-001"),
        ));
        assert_success(&resp2);
        let calls_after_second = calls.load(AtomicOrdering::SeqCst);
        assert_eq!(
            calls_after_second, calls_after_first,
            "provider must not be called for a cached idempotency key: \
             first={calls_after_first}, second={calls_after_second}"
        );
    }
}
