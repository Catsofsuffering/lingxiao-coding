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
        }
    }

    /// Run the agent until completion, cancellation, or max_rounds.
    pub fn run(self) {
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
                    tool_result.output
                } else {
                    json!({"error": tool_result.error})
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
}
