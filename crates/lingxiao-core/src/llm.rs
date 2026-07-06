use std::collections::HashMap;
use std::io::Write;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use wait_timeout::ChildExt;

pub use lingxiao_llm_host_protocol::*;

const EXTERNAL_PROCESS_OUTPUT_LIMIT: usize = 1024 * 1024;

/// Retry engine with exponential backoff and full jitter.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 500,
            max_delay_ms: 8000,
        }
    }
}

impl RetryConfig {
    /// Execute a fallible operation with exponential backoff + full jitter.
    pub fn retry<F, T, E>(&self, mut f: F) -> Result<T, E>
    where
        F: FnMut(u32) -> Result<T, E>,
        E: RetryableError,
    {
        let mut attempt = 0;
        loop {
            match f(attempt) {
                Ok(value) => return Ok(value),
                Err(err) if !err.is_retryable() => return Err(err),
                Err(err) if attempt >= self.max_retries => return Err(err),
                Err(_) => {
                    // Exponential backoff: delay = min(base * 2^attempt, max)
                    let multiplier = 2_u64.saturating_pow(attempt);
                    let exp_delay = self.base_delay_ms.saturating_mul(multiplier);
                    let capped = exp_delay.min(self.max_delay_ms);
                    // Full jitter: actual = random(0, capped)
                    let jitter = (capped as f64 * fastrand::f64()).round() as u64;
                    thread::sleep(Duration::from_millis(jitter));
                    attempt += 1;
                }
            }
        }
    }
}

/// Trait for errors that can be classified as retryable or terminal.
pub trait RetryableError {
    fn is_retryable(&self) -> bool;
}

impl RetryableError for ProviderError {
    fn is_retryable(&self) -> bool {
        self.retryable
    }
}

/// Circuit breaker per provider to prevent cascade failures.
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_count: AtomicU32,
    last_failure_ms: AtomicU64,
    threshold: u32,
    reset_timeout_ms: u64,
}

impl CircuitBreaker {
    pub fn new(threshold: u32, reset_timeout_ms: u64) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            last_failure_ms: AtomicU64::new(0),
            threshold,
            reset_timeout_ms,
        }
    }

    /// Check if the circuit is open (too many recent failures).
    pub fn is_open(&self) -> bool {
        let count = self.failure_count.load(Ordering::Relaxed);
        if count < self.threshold {
            return false;
        }
        let last_fail = self.last_failure_ms.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        now.saturating_sub(last_fail) < self.reset_timeout_ms
    }

    /// Record a successful call (resets failure count).
    pub fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
    }

    /// Record a failed call (increments failure count).
    pub fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_failure_ms.store(now, Ordering::Relaxed);
    }

    /// Reset the circuit breaker (for manual recovery or testing).
    pub fn reset(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.last_failure_ms.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> CircuitBreakerSnapshot {
        CircuitBreakerSnapshot {
            failure_count: self.failure_count.load(Ordering::Relaxed),
            last_failure_ms: self.last_failure_ms.load(Ordering::Relaxed),
            circuit_open: self.is_open(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CircuitBreakerSnapshot {
    pub failure_count: u32,
    pub last_failure_ms: u64,
    pub circuit_open: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderHealthSnapshot {
    pub provider_id: String,
    pub failure_count: u32,
    pub last_failure_ms: u64,
    pub circuit_open: bool,
}

pub trait LlmProvider: Send + Sync {
    fn provider_id(&self) -> &'static str;

    fn supports_model(&self, model_id: &str) -> bool;

    fn generate(&self, request: GenerateRequest) -> Result<GenerateResponse, ProviderError>;

    fn stream(
        &self,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError> {
        for event in self.generate_stream(request)? {
            sink.emit(event)?;
        }
        Ok(())
    }

    fn generate_stream(
        &self,
        request: GenerateRequest,
    ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
        let mut events = Vec::new();
        let mut sink = |event| {
            events.push(event);
            Ok(())
        };
        self.stream(request, &mut sink)?;
        Ok(events)
    }
}

pub trait LlmEventSink {
    fn emit(&mut self, event: Result<StreamEvent, ProviderError>) -> Result<(), ProviderError>;
}

impl<F> LlmEventSink for F
where
    F: FnMut(Result<StreamEvent, ProviderError>) -> Result<(), ProviderError>,
{
    fn emit(&mut self, event: Result<StreamEvent, ProviderError>) -> Result<(), ProviderError> {
        self(event)
    }
}

#[derive(Debug, Clone)]
pub struct ExternalProcessProvider {
    provider_id: &'static str,
    supported_models: Vec<String>,
    program: PathBuf,
    args: Vec<String>,
    cwd: Option<PathBuf>,
    default_timeout_ms: u64,
}

impl ExternalProcessProvider {
    pub fn new(provider_id: &'static str, program: impl Into<PathBuf>) -> Self {
        Self {
            provider_id,
            supported_models: Vec::new(),
            program: program.into(),
            args: Vec::new(),
            cwd: None,
            default_timeout_ms: 30_000,
        }
    }

    pub fn with_args(mut self, args: Vec<impl Into<String>>) -> Self {
        self.args = args.into_iter().map(|arg| arg.into()).collect();
        self
    }

    pub fn with_models(mut self, models: Vec<impl Into<String>>) -> Self {
        self.supported_models = models.into_iter().map(|m| m.into()).collect();
        self
    }

    pub fn with_cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }

    fn run(&self, request: &GenerateRequest) -> Result<String, ProviderError> {
        let timeout_ms = request
            .options
            .timeout_ms_hint
            .unwrap_or(self.default_timeout_ms);
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .current_dir(self.cwd.as_deref().unwrap_or_else(|| Path::new(".")))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::ServerError,
                    format!("provider executor spawn failed: {e}"),
                )
            })?;

        if let Some(stdin) = child.stdin.as_mut() {
            serde_json::to_writer(&mut *stdin, request).map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::BadRequest,
                    format!("provider request serialization failed: {e}"),
                )
            })?;
            stdin.write_all(b"\n").map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::ServerError,
                    format!("provider request write failed: {e}"),
                )
            })?;
        }
        drop(child.stdin.take());

        let stdout_drain = child.stdout.take().map(spawn_external_output_drain);
        let mut stderr_drain = child.stderr.take().map(spawn_external_output_drain);

        let status = match child
            .wait_timeout(Duration::from_millis(timeout_ms))
            .map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::ServerError,
                    format!("provider executor wait failed: {e}"),
                )
            })? {
            Some(status) => status,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = collect_external_output(stdout_drain);
                let _ = collect_external_output(stderr_drain.take());
                return Err(ProviderError::new(
                    ProviderErrorCode::Timeout,
                    "provider executor timeout",
                ));
            }
        };

        let stdout = collect_external_output(stdout_drain);
        let stderr = collect_external_output(stderr_drain);
        if !status.success() {
            return Err(ProviderError::new(
                ProviderErrorCode::ServerError,
                provider_exit_message(status.to_string(), &stderr.bytes),
            ));
        }
        String::from_utf8(stdout.bytes).map_err(|e| {
            ProviderError::new(
                ProviderErrorCode::StreamInterrupted,
                format!("provider executor emitted non-utf8 stdout: {e}"),
            )
        })
    }
}

impl LlmProvider for ExternalProcessProvider {
    fn provider_id(&self) -> &'static str {
        self.provider_id
    }

    fn supports_model(&self, model_id: &str) -> bool {
        self.supported_models.is_empty() || self.supported_models.iter().any(|m| m == model_id)
    }

    fn generate(&self, request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
        let stdout = self.run(&request)?;
        let line = stdout
            .lines()
            .find(|line| !line.trim().is_empty())
            .ok_or_else(|| {
                ProviderError::new(
                    ProviderErrorCode::StreamInterrupted,
                    "provider executor emitted no response",
                )
            })?;
        serde_json::from_str(line).map_err(|e| {
            ProviderError::new(
                ProviderErrorCode::StreamInterrupted,
                format!("provider response decode failed: {e}"),
            )
        })
    }

    fn stream(
        &self,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError> {
        let timeout_ms = request
            .options
            .timeout_ms_hint
            .unwrap_or(self.default_timeout_ms);
        let mut child = Command::new(&self.program)
            .args(&self.args)
            .current_dir(self.cwd.as_deref().unwrap_or_else(|| Path::new(".")))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::ServerError,
                    format!("provider executor spawn failed: {e}"),
                )
            })?;

        if let Some(stdin) = child.stdin.as_mut() {
            serde_json::to_writer(&mut *stdin, &request).map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::BadRequest,
                    format!("provider request serialization failed: {e}"),
                )
            })?;
            stdin.write_all(b"\n").map_err(|e| {
                ProviderError::new(
                    ProviderErrorCode::ServerError,
                    format!("provider request write failed: {e}"),
                )
            })?;
        }
        drop(child.stdin.take());

        let stdout = child.stdout.take().ok_or_else(|| {
            ProviderError::new(
                ProviderErrorCode::ServerError,
                "provider executor stdout unavailable",
            )
        })?;
        let mut stderr_drain = child.stderr.take().map(spawn_external_output_drain);
        let (tx, rx) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });

        let started = Instant::now();
        let mut emitted = 0usize;
        loop {
            match rx.recv_timeout(Duration::from_millis(10)) {
                Ok(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    emitted += 1;
                    let event = serde_json::from_str::<StreamEvent>(&line).map_err(|e| {
                        ProviderError::new(
                            ProviderErrorCode::StreamInterrupted,
                            format!("provider stream event decode failed: {e}"),
                        )
                    });
                    sink.emit(event)?;
                }
                Ok(Err(e)) => {
                    return Err(ProviderError::new(
                        ProviderErrorCode::StreamInterrupted,
                        format!("provider stdout read failed: {e}"),
                    ));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
            if started.elapsed() > Duration::from_millis(timeout_ms) {
                let _ = child.kill();
                let _ = child.wait();
                let _ = reader.join();
                let _ = collect_external_output(stderr_drain.take());
                return Err(ProviderError::new(
                    ProviderErrorCode::Timeout,
                    "provider executor timeout",
                ));
            }
        }
        let _ = reader.join();
        let remaining = Duration::from_millis(timeout_ms).saturating_sub(started.elapsed());
        match child.wait_timeout(remaining).map_err(|e| {
            ProviderError::new(
                ProviderErrorCode::ServerError,
                format!("provider executor wait failed: {e}"),
            )
        })? {
            Some(status) => {
                let stderr = collect_external_output(stderr_drain.take());
                if !status.success() {
                    return Err(ProviderError::new(
                        ProviderErrorCode::ServerError,
                        provider_exit_message(status.to_string(), &stderr.bytes),
                    ));
                }
            }
            None => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = collect_external_output(stderr_drain.take());
                return Err(ProviderError::new(
                    ProviderErrorCode::Timeout,
                    "provider executor timeout",
                ));
            }
        }
        if emitted == 0 {
            return Err(ProviderError::new(
                ProviderErrorCode::StreamInterrupted,
                "provider executor emitted no stream events",
            ));
        }
        Ok(())
    }
}

struct ExternalOutputDrain {
    buffer: Arc<Mutex<ExternalBoundedOutput>>,
    handle: thread::JoinHandle<()>,
}

#[derive(Clone, Default)]
struct ExternalBoundedOutput {
    bytes: Vec<u8>,
    truncated: bool,
}

fn spawn_external_output_drain<R>(mut reader: R) -> ExternalOutputDrain
where
    R: Read + Send + 'static,
{
    let buffer = Arc::new(Mutex::new(ExternalBoundedOutput::default()));
    let thread_buffer = Arc::clone(&buffer);
    let handle = thread::spawn(move || {
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => append_external_output(&thread_buffer, &chunk[..n]),
                Err(_) => break,
            }
        }
    });
    ExternalOutputDrain { buffer, handle }
}

fn append_external_output(buffer: &Arc<Mutex<ExternalBoundedOutput>>, bytes: &[u8]) {
    let mut guard = buffer.lock().unwrap();
    guard.bytes.extend_from_slice(bytes);
    if guard.bytes.len() > EXTERNAL_PROCESS_OUTPUT_LIMIT {
        let overflow = guard.bytes.len() - EXTERNAL_PROCESS_OUTPUT_LIMIT;
        guard.bytes.drain(..overflow);
        guard.truncated = true;
    }
}

fn collect_external_output(drain: Option<ExternalOutputDrain>) -> ExternalBoundedOutput {
    let Some(drain) = drain else {
        return ExternalBoundedOutput::default();
    };
    let _ = drain.handle.join();
    let output = drain.buffer.lock().unwrap().clone();
    output
}

fn provider_exit_message(status: String, stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    let first_line = stderr.lines().next().unwrap_or("").trim();
    if first_line.is_empty() || first_line.contains("sk-") {
        format!("provider executor exited with status {status}")
    } else {
        format!("provider executor exited with status {status}: {first_line}")
    }
}

pub struct MockLlmProvider {
    provider_id: &'static str,
    supported_models: Vec<String>,
    predefined_stream: Option<Vec<Result<StreamEvent, ProviderError>>>,
}

impl MockLlmProvider {
    pub fn new(provider_id: &'static str) -> Self {
        Self {
            provider_id,
            supported_models: Vec::new(),
            predefined_stream: None,
        }
    }

    pub fn with_models(mut self, models: Vec<impl Into<String>>) -> Self {
        self.supported_models = models.into_iter().map(|m| m.into()).collect();
        self
    }

    pub fn with_stream(mut self, events: Vec<Result<StreamEvent, ProviderError>>) -> Self {
        self.predefined_stream = Some(events);
        self
    }

    /// Build a standard mock stream: thinking → text → tool_call → usage → finished.
    pub fn standard_stream() -> Vec<Result<StreamEvent, ProviderError>> {
        vec![
            Ok(StreamEvent::ThinkingDelta("thinking about it...".into())),
            Ok(StreamEvent::TextDelta("Hello, ".into())),
            Ok(StreamEvent::TextDelta("world!".into())),
            Ok(StreamEvent::ToolCallDelta(ToolCallDelta {
                index: 0,
                id: Some("call_abc".into()),
                name: Some("get_weather".into()),
                partial_json: Some(r#"{"location":"Shanghai"}"#.into()),
            })),
            Ok(StreamEvent::ToolCall(ToolCall {
                id: "call_abc".into(),
                name: "get_weather".into(),
                arguments: serde_json::json!({"location": "Shanghai"}),
            })),
            Ok(StreamEvent::Usage(TokenUsage {
                prompt_tokens: 50,
                completion_tokens: 30,
                total_tokens: 80,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                reasoning_tokens: Some(10),
            })),
            Ok(StreamEvent::Finished(FinishReason::ToolCalls)),
        ]
    }
}

impl LlmProvider for MockLlmProvider {
    fn provider_id(&self) -> &'static str {
        self.provider_id
    }

    fn supports_model(&self, model_id: &str) -> bool {
        self.supported_models.is_empty() || self.supported_models.iter().any(|m| m == model_id)
    }

    fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
        Ok(GenerateResponse {
            content: "Mock response".into(),
            finish_reason: "stop".into(),
            usage: TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 10,
                total_tokens: 20,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                reasoning_tokens: None,
            },
        })
    }

    fn stream(
        &self,
        _request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError> {
        let events = self
            .predefined_stream
            .clone()
            .unwrap_or_else(Self::standard_stream);
        for event in events {
            sink.emit(event)?;
        }
        Ok(())
    }
}

pub struct ProviderRegistry {
    providers: HashMap<&'static str, Arc<dyn LlmProvider>>,
    provider_order: Vec<&'static str>,
    model_metadata: HashMap<(String, String), ModelRoutingMetadata>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRoutingMetadata {
    pub provider_id: String,
    pub model_id: String,
    pub input_cost_per_million: Option<f64>,
    pub output_cost_per_million: Option<f64>,
    pub context_window: Option<u32>,
    pub supports_tools: bool,
    pub supports_streaming: bool,
}

impl ModelRoutingMetadata {
    pub fn new(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            model_id: model_id.into(),
            input_cost_per_million: None,
            output_cost_per_million: None,
            context_window: None,
            supports_tools: true,
            supports_streaming: true,
        }
    }

    pub fn with_cost(mut self, input_per_million: f64, output_per_million: f64) -> Self {
        self.input_cost_per_million = Some(input_per_million);
        self.output_cost_per_million = Some(output_per_million);
        self
    }

    pub fn with_context_window(mut self, context_window: u32) -> Self {
        self.context_window = Some(context_window);
        self
    }

    pub fn with_tool_support(mut self, supports_tools: bool) -> Self {
        self.supports_tools = supports_tools;
        self
    }

    pub fn with_streaming_support(mut self, supports_streaming: bool) -> Self {
        self.supports_streaming = supports_streaming;
        self
    }

    fn estimated_unit_cost(&self) -> Option<f64> {
        Some(self.input_cost_per_million.unwrap_or(0.0) + self.output_cost_per_million?)
    }
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: HashMap::new(),
            provider_order: Vec::new(),
            model_metadata: HashMap::new(),
        }
    }

    pub fn register(&mut self, provider: Arc<dyn LlmProvider>) {
        let provider_id = provider.provider_id();
        if !self.providers.contains_key(provider_id) {
            self.provider_order.push(provider_id);
        }
        self.providers.insert(provider_id, provider);
    }

    pub fn get(&self, provider_id: &str) -> Option<&Arc<dyn LlmProvider>> {
        self.providers.get(provider_id)
    }

    pub fn register_model_metadata(&mut self, metadata: ModelRoutingMetadata) {
        self.model_metadata.insert(
            (metadata.provider_id.clone(), metadata.model_id.clone()),
            metadata,
        );
    }

    pub fn model_metadata(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Option<&ModelRoutingMetadata> {
        self.model_metadata
            .get(&(provider_id.to_string(), model_id.to_string()))
    }

    pub fn resolve(&self, model_id: &str) -> Option<&Arc<dyn LlmProvider>> {
        self.provider_order
            .iter()
            .filter_map(|provider_id| self.providers.get(provider_id))
            .find(|provider| provider.supports_model(model_id))
    }

    pub fn resolve_for_request(
        &self,
        model_id: &str,
        needs_tools: bool,
    ) -> Option<&Arc<dyn LlmProvider>> {
        let candidates: Vec<&Arc<dyn LlmProvider>> = self
            .provider_order
            .iter()
            .filter_map(|provider_id| self.providers.get(provider_id))
            .filter(|provider| provider.supports_model(model_id))
            .filter(|provider| {
                if !needs_tools {
                    return true;
                }
                self.model_metadata(provider.provider_id(), model_id)
                    .map(|metadata| metadata.supports_tools)
                    .unwrap_or(true)
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        if candidates.iter().any(|provider| {
            self.model_metadata(provider.provider_id(), model_id)
                .is_some()
        }) {
            return candidates.into_iter().min_by(|left, right| {
                let left_cost = self
                    .model_metadata(left.provider_id(), model_id)
                    .and_then(ModelRoutingMetadata::estimated_unit_cost)
                    .unwrap_or(f64::MAX);
                let right_cost = self
                    .model_metadata(right.provider_id(), model_id)
                    .and_then(ModelRoutingMetadata::estimated_unit_cost)
                    .unwrap_or(f64::MAX);
                left_cost
                    .partial_cmp(&right_cost)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        candidates.into_iter().next()
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct LlmRouter {
    registry: ProviderRegistry,
    retry_config: RetryConfig,
    circuit_breakers: HashMap<&'static str, Arc<CircuitBreaker>>,
    fallback_models: HashMap<String, Vec<String>>,
}

impl LlmRouter {
    pub fn new(registry: ProviderRegistry) -> Self {
        let mut circuit_breakers = HashMap::new();
        for provider_id in registry.providers.keys() {
            circuit_breakers.insert(*provider_id, Arc::new(CircuitBreaker::new(5, 15_000)));
        }
        Self {
            registry,
            retry_config: RetryConfig::default(),
            circuit_breakers,
            fallback_models: HashMap::new(),
        }
    }

    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Register fallback model chain: if `primary` is exhausted/circuit-open,
    /// try each entry in `fallbacks` in order.
    pub fn with_fallback_chain(
        mut self,
        primary: impl Into<String>,
        fallbacks: Vec<impl Into<String>>,
    ) -> Self {
        self.fallback_models.insert(
            primary.into(),
            fallbacks.into_iter().map(|m| m.into()).collect(),
        );
        self
    }

    pub fn registry(&self) -> &ProviderRegistry {
        &self.registry
    }

    pub fn provider_health_snapshots(&self) -> Vec<ProviderHealthSnapshot> {
        let mut snapshots: Vec<_> = self
            .circuit_breakers
            .iter()
            .map(|(provider_id, breaker)| {
                let snapshot = breaker.snapshot();
                ProviderHealthSnapshot {
                    provider_id: (*provider_id).to_string(),
                    failure_count: snapshot.failure_count,
                    last_failure_ms: snapshot.last_failure_ms,
                    circuit_open: snapshot.circuit_open,
                }
            })
            .collect();
        snapshots.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
        snapshots
    }

    /// Attempt a stream call with retry + circuit-breaker + fallback semantics:
    /// 1. Check circuit breaker for primary provider; skip to fallback if open.
    /// 2. Retry transient errors up to max_retries with exponential backoff + jitter.
    /// 3. On terminal non-retryable errors (auth, bad-request), stop immediately.
    /// 4. If retries exhausted, try each fallback model in declared order.
    /// 5. Each fallback gets its own independent retry budget.
    pub fn route_stream(
        &self,
        request: GenerateRequest,
    ) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
        let mut events = Vec::new();
        self.route_stream_with_sink(request, &mut |event| {
            events.push(event);
            Ok(())
        })?;
        Ok(events)
    }

    pub fn route_stream_with_sink(
        &self,
        request: GenerateRequest,
        sink: &mut dyn LlmEventSink,
    ) -> Result<(), ProviderError> {
        let max_retries = request
            .options
            .max_retries_hint
            .unwrap_or(self.retry_config.max_retries);

        // Build the ordered model chain: primary + optional fallbacks
        let fallbacks = self
            .fallback_models
            .get(&request.model)
            .cloned()
            .unwrap_or_default();
        let mut model_chain = Vec::with_capacity(1 + fallbacks.len());
        model_chain.push(request.model.clone());
        model_chain.extend(fallbacks);

        let mut last_err = ProviderError::new(
            ProviderErrorCode::UnsupportedModel,
            format!("No provider found for model '{}'", request.model),
        );

        for model_id in &model_chain {
            let provider = match self
                .registry
                .resolve_for_request(model_id, !request.tools.is_empty())
            {
                Some(p) => p,
                None => {
                    last_err = ProviderError::new(
                        ProviderErrorCode::UnsupportedModel,
                        format!("No provider for model '{model_id}'"),
                    );
                    continue;
                }
            };
            if let Some(context_window) = self
                .registry
                .model_metadata(provider.provider_id(), model_id)
                .and_then(|metadata| metadata.context_window)
            {
                let estimated_tokens = estimate_request_tokens(&request);
                if estimated_tokens > u64::from(context_window) {
                    return Err(redact_error_for_request(
                        ProviderError::new(
                            ProviderErrorCode::ContextOverflow,
                            format!(
                                "Request estimated at {estimated_tokens} tokens exceeds context window {context_window} for model '{model_id}'"
                            ),
                        ),
                        &request,
                    ));
                }
            }

            // Circuit breaker check per provider
            if let Some(breaker) = self.circuit_breakers.get(provider.provider_id()) {
                if breaker.is_open() {
                    last_err = ProviderError::new(
                        ProviderErrorCode::CircuitOpen,
                        format!(
                            "Circuit breaker open for provider '{}'",
                            provider.provider_id()
                        ),
                    );
                    continue; // Try next fallback
                }
            }

            // Clone request with this specific model
            let mut model_request = request.clone();
            model_request.model = model_id.clone();

            let retry = RetryConfig {
                max_retries,
                base_delay_ms: self.retry_config.base_delay_ms,
                max_delay_ms: self.retry_config.max_delay_ms,
            };

            let breaker_ref = self.circuit_breakers.get(provider.provider_id()).cloned();
            let result = retry.retry(|_attempt| {
                if let Some(ref breaker) = breaker_ref {
                    if breaker.is_open() {
                        return Err(ProviderError::new(
                            ProviderErrorCode::CircuitOpen,
                            format!(
                                "Circuit breaker open for provider '{}'",
                                provider.provider_id()
                            ),
                        ));
                    }
                }
                let mut attempt_events = Vec::new();
                let res = provider.stream(model_request.clone(), &mut |event| {
                    attempt_events.push(event);
                    Ok(())
                });
                let res = res.map(|()| attempt_events).and_then(normalize_stream);
                if let Some(ref breaker) = breaker_ref {
                    match &res {
                        Ok(_) => breaker.record_success(),
                        Err(e) if e.is_retryable() => breaker.record_failure(),
                        Err(_) => {} // Terminal — don't open circuit for auth/bad-request
                    }
                }
                res
            });

            match result {
                Ok(events) => {
                    for event in events {
                        sink.emit(event)?;
                    }
                    return Ok(());
                }
                Err(e) if e.code == ProviderErrorCode::CircuitOpen => {
                    last_err = redact_error_for_request(e, &request);
                }
                Err(e) if !e.is_retryable() => {
                    // Non-retryable (auth, bad-request, context-overflow, content-filtered):
                    // do not fall back — return immediately with the error.
                    return Err(redact_error_for_request(e, &request));
                }
                Err(e) => {
                    last_err = redact_error_for_request(e, &request);
                    // Retryable exhausted — try next fallback
                }
            }
        }

        Err(last_err)
    }
}

fn normalize_stream(
    events: Vec<Result<StreamEvent, ProviderError>>,
) -> Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError> {
    for event in &events {
        match event {
            Ok(StreamEvent::Error(error)) | Err(error) => return Err(error.clone()),
            _ => {}
        }
    }
    Ok(events)
}

fn redact_error_for_request(mut error: ProviderError, request: &GenerateRequest) -> ProviderError {
    let mut message = error.message;
    for secret in auth_secret_values(&request.auth_context) {
        if !secret.is_empty() {
            message = message.replace(secret, "<redacted>");
        }
    }
    error.message = message;
    error
}

fn auth_secret_values(auth_context: &AuthContext) -> Vec<&str> {
    match auth_context {
        AuthContext::ApiKey { key, .. } => vec![key.as_str()],
        AuthContext::BearerToken { token, .. } => vec![token.as_str()],
        AuthContext::AwsSignature {
            access_key_id,
            secret_access_key,
            session_token,
            ..
        } => {
            let mut secrets = vec![access_key_id.as_str(), secret_access_key.as_str()];
            if let Some(token) = session_token {
                secrets.push(token.as_str());
            }
            secrets
        }
        AuthContext::AzureToken { api_key, .. } => vec![api_key.as_str()],
        AuthContext::None => Vec::new(),
    }
}

fn estimate_request_tokens(request: &GenerateRequest) -> u64 {
    request
        .messages
        .iter()
        .map(|message| estimate_text_tokens(&message.content).saturating_add(4))
        .sum::<u64>()
        .saturating_add(request.tools.len() as u64 * 8)
        .max(1)
}

fn estimate_text_tokens(text: &str) -> u64 {
    let chars = text.chars().count() as u64;
    let words = text.split_whitespace().count() as u64;
    chars.div_ceil(4).max(words).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    type StreamResult = Result<Vec<Result<StreamEvent, ProviderError>>, ProviderError>;

    struct PlannedLlmProvider {
        provider_id: &'static str,
        supported_models: Vec<String>,
        outcomes: Mutex<VecDeque<StreamResult>>,
        calls: AtomicU32,
    }

    impl PlannedLlmProvider {
        fn new(
            provider_id: &'static str,
            supported_models: Vec<impl Into<String>>,
            outcomes: Vec<StreamResult>,
        ) -> Self {
            Self {
                provider_id,
                supported_models: supported_models.into_iter().map(|m| m.into()).collect(),
                outcomes: Mutex::new(VecDeque::from(outcomes)),
                calls: AtomicU32::new(0),
            }
        }

        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl LlmProvider for PlannedLlmProvider {
        fn provider_id(&self) -> &'static str {
            self.provider_id
        }

        fn supports_model(&self, model_id: &str) -> bool {
            self.supported_models.iter().any(|m| m == model_id)
        }

        fn generate(&self, _request: GenerateRequest) -> Result<GenerateResponse, ProviderError> {
            Ok(GenerateResponse {
                content: "planned response".into(),
                finish_reason: "stop".into(),
                usage: TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
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
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(MockLlmProvider::standard_stream()))
        }
    }

    #[test]
    fn test_model_routing_metadata_prefers_lower_cost_provider() {
        let expensive = Arc::new(PlannedLlmProvider::new(
            "expensive",
            vec!["shared/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let cheap = Arc::new(PlannedLlmProvider::new(
            "cheap",
            vec!["shared/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(expensive.clone());
        registry.register(cheap.clone());
        registry.register_model_metadata(
            ModelRoutingMetadata::new("expensive", "shared/model").with_cost(10.0, 20.0),
        );
        registry.register_model_metadata(
            ModelRoutingMetadata::new("cheap", "shared/model").with_cost(1.0, 2.0),
        );
        let router = LlmRouter::new(registry);
        router
            .route_stream(sample_router_request("shared/model"))
            .unwrap();
        assert_eq!(cheap.call_count(), 1);
        assert_eq!(expensive.call_count(), 0);
    }

    #[test]
    fn test_model_routing_metadata_filters_tool_incompatible_provider() {
        let text_only = Arc::new(PlannedLlmProvider::new(
            "text-only",
            vec!["shared/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let tool_capable = Arc::new(PlannedLlmProvider::new(
            "tool-capable",
            vec!["shared/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(text_only.clone());
        registry.register(tool_capable.clone());
        registry.register_model_metadata(
            ModelRoutingMetadata::new("text-only", "shared/model")
                .with_cost(1.0, 1.0)
                .with_tool_support(false),
        );
        registry.register_model_metadata(
            ModelRoutingMetadata::new("tool-capable", "shared/model").with_cost(5.0, 5.0),
        );
        let router = LlmRouter::new(registry);
        let mut request = sample_router_request("shared/model");
        request.tools.push(ToolDefinition {
            name: "file_read".to_string(),
            description: "read".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
        });
        router.route_stream(request).unwrap();
        assert_eq!(tool_capable.call_count(), 1);
        assert_eq!(text_only.call_count(), 0);
    }

    #[test]
    fn test_model_context_window_rejects_before_provider_call() {
        let provider = Arc::new(PlannedLlmProvider::new(
            "limited",
            vec!["limited/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());
        registry.register_model_metadata(
            ModelRoutingMetadata::new("limited", "limited/model").with_context_window(4),
        );
        let router = LlmRouter::new(registry);
        let err = router
            .route_stream(GenerateRequest {
                model: "limited/model".into(),
                messages: vec![Message {
                    role: "user".into(),
                    content: "this prompt is intentionally larger than the tiny window".into(),
                    ..Default::default()
                }],
                tools: vec![],
                stream: true,
                auth_context: AuthContext::None,
                options: RequestOptions::default(),
            })
            .unwrap_err();
        assert_eq!(err.code, ProviderErrorCode::ContextOverflow);
        assert_eq!(provider.call_count(), 0);
    }

    #[test]
    fn test_p2_retry_succeeds_after_transient_failures() {
        let provider = Arc::new(PlannedLlmProvider::new(
            "mock",
            vec!["mock/model"],
            vec![
                Err(ProviderError::new(
                    ProviderErrorCode::ServerError,
                    "upstream 500",
                )),
                Err(ProviderError::new(
                    ProviderErrorCode::Timeout,
                    "request timed out",
                )),
                Ok(MockLlmProvider::standard_stream()),
            ],
        ));

        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());

        let router = LlmRouter::new(registry).with_retry_config(RetryConfig {
            max_retries: 3,
            base_delay_ms: 0,
            max_delay_ms: 0,
        });

        let result = router.route_stream(sample_router_request("mock/model"));
        assert!(result.is_ok(), "Should succeed after retries");
        assert_eq!(provider.call_count(), 3);
    }

    #[test]
    fn test_p2_stream_event_error_triggers_retry_and_circuit_accounting() {
        let provider = Arc::new(PlannedLlmProvider::new(
            "mock",
            vec!["mock/model"],
            vec![
                Ok(vec![Ok(StreamEvent::Error(ProviderError::new(
                    ProviderErrorCode::ServerError,
                    "stream 500",
                )))]),
                Ok(MockLlmProvider::standard_stream()),
            ],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(provider.clone());
        let router = LlmRouter::new(registry).with_retry_config(RetryConfig {
            max_retries: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
        });

        let events = router
            .route_stream(sample_router_request("mock/model"))
            .unwrap();
        assert!(events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::TextDelta(_)))));
        assert_eq!(provider.call_count(), 2);
    }

    #[test]
    fn test_p2_retry_stops_on_non_retryable() {
        // Register the mock only for "mock/model" — not for "nonexistent/model"
        let provider = MockLlmProvider::new("mock").with_models(vec!["mock/model"]);
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(provider));

        let router = LlmRouter::new(registry).with_retry_config(RetryConfig {
            max_retries: 5,
            base_delay_ms: 1,
            max_delay_ms: 10,
        });

        // Request a model no provider supports
        let request = GenerateRequest {
            model: "nonexistent/model".into(),
            messages: vec![],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let result = router.route_stream(request);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, ProviderErrorCode::UnsupportedModel);
    }

    #[test]
    fn test_p2_retry_stops_on_auth_and_bad_request_without_fallback() {
        for code in [
            ProviderErrorCode::Authentication,
            ProviderErrorCode::BadRequest,
        ] {
            let primary = Arc::new(PlannedLlmProvider::new(
                "primary",
                vec!["primary/model"],
                vec![Err(ProviderError::new(code, "terminal provider error"))],
            ));
            let fallback = Arc::new(PlannedLlmProvider::new(
                "fallback",
                vec!["fallback/model"],
                vec![Ok(MockLlmProvider::standard_stream())],
            ));
            let mut registry = ProviderRegistry::new();
            registry.register(primary.clone());
            registry.register(fallback.clone());

            let router = LlmRouter::new(registry)
                .with_fallback_chain("primary/model", vec!["fallback/model"])
                .with_retry_config(RetryConfig {
                    max_retries: 5,
                    base_delay_ms: 0,
                    max_delay_ms: 0,
                });

            let err = router
                .route_stream(sample_router_request("primary/model"))
                .unwrap_err();
            assert_eq!(err.code, code);
            assert_eq!(primary.call_count(), 1);
            assert_eq!(fallback.call_count(), 0);
        }
    }

    #[test]
    fn test_p2_circuit_breaker_opens_and_resets() {
        let provider = MockLlmProvider::new("mock");
        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(provider));

        let router = LlmRouter::new(registry);
        let breaker = router.circuit_breakers.get("mock").unwrap();

        // Simulate failures
        for _ in 0..5 {
            breaker.record_failure();
        }
        assert!(breaker.is_open(), "Circuit should be open after 5 failures");

        // Reset
        breaker.reset();
        assert!(!breaker.is_open(), "Circuit should be closed after reset");
    }

    #[test]
    fn test_p2_circuit_breaker_resets_after_timeout_and_manual_reset() {
        let breaker = CircuitBreaker::new(2, 20);

        breaker.record_failure();
        breaker.record_failure();
        assert!(breaker.is_open(), "Circuit should open at threshold");

        thread::sleep(Duration::from_millis(25));
        assert!(
            !breaker.is_open(),
            "Circuit should allow a probe after reset timeout"
        );

        breaker.record_failure();
        assert!(
            breaker.is_open(),
            "Circuit should reopen after failed half-open probe"
        );

        breaker.reset();
        assert!(!breaker.is_open(), "Manual reset should close circuit");
    }

    #[test]
    fn test_p2_fallback_uses_secondary_on_retry_exhausted() {
        let primary = Arc::new(PlannedLlmProvider::new(
            "primary",
            vec!["primary/model"],
            vec![
                Err(ProviderError::new(
                    ProviderErrorCode::ServerError,
                    "upstream 500",
                )),
                Err(ProviderError::new(
                    ProviderErrorCode::ServerError,
                    "upstream 500 again",
                )),
            ],
        ));
        let fallback = Arc::new(PlannedLlmProvider::new(
            "fallback",
            vec!["fallback/model"],
            vec![Ok(MockLlmProvider::standard_stream())],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(primary.clone());
        registry.register(fallback.clone());

        let router = LlmRouter::new(registry)
            .with_fallback_chain("primary/model", vec!["fallback/model"])
            .with_retry_config(RetryConfig {
                max_retries: 1,
                base_delay_ms: 0,
                max_delay_ms: 0,
            });

        let result = router.route_stream(sample_router_request("primary/model"));
        assert!(
            result.is_ok(),
            "Should fallback to secondary after retry budget is exhausted"
        );
        assert_eq!(primary.call_count(), 2);
        assert_eq!(fallback.call_count(), 1);
    }

    #[test]
    fn test_p2_fallback_uses_secondary_on_circuit_open() {
        let primary = MockLlmProvider::new("primary").with_models(vec!["primary/model"]);
        let fallback = MockLlmProvider::new("fallback")
            .with_models(vec!["fallback/model"])
            .with_stream(MockLlmProvider::standard_stream());

        let mut registry = ProviderRegistry::new();
        registry.register(Arc::new(primary));
        registry.register(Arc::new(fallback));

        let router = LlmRouter::new(registry)
            .with_fallback_chain("primary/model", vec!["fallback/model"])
            .with_retry_config(RetryConfig {
                max_retries: 0, // No retries, fail fast
                base_delay_ms: 1,
                max_delay_ms: 10,
            });

        // Open primary circuit
        if let Some(breaker) = router.circuit_breakers.get("primary") {
            for _ in 0..5 {
                breaker.record_failure();
            }
        }

        let request = GenerateRequest {
            model: "primary/model".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "test".into(),
                ..Default::default()
            }],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let result = router.route_stream(request);
        assert!(
            result.is_ok(),
            "Should fallback to secondary when primary circuit is open"
        );
    }

    #[test]
    fn test_p2_fallback_errors_do_not_leak_auth_secrets() {
        let provider = Arc::new(PlannedLlmProvider::new(
            "primary",
            vec!["primary/model"],
            vec![Err(ProviderError::new(
                ProviderErrorCode::ServerError,
                "upstream echoed sk-router-secret",
            ))],
        ));
        let mut registry = ProviderRegistry::new();
        registry.register(provider);

        let router = LlmRouter::new(registry).with_retry_config(RetryConfig {
            max_retries: 0,
            base_delay_ms: 0,
            max_delay_ms: 0,
        });

        let mut request = sample_router_request("primary/model");
        request.auth_context = AuthContext::ApiKey {
            provider: "primary".into(),
            key: "sk-router-secret".into(),
        };

        let err = router.route_stream(request).unwrap_err();
        assert_eq!(err.code, ProviderErrorCode::ServerError);
        assert!(!err.message.contains("sk-router-secret"));
        assert!(!format!("{err:?}").contains("sk-router-secret"));
        assert!(err.message.contains("<redacted>"));
    }

    #[test]
    fn test_mock_provider_stream_order_gs019() {
        let provider = MockLlmProvider::new("mock");
        let request = GenerateRequest {
            model: "mock/model".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let events = provider
            .generate_stream(request)
            .expect("stream should succeed");

        let mut idx = 0;

        // Expect: ThinkingDelta → TextDelta → ToolCallDelta → ToolCall → Usage → Finished
        assert!(
            matches!(&events[idx], Ok(StreamEvent::ThinkingDelta(_))),
            "event[{}] should be ThinkingDelta",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::TextDelta(_))),
            "event[{}] should be TextDelta",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::TextDelta(_))),
            "event[{}] should be TextDelta",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::ToolCallDelta(_))),
            "event[{}] should be ToolCallDelta",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::ToolCall(_))),
            "event[{}] should be ToolCall",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::Usage(_))),
            "event[{}] should be Usage",
            idx
        );
        idx += 1;

        assert!(
            matches!(&events[idx], Ok(StreamEvent::Finished(_))),
            "event[{}] should be Finished",
            idx
        );
    }

    #[test]
    fn test_mock_provider_non_stream() {
        let provider = MockLlmProvider::new("mock");
        let request = GenerateRequest {
            model: "mock/model".into(),
            messages: vec![],
            tools: vec![],
            stream: false,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let response = provider.generate(request).expect("generate should succeed");
        assert_eq!(response.content, "Mock response");
        assert_eq!(response.finish_reason, "stop");
    }

    #[test]
    fn test_provider_registry_roundtrip() {
        let mut registry = ProviderRegistry::new();
        let provider = Arc::new(MockLlmProvider::new("mock").with_models(vec!["mock/model"]));

        registry.register(provider);

        let resolved = registry
            .resolve("mock/model")
            .expect("should resolve model");
        assert_eq!(resolved.provider_id(), "mock");
        assert!(resolved.supports_model("mock/model"));

        let not_found = registry.resolve("unknown/model");
        assert!(not_found.is_none());
    }

    #[test]
    fn test_llm_router_routes_to_correct_provider() {
        let mut registry = ProviderRegistry::new();
        let provider = Arc::new(MockLlmProvider::new("mock").with_models(vec!["mock/model"]));
        registry.register(provider);

        let router = LlmRouter::new(registry);

        let request = GenerateRequest {
            model: "mock/model".into(),
            messages: vec![],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let events = router
            .route_stream(request)
            .expect("routing should succeed");
        assert!(!events.is_empty());
    }

    #[test]
    fn test_llm_router_unsupported_model_error() {
        let registry = ProviderRegistry::new();
        let router = LlmRouter::new(registry);

        let request = GenerateRequest {
            model: "no-such-model".into(),
            messages: vec![],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let result = router.route_stream(request);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code,
            ProviderErrorCode::UnsupportedModel
        );
    }

    #[test]
    fn test_mock_provider_custom_stream() {
        let events = vec![
            Ok(StreamEvent::TextDelta("custom".into())),
            Ok(StreamEvent::Finished(FinishReason::Stop)),
        ];
        let provider = MockLlmProvider::new("custom").with_stream(events.clone());

        let request = GenerateRequest {
            model: "any".into(),
            messages: vec![],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        };

        let result = provider.generate_stream(request).expect("should succeed");
        assert_eq!(result.len(), 2);
        assert!(matches!(&result[0], Ok(StreamEvent::TextDelta(t)) if t == "custom"));
    }

    /// GS-019: durable/realtime separation
    /// StreamEvents are non-durable realtime channel events, not written to event_log.
    /// This test verifies the naming boundary: StreamEvent variants use realtime-oriented
    /// names (TextDelta, ThinkingDelta, ToolCallDelta, etc.) rather than durable event names
    /// (llm.call_started, llm.call_finished, llm.usage_reported).
    #[test]
    fn test_stream_events_are_not_durable_events_gs019() {
        let durable_substrings = ["call_started", "call_finished", "usage_reported"];

        let events = [
            StreamEvent::TextDelta("".into()),
            StreamEvent::ThinkingDelta("".into()),
            StreamEvent::ToolCallDelta(ToolCallDelta {
                index: 0,
                id: None,
                name: None,
                partial_json: None,
            }),
            StreamEvent::ToolCall(ToolCall {
                id: "".into(),
                name: "".into(),
                arguments: serde_json::Value::Null,
            }),
            StreamEvent::Usage(TokenUsage {
                prompt_tokens: 0,
                completion_tokens: 0,
                total_tokens: 0,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                reasoning_tokens: None,
            }),
            StreamEvent::Finished(FinishReason::Stop),
        ];

        for ev in &events {
            let debug = format!("{:?}", ev);
            for sub in &durable_substrings {
                assert!(
                    !debug.contains(sub),
                    "StreamEvent '{debug}' should not contain durable substring '{sub}'"
                );
            }
        }
    }

    /// No env/config reading: MockLlmProvider never reads environment variables.
    /// This is a contract test — if the provider implementation calls std::env::var,
    /// it breaks the boundary.
    #[test]
    fn test_mock_provider_does_not_read_env() {
        let provider = MockLlmProvider::new("mock");
        let request = GenerateRequest {
            model: "mock/model".into(),
            messages: vec![],
            tools: vec![],
            stream: false,
            auth_context: AuthContext::ApiKey {
                provider: "mock".into(),
                key: "test-key".into(),
            },
            options: RequestOptions::default(),
        };

        // AuthContext is passed in — provider never reads from env
        let response = provider.generate(request).expect("generate should succeed");
        assert_eq!(response.content, "Mock response");
    }

    #[test]
    fn test_external_process_provider_non_stream_roundtrip() {
        let provider = ExternalProcessProvider::new("external", powershell()).with_args(vec![
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &write_non_stream_provider().to_string_lossy(),
        ]);

        let response = provider
            .generate(sample_request(false))
            .expect("external provider should return GenerateResponse");

        assert_eq!(response.content, "external response");
        assert_eq!(response.finish_reason, "stop");
        assert_eq!(response.usage.total_tokens, 3);
    }

    #[test]
    fn test_external_process_provider_stream_roundtrip() {
        let provider = ExternalProcessProvider::new("external", powershell()).with_args(vec![
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &write_stream_provider().to_string_lossy(),
        ]);

        let events = provider
            .generate_stream(sample_request(true))
            .expect("external provider should return stream events");

        assert!(matches!(&events[0], Ok(StreamEvent::ThinkingDelta(text)) if text == "plan"));
        assert!(matches!(&events[1], Ok(StreamEvent::TextDelta(text)) if text == "hello"));
        assert!(matches!(&events[2], Ok(StreamEvent::Usage(usage)) if usage.total_tokens == 3));
        assert!(matches!(
            &events[3],
            Ok(StreamEvent::Finished(FinishReason::Stop))
        ));
    }

    #[test]
    fn test_external_process_provider_stream_drains_large_stderr() {
        let provider = ExternalProcessProvider::new("external", powershell())
            .with_args(vec![
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                &write_large_stderr_stream_provider().to_string_lossy(),
            ])
            .with_timeout_ms(30_000);

        let started = Instant::now();
        let events = provider
            .generate_stream(sample_request(true))
            .expect("large stderr should not deadlock stream provider");

        assert!(
            started.elapsed() < Duration::from_secs(30),
            "provider likely blocked on stderr pipe"
        );
        assert!(matches!(&events[0], Ok(StreamEvent::TextDelta(text)) if text == "stderr-ok"));
        assert!(matches!(
            &events[1],
            Ok(StreamEvent::Finished(FinishReason::Stop))
        ));
    }

    #[test]
    fn test_external_process_provider_timeout() {
        let provider = ExternalProcessProvider::new("external", powershell())
            .with_args(vec![
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                &write_sleep_provider().to_string_lossy(),
            ])
            .with_timeout_ms(50);

        let err = provider.generate(sample_request(false)).unwrap_err();
        assert_eq!(err.code, ProviderErrorCode::Timeout);
        assert!(err.retryable);
    }

    #[test]
    fn test_external_process_provider_error_does_not_leak_stderr_secret() {
        let provider = ExternalProcessProvider::new("external", powershell()).with_args(vec![
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            &write_failing_provider().to_string_lossy(),
        ]);

        let err = provider.generate(sample_request(false)).unwrap_err();
        assert_eq!(err.code, ProviderErrorCode::ServerError);
        assert!(!err.message.contains("sk-core-secret"));
        assert!(!format!("{err:?}").contains("sk-core-secret"));
    }

    fn sample_router_request(model: &str) -> GenerateRequest {
        GenerateRequest {
            model: model.into(),
            messages: vec![Message {
                role: "user".into(),
                content: "test".into(),
                ..Default::default()
            }],
            tools: vec![],
            stream: true,
            auth_context: AuthContext::None,
            options: RequestOptions::default(),
        }
    }

    fn sample_request(stream: bool) -> GenerateRequest {
        GenerateRequest {
            model: "external/model".into(),
            messages: vec![Message {
                role: "user".into(),
                content: "hello".into(),
                ..Default::default()
            }],
            tools: vec![],
            stream,
            auth_context: AuthContext::ApiKey {
                provider: "external".into(),
                key: "sk-core-secret".into(),
            },
            options: RequestOptions::default(),
        }
    }

    fn write_non_stream_provider() -> PathBuf {
        write_provider_script(
            "non_stream_provider.ps1",
            r#"
$line = [Console]::In.ReadLine()
$req = $line | ConvertFrom-Json
if ($req.auth_context.key -ne 'sk-core-secret') { exit 2 }
$response = @{
  content = 'external response'
  finish_reason = 'stop'
  usage = @{
    prompt_tokens = 1
    completion_tokens = 2
    total_tokens = 3
    cache_creation_input_tokens = $null
    cache_read_input_tokens = $null
    reasoning_tokens = $null
  }
}
$response | ConvertTo-Json -Depth 8 -Compress
"#,
        )
    }

    fn write_stream_provider() -> PathBuf {
        write_provider_script(
            "stream_provider.ps1",
            r#"
$null = [Console]::In.ReadLine()
@{ ThinkingDelta = 'plan' } | ConvertTo-Json -Compress
@{ TextDelta = 'hello' } | ConvertTo-Json -Compress
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

    fn write_large_stderr_stream_provider() -> PathBuf {
        write_provider_script(
            "large_stderr_stream_provider.ps1",
            r#"
$null = [Console]::In.ReadLine()
$chunk = 'E' * 80000
for ($i = 0; $i -lt 30; $i++) { [Console]::Error.WriteLine($chunk) }
@{ TextDelta = 'stderr-ok' } | ConvertTo-Json -Compress
@{ Finished = 'Stop' } | ConvertTo-Json -Compress
"#,
        )
    }

    fn write_sleep_provider() -> PathBuf {
        write_provider_script(
            "sleep_provider.ps1",
            r#"
Start-Sleep -Seconds 3
"#,
        )
    }

    fn write_failing_provider() -> PathBuf {
        write_provider_script(
            "failing_provider.ps1",
            r#"
$null = [Console]::In.ReadLine()
[Console]::Error.WriteLine('sk-core-secret')
exit 42
"#,
        )
    }

    fn write_provider_script(name: &str, body: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("lingxiao_llm_{}", now_ms()));
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

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}
