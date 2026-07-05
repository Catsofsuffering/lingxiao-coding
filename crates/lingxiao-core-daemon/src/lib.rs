use lingxiao_core::command::CommandRouter;
use lingxiao_core::llm::{
    ExternalProcessProvider, LlmRouter, ModelRoutingMetadata, ProviderRegistry,
};
use lingxiao_core::persistence::DbOwner;
use lingxiao_core::process::ProcessRegistry;
use lingxiao_core::sidecar::SidecarCommand;
use lingxiao_core_protocol::actor::{Actor, ActorKind};
use lingxiao_core_protocol::command::{CommandEnvelope, CommandResponse};
use lingxiao_core_protocol::error::{CoreError, ErrorCode};
use serde::Deserialize;
use serde_json::json;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use std::time::Duration;

pub const DAEMON_NAME: &str = "lingxiao-core-daemon";
pub const DAEMON_VERSION: &str = env!("CARGO_PKG_VERSION");

pub fn daemon_info() -> String {
    format!("{DAEMON_NAME} v{DAEMON_VERSION}")
}

pub fn build_info() -> String {
    format!("{} (rustc {})", daemon_info(), rustc_version())
}

fn rustc_version() -> &'static str {
    option_env!("CARGO_PKG_RUST_VERSION").unwrap_or("unknown")
}

/// Open DB at `db_path`, initialize schema, and run the stdio JSON-lines loop.
pub fn serve(db_path: &str) -> Result<(), String> {
    serve_with_runtime_config(db_path, None)
}

/// Open DB at `db_path`, optionally configure external runtime executors, and
/// run the stdio JSON-lines loop.
pub fn serve_with_runtime_config(db_path: &str, config_path: Option<&str>) -> Result<(), String> {
    let db = DbOwner::open(db_path).map_err(|e| format!("Failed to open DB at {db_path}: {e}"))?;
    db.initialize()
        .map_err(|e| format!("Failed to initialize DB schema: {e}"))?;
    // Reset stuck workflows/agents left over from a previous crash. This is
    // safe to run unconditionally: it is idempotent and only touches rows in
    // terminal-equivalent stuck states (workflow running→paused, agent running→stopped).
    let recovery = db
        .recover_orphans()
        .map_err(|e| format!("Recovery sweep failed: {e}"))?;
    if recovery.recovered_any() {
        eprintln!(
            "Bootstrap recovery: workflows_paused={}, agents_stopped={}",
            recovery.workflows_paused, recovery.agents_stopped
        );
    }
    let process_cleanup = ProcessRegistry::new(db.clone())
        .cleanup_orphans()
        .map_err(|e| format!("Process cleanup sweep failed: {e}"))?;
    if process_cleanup.attempted > 0 {
        eprintln!(
            "Process cleanup: attempted={}, cleaned={}, failed={}",
            process_cleanup.attempted, process_cleanup.cleaned, process_cleanup.failed
        );
    }
    let config = match config_path {
        Some(path) => RuntimeConfig::load(path)?,
        None => RuntimeConfig::default(),
    };
    let router = configure_router(CommandRouter::new(db), &config)?;
    let router = Arc::new(router);
    let _schedule_ticker = config
        .background_schedule_tick_ms
        .filter(|interval| *interval > 0)
        .map(|interval| ScheduleTicker::start(Arc::clone(&router), interval));
    serve_with_router_arc(router)
}

fn configure_router(
    router: CommandRouter,
    config: &RuntimeConfig,
) -> Result<CommandRouter, String> {
    let mut router = router;

    if !config.llm_providers.is_empty() {
        let mut registry = ProviderRegistry::new();
        for provider in config.llm_providers.clone() {
            let provider_id = provider.provider_id.clone();
            let mut external = ExternalProcessProvider::new(
                Box::leak(provider.provider_id.into_boxed_str()),
                provider.program,
            )
            .with_args(provider.args)
            .with_models(provider.models.clone());
            if let Some(cwd) = provider.cwd {
                external = external.with_cwd(cwd);
            }
            if let Some(timeout_ms) = provider.timeout_ms {
                external = external.with_timeout_ms(timeout_ms);
            }
            registry.register(Arc::new(external));
            for model in provider.models {
                let mut metadata = ModelRoutingMetadata::new(provider_id.clone(), model);
                if let (Some(input), Some(output)) = (
                    provider.input_cost_per_million,
                    provider.output_cost_per_million,
                ) {
                    metadata = metadata.with_cost(input, output);
                }
                if let Some(context_window) = provider.context_window {
                    metadata = metadata.with_context_window(context_window);
                }
                if let Some(supports_tools) = provider.supports_tools {
                    metadata = metadata.with_tool_support(supports_tools);
                }
                if let Some(supports_streaming) = provider.supports_streaming {
                    metadata = metadata.with_streaming_support(supports_streaming);
                }
                registry.register_model_metadata(metadata);
            }
        }
        router = router.with_llm_router(LlmRouter::new(registry));
    }

    for sidecar in config.sidecars.clone() {
        router = router.with_sidecar_command(
            sidecar.tool_name,
            SidecarCommand {
                program: sidecar.program,
                args: sidecar.args,
                cwd: sidecar.cwd,
            },
        );
    }

    Ok(router)
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RuntimeConfig {
    #[serde(default)]
    llm_providers: Vec<LlmProviderConfig>,
    #[serde(default)]
    sidecars: Vec<SidecarConfig>,
    #[serde(default)]
    background_schedule_tick_ms: Option<u64>,
}

impl RuntimeConfig {
    fn load(path: &str) -> Result<Self, String> {
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read runtime config at {path}: {e}"))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("Failed to parse runtime config at {path}: {e}"))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct LlmProviderConfig {
    provider_id: String,
    program: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    models: Vec<String>,
    cwd: Option<PathBuf>,
    timeout_ms: Option<u64>,
    input_cost_per_million: Option<f64>,
    output_cost_per_million: Option<f64>,
    context_window: Option<u32>,
    supports_tools: Option<bool>,
    supports_streaming: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct SidecarConfig {
    tool_name: String,
    program: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    cwd: Option<PathBuf>,
}

pub struct ScheduleTicker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ScheduleTicker {
    pub fn start(router: Arc<CommandRouter>, interval_ms: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("lingxiao-schedule-ticker".into())
            .spawn(move || {
                while !stop_thread.load(Ordering::SeqCst) {
                    let submitted_at = now_ms();
                    let _ = router.dispatch(CommandEnvelope {
                        request_id: format!("retention-sweep-{submitted_at}"),
                        method: "retention.sweep".into(),
                        params: json!({}),
                        actor: Actor::with_id(ActorKind::System, "schedule-ticker"),
                        session_id: None,
                        idempotency_key: None,
                        submitted_at,
                    });
                    let _ = router.dispatch(CommandEnvelope {
                        request_id: format!("schedule-tick-{submitted_at}"),
                        method: "schedule.fire_due".into(),
                        params: json!({}),
                        actor: Actor::with_id(ActorKind::System, "schedule-ticker"),
                        session_id: None,
                        idempotency_key: None,
                        submitted_at,
                    });
                    std::thread::sleep(Duration::from_millis(interval_ms.max(1)));
                }
            })
            .expect("failed to spawn schedule ticker thread");
        Self {
            stop,
            handle: Some(handle),
        }
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ScheduleTicker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Run the stdio JSON-lines dispatch loop using an already-configured router.
///
/// Reads lines from stdin, parses each as [`CommandEnvelope`], dispatches through
/// `router`, and writes the [`CommandResponse`] as JSON to stdout.
/// Empty lines are ignored. Malformed JSON returns an error response with a
/// synthetic `request_id` if none can be extracted. EOF triggers a clean shutdown.
pub fn serve_with_router(router: &CommandRouter) -> Result<(), String> {
    serve_with_router_ref(router)
}

fn serve_with_router_arc(router: Arc<CommandRouter>) -> Result<(), String> {
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    for line in reader.lines() {
        let line = line.map_err(|e| format!("stdin read error: {e}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let response = dispatch_line(router.as_ref(), trimmed);
        let json =
            serde_json::to_string(&response).map_err(|e| format!("JSON serialize error: {e}"))?;
        writeln!(stdout, "{json}").map_err(|e| format!("stdout write error: {e}"))?;
        stdout
            .flush()
            .map_err(|e| format!("stdout flush error: {e}"))?;
    }

    Ok(())
}

fn serve_with_router_ref(router: &CommandRouter) -> Result<(), String> {
    let stdin = std::io::stdin();
    let reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    for line in reader.lines() {
        let line = line.map_err(|e| format!("stdin read error: {e}"))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let response = dispatch_line(router, trimmed);
        let json =
            serde_json::to_string(&response).map_err(|e| format!("JSON serialize error: {e}"))?;
        writeln!(stdout, "{json}").map_err(|e| format!("stdout write error: {e}"))?;
        stdout
            .flush()
            .map_err(|e| format!("stdout flush error: {e}"))?;
    }

    Ok(())
}

fn dispatch_line(router: &CommandRouter, trimmed: &str) -> CommandResponse {
    match serde_json::from_str::<CommandEnvelope>(trimmed) {
        Ok(cmd) => router.dispatch(cmd),
        Err(_) => {
            let rid = try_extract_request_id(trimmed);
            CommandResponse::err(
                rid,
                CoreError::new(ErrorCode::Serialization, "Malformed command JSON"),
            )
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Try to extract `request_id` from malformed JSON; fall back to `"invalid-json"`.
fn try_extract_request_id(line: &str) -> String {
    serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| v.get("request_id")?.as_str().map(String::from))
        .unwrap_or_else(|| "invalid-json".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lingxiao_core_protocol::actor::{Actor, ActorKind};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_REQ_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_cmd(
        method: &str,
        session_id: Option<&str>,
        params: serde_json::Value,
    ) -> CommandEnvelope {
        let ctr = TEST_REQ_COUNTER.fetch_add(1, Ordering::SeqCst);
        CommandEnvelope {
            request_id: format!("daemon-test-{ctr}"),
            method: method.into(),
            params,
            actor: Actor::new(ActorKind::User),
            session_id: session_id.map(str::to_string),
            idempotency_key: None,
            submitted_at: now_ms(),
        }
    }

    fn create_session(router: &CommandRouter) -> String {
        let response = router.dispatch(make_cmd(
            "session.create",
            None,
            json!({"workspace": "daemon-test"}),
        ));
        assert!(response.success, "{:?}", response.error);
        response.events[0].payload["session_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_daemon_info() {
        let info = daemon_info();
        assert!(info.contains(DAEMON_NAME));
        assert!(info.contains(DAEMON_VERSION));
    }

    #[test]
    fn test_try_extract_request_id_from_valid_json() {
        let line = r#"{"request_id":"req-001","method":"session.create","params":{},"actor":{"kind":"user"},"submitted_at":1000}"#;
        assert_eq!(try_extract_request_id(line), "req-001");
    }

    #[test]
    fn test_try_extract_request_id_fallback_on_garbage() {
        assert_eq!(try_extract_request_id("not json at all"), "invalid-json");
    }

    #[test]
    fn test_try_extract_request_id_fallback_on_json_without_request_id() {
        let line = r#"{"foo":"bar"}"#;
        assert_eq!(try_extract_request_id(line), "invalid-json");
    }

    #[test]
    fn test_runtime_config_parses_background_schedule_tick() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runtime.json");
        fs::write(&path, r#"{"background_schedule_tick_ms":25}"#).unwrap();
        let config = RuntimeConfig::load(path.to_str().unwrap()).unwrap();
        assert_eq!(config.background_schedule_tick_ms, Some(25));
    }

    #[test]
    fn test_runtime_config_model_metadata_controls_routing() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let config = RuntimeConfig {
            llm_providers: vec![LlmProviderConfig {
                provider_id: "external".into(),
                program: PathBuf::from("missing-provider-binary-for-metadata-test"),
                args: Vec::new(),
                models: vec!["external/model".into()],
                cwd: None,
                timeout_ms: None,
                input_cost_per_million: Some(1.0),
                output_cost_per_million: Some(1.0),
                context_window: Some(1024),
                supports_tools: Some(false),
                supports_streaming: Some(true),
            }],
            sidecars: Vec::new(),
            background_schedule_tick_ms: None,
        };
        let router = configure_router(CommandRouter::new(db), &config).unwrap();
        let sid = create_session(&router);

        let response = router.dispatch(make_cmd(
            "llm.call",
            Some(&sid),
            json!({
                "llm_call_id": "metadata-routed",
                "model": "external/model",
                "prompt": "needs native tool schemas"
            }),
        ));

        assert!(
            !response.success,
            "metadata should reject tool-incompatible provider"
        );
        assert_eq!(
            response.error.unwrap().message,
            "LLM provider routing failed"
        );
    }

    #[test]
    fn test_daemon_router_agent_spawn_supervised_lifecycle() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let router = configure_router(CommandRouter::new(db.clone()), &RuntimeConfig::default())
            .expect("router config")
            .with_test_mock_llm();
        let sid = create_session(&router);

        let task = router.dispatch(make_cmd(
            "task.create",
            Some(&sid),
            json!({"task_id": "daemon-agent-task", "subject": "daemon supervised task"}),
        ));
        assert!(task.success, "{:?}", task.error);

        let spawned = router.dispatch(make_cmd(
            "agent.spawn",
            Some(&sid),
            json!({
                "agent_id": "daemon-agent",
                "task_id": "daemon-agent-task",
                "run": true,
                "model": "mock/model",
                "max_rounds": 1,
            }),
        ));
        assert!(spawned.success, "{:?}", spawned.error);

        for _ in 0..100 {
            let status: String = db
                .conn()
                .query_row(
                    "SELECT status FROM agent_state WHERE session_id = ?1 AND agent_id = 'daemon-agent'",
                    [sid.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            if status == "stopped" {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let (status, stopped): (String, i64) = db
            .conn()
            .query_row(
                "SELECT status, stopped FROM agent_state WHERE session_id = ?1 AND agent_id = 'daemon-agent'",
                [sid.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(status, "stopped");
        assert_eq!(stopped, 1);

        let completed_logs: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM agent_logs \
                 WHERE session_id = ?1 AND agent_id = 'daemon-agent' \
                   AND event_type = 'agent.completed'",
                [sid.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(completed_logs, 1);
    }

    #[test]
    fn test_schedule_ticker_fires_due_schedule() {
        let db = DbOwner::open_in_memory().unwrap();
        db.initialize().unwrap();
        let router = Arc::new(CommandRouter::new(db.clone()));
        let sid = create_session(router.as_ref());
        let created = router.dispatch(make_cmd(
            "schedule.create",
            Some(&sid),
            json!({
                "schedule_id": "daemon-sched",
                "cron": "every 5m",
                "prompt": "daemon scheduled work",
                "next_run_at": 0.0,
            }),
        ));
        assert!(created.success, "{:?}", created.error);

        let ticker = ScheduleTicker::start(Arc::clone(&router), 10);
        for _ in 0..20 {
            let count: i64 = db
                .conn()
                .query_row(
                    "SELECT COUNT(*) FROM tasks WHERE session_id = ?1 AND origin = 'schedule:daemon-sched'",
                    [sid.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            if count == 1 {
                ticker.shutdown();
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        ticker.shutdown();
        panic!("schedule ticker did not fire due schedule");
    }
}
