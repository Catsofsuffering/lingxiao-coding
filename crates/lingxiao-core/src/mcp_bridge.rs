use crate::process::ProcessRegistry;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use wait_timeout::ChildExt;

#[derive(Debug, Clone)]
pub struct McpBridgeRequest {
    pub bridge_id: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub payload: Value,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpBridgeError {
    MissingProgram,
    SpawnFailed(String),
    WriteFailed(String),
    Timeout,
    Failed(String),
    InvalidJson(String),
}

#[derive(Clone)]
pub struct McpBridgeRunner {
    process_registry: ProcessRegistry,
    servers: Arc<Mutex<HashMap<String, PersistentMcpServer>>>,
}

pub struct McpServerStartResult {
    pub initialized: Value,
    pub tools_response: Value,
}

impl McpBridgeRunner {
    pub fn new(process_registry: ProcessRegistry) -> Self {
        Self {
            process_registry,
            servers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn invoke(&self, request: McpBridgeRequest) -> Result<Value, McpBridgeError> {
        if request.program.as_os_str().is_empty() {
            return Err(McpBridgeError::MissingProgram);
        }
        let mut command = Command::new(&request.program);
        command
            .args(&request.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(cwd) = request.cwd.as_deref() {
            command.current_dir(cwd);
        }
        let mut child = command
            .spawn()
            .map_err(|error| McpBridgeError::SpawnFailed(error.to_string()))?;
        let process_id = format!("mcp:{}", request.bridge_id);
        self.process_registry
            .register(
                process_id.clone(),
                child.id(),
                "mcp",
                request.bridge_id.clone(),
                request.program.display().to_string(),
            )
            .map_err(|error| McpBridgeError::SpawnFailed(error.to_string()))?;

        if let Some(stdin) = child.stdin.as_mut() {
            let mut line = serde_json::to_vec(&request.payload)
                .map_err(|error| McpBridgeError::WriteFailed(error.to_string()))?;
            line.push(b'\n');
            stdin
                .write_all(&line)
                .map_err(|error| McpBridgeError::WriteFailed(error.to_string()))?;
        }
        drop(child.stdin.take());
        if let Err(error) = wait_child(&mut child, request.timeout_ms) {
            let _ = self
                .process_registry
                .mark_failed(&process_id, &mcp_error_message_local(&error));
            return Err(error);
        }
        let output = child
            .wait_with_output()
            .map_err(|error| McpBridgeError::Failed(error.to_string()))?;
        let exit_code = output.status.code().unwrap_or(-1);
        if output.status.success() {
            let _ = self.process_registry.complete(&process_id, Some(exit_code));
            let stdout = String::from_utf8_lossy(&output.stdout);
            let response = serde_json::from_str::<Value>(stdout.trim())
                .map_err(|error| McpBridgeError::InvalidJson(error.to_string()))?;
            Ok(json!({
                "bridge_id": request.bridge_id,
                "exit_code": exit_code,
                "response": response,
            }))
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr)
                .lines()
                .next()
                .unwrap_or("mcp bridge failed")
                .to_string();
            let _ = self.process_registry.mark_failed(&process_id, &stderr);
            Err(McpBridgeError::Failed(stderr))
        }
    }

    pub fn start_server(
        &self,
        request: McpBridgeRequest,
    ) -> Result<McpServerStartResult, McpBridgeError> {
        if request.program.as_os_str().is_empty() {
            return Err(McpBridgeError::MissingProgram);
        }
        self.stop_server(&request.bridge_id).ok();
        let mut server = spawn_persistent_server(&self.process_registry, &request)?;
        let initialized = match server.exchange(request.payload, request.timeout_ms) {
            Ok(response) => response,
            Err(error) => {
                server.fail(&self.process_registry, &mcp_error_message_local(&error));
                return Err(error);
            }
        };
        let tools_response = match server.exchange(
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            request.timeout_ms,
        ) {
            Ok(response) => response,
            Err(error) => {
                server.fail(&self.process_registry, &mcp_error_message_local(&error));
                return Err(error);
            }
        };
        self.servers
            .lock()
            .unwrap()
            .insert(request.bridge_id, server);
        Ok(McpServerStartResult {
            initialized,
            tools_response,
        })
    }

    pub fn call_server(
        &self,
        server_id: &str,
        payload: Value,
        timeout_ms: u64,
    ) -> Result<Value, McpBridgeError> {
        let result = {
            let mut servers = self.servers.lock().unwrap();
            let Some(server) = servers.get_mut(server_id) else {
                return Err(McpBridgeError::Failed(
                    "MCP server is not running".to_string(),
                ));
            };
            server.exchange(payload, timeout_ms)
        };
        if let Err(error) = &result {
            if matches!(
                error,
                McpBridgeError::Timeout
                    | McpBridgeError::Failed(_)
                    | McpBridgeError::WriteFailed(_)
            ) {
                self.fail_server(server_id, &mcp_error_message_local(error));
            }
        }
        result
    }

    pub fn stop_server(&self, server_id: &str) -> Result<Option<i32>, McpBridgeError> {
        let Some(mut server) = self.servers.lock().unwrap().remove(server_id) else {
            return Ok(None);
        };
        Ok(Some(server.stop(&self.process_registry)))
    }

    fn fail_server(&self, server_id: &str, message: &str) {
        if let Some(mut server) = self.servers.lock().unwrap().remove(server_id) {
            server.fail(&self.process_registry, message);
        }
    }
}

struct PersistentMcpServer {
    process_id: String,
    child: Child,
    stdin: ChildStdin,
    stdout_rx: mpsc::Receiver<Result<String, String>>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
}

fn spawn_persistent_server(
    registry: &ProcessRegistry,
    request: &McpBridgeRequest,
) -> Result<PersistentMcpServer, McpBridgeError> {
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(cwd) = request.cwd.as_deref() {
        command.current_dir(cwd);
    }
    let mut child = command
        .spawn()
        .map_err(|error| McpBridgeError::SpawnFailed(error.to_string()))?;
    let process_id = format!("mcp:{}", request.bridge_id);
    registry
        .register(
            process_id.clone(),
            child.id(),
            "mcp",
            request.bridge_id.clone(),
            request.program.display().to_string(),
        )
        .map_err(|error| McpBridgeError::SpawnFailed(error.to_string()))?;
    let stdin = child.stdin.take().ok_or_else(|| {
        let _ = registry.mark_failed(&process_id, "mcp stdin unavailable");
        McpBridgeError::SpawnFailed("mcp stdin unavailable".to_string())
    })?;
    let stdout = child.stdout.take().ok_or_else(|| {
        let _ = registry.mark_failed(&process_id, "mcp stdout unavailable");
        McpBridgeError::SpawnFailed("mcp stdout unavailable".to_string())
    })?;
    let stderr = child.stderr.take();
    let (tx, rx) = mpsc::channel();
    let stdout_reader = thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if tx.send(line.map_err(|error| error.to_string())).is_err() {
                break;
            }
        }
    });
    let stderr_reader = stderr.map(|mut stderr| {
        thread::spawn(move || {
            let mut buffer = [0_u8; 8192];
            loop {
                match stderr.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
    });
    Ok(PersistentMcpServer {
        process_id,
        child,
        stdin,
        stdout_rx: rx,
        stdout_reader: Some(stdout_reader),
        stderr_reader,
    })
}

impl PersistentMcpServer {
    fn exchange(&mut self, payload: Value, timeout_ms: u64) -> Result<Value, McpBridgeError> {
        let mut line = serde_json::to_vec(&payload)
            .map_err(|error| McpBridgeError::WriteFailed(error.to_string()))?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .map_err(|error| McpBridgeError::WriteFailed(error.to_string()))?;
        self.stdin
            .flush()
            .map_err(|error| McpBridgeError::WriteFailed(error.to_string()))?;
        match self
            .stdout_rx
            .recv_timeout(Duration::from_millis(timeout_ms))
        {
            Ok(Ok(line)) => serde_json::from_str::<Value>(line.trim())
                .map_err(|error| McpBridgeError::InvalidJson(error.to_string())),
            Ok(Err(error)) => Err(McpBridgeError::Failed(error)),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(McpBridgeError::Timeout),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(McpBridgeError::Failed(
                "MCP server stdout closed".to_string(),
            )),
        }
    }

    fn stop(&mut self, registry: &ProcessRegistry) -> i32 {
        let _ = self.child.kill();
        let status = self.child.wait().ok();
        let exit_code = status.and_then(|status| status.code()).unwrap_or(-1);
        let _ = registry.complete(&self.process_id, Some(exit_code));
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
        exit_code
    }

    fn fail(&mut self, registry: &ProcessRegistry, message: &str) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = registry.mark_failed(&self.process_id, message);
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
    }
}

fn wait_child(child: &mut Child, timeout_ms: u64) -> Result<(), McpBridgeError> {
    match child
        .wait_timeout(Duration::from_millis(timeout_ms))
        .map_err(|error| McpBridgeError::Failed(error.to_string()))?
    {
        Some(_) => Ok(()),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            Err(McpBridgeError::Timeout)
        }
    }
}

fn mcp_error_message_local(error: &McpBridgeError) -> String {
    match error {
        McpBridgeError::MissingProgram => "MCP bridge program is missing".to_string(),
        McpBridgeError::SpawnFailed(message) => format!("MCP bridge spawn failed: {message}"),
        McpBridgeError::WriteFailed(message) => format!("MCP bridge write failed: {message}"),
        McpBridgeError::Timeout => "MCP bridge timed out".to_string(),
        McpBridgeError::Failed(message) => format!("MCP bridge failed: {message}"),
        McpBridgeError::InvalidJson(message) => {
            format!("MCP bridge returned invalid JSON: {message}")
        }
    }
}

pub fn mcp_error_payload(error: &McpBridgeError) -> Value {
    match error {
        McpBridgeError::MissingProgram => json!({"kind": "missing_program"}),
        McpBridgeError::SpawnFailed(_) => json!({"kind": "spawn_failed"}),
        McpBridgeError::WriteFailed(_) => json!({"kind": "write_failed"}),
        McpBridgeError::Timeout => json!({"kind": "timeout"}),
        McpBridgeError::Failed(_) => json!({"kind": "failed"}),
        McpBridgeError::InvalidJson(_) => json!({"kind": "invalid_json"}),
    }
}

pub fn mcp_error_message(error: &McpBridgeError) -> String {
    match error {
        McpBridgeError::MissingProgram => "MCP bridge program is missing".to_string(),
        McpBridgeError::SpawnFailed(message) => format!("MCP bridge spawn failed: {message}"),
        McpBridgeError::WriteFailed(message) => format!("MCP bridge write failed: {message}"),
        McpBridgeError::Timeout => "MCP bridge timed out".to_string(),
        McpBridgeError::Failed(message) => format!("MCP bridge failed: {message}"),
        McpBridgeError::InvalidJson(message) => {
            format!("MCP bridge returned invalid JSON: {message}")
        }
    }
}
