use crate::process::ProcessRegistry;
use serde_json::{json, Value};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
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
}

impl McpBridgeRunner {
    pub fn new(process_registry: ProcessRegistry) -> Self {
        Self { process_registry }
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
        wait_child(&mut child, request.timeout_ms)?;
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
